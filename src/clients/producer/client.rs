use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use kafka_protocol::{
    messages::{
        produce_request::{PartitionProduceData, TopicProduceData},
        ProduceRequest, ProduceResponse, TopicName,
    },
    protocol::StrBytes,
    records::{
        Compression, Record, RecordEncodeOptions, TimestampType, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    },
};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::task::TaskTracker;

use crate::{
    clients::network::NetworkClient,
    common::TopicPartition,
    conn::RecordBatchEncoder,
    error::{ErrorCode, KafkaError},
    proto::ver::with_max_version,
};

use super::{
    arena::{PreparedRecord, ProducerArena},
    KeyHashPartitioner, Partitioner, PartitionerSession,
};

pub struct ProducerRecord {
    pub topic: TopicName,
    pub partition: Option<i32>,
    pub timestamp: Option<i64>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: indexmap::IndexMap<StrBytes, Option<Bytes>>,
}

#[non_exhaustive]
pub struct RecordMetadata {
    pub topic_partition: TopicPartition,
    pub base_offset: i64,
}

struct ProduceChunk<'p, P> {
    messages: Vec<ProducerTaskMessage>,
    partitioner: &'p mut P,
}

struct ProducerTaskMessage {
    record: ProducerRecord,
    /// The partition set on the record by the user, so the partitioning strategy can determine if it should modify the
    /// one on the record.
    user_partition: Option<i32>,
    tx: oneshot::Sender<Result<RecordMetadata, KafkaError>>,
}

impl ProducerTaskMessage {
    fn new(
        record: ProducerRecord,
        tx: oneshot::Sender<Result<RecordMetadata, KafkaError>>,
    ) -> Self {
        Self {
            user_partition: record.partition,
            record,
            tx,
        }
    }
}

struct ProducerTask {
    client: NetworkClient,
    arena: ProducerArena,
}

const CHUNK_SIZE: usize = 2000;
const CHUNK_TIMEOUT: Duration = Duration::from_millis(500);

impl ProducerTask {
    fn new(client: NetworkClient) -> Self {
        Self {
            client,
            arena: Default::default(),
        }
    }

    async fn run(
        mut self,
        rx: mpsc::Receiver<ProducerTaskMessage>,
        mut create_partitioner: impl Partitioner,
    ) {
        let record_stream = ReceiverStream::new(rx).chunks_timeout(CHUNK_SIZE, CHUNK_TIMEOUT);

        tokio::pin!(record_stream);

        while let Some(chunk) = record_stream.next().await {
            if let Err(e) = self
                .send_chunk(ProduceChunk {
                    messages: chunk,
                    partitioner: &mut create_partitioner,
                })
                .await
            {
                tracing::error!("encountered an unrecoverable error while producing messages: {e}");
                break;
            }
        }
    }

    async fn send_chunk(
        &mut self,
        mut chunk: ProduceChunk<'_, impl Partitioner>,
    ) -> Result<(), KafkaError> {
        self.client
            .load_topic_metadata(
                chunk
                    .messages
                    .iter()
                    .map(|msg| &msg.record.topic)
                    .collect::<FxHashSet<_>>(),
            )
            .await?;

        let invalid_topic_names = self.get_invalid_topics_and_populate_partitions(&mut chunk);

        if !invalid_topic_names.is_empty() {
            // Refresh invalid topics
            self.client
                .invalidate_topic_metadata(invalid_topic_names.iter());
            self.client
                .load_topic_metadata(invalid_topic_names.iter())
                .await?;
        }

        self.populate_arena(chunk);

        for leader in self.arena.brokers.iter_mut() {
            if leader.is_empty() {
                self.arena.empty_leaders.push(leader.broker_id);
                continue;
            }

            let mut req = ProduceRequest::default();

            for (tp, prepared_records) in leader.partitions.iter_mut() {
                if prepared_records.is_empty() {
                    self.arena.empty_partitions.push(tp.clone());
                    continue;
                }

                let mut records = BytesMut::new();

                if let Err(e) = RecordBatchEncoder::encode(
                    &mut records,
                    prepared_records.iter().map(|ctx| &ctx.record),
                    &RecordEncodeOptions {
                        version: 2,
                        compression: Compression::None, // TODO config
                    },
                ) {
                    tracing::error!("failed to encode record batch for topic {tp}: {e}");

                    for ctx in prepared_records.drain(..) {
                        let _ = ctx.tx.send(Err(KafkaError::Channel(
                            io::Error::new(io::ErrorKind::Other, "record batch failed to encode")
                                .into(),
                        )));
                    }
                    continue;
                }

                let partition_data = PartitionProduceData::default()
                    .with_index(tp.partition())
                    .with_records(Some(records.into()));

                if let Some(produce_data) = self.arena.topic_data.get_mut(tp.name()) {
                    produce_data.partition_data.push(partition_data);
                } else {
                    self.arena.topic_data.insert(
                        tp.name().clone(),
                        TopicProduceData::default().with_partition_data(vec![partition_data]),
                    );
                }
            }

            for tp in self.arena.empty_partitions.drain(..) {
                leader.partitions.remove(&tp);
            }

            for (name, mut data) in self.arena.topic_data.drain() {
                data.name = name;
                req.topic_data.push(data);
            }

            let build_req = with_max_version(|_ver| {
                // TODO config
                req.acks = 1;
                req.timeout_ms = 1000;
                req.transactional_id = None;

                Some(req)
            });

            let res = self.client.send_to(build_req, leader.broker_id).await;

            match res {
                Ok(response) => Self::handle_produce_response(response, &mut leader.partitions),
                Err(e) => {
                    tracing::error!("failed to send produce request: {e}");

                    for contexts in leader.partitions.values_mut() {
                        for context in contexts.drain(..) {
                            let _ = context.tx.send(Err(e.representative_clone()));
                        }
                    }
                }
            }
        }

        for leader_id in self.arena.empty_leaders.drain(..) {
            self.arena.brokers.remove(&leader_id);
        }

        Ok(())
    }

    fn get_invalid_topics_and_populate_partitions(
        &self,
        chunk: &mut ProduceChunk<'_, impl Partitioner>,
    ) -> FxHashSet<TopicName> {
        let cluster = &self.client.borrow_cluster();

        let mut invalid_topic_names = FxHashSet::<TopicName>::default();

        let mut partitioner = chunk.partitioner.new_partitioner(cluster);

        for msg in chunk.messages.iter_mut() {
            let Ok(topic_data) = cluster
                .metadata
                .get_topic_metadata_by_name(&msg.record.topic)
            else {
                invalid_topic_names.insert(msg.record.topic.clone());
                continue;
            };

            if msg.user_partition.is_none() {
                partitioner.partition(&mut msg.record, topic_data);
            }

            if msg
                .record
                .partition
                .and_then(|index| topic_data.get_partition_metadata(index).ok())
                .is_none()
            {
                // The partitioner gave us an invalid partition
                invalid_topic_names.insert(msg.record.topic.clone());
                continue;
            };
        }

        chunk.partitioner.finish_partitioning(partitioner);

        invalid_topic_names
    }

    fn populate_arena(&mut self, chunk: ProduceChunk<'_, impl Partitioner>) {
        let cluster = &self.client.borrow_cluster();

        let mut partitioner = chunk.partitioner.new_partitioner(cluster);

        let fallback_timestamp = {
            let start = SystemTime::now();
            start
                .duration_since(UNIX_EPOCH)
                .map(|ts| ts.as_millis() as i64)
                .unwrap_or_else(|e| -(e.duration().as_millis() as i64))
        };

        for mut msg in chunk.messages {
            let topic_data = match cluster
                .metadata
                .get_topic_metadata_by_name(&msg.record.topic)
            {
                Ok(topic_data) => topic_data,
                Err(e) => {
                    let _ = msg.tx.send(Err(e.into()));
                    continue;
                }
            };

            let original_partition_index = msg.record.partition;

            let mut partition = match msg.record.partition {
                Some(index) => topic_data.get_partition_metadata(index),
                None => Err(ErrorCode::UnknownTopicOrPartition),
            };

            if partition.is_err() && msg.user_partition.is_none() {
                // Only partition records that did not get partitioned in the first round, or whose partitions no
                // longer exist after refreshing topic data.
                // However, if there is a user-specified partition, do not re-partition to another one.
                partitioner.partition(&mut msg.record, topic_data);
            }

            // If the index changed, try to find it again
            if original_partition_index != msg.record.partition {
                partition = match msg.record.partition {
                    Some(index) => topic_data.get_partition_metadata(index),
                    None => Err(ErrorCode::UnknownTopicOrPartition),
                };
            }

            let partition = match partition {
                Ok(partition) => partition,
                Err(e) => {
                    let _ = msg.tx.send(Err(e.into()));
                    continue;
                }
            };

            let timestamp = msg.record.timestamp.unwrap_or(fallback_timestamp);

            let part_map = self.arena.brokers.get_mut_or_default(partition.leader_id);

            let records = part_map
                .partitions
                .entry(TopicPartition::new(
                    msg.record.topic.clone(),
                    partition.index,
                ))
                .or_default();

            partitioner.partition_validated(&msg.record, partition);

            let record = Record {
                transactional: false,
                control: false,
                partition_leader_epoch: partition.leader_epoch,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: records.len() as i64,
                sequence: records.len() as i32,
                timestamp,
                key: msg.record.key,
                value: msg.record.value,
                headers: msg.record.headers,
            };

            records.push(PreparedRecord { record, tx: msg.tx });
        }

        chunk.partitioner.finish_partitioning(partitioner);
    }

    fn handle_produce_response(
        response: ProduceResponse,
        context_map: &mut FxHashMap<TopicPartition, Vec<PreparedRecord>>,
    ) {
        for response in response.responses.into_iter() {
            for part_response in response.partition_responses.into_iter() {
                let tp = TopicPartition::new(response.name.clone(), part_response.index);

                let Some(contexts) = context_map.get_mut(&tp) else {
                    tracing::warn!(
                        "got a produce response for a partition we did not send data to: {tp}"
                    );
                    continue;
                };

                if part_response.error_code != ErrorCode::None as i16 {
                    for ctx in contexts.drain(..) {
                        let _ = ctx
                            .tx
                            .send(Err(KafkaError::ErrorCode(part_response.error_code.into())));
                    }

                    continue;
                }

                for ctx in contexts.drain(..) {
                    let _ = ctx.tx.send(Ok(RecordMetadata {
                        topic_partition: tp.clone(),
                        base_offset: part_response.base_offset,
                    }));
                }
            }
        }
    }
}

pub struct Producer {
    client: NetworkClient,
    tx: mpsc::Sender<ProducerTaskMessage>,
    task_tracker: TaskTracker,
}

/// A future that resolves to [`RecordMetadata`] once the server has acknowledged the record.
pub struct ProduceFuture {
    rx: oneshot::Receiver<Result<RecordMetadata, KafkaError>>,
}

impl Unpin for ProduceFuture {}

impl Future for ProduceFuture {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.rx.poll_unpin(cx) {
            Poll::Ready(rdy) => Poll::Ready(rdy.unwrap_or_else(|e| Err(e.into()))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Producer {
    /// Create a new producer with the default [`Partitioner`].
    pub fn new(client: NetworkClient) -> Self {
        Self::new_with_partitioner(client, KeyHashPartitioner)
    }

    /// Create a new producer with the [`Partitioner`] implementation specified.
    pub fn new_with_partitioner(
        client: NetworkClient,
        partitioner: impl Partitioner + 'static,
    ) -> Self {
        let (tx, rx) = mpsc::channel(CHUNK_SIZE);

        let task = ProducerTask::new(client.clone());

        let task_tracker = TaskTracker::new();

        task_tracker.spawn(task.run(rx, partitioner));

        Self {
            client,
            tx,
            task_tracker,
        }
    }

    /// Send a message to a topic.
    ///
    /// This method returns a nested future.
    /// - The outer future resolves when the message has been queued. Await this for backpressure.
    /// - The inner future resolves when the record has been acknowledged by the server.
    pub async fn send(&self, record: ProducerRecord) -> Result<ProduceFuture, KafkaError> {
        let (tx, rx) = oneshot::channel();

        self.tx.send(ProducerTaskMessage::new(record, tx)).await?;

        Ok(ProduceFuture { rx })
    }

    /// Flush all pending messages, but do not close any connections.
    ///
    /// This is useful if you are running multiple producers using a shared network client to have more control over
    /// when messages are flushed.
    pub async fn flush_and_close(self) {
        drop(self.tx);
        self.task_tracker.close();
        self.task_tracker.wait().await;
    }

    /// Flush all pending messages, then shut down the client after the producer has sent them all.
    pub async fn flush_and_shutdown(self) {
        drop(self.tx);
        self.task_tracker.close();
        self.task_tracker.wait().await;
        self.client.shutdown().await
    }

    /// Stop the client and prevent any future messages
    pub async fn shutdown(&self) {
        self.client.shutdown().await
    }

    /// Resolves when the client as shut down
    pub async fn await_shutdown(&self) {
        self.client.await_shutdown().await
    }
}
