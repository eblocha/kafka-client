use std::{
    collections::HashMap,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use fnv::FnvHashMap;
use futures::FutureExt;
use itertools::izip;
use kafka_protocol::{
    messages::{produce_request::PartitionProduceData, ProduceRequest, ProduceResponse, TopicName},
    protocol::StrBytes,
    records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    },
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::task::TaskTracker;

use crate::{
    clients::network::NetworkClient,
    common::TopicPartition,
    error::{ErrorCode, KafkaError},
    proto::ver::with_max_version,
};

pub struct ProducerRecord {
    pub topic: TopicName,
    pub partition: Option<i32>,
    pub timestamp: Option<i64>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: indexmap::IndexMap<StrBytes, Option<Bytes>>,
}

struct ProduceContext {
    record: Record,
    #[allow(unused)]
    tx: oneshot::Sender<Result<RecordMetadata, KafkaError>>,
}

#[non_exhaustive]
pub struct RecordMetadata {}

struct ProducerTaskMessage {
    record: ProducerRecord,
    tx: oneshot::Sender<Result<RecordMetadata, KafkaError>>,
}

struct ProducerTask {
    // sequence: i32,
    client: NetworkClient,
}

const CHUNK_SIZE: usize = 2000;
const CHUNK_TIMEOUT: Duration = Duration::from_millis(500);

impl ProducerTask {
    fn new(client: NetworkClient) -> Self {
        Self {
            client,
            // sequence: 0,
        }
    }

    async fn run(mut self, rx: mpsc::Receiver<ProducerTaskMessage>) {
        let record_stream = ReceiverStream::new(rx).chunks_timeout(CHUNK_SIZE, CHUNK_TIMEOUT);

        tokio::pin!(record_stream);

        while let Some(chunk) = record_stream.next().await {
            if let Err(e) = self.send_chunk(chunk).await {
                tracing::error!("encountered an unrecoverable error while producing messages: {e}");
                break;
            }
        }
    }

    async fn send_chunk(&mut self, chunk: Vec<ProducerTaskMessage>) -> Result<(), KafkaError> {
        let topic_names = chunk
            .iter()
            .map(|msg| msg.record.topic.clone())
            .collect::<Vec<_>>();

        let mut partitions = chunk
            .iter()
            .map(|msg| msg.record.partition)
            .collect::<Vec<_>>();

        self.client.load_topic_metadata(topic_names.iter()).await?;

        let invalid_topic_names =
            self.get_invalid_topics_and_populate_partitions(&mut partitions, &topic_names, &chunk);

        if !invalid_topic_names.is_empty() {
            // Refresh invalid topics
            self.client
                .invalidate_topic_metadata(invalid_topic_names.iter());
            self.client
                .load_topic_metadata(invalid_topic_names.iter())
                .await?;
        }

        let mapping = self.create_produce_contexts(&partitions, &topic_names, chunk);

        for (leader_id, partitions) in mapping.into_iter() {
            let mut req = ProduceRequest::default();
            let mut context_map = HashMap::new();

            for (tp, contexts) in partitions.into_iter() {
                let mut records = BytesMut::new();

                if let Err(e) = RecordBatchEncoder::encode(
                    &mut records,
                    contexts.iter().map(|ctx| &ctx.record),
                    &RecordEncodeOptions {
                        version: 2,
                        compression: Compression::None, // TODO config
                    },
                ) {
                    tracing::error!("failed to encode record batch for topic {tp}: {e}");

                    for ctx in contexts.into_iter() {
                        let _ = ctx.tx.send(Err(KafkaError::Channel(
                            io::Error::from(io::ErrorKind::InvalidInput).into(),
                        )));
                    }
                    continue;
                }

                req.topic_data
                    .entry(tp.name().clone())
                    .or_default()
                    .partition_data
                    .push({
                        let mut partition_data = PartitionProduceData::default();

                        partition_data.index = tp.partition();
                        partition_data.records = Some(records.into());

                        partition_data
                    });

                context_map.insert(tp, contexts);
            }

            self.send(req, leader_id, context_map).await;
        }

        Ok(())
    }

    fn get_invalid_topics_and_populate_partitions(
        &self,
        partitions: &mut [Option<i32>],
        topic_names: &[TopicName],
        chunk: &[ProducerTaskMessage],
    ) -> Vec<TopicName> {
        let topic_map = &self.client.borrow_cluster().metadata.topics;

        let mut invalid_topic_names = Vec::<TopicName>::new();

        for (partiton, topic_name, msg) in izip!(partitions, topic_names, chunk) {
            let Some(topic_data) = topic_map.get(topic_name) else {
                invalid_topic_names.push(topic_name.clone());
                continue;
            };

            if topic_data.error_code != ErrorCode::None as i16 {
                invalid_topic_names.push(topic_name.clone());
                continue;
            }

            // TODO compute from key value, or random if no key
            let partition_index = msg.record.partition.unwrap_or_default();
            *partiton = Some(partition_index);

            let Some(partition) = topic_data
                .partitions
                .iter()
                .find(|part| part.partition_index == partition_index)
            else {
                invalid_topic_names.push(topic_name.clone());
                continue;
            };

            if partition.error_code != ErrorCode::None as i16 {
                invalid_topic_names.push(topic_name.clone());
                continue;
            }
        }

        invalid_topic_names
    }

    fn create_produce_contexts(
        &self,
        partitions: &[Option<i32>],
        topic_names: &[TopicName],
        chunk: Vec<ProducerTaskMessage>,
    ) -> FnvHashMap<i32, HashMap<TopicPartition, Vec<ProduceContext>>> {
        let mut mapping =
            FnvHashMap::<i32, HashMap<TopicPartition, Vec<ProduceContext>>>::default();

        let topic_map = &self.client.borrow_cluster().metadata.topics;

        for (partition, topic_name, msg) in izip!(partitions, topic_names, chunk) {
            let Some(topic_data) = topic_map.get(topic_name) else {
                let _ = msg.tx.send(Err(ErrorCode::UnknownTopicOrPartition.into()));
                continue;
            };

            if topic_data.error_code != ErrorCode::None as i16 {
                let _ = msg
                    .tx
                    .send(Err(ErrorCode::from(topic_data.error_code).into()));
                continue;
            }

            // TODO compute from key value, or random if no key
            let partition_index = partition.unwrap_or_default();

            let Some(partition) = topic_data
                .partitions
                .iter()
                .find(|part| part.partition_index == partition_index)
            else {
                let _ = msg.tx.send(Err(ErrorCode::UnknownTopicOrPartition.into()));
                continue;
            };

            if partition.error_code != ErrorCode::None as i16 {
                let _ = msg
                    .tx
                    .send(Err(ErrorCode::from(topic_data.error_code).into()));
                continue;
            }

            let timestamp = msg.record.timestamp.unwrap_or_else(|| {
                let start = SystemTime::now();
                start
                    .duration_since(UNIX_EPOCH)
                    .map(|ts| ts.as_millis() as i64)
                    .unwrap_or_else(|e| -(e.duration().as_millis() as i64))
            });

            let part_map = mapping.entry(partition.leader_id.0).or_default();

            let records = part_map
                .entry(TopicPartition::new(topic_name.clone(), partition_index))
                .or_default();

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

            records.push(ProduceContext { record, tx: msg.tx });
        }

        mapping
    }

    fn handle_produce_response(
        &self,
        response: ProduceResponse,
        mut context_map: HashMap<TopicPartition, Vec<ProduceContext>>,
    ) {
        for (topic_name, response) in response.responses.into_iter() {
            for part_response in response.partition_responses.into_iter() {
                let tp = TopicPartition::new(topic_name.clone(), part_response.index);

                let Some(contexts) = context_map.remove(&tp) else {
                    tracing::warn!(
                        "got a produce response for a partition we did not send data to: {tp}"
                    );
                    continue;
                };

                if part_response.error_code != ErrorCode::None as i16 {
                    for ctx in contexts.into_iter() {
                        let _ = ctx
                            .tx
                            .send(Err(KafkaError::ErrorCode(part_response.error_code.into())));
                    }

                    continue;
                }

                for ctx in contexts.into_iter() {
                    let _ = ctx.tx.send(Ok(RecordMetadata {}));
                }
            }
        }
    }

    async fn send(
        &self,
        mut req: ProduceRequest,
        leader_id: i32,
        context_map: HashMap<TopicPartition, Vec<ProduceContext>>,
    ) {
        let res = self
            .client
            .send_to(
                with_max_version(|_ver| {
                    // TODO config
                    req.acks = 1;
                    req.timeout_ms = 1000;
                    req.transactional_id = None;

                    Some(req)
                }),
                leader_id,
            )
            .await;

        match res {
            Ok(response) => self.handle_produce_response(response, context_map),
            Err(_e) => todo!(),
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
    pub fn new(client: NetworkClient) -> Self {
        let (tx, rx) = mpsc::channel(CHUNK_SIZE);

        let task = ProducerTask::new(client.clone());

        let task_tracker = TaskTracker::new();

        task_tracker.spawn(task.run(rx));

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

        self.tx.send(ProducerTaskMessage { record, tx }).await?;

        Ok(ProduceFuture { rx })
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
