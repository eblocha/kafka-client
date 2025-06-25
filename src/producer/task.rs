use std::{io, pin::Pin, time::Duration};

use bytes::{Bytes, BytesMut};
use futures::StreamExt as FuturesStreamExt;
use kafka_protocol::{
    messages::{
        produce_request::{PartitionProduceData, TopicProduceData},
        ProduceRequest, ProduceResponse, TopicName, TransactionalId,
    },
    protocol::StrBytes,
    records::{
        Compression, Record, RecordEncodeOptions, TimestampType, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    },
};
use rustc_hash::FxHashMap;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    cancel::OrCancelled,
    common::{Node, TopicPartition},
    config::KafkaConfig,
    conn::{
        broker::task::{
            BrokerTask, BrokerTaskContext, BrokerTaskHandle, PartitionQueue, PartitionQueueMap,
        },
        connect::Connect,
        RecordBatchEncoder,
    },
    error::{ErrorCode, KafkaError},
    network::{handle::NetworkTaskHandle, task::NetworkTask},
    producer::{prepared_record::PreparedRecord, record::RecordMetadata},
    util::StreamExt,
};

pub(super) struct ProducerSendRecord {
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: indexmap::IndexMap<StrBytes, Option<Bytes>>,
    pub leader_epoch: i32,
}

pub(super) struct ProducerSendMessage {
    pub record: ProducerSendRecord,
    pub tx: oneshot::Sender<Result<RecordMetadata, KafkaError>>,
}

impl From<PreparedRecord> for ProducerSendMessage {
    fn from(value: PreparedRecord) -> Self {
        ProducerSendMessage {
            record: ProducerSendRecord {
                timestamp: value.record.timestamp,
                key: value.record.key,
                value: value.record.value,
                headers: value.record.headers,
                leader_epoch: value.record.partition_leader_epoch,
            },
            tx: value.tx,
        }
    }
}

pub(super) struct ProducerTask<Conn> {
    pub(super) partitions: PartitionQueueMap<ProducerSendMessage>,
    pub(super) inner_handle: NetworkTaskHandle,
    pub(super) inner_task: NetworkTask<Conn>,
    pub(super) config: KafkaConfig,
}

impl<Conn> ProducerTask<Conn> {
    fn split(
        self,
    ) -> (
        NetworkTask<Conn>,
        PartitionQueueMap<ProducerSendMessage>,
        PartialProducerTask,
    ) {
        (
            self.inner_task,
            self.partitions,
            PartialProducerTask {
                inner_handle: self.inner_handle,
                config: self.config,
            },
        )
    }

    async fn run_empty(mut self, ctx: BrokerTaskContext) -> Option<Self>
    where
        Conn: Connect + Send + 'static,
    {
        let node = self.inner_task.get_node();

        tracing::debug!(
            broker_id = node.id,
            host = ?node.host,
            "running as network task as this broker is not assigned any partitions"
        );

        Some(Self {
            partitions: self.partitions,
            inner_handle: self.inner_handle,
            inner_task: self.inner_task.run(ctx).await?,
            config: self.config,
        })
    }
}

struct PartialProducerTask {
    inner_handle: NetworkTaskHandle,
    config: KafkaConfig,
}

impl PartialProducerTask {
    async fn stop<Conn>(
        self,
        partitions: PartitionQueueMap<ProducerSendMessage>,
        join_handle: JoinHandle<Option<NetworkTask<Conn>>>,
        ctx: BrokerTaskContext,
        node: &Node,
        flushing: bool,
    ) -> Option<ProducerTask<Conn>> {
        if flushing {
            ctx.flush.cancel();
        } else {
            ctx.cancellation_token.cancel();
        }

        let inner_task_result = join_handle.await;

        let inner_task = match inner_task_result {
            Ok(task) => task?,
            Err(e) => {
                if e.is_panic() {
                    tracing::error!(
                        broker_id = node.id,
                        host = ?node.host,
                        "network task panicked {e}"
                    );
                }
                return None;
            }
        };

        Some(ProducerTask {
            partitions,
            inner_handle: self.inner_handle,
            inner_task,
            config: self.config,
        })
    }
}

impl<Conn: Connect + Send + 'static> BrokerTask for ProducerTask<Conn> {
    type PartitionMessage = ProducerSendMessage;

    async fn run(mut self, ctx: BrokerTaskContext) -> Option<Self> {
        if self.partitions.is_empty() {
            return self.run_empty(ctx).await;
        }

        let (inner_task, mut partitions, mut this) = self.split();
        let node = inner_task.get_node().clone();

        tracing::debug!(
            broker_id = node.id,
            host = ?node.host,
            "started producer task"
        );

        let network_ctx = BrokerTaskContext {
            cancellation_token: ctx.cancellation_token.clone(),
            // We don't want the flush to propagate to the network task since we still need it to send messages while flushing
            flush: CancellationToken::new(),
        };
        let flush_network = CancellationToken::new();
        let network_task_tracker = TaskTracker::new();
        let connection_join_handle =
            network_task_tracker.spawn(inner_task.run(network_ctx.clone()));
        network_task_tracker.close();

        let mut chunks = (&mut partitions).chunks_timeout(
            this.config.producer.batch_count,
            this.config.producer.linger,
        );

        tokio::pin!(chunks);

        let transactional_id = this
            .config
            .producer
            .transactional_id
            .clone()
            .map(StrBytes::from_string)
            .map(TransactionalId);

        let mut chunk = Vec::with_capacity(this.config.producer.batch_count);
        let mut flushing = false;

        loop {
            let tracker_wait = network_task_tracker.wait();

            tokio::select! {
                biased;
                () = ctx.cancellation_token.cancelled() => {
                    flushing = false;
                    break;
                },
                () = ctx.flush.cancelled(), if !ctx.flush.is_cancelled() => {
                    close_all(*chunks.as_mut().get_pin_mut().get_mut());
                    flushing = true;
                    continue;
                }
                () = tracker_wait => break,
                res = chunks.next() => {
                    match res {
                        Some(chunk) => chunk,
                        None => break
                    }
                },
            };

            chunks.as_mut().take_into(&mut chunk);

            debug_assert!(!chunk.is_empty());

            let compression: Compression = this.config.producer.compression_codec.into();
            let transactional_id = transactional_id.clone();
            let acks = this.config.producer.required_acks;
            let timeout = this.config.producer.request_timeout;
            let drain = chunk.drain(..);

            // There's a perf tradeoff here between the block_in_place overhead and the vec allocation overhead.
            // Not sure which is better.
            let (request, partition_map) = tokio::task::block_in_place(|| {
                create_request(drain, compression, transactional_id, acks, timeout)
            });

            tracing::trace!(
                broker_id = node.id,
                host = ?node.host,
                "sending produce request"
            );

            let Some(response) = send_isomorphic(&this.inner_handle, request)
                .or_cancel(&ctx.cancellation_token)
                .await
            else {
                // Refill the chunk with the partition map data so it gets re-sent.
                // This can result in duplicate messages sent, but expedites shutdown.
                // The idempotent producer prevents duplicates broker-side.
                for (tp, records) in partition_map {
                    queue_retry_partition(
                        tp,
                        records,
                        &mut partitions,
                        this.config.producer.batch_count,
                    );
                }
                break;
            };

            tracing::trace!(
                broker_id = node.id,
                host = ?node.host,
                "handling produce response"
            );

            match response {
                Ok(None) => {
                    for (_, records) in partition_map {
                        for record in records {
                            let _ = record.tx.send(Ok(RecordMetadata { base_offset: -1 }));
                        }
                    }
                }
                Ok(Some(response)) => handle_produce_response(response, partition_map),
                Err(e) => {
                    tracing::error!(
                        broker_id = node.id,
                        host = ?node.host,
                        "failed to send produce request: {e}"
                    );

                    for (_, records) in partition_map {
                        for record in records {
                            let _ = record.tx.send(Err(e.representative_clone()));
                        }
                    }
                }
            }
        }

        // Fill partition retry buffers with the chunk on shutdown
        queue_retry_chunk(chunk, &mut partitions, this.config.producer.batch_count);

        this.stop(
            partitions,
            connection_join_handle,
            network_ctx,
            &node,
            flushing,
        )
        .await
    }

    async fn shutdown(self) -> Self {
        Self {
            partitions: self.partitions,
            inner_handle: self.inner_handle,
            inner_task: self.inner_task.shutdown().await,
            config: self.config,
        }
    }

    fn get_partitions_mut(&mut self) -> &mut PartitionQueueMap<Self::PartitionMessage> {
        &mut self.partitions
    }

    fn get_node(&self) -> &Node {
        self.inner_task.get_node()
    }

    fn set_node(&mut self, node: Node) {
        self.inner_task.set_node(node);
    }
}

async fn send_isomorphic<Handle: BrokerTaskHandle>(
    handle: &Handle,
    request: ProduceRequest,
) -> Result<Option<ProduceResponse>, KafkaError> {
    if request.acks == 0 {
        handle.send_and_forget(request).await?;
        Ok(None)
    } else {
        Ok(Some(handle.send(request).await?))
    }
}

fn queue_retry_chunk(
    chunk: Vec<(TopicPartition, ProducerSendMessage)>,
    partitions: &mut PartitionQueueMap<ProducerSendMessage>,
    batch_count: usize,
) {
    for (tp, record) in chunk {
        // StreamMap doesn't implement any get_ methods, so remove and re-insert it
        if let Some(mut records) = partitions.remove(&tp) {
            records.retry(record);
            partitions.insert(tp, records);
            continue;
        };
        // The partition was removed because all the senders dropped.
        // This means the producer is flushing. In this case we collect the undelivered messages to hand them back to
        // the application.
        let (_, rx) = mpsc::channel(batch_count);
        let mut queue = PartitionQueue::new(rx);
        queue.retry(record);
        partitions.insert(tp, queue);
    }
}

fn queue_retry_partition(
    tp: TopicPartition,
    records: Vec<PreparedRecord>,
    partitions: &mut PartitionQueueMap<ProducerSendMessage>,
    batch_count: usize,
) {
    // StreamMap doesn't implement any get_ methods, so remove and re-insert it
    if let Some(mut partition) = partitions.remove(&tp) {
        for record in records {
            partition.retry(record.into());
        }
        partitions.insert(tp, partition);
        return;
    };
    // The partition was removed because all the senders dropped.
    // This means the producer is flushing. In this case we collect the undelivered messages to hand them back to
    // the application.
    let (_, rx) = mpsc::channel(batch_count);
    let mut partition = PartitionQueue::new(rx);
    for record in records {
        partition.retry(record.into());
    }
    partitions.insert(tp, partition);
}

fn close_all(mut partitions: &mut PartitionQueueMap<ProducerSendMessage>) {
    for (_, partition) in partitions.iter_mut() {
        partition.close();
    }
}

fn create_request(
    chunk: impl IntoIterator<Item = (TopicPartition, ProducerSendMessage)>,
    compression: Compression,
    transactional_id: Option<TransactionalId>,
    acks: i16,
    timeout: Duration,
) -> (
    ProduceRequest,
    FxHashMap<TopicPartition, Vec<PreparedRecord>>,
) {
    let mut req = ProduceRequest::default();

    let mut partitions = FxHashMap::<TopicPartition, Vec<PreparedRecord>>::default();

    for (tp, msg) in chunk {
        let records = partitions.entry(tp).or_default();

        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: msg.record.leader_epoch,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: records.len() as i64,
            sequence: records.len() as i32,
            timestamp: msg.record.timestamp,
            key: msg.record.key,
            value: msg.record.value,
            headers: msg.record.headers,
        };

        records.push(PreparedRecord { record, tx: msg.tx });
    }

    let mut topic_data = FxHashMap::<TopicName, TopicProduceData>::default();

    for (tp, records) in &mut partitions {
        let mut buf = BytesMut::new();

        if let Err(e) = RecordBatchEncoder::encode(
            &mut buf,
            records.iter().map(|ctx| &ctx.record),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        ) {
            tracing::error!("failed to encode record batch for topic {tp}: {e}");

            for ctx in records.drain(..) {
                let _ = ctx.tx.send(Err(KafkaError::Channel(
                    io::Error::new(io::ErrorKind::Other, "record batch failed to encode").into(),
                )));
            }
            continue;
        }

        let partition_data = PartitionProduceData::default()
            .with_index(tp.partition())
            .with_records(Some(buf.into()));

        if let Some(produce_data) = topic_data.get_mut(tp.name()) {
            produce_data.partition_data.push(partition_data);
        } else {
            topic_data.insert(
                tp.name().clone(),
                TopicProduceData::default().with_partition_data(vec![partition_data]),
            );
        }
    }
    for (name, mut data) in topic_data {
        data.name = name;
        req.topic_data.push(data);
    }

    req.acks = acks;
    req.timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
    req.transactional_id = transactional_id;

    (req, partitions)
}

fn handle_produce_response(
    response: ProduceResponse,
    mut context_map: FxHashMap<TopicPartition, Vec<PreparedRecord>>,
) {
    for response in response.responses {
        for part_response in response.partition_responses {
            let tp = TopicPartition::new(response.name.clone(), part_response.index);

            let Some(contexts) = context_map.remove(&tp) else {
                tracing::warn!(
                    "got a produce response for a partition we did not send data to: {tp}"
                );
                continue;
            };

            if part_response.error_code != ErrorCode::None as i16 {
                for ctx in contexts {
                    let _ = ctx
                        .tx
                        .send(Err(KafkaError::ErrorCode(part_response.error_code.into())));
                }

                continue;
            }

            for ctx in contexts {
                let _ = ctx.tx.send(Ok(RecordMetadata {
                    base_offset: part_response.base_offset,
                }));
            }
        }
    }
}
