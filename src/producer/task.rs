use std::io;

use bytes::{Bytes, BytesMut};
use kafka_protocol::{
    messages::{
        produce_request::{PartitionProduceData, TopicProduceData},
        ProduceRequest, ProduceResponse, TopicName, TransactionalId,
    },
    protocol::StrBytes,
    records::{Record, RecordEncodeOptions, TimestampType, NO_PRODUCER_EPOCH, NO_PRODUCER_ID},
};
use rustc_hash::FxHashMap;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;

use crate::{
    cancel::OrCancelled,
    common::{Node, TopicPartition},
    config::KafkaConfig,
    conn::{
        broker::task::{BrokerTask, BrokerTaskContext, BrokerTaskHandle, PartitionQueueMap},
        connect::Connect,
        RecordBatchEncoder,
    },
    error::{ErrorCode, KafkaError},
    network::{handle::NetworkTaskHandle, task::NetworkTask},
    producer::prepared_record::PreparedRecord,
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
    pub tx: oneshot::Sender<Result<(), KafkaError>>,
}

pub(super) struct ProducerTask<Conn> {
    pub(super) partitions: PartitionQueueMap<ProducerSendMessage>,
    pub(super) inner_handle: NetworkTaskHandle,
    pub(super) inner_task: NetworkTask<Conn>,
    pub(super) config: KafkaConfig,
}

impl<Conn: Connect + Send + 'static> BrokerTask for ProducerTask<Conn> {
    type PartitionMessage = ProducerSendMessage;

    async fn run(mut self, ctx: BrokerTaskContext) -> Self {
        let node = self.inner_task.get_node().clone();

        if self.partitions.is_empty() {
            tracing::debug!(
                broker_id = node.id,
                host = ?node.host,
                "falling back to network task since this broker is not assigned any partitions"
            );
            let inner_task = self.inner_task.run(ctx).await;
            return Self {
                partitions: self.partitions,
                inner_handle: self.inner_handle,
                inner_task,
                config: self.config,
            };
        }

        tracing::debug!(
            broker_id = node.id,
            host = ?node.host,
            "started producer task"
        );

        let connection_join_handle = tokio::spawn(self.inner_task.run(ctx.clone()));

        let chunks = (&mut self.partitions).chunks_timeout(
            self.config.producer.batch_count,
            self.config.producer.linger,
        );

        tokio::pin!(chunks);

        let transactional_id = self
            .config
            .producer
            .transactional_id
            .clone()
            .map(StrBytes::from_string)
            .map(TransactionalId);

        loop {
            // TODO stop if connection task stops

            let Some(Some(chunk)) = chunks.next().or_cancel(&ctx.cancellation_token).await else {
                break;
            };

            let (request, partitions) =
                create_request(chunk, &self.config, transactional_id.clone());

            tracing::trace!(
                broker_id = node.id,
                host = ?node.host,
                "sending produce request"
            );

            let Some(response) = self
                .inner_handle
                .send(request)
                .or_cancel(&ctx.cancellation_token)
                .await
            else {
                break;
            };

            tracing::trace!(
                broker_id = node.id,
                host = ?node.host,
                "handling produce response"
            );

            match response {
                Ok(_) => {
                    for (_, records) in partitions {
                        for record in records {
                            let _ = record.tx.send(Ok(()));
                        }
                    }
                }
                // Ok(response) => handle_produce_response(response, partitions),
                Err(e) => {
                    tracing::error!("failed to send produce request: {e}");

                    for (_, records) in partitions {
                        for record in records {
                            let _ = record.tx.send(Err(e.representative_clone()));
                        }
                    }
                }
            }
        }

        ctx.cancellation_token.cancel();

        // TODO any way to handle this more gracefully?
        // This is err if the task is aborted forcefully, or it panics
        let connection_task = connection_join_handle.await.unwrap();

        Self {
            partitions: self.partitions,
            inner_handle: self.inner_handle,
            inner_task: connection_task,
            config: self.config,
        }
    }

    async fn shutdown(self) -> Self {
        let connection_task = self.inner_task.shutdown().await;

        Self {
            partitions: self.partitions,
            inner_handle: self.inner_handle,
            inner_task: connection_task,
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

fn create_request(
    chunk: Vec<(TopicPartition, ProducerSendMessage)>,
    config: &KafkaConfig,
    transactional_id: Option<TransactionalId>,
) -> (
    ProduceRequest,
    FxHashMap<TopicPartition, Vec<PreparedRecord>>,
) {
    let mut req = ProduceRequest::default();

    let mut partitions = FxHashMap::<TopicPartition, Vec<PreparedRecord>>::default();

    for (tp, msg) in chunk {
        let records = partitions.entry(tp.clone()).or_default();

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
                compression: config.producer.compression_codec.into(),
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

    req.acks = config.producer.required_acks;
    req.timeout_ms = i32::try_from(config.producer.request_timeout.as_millis()).unwrap_or(i32::MAX);
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
                let _ = ctx.tx.send(Ok(()));
            }
        }
    }
}
