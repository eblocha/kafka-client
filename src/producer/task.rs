use std::{
    io,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
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
use rustc_hash::FxHashMap;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;

use crate::{
    cancel::OrCancelled,
    common::{Node, TopicPartition},
    conn::{
        broker::{
            connection_task::{ConnectionTask, ConnectionTaskHandle},
            task::{BrokerTask, BrokerTaskContext, BrokerTaskHandle, PartitionQueueMap},
        },
        selector::connect::Connect,
        RecordBatchEncoder,
    },
    error::{ErrorCode, KafkaError},
    producer::prepared_record::PreparedRecord,
};

pub struct ProducerSendRecord {
    pub topic_partition: TopicPartition,
    pub timestamp: Option<i64>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: indexmap::IndexMap<StrBytes, Option<Bytes>>,
}

pub struct ProducerSendMessage {
    pub record: ProducerSendRecord,
    pub tx: oneshot::Sender<Result<(), KafkaError>>,
}

pub struct ProducerTask<Conn> {
    pub partitions: PartitionQueueMap<ProducerSendMessage>,
    pub connection_handle: ConnectionTaskHandle,
    pub connection_task: ConnectionTask<Conn>,
}

impl<Conn: Connect + Send + 'static> BrokerTask for ProducerTask<Conn> {
    type PartitionMessage = ProducerSendMessage;

    async fn run(mut self, ctx: BrokerTaskContext) -> Self {
        let connection_join_handle = tokio::spawn(self.connection_task.run(ctx.clone()));

        // TODO batch.size and linger.ms config
        let chunks = (&mut self.partitions).chunks_timeout(2000, Duration::from_millis(500));

        tokio::pin!(chunks);

        loop {
            // TODO stop if connection task stops

            let Some(Some(chunk)) = chunks.next().or_cancel(&ctx.cancellation_token).await else {
                break;
            };

            let (request, partitions) = create_request(chunk);

            let Some(response) = self
                .connection_handle
                .send(request)
                .or_cancel(&ctx.cancellation_token)
                .await
            else {
                break;
            };

            match response {
                Ok(response) => handle_produce_response(response, partitions),
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
            connection_handle: self.connection_handle,
            connection_task,
        }
    }

    async fn shutdown(self) -> Self {
        let connection_task = self.connection_task.shutdown().await;

        Self {
            partitions: self.partitions,
            connection_handle: self.connection_handle,
            connection_task,
        }
    }

    fn get_partitions_mut(&mut self) -> &mut PartitionQueueMap<Self::PartitionMessage> {
        &mut self.partitions
    }

    fn get_node(&self) -> &Node {
        self.connection_task.get_node()
    }

    fn set_node(&mut self, node: Node) {
        self.connection_task.set_node(node);
    }
}

fn create_request(
    chunk: Vec<(TopicPartition, ProducerSendMessage)>,
) -> (
    ProduceRequest,
    FxHashMap<TopicPartition, Vec<PreparedRecord>>,
) {
    let mut req = ProduceRequest::default();

    let fallback_timestamp = {
        let start = SystemTime::now();
        start.duration_since(UNIX_EPOCH).map_or_else(
            |e| -(e.duration().as_millis() as i64),
            |ts| ts.as_millis() as i64,
        )
    };

    let mut partitions = FxHashMap::<TopicPartition, Vec<PreparedRecord>>::default();

    for (tp, msg) in chunk {
        let records = partitions.entry(tp.clone()).or_default();

        let timestamp = msg.record.timestamp.unwrap_or(fallback_timestamp);

        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: -1, // TODO
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

    let mut topic_data = FxHashMap::<TopicName, TopicProduceData>::default();

    for (tp, records) in &mut partitions {
        let mut buf = BytesMut::new();

        if let Err(e) = RecordBatchEncoder::encode(
            &mut buf,
            records.iter().map(|ctx| &ctx.record),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None, // TODO config
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

    // TODO config
    req.acks = 1;
    req.timeout_ms = 1000;
    req.transactional_id = None;

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
