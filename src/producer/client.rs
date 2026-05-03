use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use futures::FutureExt;
use kafka_protocol::messages::TopicName;
use tokio::sync::oneshot;

use crate::{
    common::{BrokerHost, TopicPartition},
    config::KafkaConfig,
    conn::{
        connect::{Connect, Tcp},
        selector::SelectorTaskHandle,
    },
    error::{ErrorCode, KafkaError},
    producer::{
        handle::{ProducerTaskFactory, ProducerTaskHandle},
        partitioner::{KeyHashPartitioner, Partitioner},
        prepared_record::DeliveryMetadata,
        record::{ProducerRecord, RecordMetadata},
        task::{ProducerSendMessage, ProducerSendRecord, ProducerTask},
    },
    util::TopicNameExt,
};

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

pub struct Producer<Conn: Connect + Send + 'static, P> {
    selector: SelectorTaskHandle<ProducerTask<Conn>, ProducerTaskHandle>,
    partitioner: P,
}

impl Producer<Tcp, KeyHashPartitioner> {
    pub async fn try_new(
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
    ) -> Result<Self, KafkaError> {
        tracing::debug!("{config:#?}");

        let selector = SelectorTaskHandle::try_new_tcp(
            bootstrap,
            config.clone(),
            ProducerTaskFactory { config },
        )
        .await?;

        Ok(Self {
            selector,
            partitioner: KeyHashPartitioner,
        })
    }
}

impl<Conn: Connect + Send + 'static, P: Partitioner> Producer<Conn, P> {
    pub async fn produce(&self, mut record: ProducerRecord) -> Result<ProduceFuture, KafkaError> {
        let topic_name = TopicName::from_string(record.topic.clone());
        self.selector.check_topic_metadata(&topic_name).await?;

        let cluster = self.selector.cluster.load();

        let Some(topic_data) = cluster.metadata.get_topic_metadata_by_name(&topic_name) else {
            return Err(ErrorCode::UnknownTopicOrPartition.into());
        };

        let topic_data = topic_data.metadata.as_ref().map_err(Clone::clone)?;

        if record.partition.is_none() {
            self.partitioner.partition(&mut record, topic_data);
        }

        let Some(partition) = record.partition else {
            return Err(ErrorCode::UnknownTopicOrPartition.into());
        };

        let partition_metadata = topic_data.get_partition_metadata(partition)?;

        let tp = TopicPartition::new(topic_name, partition);

        let Some(tx_partition) = cluster.partitions.get(&tp) else {
            return Err(ErrorCode::UnknownTopicOrPartition.into());
        };

        let (tx, rx) = oneshot::channel();

        let timestamp = record.timestamp.unwrap_or_else(|| {
            let start = SystemTime::now();
            start.duration_since(UNIX_EPOCH).map_or_else(
                |e| -(e.duration().as_millis() as i64),
                |ts| ts.as_millis() as i64,
            )
        });

        tx_partition
            .send(ProducerSendMessage {
                record: ProducerSendRecord {
                    value: record.value,
                    key: record.key,
                    headers: record.headers,
                    timestamp,
                    leader_epoch: partition_metadata.leader_epoch,
                },
                tx,
                delivery: DeliveryMetadata::default(),
            })
            .await?;

        Ok(ProduceFuture { rx })
    }

    pub async fn flush_and_shutdown(self) {
        self.selector.flush().await;
    }
}
