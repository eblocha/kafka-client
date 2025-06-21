use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::FutureExt;
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
        record::ProducerRecord,
        task::{ProducerSendMessage, ProducerSendRecord, ProducerTask},
    },
};

/// A future that resolves to [`RecordMetadata`] once the server has acknowledged the record.
pub struct ProduceFuture {
    rx: oneshot::Receiver<Result<(), KafkaError>>,
}

impl Unpin for ProduceFuture {}

impl Future for ProduceFuture {
    type Output = Result<(), KafkaError>;

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

impl<Conn: Connect + Send + 'static, P: Clone> Clone for Producer<Conn, P> {
    fn clone(&self) -> Self {
        Self {
            selector: self.selector.clone(),
            partitioner: self.partitioner.clone(),
        }
    }
}

impl Producer<Tcp, KeyHashPartitioner> {
    pub async fn try_new(
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
    ) -> Result<Self, KafkaError> {
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
        let cluster = self.selector.cluster.load();

        // TODO: if err, refresh the metadata for the topic
        let topic_data = cluster.metadata.get_topic_metadata_by_name(&record.topic)?;

        if record.partition.is_none() {
            self.partitioner.partition(&mut record, topic_data);
        }

        let Some(partition) = record.partition else {
            return Err(ErrorCode::UnknownTopicOrPartition.into());
        };

        topic_data.get_partition_metadata(partition)?;

        let tp = TopicPartition::new(record.topic.clone(), partition);

        let Some(tx_partition) = cluster.partitions.get(&tp) else {
            return Err(ErrorCode::UnknownTopicOrPartition.into());
        };

        let (tx, rx) = oneshot::channel();

        tx_partition
            .send(ProducerSendMessage {
                record: ProducerSendRecord {
                    value: record.value,
                    key: record.key,
                    headers: record.headers,
                    timestamp: record.timestamp,
                },
                tx,
            })
            .await?;

        Ok(ProduceFuture { rx })
    }
}
