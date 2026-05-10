//! The producer client

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
    conn::connect::Connect,
    error::{ErrorCode, KafkaError},
    producer::{
        handle::{ProducerTaskFactory, ProducerTaskHandle},
        partitioner::{KeyHashPartitioner, Partitioner},
        prepared_record::DeliveryMetadata,
        record::{ProducerRecord, RecordMetadata},
        task::{ProducerSendMessage, ProducerSendRecord, ProducerTask},
    },
    selector::SelectorTaskHandle,
    util::TopicNameExt,
};

/// A future that resolves to [`RecordMetadata`] once the server has acknowledged the record, or when the message is
/// flushed when acks=0.
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

/// Produces messages to topics.
pub struct Producer<Conn: Connect + Send + 'static, P> {
    selector: SelectorTaskHandle<ProducerTask<Conn>, ProducerTaskHandle>,
    partitioner: P,
}

impl<Conn: Connect + Send + 'static> Producer<Conn, KeyHashPartitioner> {
    /// Bootstrap a new producer client using the [`KeyHashPartitioner`].
    ///
    /// Provide the connection mechanism with `connect`.
    /// For example, [`crate::connect::Tcp`] for a non-TLS TCP connection.
    pub async fn bootstrap(
        connect: Conn,
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
    ) -> Result<Self, KafkaError>
    where
        Conn: Clone,
    {
        tracing::debug!("{config:#?}");

        let selector = SelectorTaskHandle::bootstrap(
            connect,
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

impl<Conn: Connect + Send + 'static, P> Producer<Conn, P> {
    /// Bootstrap a new producer client with a custom partitioner.
    ///
    /// Provide the connection mechanism with `connect`.
    /// For example, [`crate::connect::Tcp`] for a non-TLS TCP connection.
    ///
    /// Provide the record partitioner with `partitioner`.
    pub async fn bootstrap_with_partitioner(
        connect: Conn,
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
        partitioner: P,
    ) -> Result<Self, KafkaError>
    where
        Conn: Clone,
    {
        tracing::debug!("{config:#?}");

        let selector = SelectorTaskHandle::bootstrap(
            connect,
            bootstrap,
            config.clone(),
            ProducerTaskFactory { config },
        )
        .await?;

        Ok(Producer {
            selector,
            partitioner,
        })
    }

    /// Change the partitioner for a producer.
    ///
    /// This consumes the existing producer, returning a new one in its place without shutting down.
    pub fn with_partitioner<P2>(self, partitioner: P2) -> Producer<Conn, P2> {
        Producer {
            selector: self.selector,
            partitioner,
        }
    }

    /// Produce a message.
    ///
    /// This returns a nested future. The outer future completes when the message is queued.
    /// The inner future completes when the server ackowledges the message (or when the message is flushed when acks=0).
    pub async fn produce(&self, mut record: ProducerRecord) -> Result<ProduceFuture, KafkaError>
    where
        P: Partitioner,
    {
        let topic_name = TopicName::from_string(record.topic.clone());
        self.selector.check_topic_metadata(&topic_name).await?;

        let cluster = self.selector.cluster.load();

        let Some(topic_data) = cluster.metadata.get_topic_metadata(&topic_name) else {
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

    /// Flush any remaining messages, then stop the client. This closes all connections and consumes the producer.
    pub async fn flush_and_shutdown(self) {
        self.selector.flush().await;
    }
}
