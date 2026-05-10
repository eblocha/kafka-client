//! The consumer client

use kafka_protocol::messages::TopicName;
use tokio::sync::mpsc;

use crate::{
    common::BrokerHost,
    config::KafkaConfig,
    conn::KafkaChannelError,
    connect::Connect,
    consumer::{
        handle::{ConsumerTaskFactory, ConsumerTaskHandle},
        record::ConsumerRecordsResult,
        subscription::Subscription,
        task::ConsumerTask,
    },
    error::KafkaError,
    selector::SelectorTaskHandle,
    util::TopicNameExt,
};

/// Consumes messages from topics
pub struct Consumer<Conn: Connect + Send + 'static> {
    selector: SelectorTaskHandle<ConsumerTask<Conn>, ConsumerTaskHandle>,
    rx: mpsc::Receiver<ConsumerRecordsResult>,
}

impl<Conn: Connect + Send + 'static> Consumer<Conn> {
    /// Bootstrap a new consumer client.
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

        let (tx, rx) = mpsc::channel(1);

        let selector = SelectorTaskHandle::bootstrap(
            connect,
            bootstrap,
            config.clone(),
            ConsumerTaskFactory { config, tx },
        )
        .await?;

        Ok(Self { selector, rx })
    }

    /// Subscribe to topics.
    pub async fn subscribe(&self, subscription: Subscription) -> Result<(), KafkaError> {
        // request topics
        match subscription {
            Subscription::TopicPattern(_) => unimplemented!(),
            Subscription::Topic(topic_name) => {
                self.selector
                    .check_topic_metadata(&TopicName::from_string(topic_name))
                    .await?;
            }
            Subscription::TopicPartition(topic_partition) => {
                self.selector
                    .check_topic_metadata(topic_partition.name())
                    .await?;
            }
        }

        Ok(())
    }

    /// Receive the next message from subscribed topics
    ///
    /// # Errors
    ///
    /// This returns an [`Err`] if the client encountered an error it could not handle automatically.
    pub async fn recv(&mut self) -> ConsumerRecordsResult {
        self.rx
            .recv()
            .await
            .ok_or(KafkaError::Channel(KafkaChannelError::Closed))?
    }

    /// Gracefully shut down the consumer, closing all connections.
    pub async fn shutdown(self) {
        self.selector.shutdown().await;
    }
}
