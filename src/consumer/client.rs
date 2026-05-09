use kafka_protocol::messages::TopicName;
use tokio::sync::mpsc;

use crate::{
    common::BrokerHost,
    config::KafkaConfig,
    conn::{KafkaChannelError, selector::SelectorTaskHandle},
    connect::{Connect, Tcp},
    consumer::{
        handle::{ConsumerTaskFactory, ConsumerTaskHandle},
        record::ConsumerRecordsResult,
        subscription::Subscription,
        task::ConsumerTask,
    },
    error::KafkaError,
    util::TopicNameExt,
};

pub struct Consumer<Conn: Connect + Send + 'static> {
    selector: SelectorTaskHandle<ConsumerTask<Conn>, ConsumerTaskHandle>,
    rx: mpsc::Receiver<ConsumerRecordsResult>,
}

impl Consumer<Tcp> {
    pub async fn try_new(
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
    ) -> Result<Self, KafkaError> {
        tracing::debug!("{config:#?}");

        let (tx, rx) = mpsc::channel(1);

        let selector = SelectorTaskHandle::try_new_tcp(
            bootstrap,
            config.clone(),
            ConsumerTaskFactory { config, tx },
        )
        .await?;

        Ok(Self { selector, rx })
    }
}

impl<Conn: Connect + Send + 'static> Consumer<Conn> {
    pub async fn subscribe(&self, subscription: Subscription) -> Result<(), KafkaError> {
        // request topics
        match &subscription {
            Subscription::TopicPattern(_) => unimplemented!(),
            Subscription::Topic(topic_name) => {
                self.selector
                    .check_topic_metadata(&TopicName::from_string(topic_name.clone()))
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
}
