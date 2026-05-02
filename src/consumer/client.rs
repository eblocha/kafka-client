use kafka_protocol::messages::TopicName;
use tokio::sync::mpsc;

use crate::{
    common::BrokerHost,
    config::KafkaConfig,
    conn::{KafkaChannelError, selector::SelectorTaskHandle},
    connect::{Connect, Tcp},
    consumer::{record::ConsumerRecords, subscription::Subscription, unsubscribe::Unsubscribe},
    error::KafkaError,
    network::{
        handle::{NetworkTaskFactory, NetworkTaskHandle},
        task::NetworkTask,
    },
    util::TopicNameExt,
};

pub struct Consumer<Conn: Connect + Send + 'static> {
    selector: SelectorTaskHandle<NetworkTask<Conn>, NetworkTaskHandle>,
    rx: mpsc::Receiver<Result<ConsumerRecords, KafkaError>>,
}

impl Consumer<Tcp> {
    pub async fn try_new(
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
    ) -> Result<Self, KafkaError> {
        tracing::debug!("{config:#?}");

        let (_tx, rx) = mpsc::channel(1);

        let selector =
            SelectorTaskHandle::try_new_tcp(bootstrap, config.clone(), NetworkTaskFactory).await?;

        Ok(Self { selector, rx })
    }
}

impl<Conn: Connect + Send + 'static> Consumer<Conn> {
    pub async fn add_subscription(
        &self,
        subscription: Subscription,
    ) -> Result<Unsubscribe, KafkaError> {
        // request topics
        match subscription {
            Subscription::TopicPattern(_) => todo!(),
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

        todo!()
    }

    /// Receive the next message from subscribed topics
    ///
    /// # Errors
    ///
    /// This returns an [`Err`] if the client encountered an error it could not handle automatically.
    pub async fn recv(&mut self) -> Result<ConsumerRecords, KafkaError> {
        self.rx
            .recv()
            .await
            .ok_or(KafkaError::Channel(KafkaChannelError::Closed))?
    }
}
