use kafka_protocol::messages::{metadata_request::MetadataRequestTopic, TopicName};
use tokio::sync::watch::Ref;

use crate::{
    common::BrokerHost,
    conn::{
        config::ConnectionManagerConfig,
        selector::{Cluster, SelectorTaskHandle},
        KafkaChannelError, Sendable,
    },
    error::KafkaError,
    proto::ver::{FromVersionRange, GetApiKey},
};

/// Maintains connections to the entire cluster, and forwards requests to the appropriate broker.
#[derive(Clone)]
pub struct NetworkClient {
    selector: SelectorTaskHandle,
}

impl NetworkClient {
    /// Create a new client with bootstrap servers
    pub async fn try_new(
        brokers: &[BrokerHost],
        config: ConnectionManagerConfig,
    ) -> Result<Self, KafkaError> {
        let selector = SelectorTaskHandle::try_new_tcp(brokers, config).await?;

        Ok(Self { selector })
    }

    /// Send a message to any available broker.
    /// This uses load-balancing to distribute the request load.
    pub async fn send<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<R::Response, KafkaError> {
        let handle = {
            // Closure is to prevent holding the cluster across an await point, which would make this non-Send.
            let Some(entry) = self.borrow_cluster().broker_channels.get_best_connection() else {
                return Err(KafkaChannelError::Closed.into());
            };
            entry.handle
        };

        handle.send(req).await
    }

    /// Send a request to a specific broker by id.
    pub async fn send_to<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
        broker_id: i32,
    ) -> Result<R::Response, KafkaError> {
        let handle = {
            // Closure is to prevent holding the cluster across an await point, which would make this non-Send.
            let cluster = self.borrow_cluster();
            let Some(entry) = cluster.broker_channels.0.get(&broker_id) else {
                tracing::error!("no broker handle for id {broker_id}");
                return Err(KafkaChannelError::Closed.into());
            };
            entry.handle.clone()
        };

        handle.send(req).await
    }

    pub fn invalidate_topic_metadata<'a>(&self, topic_names: impl Iterator<Item = &'a TopicName>) {
        self.selector.tx_cluster.send_modify(|cluster| {
            for topic_name in topic_names {
                cluster.metadata.topics.swap_remove(topic_name);
            }
        });
    }

    pub async fn load_topic_metadata<'a>(
        &self,
        topic_names: impl Iterator<Item = &'a TopicName>,
    ) -> Result<(), KafkaError> {
        let missing_topic_names = {
            // Closure is to prevent holding the cluster across an await point, which would make this non-Send.
            let cluster_state = self.borrow_cluster();
            topic_names
                .filter(|topic_name| !cluster_state.metadata.topics.contains_key(*topic_name))
                .map(|name| {
                    let mut req_topic = MetadataRequestTopic::default();
                    req_topic.name = Some(name.clone());
                    req_topic
                })
                .collect::<Vec<_>>()
        };

        if !missing_topic_names.is_empty() {
            self.selector
                .refresh_metadata_for_topics(Some(missing_topic_names))
                .await?;
        }

        Ok(())
    }

    pub fn borrow_cluster(&self) -> Ref<'_, Cluster> {
        self.selector.cluster.borrow()
    }

    pub async fn shutdown(&self) {
        self.selector.shutdown().await;
    }

    pub async fn await_shutdown(&self) {
        self.selector.await_shutdown().await
    }
}
