use kafka_protocol::messages::{metadata_request::MetadataRequestTopic, TopicName};
use tokio::sync::watch::Ref;

use crate::{
    conn::{
        config::ConnectionManagerConfig,
        host::{try_parse_hosts, BrokerHost},
        selector::{Cluster, SelectorTaskHandle},
        KafkaChannelError, Sendable,
    },
    proto::ver::{FromVersionRange, GetApiKey},
};

/// Maintains connections to the entire cluster, and forwards requests to the appropriate broker.
#[derive(Clone)]
pub struct NetworkClient {
    selector: SelectorTaskHandle,
}

impl NetworkClient {
    /// Create a new client with bootstrap servers
    ///
    /// This can fail if the broker hostnames are not valid.
    pub async fn try_new(
        brokers: &[String],
        config: ConnectionManagerConfig,
    ) -> Result<Self, url::ParseError> {
        Ok(Self::new_with_hosts(&try_parse_hosts(brokers)?, config).await)
    }

    /// Create a new client with bootstrap servers
    pub async fn new_with_hosts(brokers: &[BrokerHost], config: ConnectionManagerConfig) -> Self {
        let selector = SelectorTaskHandle::new_tcp(brokers, config).await;

        Self { selector }
    }

    /// Send a message to any available broker.
    /// This uses load-balancing to distribute the request load.
    pub async fn send<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<R::Response, KafkaChannelError> {
        let Some((_, handle)) = self
            .selector
            .cluster
            .borrow()
            .broker_channels
            .get_best_connection()
        else {
            return Err(KafkaChannelError::Closed);
        };

        handle.send(req).await
    }

    /// Send a request to a specific broker by id.
    pub async fn send_to<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
        broker_id: i32,
    ) -> Result<R::Response, KafkaChannelError> {
        let handle = {
            let cluster = self.borrow_cluster();
            let Some((_, handle)) = cluster.broker_channels.0.get(&broker_id) else {
                tracing::error!("no broker handle for id {broker_id}");
                return Err(KafkaChannelError::Closed);
            };
            handle.clone()
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
    ) -> Result<(), KafkaChannelError> {
        let cluster_state = self.selector.cluster.borrow();

        let missing_topic_names = topic_names
            .filter(|topic_name| !cluster_state.metadata.topics.contains_key(*topic_name))
            .map(|name| {
                let mut req_topic = MetadataRequestTopic::default();
                req_topic.name = Some(name.clone());
                req_topic
            })
            .collect::<Vec<_>>();

        drop(cluster_state);

        if !missing_topic_names.is_empty() {
            self.selector
                .refresh_metadata_for_topics(Some(missing_topic_names))
                .await?;
        }

        return Ok(());
    }

    pub async fn shutdown(&self) {
        self.selector.shutdown().await;
    }

    pub fn borrow_cluster(&self) -> Ref<'_, Cluster> {
        self.selector.cluster.borrow()
    }
}
