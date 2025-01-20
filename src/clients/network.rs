use kafka_protocol::{
    indexmap::IndexMap,
    messages::{
        metadata_request::MetadataRequestTopic, metadata_response::MetadataResponseTopic, TopicName,
    },
};

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
        let cluster = self.read_cluster_snapshot();
        let Some((_, handle)) = cluster.broker_channels.0.get(&broker_id) else {
            tracing::error!("no broker handle for id {broker_id}");
            return Err(KafkaChannelError::Closed);
        };

        handle.send(req).await
    }

    pub fn invalidate_topic_metadata(&mut self, topic_names: &[&TopicName]) {
        self.selector.tx_cluster.send_modify(|cluster| {
            for topic_name in topic_names {
                cluster.metadata.topics.swap_remove(*topic_name);
            }
        });
    }

    pub async fn get_topic_metadata(
        &self,
        topic_names: &[&TopicName],
    ) -> Result<IndexMap<TopicName, MetadataResponseTopic>, KafkaChannelError> {
        let cluster_state = self.selector.cluster.borrow();

        let missing_topic_names = topic_names
            .iter()
            .filter(|topic_name| !cluster_state.metadata.topics.contains_key(**topic_name))
            .map(|name| {
                let mut req_topic = MetadataRequestTopic::default();
                req_topic.name = Some((*name).clone());
                req_topic
            })
            .collect::<Vec<_>>();

        drop(cluster_state);

        if !missing_topic_names.is_empty() {
            self.selector
                .refresh_metadata_for_topics(Some(missing_topic_names))
                .await?;
        }

        return Ok(self.read_cluster_snapshot().metadata.topics);
    }

    pub async fn shutdown(&self) {
        self.selector.shutdown().await;
    }

    pub fn read_cluster_snapshot(&self) -> Cluster {
        self.selector.cluster.borrow().clone()
    }
}
