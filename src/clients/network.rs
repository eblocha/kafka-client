use kafka_protocol::messages::{
    metadata_request::MetadataRequestTopic, metadata_response::MetadataResponseTopic, TopicName,
};

use crate::{
    conn::{
        config::ConnectionManagerConfig,
        host::{try_parse_hosts, BrokerHost},
        selector::{Cluster, SelectorTaskHandle},
        KafkaChannelError, Sendable,
    },
    proto::{
        error_codes::ErrorCode,
        ver::{FromVersionRange, GetApiKey},
    },
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

    pub async fn get_topic_metadata(
        &self,
        name: &TopicName,
    ) -> Result<Result<MetadataResponseTopic, ErrorCode>, KafkaChannelError> {
        let cluster_state = self.selector.cluster.borrow();

        let topic_data = match cluster_state.metadata.topics.get(name) {
            Some(td) if td.error_code == ErrorCode::None as i16 => td.clone(),
            _ => {
                drop(cluster_state); // release lock on cluster state
                let mut req_topic = MetadataRequestTopic::default();
                req_topic.name = Some(name.clone());

                tracing::debug!("refreshing metadata for topic {:?}", name.0);

                self.selector
                    .refresh_metadata_for_topics(Some(vec![req_topic]))
                    .await?;

                let cluster_state = self.selector.cluster.borrow();

                let Some(topic_data) = cluster_state.metadata.topics.get(name) else {
                    return Ok(Err(ErrorCode::UnknownTopicOrPartition));
                };

                topic_data.clone()
            }
        };

        let error_code: ErrorCode = topic_data.error_code.into();

        if error_code != ErrorCode::None {
            return Ok(Err(error_code));
        }

        return Ok(Ok(topic_data));
    }

    pub async fn shutdown(&self) {
        self.selector.shutdown().await;
    }

    pub fn read_cluster_snapshot(&self) -> Cluster {
        self.selector.cluster.borrow().clone()
    }
}
