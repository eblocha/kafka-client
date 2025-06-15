use std::sync::Arc;

use arc_swap::Guard;
use kafka_protocol::messages::{metadata_request::MetadataRequestTopic, TopicName};

use crate::{
    common::BrokerHost,
    conn::{
        config::ConnectionManagerConfig,
        selector::{Cluster, NodeTaskHandle, SelectorTaskHandle},
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
        let handle = self.get_best_handle()?;
        handle.send(req).await
    }

    /// Send a message to any available broker, and abandon the response
    /// This uses load-balancing to distribute the request load.
    pub async fn send_and_forget<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<(), KafkaError> {
        let handle = self.get_best_handle()?;
        handle.send_and_forget(req).await
    }

    /// Send a request to a specific broker by id.
    pub async fn send_to<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
        broker_id: i32,
    ) -> Result<R::Response, KafkaError> {
        let handle = self.get_handle_for_broker(broker_id)?;
        handle.send(req).await
    }

    /// Send a request to a specific broker by id, then abandon it.
    pub async fn send_to_and_forget<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
        broker_id: i32,
    ) -> Result<(), KafkaError> {
        let handle = self.get_handle_for_broker(broker_id)?;
        handle.send_and_forget(req).await
    }

    fn get_handle_for_broker(&self, broker_id: i32) -> Result<NodeTaskHandle, KafkaError> {
        let cluster = self.borrow_cluster();
        let Some(entry) = cluster.brokers.get_connection_for(broker_id) else {
            tracing::error!("no broker handle for id {broker_id}");
            return Err(KafkaChannelError::Closed.into());
        };
        Ok(entry.handle.clone())
    }

    fn get_best_handle(&self) -> Result<NodeTaskHandle, KafkaError> {
        let Some(entry) = self.borrow_cluster().brokers.get_best_connection() else {
            return Err(KafkaChannelError::Closed.into());
        };
        Ok(entry.handle)
    }

    pub(crate) fn get_errored_topic_names<'a>(
        &self,
        topic_names: impl IntoIterator<Item = &'a TopicName>,
    ) -> Vec<TopicName> {
        let cluster_state = self.borrow_cluster();
        topic_names
            .into_iter()
            .filter(|topic_name| {
                cluster_state
                    .metadata
                    .get_topic_metadata_by_name(topic_name)
                    .is_err()
            })
            .cloned()
            .collect::<Vec<_>>()
    }

    pub(crate) async fn load_topic_metadata(
        &self,
        topic_names: Vec<MetadataRequestTopic>,
    ) -> Result<(), KafkaError> {
        if !topic_names.is_empty() {
            self.selector
                .refresh_metadata_for_topics(Some(topic_names))
                .await?;
        }

        Ok(())
    }

    pub(crate) fn borrow_cluster(&self) -> Guard<Arc<Cluster>> {
        self.selector.cluster.load()
    }

    pub async fn shutdown(&self) {
        self.selector.shutdown().await;
    }

    pub async fn await_shutdown(&self) {
        self.selector.await_shutdown().await;
    }
}
