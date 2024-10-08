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
pub struct NetworkClient {
    selector: SelectorTaskHandle,
}

impl NetworkClient {
    pub async fn try_new(
        brokers: &[String],
        config: ConnectionManagerConfig,
    ) -> Result<Self, url::ParseError> {
        Ok(Self::new_with_hosts(&try_parse_hosts(brokers)?, config).await)
    }

    pub async fn new_with_hosts(brokers: &[BrokerHost], config: ConnectionManagerConfig) -> Self {
        let selector = SelectorTaskHandle::new_tcp(brokers, config).await;

        Self { selector }
    }

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

    pub async fn send_to<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
        broker_id: i32,
    ) -> Result<R::Response, KafkaChannelError> {
        let cluster = self.selector.cluster.borrow();
        let Some((_, handle)) = cluster.broker_channels.0.get(&broker_id) else {
            tracing::error!("no broker handle for id {broker_id}");
            return Err(KafkaChannelError::Closed);
        };

        handle.clone().send(req).await
    }

    pub async fn shutdown(&self) {
        self.selector.shutdown().await;
    }

    pub fn read_cluster_snapshot(&self) -> Cluster {
        self.selector.cluster.borrow().clone()
    }
}
