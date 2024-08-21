use crate::conn::{
    config::ConnectionManagerConfig,
    host::{try_parse_hosts, BrokerHost},
    selector::selector::SelectorTaskHandle,
    KafkaConnectionError, Sendable,
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
        let selector = SelectorTaskHandle::new(brokers, config).await;

        Self { selector }
    }

    pub async fn send<R: Sendable>(&self, req: R) -> Result<R::Response, KafkaConnectionError> {
        let Some((_, handle)) = self
            .selector
            .cluster
            .borrow()
            .broker_channels
            .get_best_connection()
        else {
            return Err(KafkaConnectionError::Closed);
        };

        handle.send(req).await
    }

    pub async fn shutdown(&self) {
        self.selector.shutdown().await;
    }
}
