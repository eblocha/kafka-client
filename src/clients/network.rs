use std::sync::Arc;

use kafka_protocol::messages::BrokerId;
use tokio::sync::{mpsc, oneshot};

use crate::conn::{
    manager::{BrokerBoundRequest, ConnectionConfig, ConnectionManager, NodeTaskHandle},
    PreparedConnectionError, Sendable,
};

/// Maintains connections to the entire cluster, and forwards requests to the appropriate broker.
pub struct NetworkClient {
    tx: mpsc::Sender<BrokerBoundRequest>,
}

impl NetworkClient {
    pub fn new(brokers: Vec<String>, config: ConnectionConfig) -> Self {
        let connections = brokers
            .into_iter()
            .map(|broker| {
                let broker: Arc<str> = broker.into();
                (
                    broker.clone(),
                    Arc::new(NodeTaskHandle::new(broker.clone(), Default::default())),
                )
            })
            .collect();

        let (tx, rx) = mpsc::channel(1);

        let mgr = ConnectionManager {
            metadata: None,
            connections,
            config,
            rx,
        };

        // TODO cancellation token, etc.
        tokio::spawn(mgr.run());

        Self { tx }
    }

    pub async fn send<R: Sendable>(
        &self,
        req: R,
        broker_id: Option<BrokerId>,
    ) -> Result<R::Response, PreparedConnectionError> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(BrokerBoundRequest {
                broker_id,
                request: req.into(),
                tx,
            })
            .await
            .map_err(|_| PreparedConnectionError::Closed)?;

        rx.await
            .map_err(|_| PreparedConnectionError::Closed)?
            .and_then(|(frame, record)| {
                R::decode_frame(frame, record).map_err(PreparedConnectionError::Io)
            })
    }
}
