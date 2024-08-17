use tokio::sync::{mpsc, oneshot};

use crate::conn::{
    manager::{ConnectionConfig, ConnectionManager, GenericRequest},
    PreparedConnectionError, Sendable,
};

/// Maintains connections to the entire cluster, and forwards requests to the appropriate broker.
pub struct NetworkClient {
    tx: mpsc::Sender<GenericRequest>,
}

impl NetworkClient {
    pub fn new(brokers: Vec<String>, config: ConnectionConfig) -> Self {
        let (tx, rx) = mpsc::channel(1);

        let mgr = ConnectionManager::new(brokers, config, rx);

        // TODO cancellation token, etc.
        tokio::spawn(mgr.run());

        Self { tx }
    }

    pub async fn send<R: Sendable>(&self, req: R) -> Result<R::Response, PreparedConnectionError> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(GenericRequest {
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
