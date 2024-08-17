use tokio::sync::{mpsc, oneshot};
use tokio_util::{
    sync::CancellationToken,
    task::{task_tracker::TaskTrackerWaitFuture, TaskTracker},
};

use crate::conn::{
    manager::{ConnectionConfig, ConnectionManager, GenericRequest},
    PreparedConnectionError, Sendable,
};

/// Maintains connections to the entire cluster, and forwards requests to the appropriate broker.
pub struct NetworkClient {
    tx: mpsc::Sender<GenericRequest>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl NetworkClient {
    pub fn new(brokers: Vec<String>, config: ConnectionConfig) -> Self {
        let (tx, rx) = mpsc::channel(1);

        let cancellation_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        let mgr = ConnectionManager::new(brokers, config, rx, cancellation_token.clone());

        // TODO cancellation token, etc.
        let _ = task_tracker.spawn(mgr.run());
        task_tracker.close();

        Self {
            tx,
            cancellation_token,
            task_tracker,
        }
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

    pub fn shutdown(&self) -> TaskTrackerWaitFuture<'_> {
        tracing::info!("shutting down network client");
        self.cancellation_token.cancel();
        self.task_tracker.wait()
    }
}
