use std::sync::Arc;

use crossbeam::sync::ShardedLock;
use kafka_protocol::messages::MetadataResponse;
use tokio::sync::{mpsc, oneshot};
use tokio_util::{
    sync::CancellationToken,
    task::{task_tracker::TaskTrackerWaitFuture, TaskTracker},
};

use crate::conn::{
    manager::{
        try_parse_hosts, BrokerHost, ConnectionManager, ConnectionManagerConfig, GenericRequest,
        InitializationError,
    },
    PreparedConnectionError, Sendable,
};

/// Maintains connections to the entire cluster, and forwards requests to the appropriate broker.
pub struct NetworkClient {
    metadata: Arc<ShardedLock<Option<Arc<MetadataResponse>>>>,
    tx: mpsc::Sender<GenericRequest>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl NetworkClient {
    pub fn try_new(
        brokers: Vec<String>,
        config: ConnectionManagerConfig,
    ) -> Result<Self, InitializationError> {
        Self::try_new_with_hosts(try_parse_hosts(&brokers)?, config)
    }

    pub fn try_new_with_hosts(
        brokers: Vec<BrokerHost>,
        config: ConnectionManagerConfig,
    ) -> Result<Self, InitializationError> {
        // sends are handled in a spawned task, meaning new requests won't need to wait.
        let (tx, rx) = mpsc::channel(1);

        let metadata = Arc::new(ShardedLock::new(None));

        let cancellation_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        let mgr = ConnectionManager::try_new(
            brokers,
            config,
            rx,
            metadata.clone(),
            cancellation_token.clone(),
        )?;

        task_tracker.spawn(mgr.run());
        task_tracker.close();

        Ok(Self {
            metadata: metadata.clone(),
            tx,
            cancellation_token,
            task_tracker,
        })
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

    /// Shut down the network client, closing all connections and cancelling requests
    pub fn shutdown(&self) -> TaskTrackerWaitFuture<'_> {
        tracing::info!("shutting down network client");
        self.cancellation_token.cancel();
        self.task_tracker.wait()
    }

    /// Read a snapshot of the current metadata
    pub fn read_metadata(&self) -> Option<Arc<MetadataResponse>> {
        self.metadata
            .read()
            .unwrap_or_else(|e| {
                tracing::warn!("detected poisoned metadata lock during read {e:?}");
                e.into_inner()
            })
            .as_ref()
            .cloned()
    }
}
