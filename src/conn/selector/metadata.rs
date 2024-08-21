use kafka_protocol::messages::{MetadataRequest, MetadataResponse};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::conn::config::ConnectionManagerConfig;

use super::selector::Cluster;

struct MetadataRefreshTask {
    rx: watch::Receiver<Cluster>,
    tx: watch::Sender<MetadataResponse>,
    cancellation_token: CancellationToken,
    config: ConnectionManagerConfig,
}

impl MetadataRefreshTask {
    pub async fn run(mut self) {
        let mut metadata_interval = tokio::time::interval(self.config.metadata_refresh_interval);

        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => return,
                _ = metadata_interval.tick() => {},
            };

            if self.refresh_metadata().await.is_none() {
                return;
            }
        }
    }

    async fn refresh_metadata(&mut self) -> Option<()> {
        let Some((host_for_refresh, handle_for_refresh)) =
            self.rx.borrow().broker_channels.get_best_connection()
        else {
            tracing::error!("no connections available for metadata refresh!");
            return None;
        };

        tracing::info!(broker = ?host_for_refresh, "attempting to refresh metadata");

        let request = {
            let mut r = MetadataRequest::default();
            r.allow_auto_topic_creation = false;
            r.topics = None;
            r
        };

        let metadata = match handle_for_refresh.send(request).await {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(broker = ?host_for_refresh, "failed to get metadata: {e}");
                return Some(());
            }
        };

        let result = self.tx.send(metadata).ok();

        tracing::info!("successfully updated metadata using broker {host_for_refresh:?}");

        result
    }
}

pub struct MetadataRefreshTaskHandle {
    pub rx: watch::Receiver<MetadataResponse>,
    pub cancellation_token: CancellationToken,
}

impl MetadataRefreshTaskHandle {
    pub fn new(cluster_rx: watch::Receiver<Cluster>, config: ConnectionManagerConfig) -> Self {
        let (tx, rx) = watch::channel(cluster_rx.borrow().metadata.clone());

        let cancellation_token = CancellationToken::new();

        let task = MetadataRefreshTask {
            rx: cluster_rx,
            tx,
            cancellation_token: cancellation_token.clone(),
            config: config.clone(),
        };

        tokio::spawn(task.run());

        Self {
            rx,
            cancellation_token,
        }
    }
}
