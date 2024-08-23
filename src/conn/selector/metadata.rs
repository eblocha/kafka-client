use kafka_protocol::messages::{MetadataRequest, MetadataResponse};
use tokio::sync::watch;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{backoff::exponential_backoff, conn::config::MetadataRefreshConfig};

use super::selector::Cluster;

/// Refreshes cluster metadata periodically, and exposes it as a [`watch::Sender`].
///
/// This also contains a [`watch::Receiver`] for the [`Cluster`], which provides connection handles over which to send
/// the metadata requests.
struct MetadataRefreshTask {
    rx: watch::Receiver<Cluster>,
    tx: watch::Sender<MetadataResponse>,
    cancellation_token: CancellationToken,
    config: MetadataRefreshConfig,
}

impl MetadataRefreshTask {
    async fn run(self) {
        let mut metadata_interval = tokio::time::interval(self.config.interval);
        metadata_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        'outer: loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => return,
                _ = metadata_interval.tick() => {},
            };

            let mut retries = 0;

            loop {
                let Some((host_for_refresh, handle_for_refresh)) =
                    self.rx.borrow().broker_channels.get_best_connection()
                else {
                    tracing::error!("no connections available for metadata refresh!");
                    return;
                };

                tracing::info!(broker = ?host_for_refresh, "attempting to refresh metadata");

                let request = {
                    let mut r = MetadataRequest::default();
                    r.allow_auto_topic_creation = false;
                    r.topics = None;
                    r
                };

                let metadata = handle_for_refresh.send(request).await;

                match metadata {
                    Ok(metadata) => {
                        if self.tx.send(metadata).ok().is_none() {
                            return;
                        }

                        tracing::info!(
                            "successfully updated metadata using broker {host_for_refresh:?}"
                        );
                        break 'outer;
                    }
                    Err(e) => {
                        let backoff = exponential_backoff(
                            self.config.min_backoff,
                            self.config.max_backoff,
                            retries,
                        );
                        retries += 1;
                        tracing::error!(broker = ?host_for_refresh, "failed to get metadata: {e}, backing off for {backoff:?}, retries: {retries}");
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        }
    }
}

/// Handle to [`MetadataRefreshTask`].
pub struct MetadataRefreshTaskHandle {
    pub rx: watch::Receiver<MetadataResponse>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl MetadataRefreshTaskHandle {
    pub fn new(cluster_rx: watch::Receiver<Cluster>, config: MetadataRefreshConfig) -> Self {
        let (tx, rx) = watch::channel(cluster_rx.borrow().metadata.clone());

        let cancellation_token = CancellationToken::new();

        let task = MetadataRefreshTask {
            rx: cluster_rx,
            tx,
            cancellation_token: cancellation_token.clone(),
            config,
        };

        let task_tracker = TaskTracker::new();

        task_tracker.spawn(task.run());

        Self {
            rx,
            cancellation_token,
            task_tracker,
        }
    }

    /// Stop refreshing metadata.
    pub async fn shutdown(&self) {
        self.task_tracker.close();
        self.cancellation_token.cancel();
        self.task_tracker.wait().await;
    }
}
