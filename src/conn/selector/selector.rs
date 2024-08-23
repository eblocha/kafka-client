use std::sync::atomic::Ordering;

use derive_more::derive::From;
use fnv::FnvHashMap;
use kafka_protocol::messages::MetadataResponse;
use tokio::{sync::watch, task::JoinSet};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    backoff::exponential_backoff,
    conn::{config::ConnectionManagerConfig, host::BrokerHost},
};

use super::{
    metadata::MetadataRefreshTaskHandle,
    node_task::{new_pair, NodeTask, NodeTaskHandle},
};

/// Mapping of broker id to [`BrokerHost`] and [`NodeTaskHandle`].
///
/// Used to send requests to specific brokers, or the current least-loaded broker.
#[derive(Debug, Clone, From, Default)]
pub struct BrokerMap(#[from] pub FnvHashMap<i32, (BrokerHost, NodeTaskHandle)>);

/// Current cluster state since the last metadata refresh.
#[derive(Debug, Default, Clone)]
pub struct Cluster {
    /// Mapping of broker id to the [`BrokerHost`] and [`NodeTaskHandle`] to send requests to it.
    pub broker_channels: BrokerMap,
    /// Metadata response last recieved from a refresh.
    pub metadata: MetadataResponse,
}

impl BrokerMap {
    /// Get the current "best" connection handle.
    ///
    /// This will prefer connected brokers with the minimum number of pending requests, then favor the minimum number of
    /// pending requests, connected or not.
    pub fn get_best_connection(&self) -> Option<(BrokerHost, NodeTaskHandle)> {
        // prefer connected nodes with least waiting connections
        let least_loaded_connected = self
            .0
            .iter()
            .filter_map(|(_, (broker, handle))| {
                if handle.connected.load(Ordering::SeqCst) && handle.tx.capacity() > 0 {
                    Some((broker, handle))
                } else {
                    None
                }
            })
            .max_by(|left, right| left.1.tx.capacity().cmp(&right.1.tx.capacity()));

        if let Some((host, handle)) = least_loaded_connected {
            return Some((host.clone(), handle.clone()));
        }

        self.0
            .iter()
            .map(|(_, (broker, handle))| (broker, handle))
            .max_by(|left, right| left.1.tx.capacity().cmp(&right.1.tx.capacity()))
            .map(|(host, handle)| (host.clone(), handle.clone()))
    }
}

/// Keeps connections to each broker alive.
///
/// This task will listen for changes to metadata from the [`MetadataRefreshTaskHandle`], then start/stop
/// broker handles as necessary to keep a valid mapping of broker id to host connection.
///
/// If any nodes fail to connect or close their connection unexpectedly, this task will re-spawn those connection
/// tasks (with exponential backoff as appropriate) to keep connections alive.
///
/// If a [`NodeTask`] panics, then pending requests for the broker will recieve a [`crate::conn::KafkaConnectionError::Closed`]
/// error, and a new [`NodeTask`] and [`NodeTaskHandle`] pair will be created and spawned.
struct SelectorTask {
    /// Mapping of broker id to its host
    hosts: BrokerMap,
    /// Shared global cluster state. Contains the latest metadata and mapping of broker id to connection
    tx: watch::Sender<Cluster>,
    /// Task handle for metadata refreshes
    metadata_task_handle: MetadataRefreshTaskHandle,
    /// Join set for running connection tasks. Used to detect failed connections
    join_set: JoinSet<NodeTask>,
    /// Configuration settings
    config: ConnectionManagerConfig,
    /// Cancellation signal
    cancellation_token: CancellationToken,
}

enum Event {
    /// Metadata changed, so re-configure connections. This is also invoked when a [`NodeTask`]
    /// panics or is aborted, because we no longer have access to the original channel in that case.
    Refresh,
    /// A node stopped. Note this doesn't necessarily indicate that it should be running.
    /// The [`SelectorTask`] will restart it if it points to a valid broker in the cluster.
    NodeDied(NodeTask),
    /// Stop all connections and shut down
    Shutdown,
}

impl SelectorTask {
    async fn run(mut self) {
        loop {
            let event = tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => Event::Shutdown,
                // if this returns None, it means the metadata refresh task has stopped, and we will never get metadata again.
                Ok(_) = self.metadata_task_handle.rx.changed() => Event::Refresh,
                Some(result) = self.join_set.join_next() => match result {
                    Ok(node_died) => Event::NodeDied(node_died),
                    Err(join_err) => {
                        tracing::error!("node connection task stopped unexpectedly, attempting to recover: {join_err:?}");
                        Event::Refresh
                    }
                },
                else => continue
            };

            match event {
                Event::Refresh => {
                    let metadata = self.metadata_task_handle.rx.borrow().clone();
                    if self.update_metadata(metadata).is_none() {
                        break;
                    }
                }
                Event::NodeDied(mut dead_task) => {
                    // is this node supposed to be running?
                    if let Some((host, mut handle)) = self.hosts.0.remove(&dead_task.broker_id) {
                        tracing::debug!(
                            "restarting connection handle to broker_id {}",
                            dead_task.broker_id
                        );

                        // create new cancellation token to not immediately exit when the task starts
                        let cancellation_token = CancellationToken::new();

                        dead_task.cancellation_token = cancellation_token.clone();
                        handle.cancellation_token = cancellation_token;

                        // restart it with the host it should be connected to.
                        if dead_task.connected.fetch_and(false, Ordering::SeqCst)
                            || dead_task.host != host
                        {
                            // remove delay if it was connected or changed hosts
                            dead_task.retries = 0;
                            dead_task.delay = None;
                        } else {
                            let (min, max) = (
                                self.config.conn.retry.min_backoff,
                                self.config.conn.retry.max_backoff,
                            );

                            let backoff = exponential_backoff(min, max, dead_task.retries);

                            dead_task.retries += 1;
                            dead_task.delay = Some(backoff);
                        }

                        dead_task.host = host.clone();

                        self.hosts.0.insert(dead_task.broker_id, (host, handle));
                        self.join_set.spawn(dead_task.run());
                    }
                }
                Event::Shutdown => break,
            }
        }

        for (_, (_, handle)) in self.hosts.0.drain() {
            handle.cancellation_token.cancel();
        }

        while self.join_set.join_next().await.is_some() {}

        self.metadata_task_handle.shutdown().await;

        let _ = self.tx.send(Default::default());
    }

    fn update_metadata(&mut self, metadata: MetadataResponse) -> Option<()> {
        if metadata.brokers.is_empty() {
            tracing::warn!("metadata response has no brokers, ignoring");
            return Some(());
        }

        // mapping of broker id to broker host information in the new metadata
        let new_broker_ids: FnvHashMap<_, _> = metadata
            .brokers
            .iter()
            .map(|(id, broker)| (id.0, broker))
            .collect();

        // remove nodes that are not in the cluster
        self.hosts.0.retain(|id, (host, handle)| {
            let keep = new_broker_ids.contains_key(id);

            if !keep {
                tracing::info!("removing connection to broker {host:?} with id {id}");
                handle.cancellation_token.cancel();
            }

            keep
        });

        // spawn nodes that should be in the cluster
        for (broker_id, broker) in new_broker_ids {
            let new_host = BrokerHost(broker.host.as_str().into(), broker.port as u16);

            if let Some(pair) = self.hosts.0.get_mut(&broker_id) {
                let host = &pair.0;

                if pair.1.tx.is_closed() {
                    // the node is not running, and the receiver dropped - it likely panicked
                    self.start_new_task(broker_id, new_host);
                }
                // if the host is different, stop it (it will restart automatically with the new host)
                else if host != &new_host {
                    tracing::debug!(
                        "stopping connection to host {host:?} for broker_id {}",
                        broker_id
                    );
                    pair.1.cancellation_token.cancel();
                    pair.0 = new_host;
                }
            } else {
                // we don't have a handle to the broker - create one
                self.start_new_task(broker_id, new_host);
            }
        }

        self.tx
            .send(Cluster {
                broker_channels: self.hosts.clone(),
                metadata,
            })
            .ok()
    }

    fn start_new_task(&mut self, broker_id: i32, host: BrokerHost) {
        let (handle, task) = new_pair(
            broker_id,
            host.clone(),
            self.config.conn.retry.connection_timeout,
            self.config.conn.io.clone(),
        );

        self.join_set.spawn(task.run());

        self.hosts.0.insert(broker_id, (host, handle));
    }
}

/// Handle to [`SelectorTask`] to pass commands to it.
pub(crate) struct SelectorTaskHandle {
    pub cluster: watch::Receiver<Cluster>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl SelectorTaskHandle {
    pub async fn new(bootstrap: &[BrokerHost], config: ConnectionManagerConfig) -> Self {
        let mut hosts: BrokerMap = Default::default();
        let mut join_set = JoinSet::new();

        for (id, host) in bootstrap.iter().enumerate() {
            let (handle, task) = new_pair(
                id as i32,
                host.clone(),
                config.conn.retry.connection_timeout,
                config.conn.io.clone(),
            );

            join_set.spawn(task.run());

            hosts.0.insert(id as i32, (host.clone(), handle));
        }

        let cancellation_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        // create the watch channel for the metadata
        let (cluster_tx, mut cluster_rx) = watch::channel::<Cluster>(Cluster {
            broker_channels: hosts.clone(),
            metadata: Default::default(),
        });

        let metadata_task_handle =
            MetadataRefreshTaskHandle::new(cluster_rx.clone(), config.metadata.clone());

        // start the selector task to manage broker connections
        let selector_task = SelectorTask {
            hosts,
            tx: cluster_tx,
            metadata_task_handle,
            join_set,
            config,
            cancellation_token: cancellation_token.clone(),
        };

        task_tracker.spawn(selector_task.run());

        // wait for metadata refresh
        let _ = cluster_rx.changed().await;

        Self {
            cluster: cluster_rx,
            cancellation_token,
            task_tracker,
        }
    }

    pub async fn shutdown(&self) {
        self.task_tracker.close();
        self.cancellation_token.cancel();
        self.task_tracker.wait().await;
    }
}
