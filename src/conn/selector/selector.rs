use std::{sync::atomic::Ordering, time::Duration};

use fnv::FnvHashMap;
use kafka_protocol::messages::{MetadataRequest, MetadataResponse};
use rand::Rng;
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

use crate::conn::{config::ConnectionConfig, manager::host::BrokerHost};

use super::node_task::{new_pair, NodeTask, NodeTaskHandle};

#[derive(Debug, Clone, Default)]
pub struct Cluster {
    broker_channels: FnvHashMap<i32, (BrokerHost, NodeTaskHandle)>,
    metadata: MetadataResponse,
}

impl Cluster {
    pub fn get_best_connection(&self) -> Option<(BrokerHost, NodeTaskHandle)> {
        // prefer connected nodes with least waiting connections
        let least_loaded_connected = self
            .broker_channels
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

        self.broker_channels
            .iter()
            .map(|(_, (broker, handle))| (broker, handle))
            .max_by(|left, right| left.1.tx.capacity().cmp(&right.1.tx.capacity()))
            .map(|(host, handle)| (host.clone(), handle.clone()))
    }
}

/// Keeps connections to each broker alive
pub struct SelectorTask {
    /// Mapping of broker id to its host
    hosts: FnvHashMap<i32, (BrokerHost, NodeTaskHandle)>,
    tx: watch::Sender<Cluster>,
    join_set: JoinSet<NodeTask>,
    config: ConnectionConfig,
}

enum Event {
    /// Metadata changed, so re-configure connections
    Refresh,
    /// A node stopped
    NodeDied(NodeTask),
}

impl SelectorTask {
    pub async fn run(mut self) {
        let mut metadata_interval = tokio::time::interval(Duration::from_millis(500));

        loop {
            let event = tokio::select! {
                _ = metadata_interval.tick() => Event::Refresh,
                // if a node panics, the task handle will need to request a new node.
                // we never abort these tasks.
                Some(Ok(node_died)) = self.join_set.join_next(), if !self.join_set.is_empty() => Event::NodeDied(node_died),
                else => continue
            };

            match event {
                Event::Refresh => match self.refresh_metadata().await {
                    Some(_) => {}
                    None => break, // unrecoverable
                },
                Event::NodeDied(mut dead_task) => {
                    // is this node supposed to be running?
                    if let Some((host, handle)) = self.hosts.remove(&dead_task.broker_id) {
                        // restart it with the host it should be connected to.
                        if dead_task.connected.load(Ordering::SeqCst) || dead_task.host != host {
                            // remove delay if it was connected or changed hosts
                            dead_task.attempts = 0;
                            dead_task.delay = None;
                        } else {
                            let (min, max, jitter) = (
                                self.config.retry.min_backoff,
                                self.config.retry.max_backoff,
                                rand::thread_rng().gen_range(0..self.config.retry.jitter),
                            );

                            let backoff =
                                std::cmp::min(min * 2u32.saturating_pow(dead_task.attempts), max)
                                    + min * jitter;

                            dead_task.attempts += 1;
                            dead_task.delay = Some(backoff);
                        }

                        self.hosts.insert(dead_task.broker_id, (host, handle));
                        self.join_set.spawn(dead_task.run());
                    }
                }
            }
        }
    }

    async fn refresh_metadata(&mut self) -> Option<()> {
        let Some((host_for_refresh, handle_for_refresh)) = self.tx.borrow().get_best_connection()
        else {
            tracing::error!("no connections available for metadata refresh!");
            return None;
        };

        tracing::debug!(broker = ?host_for_refresh, "attempting to refresh metadata");

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
        self.hosts.retain(|id, (host, handle)| {
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

            if let Some(pair) = self.hosts.get_mut(&broker_id) {
                let host = &pair.0;

                // if the host is different, stop it (it will restart automatically with the new host)
                if host != &new_host {
                    handle_for_refresh.cancellation_token.cancel();
                }

                pair.0 = new_host;
            } else {
                tracing::info!("creating connection to broker {new_host:?} with id {broker_id}");
                // we don't have a handle to the broker - create one
                let (tx, rx) = mpsc::channel(self.config.io.send_buffer_size);
                let (handle, task) = new_pair(
                    broker_id,
                    new_host.clone(),
                    self.config.retry.connection_timeout,
                    tx,
                    rx,
                );

                self.join_set.spawn(task.run());

                self.hosts.insert(broker_id, (new_host, handle));
            }
        }

        tracing::debug!("successfully updated metadata using broker {host_for_refresh:?}");

        self.tx
            .send(Cluster {
                broker_channels: self.hosts.clone(),
                metadata,
            })
            .ok()
    }
}

pub struct SelectorTaskHandle {
    pub cluster: watch::Receiver<Cluster>,
}

impl SelectorTaskHandle {
    pub async fn new(bootstrap: &[BrokerHost], config: ConnectionConfig) -> Self {
        // fetch metadata from one of the bootstrap servers

        // create the watch channel for the metadata
        let (cluster_tx, cluster_rx) = watch::channel(Default::default());

        // start the selector task to manage broker connections
        let selector_task = SelectorTask {
            hosts: Default::default(),
            tx: cluster_tx,
            join_set: JoinSet::new(),
            config,
        };

        tokio::spawn(selector_task.run());

        Self {
            cluster: cluster_rx,
        }
    }
}
