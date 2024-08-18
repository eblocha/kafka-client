pub mod host;
pub mod node;

use std::sync::Arc;

use crossbeam::sync::{ShardedLock, ShardedLockWriteGuard};
use futures::{future::Either, stream::FuturesUnordered, StreamExt};
use host::BrokerHost;
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse};
use node::NodeTaskHandle;
use rand::seq::SliceRandom;
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    time::MissedTickBehavior,
};
use tokio_util::{
    sync::{CancellationToken, DropGuard},
    task::TaskTracker,
};

use crate::proto::request::KafkaRequest;

use super::{
    codec::sendable::DecodableResponse,
    config::{ConnectionConfig, ConnectionManagerConfig},
    PreparedConnection, PreparedConnectionError,
};

/// The broker could not be determined from the current metadata.
pub struct IndeterminateBrokerError;

#[derive(Debug)]
pub(crate) struct GenericRequest {
    pub request: KafkaRequest,
    pub tx: oneshot::Sender<Result<DecodableResponse, PreparedConnectionError>>,
}

impl GenericRequest {
    /// Determine the broker id to forward the request to, based on the current cluster metadata
    fn broker_id(
        &self,
        _metadata: &MetadataResponse,
    ) -> Result<Option<BrokerId>, IndeterminateBrokerError> {
        Ok(None)
    }
}

#[derive(Debug, Clone)]
struct ManagerState {
    /// Vec of host and connection handle
    ///
    /// It's _critical_ that this remains sorted!
    connections: Vec<(BrokerHost, Arc<NodeTaskHandle>)>,
}

impl ManagerState {
    async fn get_best_connection(&mut self) -> Option<(BrokerHost, Arc<PreparedConnection>)> {
        let mut active_conn_futures: Vec<_> = self
            .connections
            .iter()
            .map(|(broker, handle)| async move {
                let conn = handle.get_if_connected().await;
                (broker.clone(), conn)
            })
            .collect();

        active_conn_futures.shuffle(&mut rand::thread_rng());

        let mut active = FuturesUnordered::from_iter(active_conn_futures);

        let mut best: Option<(BrokerHost, Arc<PreparedConnection>)> = None;
        let mut connected_hosts = Vec::new();

        while let Some((broker, conn)) = active.next().await {
            match conn {
                Some(conn) if conn.capacity() == conn.max_capacity() => {
                    tracing::info!(broker = ?broker, "found active unused connection");
                    // the connection is unused
                    return Some((broker, conn));
                }
                Some(conn)
                    if best
                        .as_ref()
                        .map(|best| best.1.capacity() < conn.capacity())
                        .unwrap_or(true) =>
                {
                    // the connection is better than any other we have so far
                    connected_hosts.push(broker.clone());
                    best.replace((broker, conn));
                }
                Some(_) => {
                    // it's worse than the best so far
                    connected_hosts.push(broker.clone());
                }
                None => {
                    // not connected
                }
            }
        }

        if let Some(best) = best {
            tracing::info!(
                broker = ?best.0,
                "reusing active connection with {} in-flight requests",
                best.1.max_capacity() - best.1.capacity()
            );
            return Some(best);
        }

        // There are no active, unsaturated connections. Race the remaining nodes.
        // We are choosing at random to give healthier nodes a chance at stealing the work

        let mut inactive_conn_futures: Vec<_> = self
            .connections
            .iter()
            .filter(|(broker, _)| !connected_hosts.contains(broker))
            .map(|(broker, handle)| async move {
                let conn = handle.get_connection().await;
                (broker.clone(), conn)
            })
            .collect();

        inactive_conn_futures.shuffle(&mut rand::thread_rng());

        let mut inactive = FuturesUnordered::from_iter(inactive_conn_futures);

        while let Some((broker, conn)) = inactive.next().await {
            match conn {
                Some(conn) => return Some((broker, conn)),
                None => {
                    // failed or not running
                }
            }
        }

        None
    }
}

#[derive(Debug, Error)]
pub enum InitializationError {
    #[error(transparent)]
    ParseError(#[from] url::ParseError),
    #[error("at least 1 host must be specified")]
    NoHosts,
}

impl ManagerState {
    fn try_new(
        brokers: Vec<BrokerHost>,
        config: &ConnectionConfig,
    ) -> Result<Self, InitializationError> {
        if brokers.is_empty() {
            return Err(InitializationError::NoHosts);
        }

        Ok(Self {
            connections: brokers
                .into_iter()
                .map(|host| {
                    (
                        host.clone(),
                        Arc::new(NodeTaskHandle::new(host.clone(), config.clone())),
                    )
                })
                .collect(),
        })
    }
}

/// Manages the cluster metadata and forwards requests to the appropriate broker.
///
/// This is the background task for the NetworkClient.
#[derive(Debug)]
pub struct ConnectionManager {
    /// Broker ID to host
    metadata: Arc<ShardedLock<Option<Arc<MetadataResponse>>>>,
    state: ManagerState,
    config: ConnectionManagerConfig,
    rx: mpsc::Receiver<GenericRequest>,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
    _cancel_on_drop: DropGuard,
}

impl ConnectionManager {
    pub fn try_new(
        brokers: Vec<BrokerHost>,
        config: ConnectionManagerConfig,
        rx: mpsc::Receiver<GenericRequest>,
        metadata: Arc<ShardedLock<Option<Arc<MetadataResponse>>>>,
        cancellation_token: CancellationToken,
    ) -> Result<Self, InitializationError> {
        let task_tracker = TaskTracker::new();
        task_tracker.close();

        let state = ManagerState::try_new(brokers, &config.conn)?;

        Ok(Self {
            metadata,
            state,
            config,
            rx,
            task_tracker,
            cancellation_token: cancellation_token.clone(),
            _cancel_on_drop: cancellation_token.drop_guard(),
        })
    }

    fn read_metadata_snapshot(&self) -> Option<Arc<MetadataResponse>> {
        self.metadata
            .read()
            .unwrap_or_else(|e| {
                tracing::warn!("detected poisoned metadata lock during read {e:?}");
                e.into_inner()
            })
            .as_ref()
            .cloned()
    }

    fn write_metadata(&self) -> ShardedLockWriteGuard<'_, Option<Arc<MetadataResponse>>> {
        self.metadata.write().unwrap_or_else(|e| {
            tracing::warn!("detected poisoned metadata lock during write {e:?}");
            e.into_inner()
        })
    }

    /// Get the connection handle for a broker id
    fn get_handle(&self, broker_id: &BrokerId) -> Option<Arc<NodeTaskHandle>> {
        self.read_metadata_snapshot()
            .as_ref()
            .and_then(|m| {
                m.brokers.get(broker_id).and_then(|broker| {
                    let broker: BrokerHost = broker.into();
                    self.state.connections.iter().find_map(|(host, handle)| {
                        if &broker == host {
                            Some(handle)
                        } else {
                            None
                        }
                    })
                })
            })
            .cloned()
    }

    /// Refresh the metadata and update internal state
    ///
    /// Returns None if there are no brokers in the cluster metadata
    async fn refresh_metadata(&mut self) -> Option<()> {
        let Some((broker, conn)) = self.state.get_best_connection().await else {
            tracing::error!("no connections available for metadata refresh!");
            return None;
        };

        tracing::debug!(broker = ?broker, "attempting to refresh metadata");

        let request = {
            let mut r = MetadataRequest::default();
            r.allow_auto_topic_creation = false;
            r.topics = None;
            r
        };

        let metadata = match conn.send(request).await {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(broker = ?broker, "failed to get metadata: {}", e);
                return Some(());
            }
        };

        if metadata.brokers.is_empty() {
            tracing::warn!("metadata response has no brokers, ignoring");
            return Some(());
        }

        let mut hosts: Vec<BrokerHost> = metadata
            .brokers
            .values()
            .map(|broker| BrokerHost(Arc::from(broker.host.as_str()), broker.port as u16))
            .collect();

        hosts.sort();
        hosts.dedup();

        let mut new_connections: Vec<(BrokerHost, Arc<NodeTaskHandle>)> =
            Vec::with_capacity(hosts.len());

        for host in hosts.iter() {
            if let Ok(idx) = self
                .state
                .connections
                .binary_search_by(|(h, _)| h.cmp(host))
            {
                new_connections.push(self.state.connections[idx].clone())
            } else {
                tracing::info!("discovered broker {host:?}");
                new_connections.push((
                    host.clone(),
                    Arc::new(NodeTaskHandle::new(host.clone(), self.config.conn.clone())),
                ));
            }
        }

        for (host, handle) in self.state.connections.drain(..) {
            if hosts.binary_search(&host).is_err() {
                tracing::info!("closing connection to broker {host:?} because it is no longer part of the cluster");
                handle.shutdown().await;
            }
        }

        if new_connections.is_empty() {
            tracing::error!(
                "metadata refresh resulted in no brokers in the cluster, this is a bug"
            );
            return Some(());
        }

        let metadata = Arc::new(metadata);

        self.state.connections = new_connections;

        self.write_metadata().replace(metadata);

        tracing::debug!("successfully updated metadata using broker {:?}", broker);

        Some(())
    }

    pub async fn run(mut self) {
        let mut metadata_interval = tokio::time::interval(self.config.metadata_refresh_interval);

        // skip missed metadata refreshes in case initial connection backs off
        metadata_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            let either = tokio::select! {
                biased; // prefer cancel, then metadata refresh
                _ = self.cancellation_token.cancelled() => break,
                left = metadata_interval.tick() => Either::Left(left),
                // don't process requests until we know the state of the cluster
                right = self.rx.recv(), if self.read_metadata_snapshot().is_some() => Either::Right(right)
            };

            match either {
                Either::Left(_) => {
                    // metadata refresh returns None when there are no brokers in the cluster
                    // in this case, there is no way to recover
                    if self.refresh_metadata().await.is_none() {
                        return;
                    }
                    // TODO rerun metadata refresh sooner if it failed the first time
                }
                Either::Right(Some(mut req)) => {
                    macro_rules! or_cancel {
                        ($fut:expr) => {
                            tokio::select! {
                                biased;
                                _ = req.tx.closed() => return,
                                _ = self.cancellation_token.cancelled() => return,
                                v = $fut => v
                            }
                        };
                    }

                    let conn = match req.broker_id(self.read_metadata_snapshot().as_ref().expect(
                            "Requests should not be processed until metadata is fetched. This is a bug.",
                        )) {
                            // request needs specific broker
                            Ok(Some(broker_id)) => {
                                let Some(handle) = self.get_handle(&broker_id) else {
                                    tracing::error!("no connections available for request!");
                                    let _ = req.tx.send(Err(PreparedConnectionError::Closed));
                                    continue;
                                };

                                or_cancel!(handle.get_connection())
                            },
                            // any broker will do, race them
                            Ok(None) => or_cancel!(self.state.get_best_connection()).map(|(_, c)| c),
                            _ => {
                                tracing::error!("could not determine broker for request!");
                                let _ = req.tx.send(Err(PreparedConnectionError::Closed));
                                continue;
                            },
                        };

                    let Some(conn) = conn else {
                        tracing::error!("no connections available for request!");
                        let _ = req.tx.send(Err(PreparedConnectionError::Closed));
                        continue;
                    };

                    let tracker = self.task_tracker.clone();

                    tracker.spawn(async move {
                        let res = conn.send(req.request).await;
                        let _ = req.tx.send(res);
                    });
                }
                Either::Right(None) => break, // NetworkClient dropped
            }
        }

        tracing::info!("connection manager is shutting down");

        let mut all = FuturesUnordered::from_iter(
            self.state
                .connections
                .iter()
                .map(|(_, handle)| handle.shutdown()),
        );

        while let Some(()) = all.next().await {}
        self.task_tracker.wait().await;
    }
}
