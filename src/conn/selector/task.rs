use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    time::Duration,
};

use derive_more::derive::From;
use fnv::FnvHashMap;
use kafka_protocol::messages::{
    metadata_request::MetadataRequestTopic, metadata_response::MetadataResponseTopic,
    MetadataResponse, TopicName,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    backoff::{exponential_backoff, BackoffSession},
    conn::{
        config::{ConnectionManagerConfig, ConnectionRetryConfig, MetadataRefreshConfig},
        host::BrokerHost,
        selector::metadata::{MetadataRefreshContext, MetadataRefreshTask},
    },
    error::KafkaError,
};

use super::{
    connect::{Connect, Tcp},
    metadata::MetadataRefreshResult,
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

fn least_in_flight(
    left: &(&i32, &BrokerHost, &NodeTaskHandle),
    right: &(&i32, &BrokerHost, &NodeTaskHandle),
) -> Ordering {
    left.2.in_flight().cmp(&right.2.in_flight())
}

fn least_failure_streak(
    left: &(&i32, &BrokerHost, &NodeTaskHandle),
    right: &(&i32, &BrokerHost, &NodeTaskHandle),
) -> Ordering {
    left.2.failure_streak().cmp(&right.2.failure_streak())
}

impl BrokerMap {
    /// Get the current "best" connection handle.
    ///
    /// This will prefer connected brokers with the minimum number of pending requests, then favor the minimum number of
    /// pending requests, connected or not.
    pub fn get_best_connection(&self) -> Option<(i32, BrokerHost, NodeTaskHandle)> {
        // prefer connected, non-saturated nodes with least in-flight requests
        let least_loaded_connected = self
            .0
            .iter()
            .filter_map(|(id, (broker, handle))| {
                if handle.capacity().is_some_and(|cap| cap > 0) {
                    Some((id, broker, handle))
                } else {
                    None
                }
            })
            .min_by(least_in_flight);

        if let Some((id, host, handle)) = least_loaded_connected {
            return Some((*id, host.clone(), handle.clone()));
        }

        // next, prefer nodes with no failure streak and least in-flight requests
        let least_loaded_no_failures = self
            .0
            .iter()
            .filter_map(|(id, (broker, handle))| {
                if handle.failure_streak() == 0 {
                    Some((id, broker, handle))
                } else {
                    None
                }
            })
            .min_by(least_in_flight);

        if let Some((id, host, handle)) = least_loaded_no_failures {
            return Some((*id, host.clone(), handle.clone()));
        }

        // lastly, prefer nodes with the lowest failure streak
        self.0
            .iter()
            .map(|(id, (host, handle))| (id, host, handle))
            .min_by(least_failure_streak)
            .map(|(id, host, handle)| (*id, host.clone(), handle.clone()))
    }
}

/// A request to fetch metadata for a specific set of topics, or all topics
pub struct RefreshMetadataRequest {
    /// The requested topics
    ///
    /// If None, this will fetch metadata for all topics.
    ///
    /// If Some(vec![]), this will only refresh the cluster metadata
    pub topics: Option<Vec<MetadataRequestTopic>>,
    /// Channel which sends when the request has been executed
    pub tx: oneshot::Sender<Result<(), KafkaError>>,
}

fn metadata_request_topic_from_entry(
    entry: (&TopicName, &MetadataResponseTopic),
) -> MetadataRequestTopic {
    let mut req = MetadataRequestTopic::default();
    req.name = Some(entry.0.clone());
    req.topic_id = entry.1.topic_id;

    req
}

/// Keeps connections to each broker alive.
///
/// This task will listen for changes to metadata from the [`MetadataRefreshTaskHandle`], then start/stop
/// broker handles as necessary to keep a valid mapping of broker id to host connection.
///
/// If any nodes fail to connect or close their connection unexpectedly, this task will re-spawn those connection
/// tasks to keep connections alive.
///
/// If a [`NodeTask`] panics, then pending requests for the broker will recieve a [`crate::conn::KafkaConnectionError::Closed`]
/// error, and a new [`NodeTask`] and [`NodeTaskHandle`] pair will be created and spawned.
struct SelectorTask<Conn> {
    /// Mapping of broker id to its host
    hosts: BrokerMap,
    /// Shared global cluster state. Contains the latest metadata and mapping of broker id to connection
    tx: watch::Sender<Cluster>,
    /// Join set for running connection tasks. Used to detect failed connections
    join_set: JoinSet<NodeTask<Conn>>,
    /// Configuration settings for retries
    retry_config: ConnectionRetryConfig,
    /// Configuration for metadata refresh process
    metadata_config: MetadataRefreshConfig,
    /// Receiver for requests to refresh metadata now
    rx_topic_metadata: mpsc::Receiver<RefreshMetadataRequest>,
    /// Container to store metadata backoff state per-broker
    metadata_backoff: HashMap<BrokerHost, BackoffSession<()>>,
    /// Join set for the metadata refresh task. This should only have one task spawned at any time.
    metadata_join_set: JoinSet<MetadataRefreshResult>,
    /// Cancellation signal
    cancellation_token: CancellationToken,
    /// Used to create new tcp streams
    connect: Conn,
}

enum Event<Conn> {
    /// Start a refresh of metadata. This is also invoked when a [`NodeTask`]
    /// panics or is aborted, because we no longer have access to the original channel in that case.
    RefreshStart(Option<RefreshMetadataRequest>),
    /// Metadata refresh completed with success or failure.
    RefreshComplete(MetadataRefreshResult),
    /// A node stopped. Note this doesn't necessarily indicate that it should be running.
    /// The [`SelectorTask`] will restart it if it points to a valid broker in the cluster.
    NodeDied(NodeTask<Conn>),
}

impl<Conn: Connect + Send + Clone + 'static> SelectorTask<Conn> {
    async fn run(mut self) -> Result<(), KafkaError> {
        let mut metadata_interval = tokio::time::interval(self.metadata_config.interval);
        metadata_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut bootstrap_sucessful = false;
        let mut retry_metadata_immediately = false;

        loop {
            let allow_metadata_requests = self.metadata_join_set.is_empty();
            let metadata_fut = async {
                // We only allow one metadata refresh task to run at a time.
                if !allow_metadata_requests {
                    return None;
                }

                if retry_metadata_immediately {
                    return Some(None);
                }

                Some(tokio::select! {
                    _ = metadata_interval.tick() => None,
                    Some(req) = self.rx_topic_metadata.recv() => Some(req),
                })
            };

            let event = tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => break,
                // An err here means it panicked. There's no way to recover the original context, so let it go.
                Some(Ok(metadata_refreshed)) = self.metadata_join_set.join_next() => Event::RefreshComplete(metadata_refreshed),
                Some(req) = metadata_fut => Event::RefreshStart(req),
                Some(result) = self.join_set.join_next() => match result {
                    Ok(node_died) => Event::NodeDied(node_died),
                    Err(join_err) => {
                        tracing::error!("node connection task stopped unexpectedly, attempting to recover: {join_err}");
                        Event::RefreshStart(None)
                    }
                },
                // Either all handles are dropped, or we have no nodes to connect to in the cluster.
                else => break
            };

            match event {
                Event::RefreshComplete((mut ctx, metadata)) => {
                    let tx = ctx.request.map(|r| r.tx);

                    match metadata {
                        Ok(metadata) => {
                            bootstrap_sucessful = true;
                            retry_metadata_immediately = false;

                            let throttle_ms: u64 =
                                metadata.throttle_time_ms.try_into().unwrap_or_default();

                            self.update_metadata(metadata);

                            tracing::info!(
                                broker_id = ctx.broker_id,
                                host = ?ctx.host,
                                "successfully updated metadata"
                            );

                            tracing::debug!(
                                "new broker mapping {:?}",
                                self.hosts
                                    .0
                                    .iter()
                                    .map(|(id, (host, _))| (id, host))
                                    .collect::<Vec<_>>()
                            );

                            ctx.backoff.success();

                            if throttle_ms > 0 {
                                ctx.backoff
                                    .schedule_next(Duration::from_millis(throttle_ms), ());
                            }

                            self.metadata_backoff.insert(ctx.host, ctx.backoff);

                            if let Some(tx) = tx {
                                let _ = tx.send(Ok(()));
                            }
                        }
                        Err(e) => {
                            // Check if we've exceeded our limit for bootstrap retries
                            if !bootstrap_sucessful
                                && self
                                    .metadata_config
                                    .max_retries
                                    .is_some_and(|max| ctx.backoff.count() >= max)
                            {
                                tracing::error!(
                                    broker_id = ctx.broker_id,
                                    host = ?ctx.host,
                                    "retries exhausted while bootstrapping: {e}, retries: {}", ctx.backoff.count()
                                );
                                return Err(e);
                            }

                            // Send another attempt immediately if this is not from an upstream request.
                            // The task handles backoff.
                            retry_metadata_immediately = tx.is_none();

                            if matches!(e, KafkaError::Init(_)) {
                                // Error with establishing a connection, so don't back off and reset the attempts
                                // The node task handles backoff and logging in this case.
                                ctx.backoff.schedule_immediate((), true);
                                if let Some(tx) = tx {
                                    let _ = tx.send(Err(e));
                                }
                                continue;
                            }

                            let backoff = exponential_backoff(
                                self.metadata_config.min_backoff,
                                self.metadata_config.max_backoff,
                                ctx.backoff.count(),
                            );

                            ctx.backoff.failure(backoff, ());

                            tracing::error!(
                                broker_id = ctx.broker_id,
                                host = ?ctx.host,
                                "failed to get metadata: {e}, backing off for {backoff:?}, retries: {}", ctx.backoff.count()
                            );

                            self.metadata_backoff.insert(ctx.host, ctx.backoff);

                            if let Some(tx) = tx {
                                let _ = tx.send(Err(e));
                            }
                            continue;
                        }
                    }
                }
                Event::RefreshStart(req) => {
                    let Some((broker_id_for_refresh, host_for_refresh, handle_for_refresh)) =
                        self.tx.borrow().broker_channels.get_best_connection()
                    else {
                        tracing::error!("no connections available for metadata refresh!");
                        break;
                    };

                    tracing::info!(
                        broker_id = broker_id_for_refresh,
                        host = ?host_for_refresh,
                        "attempting to refresh metadata"
                    );

                    let topics = req.as_ref().map(|r| r.topics.clone()).unwrap_or_else(|| {
                        Some(
                            self.tx
                                .borrow()
                                .metadata
                                .topics
                                .iter()
                                .map(metadata_request_topic_from_entry)
                                .collect(),
                        )
                    });

                    let backoff = self
                        .metadata_backoff
                        .remove(&host_for_refresh)
                        .unwrap_or_default();

                    let task = MetadataRefreshTask {
                        context: MetadataRefreshContext {
                            broker_id: broker_id_for_refresh,
                            host: host_for_refresh,
                            node_handle: handle_for_refresh,
                            request: req,
                            backoff,
                        },
                        topics,
                    };

                    self.metadata_join_set.spawn(task.run());
                }
                Event::NodeDied(dead_task) => self.restart_if_needed(dead_task).await,
            }
        }

        for (_, (_, handle)) in self.hosts.0.drain() {
            handle.cancellation_token.cancel();
        }

        while self.join_set.join_next().await.is_some() {}

        let _ = self.tx.send(Default::default());

        Ok(())
    }

    fn update_metadata(&mut self, mut metadata: MetadataResponse) {
        if metadata.brokers.is_empty() {
            tracing::warn!("metadata response has no brokers, ignoring");
        }

        // mapping of broker id to broker host information in the new metadata
        let new_broker_ids: FnvHashMap<_, _> = metadata
            .brokers
            .iter()
            .map(|(id, broker)| (id.0, broker))
            .collect();

        let new_broker_hosts: HashSet<BrokerHost> = metadata
            .brokers
            .iter()
            .map(|(_, broker)| broker.into())
            .collect();

        // remove backoff state for nodes not in the cluster
        self.metadata_backoff
            .retain(|host, _| new_broker_hosts.contains(host));

        // remove nodes that are not in the cluster
        self.hosts.0.retain(|id, (host, handle)| {
            let keep = new_broker_ids.contains_key(id);

            if !keep {
                tracing::debug!(
                    broker_id = id,
                    host = ?host,
                    "removing connection to broker"
                );
                handle.cancellation_token.cancel();
            }

            keep
        });

        let mut broker_ids_changing_hosts = HashSet::new();

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
                        broker_id = broker_id,
                        host = ?host,
                        new_host = ?new_host,
                        "changing hosts"
                    );
                    pair.1.cancellation_token.cancel();
                    pair.0 = new_host;
                    broker_ids_changing_hosts.insert(broker_id);
                }
            } else {
                // we don't have a handle to the broker - create one
                self.start_new_task(broker_id, new_host);
            }
        }

        self.tx.send_modify(|cluster| {
            cluster.broker_channels = self.hosts.clone();
            // merge topic metadata with existing metadata
            // TODO: can we remove any topics here?
            for (k, v) in cluster.metadata.topics.drain(..) {
                if !metadata.topics.contains_key(&k) {
                    metadata.topics.insert(k, v);
                }
            }
            cluster.metadata = metadata;
        });
    }

    fn start_new_task(&mut self, broker_id: i32, host: BrokerHost) {
        tracing::debug!(
            broker_id = broker_id,
            host = ?host,
            "creating new connection task"
        );

        let (handle, task) = new_pair(
            broker_id,
            host.clone(),
            self.retry_config.clone(),
            self.connect.clone(),
        );

        self.join_set.spawn(task.run());

        self.hosts.0.insert(broker_id, (host, handle));
    }

    async fn restart_if_needed(&mut self, mut dead_task: NodeTask<Conn>) {
        if let Some((host, mut handle)) = self.hosts.0.remove(&dead_task.broker_id) {
            tracing::debug!(
                host = ?host,
                broker_id = dead_task.broker_id,
                "restarting connection handle",
            );

            // create new cancellation token to not immediately exit when the task starts
            let cancellation_token = CancellationToken::new();

            dead_task.cancellation_token = cancellation_token.clone();
            handle.cancellation_token = cancellation_token;

            // if the host is different, stop the existing connection
            if dead_task.host != host {
                tracing::debug!(
                    host = ?dead_task.host,
                    broker_id = dead_task.broker_id,
                    "stopping existing connection"
                );
                dead_task = dead_task.shutdown_existing_connection().await;
            }

            dead_task.host = host.clone();

            self.hosts.0.insert(dead_task.broker_id, (host, handle));
            self.join_set.spawn(dead_task.run());
        }
    }
}

/// Handle to [`SelectorTask`] to pass commands to it.
///
/// The selector manages connections to each broker in the cluster.
///
/// The cluster state is exposed as a `watch` channel, which contains a mapping of broker id to a handle to send
/// requests to the broker.
///
/// This handle can queue requests for the broker even if the connection is not yet connected. It will also ensure that
/// queued requests will be sent to the specific broker by id, rather than host. If the cluster configuration changes
/// when the request is queued, it will remain queued and be sent to the new host for the broker. If the new cluster
/// config does not contain the broker id, the request will be dropped and the sender will receive an error indicating
/// the connection is closed.
#[derive(Clone)]
pub(crate) struct SelectorTaskHandle {
    pub cluster: watch::Receiver<Cluster>,
    pub tx_topic_metadata: mpsc::Sender<RefreshMetadataRequest>,
    pub tx_cluster: watch::Sender<Cluster>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl SelectorTaskHandle {
    /// Create a new selector task handle using TCP without TLS.
    pub async fn try_new_tcp(
        bootstrap: &[BrokerHost],
        config: ConnectionManagerConfig,
    ) -> Result<Self, KafkaError> {
        Self::try_new_with_connect(
            bootstrap,
            config.clone(),
            Tcp {
                nodelay: true,
                config: config.conn.io,
            },
        )
        .await
    }

    pub async fn shutdown(&self) {
        self.task_tracker.close();
        self.cancellation_token.cancel();
        self.task_tracker.wait().await;
    }

    async fn try_new_with_connect<Conn: Connect + Clone + Send + 'static>(
        bootstrap: &[BrokerHost],
        config: ConnectionManagerConfig,
        connect: Conn,
    ) -> Result<Self, KafkaError> {
        let mut hosts: BrokerMap = Default::default();
        let mut join_set = JoinSet::new();

        for (id, host) in bootstrap.iter().enumerate() {
            let (handle, task) = new_pair(
                id as i32,
                host.clone(),
                config.conn.retry.clone(),
                connect.clone(),
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

        // TODO: what size for refresh channel?
        let (tx_topic_metadata, rx_topic_metadata) = mpsc::channel(1);

        // start the selector task to manage broker connections
        let selector_task = SelectorTask {
            hosts,
            tx: cluster_tx.clone(),
            rx_topic_metadata,
            join_set,
            retry_config: config.conn.retry,
            metadata_config: config.metadata,
            metadata_backoff: Default::default(),
            metadata_join_set: JoinSet::new(),
            cancellation_token: cancellation_token.clone(),
            connect,
        };

        let join_handle = task_tracker.spawn(selector_task.run());

        tokio::select! {
            // wait for metadata refresh (bootstrap)
            _ = cluster_rx.changed() => Ok(()),
            // or failure to bootstrap
            result = join_handle => result.unwrap(), // TODO handle join error
        }?;

        Ok(Self {
            cluster: cluster_rx,
            tx_topic_metadata,
            cancellation_token,
            task_tracker,
            tx_cluster: cluster_tx,
        })
    }

    pub async fn refresh_metadata_for_topics(
        &self,
        topics: Option<Vec<MetadataRequestTopic>>,
    ) -> Result<(), KafkaError> {
        let (tx, rx) = oneshot::channel();

        self.tx_topic_metadata
            .send(RefreshMetadataRequest { topics, tx })
            .await?;

        rx.await??;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    // TODO tests
}
