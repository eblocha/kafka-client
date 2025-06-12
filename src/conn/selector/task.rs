use std::time::Duration;

use kafka_protocol::messages::{metadata_request::MetadataRequestTopic, MetadataResponse};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    backoff::{exponential_backoff, BackoffSession},
    common::{BrokerHost, Node},
    conn::{
        config::{ConnectionManagerConfig, ConnectionRetryConfig, MetadataRefreshConfig},
        selector::{
            cluster::BrokerMapEntry,
            metadata::{MetadataRefreshContext, MetadataRefreshTask},
            ConnectionInitError,
        },
    },
    error::KafkaError,
};

use super::{
    cluster::{BrokerMap, Cluster},
    connect::{Connect, Tcp},
    metadata::MetadataRefreshResult,
    node_task::{new_pair, NodeTask},
};

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
    metadata_backoff: FxHashMap<BrokerHost, BackoffSession<()>>,
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
                () = self.cancellation_token.cancelled() => break,
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

                            tracing::debug!(
                                broker_id = ctx.entry.node.id,
                                host = ?ctx.entry.node.host,
                                "successfully updated metadata {:?}",
                                self.hosts.list_nodes()
                            );

                            ctx.backoff.success();

                            if throttle_ms > 0 {
                                ctx.backoff
                                    .schedule_next(Duration::from_millis(throttle_ms), ());
                            }

                            self.metadata_backoff
                                .insert(ctx.entry.node.host, ctx.backoff);

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
                                    broker_id = ctx.entry.node.id,
                                    host = ?ctx.entry.node.host,
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
                                broker_id = ctx.entry.node.id,
                                host = ?ctx.entry.node.host,
                                "failed to get metadata: {e}, backing off for {backoff:?}, retries: {}", ctx.backoff.count()
                            );

                            self.metadata_backoff
                                .insert(ctx.entry.node.host, ctx.backoff);

                            if let Some(tx) = tx {
                                let _ = tx.send(Err(e));
                            }
                            continue;
                        }
                    }
                }
                Event::RefreshStart(req) => {
                    let Some(entry_for_refresh) = self.tx.borrow().brokers.get_best_connection()
                    else {
                        tracing::error!("no connections available for metadata refresh!");
                        break;
                    };

                    tracing::debug!(
                        broker_id = entry_for_refresh.node.id,
                        host = ?entry_for_refresh.node.host,
                        "attempting to refresh metadata"
                    );

                    let topics = req.as_ref().map(|r| r.topics.clone()).unwrap_or_else(|| {
                        Some(self.tx.borrow().metadata.create_topics_for_refresh())
                    });

                    let backoff = self
                        .metadata_backoff
                        .remove(&entry_for_refresh.node.host)
                        .unwrap_or_default();

                    let task = MetadataRefreshTask {
                        context: MetadataRefreshContext {
                            entry: entry_for_refresh,
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

        for entry in self.hosts.drain() {
            entry.handle.cancellation_token.cancel();
        }

        let mut clean_shutdown = true;

        self.metadata_join_set.abort_all();

        while let Some(result) = self.metadata_join_set.join_next().await {
            if let Err(join_err) = result {
                if join_err.is_panic() {
                    clean_shutdown = false;
                    tracing::error!("metadata refresh task stopped with an error: {join_err}");
                }
            }
        }

        while let Some(result) = self.join_set.join_next().await {
            if let Err(join_err) = result {
                if join_err.is_panic() {
                    clean_shutdown = false;
                    tracing::error!("a broker connection task stopped with an error: {join_err}");
                }
            }
        }

        let _ = self.tx.send(Cluster::default());

        if clean_shutdown {
            tracing::info!("shut down gracefully");
        } else {
            tracing::warn!("shut down with errors");
        }

        Ok(())
    }

    fn update_metadata(&mut self, metadata: MetadataResponse) {
        if metadata.brokers.is_empty() {
            tracing::warn!("metadata response has no brokers, ignoring");
        }

        // mapping of broker id to broker host information in the new metadata
        let new_broker_ids: FxHashMap<_, _> = metadata
            .brokers
            .iter()
            .map(|broker| (broker.node_id.0, broker))
            .collect();

        let new_broker_hosts: FxHashSet<BrokerHost> =
            metadata.brokers.iter().map(BrokerHost::from).collect();

        // remove backoff state for nodes not in the cluster
        self.metadata_backoff
            .retain(|host, _| new_broker_hosts.contains(host));

        // remove nodes that are not in the cluster
        self.hosts.retain(|entry| {
            let keep = new_broker_ids.contains_key(&entry.node.id);

            if !keep {
                tracing::debug!(
                    broker_id = entry.node.id,
                    host = ?entry.node.host,
                    "removing connection to broker"
                );
                entry.handle.cancellation_token.cancel();
            }

            keep
        });

        let mut broker_ids_changing_hosts = FxHashSet::default();

        // spawn nodes that should be in the cluster
        for (broker_id, broker) in new_broker_ids {
            let new_node = Node::from(broker);

            if let Some(entry) = self.hosts.get_mut(&broker_id) {
                let host = &entry.node.host;

                if entry.handle.tx.is_closed() {
                    // the node is not running, and the receiver dropped - it likely panicked
                    self.start_new_task(broker_id, new_node);
                }
                // if the host is different, stop it (it will restart automatically with the new host)
                else if host != &new_node.host {
                    tracing::debug!(
                        broker_id = broker_id,
                        host = ?host,
                        new_host = ?new_node.host,
                        "changing hosts"
                    );
                    entry.handle.cancellation_token.cancel();
                    entry.node = new_node;
                    broker_ids_changing_hosts.insert(broker_id);
                }
            } else {
                // we don't have a handle to the broker - create one
                self.start_new_task(broker_id, new_node);
            }
        }

        self.tx.send_modify(|cluster| {
            cluster.brokers = self.hosts.clone();
            cluster.metadata.update_with(metadata);
        });
    }

    fn start_new_task(&mut self, broker_id: i32, node: Node) {
        tracing::debug!(
            broker_id = broker_id,
            host = ?node.host,
            "creating new connection task"
        );

        let (handle, task) = new_pair(
            broker_id,
            node.host.clone(),
            self.retry_config.clone(),
            self.connect.clone(),
        );

        self.join_set.spawn(task.run());

        self.hosts.insert(BrokerMapEntry { node, handle });
    }

    async fn restart_if_needed(&mut self, mut dead_task: NodeTask<Conn>) {
        if let Some(mut entry) = self.hosts.remove(&dead_task.broker_id) {
            tracing::debug!(
                broker_id = dead_task.broker_id,
                host = ?entry.node.host,
                "restarting connection handle",
            );

            // create new cancellation token to not immediately exit when the task starts
            let cancellation_token = CancellationToken::new();

            dead_task.cancellation_token = cancellation_token.clone();
            entry.handle.cancellation_token = cancellation_token;

            // if the host is different, stop the existing connection
            if dead_task.host != entry.node.host {
                tracing::debug!(
                    host = ?dead_task.host,
                    broker_id = dead_task.broker_id,
                    "stopping existing connection"
                );
                dead_task = dead_task.shutdown_existing_connection().await;
            }

            dead_task.host = entry.node.host.clone();

            self.hosts.insert(entry);
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

    pub async fn await_shutdown(&self) {
        self.task_tracker.wait().await;
    }

    pub async fn shutdown(&self) {
        self.task_tracker.close();
        self.cancellation_token.cancel();
        self.await_shutdown().await;
    }

    async fn try_new_with_connect<Conn: Connect + Clone + Send + 'static>(
        bootstrap: &[BrokerHost],
        config: ConnectionManagerConfig,
        connect: Conn,
    ) -> Result<Self, KafkaError> {
        let mut hosts: BrokerMap = BrokerMap::default();
        let mut join_set = JoinSet::new();

        for (id, host) in bootstrap.iter().enumerate() {
            let id = id as i32;
            let (handle, task) =
                new_pair(id, host.clone(), config.conn.retry.clone(), connect.clone());

            join_set.spawn(task.run());

            hosts.insert(BrokerMapEntry {
                node: Node {
                    id,
                    host: host.clone(),
                    rack: None,
                },
                handle,
            });
        }

        let cancellation_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        // create the watch channel for the metadata
        let (cluster_tx, mut cluster_rx) = watch::channel::<Cluster>(Cluster::new(hosts.clone()));

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
            result = join_handle => result.map_err(|join_err| {
                tracing::error!("bootstrapping stopped unexpectedly: {join_err}");
                KafkaError::Init(ConnectionInitError::Closed)
            })?,
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
