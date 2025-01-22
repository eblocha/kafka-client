use std::cmp::Ordering;

use derive_more::derive::From;
use fnv::FnvHashMap;
use kafka_protocol::messages::{
    metadata_request::MetadataRequestTopic, metadata_response::MetadataResponseTopic,
    MetadataRequest, MetadataResponse, TopicName,
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
        KafkaChannelError,
    },
    proto::ver::with_max_version,
};

use super::{
    connect::{Connect, Tcp},
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
    left: &(&BrokerHost, &NodeTaskHandle),
    right: &(&BrokerHost, &NodeTaskHandle),
) -> Ordering {
    left.1.in_flight().cmp(&right.1.in_flight())
}

fn least_failure_streak(
    left: &(&BrokerHost, &NodeTaskHandle),
    right: &(&BrokerHost, &NodeTaskHandle),
) -> Ordering {
    left.1.failure_streak().cmp(&right.1.failure_streak())
}

impl BrokerMap {
    /// Get the current "best" connection handle.
    ///
    /// This will prefer connected brokers with the minimum number of pending requests, then favor the minimum number of
    /// pending requests, connected or not.
    pub fn get_best_connection(&self) -> Option<(BrokerHost, NodeTaskHandle)> {
        // prefer connected, non-saturated nodes with least in-flight requests
        let least_loaded_connected = self
            .0
            .iter()
            .filter_map(|(_, (broker, handle))| {
                if handle.capacity().is_some_and(|cap| cap > 0) {
                    Some((broker, handle))
                } else {
                    None
                }
            })
            .min_by(least_in_flight);

        if let Some((host, handle)) = least_loaded_connected {
            return Some((host.clone(), handle.clone()));
        }

        // next, prefer nodes with no failure streak and least in-flight requests
        let least_loaded_no_failures = self
            .0
            .iter()
            .filter_map(|(_, (broker, handle))| {
                if handle.failure_streak() == 0 {
                    Some((broker, handle))
                } else {
                    None
                }
            })
            .min_by(least_in_flight);

        if let Some((host, handle)) = least_loaded_no_failures {
            return Some((host.clone(), handle.clone()));
        }

        // lastly, prefer nodes with the lowest failure streak
        self.0
            .iter()
            .map(|(_, (broker, handle))| (broker, handle))
            .min_by(least_failure_streak)
            .map(|(host, handle)| (host.clone(), handle.clone()))
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
    pub tx: oneshot::Sender<()>,
}

fn create_metadata_request(
    version: i16,
    topics: Option<Vec<MetadataRequestTopic>>,
) -> Option<MetadataRequest> {
    let mut r = MetadataRequest::default();

    if version >= 4 {
        r.allow_auto_topic_creation = false;
    }

    if version >= 8 {
        if version <= 10 {
            r.include_cluster_authorized_operations = true;
        }

        r.include_topic_authorized_operations = true;
    }

    r.topics = topics;

    Some(r)
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
    /// Container to store metadata backoff state
    metadata_backoff: BackoffSession<Option<RefreshMetadataRequest>>,
    /// Cancellation signal
    cancellation_token: CancellationToken,
    /// Used to create new tcp streams
    connect: Conn,
}

enum Event<Conn> {
    /// Metadata changed, so re-configure connections. This is also invoked when a [`NodeTask`]
    /// panics or is aborted, because we no longer have access to the original channel in that case.
    Refresh(Option<RefreshMetadataRequest>),
    /// A node stopped. Note this doesn't necessarily indicate that it should be running.
    /// The [`SelectorTask`] will restart it if it points to a valid broker in the cluster.
    NodeDied(NodeTask<Conn>),
}

impl<Conn: Connect + Send + Clone + 'static> SelectorTask<Conn> {
    async fn run(mut self) {
        let mut metadata_interval = tokio::time::interval(self.metadata_config.interval);
        metadata_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let metadata_fut = async {
                if let Some(payload) = self.metadata_backoff.wait_next().await {
                    return payload;
                }

                tokio::select! {
                    _ = metadata_interval.tick() => None,
                    Some(req) = self.rx_topic_metadata.recv() => Some(req),
                }
            };

            let event = tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => break,
                req = metadata_fut => Event::Refresh(req),
                Some(result) = self.join_set.join_next() => match result {
                    Ok(node_died) => Event::NodeDied(node_died),
                    Err(join_err) => {
                        tracing::error!("node connection task stopped unexpectedly, attempting to recover: {join_err:?}");
                        Event::Refresh(None)
                    }
                },
                // Either all handles are dropped, or we have no nodes to connect to in the cluster.
                else => break
            };

            match event {
                Event::Refresh(req) => {
                    let Some((host_for_refresh, handle_for_refresh)) =
                        self.tx.borrow().broker_channels.get_best_connection()
                    else {
                        tracing::error!("no connections available for metadata refresh!");
                        return;
                    };

                    tracing::info!(broker = ?host_for_refresh, "attempting to refresh metadata");

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

                    let metadata = handle_for_refresh
                        .send(with_max_version(move |ver| {
                            create_metadata_request(ver, topics)
                        }))
                        .await;

                    match metadata {
                        Ok(metadata) => {
                            // TODO respect throttle time
                            self.update_metadata(metadata);

                            tracing::info!(
                                "successfully updated metadata using broker {host_for_refresh:?}"
                            );

                            self.metadata_backoff.success();

                            if let Some(tx) = req.map(|r| r.tx) {
                                let _ = tx.send(());
                            }
                        }
                        Err(e) => {
                            let backoff = exponential_backoff(
                                self.metadata_config.min_backoff,
                                self.metadata_config.max_backoff,
                                self.metadata_backoff.count(),
                            );

                            self.metadata_backoff.failure(backoff, req);

                            tracing::error!(broker = ?host_for_refresh, "failed to get metadata: {e}, backing off for {backoff:?}, retries: {}", self.metadata_backoff.count());
                            continue;
                        }
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

                        // if the host is different, stop the existing connection
                        if dead_task.host != host {
                            dead_task.connection.store(None);
                        }

                        dead_task.host = host.clone();

                        self.hosts.0.insert(dead_task.broker_id, (host, handle));
                        self.join_set.spawn(dead_task.run());
                    }
                }
            }
        }

        for (_, (_, handle)) in self.hosts.0.drain() {
            handle.cancellation_token.cancel();
        }

        while self.join_set.join_next().await.is_some() {}

        let _ = self.tx.send(Default::default());
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
        let (handle, task) = new_pair(
            broker_id,
            host.clone(),
            self.retry_config.clone(),
            self.connect.clone(),
        );

        self.join_set.spawn(task.run());

        self.hosts.0.insert(broker_id, (host, handle));
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
    pub async fn new_tcp(bootstrap: &[BrokerHost], config: ConnectionManagerConfig) -> Self {
        Self::new_with_connect(
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

    async fn new_with_connect<Conn: Connect + Clone + Send + 'static>(
        bootstrap: &[BrokerHost],
        config: ConnectionManagerConfig,
        connect: Conn,
    ) -> Self {
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
            cancellation_token: cancellation_token.clone(),
            connect,
        };

        task_tracker.spawn(selector_task.run());

        // wait for metadata refresh
        let _ = cluster_rx.changed().await;

        Self {
            cluster: cluster_rx,
            tx_topic_metadata,
            cancellation_token,
            task_tracker,
            tx_cluster: cluster_tx,
        }
    }

    pub async fn refresh_metadata_for_topics(
        &self,
        topics: Option<Vec<MetadataRequestTopic>>,
    ) -> Result<(), KafkaChannelError> {
        let (tx, rx) = oneshot::channel();

        self.tx_topic_metadata
            .send(RefreshMetadataRequest { topics, tx })
            .await?;

        rx.await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    // TODO tests
}
