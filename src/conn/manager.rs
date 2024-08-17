use std::{collections::HashMap, io, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures::{future::Either, stream::FuturesUnordered, FutureExt, StreamExt, TryFutureExt};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse};
use rand::Rng;

use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    time::MissedTickBehavior,
};
use tokio_util::{
    sync::{CancellationToken, DropGuard},
    task::{task_tracker::TaskTrackerWaitFuture, TaskTracker},
};

use super::{
    codec::sendable::RequestRecord, request::KafkaRequest, PreparedConnection,
    PreparedConnectionError, PreparedConnectionInitializationError,
};

// recv request for connection
// if bound for specific broker, choose that broker for connection

// otherwise: https://github.com/apache/kafka/blob/0f7cd4dcdeb2c705c01743927e36b66b06010f20/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L709
// chose a random number 0..nodes, this is where we start iteration (and wrap around)
// for each node:
// if the node is connected and isn't due for metadata refresh , we have at least one "ready".
// if it also has no in-flight requests, select it as the best node
// otherwise, choose the node that meets the above with the lowest in-flight requests

// if no nodes meet the above, get any connecting node (kafka chooses the last one?)

// if no connecting node, then find nodes that:
// - is disconnected
// - has not tried to connect within the backoff period
// and choose the one that has been the longest since last connection attempt

// if no nodes left after the above, error

/// Controls how reconnection attempts are handled.
#[derive(Debug, Clone)]
pub struct ConnectionRetryConfig {
    /// Maximum number of connection retry attempts before returning an error.
    /// If None, the retries are infinite.
    ///
    /// Default None
    pub max_retries: Option<u32>,
    /// Minimum time to wait between connection attempts.
    ///
    /// Default 10ms
    pub min_backoff: Duration,
    /// Maximum time to wait between connection attempts.
    ///
    /// Default 30s
    pub max_backoff: Duration,
    /// Random noise to apply to backoff duration.
    /// Every backoff adds `min_backoff * 0..jitter` to its wait time.
    ///
    /// Default 10
    pub jitter: u32,
    /// Timeout to establish a connection before retrying.
    ///
    /// Default 10s
    pub connection_timeout: Duration,
}

impl Default for ConnectionRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: None,
            jitter: 10,
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConnectionConfig {
    /// Connection retry configuration
    pub retry: ConnectionRetryConfig,
}

// TODO config?
/// Number of requests for a connection to batch at the same time.
const CONNECTION_REQ_BUFFER_SIZE: usize = 10;

struct NodeBackgroundTask {
    rx: mpsc::Receiver<oneshot::Sender<Arc<PreparedConnection>>>,
    broker: Arc<str>,
    connection: Option<Arc<PreparedConnection>>,
    cancellation_token: CancellationToken,
    config: ConnectionConfig,
}

impl NodeBackgroundTask {
    async fn run(mut self) {
        let mut recv_buf = Vec::with_capacity(CONNECTION_REQ_BUFFER_SIZE);

        'outer: loop {
            let count = tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => break,
                count = self.rx.recv_many(&mut recv_buf, CONNECTION_REQ_BUFFER_SIZE) => count,
            };

            if count == 0 {
                break;
            }

            if let Some(ref conn) = self.connection {
                if !conn.is_closed() {
                    tracing::debug!(broker = ?self.broker, "reusing existing connection");
                    for sender in recv_buf.drain(..) {
                        let _ = sender.send(conn.clone());
                    }
                    continue;
                }

                tracing::info!(
                    broker = ?self.broker,
                    "existing connection is disconnected, attempting to reconnect",
                );
            }

            let mut current_backoff;
            let mut current_retries = 0u32;

            let conn = loop {
                let config = super::KafkaConnectionConfig::default();

                tracing::info!(broker = ?self.broker, "attempting to connect");

                let senders_dropped =
                    FuturesUnordered::from_iter(recv_buf.iter_mut().map(|s| s.closed()));

                let timeout = tokio::time::sleep(self.config.retry.connection_timeout);

                macro_rules! abandon {
                    () => {{
                        tracing::info!(
                            broker = ?self.broker,
                            "abandoning connection attempts because all clients aborted"
                        );
                        continue 'outer
                    }};
                }

                let connect_fut = TcpStream::connect(self.broker.as_ref())
                    .map_err(PreparedConnectionInitializationError::Io)
                    .and_then(|io| PreparedConnection::connect(io, &config));

                let res = tokio::select! {
                    biased;
                    // cancel reasons
                    _ = self.cancellation_token.cancelled() => break 'outer,
                    _ = senders_dropped.count() => abandon!(),
                    _ = timeout => Err(PreparedConnectionInitializationError::Io(io::Error::from(io::ErrorKind::TimedOut))),

                    // connect
                    res = connect_fut => res,
                };

                match res {
                    Ok(conn) => break Some(Arc::new(conn)),
                    Err(e) => {
                        let jitter = rand::thread_rng().gen_range(0..self.config.retry.jitter);

                        current_backoff = std::cmp::min(
                            self.config.retry.min_backoff * 2u32.saturating_pow(current_retries),
                            self.config.retry.max_backoff,
                        ) + self.config.retry.min_backoff * jitter;

                        current_retries = current_retries.saturating_add(1);

                        tracing::error!(
                            broker = ?self.broker,
                            "failed to connect: {}, backing off for {}ms: {} of {} attempts",
                            e,
                            current_backoff.as_millis(),
                            current_retries,
                            self.config
                                .retry
                                .max_retries
                                .map(|c| c.to_string())
                                .unwrap_or_else(|| "Inf".to_string())
                        );

                        let mut senders_dropped = FuturesUnordered::new();
                        senders_dropped.extend(recv_buf.iter_mut().map(|s| s.closed().fuse()));

                        let sleep = tokio::time::sleep(current_backoff);

                        tokio::select! {
                            biased;
                            _ = senders_dropped.count() => abandon!(),
                            _ = sleep => {},
                        }
                    }
                }
            };

            if let Some(ref conn) = conn {
                tracing::info!(broker = ?self.broker, "connected successfully");
                for sender in recv_buf.drain(..) {
                    let _ = sender.send(conn.clone());
                }
            } else {
                tracing::error!(broker = ?self.broker, "ran out of retries while connecting");
                recv_buf.clear();
            }

            self.connection = conn;
        }

        if let Some(conn) = self.connection.take() {
            tracing::debug!(broker = ?self.broker, "closing active connection");
            conn.shutdown().await;
            tracing::info!(broker = ?self.broker, "active connection closed");
        }
    }
}

#[derive(Debug)]
struct NodeTaskHandle {
    tx: mpsc::Sender<oneshot::Sender<Arc<PreparedConnection>>>,
    broker: Arc<str>,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
    _cancel_on_drop: DropGuard,
}

impl NodeTaskHandle {
    pub fn new(broker: Arc<str>, config: ConnectionConfig) -> Self {
        let (tx, rx) = mpsc::channel(CONNECTION_REQ_BUFFER_SIZE);

        let cancellation_token = CancellationToken::new();

        let task = NodeBackgroundTask {
            broker: broker.clone(),
            rx,
            connection: Default::default(),
            cancellation_token: cancellation_token.clone(),
            config,
        };

        let task_tracker = TaskTracker::new();

        task_tracker.spawn(task.run());
        task_tracker.close();

        Self {
            tx,
            broker,
            task_tracker,
            // task_handle,
            cancellation_token: cancellation_token.clone(),
            _cancel_on_drop: cancellation_token.drop_guard(),
        }
    }

    pub async fn get_connection(&self) -> Option<Arc<PreparedConnection>> {
        let (tx, rx) = oneshot::channel();

        self.tx.send(tx).await.ok()?;

        rx.await.ok()
    }

    pub fn shutdown(&self) -> TaskTrackerWaitFuture<'_> {
        tracing::info!(broker = ?self.broker, "shutting down connection handle");
        self.cancellation_token.cancel();
        self.task_tracker.wait()
    }
}

/// The broker could not be determined from the current metadata.
pub struct IndeterminateBrokerError;

#[derive(Debug)]
pub(crate) struct GenericRequest {
    pub request: KafkaRequest,
    pub tx: oneshot::Sender<Result<(BytesMut, RequestRecord), PreparedConnectionError>>,
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

async fn connection_race(
    iter: impl IntoIterator<Item = (Arc<str>, Arc<NodeTaskHandle>)>,
) -> Option<(Arc<str>, Arc<PreparedConnection>)> {
    let mut all =
        FuturesUnordered::from_iter(iter.into_iter().map(|(broker, handle)| async move {
            let conn = handle.get_connection().await;
            (broker, conn)
        }));

    while let Some((broker, conn)) = all.next().await {
        match conn {
            Some(conn) => return Some((broker, conn)),
            None => continue,
        }
    }

    None
}

#[derive(Debug, Clone)]
struct ManagerState {
    /// Host to connection
    connections: HashMap<Arc<str>, Arc<NodeTaskHandle>>,
}

impl ManagerState {
    /// Get a random connection by racing the connection handles
    async fn connection_race(&self) -> Option<(Arc<str>, Arc<PreparedConnection>)> {
        let options = self
            .connections
            .iter()
            .map(|(broker, handle)| (broker.clone(), handle.clone()));

        connection_race(options).await
    }
}

impl From<Vec<String>> for ManagerState {
    fn from(brokers: Vec<String>) -> Self {
        let connections = brokers
            .into_iter()
            .map(|broker| {
                let broker: Arc<str> = broker.into();
                (
                    broker.clone(),
                    Arc::new(NodeTaskHandle::new(broker.clone(), Default::default())),
                )
            })
            .collect();

        Self { connections }
    }
}

/// Manages the cluster metadata and forwards requests to the appropriate broker.
///
/// This is the background task for the NetworkClient.
#[derive(Debug)]
pub struct ConnectionManager {
    /// Broker ID to host
    metadata: Option<MetadataResponse>,
    state: ManagerState,
    config: ConnectionConfig,
    rx: mpsc::Receiver<GenericRequest>,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
    _cancel_on_drop: DropGuard,
}

impl ConnectionManager {
    pub fn new(
        brokers: Vec<String>,
        config: ConnectionConfig,
        rx: mpsc::Receiver<GenericRequest>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let task_tracker = TaskTracker::new();
        task_tracker.close();

        Self {
            metadata: None,
            state: brokers.into(),
            config,
            rx,
            task_tracker,
            cancellation_token: cancellation_token.clone(),
            _cancel_on_drop: cancellation_token.drop_guard(),
        }
    }

    /// Get the connection handle for a broker id
    fn get_handle(&self, broker_id: &BrokerId) -> Option<Arc<NodeTaskHandle>> {
        self.metadata
            .as_ref()
            .and_then(|m| {
                m.brokers.get(broker_id).and_then(|broker| {
                    self.state
                        .connections
                        .get(format!("{}:{}", broker.host.as_str(), broker.port).as_str())
                })
            })
            .cloned()
    }

    /// Refresh the metadata and update internal state
    ///
    /// Returns None if there are no brokers in the cluster metadata
    async fn refresh_metadata(&mut self) -> Option<()> {
        // TODO load balancing?
        // try to get a connection
        let Some((broker, conn)) = self.state.connection_race().await else {
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

        if metadata.brokers.len() == 0 {
            tracing::warn!("metadata request returned no brokers, ignoring");
            return Some(());
        }

        let hosts: Vec<_> = metadata
            .brokers
            .values()
            .map(|broker| format!("{}:{}", broker.host.as_str(), broker.port))
            .collect();

        let new_connections: HashMap<Arc<str>, Arc<NodeTaskHandle>> = hosts
            .into_iter()
            .map(|host| {
                let broker: Arc<str> = Arc::from(host);
                let handle = self.state.connections.remove(&broker).unwrap_or_else(|| {
                    tracing::info!("discovered broker {}", broker);
                    Arc::new(NodeTaskHandle::new(broker.clone(), self.config.clone()))
                });

                (broker.clone(), handle)
            })
            .collect();

        for (host, handle) in self.state.connections.drain() {
            tracing::info!(
                "closing connection to broker {} because it is no longer part of the cluster",
                host
            );
            handle.shutdown().await;
        }

        self.state.connections = new_connections;
        self.metadata.replace(metadata);

        tracing::debug!("successfully updated metadata using broker {}", broker);

        Some(())
    }

    pub async fn run(mut self) {
        let mut metadata_interval = tokio::time::interval(Duration::from_millis(500));

        // skip missed metadata refreshes in case initial connection backs off
        metadata_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            let either = tokio::select! {
                biased; // prefer cancel, then metadata refresh
                _ = self.cancellation_token.cancelled() => break,
                left = metadata_interval.tick() => Either::Left(left),
                // don't process requests until we know the state of the cluster
                right = self.rx.recv(), if self.metadata.is_some() => Either::Right(right)
            };

            match either {
                Either::Left(_) => {
                    // metadata refresh returns None when there are no brokers in the cluster
                    // in this case, there is no way to recover
                    if self.refresh_metadata().await.is_none() {
                        tracing::error!("no brokers in cluster!");
                        return;
                    }
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

                    let conn = match req.broker_id(self.metadata.as_ref().expect(
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
                            Ok(None) => or_cancel!(self.state.connection_race()).map(|(_, c)| c),
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
                .values()
                .map(|handle| handle.shutdown()),
        );

        while let Some(()) = all.next().await {}
        self.task_tracker.wait().await;
    }
}
