use std::{collections::HashMap, io, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures::{future::Either, stream::FuturesUnordered, StreamExt, TryFutureExt};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse};
use rand::Rng;

use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
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
                    tracing::debug!("reusing existing connection for broker {}", self.broker);
                    for sender in recv_buf.drain(..) {
                        let _ = sender.send(conn.clone());
                    }
                    continue;
                }

                tracing::info!(
                    "existing connection for broker {} is disconnected, attempting to reconnect",
                    self.broker
                );
            }

            let mut current_backoff;
            let mut current_retries = 0u32;

            let conn = loop {
                let config = super::KafkaConnectionConfig::default();

                tracing::info!("attempting to connect to broker {}", self.broker);

                let mut senders_dropped = FuturesUnordered::new();
                senders_dropped.extend(recv_buf.iter_mut().map(|s| s.closed()));

                let timeout = tokio::time::sleep(self.config.retry.connection_timeout);

                macro_rules! abandon {
                    () => {{
                        tracing::info!("abandoning connection attempts to broker {} because all clients aborted", self.broker);
                        break None
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

                        tracing::warn!(
                            "failed to connect to broker at {}: {}, backing off for {}ms with retries: {} of {}",
                            self.broker,
                            e,
                            current_backoff.as_millis(),
                            current_retries,
                            // FIXME avoid pre-formatting
                            if let Some(max_retries) = self.config.retry.max_retries { max_retries.to_string() } else { "Inf".to_string() }
                        );

                        let mut senders_dropped = FuturesUnordered::new();
                        senders_dropped.extend(recv_buf.iter_mut().map(|s| s.closed()));

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
                tracing::info!("connected to broker {}", self.broker);
                for sender in recv_buf.drain(..) {
                    let _ = sender.send(conn.clone());
                }
            } else {
                tracing::error!("ran out of retries connecting to broker {}", self.broker);
                recv_buf.clear();
            }

            self.connection = conn;
        }

        if let Some(conn) = self.connection.take() {
            conn.shutdown().await;
        }
    }
}

#[derive(Debug)]
struct NodeTaskHandle {
    tx: mpsc::Sender<oneshot::Sender<Arc<PreparedConnection>>>,
    broker: Arc<str>,
    task_tracker: TaskTracker,
    // task_handle: JoinHandle<()>,
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

        let _task_handle = task_tracker.spawn(task.run());

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
        tracing::info!("shutting down connection handle for broker {}", self.broker);
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

/// Manages the cluster metadata and forwards requests to the appropriate broker.
///
/// This is the background task for the NetworkClient.
#[derive(Debug)]
pub struct ConnectionManager {
    /// Broker ID to host
    metadata: Option<MetadataResponse>,
    /// Host to connection
    connections: HashMap<Arc<str>, Arc<NodeTaskHandle>>,
    config: ConnectionConfig,
    rx: mpsc::Receiver<GenericRequest>,
}

impl ConnectionManager {
    pub fn new(
        brokers: Vec<String>,
        config: ConnectionConfig,
        rx: mpsc::Receiver<GenericRequest>,
    ) -> Self {
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

        Self {
            metadata: None,
            connections,
            config,
            rx,
        }
    }

    /// Get a random connection handle.
    fn random_handle(&self) -> Option<(Arc<str>, Arc<NodeTaskHandle>)> {
        if self.connections.is_empty() {
            return None;
        }

        let index = rand::thread_rng().gen_range(0..self.connections.len());

        self.connections
            .iter()
            .nth(index)
            .map(|(broker, handle)| (broker.to_owned(), handle.clone()))
    }

    /// Get the connection handle for a broker id
    fn get_handle(&self, broker_id: &BrokerId) -> Option<Arc<NodeTaskHandle>> {
        self.metadata
            .as_ref()
            .and_then(|m| {
                m.brokers.get(broker_id).and_then(|broker| {
                    self.connections
                        .get(format!("{}:{}", broker.host.as_str(), broker.port).as_str())
                })
            })
            .cloned()
    }

    /// Refresh the metadata and update internal state
    async fn refresh_metadata(&mut self) {
        // TODO load balancing?
        // get a random handle
        let Some((broker, handle)) = self.random_handle() else {
            // our connections map is empty
            tracing::error!("no connections available for metadata refresh!");
            return;
        };

        tracing::debug!("attempting to refresh metadata using broker {}", broker);

        // try to get a connection
        let Some(conn) = handle.get_connection().await else {
            // ran out of retries or the handle is closed.
            // either way, shut down the handle and try again next time
            tracing::warn!("restarting connection task for broker {}", broker);
            handle.shutdown().await;
            self.connections.insert(
                broker.clone(),
                Arc::new(NodeTaskHandle::new(broker, self.config.clone())),
            );
            return;
        };

        let request = {
            let mut r = MetadataRequest::default();
            r.allow_auto_topic_creation = false;
            r.topics = None;
            r
        };

        let metadata = match conn.send(request).await {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("failed to get metadata from broker {}: {}", broker, e);
                return;
            }
        };

        let hosts: Vec<_> = metadata
            .brokers
            .values()
            .map(|broker| format!("{}:{}", broker.host.as_str(), broker.port))
            .collect();

        let new_connections: HashMap<Arc<str>, Arc<NodeTaskHandle>> = hosts
            .into_iter()
            .map(|host| {
                let broker: Arc<str> = Arc::from(host);
                let handle = self.connections.remove(&broker).unwrap_or_else(|| {
                    tracing::info!("discovered broker {}", broker);
                    Arc::new(NodeTaskHandle::new(broker.clone(), self.config.clone()))
                });

                (broker.clone(), handle)
            })
            .collect();

        for (host, handle) in self.connections.drain() {
            tracing::info!(
                "closing connection to broker {} because it is no longer part of the cluster",
                host
            );
            handle.shutdown().await;
        }

        self.connections = new_connections;
        self.metadata.replace(metadata);

        tracing::debug!("successfully updated metadata using broker {}", broker);
    }

    pub async fn run(mut self) {
        let mut metadata_interval = tokio::time::interval(Duration::from_millis(500));

        loop {
            let either = tokio::select! {
                biased; // prefer metadata refresh
                left = metadata_interval.tick() => Either::Left(left),
                // don't process requests until we know the state of the cluster
                right = self.rx.recv(), if self.metadata.is_some() => Either::Right(right)
            };

            match either {
                Either::Left(_) => self.refresh_metadata().await,
                Either::Right(req) => match req {
                    Some(req) => {
                        macro_rules! abort_no_connections {
                            () => {{
                                tracing::error!("no connections available for request!");
                                let _ = req.tx.send(Err(PreparedConnectionError::Closed));
                                continue;
                            }};
                        }

                        let handle = match req.broker_id(self.metadata.as_ref().expect(
                            "Requests are not processed until metadata is fetched. This is a bug.",
                        )) {
                            // request needs specific broker
                            Ok(Some(broker_id)) => {
                                if let Some(handle) = self.get_handle(&broker_id) {
                                    handle
                                } else {
                                    abort_no_connections!()
                                }
                            }
                            // request can use any broker
                            Ok(None) => {
                                // TODO load balancing, work-stealing?
                                let Some((_, handle)) = self.random_handle() else {
                                    // our connections map is empty
                                    abort_no_connections!()
                                };

                                handle
                            }
                            _ => abort_no_connections!(),
                        };

                        tokio::spawn(async move {
                            // TODO abort if tx drops
                            let Some(conn) = handle.get_connection().await else {
                                // failed to connect (retries exhausted)
                                let _ = req.tx.send(Err(PreparedConnectionError::Closed));
                                return;
                            };

                            let _ = req.tx.send(conn.send(req.request).await);
                        });
                    }
                    None => break, // NetworkClient dropped
                },
            }
        }
    }
}
