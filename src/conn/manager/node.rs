//! Maintains a connection to a broker node by host
//!
//! This spawns a background task which can respond to requests for a connection to the node.
//!
//! If the connection is not alive when requested, it will be bootstrapped.
//!
//! Clients may also request a connection, then abort the request to cancel connecting, to enable racing connections to
//! multiple brokers.

use std::{future::Future, io, sync::Arc};

use futures::{stream::FuturesUnordered, StreamExt, TryFutureExt};
use rand::Rng;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::{
    sync::{CancellationToken, DropGuard},
    task::TaskTracker,
};

use crate::conn::{config::ConnectionConfig, PreparedConnection, PreparedConnectionInitError};

use super::host::BrokerHost;

// TODO config?
/// Number of requests for a connection to batch at the same time.
const CONNECTION_REQ_BUFFER_SIZE: usize = 10;

/// Requirement for the connection
enum ConnectionRequestKind {
    /// The connection must be alive
    Connected,
    /// Allows bootstrapping a new connection
    Any,
}

/// A request for a connection. This may trigger a new connection if the connection is dead and `kind` allows it.
struct ConnectionRequest {
    kind: ConnectionRequestKind,
    tx: oneshot::Sender<Arc<PreparedConnection>>,
}

/// Actor that is waiting for connection requests. Holds a connection internally for reuse, or will bootstrap a new
/// connection if allowed.
struct NodeBackgroundTask {
    rx: mpsc::Receiver<ConnectionRequest>,
    broker: BrokerHost,
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
                    for connection_request in recv_buf.drain(..) {
                        let _ = connection_request.tx.send(conn.clone());
                    }
                    continue;
                }
            }

            // drop any senders that require an active connection
            recv_buf.retain(|req| matches!(req.kind, ConnectionRequestKind::Any));

            if recv_buf.is_empty() {
                continue;
            }

            let mut current_backoff;
            let mut current_retries = 0u32;

            let conn = loop {
                tracing::info!(broker = ?self.broker, "attempting to connect");

                let senders_dropped =
                    FuturesUnordered::from_iter(recv_buf.iter_mut().map(|req| req.tx.closed()));

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

                let connect_fut = TcpStream::connect((self.broker.0.as_ref(), self.broker.1))
                    .map_err(PreparedConnectionInitError::Io)
                    .and_then(|io| PreparedConnection::connect(io, &self.config.io));

                let res = tokio::select! {
                    biased;
                    // cancel reasons
                    _ = self.cancellation_token.cancelled() => break 'outer,
                    _ = senders_dropped.count() => abandon!(),
                    _ = timeout => Err(PreparedConnectionInitError::Io(io::ErrorKind::TimedOut.into())),

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
                        senders_dropped.extend(recv_buf.iter_mut().map(|req| req.tx.closed()));

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
                for req in recv_buf.drain(..) {
                    let _ = req.tx.send(conn.clone());
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

/// A handle to send requests to the [`NodeBackgroundTask`].
#[derive(Debug)]
pub struct NodeTaskHandle {
    tx: mpsc::Sender<ConnectionRequest>,
    broker: BrokerHost,
    task_tracker: TaskTracker,
    task_handle: JoinHandle<()>,
    cancellation_token: CancellationToken,
    _cancel_on_drop: DropGuard,
}

impl NodeTaskHandle {
    pub fn new(broker: BrokerHost, config: ConnectionConfig) -> Self {
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

        let task_handle = task_tracker.spawn(task.run());
        task_tracker.close();

        Self {
            tx,
            broker,
            task_tracker,
            task_handle,
            cancellation_token: cancellation_token.clone(),
            _cancel_on_drop: cancellation_token.drop_guard(),
        }
    }

    pub async fn get_connection(&self) -> Option<Arc<PreparedConnection>> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(ConnectionRequest {
                kind: ConnectionRequestKind::Any,
                tx,
            })
            .await
            .ok()?;

        rx.await.ok()
    }

    /// Get a connection if the node is currently connected
    pub async fn get_if_connected(&self) -> Option<Arc<PreparedConnection>> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(ConnectionRequest {
                kind: ConnectionRequestKind::Connected,
                tx,
            })
            .await
            .ok()?;

        rx.await.ok()
    }

    /// Returns true if this task has stopped
    pub fn is_closed(&self) -> bool {
        self.task_handle.is_finished()
    }

    /// Shut down the connection task, closing the connection if active.
    ///
    /// Returns a future to await final shutdown.
    pub fn shutdown(&self) -> impl Future<Output = ()> + '_ {
        tracing::info!(broker = ?self.broker, "shutting down connection handle");
        self.cancellation_token.cancel();
        self.task_tracker.wait()
    }
}
