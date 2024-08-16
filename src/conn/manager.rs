use std::{collections::HashMap, io, sync::Arc, time::Duration};

use futures::{channel::oneshot, stream::FuturesUnordered, StreamExt, TryFutureExt};
use rand::Rng;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::{
    sync::{CancellationToken, DropGuard},
    task::{task_tracker::TaskTrackerWaitFuture, TaskTracker},
};

use super::{PreparedConnection, PreparedConnectionInitializationError};

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

/// Find the least loaded node to connect to
// fn least_loaded_node(connections: &HashMap<String, NodeTaskHandle>) -> Option<&str> {
//     // prefer connected node with least number of in-flight requests
//     let least_loaded_connected = connections
//         .iter()
//         .filter_map(|(node, handle)| {
//             let conn = handle.connection.clone();
//             conn.map(|c| (node, c.capacity()))
//                 .filter(|(_, cap)| *cap > 0)
//         })
//         .max_by(|(_, left), (_, right)| left.cmp(&right));

//     if let Some((node, _)) = least_loaded_connected {
//         return Some(node);
//     }

//     None

//     // choose the node with the least number of clients waiting to connect
//     // let least_loaded_disconnected = connections
//     //     .iter()
//     //     .filter_map(|(node, handle)| match conn {
//     //         MaybeConnected::Disconnected(disconnected) => Some((node, &disconnected.tx)),
//     //         MaybeConnected::Connecting(Connecting { tx, .. }) => Some((node, tx)),
//     //         _ => None,
//     //     })
//     //     .min_by(|(_, left), (_, right)| left.receiver_count().cmp(&right.receiver_count()));

//     // least_loaded_disconnected.map(|(node, _)| node.as_str())
// }

/// Number of requests for a connection to batch at the same time
const CONNECTION_REQ_BUFFER_SIZE: usize = 10;

struct NodeBackgroundTask {
    rx: mpsc::Receiver<oneshot::Sender<Arc<PreparedConnection>>>,
    broker: Arc<str>,
    connection: Option<Arc<PreparedConnection>>,
    cancellation_token: CancellationToken,
}

impl NodeBackgroundTask {
    async fn run(mut self) {
        let mut recv_buf = Vec::with_capacity(CONNECTION_REQ_BUFFER_SIZE);

        'outer: loop {
            let count = tokio::select! {
                count = self.rx.recv_many(&mut recv_buf, CONNECTION_REQ_BUFFER_SIZE) => count,
                _ = self.cancellation_token.cancelled() => break
            };

            if count == 0 {
                break;
            }

            if let Some(ref conn) = self.connection {
                if !conn.is_closed() {
                    for sender in recv_buf.drain(..) {
                        let _ = sender.send(conn.clone());
                    }
                    continue;
                }
            }

            let mut current_backoff;
            let mut current_retries = 0u32;

            let conn = loop {
                let config = super::KafkaConnectionConfig::default();

                tracing::info!("attempting to connect to broker {}", self.broker);

                let mut senders_dropped = FuturesUnordered::new();
                senders_dropped.extend(recv_buf.iter_mut().map(|s| s.cancellation()));

                let timeout = tokio::time::sleep(Duration::from_secs(5));

                macro_rules! abandon {
                    () => {{
                        tracing::info!("abandoning connection attempts to broker {} because all clients aborted", self.broker);
                        break None
                    }};
                }

                let res = tokio::select! {
                    res = TcpStream::connect(self.broker.as_ref())
                    .map_err(|e| {
                        PreparedConnectionInitializationError::Io(e)
                    })
                    .and_then(|io| PreparedConnection::connect(io, &config)) => res,
                    _ = self.cancellation_token.cancelled() => break 'outer,
                    _ = senders_dropped.count() => abandon!(),
                    _ = timeout => Err(PreparedConnectionInitializationError::Io(io::Error::from(io::ErrorKind::TimedOut)))
                };

                match res {
                    Ok(conn) => break Some(Arc::new(conn)),
                    Err(e) => {
                        let jitter = rand::thread_rng().gen_range(0..10);

                        current_backoff =
                            std::cmp::min(2 * 2u32.saturating_pow(current_retries), 30)
                                + 2 * jitter;
                        current_retries += 1;

                        tracing::warn!(
                            "failed to connect to broker at {}: {}, backing off for {}s with current retries: {}",
                            self.broker,
                            e,
                            current_backoff,
                            current_retries,
                        );

                        let mut senders_dropped = FuturesUnordered::new();
                        senders_dropped.extend(recv_buf.iter_mut().map(|s| s.cancellation()));

                        let sleep = tokio::time::sleep(Duration::from_secs(current_backoff.into()));

                        tokio::select! {
                            _ = sleep => {},
                            _ = senders_dropped.count() => abandon!(),
                        }
                    }
                }
            };

            tracing::info!("connected to broker {}", self.broker);

            if let Some(ref conn) = conn {
                for sender in recv_buf.drain(..) {
                    let _ = sender.send(conn.clone());
                }
            } else {
                recv_buf.clear();
            }

            self.connection = conn;
        }

        if let Some(conn) = self.connection.take() {
            conn.shutdown().await;
        }
    }
}

pub struct NodeTaskHandle {
    tx: mpsc::Sender<oneshot::Sender<Arc<PreparedConnection>>>,
    broker: Arc<str>,
    task_tracker: TaskTracker,
    // task_handle: JoinHandle<()>,
    cancellation_token: CancellationToken,
    _cancel_on_drop: DropGuard,
}

impl NodeTaskHandle {
    pub fn new(broker: Arc<str>) -> Self {
        let (tx, rx) = mpsc::channel(CONNECTION_REQ_BUFFER_SIZE);

        let cancellation_token = CancellationToken::new();

        let task = NodeBackgroundTask {
            broker: broker.clone(),
            rx,
            connection: Default::default(),
            cancellation_token: cancellation_token.clone(),
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

pub struct ConnectionManager {
    connections: HashMap<String, NodeTaskHandle>,
}

impl ConnectionManager {
    pub fn new(brokers: Vec<String>) -> Self {
        Self {
            connections: brokers
                .into_iter()
                .map(|broker| (broker.clone(), NodeTaskHandle::new(Arc::from(broker))))
                .collect(),
        }
    }

    pub fn handle(&self, node: &str) -> Option<&NodeTaskHandle> {
        self.connections.get(node)
    }

    pub async fn get_connection(&self, node: &str) -> Option<Arc<PreparedConnection>> {
        let Some(handle) = self.connections.get(node) else {
            return None;
        };

        handle.get_connection().await
    }
}
