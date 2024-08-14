use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use futures::{channel::oneshot, TryFutureExt};
use rand::Rng;
use thiserror::Error;
use tokio::net::TcpStream;

use crate::conn::{
    KafkaConnectionConfig, KafkaConnectionError, PreparedConnection,
    PreparedConnectionInitializationError,
};

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("the connection request ran out of retries")]
    Cancelled,
}

pub enum ConnectionStatus {
    Connected(Arc<PreparedConnection>),
    Connecting(Vec<oneshot::Sender<Result<Arc<PreparedConnection>, ConnectionError>>>),
}

pub struct ConnectionManager {
    connections: Arc<DashMap<String, ConnectionStatus>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
        }
    }

    /// get an active Connection from a broker address
    ///
    /// creates a connection if not available
    pub async fn get_connection(
        &self,
        broker: &str,
    ) -> Result<Arc<PreparedConnection>, ConnectionError> {
        let rx = {
            match self.connections.get_mut(broker) {
                // no connection was ever created for the broker
                None => None,
                Some(mut value) => match value.value_mut() {
                    // there is a connection for the broker
                    ConnectionStatus::Connected(conn) => {
                        if conn.is_closed() {
                            // it's closed, so get a new one.
                            None
                        } else {
                            return Ok(conn.clone());
                        }
                    }
                    ConnectionStatus::Connecting(ref mut v) => {
                        let (tx, rx) = oneshot::channel();
                        v.push(tx);
                        Some(rx)
                    }
                },
            }
        };

        match rx {
            // we need a new connection
            None => self.connect(broker).await,
            Some(rx) => match rx.await {
                Ok(res) => res,
                Err(_) => Err(ConnectionError::Cancelled),
            },
        }
    }

    async fn connect_inner(
        &self,
        broker: &str,
    ) -> Result<Arc<PreparedConnection>, ConnectionError> {
        let rx = {
            match self
                .connections
                .entry(broker.to_owned())
                .or_insert_with(|| ConnectionStatus::Connecting(Vec::new()))
                .value_mut()
            {
                ConnectionStatus::Connected(_) => None,
                ConnectionStatus::Connecting(ref mut v) => {
                    if v.is_empty() {
                        None
                    } else {
                        let (tx, rx) = oneshot::channel();
                        v.push(tx);
                        Some(rx)
                    }
                }
            }
        };

        if let Some(rx) = rx {
            return match rx.await {
                Ok(res) => res,
                Err(_) => Err(ConnectionError::Cancelled),
            };
        }

        let mut current_backoff;
        let mut current_retries = 0u32;

        let conn = loop {
            let config = KafkaConnectionConfig::default();

            let res = TcpStream::connect(broker)
                .map_err(|e| {
                    PreparedConnectionInitializationError::Client(KafkaConnectionError::Io(e))
                })
                .and_then(|io| PreparedConnection::connect(io, &config))
                .await;

            match res {
                Ok(conn) => break conn,
                Err(_) => {
                    let jitter = rand::thread_rng().gen_range(0..10);

                    current_backoff =
                        std::cmp::min(2 * 2u32.saturating_pow(current_retries), 30) + 2 * jitter;
                    current_retries += 1;

                    tokio::time::sleep(Duration::from_secs(current_backoff.into())).await;
                }
            }
        };

        Ok(Arc::new(conn))
    }

    async fn connect(&self, broker: &str) -> Result<Arc<PreparedConnection>, ConnectionError> {
        let c = match self.connect_inner(broker).await {
            Ok(c) => c,
            Err(e) => {
                if let Some((_, ConnectionStatus::Connecting(mut v))) =
                    self.connections.remove(broker)
                {
                    for tx in v.drain(..) {
                        let _ = tx.send(Err(ConnectionError::Cancelled));
                    }
                }

                return Err(e);
            }
        };

        let old = self
            .connections
            .insert(broker.to_owned(), ConnectionStatus::Connected(c.clone()));

        match old {
            Some(ConnectionStatus::Connecting(mut v)) => {
                for tx in v.drain(..) {
                    let _ = tx.send(Ok(c.clone()));
                }
            }
            _ => {}
        };

        Ok(c)
    }
}
