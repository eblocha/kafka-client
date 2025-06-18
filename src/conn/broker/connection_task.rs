use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use arc_swap::ArcSwapOption;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    cancel::OrCancelled,
    conn::{
        broker::connector::{NodeConnector, VersionedConnection},
        selector::{connect::Connect, ConnectionInitError},
        Sendable,
    },
    error::KafkaError,
    proto::ver::{FromVersionRange, GetApiKey},
};

#[derive(Debug)]
pub struct ConnectionTaskMessage {
    tx: oneshot::Sender<Result<Arc<VersionedConnection>, ConnectionInitError>>,
}

#[derive(Debug)]
pub struct ConnectionTask<Conn> {
    pub cancellation_token: CancellationToken,
    pub rx: mpsc::Receiver<ConnectionTaskMessage>,
    pub connector: NodeConnector<Conn>,
}

impl<Conn: Connect + Send + 'static> ConnectionTask<Conn> {
    pub async fn run(mut self) -> Self {
        loop {
            let Some(Some(ConnectionTaskMessage { tx })) =
                self.rx.recv().or_cancel(&self.cancellation_token).await
            else {
                break;
            };

            let Some(conn) = self
                .connector
                .connect()
                .or_cancel(&self.cancellation_token)
                .await
            else {
                break;
            };

            let _ = tx.send(conn);
        }

        self
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionTaskHandle {
    pub(super) tx: mpsc::Sender<ConnectionTaskMessage>,
    pub(super) cancellation_token: CancellationToken,
    connection: Arc<ArcSwapOption<VersionedConnection>>,
    in_flight: Arc<AtomicUsize>,
    failure_streak: Arc<AtomicUsize>,
}

impl ConnectionTaskHandle {
    /// Send a request to the broker and wait for a response.
    pub async fn send<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<R::Response, KafkaError> {
        self.in_flight.fetch_add(1, Ordering::Acquire);

        let result = self.send_inner(req).await;

        self.in_flight.fetch_sub(1, Ordering::Release);

        result
    }

    /// Sends a request and returns a future that resolves when the message is sent.
    pub async fn send_and_forget<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<(), KafkaError> {
        self.in_flight.fetch_add(1, Ordering::Acquire);

        let result = self.send_inner_and_forget(req).await;

        self.in_flight.fetch_sub(1, Ordering::Release);

        result
    }

    async fn send_inner<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<R::Response, KafkaError> {
        let conn = self.get_connection_with_failure_count().await?;
        conn.send(req).await
    }

    async fn send_inner_and_forget<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<(), KafkaError> {
        let conn = self.get_connection_with_failure_count().await?;
        Ok(conn.send_and_forget(req).await?)
    }

    async fn get_connection_with_failure_count(
        &self,
    ) -> Result<Arc<VersionedConnection>, KafkaError> {
        let conn_result = self.get_connection().await;

        if conn_result.is_err() {
            self.failure_streak.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failure_streak.store(0, Ordering::Relaxed);
        }

        conn_result
    }

    async fn get_connection(&self) -> Result<Arc<VersionedConnection>, KafkaError> {
        if let Some(conn) = self
            .connection
            .load()
            .as_ref()
            .filter(|conn| !conn.is_closed())
        {
            return Ok(conn.clone());
        }

        let (tx, rx) = oneshot::channel();

        let msg = ConnectionTaskMessage { tx };

        self.tx.send(msg).await?;

        Ok(rx.await??)
    }

    /// Determine the number of in-flight requests to this broker
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Determine the number of failed connection attempts to this broker in a row
    pub fn failure_streak(&self) -> usize {
        self.failure_streak.load(Ordering::Relaxed)
    }

    /// Determine the capacity of the connection send buffer if connected.
    ///
    /// If the node is not connected, this will return None.
    pub fn capacity(&self) -> Option<usize> {
        self.connection.load().as_ref().map(|conn| conn.capacity())
    }
}

pub fn new_pair<Conn>(
    connector: NodeConnector<Conn>,
) -> (ConnectionTaskHandle, ConnectionTask<Conn>) {
    // We only need 1 slot because we are just waiting for a shared connection, not sending messages.
    let (tx, rx) = mpsc::channel(1);

    let connection = Arc::new(ArcSwapOption::empty());

    let handle = ConnectionTaskHandle {
        cancellation_token: CancellationToken::new(),
        connection,
        tx,
        in_flight: Arc::new(AtomicUsize::new(0)),
        failure_streak: Arc::new(AtomicUsize::new(0)),
    };

    let task = ConnectionTask {
        cancellation_token: handle.cancellation_token.clone(),
        rx,
        connector,
    };

    (handle, task)
}
