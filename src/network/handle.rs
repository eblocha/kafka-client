use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use arc_swap::ArcSwapOption;
use tokio::sync::{mpsc, oneshot};

use crate::{
    conn::{
        broker::{
            connector::{NodeConnector, VersionedConnection},
            task::{BrokerTaskFactory, BrokerTaskHandle, PartitionQueueMap},
        },
        connect::Connect,
        Sendable,
    },
    error::KafkaError,
    network::task::{NetworkTask, NetworkTaskMessage},
    proto::ver::{FromVersionRange, GetApiKey},
};

#[derive(Debug, Clone)]
pub struct NetworkTaskHandle {
    tx: mpsc::Sender<NetworkTaskMessage>,
    connection: Arc<ArcSwapOption<VersionedConnection>>,
    in_flight: Arc<AtomicUsize>,
    failure_streak: Arc<AtomicUsize>,
}

impl NetworkTaskHandle {
    async fn send_inner<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<R::Response, KafkaError> {
        let conn = self.get_connection().await?;
        conn.send(req).await
    }

    async fn send_inner_and_forget<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<(), KafkaError> {
        let conn = self.get_connection().await?;
        conn.send_and_forget(req).await
    }

    async fn get_connection(&self) -> Result<Arc<VersionedConnection>, KafkaError> {
        let conn_result = self.get_connection_inner().await;

        if conn_result.is_err() {
            self.failure_streak.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failure_streak.store(0, Ordering::Relaxed);
        }

        conn_result
    }

    async fn get_connection_inner(&self) -> Result<Arc<VersionedConnection>, KafkaError> {
        if let Some(conn) = self
            .connection
            .load()
            .as_ref()
            .filter(|conn| !conn.is_closed())
        {
            return Ok(conn.clone());
        }

        let (tx, rx) = oneshot::channel();

        let msg = NetworkTaskMessage { tx };

        self.tx.send(msg).await?;

        Ok(rx.await??)
    }
}

impl BrokerTaskHandle for NetworkTaskHandle {
    async fn send<R: Sendable + Send, F: FromVersionRange<Req = R> + GetApiKey + Send>(
        &self,
        req: F,
    ) -> Result<R::Response, KafkaError> {
        self.in_flight.fetch_add(1, Ordering::Acquire);

        let result = self.send_inner(req).await;

        self.in_flight.fetch_sub(1, Ordering::Release);

        result
    }

    async fn send_and_forget<
        R: Sendable + Send,
        F: FromVersionRange<Req = R> + GetApiKey + Send,
    >(
        &self,
        req: F,
    ) -> Result<(), KafkaError> {
        self.in_flight.fetch_add(1, Ordering::Acquire);

        let result = self.send_inner_and_forget(req).await;

        self.in_flight.fetch_sub(1, Ordering::Release);

        result
    }

    fn requests_in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    fn connect_failure_streak(&self) -> usize {
        self.failure_streak.load(Ordering::Relaxed)
    }

    fn capacity(&self) -> Option<usize> {
        self.connection.load().as_ref().map(|conn| conn.capacity())
    }

    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

pub struct NetworkTaskFactory;

impl<Conn: Connect + Send + 'static> BrokerTaskFactory<Conn> for NetworkTaskFactory {
    type Task = NetworkTask<Conn>;
    type Handle = NetworkTaskHandle;

    fn new_task(&self, connector: NodeConnector<Conn>) -> (Self::Handle, Self::Task) {
        // We only need 1 slot because we are just waiting for a shared connection, not sending messages.
        let (tx, rx) = mpsc::channel(1);

        let handle = NetworkTaskHandle {
            connection: connector.connection.clone(),
            tx,
            in_flight: Arc::new(AtomicUsize::new(0)),
            failure_streak: Arc::new(AtomicUsize::new(0)),
        };

        let task = NetworkTask {
            rx,
            connector,
            partitions: PartitionQueueMap::default(),
        };

        (handle, task)
    }
}
