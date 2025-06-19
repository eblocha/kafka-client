use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use arc_swap::ArcSwapOption;
use tokio::sync::{mpsc, oneshot};

use crate::{
    cancel::OrCancelled,
    common::Node,
    conn::{
        broker::{
            connector::{NodeConnector, VersionedConnection},
            init_error::ConnectionInitError,
            task::{
                BrokerTask, BrokerTaskContext, BrokerTaskFactory, BrokerTaskHandle,
                PartitionQueueMap,
            },
        },
        selector::connect::Connect,
        Sendable,
    },
    error::KafkaError,
    proto::ver::{FromVersionRange, GetApiKey},
};

#[derive(Debug)]
pub struct ConnectionTaskMessage {
    pub tx: oneshot::Sender<Result<Arc<VersionedConnection>, ConnectionInitError>>,
}

pub struct ConnectionTask<Conn> {
    connector: NodeConnector<Conn>,
    rx: mpsc::Receiver<ConnectionTaskMessage>,
    partitions: PartitionQueueMap<()>,
}

impl<Conn: Connect + Send + 'static> BrokerTask for ConnectionTask<Conn> {
    type PartitionMessage = ();

    async fn run(mut self, ctx: BrokerTaskContext) -> Self {
        loop {
            let Some(Some(ConnectionTaskMessage { tx })) =
                self.rx.recv().or_cancel(&ctx.cancellation_token).await
            else {
                break;
            };

            let Some(conn) = self
                .connector
                .connect()
                .or_cancel(&ctx.cancellation_token)
                .await
            else {
                break;
            };

            let _ = tx.send(conn);
        }

        self
    }

    async fn shutdown(self) -> Self {
        let connector = self.connector.shutdown().await;

        Self {
            connector,
            rx: self.rx,
            partitions: self.partitions,
        }
    }

    fn get_partitions_mut(&mut self) -> &mut PartitionQueueMap<Self::PartitionMessage> {
        &mut self.partitions
    }

    fn get_node(&self) -> &Node {
        &self.connector.node
    }

    fn set_node(&mut self, node: Node) {
        self.connector.node = node;
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionTaskHandle {
    tx: mpsc::Sender<ConnectionTaskMessage>,
    connection: Arc<ArcSwapOption<VersionedConnection>>,
    in_flight: Arc<AtomicUsize>,
    failure_streak: Arc<AtomicUsize>,
}

impl ConnectionTaskHandle {
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
        Ok(conn.send_and_forget(req).await?)
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

        let msg = ConnectionTaskMessage { tx };

        self.tx.send(msg).await?;

        Ok(rx.await??)
    }
}

impl BrokerTaskHandle for ConnectionTaskHandle {
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

pub struct ConnectionTaskFactory;

impl<Conn: Connect + Send + 'static> BrokerTaskFactory<Conn> for ConnectionTaskFactory {
    type Task = ConnectionTask<Conn>;
    type Handle = ConnectionTaskHandle;

    fn new(&self, connector: NodeConnector<Conn>) -> (Self::Handle, Self::Task) {
        // We only need 1 slot because we are just waiting for a shared connection, not sending messages.
        let (tx, rx) = mpsc::channel(1);

        let handle = ConnectionTaskHandle {
            connection: connector.connection.clone(),
            tx,
            in_flight: Arc::new(AtomicUsize::new(0)),
            failure_streak: Arc::new(AtomicUsize::new(0)),
        };

        let task = ConnectionTask {
            rx,
            connector,
            partitions: PartitionQueueMap::default(),
        };

        (handle, task)
    }
}
