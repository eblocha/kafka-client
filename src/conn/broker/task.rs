use std::{future::Future, sync::Arc};

use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::ReceiverStream, StreamMap};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{Node, TopicPartition},
    conn::{
        broker::{
            connector::{NodeConnector, VersionedConnection},
            init_error::ConnectionInitError,
        },
        Sendable,
    },
    error::KafkaError,
    proto::ver::{FromVersionRange, GetApiKey},
};

pub type PartitionQueue<M> = ReceiverStream<M>;
pub type PartitionQueueMap<M> = StreamMap<TopicPartition, PartitionQueue<M>>;

/// Create a new [`PartitionQueue`] from an [`mpsc::Receiver`].
pub fn into_partition_queue<M: Send + 'static>(rx: mpsc::Receiver<M>) -> PartitionQueue<M> {
    ReceiverStream::new(rx)
}

#[derive(Debug)]
pub struct BrokerTaskMessage {
    pub tx: oneshot::Sender<Result<Arc<VersionedConnection>, ConnectionInitError>>,
}

#[derive(Debug, Clone)]
pub struct BrokerTaskContext {
    pub cancellation_token: CancellationToken,
}

pub trait BrokerTask: Send + 'static {
    type PartitionMessage: Send;

    /// Run the task.
    ///
    /// This must return itself to be able to reconfigure when a metadata refresh is received.
    fn run(self, ctx: BrokerTaskContext) -> impl Future<Output = Self> + Send;

    /// Stop the connection
    fn shutdown(self) -> impl Future<Output = Self> + Send;

    /// Get a mutable reference to the mapping of topic partition to a queue of messages for the partition.
    ///
    /// This mapping will be modified when a metadata refresh is received.
    fn get_partitions_mut(&mut self) -> &mut PartitionQueueMap<Self::PartitionMessage>;

    fn get_node(&self) -> &Node;

    fn set_node(&mut self, node: Node);
}

pub trait BrokerTaskHandle: Clone + Send + Sync + 'static {
    fn send<R: Sendable + Send, F: FromVersionRange<Req = R> + GetApiKey + Send>(
        &self,
        req: F,
    ) -> impl Future<Output = Result<R::Response, KafkaError>> + Send;

    fn send_and_forget<R: Sendable + Send, F: FromVersionRange<Req = R> + GetApiKey + Send>(
        &self,
        req: F,
    ) -> impl Future<Output = Result<(), KafkaError>> + Send;

    fn requests_in_flight(&self) -> usize;

    fn connect_failure_streak(&self) -> usize;

    fn capacity(&self) -> Option<usize>;

    fn is_closed(&self) -> bool;
}

pub trait BrokerTaskFactory<Conn>: Send + 'static {
    type Task: BrokerTask;
    type Handle: BrokerTaskHandle;

    fn new(&self, connector: NodeConnector<Conn>) -> (Self::Handle, Self::Task);
}
