use std::future::Future;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    broker::connector::NodeConnector,
    common::{Node, TopicPartition},
    config::KafkaConfig,
    conn::Sendable,
    error::KafkaError,
    proto::ver::{FromVersionRange, GetApiKey},
    selector::{ClusterMetadata, RefreshMetadataRequest},
};

/// Allows a task to communicate with the task orchestrator.
///
/// This includes cancellation tokens, which is how the orchestrator stops the task.
/// The Sender can request a metadata refresh from the orchestrator.
#[derive(Debug, Clone)]
pub struct BrokerTaskContext {
    pub cancellation_token: CancellationToken,
    pub flush: CancellationToken,
    pub tx: mpsc::Sender<RefreshMetadataRequest>,
}

impl BrokerTaskContext {
    /// Initialize a new context
    pub fn init(config: &KafkaConfig) -> (Self, mpsc::Receiver<RefreshMetadataRequest>) {
        let (tx, rx) = mpsc::channel(config.metadata.refresh_batch_count);

        (
            Self {
                cancellation_token: CancellationToken::new(),
                flush: CancellationToken::new(),
                tx,
            },
            rx,
        )
    }

    /// Create a child context
    pub fn child_context(&self) -> Self {
        Self {
            cancellation_token: self.cancellation_token.child_token(),
            flush: self.flush.child_token(),
            tx: self.tx.clone(),
        }
    }
}

/// A background task assigned to a unique broker id.
///
/// This performs network requests to a specific broker.
/// It also gets assigned the partitions led by its broker, and defines state associated with each partition.
pub trait BrokerTask: Send + Sized + 'static {
    /// The internal state of the partition for use by the task.
    /// This state will persist if the partition is moved to a different broker.
    type PartitionState: Send;

    /// The public interface for clients to interact with a particular partition assigned to this broker.
    /// A copy of this is kept in the selector task to be accessible to clients.
    type PublicPartitionState: Send + Sync + Clone;

    /// Run the task.
    ///
    /// This should return [`Some`] if it is eligible to be restarted, or [`None`] if the selector should not restart it.
    fn run(
        self,
        ctx: BrokerTaskContext,
        cluster: ClusterMetadata,
    ) -> impl Future<Output = Option<Self>> + Send;

    /// Stop the connection
    fn shutdown(self) -> impl Future<Output = Self> + Send;

    /// Assign an existing partition to this broker
    fn assign(&mut self, topic_partition: TopicPartition, state: Self::PartitionState);

    /// Assign a new partition to this broker.
    ///
    /// The task should handle construction and assignment of the inner state data.
    fn assign_new(&mut self, topic_partition: TopicPartition) -> Self::PublicPartitionState;

    /// Remove an assignment from this broker.
    ///
    /// Returns [`None`] if the broker is not assigned the partition.
    fn revoke(&mut self, topic_partition: &TopicPartition) -> Option<Self::PartitionState>;

    /// Get all of the assignments for the broker.
    fn get_assignments(&self) -> Vec<TopicPartition>;

    /// Get the broker's node information
    fn get_node(&self) -> &Node;

    /// Get a mutable reference to the broker's node information
    fn get_node_mut(&mut self) -> &mut Node;
}

/// A handle to a [`BrokerTask`].
///
/// This allows a client to send a request to the specific broker, or query the percieved health of the broker.
pub trait BrokerTaskHandle: Clone + Send + Sync + 'static {
    /// Send a request to the broker.
    fn send<R: Sendable + Send, F: FromVersionRange<Req = R> + GetApiKey + Send>(
        &self,
        req: F,
    ) -> impl Future<Output = Result<R::Response, KafkaError>> + Send;

    /// Send a request to the broker, and do not expect a response.
    ///
    /// This will resolve the future when the request is flushed in the socket.
    fn send_and_forget<R: Sendable + Send, F: FromVersionRange<Req = R> + GetApiKey + Send>(
        &self,
        req: F,
    ) -> impl Future<Output = Result<(), KafkaError>> + Send;

    /// Query for the number of in-flight requests to this broker.
    fn requests_in_flight(&self) -> usize;

    /// Query for the number of repeated failures when attempting to _connect_ to this broker.
    fn connect_failure_streak(&self) -> usize;

    /// Query for the available space to queue more messages to this broker.
    ///
    /// This will return [`None`] if the task is not connected.
    fn capacity(&self) -> Option<usize>;
}

/// Creates new [`BrokerTaskHandle`] and [`BrokerTask`] pairs.
///
/// This is used by the task orchestrator to connect to newly-discovered brokers in the cluster, or to revive a task
/// that stopped and could not recover its state.
pub trait BrokerTaskFactory<Conn>: Send + 'static {
    type Task: BrokerTask;
    type Handle: BrokerTaskHandle;

    /// Create a new [`BrokerTaskHandle`] and [`BrokerTask`] pair.
    fn new_task(&self, connector: NodeConnector<Conn>) -> (Self::Handle, Self::Task);
}
