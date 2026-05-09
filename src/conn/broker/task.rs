use std::future::Future;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{Node, TopicPartition},
    config::KafkaConfig,
    conn::{
        Sendable,
        broker::connector::NodeConnector,
        selector::{ClusterMetadata, RefreshMetadataRequest},
    },
    error::KafkaError,
    proto::ver::{FromVersionRange, GetApiKey},
};

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

    pub fn child_context(&self) -> Self {
        Self {
            cancellation_token: self.cancellation_token.child_token(),
            flush: self.flush.child_token(),
            tx: self.tx.clone(),
        }
    }
}

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
}

pub trait BrokerTaskFactory<Conn>: Send + 'static {
    type Task: BrokerTask;
    type Handle: BrokerTaskHandle;

    fn new_task(&self, connector: NodeConnector<Conn>) -> (Self::Handle, Self::Task);
}
