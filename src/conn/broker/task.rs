use std::future::Future;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    common::Node,
    config::KafkaConfig,
    conn::{
        Sendable,
        broker::{connector::NodeConnector, partition_queue::PartitionQueueMap},
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
    type PartitionMessage: Send;

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

    /// Get a mutable reference to the mapping of topic partition to a queue of messages for the partition.
    ///
    /// This mapping will be modified when a metadata refresh is received.
    fn get_partitions_mut(&mut self) -> &mut PartitionQueueMap<Self::PartitionMessage>;

    fn get_node(&self) -> &Node;
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
