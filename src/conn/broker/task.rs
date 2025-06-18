use std::{future::Future, pin::Pin};

use futures::Stream;
use tokio_stream::StreamMap;
use tokio_util::sync::CancellationToken;

use crate::{common::TopicPartition, conn::broker::connection_task::ConnectionTaskHandle};

pub type PartitionQueueMap<M> = StreamMap<TopicPartition, Pin<Box<dyn Stream<Item = M> + Send>>>;

pub trait BrokerTask {
    type PartitionMessage;

    /// Run the task.
    ///
    /// This must return itself to be able to reconfigure when a metadata refresh is received.
    fn run(self, cancellation_token: CancellationToken) -> impl Future<Output = Self> + Send;

    /// Get a mutable reference to the mapping of topic partition to a queue of messages for the partition.
    ///
    /// This mapping will be modified when a metadata refresh is received.
    fn get_partitions_mut(&mut self) -> &mut PartitionQueueMap<Self::PartitionMessage>;
}

pub trait BrokerTaskHandleFactory: Send + 'static {
    type Task: BrokerTask;

    fn new(&self, connection_handle: ConnectionTaskHandle) -> Self::Task;
}
