use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use futures::{ready, Stream};
use tokio::{sync::mpsc, time::Sleep};
use tokio_stream::StreamMap;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{Node, TopicPartition},
    conn::{broker::connector::NodeConnector, selector::RefreshMetadataRequest, Sendable},
    error::KafkaError,
    proto::ver::{FromVersionRange, GetApiKey},
};

pub struct PartitionQueue<M> {
    retry_buffer: VecDeque<(Option<Pin<Box<Sleep>>>, M)>,
    rx: mpsc::Receiver<M>,
}

impl<M> PartitionQueue<M> {
    pub fn new(rx: mpsc::Receiver<M>) -> Self {
        Self {
            retry_buffer: VecDeque::new(),
            rx,
        }
    }

    /// Queue a message for retry
    ///
    /// If `due` is [`None`], the message will not have a retry delay.
    pub fn retry(&mut self, message: M, due: Option<Instant>) {
        self.retry_buffer.push_front((
            due.map(|deadline| Box::pin(tokio::time::sleep_until(deadline.into()))),
            message,
        ));
    }

    pub fn close(&mut self) {
        self.rx.close();
    }
}

impl<M> Unpin for PartitionQueue<M> {}

impl<M> Stream for PartitionQueue<M> {
    type Item = M;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some((Some(sleep), _msg)) = this.retry_buffer.get_mut(0) {
            // If the next message has a deadline, make sure we have passed it before continuing.
            ready!(sleep.as_mut().poll(cx));
        }

        if let Some((_, msg)) = this.retry_buffer.pop_front() {
            return Poll::Ready(Some(msg));
        }

        this.rx.poll_recv(cx)
    }
}

pub type PartitionQueueMap<M> = StreamMap<TopicPartition, PartitionQueue<M>>;

#[derive(Debug, Clone)]
pub struct BrokerTaskContext {
    pub cancellation_token: CancellationToken,
    pub flush: CancellationToken,
    pub tx: mpsc::Sender<RefreshMetadataRequest>,
}

pub trait BrokerTask: Send + Sized + 'static {
    type PartitionMessage: Send;

    /// Run the task.
    ///
    /// This should return [`Some`] if it is eligible to be restarted, or [`None`] if the selector should not restart it.
    fn run(self, ctx: BrokerTaskContext) -> impl Future<Output = Option<Self>> + Send;

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

    fn new_task(&self, connector: NodeConnector<Conn>) -> (Self::Handle, Self::Task);
}
