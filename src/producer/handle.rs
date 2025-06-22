use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{
    config::KafkaConfig,
    conn::{
        broker::{
            connector::NodeConnector,
            task::{BrokerTaskFactory, BrokerTaskHandle, PartitionQueueMap},
        },
        connect::Connect,
        Sendable,
    },
    error::KafkaError,
    network::handle::{NetworkTaskFactory, NetworkTaskHandle},
    producer::task::ProducerTask,
    proto::ver::{FromVersionRange, GetApiKey},
};

#[derive(Debug, Clone)]
pub struct ProducerTaskHandle {
    inner_handle: NetworkTaskHandle,
}

impl BrokerTaskHandle for ProducerTaskHandle {
    async fn send<R: Sendable + Send, F: FromVersionRange<Req = R> + GetApiKey + Send>(
        &self,
        req: F,
    ) -> Result<R::Response, KafkaError> {
        self.inner_handle.send(req).await
    }

    async fn send_and_forget<
        R: Sendable + Send,
        F: FromVersionRange<Req = R> + GetApiKey + Send,
    >(
        &self,
        req: F,
    ) -> Result<(), KafkaError> {
        self.inner_handle.send_and_forget(req).await
    }

    fn requests_in_flight(&self) -> usize {
        self.inner_handle.requests_in_flight()
    }

    fn connect_failure_streak(&self) -> usize {
        self.inner_handle.connect_failure_streak()
    }

    fn capacity(&self) -> Option<usize> {
        self.inner_handle.capacity()
    }

    fn is_closed(&self) -> bool {
        self.inner_handle.is_closed()
    }
}

pub(super) struct ProducerTaskFactory {
    pub config: KafkaConfig,
}

impl<Conn: Connect + Send + 'static> BrokerTaskFactory<Conn> for ProducerTaskFactory {
    type Task = ProducerTask<Conn>;
    type Handle = ProducerTaskHandle;

    fn new_task(&self, connector: NodeConnector<Conn>) -> (Self::Handle, Self::Task) {
        let (inner_handle, inner_task) = NetworkTaskFactory.new_task(connector);

        let handle = ProducerTaskHandle {
            inner_handle: inner_handle.clone(),
        };

        let task = ProducerTask {
            partitions: PartitionQueueMap::default(),
            inner_handle,
            inner_task,
            config: self.config.clone(),
        };

        (handle, task)
    }
}
