use crate::{
    conn::{
        broker::{
            connection_task::{ConnectionTaskFactory, ConnectionTaskHandle},
            connector::NodeConnector,
            task::{BrokerTaskFactory, BrokerTaskHandle, PartitionQueueMap},
        },
        selector::connect::Connect,
        Sendable,
    },
    error::KafkaError,
    producer::task::ProducerTask,
    proto::ver::{FromVersionRange, GetApiKey},
};

pub struct ProducerTaskFactory {
    // TODO config goes here
}

#[derive(Debug, Clone)]
pub struct ProducerTaskHandle {
    connection_handle: ConnectionTaskHandle,
}

impl BrokerTaskHandle for ProducerTaskHandle {
    async fn send<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<R::Response, KafkaError> {
        self.connection_handle.send(req).await
    }

    async fn send_and_forget<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<(), KafkaError> {
        self.connection_handle.send_and_forget(req).await
    }

    fn requests_in_flight(&self) -> usize {
        self.connection_handle.requests_in_flight()
    }

    fn connect_failure_streak(&self) -> usize {
        self.connection_handle.connect_failure_streak()
    }

    fn capacity(&self) -> Option<usize> {
        self.connection_handle.capacity()
    }

    fn is_closed(&self) -> bool {
        self.connection_handle.is_closed()
    }
}

impl<Conn: Connect + Send + 'static> BrokerTaskFactory<Conn> for ProducerTaskHandle {
    type Task = ProducerTask<Conn>;
    type Handle = ProducerTaskHandle;

    fn new(&self, connector: NodeConnector<Conn>) -> (Self::Handle, Self::Task) {
        let (connection_handle, connection_task) = ConnectionTaskFactory.new(connector);

        let handle = Self {
            connection_handle: connection_handle.clone(),
        };

        let task = ProducerTask {
            partitions: PartitionQueueMap::default(),
            connection_handle,
            connection_task,
        };

        (handle, task)
    }
}
