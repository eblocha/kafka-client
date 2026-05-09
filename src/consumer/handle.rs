use tokio::sync::mpsc;

use crate::{
    config::KafkaConfig,
    conn::{
        Sendable,
        broker::{
            connector::NodeConnector,
            task::{BrokerTaskFactory, BrokerTaskHandle},
        },
    },
    connect::Connect,
    consumer::{
        record::ConsumerRecordsResult,
        task::{ConsumerState, ConsumerTask},
    },
    error::KafkaError,
    network::handle::{NetworkTaskFactory, NetworkTaskHandle},
    proto::ver::{FromVersionRange, GetApiKey},
};

#[derive(Debug, Clone)]
pub struct ConsumerTaskHandle {
    inner_handle: NetworkTaskHandle,
}

impl BrokerTaskHandle for ConsumerTaskHandle {
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
}

pub(super) struct ConsumerTaskFactory {
    pub config: KafkaConfig,
    pub tx: mpsc::Sender<ConsumerRecordsResult>,
}

impl<Conn: Connect + Send + 'static> BrokerTaskFactory<Conn> for ConsumerTaskFactory {
    type Task = ConsumerTask<Conn>;
    type Handle = ConsumerTaskHandle;

    fn new_task(&self, connector: NodeConnector<Conn>) -> (Self::Handle, Self::Task) {
        let (inner_handle, inner_task) = NetworkTaskFactory.new_task(connector);

        let handle = ConsumerTaskHandle {
            inner_handle: inner_handle.clone(),
        };

        let task = ConsumerTask {
            partitions: ConsumerState::default(),
            inner_handle,
            inner_task,
            config: self.config.clone(),
            tx: self.tx.clone(),
        };

        (handle, task)
    }
}
