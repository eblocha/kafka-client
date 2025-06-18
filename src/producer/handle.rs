use tokio_stream::StreamMap;

use crate::{
    conn::broker::{connection_task::ConnectionTaskHandle, task::BrokerTaskHandleFactory},
    producer::task::ProducerTask,
};

pub struct ProducerTaskHandleFactory {
    // TODO config goes here
}

impl BrokerTaskHandleFactory for ProducerTaskHandleFactory {
    type Task = ProducerTask;

    fn new(&self, connection_handle: ConnectionTaskHandle) -> Self::Task {
        ProducerTask {
            connection_task: connection_handle,
            partitions: StreamMap::default(),
        }
    }
}
