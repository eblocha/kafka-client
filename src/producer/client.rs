use crate::{
    common::BrokerHost,
    conn::{
        config::ConnectionManagerConfig,
        selector::{
            connect::{Connect, Tcp},
            SelectorTaskHandle,
        },
    },
    error::KafkaError,
    producer::{
        handle::{ProducerTaskFactory, ProducerTaskHandle},
        task::ProducerTask,
    },
};

pub struct Producer<Conn: Connect + Send + 'static> {
    selector: SelectorTaskHandle<ProducerTask<Conn>, ProducerTaskHandle>,
}

impl Producer<Tcp> {
    pub async fn try_new(
        bootstrap: &[BrokerHost],
        config: ConnectionManagerConfig,
    ) -> Result<Self, KafkaError> {
        let selector =
            SelectorTaskHandle::try_new_tcp(bootstrap, config, ProducerTaskFactory {}).await?;

        Ok(Self { selector })
    }
}
