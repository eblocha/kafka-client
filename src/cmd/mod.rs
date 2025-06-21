use kafka_client::{common::BrokerHost, config::KafkaConfig};

pub mod admin;
// pub mod consumer;
pub mod producer;

pub trait Run {
    type Response;

    async fn run(
        self,
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
    ) -> anyhow::Result<Self::Response>;
}
