use anyhow::Context;
use kafka_client::{
    common::BrokerHost,
    config::KafkaConfig,
    connect::Tcp,
    consumer::{client::Consumer, subscription::Subscription},
};

use crate::shutdown::shutdown_signal;

use super::Run;

pub struct EchoTopics {
    pub topics: Vec<String>,
}

impl EchoTopics {
    async fn run_inner(self, mut consumer: Consumer<Tcp>) -> anyhow::Result<()> {
        for topic in self.topics {
            consumer
                .subscribe(Subscription::Topic(topic))
                .await
                .context("failed to subscribe to topics")?;
        }

        let signal = shutdown_signal();
        tokio::pin!(signal);

        loop {
            let result = tokio::select! {
                biased;
                _ = &mut signal => {
                    break;
                },
                res = consumer.recv() => res
            };

            let records = result?;

            for set in records {
                for record in set.records {
                    let Some(value) = record.value else {
                        continue;
                    };

                    let Ok(value) = std::str::from_utf8(&value) else {
                        println!("(invalid utf-8) {value:?}");
                        continue;
                    };

                    println!("{value}");
                }
            }
        }

        consumer.shutdown().await;

        Ok(())
    }
}

impl Run for EchoTopics {
    type Response = ();

    async fn run(
        self,
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
    ) -> anyhow::Result<Self::Response> {
        let consumer = Consumer::try_new(bootstrap, config).await?;
        self.run_inner(consumer).await
    }
}
