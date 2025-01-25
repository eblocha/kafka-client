use anyhow::Context;

use kafka_client::clients::{consumer::Consumer, network::NetworkClient};

use crate::shutdown::shutdown_signal;

use super::Run;

pub struct EchoTopics {
    pub topics: Vec<String>,
}

impl EchoTopics {
    async fn run_inner(self, consumer: &mut Consumer) -> anyhow::Result<()> {
        consumer
            .subscribe(self.topics)
            .await
            .context("failed to subscribe to topics")?;

        while let Some(batch) = consumer.next().await {
            for set in batch.into_iter() {
                for record in set.records.into_iter() {
                    let Some(value) = record.value else {
                        continue;
                    };

                    let Ok(value) = std::str::from_utf8(&value) else {
                        println!("(invalid utf-8) {:?}", value);
                        continue;
                    };

                    println!("{}", value);
                }
            }
        }

        Ok(())
    }
}

impl Run for EchoTopics {
    type Response = ();

    async fn run(self, client: NetworkClient) -> anyhow::Result<Self::Response> {
        let mut consumer = Consumer::new(client);

        let result = tokio::select! {
            result = self.run_inner(&mut consumer) => result,
            _ = shutdown_signal() => Ok(())
        };

        consumer.shutdown().await;

        result
    }
}
