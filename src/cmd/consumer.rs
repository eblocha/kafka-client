use anyhow::Context;
use kafka_protocol::{messages::TopicName, protocol::StrBytes};

use crate::clients::{consumer::Consumer, network::NetworkClient};

use super::Run;

pub struct EchoTopics {
    pub topics: Vec<String>,
}

impl Run for EchoTopics {
    type Response = ();

    async fn run(self, client: NetworkClient) -> anyhow::Result<Self::Response> {
        let mut consumer = Consumer::new(client);

        consumer
            .subscribe(
                self.topics
                    .into_iter()
                    .map(|name| TopicName(StrBytes::from_string(name)))
                    .collect(),
            )
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

        consumer.shutdown().await;

        Ok(())
    }
}
