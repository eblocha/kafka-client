use std::{path::PathBuf, time::Instant};

use anyhow::anyhow;
use tokio::{
    fs::File,
    io::{self, AsyncBufReadExt},
    task::JoinSet,
};

use kafka_client::{
    clients::{
        network::NetworkClient,
        producer::{Producer, ProducerRecord, RecordMetadata},
    },
    error::KafkaError,
};

use super::Run;

pub struct ProduceFromFile {
    pub topic: String,
    pub file: PathBuf,
}

impl ProduceFromFile {
    async fn run_inner(
        producer: &mut Producer,
        file: File,
        topic: String,
    ) -> anyhow::Result<JoinSet<Result<RecordMetadata, KafkaError>>> {
        let mut reader = io::BufReader::new(file).lines();

        let mut join_set = JoinSet::new();

        while let Some(line) = reader.next_line().await? {
            let rx = producer
                .send(ProducerRecord {
                    headers: Default::default(),
                    key: None,
                    partition: None,
                    timestamp: None,
                    topic: topic.clone(),
                    value: Some(line.into()),
                })
                .await?;

            join_set.spawn(rx);
        }

        // while join_set.join_next().await.is_some() {}

        Ok(join_set)
    }
}

impl Run for ProduceFromFile {
    type Response = ();

    async fn run(self, client: NetworkClient) -> anyhow::Result<Self::Response> {
        let file = File::open(self.file).await?;
        let mut producer = Producer::new(client);

        let now = Instant::now();

        let result = Self::run_inner(&mut producer, file, self.topic).await;

        producer.flush_and_shutdown().await;

        let finish = now.elapsed();

        match result {
            Ok(mut join_set) => {
                println!("produced {} messages in {finish:?}", join_set.len());

                let mut some_failed = false;

                while let Some(res) = join_set.join_next().await {
                    if let Ok(Err(e)) = res {
                        tracing::error!("failed to produce: {e}");
                        some_failed = true;
                    }
                }

                if some_failed {
                    Err(anyhow!("failed to produce all records"))
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(e),
        }
    }
}
