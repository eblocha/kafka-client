use std::{path::PathBuf, time::Instant};

use tokio::{
    fs::File,
    io::{self, AsyncBufReadExt},
};

use kafka_client::clients::{
    network::NetworkClient,
    producer::{Producer, ProducerRecord},
};

use super::Run;

pub struct ProduceFromFile {
    pub topic: String,
    pub file: PathBuf,
}

impl Run for ProduceFromFile {
    type Response = ();

    async fn run(self, client: NetworkClient) -> anyhow::Result<Self::Response> {
        let file = File::open(self.file).await?;

        let mut reader = io::BufReader::new(file).lines();

        let mut producer = Producer::new(client);

        let now = Instant::now();
        let mut iter = 0;

        while let Some(line) = reader.next_line().await? {
            producer
                .send(ProducerRecord {
                    headers: Default::default(),
                    key: None,
                    partition: None,
                    timestamp: None,
                    topic: self.topic.clone(),
                    value: Some(line.into()),
                })
                .await?;

            iter += 1;
        }

        let finish = now.elapsed();

        println!("produced {iter} messages in {finish:?}");

        Ok(())
    }
}
