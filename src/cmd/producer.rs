use std::{path::PathBuf, time::Instant};

use anyhow::anyhow;
use bytes::Bytes;
use clap::Subcommand;
use indicatif::{HumanCount, ProgressBar, ProgressStyle};
use kafka_protocol::{messages::TopicName, protocol::StrBytes};
use rand::Rng;
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

#[derive(Subcommand)]
pub enum ProducerCommands {
    Random {
        #[arg(short, long)]
        topic: String,
    },
    File {
        #[arg(short, long)]
        file: PathBuf,
        #[arg(short, long)]
        topic: String,
    },
}

impl Run for ProducerCommands {
    type Response = ();

    async fn run(self, client: NetworkClient) -> anyhow::Result<()> {
        match self {
            ProducerCommands::Random { topic } => ProduceRandom { topic }.run(client).await,
            ProducerCommands::File { file, topic } => {
                ProduceFromFile { file, topic }.run(client).await
            }
        }
    }
}

pub struct ProduceRandom {
    topic: String,
}

const SIZE: i32 = 800_000;

impl Run for ProduceRandom {
    type Response = ();

    async fn run(self, client: NetworkClient) -> anyhow::Result<Self::Response> {
        let producer = Producer::new(client);
        let topic = TopicName(StrBytes::from_string(self.topic));

        let now = Instant::now();

        let bar = ProgressBar::new(SIZE as u64);
        bar.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg} {per_sec}",
            )
            .unwrap()
            .progress_chars("##-"),
        );

        let mut rng = rand::thread_rng();

        for _i in 0..SIZE {
            let msg = rng.gen::<[u8; 32]>();

            let msg = Bytes::from(msg.to_vec());

            producer
                .send(ProducerRecord {
                    headers: Default::default(),
                    key: None,
                    partition: None,
                    timestamp: None,
                    topic: topic.clone(),
                    value: Some(msg),
                })
                .await?;

            bar.inc(1);
        }

        producer.flush_and_shutdown().await;

        bar.finish();

        let finish = now.elapsed();

        println!(
            "produced {} messages in {finish:?}",
            HumanCount(SIZE as u64)
        );

        Ok(())
    }
}

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

        let topic = TopicName(StrBytes::from_string(topic));

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
