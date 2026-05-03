use std::{path::PathBuf, time::Instant};

use anyhow::anyhow;
use bytes::Bytes;
use clap::Subcommand;
use indexmap::IndexMap;
use indicatif::{HumanCount, ProgressBar, ProgressStyle};
use kafka_protocol::{messages::TopicName, protocol::StrBytes};
use rand::RngCore;
use tokio::{
    fs::File,
    io::{self, AsyncBufReadExt},
    task::JoinSet,
};

use kafka_client::{
    common::BrokerHost,
    config::KafkaConfig,
    connect::Tcp,
    error::KafkaError,
    producer::{
        client::Producer,
        partitioner::KeyHashPartitioner,
        record::{ProducerRecord, RecordMetadata},
    },
};

use super::Run;

#[derive(Subcommand)]
pub enum ProducerCommands {
    /// Send randomly generated data to a topic and report timing.
    ///
    /// This will not wait for responses or verify the data was sent properly.
    Random {
        /// The topic to send random data to
        #[arg(short, long, required = true)]
        topic: String,
        /// Number of records to send
        #[arg(short, long, default_value_t = 10_000_000)]
        count: u64,
        /// Number of bytes to generate for each record value
        #[arg(short, long, default_value_t = 100)]
        size: usize,
    },
    /// Send the contents of a file to a topic, sending each line as a separate record.
    ///
    /// This command will exit with an error if the send fails.
    File {
        /// The file to send
        #[arg(short, long, required = true)]
        file: PathBuf,
        /// The topic to send to
        #[arg(short, long, required = true)]
        topic: String,
    },
}

impl Run for ProducerCommands {
    type Response = ();

    async fn run(self, bootstrap: &[BrokerHost], config: KafkaConfig) -> anyhow::Result<()> {
        match self {
            ProducerCommands::Random { topic, count, size } => {
                ProduceRandom { topic, count, size }
                    .run(bootstrap, config)
                    .await
            }
            ProducerCommands::File { file, topic } => {
                ProduceFromFile { topic, file }.run(bootstrap, config).await
            }
        }
    }
}

pub struct ProduceRandom {
    topic: String,
    count: u64,
    size: usize,
}

impl Run for ProduceRandom {
    type Response = ();

    async fn run(
        self,
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
    ) -> anyhow::Result<Self::Response> {
        let producer = Producer::try_new(bootstrap, config).await?;

        let topic = self.topic.clone();

        let now = Instant::now();

        let u64_usize = self.size as u64;

        let bar = ProgressBar::new(self.count * u64_usize);
        bar.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg} {bytes_per_sec}",
            )
            .unwrap()
            .progress_chars("##-"),
        );

        let mut rng = rand::thread_rng();

        for _i in 0..self.count {
            let mut msg = vec![0_u8; self.size];
            rng.fill_bytes(&mut msg);

            let msg = Bytes::from(msg);

            producer
                .produce(ProducerRecord {
                    headers: IndexMap::default(),
                    key: None,
                    partition: None,
                    timestamp: None,
                    topic: topic.clone(),
                    value: Some(msg),
                })
                .await?;

            bar.inc(u64_usize);
        }

        producer.flush_and_shutdown().await;

        bar.finish();

        let finish = now.elapsed();

        println!("produced {} messages in {finish:?}", HumanCount(self.count));

        Ok(())
    }
}

pub struct ProduceFromFile {
    pub topic: String,
    pub file: PathBuf,
}

impl ProduceFromFile {
    async fn run_inner(
        producer: &Producer<Tcp, KeyHashPartitioner>,
        file: File,
        topic: String,
    ) -> anyhow::Result<JoinSet<Result<RecordMetadata, KafkaError>>> {
        let mut reader = io::BufReader::new(file).lines();

        let mut join_set = JoinSet::new();

        while let Some(line) = reader.next_line().await? {
            let rx = producer
                .produce(ProducerRecord {
                    headers: IndexMap::default(),
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

    async fn run(
        self,
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
    ) -> anyhow::Result<Self::Response> {
        let file = File::open(self.file).await?;
        let producer = Producer::try_new(bootstrap, config).await?;

        let now = Instant::now();

        let result = Self::run_inner(&producer, file, self.topic).await;

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
