mod conn;

use conn::{KafkaConnection, KafkaConnectionConfig};

use kafka_protocol::{
    messages::{
        fetch_request::{FetchPartition, FetchTopic},
        FetchRequest, TopicName,
    },
    protocol::{Builder, StrBytes},
};
use tokio::net::TcpStream;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let tcp = TcpStream::connect("127.0.0.1:9092").await?;

    let conn = KafkaConnection::connect(tcp, &KafkaConnectionConfig::default()).await?;

    let res = conn
        .send(
            FetchRequest::builder()
                .cluster_id(Some(StrBytes::from_static_str("CvtEUt71RdKpIueT-oot1Q")))
                .isolation_level(0)
                .min_bytes(1)
                .max_wait_ms(200)
                .max_bytes(1024)
                .topics(vec![FetchTopic::builder()
                    .topic(TopicName(StrBytes::from_static_str("test-topic")))
                    .partitions(vec![FetchPartition::builder()
                        .partition(0)
                        .current_leader_epoch(4)
                        .partition_max_bytes(1024)
                        .fetch_offset(2)
                        .last_fetched_epoch(-1)
                        .build()
                        .unwrap()])
                    .build()?])
                .build()?,
            12,
        )
        .await
        .unwrap();

    println!("{res:#?}");

    Ok(())
}
