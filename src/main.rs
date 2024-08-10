mod codec;
mod request;
mod response;
mod transport;

use transport::KafkaTransport;

use kafka_protocol::{messages::MetadataRequest, protocol::Builder};

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let client = KafkaTransport::connect("127.0.0.1:9092").await?;

    let res = client
        .send(
            MetadataRequest::builder()
                .allow_auto_topic_creation(false)
                .include_cluster_authorized_operations(true)
                .include_topic_authorized_operations(true)
                .topics(None)
                .build()?
                .into(),
            9,
        )
        .await?;

    println!("{res:#?}");

    Ok(())
}
