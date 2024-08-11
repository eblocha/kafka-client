mod conn;
mod manager;
mod proto;

use conn::KafkaConnectionConfig;

use kafka_protocol::{
    messages::{metadata_request::MetadataRequestTopic, MetadataRequest, TopicName},
    protocol::StrBytes,
};
use manager::version::VersionedConnection;
use tokio::net::TcpStream;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let tcp = TcpStream::connect("127.0.0.1:9092").await?;

    let conn = VersionedConnection::connect(tcp, &KafkaConnectionConfig::default()).await?;

    let topic = {
        let mut mrt = MetadataRequestTopic::default();
        mrt.name = Some(TopicName(StrBytes::from_static_str("test-topic")));
        mrt
    };

    let res = conn
        .send({
            let mut r = MetadataRequest::default();
            r.include_topic_authorized_operations = true;
            r.allow_auto_topic_creation = false;
            r.topics = Some(vec![topic]);
            r
        })
        .await?;

    conn.shutdown().await;

    println!("{res:#?}");

    Ok(())
}
