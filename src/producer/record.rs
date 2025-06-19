use bytes::Bytes;
use kafka_protocol::{messages::TopicName, protocol::StrBytes};

#[derive(Debug, Clone)]
pub struct ProducerRecord {
    pub topic: TopicName,
    pub partition: Option<i32>,
    pub timestamp: Option<i64>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: indexmap::IndexMap<StrBytes, Option<Bytes>>,
}
