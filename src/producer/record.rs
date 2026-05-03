use bytes::Bytes;
use kafka_protocol::protocol::StrBytes;

#[derive(Debug, Clone)]
pub struct ProducerRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub timestamp: Option<i64>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: indexmap::IndexMap<StrBytes, Option<Bytes>>,
}

#[derive(Debug, Clone)]
pub struct RecordMetadata {
    pub base_offset: i64,
}
