//! Data structures for sending messages to topics

use bytes::Bytes;
use kafka_protocol::protocol::StrBytes;

/// A packet of data to be sent to a topic or partition
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    /// The target topic.
    pub topic: String,
    /// The target partition.
    ///
    /// If this is [`None`], the producer's partitioner will choose a destination partition.
    pub partition: Option<i32>,
    /// The timestamp for the message, or [`None`] if a timestamp should be generated.
    pub timestamp: Option<i64>,
    /// The message key. Topics that are configured to compact logs will de-duplicate messages by this key, keeping only
    /// the last message with a unique key in each partition.
    ///
    /// Typically this will also be used to auto-partition the message.
    pub key: Option<Bytes>,
    /// The message data.
    pub value: Option<Bytes>,
    /// Message headers.
    pub headers: indexmap::IndexMap<StrBytes, Option<Bytes>>,
}

/// Information about a produced record
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RecordMetadata {
    /// The offset of the record in the partition. This will be `-1` if `acks=0`.
    pub base_offset: i64,
}
