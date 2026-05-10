//! Data structures for consuming messages from topics

use kafka_protocol::records::Record;

use crate::{common::TopicPartition, error::KafkaError};

/// A set of records from a topic partition
#[derive(Debug, Clone)]
pub struct ConsumerRecords {
    /// The topic name and partition index
    pub topic_partition: TopicPartition,
    /// The batch of records from the partition
    pub records: Vec<Record>,
    /// The offset of the last record in `records`.
    pub largest_offset: Option<i64>,
}

/// A batch of records from multiple topic partitions
pub type ConsumerRecordsResult = Result<Vec<ConsumerRecords>, KafkaError>;
