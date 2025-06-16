use kafka_protocol::{
    messages::{produce_request::TopicProduceData, TopicName},
    records::Record,
};
use rustc_hash::FxHashMap;
use tokio::sync::oneshot;

use crate::{common::TopicPartition, error::KafkaError};

use super::RecordMetadata;

/// A [`Record`] and [`oneshot::Sender`] pair that will await the response for the record.
pub struct PreparedRecord {
    /// A [`Record`] ready to be encoded.
    pub record: Record,
    /// The [`oneshot::Sender`] that is expecting a response for the record.
    pub tx: oneshot::Sender<Result<RecordMetadata, KafkaError>>,
}

/// A collection of [`PreparedRecord`]s that will be sent to a specific broker.
pub struct LeaderPreparedRecords {
    /// The broker id this is intended for.
    pub broker_id: i32,
    /// Maps each partition led by this broker to a batch of [`PreparedRecord`]s.
    pub partitions: FxHashMap<TopicPartition, Vec<PreparedRecord>>,
}

impl LeaderPreparedRecords {
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    pub fn gc(&mut self) {
        self.partitions.retain(|_, records| !records.is_empty());
    }
}

/// A collection of leader nodes with record batches prepared to send.
#[derive(Default)]
pub struct ProduceLeaders(Vec<LeaderPreparedRecords>);

impl ProduceLeaders {
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut LeaderPreparedRecords> {
        self.0.iter_mut()
    }

    pub fn get_mut(&mut self, broker_id: i32) -> Option<&mut LeaderPreparedRecords> {
        self.0.iter_mut().find(|entry| entry.broker_id == broker_id)
    }

    pub fn get_mut_or_default(&mut self, broker_id: i32) -> &mut LeaderPreparedRecords {
        let index = self.0.iter().enumerate().find_map(|(i, entry)| {
            if entry.broker_id == broker_id {
                Some(i)
            } else {
                None
            }
        });

        if let Some(index) = index {
            return self
                .0
                .get_mut(index)
                .expect("the item we just found disappeared from the array");
        }

        let index = self.0.len();

        self.0.push(LeaderPreparedRecords {
            broker_id,
            partitions: Default::default(),
        });

        self.0
            .get_mut(index)
            .expect("the item we just pushed is not in the array")
    }

    pub fn gc(&mut self) {
        self.0.retain(|leader| !leader.is_empty());
    }
}

/// Similar in concept to a memory arena, this structure is used by the producer task to prepare and organize [`Record`]s.
///
/// The fields are mutated while preparing a batch so the memory space can be reused on later sends without reallocation.
#[derive(Default)]
pub struct ProducerArena {
    /// Mapping of broker id to a mapping of topic partition to a batch of records to send to the topic.
    ///
    /// This is mutated for performance during sends.
    pub brokers: ProduceLeaders,
    /// Reusable mapping for organizing topic batches
    pub topic_data: FxHashMap<TopicName, TopicProduceData>,
}
