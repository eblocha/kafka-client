use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

use rand::{rngs::ThreadRng, seq::IteratorRandom};

use crate::{
    common::TopicPartition,
    conn::selector::{Cluster, PartitionMetadata, TopicMetadata},
};

use super::ProducerRecord;

/// Picks a partition for records that have no partition specified.
///
/// This is a separate trait from [`Partitioner`] so implementations can be non-[`Send`].
///
/// The partitioner session is never held across an await point, allowing it to store thread-local state (such as a
/// random number generator).
pub trait PartitionerSession {
    /// Pick a partition for the given record. Mutate the record's `partition` field to update it.
    fn partition(&mut self, record: &mut ProducerRecord, topic_data: &TopicMetadata);

    /// Called when the producer has validated the partition for a record and is about to send it.
    fn partition_validated(&mut self, record: &ProducerRecord, partition: &PartitionMetadata) {
        let _ = record;
        let _ = partition;
    }
}

/// Manages a [`PartitionerSession`]
pub trait Partitioner: Send {
    type Session: PartitionerSession;

    /// Called when starting to loop over a batch of records to partition them.
    ///
    /// The topic map provided is a reference to the current cluster state.
    fn new_partitioner(&mut self, cluster: &Cluster) -> Self::Session;

    /// Called after partitioning records in a batch, to allow this partitioner to observe the results.
    fn finish_partitioning(&mut self, session: Self::Session) {
        let _ = session;
    }
}

/// A [`PartitionerSession`] that will choose a partition at random.
#[derive(Debug, Default)]
pub struct RandomPartitionerSession(ThreadRng);

impl PartitionerSession for RandomPartitionerSession {
    fn partition(&mut self, record: &mut ProducerRecord, topic_data: &TopicMetadata) {
        record.partition = topic_data
            .partitions
            .iter()
            .filter_map(|result| match result {
                Ok(p) => Some(p.index),
                Err(_) => None,
            })
            .choose(&mut self.0)
    }
}

/// Creates a [`RandomPartitionerSession`].
#[derive(Debug)]
pub struct RandomPartitioner;

impl Partitioner for RandomPartitioner {
    type Session = RandomPartitionerSession;

    fn new_partitioner(&mut self, _cluster: &Cluster) -> Self::Session {
        RandomPartitionerSession::default()
    }
}

/// A [`PartitionerSession`] that attempts to evenly distribute records into partitions
pub struct RoundRobinPartitionerSession {
    sent_records: HashMap<TopicPartition, usize>,
}

impl PartitionerSession for RoundRobinPartitionerSession {
    fn partition(&mut self, record: &mut ProducerRecord, _topic_data: &TopicMetadata) {
        let min = self
            .sent_records
            .iter_mut()
            .filter(|(tp, _count)| *tp.name() == record.topic)
            .min_by(|(_, a), (_, b)| a.cmp(b));

        if let Some((tp, _)) = min {
            record.partition = Some(tp.partition());
        }
    }

    fn partition_validated(&mut self, record: &ProducerRecord, partition: &PartitionMetadata) {
        *self
            .sent_records
            .entry(TopicPartition::new(record.topic.clone(), partition.index))
            .or_default() += 1;
    }
}

#[derive(Debug, Default)]
pub struct RoundRobinPartitioner {
    sent_records: Option<HashMap<TopicPartition, usize>>,
}

impl Partitioner for RoundRobinPartitioner {
    type Session = RoundRobinPartitionerSession;

    fn new_partitioner(&mut self, cluster: &Cluster) -> Self::Session {
        let mut sent_records = self.sent_records.take().unwrap_or_default();

        sent_records.retain(|tp, _| {
            let Ok(metadata) = cluster.get_topic_metadata_by_name(tp.name()) else {
                return false;
            };

            metadata.has_partition(tp.partition())
        });

        RoundRobinPartitionerSession { sent_records }
    }

    fn finish_partitioning(&mut self, session: Self::Session) {
        self.sent_records = Some(session.sent_records);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct KeyHashPartitioner;

impl Partitioner for KeyHashPartitioner {
    type Session = Self;

    fn new_partitioner(&mut self, _cluster: &Cluster) -> Self::Session {
        *self
    }
}

impl PartitionerSession for KeyHashPartitioner {
    fn partition(&mut self, record: &mut ProducerRecord, topic_data: &TopicMetadata) {
        if topic_data.partitions.is_empty() {
            return;
        }

        let mut hasher = DefaultHasher::new();
        record.key.hash(&mut hasher);
        let index = hasher.finish() as i32 % topic_data.partitions.len() as i32;

        record.partition = Some(index);
    }
}
