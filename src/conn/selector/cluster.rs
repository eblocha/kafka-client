use std::cmp::Ordering;

use derive_more::derive::From;
use fnv::FnvHashMap;
use kafka_protocol::{
    messages::{
        metadata_request::MetadataRequestTopic,
        metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
        TopicName,
    },
    protocol::StrBytes,
};
use uuid::Uuid;

use crate::{common::Node, error::ErrorCode, util::UuidExt};

use super::NodeTaskHandle;

#[derive(Debug, Clone)]
pub struct BrokerMapEntry {
    pub node: Node,
    pub(crate) handle: NodeTaskHandle,
}

/// Mapping of broker id to [`BrokerMapEntry`].
///
/// Used to send requests to specific brokers, or the current least-loaded broker.
#[derive(Debug, Clone, From, Default)]
pub struct BrokerMap(#[from] pub(crate) FnvHashMap<i32, BrokerMapEntry>);

fn least_in_flight(left: &(&i32, &BrokerMapEntry), right: &(&i32, &BrokerMapEntry)) -> Ordering {
    left.1.handle.in_flight().cmp(&right.1.handle.in_flight())
}

fn least_failure_streak(
    left: &(&i32, &BrokerMapEntry),
    right: &(&i32, &BrokerMapEntry),
) -> Ordering {
    left.1
        .handle
        .failure_streak()
        .cmp(&right.1.handle.failure_streak())
}

impl BrokerMap {
    /// Get the current "best" connection handle.
    ///
    /// This will prefer connected brokers with the minimum number of pending requests, then favor the minimum number of
    /// pending requests, connected or not.
    pub fn get_best_connection(&self) -> Option<BrokerMapEntry> {
        // TODO shuffle before selecting
        // prefer connected, non-saturated nodes with least in-flight requests
        let least_loaded_connected = self
            .0
            .iter()
            .filter_map(|(id, entry)| {
                if entry.handle.capacity().is_some_and(|cap| cap > 0) {
                    Some((id, entry))
                } else {
                    None
                }
            })
            .min_by(least_in_flight);

        if let Some((_, entry)) = least_loaded_connected {
            return Some(entry.clone());
        }

        // next, prefer nodes with no failure streak and least in-flight requests
        let least_loaded_no_failures = self
            .0
            .iter()
            .filter_map(|(id, entry)| {
                if entry.handle.failure_streak() == 0 {
                    Some((id, entry))
                } else {
                    None
                }
            })
            .min_by(least_in_flight);

        if let Some((_, entry)) = least_loaded_no_failures {
            return Some(entry.clone());
        }

        // lastly, prefer nodes with the lowest failure streak
        self.0
            .iter()
            .min_by(least_failure_streak)
            .map(|(_, entry)| entry.clone())
    }

    pub(super) fn list_nodes(&self) -> Vec<&Node> {
        self.0.values().map(|entry| &entry.node).collect::<Vec<_>>()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum TopicKey {
    Uuid(Uuid),
    Name(TopicName),
}

impl From<&MetadataResponseTopic> for TopicKey {
    fn from(value: &MetadataResponseTopic) -> Self {
        value
            .topic_id
            .as_optional()
            .map(|uuid| Self::Uuid(uuid))
            // Either uuid or name are supposed to exist, even for responses with an error code.
            .unwrap_or_else(|| Self::Name(value.name.clone().unwrap_or_default()))
    }
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

impl TryFrom<MetadataResponsePartition> for PartitionMetadata {
    type Error = ErrorCode;
    fn try_from(value: MetadataResponsePartition) -> Result<Self, ErrorCode> {
        if value.error_code != ErrorCode::None as i16 {
            return Err(value.error_code.into());
        };

        Ok(Self {
            index: value.partition_index,
            leader_id: value.leader_id.0,
            leader_epoch: value.leader_epoch,
            replica_nodes: value.replica_nodes.into_iter().map(Into::into).collect(),
            isr_nodes: value.isr_nodes.into_iter().map(Into::into).collect(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub key: TopicKey,
    pub name: Option<TopicName>,
    pub id: Option<Uuid>,
    pub is_internal: bool,
    pub partitions: Vec<Result<PartitionMetadata, ErrorCode>>,
}

impl TopicMetadata {
    pub fn get_partition_metadata(&self, partition: i32) -> Result<&PartitionMetadata, ErrorCode> {
        if self.partitions.len() <= partition as usize {
            return Err(ErrorCode::UnknownTopicOrPartition);
        }

        return self.partitions[partition as usize].as_ref().map_err(|e| *e);
    }

    #[inline]
    pub fn has_partition(&self, partition: i32) -> bool {
        self.partitions.len() <= partition as usize
    }
}

impl TryFrom<MetadataResponseTopic> for TopicMetadata {
    type Error = ErrorCode;

    fn try_from(meta: MetadataResponseTopic) -> Result<Self, Self::Error> {
        if meta.error_code != ErrorCode::None as i16 {
            return Err(meta.error_code.into());
        }

        let key = TopicKey::from(&meta);

        let partitions: FnvHashMap<i32, Result<PartitionMetadata, ErrorCode>> = meta
            .partitions
            .into_iter()
            .map(|partition_meta| {
                (
                    partition_meta.partition_index,
                    PartitionMetadata::try_from(partition_meta),
                )
            })
            .collect();

        let max_partition = partitions.keys().max().copied();

        let mut partitions_vec = match max_partition {
            Some(size) => vec![Err(ErrorCode::UnknownTopicOrPartition); (size + 1) as usize],
            None => Vec::new(),
        };

        for (index, result) in partitions.into_iter() {
            partitions_vec[index as usize] = result;
        }

        Ok(Self {
            name: meta.name,
            id: meta.topic_id.as_optional(),
            key,
            is_internal: meta.is_internal,
            partitions: partitions_vec,
        })
    }
}

impl From<&TopicMetadata> for MetadataRequestTopic {
    fn from(value: &TopicMetadata) -> Self {
        let mut req = MetadataRequestTopic::default();
        req.name = value.name.clone();
        if let Some(id) = value.id {
            req.topic_id = id;
        }

        req
    }
}

#[derive(Debug, Default, Clone)]
pub struct Cluster {
    pub brokers: BrokerMap,
    pub cluster_id: Option<StrBytes>,
    pub controller_id: i32,
    /// Maps the [`TopicKey`] to a [`Result`] containing the last metadata fetch result for the topic.
    topics: FnvHashMap<TopicKey, Result<TopicMetadata, ErrorCode>>,
    /// Maps the topic name to either the uuid or name.
    ///
    /// The [`TopicKey`] will be a [`TopicKey::Name`] when:
    /// - The request for metadata failed with an error code, or
    /// - The server does not support topic uuids
    ///
    /// The [`TopicKey`] will be a [`TopicKey::Uuid`] when:
    /// - The request was successful, and
    /// - The server supports topic uuids
    topic_keys_by_name: FnvHashMap<TopicName, TopicKey>,
}

impl Cluster {
    pub(crate) fn new(brokers: BrokerMap) -> Self {
        Self {
            brokers,
            ..Default::default()
        }
    }

    pub fn get_topic_key_by_name(&self, name: &TopicName) -> Option<&TopicKey> {
        self.topic_keys_by_name.get(name)
    }

    pub fn get_topic_metadata(&self, key: &TopicKey) -> Result<&TopicMetadata, ErrorCode> {
        let Some(result) = self.topics.get(key) else {
            return Err(ErrorCode::UnknownTopicOrPartition);
        };

        result.as_ref().map_err(|e| *e)
    }

    pub fn get_topic_metadata_by_name(
        &self,
        name: &TopicName,
    ) -> Result<&TopicMetadata, ErrorCode> {
        let Some(key) = self.get_topic_key_by_name(name) else {
            return Err(ErrorCode::UnknownTopicOrPartition);
        };

        self.get_topic_metadata(key)
    }

    pub(crate) fn invalidate_topic(&mut self, id: &TopicName) {
        let Some(key) = self.topic_keys_by_name.remove(id) else {
            return;
        };

        self.topics.remove(&key);
    }

    pub(crate) fn insert_update(&mut self, topic_meta: MetadataResponseTopic) {
        let key = topic_meta
            .topic_id
            .as_optional()
            .map(TopicKey::Uuid)
            .map(Some)
            .unwrap_or_else(|| topic_meta.name.clone().map(TopicKey::Name));

        match key {
            Some(key) => {
                if let Some(ref name) = topic_meta.name {
                    self.topic_keys_by_name.insert(name.clone(), key.clone());
                }
                self.topics.insert(key, topic_meta.try_into());
            }
            None => {}
        }
    }

    pub(crate) fn create_topics_for_refresh(&self) -> Vec<MetadataRequestTopic> {
        self.topic_keys_by_name
            .iter()
            .map(|(name, key)| {
                let mut req = MetadataRequestTopic::default();
                req.name = Some(name.clone());
                if let TopicKey::Uuid(uuid) = key {
                    req.topic_id = *uuid;
                }

                req
            })
            .collect()
    }
}
