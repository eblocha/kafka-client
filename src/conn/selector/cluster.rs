use std::cmp::Ordering;

use derive_more::derive::From;
use kafka_protocol::{
    messages::{
        metadata_request::MetadataRequestTopic,
        metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
        MetadataResponse, TopicName,
    },
    protocol::StrBytes,
};
use rustc_hash::FxHashMap;
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
pub struct BrokerMap(#[from] Vec<BrokerMapEntry>);

fn least_in_flight(left: &&BrokerMapEntry, right: &&BrokerMapEntry) -> Ordering {
    left.handle.in_flight().cmp(&right.handle.in_flight())
}

fn least_failure_streak(left: &&BrokerMapEntry, right: &&BrokerMapEntry) -> Ordering {
    left.handle
        .failure_streak()
        .cmp(&right.handle.failure_streak())
}

impl BrokerMap {
    /// Get a broker map entry for a specific broker by id.
    pub fn get_connection_for(&self, broker_id: i32) -> Option<&BrokerMapEntry> {
        self.0.iter().find(|entry| entry.node.id == broker_id)
    }

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
            .filter(|entry| entry.handle.capacity().is_some_and(|cap| cap > 0))
            .min_by(least_in_flight);

        if let Some(entry) = least_loaded_connected {
            return Some(entry.clone());
        }

        // next, prefer nodes with no failure streak and least in-flight requests
        let least_loaded_no_failures = self
            .0
            .iter()
            .filter(|entry| entry.handle.failure_streak() == 0)
            .min_by(least_in_flight);

        if let Some(entry) = least_loaded_no_failures {
            return Some(entry.clone());
        }

        // lastly, prefer nodes with the lowest failure streak
        self.0.iter().min_by(least_failure_streak).cloned()
    }

    pub(super) fn list_nodes(&self) -> Vec<&Node> {
        self.0.iter().map(|entry| &entry.node).collect()
    }

    pub(super) fn drain(&mut self) -> impl Iterator<Item = BrokerMapEntry> + use<'_> {
        self.0.drain(..)
    }

    pub(super) fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut BrokerMapEntry) -> bool,
    {
        self.0.retain_mut(|entry| f(entry));
    }

    pub(super) fn get_mut(&mut self, broker_id: &i32) -> Option<&mut BrokerMapEntry> {
        self.0.iter_mut().find(|entry| entry.node.id == *broker_id)
    }

    pub(super) fn insert(&mut self, entry: BrokerMapEntry) {
        if let Some(existing) = self.get_mut(&entry.node.id) {
            *existing = entry;
        } else {
            self.0.push(entry);
        }
    }

    pub(super) fn remove(&mut self, broker_id: &i32) -> Option<BrokerMapEntry> {
        let idx = self.0.iter().enumerate().find_map(|(i, entry)| {
            if &entry.node.id == broker_id {
                Some(i)
            } else {
                None
            }
        })?;

        Some(self.0.swap_remove(idx))
    }
}

/// An identifier for a topic that was requested by the client, and may or may not be known to the server.
///
/// This will be a [`TopicKey::Uuid`] when the server identifies topics by id, the topic is known by the server, and we
/// got a successful response for it.
///
/// This will be a [`TopicKey::Name`] when either:
/// - The server identifies topics by name
/// - We requested a topic by name that does not exist
///
/// We use an enum here to be able to look up the topic information when handling a fetch request,
/// since the fetch response only uses id.
///
/// However, we also need to be able to look up information by name, in case the broker does not support a version that
/// uses ids, or we got an error code for the topic.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum TopicKey {
    /// A topic key identified by uuid.
    Uuid(Uuid),
    /// A topic key identified by name.
    Name(TopicName),
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

    #[inline]
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
    pub name: TopicName,
    partitions: Vec<Result<PartitionMetadata, ErrorCode>>,
}

impl TopicMetadata {
    #[inline]
    pub fn get_partition_metadata(&self, partition: i32) -> Result<&PartitionMetadata, ErrorCode> {
        let Some(result) = self.partitions.get(partition as usize) else {
            return Err(ErrorCode::UnknownTopicOrPartition);
        };

        result.as_ref().map_err(|e| *e)
    }

    #[inline]
    pub fn has_partition(&self, partition: i32) -> bool {
        self.partitions.len() <= partition as usize
    }

    #[inline]
    pub fn iter_partition_results(
        &self,
    ) -> impl Iterator<Item = &Result<PartitionMetadata, ErrorCode>> {
        self.partitions.iter()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    #[cfg(test)]
    pub(crate) fn new_from_parts(
        name: TopicName,
        partitions: Vec<Result<PartitionMetadata, ErrorCode>>,
    ) -> Self {
        Self { name, partitions }
    }
}

impl TryFrom<(TopicName, MetadataResponseTopic)> for TopicMetadata {
    type Error = ErrorCode;

    fn try_from((name, meta): (TopicName, MetadataResponseTopic)) -> Result<Self, Self::Error> {
        if meta.error_code != ErrorCode::None as i16 {
            return Err(meta.error_code.into());
        }

        let partitions: FxHashMap<usize, Result<PartitionMetadata, ErrorCode>> = meta
            .partitions
            .into_iter()
            .filter_map(|partition_meta| {
                let Ok(usize_index) = usize::try_from(partition_meta.partition_index) else {
                    return None;
                };

                Some((usize_index, PartitionMetadata::try_from(partition_meta)))
            })
            .collect();

        let max_partition = partitions.keys().max().copied();

        let mut partitions_vec = match max_partition {
            Some(size) => vec![Err(ErrorCode::UnknownTopicOrPartition); size + 1],
            None => Vec::new(),
        };

        for (index, result) in partitions {
            partitions_vec[index] = result;
        }

        Ok(Self {
            name,
            partitions: partitions_vec,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct Cluster {
    /// Mapping of all currently-known broker nodes.
    pub brokers: BrokerMap,
    /// The cluster's metadata.
    pub metadata: ClusterMetadata,
}

impl Cluster {
    pub(crate) fn new(brokers: BrokerMap) -> Self {
        Self {
            brokers,
            ..Default::default()
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ClusterMetadata {
    /// The cluster id returned by the metadata.
    pub cluster_id: Option<StrBytes>,
    /// The broker id of the controller node.
    pub controller_id: i32,
    /// Maps the [`TopicKey`] to a [`Result`] containing the last metadata fetch result for the topic.
    topics: FxHashMap<TopicKey, Result<TopicMetadata, ErrorCode>>,
    /// Maps the topic name to either the uuid or name.
    ///
    /// The [`TopicKey`] will be a [`TopicKey::Name`] when:
    /// - The request for metadata failed with an error code, or
    /// - The server does not support topic uuids
    ///
    /// The [`TopicKey`] will be a [`TopicKey::Uuid`] when:
    /// - The request was successful, and
    /// - The server supports topic uuids
    topic_keys_by_name: FxHashMap<TopicName, TopicKey>,
}

impl ClusterMetadata {
    #[inline]
    pub fn get_topic_key_by_name(&self, name: &TopicName) -> Option<&TopicKey> {
        self.topic_keys_by_name.get(name)
    }

    #[inline]
    pub fn get_topic_metadata(&self, key: &TopicKey) -> Result<&TopicMetadata, ErrorCode> {
        let Some(result) = self.topics.get(key) else {
            return Err(ErrorCode::UnknownTopicOrPartition);
        };

        result.as_ref().map_err(|e| *e)
    }

    #[inline]
    pub fn get_topic_metadata_by_name(
        &self,
        name: &TopicName,
    ) -> Result<&TopicMetadata, ErrorCode> {
        let Some(key) = self.get_topic_key_by_name(name) else {
            return Err(ErrorCode::UnknownTopicOrPartition);
        };

        self.get_topic_metadata(key)
    }

    #[inline]
    pub fn get_topic_metadata_and_key_by_name(
        &self,
        name: &TopicName,
    ) -> Result<(&TopicKey, &TopicMetadata), ErrorCode> {
        let Some(key) = self.get_topic_key_by_name(name) else {
            return Err(ErrorCode::UnknownTopicOrPartition);
        };

        self.get_topic_metadata(key).map(|meta| (key, meta))
    }

    /// Create a [`Vec<MetadataRequestTopic>`] that will refresh metadata for all topics known to the client.
    pub(super) fn create_topics_for_refresh(&self) -> Vec<MetadataRequestTopic> {
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

    /// Update the metadata from a successful [`MetadataResponse`].
    pub(super) fn update_with(&mut self, response: MetadataResponse) {
        self.cluster_id = response.cluster_id;
        self.controller_id = response.controller_id.0;
        // merge topic metadata with existing metadata
        for topic_meta in response.topics {
            self.insert_update(topic_meta);
        }
    }

    fn insert_update(&mut self, topic_meta: MetadataResponseTopic) {
        let key = topic_meta
            .topic_id
            .as_optional()
            .map(TopicKey::Uuid)
            .map_or_else(|| topic_meta.name.clone().map(TopicKey::Name), Some);

        let Some(key) = key else {
            tracing::warn!(
                "the server responded to a metadata request with a topic that has no name or id"
            );
            return;
        };

        let Some(ref topic_name) = topic_meta.name else {
            // The topic name is empty, which means we requested a topic by id that does not exist.
            // Remove the uuid-key from the topic mapping
            if let Some(Ok(meta)) = self.topics.remove(&key) {
                // Use the name for the key instead of the uuid, since it no longer exists.
                let new_key = TopicKey::Name(meta.name.clone());
                self.topics.insert(
                    new_key.clone(),
                    TopicMetadata::try_from((meta.name.clone(), topic_meta)),
                );
                self.topic_keys_by_name.insert(meta.name, new_key);
            } else {
                // we never had this topic, or the last attempt to fetch gave an error
            };
            return;
        };

        self.topic_keys_by_name
            .insert(topic_name.clone(), key.clone());

        if matches!(key, TopicKey::Uuid(_)) {
            // Remove the topic-name version if it exists.
            // For example, if a previous request for the topic by name failed
            self.topics.remove(&TopicKey::Name(topic_name.clone()));
        }

        self.topics.insert(
            key,
            TopicMetadata::try_from((topic_name.clone(), topic_meta)),
        );
    }
}

#[cfg(test)]
mod test {
    use kafka_protocol::{
        messages::{
            metadata_response::{
                MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
            },
            MetadataResponse, TopicName,
        },
        protocol::StrBytes,
    };
    use tokio_test::{assert_err, assert_ok};
    use uuid::Uuid;

    use crate::{conn::selector::TopicKey, error::ErrorCode, util::TopicNameExt};

    use super::ClusterMetadata;

    #[test]
    fn update_empty_cluster_metadata() {
        let mut metadata = ClusterMetadata::default();
        let topic_id = Uuid::parse_str("e93d0c37-745f-45c7-987d-f28350ba1203").unwrap();

        metadata.update_with(
            MetadataResponse::default()
                .with_cluster_id(Some(StrBytes::from_static_str("test")))
                .with_controller_id(1.into())
                .with_brokers(vec![
                    MetadataResponseBroker::default()
                        .with_node_id(0.into())
                        .with_host(StrBytes::from_static_str("test"))
                        .with_port(9092),
                    MetadataResponseBroker::default()
                        .with_node_id(1.into())
                        .with_host(StrBytes::from_static_str("test"))
                        .with_port(9093),
                ])
                .with_topics(vec![
                    MetadataResponseTopic::default()
                        .with_name(Some(TopicName::from_string("topic-a".to_owned())))
                        .with_topic_id(topic_id)
                        .with_partitions(vec![
                            MetadataResponsePartition::default()
                                .with_leader_id(0.into())
                                .with_leader_epoch(1)
                                .with_partition_index(1),
                            MetadataResponsePartition::default()
                                .with_leader_id(1.into())
                                .with_leader_epoch(2)
                                .with_partition_index(0), // Kakfa can respond with out-of-order partitions
                            MetadataResponsePartition::default()
                                // Not a realistic error for this request, but here to test we don't just always return unknown
                                .with_error_code(ErrorCode::LeaderNotAvailable as i16)
                                .with_leader_id((-1).into())
                                .with_leader_epoch(-1)
                                .with_partition_index(2),
                            MetadataResponsePartition::default()
                                .with_error_code(ErrorCode::LeaderNotAvailable as i16)
                                .with_leader_id((-1).into())
                                .with_leader_epoch(-1)
                                // Don't panic
                                .with_partition_index(-1),
                        ]),
                    MetadataResponseTopic::default()
                        .with_error_code(ErrorCode::LeaderNotAvailable as i16)
                        .with_name(Some(TopicName::from_string("topic-b".to_owned())))
                        .with_topic_id(Uuid::nil())
                        .with_partitions(vec![]),
                ]),
        );

        assert_eq!(metadata.controller_id, 1);
        assert_eq!(metadata.cluster_id, Some(StrBytes::from_static_str("test")));

        // Verify all requested topic keys exist
        assert_eq!(
            metadata.get_topic_key_by_name(&TopicName::from_string("topic-a".into())),
            Some(&TopicKey::Uuid(topic_id))
        );

        assert_eq!(
            metadata.get_topic_key_by_name(&TopicName::from_string("topic-b".into())),
            Some(&TopicKey::Name(TopicName::from_string("topic-b".into())))
        );

        // Verify bad topic gives us the error it had
        assert_eq!(
            metadata
                .get_topic_metadata_by_name(&TopicName::from_string("topic-b".into()))
                .unwrap_err(),
            ErrorCode::LeaderNotAvailable
        );

        // Nonexistent topics should give unknown-topic errors
        assert_eq!(
            metadata
                .get_topic_metadata_by_name(&TopicName::from_string("topic-c".into()))
                .unwrap_err(),
            ErrorCode::UnknownTopicOrPartition
        );

        // Verify good topic exists
        let topic_metadata =
            metadata.get_topic_metadata_by_name(&TopicName::from_string("topic-a".into()));
        assert_ok!(topic_metadata);

        let topic_metadata = topic_metadata.unwrap();

        assert_eq!(
            topic_metadata.name,
            TopicName::from_string("topic-a".into())
        );

        let partition_0 = topic_metadata.get_partition_metadata(0);
        let partition_1 = topic_metadata.get_partition_metadata(1);
        let partition_2 = topic_metadata.get_partition_metadata(2);
        let partition_3 = topic_metadata.get_partition_metadata(3);
        let partition_negative = topic_metadata.get_partition_metadata(-1);

        assert_ok!(partition_0);
        assert_ok!(partition_1);

        assert_eq!(partition_2.unwrap_err(), ErrorCode::LeaderNotAvailable);
        assert_eq!(partition_3.unwrap_err(), ErrorCode::UnknownTopicOrPartition);
        assert_eq!(
            partition_negative.unwrap_err(),
            ErrorCode::UnknownTopicOrPartition
        );

        let partition_0 = partition_0.unwrap();
        let partition_1 = partition_1.unwrap();

        assert_eq!(partition_0.index, 0);
        assert_eq!(partition_0.leader_id, 1);
        assert_eq!(partition_0.leader_epoch, 2);

        assert_eq!(partition_1.index, 1);
        assert_eq!(partition_1.leader_id, 0);
        assert_eq!(partition_1.leader_epoch, 1);
    }

    /// The cluster should be able to resolve topics that previously got fetched by name and did not exist,
    /// but fetched again and exist now.
    #[test]
    fn update_cluster_with_uuid_after_names() {
        let mut metadata = ClusterMetadata::default();

        let topic_name = TopicName::from_string("topic-a".to_owned());
        let topic_id = Uuid::parse_str("e93d0c37-745f-45c7-987d-f28350ba1203").unwrap();

        metadata.update_with(MetadataResponse::default().with_topics(vec![
            MetadataResponseTopic::default().with_name(Some(topic_name.clone())),
        ]));

        metadata.update_with(MetadataResponse::default().with_topics(vec![
            MetadataResponseTopic::default()
                .with_name(Some(topic_name.clone()))
                .with_topic_id(topic_id)
        ]));

        let key = metadata.get_topic_key_by_name(&topic_name);

        assert_eq!(key, Some(&TopicKey::Uuid(topic_id)));

        let topic = metadata.get_topic_metadata(key.unwrap());

        assert_ok!(topic);

        let topic = topic.unwrap();

        assert_eq!(topic.name, topic_name);

        let topic = metadata.get_topic_metadata_by_name(&topic_name);

        assert_ok!(topic);

        let topic = topic.unwrap();

        assert_eq!(topic.name, TopicName::from_string("topic-a".into()));

        assert!(
            !metadata.topics.contains_key(&TopicKey::Name(topic_name)),
            "original TopicName key still exists in the metadata"
        );
    }

    /// Verify the cluster properly handles when we attempt to fetch by uuid, but the topic is not found.
    /// The topic should not be lost in this case, but have its key changed to a name-based key.
    #[test]
    fn update_cluster_with_failed_uuid() {
        let mut metadata = ClusterMetadata::default();

        let topic_name = TopicName::from_string("topic-a".to_owned());
        let topic_id = Uuid::parse_str("e93d0c37-745f-45c7-987d-f28350ba1203").unwrap();

        // We get a response to our bootstrap query, and get a topic with an id
        metadata.update_with(MetadataResponse::default().with_topics(
            vec![MetadataResponseTopic::default()
                .with_name(Some(topic_name.clone()))
                .with_topic_id(topic_id)],
        ));

        // We try to refresh the topic, but it was deleted.
        metadata.update_with(MetadataResponse::default().with_topics(
            vec![MetadataResponseTopic::default()
                .with_error_code(ErrorCode::UnknownTopicOrPartition as i16)
                .with_name(None)
                .with_topic_id(topic_id)],
        ));

        // Our cluster should now identify that topic by name, and return an error code for it.
        let key = metadata.get_topic_key_by_name(&topic_name);

        assert_eq!(key, Some(&TopicKey::Name(topic_name.clone())));

        let topic = metadata.get_topic_metadata_by_name(&topic_name);

        assert_err!(topic);

        assert_eq!(topic.unwrap_err(), ErrorCode::UnknownTopicOrPartition);
    }
}
