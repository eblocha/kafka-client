use std::{cmp::Ordering, collections::HashMap, time::Instant};

use derive_more::derive::From;
use kafka_protocol::{
    messages::{
        MetadataResponse, TopicName,
        metadata_request::MetadataRequestTopic,
        metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
    },
    protocol::StrBytes,
};
use rustc_hash::FxHashMap;
use uuid::Uuid;

use crate::{
    common::{Node, TopicPartition},
    conn::broker::task::{BrokerTask, BrokerTaskContext, BrokerTaskHandle},
    error::ErrorCode,
};

#[derive(Debug, Clone)]
pub struct BrokerMapEntry<TaskHandle> {
    pub node: Node,
    pub(crate) handle: TaskHandle,
    pub(crate) ctx: BrokerTaskContext,
}

/// Mapping of broker id to [`BrokerMapEntry`].
///
/// Used to send requests to specific brokers, or the current least-loaded broker.
#[derive(Debug, Clone, From)]
pub struct BrokerMap<TaskHandle>(#[from] Vec<BrokerMapEntry<TaskHandle>>);

impl<TaskHandle> Default for BrokerMap<TaskHandle> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

fn least_in_flight<TaskHandle: BrokerTaskHandle>(
    left: &&BrokerMapEntry<TaskHandle>,
    right: &&BrokerMapEntry<TaskHandle>,
) -> Ordering {
    left.handle
        .requests_in_flight()
        .cmp(&right.handle.requests_in_flight())
}

fn least_failure_streak<TaskHandle: BrokerTaskHandle>(
    left: &&BrokerMapEntry<TaskHandle>,
    right: &&BrokerMapEntry<TaskHandle>,
) -> Ordering {
    left.handle
        .connect_failure_streak()
        .cmp(&right.handle.connect_failure_streak())
}

impl<TaskHandle: BrokerTaskHandle> BrokerMap<TaskHandle> {
    /// Get the current "best" connection handle.
    ///
    /// This will prefer connected brokers with the minimum number of pending requests, then favor the minimum number of
    /// pending requests, connected or not.
    pub fn get_best_connection(&self) -> Option<BrokerMapEntry<TaskHandle>> {
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
            .filter(|entry| entry.handle.connect_failure_streak() == 0)
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

    pub(super) fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut BrokerMapEntry<TaskHandle>) -> bool,
    {
        self.0.retain_mut(|entry| f(entry));
    }

    pub(super) fn get_mut(&mut self, broker_id: &i32) -> Option<&mut BrokerMapEntry<TaskHandle>> {
        self.0.iter_mut().find(|entry| entry.node.id == *broker_id)
    }

    #[cfg(test)]
    pub(super) fn get(&self, broker_id: &i32) -> Option<&BrokerMapEntry<TaskHandle>> {
        self.0.iter().find(|entry| entry.node.id == *broker_id)
    }

    pub(super) fn insert(&mut self, entry: BrokerMapEntry<TaskHandle>) {
        if let Some(existing) = self.get_mut(&entry.node.id) {
            *existing = entry;
        } else {
            self.0.push(entry);
        }
    }

    pub(super) fn remove(&mut self, broker_id: &i32) -> Option<BrokerMapEntry<TaskHandle>> {
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

#[derive(Debug, Clone)]
pub struct TopicResult {
    pub metadata: Result<TopicMetadata, ErrorCode>,
    pub timestamp: Instant,
}

#[derive(Debug, Default, Clone)]
pub struct ClusterMetadata {
    /// The cluster id returned by the metadata.
    pub cluster_id: Option<StrBytes>,
    /// The broker id of the controller node.
    pub controller_id: i32,
    /// Maps the [`TopicName`] to a [`Result`] containing the last metadata fetch result for the topic.
    topics: FxHashMap<TopicName, TopicResult>,
    /// Maps the topic name to the uuid.
    /// The uuids will not be nil.
    topic_ids_by_name: FxHashMap<TopicName, Uuid>,
}

impl ClusterMetadata {
    #[inline]
    pub fn get_topic_uuid_by_name(&self, name: &TopicName) -> Option<Uuid> {
        self.topic_ids_by_name.get(name).copied()
    }

    #[inline]
    pub fn get_topic_metadata(&self, key: &TopicName) -> Option<&TopicResult> {
        self.topics.get(key)
    }

    /// Create a [`Vec<MetadataRequestTopic>`] that will refresh metadata for all topics known to the client.
    pub(super) fn create_topics_for_refresh(&self) -> Vec<MetadataRequestTopic> {
        self.topics
            .keys()
            .map(|name| {
                let mut req = MetadataRequestTopic::default();
                req.name = Some(name.clone());
                if let Some(uuid) = self.topic_ids_by_name.get(name) {
                    req.topic_id = *uuid;
                }

                req
            })
            .collect()
    }

    /// Update the metadata from a successful [`MetadataResponse`].
    pub(super) fn update_with(&mut self, response: MetadataResponse, timestamp: Instant) {
        self.cluster_id = response.cluster_id;
        self.controller_id = response.controller_id.0;
        // merge topic metadata with existing metadata
        for topic_meta in response.topics {
            self.insert_update(topic_meta, timestamp);
        }
    }

    fn insert_update(&mut self, topic_meta: MetadataResponseTopic, timestamp: Instant) {
        if topic_meta.topic_id.is_nil() && topic_meta.name.is_none() {
            tracing::warn!(
                "the server responded to a metadata request with a topic that has no name or id"
            );
            return;
        };

        let Some(ref topic_name) = topic_meta.name else {
            // The topic name is empty, which means we requested a topic by id that does not exist.
            // Remove the uuid and metadata for the topic
            let name = self
                .topic_ids_by_name
                .iter()
                .find_map(|(name, existing_key)| {
                    if *existing_key == topic_meta.topic_id {
                        Some(name)
                    } else {
                        None
                    }
                });

            let Some(name) = name else {
                return;
            };

            // Clone is needed because `name` is a reference into `topic_ids_by_name`, which means we can't borrow it as
            // mutable to remove the key.
            // This is a cold path, so perf is not super critical here.
            let name = name.clone();

            self.topic_ids_by_name.remove(&name);

            // Create an entry for the topic name with the errored state.
            // This allows us to retry the topic by name if its id has changed.
            let new_metadata = TopicMetadata::try_from((name.clone(), topic_meta));
            self.topics.insert(
                name,
                TopicResult {
                    metadata: new_metadata,
                    timestamp,
                },
            );
            return;
        };

        let topic_name = topic_name.clone();

        if !topic_meta.topic_id.is_nil() {
            self.topic_ids_by_name
                .insert(topic_name.clone(), topic_meta.topic_id);
        } else {
            self.topic_ids_by_name.remove(&topic_name);
        }

        let new_metadata = TopicMetadata::try_from((topic_name.clone(), topic_meta));

        self.topics.insert(
            topic_name,
            TopicResult {
                metadata: new_metadata,
                timestamp,
            },
        );
    }
}

#[derive(Debug)]
pub struct Cluster<Task: BrokerTask, TaskHandle> {
    /// Mapping of all currently-known broker nodes.
    pub brokers: BrokerMap<TaskHandle>,
    pub partitions: FxHashMap<TopicPartition, Task::PublicPartitionState>,
    /// The cluster's metadata.
    pub metadata: ClusterMetadata,
}

impl<Task: BrokerTask, TaskHandle: Clone> Clone for Cluster<Task, TaskHandle> {
    fn clone(&self) -> Self {
        Self {
            brokers: self.brokers.clone(),
            partitions: self.partitions.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

impl<Task: BrokerTask, TaskHandle> Default for Cluster<Task, TaskHandle> {
    fn default() -> Self {
        Self {
            brokers: BrokerMap::default(),
            partitions: HashMap::default(),
            metadata: ClusterMetadata::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Instant;

    use kafka_protocol::{
        messages::{
            MetadataResponse, TopicName,
            metadata_response::{
                MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
            },
        },
        protocol::StrBytes,
    };
    use tokio_test::{assert_err, assert_ok};
    use uuid::Uuid;

    use crate::{error::ErrorCode, util::TopicNameExt};

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
            Instant::now(),
        );

        assert_eq!(metadata.controller_id, 1);
        assert_eq!(metadata.cluster_id, Some(StrBytes::from_static_str("test")));

        // Verify all requested topic keys exist
        assert_eq!(
            metadata.get_topic_uuid_by_name(&TopicName::from_string("topic-a".into())),
            Some(topic_id)
        );

        assert_eq!(
            metadata.get_topic_uuid_by_name(&TopicName::from_string("topic-b".into())),
            None
        );

        // Verify bad topic gives us the error it had
        assert_eq!(
            metadata
                .get_topic_metadata(&TopicName::from_string("topic-b".into()))
                .unwrap()
                .metadata
                .as_ref()
                .unwrap_err(),
            &ErrorCode::LeaderNotAvailable
        );

        // Nonexistent topics should give None
        assert!(
            metadata
                .get_topic_metadata(&TopicName::from_string("topic-c".into()))
                .is_none()
        );

        // Verify good topic exists
        let topic_metadata = metadata
            .get_topic_metadata(&TopicName::from_string("topic-a".into()))
            .unwrap()
            .metadata
            .as_ref();

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

        metadata.update_with(
            MetadataResponse::default().with_topics(vec![
                MetadataResponseTopic::default().with_name(Some(topic_name.clone())),
            ]),
            Instant::now(),
        );

        metadata.update_with(
            MetadataResponse::default().with_topics(vec![
                MetadataResponseTopic::default()
                    .with_name(Some(topic_name.clone()))
                    .with_topic_id(topic_id),
            ]),
            Instant::now(),
        );

        let key = metadata.get_topic_uuid_by_name(&topic_name);

        assert_eq!(key, Some(topic_id));

        let topic = metadata
            .get_topic_metadata(&topic_name)
            .unwrap()
            .metadata
            .as_ref();

        assert_ok!(topic);

        let topic = topic.unwrap();

        assert_eq!(topic.name, topic_name);

        let topic = metadata
            .get_topic_metadata(&topic_name)
            .unwrap()
            .metadata
            .as_ref();

        assert_ok!(topic);

        let topic = topic.unwrap();

        assert_eq!(topic.name, TopicName::from_string("topic-a".into()));
    }

    /// Verify the cluster properly handles when we attempt to fetch by uuid, but the topic is not found.
    /// The topic should not be lost in this case, but have its state changed to an error.
    #[test]
    fn update_cluster_with_failed_uuid() {
        let mut metadata = ClusterMetadata::default();

        let topic_name = TopicName::from_string("topic-a".to_owned());
        let topic_id = Uuid::parse_str("e93d0c37-745f-45c7-987d-f28350ba1203").unwrap();

        // We get a response to our bootstrap query, and get a topic with an id
        metadata.update_with(
            MetadataResponse::default().with_topics(vec![
                MetadataResponseTopic::default()
                    .with_name(Some(topic_name.clone()))
                    .with_topic_id(topic_id),
            ]),
            Instant::now(),
        );

        // We try to refresh the topic, but it was deleted.
        metadata.update_with(
            MetadataResponse::default().with_topics(vec![
                MetadataResponseTopic::default()
                    .with_error_code(ErrorCode::UnknownTopicOrPartition as i16)
                    .with_name(None)
                    .with_topic_id(topic_id),
            ]),
            Instant::now(),
        );

        // Our cluster should identify the topic, and return an error code for it.
        let key = metadata.get_topic_uuid_by_name(&topic_name);

        assert_eq!(key, None);

        let topic = metadata
            .get_topic_metadata(&topic_name)
            .unwrap()
            .metadata
            .as_ref();

        assert_err!(topic);

        assert_eq!(topic.unwrap_err(), &ErrorCode::UnknownTopicOrPartition);
    }
}
