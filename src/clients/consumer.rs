use std::collections::{HashMap, HashSet};

use fnv::FnvHashMap;
use kafka_protocol::{
    messages::{
        fetch_request::{FetchPartition, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        FetchRequest, ListOffsetsRequest, TopicName,
    },
    records::{Record, RecordBatchDecoder},
};
use tokio::task::JoinSet;
use tokio_util::either::Either;
use uuid::Uuid;

use crate::{
    clients::network::NetworkClient, conn::KafkaChannelError, proto::error_codes::ErrorCode,
};

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct TopicPartition(TopicName, i32);

#[derive(Debug, Clone)]
pub struct ConsumerRecords {
    pub topic_partition: TopicPartition,
    pub records: Vec<Record>,
}

#[derive(Debug, Default)]
struct PartitionState {
    offset: Option<i64>,
}

pub struct Consumer {
    client: NetworkClient,
    states: HashMap<TopicPartition, PartitionState>,
    subscriptions: HashMap<Uuid, TopicName>,
}

impl Consumer {
    pub fn new(client: NetworkClient) -> Self {
        Self {
            client,
            states: Default::default(),
            subscriptions: Default::default(),
        }
    }

    pub async fn subscribe(&mut self, topics: &[&TopicName]) -> Result<(), KafkaChannelError> {
        let topic_map = self.client.get_topic_metadata(topics).await?;
        self.subscriptions = topics
            .iter()
            .filter_map(|name| {
                let metadata = topic_map.get(*name);
                metadata.map(|meta| (meta.topic_id, (*name).clone()))
            })
            .collect();

        Ok(())
    }

    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecords>, KafkaChannelError> {
        let subscribed_topics: Vec<_> = self.subscriptions.values().collect();

        let topic_map = self.client.get_topic_metadata(&subscribed_topics).await?;

        let mut broker_id_to_fetch_req = FnvHashMap::<i32, FetchRequest>::default();
        let mut broker_id_to_offset_req = FnvHashMap::<i32, ListOffsetsRequest>::default();

        for (topic_id, topic_name) in self.subscriptions.iter() {
            let Some(meta) = topic_map.get(topic_name) else {
                tracing::warn!(topic = topic_name.0.as_str(), "unknown topic");
                continue;
            };

            let mut broker_id_to_fetch_topic = FnvHashMap::<i32, FetchTopic>::default();
            let mut broker_id_to_offset_topic = FnvHashMap::<i32, ListOffsetsTopic>::default();

            for part in meta.partitions.iter() {
                let state = self
                    .states
                    .entry(TopicPartition(topic_name.clone(), part.partition_index))
                    .or_default();

                if let Some(offset) = state.offset {
                    let fetch_topic = broker_id_to_fetch_topic
                        .entry(part.leader_id.0)
                        .or_insert_with(|| {
                            let mut fetch_topic = FetchTopic::default();
                            fetch_topic.topic = topic_name.clone();
                            fetch_topic.topic_id = topic_id.clone();
                            fetch_topic
                        });

                    fetch_topic.partitions.push({
                        let mut fetch_partition = FetchPartition::default();
                        fetch_partition.current_leader_epoch = part.leader_epoch;
                        fetch_partition.partition = part.partition_index;
                        fetch_partition.fetch_offset = offset;
                        fetch_partition
                    });
                } else {
                    let offsets_topic = broker_id_to_offset_topic
                        .entry(part.leader_id.0)
                        .or_insert_with(|| {
                            let mut offsets_topic = ListOffsetsTopic::default();
                            offsets_topic.name = topic_name.clone();
                            offsets_topic
                        });

                    offsets_topic.partitions.push({
                        let mut offsets_partition = ListOffsetsPartition::default();
                        offsets_partition.partition_index = part.partition_index;
                        offsets_partition
                    });
                }
            }

            for (broker_id, topic) in broker_id_to_fetch_topic {
                let req = broker_id_to_fetch_req.entry(broker_id).or_default();
                req.max_wait_ms = 5000;
                req.topics.push(topic);
            }

            for (broker_id, topic) in broker_id_to_offset_topic {
                let req = broker_id_to_offset_req.entry(broker_id).or_default();
                req.topics.push(topic);
            }
        }

        let mut fetch_join_set = JoinSet::new();
        let mut offsets_join_set = JoinSet::new();

        for (broker_id, req) in broker_id_to_fetch_req {
            let c = self.client.clone();
            fetch_join_set.spawn(async move { c.send_to(req, broker_id).await });
        }

        for (broker_id, req) in broker_id_to_offset_req {
            let c = self.client.clone();
            offsets_join_set.spawn(async move { c.send_to(req, broker_id).await });
        }

        let mut records = Vec::<ConsumerRecords>::new();

        loop {
            let event = tokio::select! {
                Some(fetch) = fetch_join_set.join_next() => Either::Left(fetch),
                Some(offsets) = offsets_join_set.join_next() => Either::Right(offsets),
                else => break
            };

            let event = match event {
                Either::Left(Ok(Ok(fetch))) => Either::Left(fetch),
                Either::Right(Ok(Ok(offsets))) => Either::Right(offsets),
                Either::Left(Ok(Err(e))) | Either::Right(Ok(Err(e))) => return Err(e),
                _ => continue, // panic when sending request
            };

            match event {
                Either::Left(fetch) => {
                    let mut invalid_topics = HashSet::<TopicName>::new();

                    for response in fetch.responses {
                        let topic_name = if !response.topic.is_empty() {
                            &response.topic
                        } else {
                            let Some(topic_name) = self.subscriptions.get(&response.topic_id)
                            else {
                                // Not subscribed
                                continue;
                            };
                            topic_name
                        };

                        for part in response.partitions {
                            let error_code: ErrorCode = part.error_code.into();

                            match error_code {
                                ErrorCode::InvalidTopicException
                                | ErrorCode::ReassignmentInProgress
                                | ErrorCode::NotLeaderOrFollower => {
                                    invalid_topics.insert(topic_name.clone());
                                    continue;
                                }
                                _ => {}
                            }

                            let topic_partition =
                                TopicPartition(topic_name.clone(), part.partition_index);

                            let Some(mut part_records) = part.records else {
                                continue;
                            };

                            let Ok(part_records) = RecordBatchDecoder::decode(&mut part_records)
                            else {
                                continue;
                            };

                            let state = self.states.entry(topic_partition.clone()).or_default();

                            let largest_offset = part_records
                                .iter()
                                .max_by(|a, b| a.offset.cmp(&b.offset))
                                .map(|record| record.offset + 1)
                                .or(state.offset)
                                .unwrap_or(part.log_start_offset);

                            state.offset.replace(largest_offset);

                            if !part_records.is_empty() {
                                records.push(ConsumerRecords {
                                    topic_partition,
                                    records: part_records,
                                });
                            }
                        }
                    }

                    if !invalid_topics.is_empty() {
                        self.client
                            .invalidate_topic_metadata(&invalid_topics.iter().collect::<Vec<_>>());
                    }
                }
                Either::Right(offsets) => {
                    for top in offsets.topics {
                        for part in top.partitions {
                            // TODO handle error codes here

                            let topic_partition =
                                TopicPartition(top.name.clone(), part.partition_index);
                            self.states.insert(
                                topic_partition,
                                PartitionState {
                                    offset: Some(part.offset),
                                },
                            );
                        }
                    }
                }
            }
        }

        Ok(records)
    }
}
