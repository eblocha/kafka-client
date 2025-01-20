use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use fnv::FnvHashMap;
use kafka_protocol::{
    messages::{
        fetch_request::{FetchPartition, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, TopicName,
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
    fetch_join_set: JoinSet<Result<FetchResponse, KafkaChannelError>>,
    offsets_join_set: JoinSet<Result<ListOffsetsResponse, KafkaChannelError>>,
}

impl Consumer {
    pub fn new(client: NetworkClient) -> Self {
        Self {
            client,
            states: Default::default(),
            subscriptions: Default::default(),
            fetch_join_set: JoinSet::new(),
            offsets_join_set: JoinSet::new(),
        }
    }

    pub async fn subscribe(&mut self, topics: &[TopicName]) -> Result<(), KafkaChannelError> {
        self.client.load_topic_metadata(topics.iter()).await?;
        let topic_map = &self.client.borrow_cluster().metadata.topics;
        self.subscriptions = topics
            .into_iter()
            .filter_map(|name| {
                let metadata = topic_map.get(name);
                metadata.map(|meta| (meta.topic_id, (*name).clone()))
            })
            .collect();

        Ok(())
    }

    pub async fn poll(
        &mut self,
    ) -> Result<(Vec<ConsumerRecords>, Option<Duration>), KafkaChannelError> {
        self.spawn_next().await?;
        let records = self.join_next().await?;
        let will_have_records_next_poll = !self.offsets_join_set.is_empty();

        if !records.is_empty() || will_have_records_next_poll {
            Ok((records, None))
        } else {
            Ok((records, Some(Duration::from_millis(500))))
        }
    }

    async fn spawn_next(&mut self) -> Result<(), KafkaChannelError> {
        self.client
            .load_topic_metadata(self.subscriptions.values())
            .await?;

        let cluster = self.client.borrow_cluster();

        let mut broker_id_to_fetch_req = FnvHashMap::<i32, FetchRequest>::default();
        let mut broker_id_to_offset_req = FnvHashMap::<i32, ListOffsetsRequest>::default();

        let mut invalid_topics = HashSet::<&TopicName>::new();

        for (topic_id, topic_name) in self.subscriptions.iter() {
            let Some(meta) = cluster.metadata.topics.get(topic_name) else {
                tracing::warn!(topic = topic_name.0.as_str(), "unknown topic");
                continue;
            };

            let error_code: ErrorCode = meta.error_code.into();

            if error_code != ErrorCode::None {
                tracing::error!(
                    "error fetching metadata for topic {}",
                    topic_name.0.as_str()
                );
                invalid_topics.insert(topic_name);
                continue;
            }

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
                        offsets_partition.timestamp = -1; // latest
                        offsets_partition
                    });
                }
            }

            for (broker_id, topic) in broker_id_to_fetch_topic {
                let req = broker_id_to_fetch_req.entry(broker_id).or_insert_with(|| {
                    let mut req = FetchRequest::default();
                    req.cluster_id = cluster.metadata.cluster_id.clone();
                    req.min_bytes = 4096;
                    req
                });
                req.topics.push(topic);
            }

            for (broker_id, topic) in broker_id_to_offset_topic {
                let req = broker_id_to_offset_req.entry(broker_id).or_default();
                req.topics.push(topic);
            }
        }

        drop(cluster);

        if !invalid_topics.is_empty() {
            self.client
                .invalidate_topic_metadata(invalid_topics.into_iter());
        }

        for (broker_id, req) in broker_id_to_fetch_req {
            let c = self.client.clone();
            self.fetch_join_set
                .spawn(async move { c.send_to(req, broker_id).await });
        }

        for (broker_id, req) in broker_id_to_offset_req {
            let c = self.client.clone();
            self.offsets_join_set
                .spawn(async move { c.send_to(req, broker_id).await });
        }

        Ok(())
    }

    async fn join_next(&mut self) -> Result<Vec<ConsumerRecords>, KafkaChannelError> {
        let mut records = Vec::<ConsumerRecords>::new();

        let mut invalid_topics = HashSet::<TopicName>::new();

        loop {
            let event = tokio::select! {
                Some(fetch) = self.fetch_join_set.join_next() => Either::Left(fetch),
                Some(offsets) = self.offsets_join_set.join_next() => Either::Right(offsets),
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
                                | ErrorCode::FencedLeaderEpoch
                                | ErrorCode::UnknownLeaderEpoch
                                | ErrorCode::StaleBrokerEpoch
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

        if !invalid_topics.is_empty() {
            self.client.invalidate_topic_metadata(invalid_topics.iter());
        }

        Ok(records)
    }
}
