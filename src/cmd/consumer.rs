use std::collections::HashMap;

use fnv::FnvHashMap;
use futures::Stream;
use kafka_protocol::{
    messages::{
        fetch_request::{FetchPartition, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        FetchRequest, ListOffsetsRequest, TopicName,
    },
    protocol::StrBytes,
    records::{Record, RecordBatchDecoder},
};
use tokio::task::JoinSet;
use tokio_util::either::Either;

use crate::clients::network::NetworkClient;

#[derive(Debug, Default)]
struct PartitionState {
    offset: Option<i64>,
}

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct TopicPartition(TopicName, i32);

#[derive(Debug, Clone)]
pub struct ConsumerRecords {
    pub topic_partition: TopicPartition,
    pub records: Vec<Record>,
}

pub struct Consumer {
    topic: TopicName,
    client: NetworkClient,
    states: HashMap<TopicPartition, PartitionState>,
}

impl Consumer {
    pub fn new(topic: String, client: NetworkClient) -> Self {
        Self {
            topic: TopicName(StrBytes::from_string(topic)),
            client,
            states: Default::default(),
        }
    }

    pub fn stream(mut self) -> impl Stream<Item = anyhow::Result<Vec<ConsumerRecords>>> {
        async_stream::stream! {
            loop {
                let next = self.next_batch().await;
                // TODO back off if error
                yield next;
            }
        }
    }

    pub async fn next_batch(&mut self) -> anyhow::Result<Vec<ConsumerRecords>> {
        let topic_meta = self.client.get_topic_metadata(&self.topic).await??;

        let mut broker_id_to_fetch_req = FnvHashMap::<i32, FetchTopic>::default();
        let mut broker_id_to_offset_req = FnvHashMap::<i32, ListOffsetsTopic>::default();

        for part in topic_meta.partitions {
            let state = self
                .states
                .entry(TopicPartition(self.topic.clone(), part.partition_index))
                .or_default();

            if let Some(offset) = state.offset {
                let req = broker_id_to_fetch_req
                    .entry(part.leader_id.0)
                    .or_insert_with(|| {
                        let mut req = FetchTopic::default();
                        req.topic_id = topic_meta.topic_id;
                        req
                    });

                req.partitions.push({
                    let mut part_req = FetchPartition::default();
                    part_req.current_leader_epoch = part.leader_epoch;
                    part_req.partition = part.partition_index;
                    part_req.fetch_offset = offset;
                    part_req
                });
            } else {
                let req = broker_id_to_offset_req
                    .entry(part.leader_id.0)
                    .or_insert_with(|| {
                        let mut top = ListOffsetsTopic::default();
                        top.name = self.topic.clone();
                        top
                    });

                req.partitions.push({
                    let mut offset_partition = ListOffsetsPartition::default();
                    offset_partition.partition_index = part.partition_index;
                    offset_partition
                });
            }
        }

        let mut fetch_join_set = JoinSet::new();
        let mut offsets_join_set = JoinSet::new();

        for (broker_id, fetch_topic) in broker_id_to_fetch_req.drain() {
            let mut req = FetchRequest::default();
            req.topics.push(fetch_topic);
            let c = self.client.clone();
            fetch_join_set.spawn(async move { c.send_to(req, broker_id).await });
        }

        for (broker_id, offsets_topic) in broker_id_to_offset_req.drain() {
            let mut req = ListOffsetsRequest::default();
            req.topics.push(offsets_topic);
            let c = self.client.clone();
            offsets_join_set.spawn(async move { c.send_to(req, broker_id).await });
        }

        let mut records = Vec::<ConsumerRecords>::new();

        loop {
            let event = tokio::select! {
                Some(Ok(fetch)) = fetch_join_set.join_next() => Either::Left(fetch),
                Some(Ok(offsets)) = offsets_join_set.join_next() => Either::Right(offsets),
                else => break
            };

            match event {
                Either::Left(fetch) => {
                    let Ok(fetch) = fetch else {
                        continue;
                    };

                    for response in fetch.responses {
                        for part in response.partitions {
                            let topic_partition =
                                TopicPartition(response.topic.clone(), part.partition_index);

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

                            records.push(ConsumerRecords {
                                topic_partition,
                                records: part_records,
                            });
                        }
                    }
                }
                Either::Right(offsets) => {
                    let Ok(response) = offsets else {
                        continue;
                    };

                    for top in response.topics {
                        for part in top.partitions {
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
