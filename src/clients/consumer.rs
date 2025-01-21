use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use fnv::FnvHashMap;
use kafka_protocol::{
    messages::{
        fetch_request::{FetchPartition, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        FetchRequest, ListOffsetsRequest, ResponseKind, TopicName,
    },
    records::{Record, RecordBatchDecoder},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use uuid::Uuid;

use crate::{
    backoff::{exponential_backoff, BackoffSession},
    clients::network::NetworkClient,
    conn::KafkaChannelError,
    proto::{error_codes::ErrorCode, request::KafkaRequest},
};

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct TopicPartition(TopicName, i32);

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

#[derive(Debug, Default)]
struct PartitionState {
    offset: Option<i64>,
}

enum ConsumerCommandKind {
    SubscribeTopics(Vec<TopicName>),
}

struct ConsumerCommand {
    /// Emits when the command has been executed successfully. Drop to abort.
    tx: oneshot::Sender<Result<(), KafkaChannelError>>,
    kind: ConsumerCommandKind,
}

enum ConsumerTaskEvent {
    Command(ConsumerCommand),
    Poll,
    Shutdown,
}

struct ConsumerTask {
    client: NetworkClient,
    states: HashMap<TopicPartition, PartitionState>,
    subscriptions: HashMap<Uuid, TopicName>,
    join_set: JoinSet<Result<ResponseKind, KafkaChannelError>>,
    tx: mpsc::Sender<Vec<ConsumerRecords>>,
    rx: mpsc::UnboundedReceiver<ConsumerCommand>,
    poll_backoff: BackoffSession<()>,
}

impl ConsumerTask {
    pub fn new(
        client: NetworkClient,
        tx: mpsc::Sender<Vec<ConsumerRecords>>,
        rx: mpsc::UnboundedReceiver<ConsumerCommand>,
    ) -> Self {
        Self {
            client,
            tx,
            rx,
            states: HashMap::new(),
            subscriptions: HashMap::new(),
            join_set: JoinSet::new(),
            poll_backoff: Default::default(),
        }
    }

    pub async fn run(mut self) {
        loop {
            // This loop is roughly equivalent to "poll" in the Java impl
            let event = self.recv_next_event().await;

            let result = match event {
                ConsumerTaskEvent::Shutdown => break,
                ConsumerTaskEvent::Command(command) => self.handle_command(command).await,
                ConsumerTaskEvent::Poll => self.handle_poll().await,
            };

            if result.is_none() {
                break;
            }
        }
    }

    async fn handle_command(&mut self, command: ConsumerCommand) -> Option<()> {
        if command.tx.is_closed() {
            return Some(());
        }

        match command.kind {
            ConsumerCommandKind::SubscribeTopics(ref topics) => {
                let result = self.subscribe(topics).await;
                if let Err(ref e) = result {
                    tracing::error!(topics = ?topics, "failed to subscribe: {e}");
                };
                let _ = command.tx.send(result);
            }
        }

        Some(())
    }

    async fn recv_next_event(&mut self) -> ConsumerTaskEvent {
        tokio::select! {
            biased;
            _ = self.tx.closed() => ConsumerTaskEvent::Shutdown,
            command = self.rx.recv() => command.map(ConsumerTaskEvent::Command).unwrap_or(ConsumerTaskEvent::Shutdown),
            _ = self.poll_backoff.wait_next() => ConsumerTaskEvent::Poll,
        }
    }

    async fn subscribe(&mut self, topics: &[TopicName]) -> Result<(), KafkaChannelError> {
        tracing::info!("subscribing to topics {topics:?}");
        self.client.load_topic_metadata(topics.iter()).await?;
        let topic_map = &self.client.borrow_cluster().metadata.topics;
        self.subscriptions = topics
            .into_iter()
            .filter_map(|name| {
                let metadata = topic_map.get(name);
                metadata.map(|meta| (meta.topic_id, (*name).clone()))
            })
            .collect();

        tracing::info!("subscribed to topics {topics:?}");

        Ok(())
    }

    async fn handle_poll(&mut self) -> Option<()> {
        match self.poll().await {
            Ok(records) => {
                if self.tx.send(records).await.is_err() {
                    // No one is listening for records. Shut down.
                    return None;
                }
            }
            Err(err) => {
                tracing::error!("failed to poll for records: {err}");
                // TODO config
                self.poll_backoff.failure(
                    exponential_backoff(
                        Duration::from_millis(100),
                        Duration::from_secs(10),
                        self.poll_backoff.count(),
                    ),
                    (),
                );
            }
        }

        Some(())
    }

    async fn poll(&mut self) -> Result<Vec<ConsumerRecords>, KafkaChannelError> {
        let may_have_records_next_poll = self.spawn_next().await?;
        let records = self.join_next().await?;

        self.poll_backoff.success();

        if records.is_empty() && !may_have_records_next_poll {
            // We are seeked to the end of all partitions. Delay the next poll.
            // TODO config
            self.poll_backoff
                .schedule_next(Duration::from_millis(500), ());
        }

        Ok(records)
    }

    async fn spawn_next(&mut self) -> Result<bool, KafkaChannelError> {
        self.client
            .load_topic_metadata(self.subscriptions.values())
            .await?;

        let cluster = self.client.borrow_cluster();

        let mut broker_id_to_fetch_req = FnvHashMap::<i32, FetchRequest>::default();
        let mut broker_id_to_offset_req = FnvHashMap::<i32, ListOffsetsRequest>::default();

        let mut spanwed_offset_requests = false;

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

                    spanwed_offset_requests = true;
                }
            }

            for (broker_id, topic) in broker_id_to_fetch_topic {
                let req = broker_id_to_fetch_req.entry(broker_id).or_insert_with(|| {
                    let mut req = FetchRequest::default();
                    req.cluster_id = cluster.metadata.cluster_id.clone();
                    // TODO config
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
            self.join_set
                .spawn(async move { c.send_to(KafkaRequest::from(req), broker_id).await });
        }

        for (broker_id, req) in broker_id_to_offset_req {
            let c = self.client.clone();
            self.join_set
                .spawn(async move { c.send_to(KafkaRequest::from(req), broker_id).await });
        }

        Ok(spanwed_offset_requests)
    }

    async fn join_next(&mut self) -> Result<Vec<ConsumerRecords>, KafkaChannelError> {
        let mut records = Vec::<ConsumerRecords>::new();

        let mut invalid_topics = HashSet::<TopicName>::new();

        loop {
            let event = tokio::select! {
                Some(response) = self.join_set.join_next() => response,
                else => break
            };

            let event = match event {
                Ok(Ok(fetch)) => fetch,
                Ok(Err(e)) => return Err(e),
                _ => continue, // panic when sending request
            };

            match event {
                ResponseKind::FetchResponse(fetch) => {
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
                                .map(|record| record.offset + 1);

                            let next_offset = largest_offset
                                .or(state.offset)
                                .unwrap_or(part.log_start_offset);

                            state.offset.replace(next_offset);

                            if !part_records.is_empty() {
                                records.push(ConsumerRecords {
                                    topic_partition,
                                    records: part_records,
                                    largest_offset,
                                });
                            }
                        }
                    }
                }
                ResponseKind::ListOffsetsResponse(offsets) => {
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
                _ => {
                    // unknown response
                }
            }
        }

        if !invalid_topics.is_empty() {
            self.client.invalidate_topic_metadata(invalid_topics.iter());
        }

        Ok(records)
    }
}

pub struct Consumer {
    rx: mpsc::Receiver<Vec<ConsumerRecords>>,
    tx: mpsc::UnboundedSender<ConsumerCommand>,
}

impl Consumer {
    pub fn new(client: NetworkClient) -> Self {
        let (tx_records, rx_records) = mpsc::channel(1);
        let (tx_commands, rx_commands) = mpsc::unbounded_channel();

        let task = ConsumerTask::new(client, tx_records, rx_commands);

        // TODO: cancellation token
        tokio::spawn(task.run());

        Self {
            rx: rx_records,
            tx: tx_commands,
        }
    }

    pub async fn subscribe(&self, topics: Vec<TopicName>) -> Result<(), KafkaChannelError> {
        let (tx, rx) = oneshot::channel();

        self.tx.send(ConsumerCommand {
            tx,
            kind: ConsumerCommandKind::SubscribeTopics(topics),
        })?;

        rx.await?
    }

    pub async fn next(&mut self) -> Option<Vec<ConsumerRecords>> {
        self.rx.recv().await
    }
}
