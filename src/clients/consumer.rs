use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use kafka_protocol::{
    messages::{
        fetch_request::{FetchPartition, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        metadata_request::MetadataRequestTopic,
        FetchRequest, ListOffsetsRequest, ResponseKind, TopicName,
    },
    records::Record,
};
use rustc_hash::FxHashMap;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

use crate::{
    backoff::{exponential_backoff, BackoffSession},
    clients::network::NetworkClient,
    common::TopicPartition,
    conn::{selector::TopicKey, RecordBatchDecoder},
    error::KafkaError,
    proto::{error_codes::ErrorCode, request::KafkaRequest},
    util::TopicNameExt,
};

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
    tx: oneshot::Sender<Result<(), KafkaError>>,
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
    subscriptions: HashSet<TopicName>,
    invalid_topics: HashSet<TopicName>,
    join_set: JoinSet<Result<ResponseKind, KafkaError>>,
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
            subscriptions: HashSet::new(),
            invalid_topics: HashSet::new(),
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

        tracing::debug!("consumer shut down gracefully");
    }

    async fn handle_command(&mut self, command: ConsumerCommand) -> Option<()> {
        if command.tx.is_closed() {
            return Some(());
        }

        match command.kind {
            ConsumerCommandKind::SubscribeTopics(ref topics) => {
                let result = self.subscribe(topics);
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
            () = self.client.await_shutdown() => ConsumerTaskEvent::Shutdown,
            () = self.tx.closed() => ConsumerTaskEvent::Shutdown,
            command = self.rx.recv() => command.map_or(ConsumerTaskEvent::Shutdown, ConsumerTaskEvent::Command),
            _ = self.poll_backoff.wait_next() => ConsumerTaskEvent::Poll,
        }
    }

    fn subscribe(&mut self, topics: &[TopicName]) -> Result<(), KafkaError> {
        self.invalid_topics = self
            .client
            .get_missing_topic_names(topics)
            .into_iter()
            .collect();

        self.subscriptions = topics.iter().cloned().collect();

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

    async fn poll(&mut self) -> Result<Vec<ConsumerRecords>, KafkaError> {
        self.refresh_topics().await?;
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

    async fn spawn_next(&mut self) -> Result<bool, KafkaError> {
        let cluster = &self.client.borrow_cluster().metadata;

        let mut broker_id_to_fetch_req = FxHashMap::<i32, FetchRequest>::default();
        let mut broker_id_to_offset_req = FxHashMap::<i32, ListOffsetsRequest>::default();

        let mut spanwed_offset_requests = false;

        for topic_name in &self.subscriptions {
            let (topic_key, topic_meta) =
                match cluster.get_topic_metadata_and_key_by_name(topic_name) {
                    Ok(meta) => meta,
                    Err(e) => {
                        tracing::error!(
                            "error fetching metadata for topic {}: {e}",
                            topic_name.0.as_str(),
                        );
                        self.invalid_topics.insert(topic_name.clone());
                        continue;
                    }
                };
            let mut broker_id_to_fetch_topic = FxHashMap::<i32, FetchTopic>::default();
            let mut broker_id_to_offset_topic = FxHashMap::<i32, ListOffsetsTopic>::default();

            for (partition_index, partition_meta) in topic_meta.iter_partition_results().enumerate()
            {
                let tp = TopicPartition::new(topic_name.clone(), partition_index as i32);

                let Ok(partition_meta) = partition_meta else {
                    tracing::error!("error fetching metadata for topic partition {tp}");
                    self.invalid_topics.insert(topic_name.clone());
                    continue;
                };

                let state = self.states.entry(tp).or_default();

                if let Some(offset) = state.offset {
                    let fetch_topic = broker_id_to_fetch_topic
                        .entry(partition_meta.leader_id)
                        .or_insert_with(|| {
                            let mut fetch_topic = FetchTopic::default();
                            fetch_topic.topic = topic_name.clone();
                            if let TopicKey::Uuid(uuid) = topic_key {
                                fetch_topic.topic_id = *uuid;
                            }
                            fetch_topic
                        });

                    fetch_topic.partitions.push(
                        FetchPartition::default()
                            .with_current_leader_epoch(partition_meta.leader_epoch)
                            .with_partition(partition_meta.index)
                            .with_fetch_offset(offset),
                    );
                } else {
                    let offsets_topic = broker_id_to_offset_topic
                        .entry(partition_meta.leader_id)
                        .or_insert_with(|| {
                            ListOffsetsTopic::default().with_name(topic_name.clone())
                        });

                    offsets_topic.partitions.push(
                        ListOffsetsPartition::default()
                            .with_partition_index(partition_meta.index)
                            .with_timestamp(-1),
                    );

                    spanwed_offset_requests = true;
                }
            }

            for (broker_id, topic) in broker_id_to_fetch_topic {
                broker_id_to_fetch_req
                    .entry(broker_id)
                    .or_insert_with(|| {
                        FetchRequest::default()
                            .with_cluster_id(cluster.cluster_id.clone())
                            .with_min_bytes(4096)
                    })
                    .topics
                    .push(topic);
            }

            for (broker_id, topic) in broker_id_to_offset_topic {
                let req = broker_id_to_offset_req.entry(broker_id).or_default();
                req.topics.push(topic);
            }
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

    async fn join_next(&mut self) -> Result<Vec<ConsumerRecords>, KafkaError> {
        let mut records = Vec::<ConsumerRecords>::new();

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
                ResponseKind::Fetch(fetch) => {
                    for response in fetch.responses {
                        let topic_name = if response.topic.is_empty() {
                            let cluster = &self.client.borrow_cluster().metadata;
                            let Some(name) = cluster
                                .get_topic_metadata(&TopicKey::Uuid(response.topic_id))
                                .ok()
                                .map(|meta| meta.name.clone())
                            else {
                                continue;
                            };
                            name
                        } else {
                            response.topic
                        };

                        if !self.subscriptions.contains(&topic_name) {
                            // Not subscribed
                            continue;
                        }

                        for part in response.partitions {
                            let error_code: ErrorCode = part.error_code.into();

                            match error_code {
                                ErrorCode::InvalidTopicException
                                | ErrorCode::ReassignmentInProgress
                                | ErrorCode::FencedLeaderEpoch
                                | ErrorCode::UnknownLeaderEpoch
                                | ErrorCode::StaleBrokerEpoch
                                | ErrorCode::NotLeaderOrFollower => {
                                    self.invalid_topics.insert(topic_name.clone());
                                    continue;
                                }
                                _ => {}
                            }

                            let topic_partition =
                                TopicPartition::new(topic_name.clone(), part.partition_index);

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
                ResponseKind::ListOffsets(offsets) => {
                    for top in offsets.topics {
                        for part in top.partitions {
                            if part.error_code != ErrorCode::None as i16 {
                                tracing::error!(
                                    "failed to determine existing offsets for {}:{}, error: {}",
                                    top.name.as_str(),
                                    part.partition_index,
                                    ErrorCode::from(part.error_code)
                                );
                                continue;
                            }

                            let topic_partition =
                                TopicPartition::new(top.name.clone(), part.partition_index);

                            self.states.insert(
                                topic_partition,
                                PartitionState {
                                    offset: Some(part.offset),
                                },
                            );
                        }
                    }
                }
                response => {
                    tracing::warn!(
                        "consumer decoded an unexpected response: {response:?}. ignoring"
                    );
                }
            }
        }

        Ok(records)
    }

    async fn refresh_topics(&mut self) -> Result<(), KafkaError> {
        let to_refresh = self
            .invalid_topics
            .drain()
            .map(|top| MetadataRequestTopic::default().with_name(Some(top)))
            .collect::<Vec<_>>();

        self.client.load_topic_metadata(to_refresh).await
    }
}

pub struct Consumer {
    rx: mpsc::Receiver<Vec<ConsumerRecords>>,
    tx: mpsc::UnboundedSender<ConsumerCommand>,
    client: NetworkClient,
}

impl Consumer {
    #[must_use]
    pub fn new(client: NetworkClient) -> Self {
        let (tx_records, rx_records) = mpsc::channel(1);
        let (tx_commands, rx_commands) = mpsc::unbounded_channel();

        let task = ConsumerTask::new(client.clone(), tx_records, rx_commands);

        tokio::spawn(task.run());

        Self {
            rx: rx_records,
            tx: tx_commands,
            client,
        }
    }

    pub async fn subscribe(&self, topics: Vec<String>) -> Result<(), KafkaError> {
        let (tx, rx) = oneshot::channel();

        let topics = topics.into_iter().map(TopicName::from_string).collect();

        self.tx.send(ConsumerCommand {
            tx,
            kind: ConsumerCommandKind::SubscribeTopics(topics),
        })?;

        rx.await?
    }

    pub async fn next(&mut self) -> Option<Vec<ConsumerRecords>> {
        self.rx.recv().await
    }

    pub async fn shutdown(&self) {
        self.client.shutdown().await;
    }

    pub async fn await_shutdown(&self) {
        self.client.await_shutdown().await;
    }
}
