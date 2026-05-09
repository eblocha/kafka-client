use itertools::Itertools;
use kafka_protocol::{
    messages::{
        FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
        fetch_request::{FetchPartition, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        list_offsets_response::ListOffsetsPartitionResponse,
    },
    records::RecordBatchDecoder,
};
use rustc_hash::FxHashMap;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    cancel::OrCancelled,
    common::{Node, TopicPartition},
    config::{ConsumerAutoOffsetReset, KafkaConfig},
    conn::{
        broker::task::{BrokerTask, BrokerTaskContext, BrokerTaskHandle},
        selector::ClusterMetadata,
    },
    connect::Connect,
    consumer::record::{ConsumerRecords, ConsumerRecordsResult},
    error::KafkaError,
    network::{handle::NetworkTaskHandle, task::NetworkTask},
};

pub type ConsumerPartitionState = Option<ListOffsetsPartitionResponse>;
pub type ConsumerState = FxHashMap<TopicPartition, ConsumerPartitionState>;

pub struct ConsumerTask<Conn> {
    pub(super) partitions: ConsumerState,
    pub(super) inner_handle: NetworkTaskHandle,
    pub(super) inner_task: NetworkTask<Conn>,
    pub(super) config: KafkaConfig,
    pub(super) tx: mpsc::Sender<ConsumerRecordsResult>,
}

struct PartialConsumerTask {
    inner_handle: NetworkTaskHandle,
    config: KafkaConfig,
    tx: mpsc::Sender<ConsumerRecordsResult>,
}

impl<Conn> ConsumerTask<Conn> {
    fn split(self) -> (NetworkTask<Conn>, ConsumerState, PartialConsumerTask) {
        (
            self.inner_task,
            self.partitions,
            PartialConsumerTask {
                inner_handle: self.inner_handle,
                config: self.config,
                tx: self.tx,
            },
        )
    }

    async fn run_empty(self, ctx: BrokerTaskContext, cluster: ClusterMetadata) -> Option<Self>
    where
        Conn: Connect + Send + 'static,
    {
        let node = self.inner_task.get_node();

        tracing::debug!(
            broker_id = node.id,
            host = ?node.host,
            "running as network task as this broker is not assigned any partitions"
        );

        Some(Self {
            partitions: self.partitions,
            inner_handle: self.inner_handle,
            inner_task: self.inner_task.run(ctx, cluster).await?,
            config: self.config,
            tx: self.tx,
        })
    }
}

impl PartialConsumerTask {
    async fn stop<Conn>(
        self,
        state: ConsumerState,
        join_handle: JoinHandle<Option<NetworkTask<Conn>>>,
        ctx: BrokerTaskContext,
        node: &Node,
        flushing: bool,
    ) -> Option<ConsumerTask<Conn>> {
        if flushing {
            ctx.flush.cancel();
        } else {
            ctx.cancellation_token.cancel();
        }

        let inner_task_result = join_handle.await;

        let inner_task = match inner_task_result {
            Ok(task) => task?,
            Err(e) => {
                if e.is_panic() {
                    tracing::error!(
                        broker_id = node.id,
                        host = ?node.host,
                        "network task panicked {e}"
                    );
                }
                return None;
            }
        };

        Some(ConsumerTask {
            partitions: state,
            inner_handle: self.inner_handle,
            inner_task,
            config: self.config,
            tx: self.tx,
        })
    }
}

impl<Conn: Connect + Send + 'static> BrokerTask for ConsumerTask<Conn> {
    type PartitionState = ConsumerPartitionState;
    type PublicPartitionState = ();

    async fn run(self, ctx: BrokerTaskContext, cluster: ClusterMetadata) -> Option<Self> {
        if self.partitions.is_empty() {
            return self.run_empty(ctx, cluster).await;
        }

        let (inner_task, mut state, this) = self.split();
        let node = inner_task.get_node().clone();

        let sorted_tps = state
            .iter()
            .map(|(tp, _)| tp)
            .sorted()
            .cloned()
            .collect::<Vec<_>>();

        tracing::debug!(
            broker_id = node.id,
            host = ?node.host,
            "subscribed to {} topic partition(s)", sorted_tps.len()
        );

        let network_ctx = BrokerTaskContext {
            cancellation_token: ctx.cancellation_token.clone(),
            // We don't want the flush to propagate to the network task since we still need it to send messages while flushing
            flush: CancellationToken::new(),
            tx: ctx.tx.clone(),
        };
        let network_task_tracker = TaskTracker::new();
        let connection_join_handle =
            network_task_tracker.spawn(inner_task.run(network_ctx.clone(), cluster.clone()));
        network_task_tracker.close();

        let mut flushing = false;

        loop {
            if flushing {
                break;
            }

            if ctx.flush.is_cancelled() {
                flushing = true;
            }

            let (mut fetch_request, list_offsets_topics) =
                build_requests(&sorted_tps, &state, &cluster, &this.config);

            // Fetch state for partitions without state

            if !list_offsets_topics.is_empty() {
                tracing::debug!(
                    broker_id = node.id,
                    host = ?node.host,
                    "listing offsets for {} topic partition(s)", list_offsets_topics.len()
                );

                let list_offsets_request =
                    ListOffsetsRequest::default().with_topics(list_offsets_topics);

                let Some(result) = this
                    .inner_handle
                    .send(list_offsets_request)
                    .or_cancel(&ctx.cancellation_token)
                    .await
                else {
                    break;
                };

                match result {
                    Ok(list_offsets_response) => {
                        update_fetch_request_and_state(
                            &mut state,
                            &mut fetch_request.topics,
                            list_offsets_response,
                            &cluster,
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            broker_id = node.id,
                            host = ?node.host,
                            "failed to send list_offsets request: {e}"
                        );
                    }
                };
            }

            if fetch_request.topics.is_empty() {
                tracing::warn!(
                    broker_id = node.id,
                    host = ?node.host,
                    "no topics to fetch!"
                );

                continue;
            }

            tracing::debug!(
                broker_id = node.id,
                host = ?node.host,
                "sending fetch request for {} topic partition(s)", fetch_request.topics.len()
            );

            let Some(result) = this
                .inner_handle
                .send(fetch_request)
                .or_cancel(&ctx.cancellation_token)
                .await
            else {
                break;
            };

            let consumer_result = result.and_then(|fetch_response| {
                build_records_result_and_update_state(fetch_response, &mut state)
            });

            match consumer_result {
                Ok(ref records) => {
                    tracing::debug!(
                        broker_id = node.id,
                        host = ?node.host,
                        "decoded {} consumer record(s)", count_records(records)
                    );
                }
                Err(ref e) => {
                    tracing::error!(
                        broker_id = node.id,
                        host = ?node.host,
                        "failed to send fetch request: {e}"
                    );
                }
            }

            let Some(Ok(())) = this
                .tx
                .send(consumer_result)
                .or_cancel(&ctx.cancellation_token)
                .await
            else {
                break;
            };
        }

        this.stop(state, connection_join_handle, network_ctx, &node, flushing)
            .await
    }

    async fn shutdown(self) -> Self {
        Self {
            partitions: self.partitions,
            inner_handle: self.inner_handle,
            inner_task: self.inner_task.shutdown().await,
            config: self.config,
            tx: self.tx,
        }
    }

    fn get_node(&self) -> &Node {
        self.inner_task.get_node()
    }

    fn get_node_mut(&mut self) -> &mut Node {
        self.inner_task.get_node_mut()
    }

    fn assign(&mut self, topic_partition: TopicPartition, state: Self::PartitionState) {
        self.partitions.insert(topic_partition, state);
    }

    fn assign_new(&mut self, topic_partition: TopicPartition) -> Self::PublicPartitionState {
        self.assign(topic_partition, ConsumerPartitionState::default());
    }

    fn revoke(&mut self, topic_partition: &TopicPartition) -> Option<Self::PartitionState> {
        self.partitions.remove(topic_partition)
    }

    fn get_assignments(&self) -> Vec<TopicPartition> {
        self.partitions.keys().cloned().collect()
    }
}

/// Build the [`FetchRequest`] and [`Vec<ListOffsetsTopic>`] based on the current [`ConsumerState`].
fn build_requests(
    sorted_tps: &[TopicPartition],
    state: &ConsumerState,
    cluster: &ClusterMetadata,
    config: &KafkaConfig,
) -> (FetchRequest, Vec<ListOffsetsTopic>) {
    let mut fetch_request = FetchRequest::default()
        .with_max_bytes(4096)
        .with_cluster_id(cluster.cluster_id.clone());

    let mut list_offsets_topics = Vec::<ListOffsetsTopic>::new();

    for tp in sorted_tps.iter() {
        // The outer `None` case should be impossible here.
        if let Some(Some(tp_state)) = state.get(tp) {
            // If we have current state, add to the fetch request
            // The partitions are iterated in sorted order, so we only have to look at the last topic
            // to see if we should append or create a new topic
            let last = fetch_request
                .topics
                .last_mut()
                .filter(|last| &last.topic == tp.name());

            let fetch_topic = match last {
                Some(last) => last,
                None => fetch_request.topics.push_mut(
                    FetchTopic::default()
                        .with_topic(tp.name().clone())
                        .with_topic_id(
                            cluster
                                .get_topic_uuid_by_name(tp.name())
                                .unwrap_or_default(),
                        ),
                ),
            };

            let partition = build_fetch_partition(tp_state);

            fetch_topic.partitions.push(partition);

            continue;
        }

        // If no current state, add to the list_offsets request
        // The partitions are iterated in sorted order, so we only have to look at the last topic
        // to see if we should append or create a new topic
        let last = list_offsets_topics
            .last_mut()
            .filter(|last| &last.name == tp.name());

        let list_offsets_topic = match last {
            Some(last) => last,
            None => list_offsets_topics
                .push_mut(ListOffsetsTopic::default().with_name(tp.name().clone())),
        };

        let partition = ListOffsetsPartition::default()
            .with_partition_index(tp.partition())
            .with_timestamp(match config.consumer.auto_offset_reset {
                ConsumerAutoOffsetReset::Earliest => 0,
                ConsumerAutoOffsetReset::Latest => -1,
                ConsumerAutoOffsetReset::None => {
                    unimplemented!("consumer.auto_offset_reset = None is not implemented")
                }
            });

        list_offsets_topic.partitions.push(partition);
    }

    (fetch_request, list_offsets_topics)
}

/// Update the consumer state and topics to be fetched with a response from a [`ListOffsetsRequest`].
fn update_fetch_request_and_state(
    state: &mut ConsumerState,
    sorted_fetch_topics: &mut Vec<FetchTopic>,
    response: ListOffsetsResponse,
    cluster: &ClusterMetadata,
) {
    for topic in response.topics {
        let pos_result =
            sorted_fetch_topics.binary_search_by(|ft| ft.topic.as_str().cmp(topic.name.as_str()));

        if let Err(ins_pos) = pos_result {
            let fetch_topic = FetchTopic::default()
                .with_topic(topic.name.clone())
                .with_topic_id(
                    cluster
                        .get_topic_uuid_by_name(&topic.name)
                        .unwrap_or_default(),
                );
            sorted_fetch_topics.insert(ins_pos, fetch_topic);
        }

        let pos = pos_result.unwrap_or_else(|e| e);
        let fetch_topic = &mut sorted_fetch_topics[pos];

        for partition in topic.partitions {
            let tp = TopicPartition::new(topic.name.clone(), partition.partition_index);
            let fetch_partition = build_fetch_partition(&partition);

            let pos = fetch_topic
                .partitions
                .binary_search_by(|fp| fp.partition.cmp(&partition.partition_index))
                .unwrap_or_else(|ins| ins);

            fetch_topic.partitions.insert(pos, fetch_partition);
            state.insert(tp, Some(partition));
        }
    }
}

fn build_fetch_partition(state: &ListOffsetsPartitionResponse) -> FetchPartition {
    FetchPartition::default()
        .with_current_leader_epoch(state.leader_epoch)
        .with_fetch_offset(state.offset)
        .with_partition(state.partition_index)
}

fn build_records_result_and_update_state(
    fetch_response: FetchResponse,
    state: &mut ConsumerState,
) -> ConsumerRecordsResult {
    if fetch_response.error_code != 0 {
        return Err(KafkaError::ErrorCode(fetch_response.error_code.into()));
    }

    let mut consumer_records = Vec::new();

    for topic in fetch_response.responses {
        for partition_data in topic.partitions {
            if partition_data.error_code != 0 {
                return Err(KafkaError::ErrorCode(partition_data.error_code.into()));
            }

            // TODO: the topic name is empty when we fetch with topic ids.
            let tp = TopicPartition::new(topic.topic.clone(), partition_data.partition_index);

            let Some(mut part_records) = partition_data.records else {
                continue;
            };

            let Ok(part_records) = RecordBatchDecoder::decode(&mut part_records) else {
                continue;
            };

            let largest_offset = part_records
                .records
                .iter()
                .map(|record| record.offset)
                .max();

            // Update the state with the fetch response
            if let Some(Some(state)) = state.get_mut(&tp) {
                let next_offset = largest_offset.map(|o| o + 1).unwrap_or(state.offset);
                state.leader_epoch = partition_data.current_leader.leader_epoch;
                state.offset = next_offset;
            }

            consumer_records.push(ConsumerRecords {
                topic_partition: tp,
                largest_offset,
                records: part_records.records,
            });
        }
    }

    Ok(consumer_records)
}

fn count_records(records: &[ConsumerRecords]) -> usize {
    records
        .iter()
        .fold(0, |count, item| count + item.records.len())
}
