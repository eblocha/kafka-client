use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use kafka_protocol::messages::{
    FetchRequest, ListOffsetsRequest,
    fetch_request::{FetchPartition, FetchTopic},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    list_offsets_response::ListOffsetsPartitionResponse,
};
use tokio::sync::broadcast;

use crate::{
    common::{Node, TopicPartition},
    config::KafkaConfig,
    conn::broker::{
        partition_queue::PartitionQueueMap,
        task::{BrokerTask, BrokerTaskContext, BrokerTaskHandle},
    },
    connect::Connect,
    network::{handle::NetworkTaskHandle, task::NetworkTask},
};

pub struct ConsumerTask<Conn> {
    pub(super) partitions: HashMap<TopicPartition, broadcast::Sender<()>>,
    pub(super) inner_handle: NetworkTaskHandle,
    pub(super) inner_task: NetworkTask<Conn>,
    pub(super) config: KafkaConfig,
}

impl<Conn> ConsumerTask<Conn> {
    async fn run_empty(self, ctx: BrokerTaskContext) -> Option<Self>
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
            inner_task: self.inner_task.run(ctx).await?,
            config: self.config,
        })
    }
}

impl<Conn: Connect + Send + 'static> BrokerTask for ConsumerTask<Conn> {
    type PartitionMessage = ();

    async fn run(self, ctx: BrokerTaskContext) -> Option<Self> {
        if self.partitions.is_empty() {
            return self.run_empty(ctx).await;
        }

        let node = self.inner_task.get_node().clone();
        let state = HashMap::<TopicPartition, ListOffsetsPartitionResponse>::new();

        let sorted_tps = self
            .partitions
            .iter()
            .map(|(tp, _)| tp)
            .sorted_by_key(|tp| tp.name())
            .collect::<Vec<_>>();

        loop {
            let fetch_request = FetchRequest::default().with_max_bytes(4096);

            let mut fetch_topics = Vec::<FetchTopic>::new();
            let mut list_offsets_topics = Vec::<ListOffsetsTopic>::new();

            for tp in sorted_tps.iter() {
                if let Some(tp_state) = state.get(tp) {
                    // If we have current state, add to the fetch request
                    // The partitions are iterated in sorted order, so we only have to look at the last topic
                    // to see if we should append or create a new topic
                    let last = fetch_topics
                        .last_mut()
                        .filter(|last| &last.topic == tp.name());

                    let fetch_topic = match last {
                        Some(last) => last,
                        None => &mut FetchTopic::default().with_topic(tp.name().clone()),
                    };

                    let partition = FetchPartition::default()
                        .with_partition(tp.partition())
                        .with_current_leader_epoch(tp_state.leader_epoch)
                        .with_fetch_offset(tp_state.offset);

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
                    None => &mut ListOffsetsTopic::default().with_name(tp.name().clone()),
                };

                let partition = ListOffsetsPartition::default()
                    .with_partition_index(tp.partition())
                    // -1 means "log_start"
                    // typically this is configured to use log_start, log_end, or error (if in a group)
                    // TODO config
                    .with_timestamp(-1);

                list_offsets_topic.partitions.push(partition);
            }

            // Fetch state for partitions without state

            if !list_offsets_topics.is_empty() {
                let list_offsets_request =
                    ListOffsetsRequest::default().with_topics(list_offsets_topics);
                let result = self.inner_handle.send(list_offsets_request).await;
                match result {
                    Ok(list_offsets_response) => {
                        // TODO add to fetch request and state
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

            let result = self.inner_handle.send(fetch_request).await;

            match result {
                Ok(fetch_response) => {
                    for topic in fetch_response.responses {
                        for partition_data in topic.partitions {
                            let Some(tx) = self.partitions.get(&TopicPartition::new(
                                topic.topic.clone(),
                                partition_data.partition_index,
                            )) else {
                                // Got a response for a partition we did not request
                                continue;
                            };

                            let _ = tx.send(());
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        broker_id = node.id,
                        host = ?node.host,
                        "failed to send fetch request: {e}"
                    );
                    // TODO handle errors
                }
            };
        }
    }

    async fn shutdown(self) -> Self {
        Self {
            partitions: self.partitions,
            inner_handle: self.inner_handle,
            inner_task: self.inner_task.shutdown().await,
            config: self.config,
        }
    }

    fn get_partitions_mut(&mut self) -> &mut PartitionQueueMap<Self::PartitionMessage> {
        todo!()
        // &mut self.partitions
    }

    fn get_node(&self) -> &Node {
        self.inner_task.get_node()
    }

    fn get_node_mut(&mut self) -> &mut Node {
        self.inner_task.get_node_mut()
    }
}
