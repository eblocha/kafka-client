use std::{collections::HashMap, sync::Arc, time::Instant};

use arc_swap::ArcSwap;
use kafka_protocol::messages::{
    metadata_request::MetadataRequestTopic, metadata_response::MetadataResponseBroker,
    MetadataResponse,
};
use rustc_hash::FxHashMap;
use tokio::{
    sync::mpsc,
    task::{JoinError, JoinSet},
};

use crate::{
    common::{BrokerHost, Node, TopicPartition},
    config::KafkaConfig,
    conn::{
        broker::{
            connector::NodeConnector,
            partition_queue::PartitionQueue,
            task::{BrokerTask, BrokerTaskContext, BrokerTaskFactory, BrokerTaskHandle},
        },
        selector::{BrokerMapEntry, Cluster},
    },
    connect::Connect,
    error::ErrorCode,
};

/// Synchronizes broker tasks with cluster state.
///
/// This will ensure each broker id in the cluster has a corresponding task running which can communicate
/// with the broker.
///
/// It will also enure each partition queue is assigned to the correct broker task.
pub struct ClusterTaskManager<
    Conn,
    Task: BrokerTask,
    TaskHandle,
    Factory: BrokerTaskFactory<Conn, Task = Task, Handle = TaskHandle>,
> {
    /// Configuration
    config: KafkaConfig,
    /// Shared global cluster state. Contains the latest metadata and mapping of broker id to connection
    shared_cluster: Arc<ArcSwap<Cluster<Task, TaskHandle>>>,
    /// Local cluster state to maintain the sender refcount
    cluster: Cluster<Task, TaskHandle>,
    /// Join set for running connection tasks. Used to detect failed connections
    join_set: JoinSet<Option<Task>>,
    /// Used to create new tcp streams
    connect: Conn,
    /// Used to spawn auxiliary tasks specific to a broker id.
    task_factory: Factory,
    /// Context used to create new broker tasks
    context: BrokerTaskContext,
}

impl<
        Conn,
        Task: BrokerTask,
        TaskHandle,
        Factory: BrokerTaskFactory<Conn, Task = Task, Handle = TaskHandle>,
    > ClusterTaskManager<Conn, Task, TaskHandle, Factory>
{
    /// Shut down all tasks
    ///
    /// Returns `true` if all tasks exited cleanly
    pub async fn await_shutdown(mut self) -> bool {
        self.shared_cluster.store(Arc::default());

        drop(self.cluster);

        let mut clean = true;

        while let Some(result) = self.join_set.join_next().await {
            match result {
                Ok(Some(task)) => {
                    task.shutdown().await;
                }
                Err(join_err) if join_err.is_panic() => {
                    clean = false;
                    tracing::error!("a broker connection task stopped with an error: {join_err}");
                }
                _ => {}
            };
        }

        clean
    }

    pub fn create_topics_for_refresh(&self) -> Vec<MetadataRequestTopic> {
        self.cluster.metadata.create_topics_for_refresh()
    }
}

impl<
        Conn: Connect + Clone,
        Task: BrokerTask,
        TaskHandle: BrokerTaskHandle,
        Factory: BrokerTaskFactory<Conn, Task = Task, Handle = TaskHandle>,
    > ClusterTaskManager<Conn, Task, TaskHandle, Factory>
{
    /// Create a new cluster manager from bootstrap servers.
    ///
    /// This will return both the manager itself and the initial cluster state.
    pub fn bootstrap(
        servers: &[BrokerHost],
        config: KafkaConfig,
        connect: Conn,
        task_factory: Factory,
        context: BrokerTaskContext,
    ) -> (Self, Arc<ArcSwap<Cluster<Task, TaskHandle>>>) {
        let mut this = Self {
            config,
            cluster: Cluster::default(),
            shared_cluster: Arc::default(),
            join_set: JoinSet::new(),
            connect,
            task_factory,
            context,
        };

        for (id, host) in servers.iter().enumerate() {
            let node = Node {
                id: id as i32,
                host: host.clone(),
                rack: None,
            };

            let (task, ctx) = this.create_new_task(node);

            this.join_set.spawn(task.run(ctx));
        }

        let shared_cluster = this.shared_cluster.clone();

        shared_cluster.store(Arc::new(this.cluster.clone()));

        (this, shared_cluster)
    }

    pub fn get_best_connection(&self) -> Option<BrokerMapEntry<TaskHandle>> {
        self.cluster.brokers.get_best_connection()
    }

    /// Wait for the next broker task to stop.
    pub async fn join_next(&mut self) -> Option<Result<Option<Task>, JoinError>> {
        self.join_set.join_next().await
    }

    pub async fn update_metadata(
        &mut self,
        metadata: MetadataResponse,
    ) -> &Cluster<Task, TaskHandle> {
        if metadata.brokers.is_empty() {
            tracing::warn!("metadata response has no brokers, ignoring");
        }

        // mapping of broker id to broker host information in the new metadata
        let new_broker_ids: FxHashMap<_, _> = metadata
            .brokers
            .iter()
            .map(|broker| (broker.node_id.0, broker))
            .collect();

        // Remove nodes that are not in the cluster
        self.cluster.brokers.retain(|entry| {
            let keep = new_broker_ids.contains_key(&entry.node.id);
            // Stop the broker task but keep the connection alive
            entry.ctx.cancellation_token.cancel();

            if !keep {
                tracing::debug!(
                    broker_id = entry.node.id,
                    host = ?entry.node.host,
                    "removing broker task"
                );
            }

            keep
        });

        let (mut tasks, mut partition_streams) =
            self.collect_current_tasks(&new_broker_ids, &metadata).await;

        // Assign topic partitions
        for topic in &metadata.topics {
            let Some(ref topic_name) = topic.name else {
                // No topic name means it was requested by id but does not exist
                continue;
            };

            for partition in &topic.partitions {
                if partition.error_code != ErrorCode::None as i16 {
                    continue;
                }

                let broker_id = partition.leader_id.0;

                let Some(broker) = new_broker_ids.get(&broker_id) else {
                    tracing::warn!(
                        broker_id = broker_id,
                        topic_name = ?topic_name,
                        partition = partition.partition_index,
                        "found a partition which refers to a leader_id not in the cluster"
                    );
                    continue;
                };

                let tp = TopicPartition::new(topic_name.clone(), partition.partition_index);

                let stream = match partition_streams.remove(&tp) {
                    Some(s) => s,
                    None => {
                        let (tx, rx) = mpsc::channel(self.config.producer.batch_count);
                        self.cluster.partitions.insert(tp.clone(), tx);
                        PartitionQueue::new(rx)
                    }
                };

                let task = tasks
                    .entry(broker_id)
                    .or_insert_with(|| self.create_new_task(Node::from(*broker)).0);

                // Assign this partition to the broker task
                task.get_partitions_mut().insert(tp, stream);
            }
        }

        // Create tasks for new brokers that aren't assigned partitions
        for (broker_id, broker) in new_broker_ids {
            if tasks.contains_key(&broker_id) {
                continue;
            }

            tasks.insert(broker_id, self.create_new_task(Node::from(broker)).0);
        }

        // Restart each task
        for (broker_id, task) in tasks {
            let Some(entry) = self.cluster.brokers.get_mut(&broker_id) else {
                tracing::error!(
                    broker_id = broker_id,
                    "detected a broker task with no handle"
                );
                task.shutdown().await;
                continue;
            };

            let cancellation_token = self.context.cancellation_token.child_token();
            entry.ctx.cancellation_token = cancellation_token.clone();
            let node = task.get_node();

            tracing::debug!(
                broker_id = node.id,
                host = ?node.host,
                "starting broker task"
            );

            self.join_set.spawn(task.run(entry.ctx.clone()));
        }

        tracing::debug!("removing {} partitions", partition_streams.len());

        // remove partition queues which no longer exist
        for (tp, _) in partition_streams.drain() {
            self.cluster.partitions.remove(&tp);
        }

        self.cluster
            .metadata
            .update_with(metadata.clone(), Instant::now());

        self.shared_cluster.store(Arc::new(self.cluster.clone()));

        &self.cluster
    }

    pub async fn restart_if_needed(&mut self, mut dead_task: Task) {
        let task_node = dead_task.get_node();

        if let Some(mut entry) = self.cluster.brokers.remove(&task_node.id) {
            tracing::debug!(
                broker_id = task_node.id,
                host = ?entry.node.host,
                "restarting connection handle",
            );

            // create new cancellation token to not immediately exit when the task starts
            let cancellation_token = self.context.cancellation_token.child_token();
            entry.ctx.cancellation_token = cancellation_token.clone();

            // if the host is different, stop the existing connection
            if task_node.host != entry.node.host {
                tracing::debug!(
                    host = ?task_node.host,
                    broker_id = task_node.id,
                    "stopping existing connection"
                );
                dead_task = dead_task.shutdown().await;
            }

            *dead_task.get_node_mut() = entry.node.clone();

            self.join_set.spawn(dead_task.run(entry.ctx.clone()));
            self.cluster.brokers.insert(entry);
        }
    }

    fn create_new_task(&mut self, node: Node) -> (Task, BrokerTaskContext) {
        tracing::debug!(
            broker_id = node.id,
            host = ?node.host,
            "creating new connection task"
        );

        let connector = NodeConnector::new(node.clone(), self.config.clone(), self.connect.clone());

        let (handle, task) = self.task_factory.new_task(connector);

        let ctx = self.context.child_context();

        self.cluster.brokers.insert(BrokerMapEntry {
            node,
            handle,
            ctx: ctx.clone(),
        });

        (task, ctx)
    }

    async fn collect_current_tasks(
        &mut self,
        new_broker_ids: &FxHashMap<i32, &MetadataResponseBroker>,
        metadata: &MetadataResponse,
    ) -> (
        FxHashMap<i32, Task>,
        FxHashMap<TopicPartition, PartitionQueue<Task::PartitionMessage>>,
    ) {
        let mut partition_streams: FxHashMap<
            TopicPartition,
            PartitionQueue<Task::PartitionMessage>,
        > = HashMap::default();
        let mut tasks: FxHashMap<i32, Task> = Default::default();

        let requested_topics = metadata
            .topics
            .iter()
            .filter_map(|topic| {
                topic.name.as_ref().map(|topic_name| {
                    let partition_map = topic
                        .partitions
                        .iter()
                        .map(|part| (part.partition_index, part))
                        .collect::<FxHashMap<_, _>>();

                    (topic_name, (topic, partition_map))
                })
            })
            .collect::<FxHashMap<_, _>>();

        while let Some(task_result) = self.join_set.join_next().await {
            let mut task = match task_result {
                Ok(Some(task)) => task,
                Ok(None) => continue,
                Err(e) => {
                    tracing::error!("broker task stopped unexpectedly: {e}");
                    continue;
                }
            };

            let current_node = task.get_node();
            let id = current_node.id;
            let current_host = current_node.host.clone();

            tracing::debug!(
                broker_id = id,
                host = ?current_host,
                "stopped broker task"
            );

            let Some(broker) = new_broker_ids.get(&id) else {
                // This broker is no longer in the cluster. Stop the connection to its host and collect its partitions.
                let parts = task.get_partitions_mut();
                let tps = parts.keys().cloned().collect::<Vec<_>>();
                for part in tps {
                    let stream = parts.remove(&part).expect(
                        "expected partition stream to exist since we are iterating over known keys",
                    );
                    partition_streams.insert(part, stream);
                }

                task.shutdown().await;
                continue;
            };

            let new_node = Node::from(*broker);

            if current_host != new_node.host {
                tracing::debug!(
                    broker_id = id,
                    host = ?current_host,
                    new_host = ?new_node.host,
                    "changing hosts"
                );
                // Stop the existing connection
                task = task.shutdown().await;
            }

            let parts = task.get_partitions_mut();
            let tps = parts.keys().cloned().collect::<Vec<_>>();

            // Revoke existing partitions
            for part in tps {
                let Some((topic, partitions)) = requested_topics.get(part.name()) else {
                    // Topic was not requested
                    continue;
                };

                if topic.error_code != ErrorCode::None as i16
                    && topic.error_code != ErrorCode::UnknownTopicOrPartition as i16
                {
                    // Topic has an error, but not an unknown topic error
                    continue;
                }

                if let Some(part_meta) = partitions.get(&part.partition()) {
                    if part_meta.error_code != ErrorCode::None as i16 {
                        // Partition has an error
                        continue;
                    }

                    if part_meta.leader_id == id {
                        // This broker is the leader of the partition
                        continue;
                    }
                }
                // else -> partition does not exist

                let stream = parts.remove(&part).expect(
                    "expected partition stream to exist since we are iterating over known keys",
                );
                partition_streams.insert(part, stream);
            }

            *task.get_node_mut() = Node::from(*broker);
            tasks.insert(id, task);
        }

        (tasks, partition_streams)
    }
}
