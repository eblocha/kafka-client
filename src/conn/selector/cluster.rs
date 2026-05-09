use std::{collections::HashMap, sync::Arc, time::Instant};

use arc_swap::ArcSwap;
use kafka_protocol::messages::{
    MetadataResponse, metadata_request::MetadataRequestTopic,
    metadata_response::MetadataResponseBroker,
};
use rustc_hash::FxHashMap;
use tokio::task::{JoinError, JoinSet};

use crate::{
    common::{BrokerHost, Node, TopicPartition},
    config::KafkaConfig,
    conn::{
        broker::{
            connector::NodeConnector,
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

            this.join_set
                .spawn(task.run(ctx, this.cluster.metadata.clone()));
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

                let task = tasks
                    .entry(broker_id)
                    .or_insert_with(|| self.create_new_task(Node::from(*broker)).0);

                match partition_streams.remove(&tp) {
                    Some(s) => {
                        task.assign(tp, s);
                    }
                    None => {
                        let public_state = task.assign_new(tp.clone());
                        self.cluster.partitions.insert(tp, public_state);
                    }
                }
            }
        }

        // Create tasks for new brokers that aren't assigned partitions
        for (broker_id, broker) in new_broker_ids {
            if tasks.contains_key(&broker_id) {
                continue;
            }

            tasks.insert(broker_id, self.create_new_task(Node::from(broker)).0);
        }

        self.cluster
            .metadata
            .update_with(metadata.clone(), Instant::now());

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

            self.join_set
                .spawn(task.run(entry.ctx.clone(), self.cluster.metadata.clone()));
        }

        tracing::debug!("removing {} partitions", partition_streams.len());

        // remove partition queues which no longer exist
        for (tp, _) in partition_streams.drain() {
            self.cluster.partitions.remove(&tp);
        }

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

            self.join_set
                .spawn(dead_task.run(entry.ctx.clone(), self.cluster.metadata.clone()));
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
        FxHashMap<TopicPartition, Task::PartitionState>,
    ) {
        let mut partition_streams: FxHashMap<TopicPartition, Task::PartitionState> =
            HashMap::default();
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

                    (topic_name.as_str(), (topic, partition_map))
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
                let tps = task.get_assignments();
                for part in tps {
                    let stream = task.revoke(&part).expect(
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

            let tps = task.get_assignments();

            // Revoke existing partitions
            for part in tps {
                let Some((topic, partitions)) = requested_topics.get(part.name().as_str()) else {
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

                let stream = task.revoke(&part).expect(
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

#[cfg(test)]
mod test {
    use kafka_protocol::{
        messages::{
            MetadataResponse, TopicName,
            metadata_response::{
                MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
            },
        },
        protocol::StrBytes,
    };
    use uuid::Uuid;

    use crate::{
        common::{BrokerHost, Node, TopicPartition},
        config::KafkaConfig,
        conn::{
            broker::task::{BrokerTask, BrokerTaskContext, BrokerTaskFactory},
            selector::cluster::ClusterTaskManager,
            testing::NeverConnects,
        },
        network::handle::NetworkTaskFactory,
    };

    /// Stop and collect the tasks inside a cluster task manager for inspection
    async fn collect_tasks<
        Conn,
        Task: BrokerTask,
        TaskHandle,
        Factory: BrokerTaskFactory<Conn, Task = Task, Handle = TaskHandle>,
    >(
        cluster_manager: &mut ClusterTaskManager<Conn, Task, TaskHandle, Factory>,
    ) -> Vec<Task> {
        cluster_manager.context.cancellation_token.cancel();

        let mut tasks = Vec::new();

        while let Some(task) = cluster_manager.join_set.join_next().await {
            tasks.push(task.unwrap().unwrap());
        }

        tasks
    }

    fn assert_partition_arrangement<Task: BrokerTask>(
        tasks: Vec<Task>,
        expect_assignments: Vec<Vec<i32>>,
        topic_name: TopicName,
        removed_partitions: Vec<i32>,
    ) {
        let all_partitions = expect_assignments
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();

        for mut task in tasks {
            let node = task.get_node().clone();

            let partitions = &expect_assignments[node.id as usize];

            for partition in &all_partitions {
                let state = task.revoke(&TopicPartition::new(topic_name.clone(), *partition));

                if partitions.contains(&partition) {
                    assert!(
                        state.is_some(),
                        "Node {} was not assigned partition {partition}",
                        node.id
                    );
                } else {
                    assert!(
                        state.is_none(),
                        "Node {} was assigned partition {partition}",
                        node.id
                    );
                }
            }

            for partition in &removed_partitions {
                let state = task.revoke(&TopicPartition::new(topic_name.clone(), *partition));

                assert!(
                    state.is_none(),
                    "Node {} was assigned partition {partition}",
                    node.id
                );
            }
        }
    }

    #[tokio::test]
    async fn test_bootstrap_initializes_cluster() {
        // Arrange ============================================================

        let config = KafkaConfig::default();

        let (context, _rx) = BrokerTaskContext::init(&config);

        let host = BrokerHost("localhost".into(), 9092);

        // Act ================================================================

        let (_cluster_manager, cluster) = ClusterTaskManager::bootstrap(
            &[host.clone()],
            config,
            NeverConnects,
            NetworkTaskFactory,
            context,
        );

        // Assert =============================================================

        let cluster_copy = cluster.load_full();

        assert_eq!(
            cluster_copy.brokers.list_nodes(),
            vec![&Node {
                id: 0,
                host,
                rack: None
            }]
        );
    }

    #[tokio::test]
    async fn test_update_reassigns_tasks() {
        // Arrange ============================================================
        let config = KafkaConfig::default();

        let (context, _rx) = BrokerTaskContext::init(&config);

        let (mut cluster_manager, _cluster) = ClusterTaskManager::bootstrap(
            &[
                // Should get removed
                BrokerHost("localhost".into(), 9094),
                // Should become broker id 0
                BrokerHost("localhost".into(), 9093),
            ],
            config,
            NeverConnects,
            NetworkTaskFactory,
            context.child_context(),
        );

        // Act ================================================================

        // New broker not in bootstrap servers
        let broker1 = MetadataResponseBroker::default()
            .with_host(StrBytes::from_static_str("localhost"))
            .with_node_id(1.into())
            .with_port(9092);

        let broker2 = MetadataResponseBroker::default()
            .with_host(StrBytes::from_static_str("localhost"))
            .with_node_id(0.into())
            .with_port(9093);

        let metadata = MetadataResponse::default()
            .with_brokers(vec![broker1, broker2])
            .with_controller_id(1.into());

        cluster_manager.update_metadata(metadata).await;

        // Assert =============================================================

        let tasks = collect_tasks(&mut cluster_manager).await;

        assert_eq!(tasks.len(), 2);

        for task in &tasks {
            let node = task.get_node();

            if node.id == 0 {
                assert_eq!(node.host, BrokerHost("localhost".into(), 9093));
            } else {
                assert_eq!(node.host, BrokerHost("localhost".into(), 9092));
            }
        }

        let topic_name: TopicName = StrBytes::from_static_str("never").into();

        // Neither should have any partition assignments
        assert_partition_arrangement(tasks, vec![vec![], vec![]], topic_name, vec![]);
    }

    #[tokio::test]
    async fn test_partition_assignment() {
        // Arrange ============================================================

        let config = KafkaConfig::default();

        let (context, _rx) = BrokerTaskContext::init(&config);

        let (mut cluster_manager, _cluster) = ClusterTaskManager::bootstrap(
            &[
                BrokerHost("localhost".into(), 9092),
                BrokerHost("localhost".into(), 9093),
            ],
            config,
            NeverConnects,
            NetworkTaskFactory,
            context.child_context(),
        );

        // Act ================================================================

        let broker1 = MetadataResponseBroker::default()
            .with_host(StrBytes::from_static_str("localhost"))
            .with_node_id(0.into())
            .with_port(9092);

        let broker2 = MetadataResponseBroker::default()
            .with_host(StrBytes::from_static_str("localhost"))
            .with_node_id(1.into())
            .with_port(9093);

        // Leader: broker2
        let partition1 = MetadataResponsePartition::default()
            .with_leader_id(1.into())
            .with_partition_index(0);

        // Leader: broker1
        let partition2 = MetadataResponsePartition::default()
            .with_leader_id(0.into())
            .with_partition_index(1);

        let topic_name: TopicName = StrBytes::from_static_str("topic").into();

        let topic = MetadataResponseTopic::default()
            .with_name(Some(topic_name.clone()))
            .with_topic_id(Uuid::max())
            .with_partitions(vec![partition1, partition2]);

        let metadata = MetadataResponse::default()
            .with_brokers(vec![broker1, broker2])
            .with_controller_id(0.into())
            .with_topics(vec![topic]);

        cluster_manager.update_metadata(metadata).await;

        // Assert =============================================================

        let tasks = collect_tasks(&mut cluster_manager).await;

        assert_eq!(tasks.len(), 2);

        assert_partition_arrangement(tasks, vec![vec![1], vec![0]], topic_name, vec![]);
    }

    #[tokio::test]
    async fn test_partition_moving_leader() {
        // Arrange ============================================================
        let config = KafkaConfig::default();

        let (context, _rx) = BrokerTaskContext::init(&config);

        let (mut cluster_manager, _cluster) = ClusterTaskManager::bootstrap(
            &[],
            config,
            NeverConnects,
            NetworkTaskFactory,
            context.child_context(),
        );

        let broker1 = MetadataResponseBroker::default()
            .with_host(StrBytes::from_static_str("localhost"))
            .with_node_id(0.into())
            .with_port(9092);

        let broker2 = MetadataResponseBroker::default()
            .with_host(StrBytes::from_static_str("localhost"))
            .with_node_id(1.into())
            .with_port(9093);

        // Leader: broker2
        let partition1 = MetadataResponsePartition::default()
            .with_leader_id(1.into())
            .with_partition_index(0);

        // Leader: broker1
        let partition2 = MetadataResponsePartition::default()
            .with_leader_id(0.into())
            .with_partition_index(1);

        let topic_name: TopicName = StrBytes::from_static_str("topic").into();

        let topic = MetadataResponseTopic::default()
            .with_name(Some(topic_name.clone()))
            .with_topic_id(Uuid::max())
            .with_partitions(vec![partition1, partition2]);

        let metadata = MetadataResponse::default()
            .with_brokers(vec![broker1.clone(), broker2.clone()])
            .with_controller_id(0.into())
            .with_topics(vec![topic]);

        // update with initial metadata
        cluster_manager.update_metadata(metadata).await;

        // Act ================================================================

        // swap partitions 0 and 1
        // Leader: broker1
        let partition1 = MetadataResponsePartition::default()
            .with_leader_id(0.into())
            .with_partition_index(0);

        // Leader: broker2
        let partition2 = MetadataResponsePartition::default()
            .with_leader_id(1.into())
            .with_partition_index(1);

        let topic_name: TopicName = StrBytes::from_static_str("topic").into();

        let topic = MetadataResponseTopic::default()
            .with_name(Some(topic_name.clone()))
            .with_topic_id(Uuid::max())
            .with_partitions(vec![partition1, partition2]);

        let metadata = MetadataResponse::default()
            .with_brokers(vec![broker1, broker2])
            .with_controller_id(0.into())
            .with_topics(vec![topic]);

        // update with new metadata
        cluster_manager.update_metadata(metadata).await;

        // Assert =============================================================

        let tasks = collect_tasks(&mut cluster_manager).await;

        assert_eq!(tasks.len(), 2);

        assert_partition_arrangement(tasks, vec![vec![0], vec![1]], topic_name, vec![]);
    }

    #[tokio::test]
    async fn test_partition_moving_leader_leaving_empty_node() {
        // Arrange ============================================================
        let config = KafkaConfig::default();

        let (context, _rx) = BrokerTaskContext::init(&config);

        let (mut cluster_manager, _cluster) = ClusterTaskManager::bootstrap(
            &[],
            config,
            NeverConnects,
            NetworkTaskFactory,
            context.child_context(),
        );

        let broker1 = MetadataResponseBroker::default()
            .with_host(StrBytes::from_static_str("localhost"))
            .with_node_id(0.into())
            .with_port(9092);

        let broker2 = MetadataResponseBroker::default()
            .with_host(StrBytes::from_static_str("localhost"))
            .with_node_id(1.into())
            .with_port(9093);

        // Leader: broker1
        let partition1 = MetadataResponsePartition::default()
            .with_leader_id(0.into())
            .with_partition_index(0);

        // Leader: broker2
        let partition2 = MetadataResponsePartition::default()
            .with_leader_id(1.into())
            .with_partition_index(1);

        let topic_name: TopicName = StrBytes::from_static_str("topic").into();

        let topic = MetadataResponseTopic::default()
            .with_name(Some(topic_name.clone()))
            .with_topic_id(Uuid::max())
            .with_partitions(vec![partition1, partition2]);

        let metadata = MetadataResponse::default()
            .with_brokers(vec![broker1.clone(), broker2.clone()])
            .with_controller_id(0.into())
            .with_topics(vec![topic]);

        // update with initial metadata
        cluster_manager.update_metadata(metadata).await;

        // Act ================================================================

        // Leader: broker1
        let partition1 = MetadataResponsePartition::default()
            .with_leader_id(0.into())
            .with_partition_index(1);

        // Leader: broker1
        let partition2 = MetadataResponsePartition::default()
            .with_leader_id(0.into())
            .with_partition_index(0);

        let topic_name: TopicName = StrBytes::from_static_str("topic").into();

        let topic = MetadataResponseTopic::default()
            .with_name(Some(topic_name.clone()))
            .with_topic_id(Uuid::max())
            .with_partitions(vec![partition1, partition2]);

        let metadata = MetadataResponse::default()
            .with_brokers(vec![broker1, broker2])
            .with_controller_id(0.into())
            .with_topics(vec![topic]);

        // update with new metadata
        cluster_manager.update_metadata(metadata).await;

        // Assert =============================================================

        let tasks = collect_tasks(&mut cluster_manager).await;

        assert_eq!(tasks.len(), 2);

        assert_partition_arrangement(tasks, vec![vec![0, 1], vec![]], topic_name, vec![]);
    }

    #[tokio::test]
    async fn test_removed_partition() {
        // Arrange ============================================================
        let config = KafkaConfig::default();

        let (context, _rx) = BrokerTaskContext::init(&config);

        let (mut cluster_manager, _cluster) = ClusterTaskManager::bootstrap(
            &[],
            config,
            NeverConnects,
            NetworkTaskFactory,
            context.child_context(),
        );

        let broker1 = MetadataResponseBroker::default()
            .with_host(StrBytes::from_static_str("localhost"))
            .with_node_id(0.into())
            .with_port(9092);

        let broker2 = MetadataResponseBroker::default()
            .with_host(StrBytes::from_static_str("localhost"))
            .with_node_id(1.into())
            .with_port(9093);

        // Leader: broker1
        let partition1 = MetadataResponsePartition::default()
            .with_leader_id(0.into())
            .with_partition_index(0);

        // Leader: broker2
        let partition2 = MetadataResponsePartition::default()
            .with_leader_id(1.into())
            .with_partition_index(1);

        let topic_name: TopicName = StrBytes::from_static_str("topic").into();

        let topic = MetadataResponseTopic::default()
            .with_name(Some(topic_name.clone()))
            .with_topic_id(Uuid::max())
            .with_partitions(vec![partition1, partition2]);

        let metadata = MetadataResponse::default()
            .with_brokers(vec![broker1.clone(), broker2.clone()])
            .with_controller_id(0.into())
            .with_topics(vec![topic]);

        // update with initial metadata
        cluster_manager.update_metadata(metadata).await;

        // Act ================================================================

        // Leader: broker1
        let partition1 = MetadataResponsePartition::default()
            .with_leader_id(0.into())
            .with_partition_index(0);

        let topic_name: TopicName = StrBytes::from_static_str("topic").into();

        let topic = MetadataResponseTopic::default()
            .with_name(Some(topic_name.clone()))
            .with_topic_id(Uuid::max())
            .with_partitions(vec![partition1]);

        let metadata = MetadataResponse::default()
            .with_brokers(vec![broker1, broker2])
            .with_controller_id(0.into())
            .with_topics(vec![topic]);

        // update with new metadata
        cluster_manager.update_metadata(metadata).await;

        // Assert =============================================================

        let tasks = collect_tasks(&mut cluster_manager).await;

        assert_eq!(tasks.len(), 2);

        assert_partition_arrangement(tasks, vec![vec![0], vec![]], topic_name, vec![1]);
    }
}
