use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use kafka_protocol::messages::{
    metadata_request::MetadataRequestTopic, metadata_response::MetadataResponseBroker,
    MetadataResponse, TopicName,
};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    backoff::{exponential_backoff, BackoffSession},
    common::{BrokerHost, Node, TopicPartition},
    config::KafkaConfig,
    conn::{
        broker::{
            connector::NodeConnector,
            init_error::ConnectionInitError,
            task::{
                BrokerTask, BrokerTaskContext, BrokerTaskFactory, BrokerTaskHandle, PartitionQueue,
            },
        },
        connect::{Connect, Tcp},
        selector::{
            cluster::BrokerMapEntry,
            metadata::{MetadataRefreshContext, MetadataRefreshTask},
        },
    },
    error::{ErrorCode, KafkaError},
};

use super::{
    cluster::{BrokerMap, Cluster},
    metadata::MetadataRefreshResult,
};

/// A request to fetch metadata for a specific set of topics, or all topics
pub struct RefreshMetadataRequest {
    /// The requested topics
    ///
    /// If None, this will fetch metadata for all topics.
    ///
    /// If Some(vec![]), this will only refresh the cluster metadata
    pub topics: Option<Vec<MetadataRequestTopic>>,
    /// Channel which sends when the request has been executed
    pub tx: oneshot::Sender<Result<(), KafkaError>>,
}

/// Keeps connections to each broker alive.
///
/// This task will listen for changes to metadata from the [`MetadataRefreshTaskHandle`], then start/stop
/// broker handles as necessary to keep a valid mapping of broker id to host connection.
///
/// If any nodes fail to connect or close their connection unexpectedly, this task will re-spawn those connection
/// tasks to keep connections alive.
///
/// If a task panics, then pending requests for the broker will recieve a [`crate::conn::KafkaConnectionError::Closed`]
/// error, and a new task and handle pair will be created and spawned.
struct SelectorTask<
    Conn,
    Task: BrokerTask,
    TaskHandle,
    Factory: BrokerTaskFactory<Conn, Task = Task, Handle = TaskHandle>,
> {
    /// Mapping of broker id to its host
    hosts: BrokerMap<TaskHandle>,
    /// Shared global cluster state. Contains the latest metadata and mapping of broker id to connection
    cluster: Arc<ArcSwap<Cluster<Task, TaskHandle>>>,
    /// Join set for running connection tasks. Used to detect failed connections
    join_set: JoinSet<Task>,
    /// Configuration
    config: KafkaConfig,
    /// Receiver for requests to refresh metadata now
    rx_topic_metadata: mpsc::Receiver<RefreshMetadataRequest>,
    /// Container to store metadata backoff state per-broker
    metadata_backoff: FxHashMap<BrokerHost, BackoffSession<()>>,
    /// Join set for the metadata refresh task. This should only have one task spawned at any time.
    metadata_join_set: JoinSet<MetadataRefreshResult<TaskHandle>>,
    /// Cancellation signal
    cancellation_token: CancellationToken,
    // The bootstrap signal is dropped when bootstrap is complete.
    bootstrap_signal: Option<oneshot::Sender<()>>,
    /// Used to create new tcp streams
    connect: Conn,
    /// Used to spawn auxiliary tasks specific to a broker id.
    task_factory: Factory,
}

enum Event<Task, TaskHandle> {
    /// Start a refresh of metadata. This is also invoked when a [`NodeTask`]
    /// panics or is aborted, because we no longer have access to the original channel in that case.
    RefreshStart(Option<RefreshMetadataRequest>),
    /// Metadata refresh completed with success or failure.
    RefreshComplete(MetadataRefreshResult<TaskHandle>),
    /// A node stopped. Note this doesn't necessarily indicate that it should be running.
    /// The [`SelectorTask`] will restart it if it points to a valid broker in the cluster.
    NodeDied(Task),
}

impl<
        Conn: Connect + Send + Clone + 'static,
        Task: BrokerTask,
        TaskHandle: BrokerTaskHandle,
        Factory: BrokerTaskFactory<Conn, Task = Task, Handle = TaskHandle>,
    > SelectorTask<Conn, Task, TaskHandle, Factory>
{
    async fn run(mut self) -> Result<(), KafkaError> {
        let mut metadata_interval = tokio::time::interval(self.config.metadata.refresh_interval);
        metadata_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut bootstrap_sucessful = false;
        let mut retry_metadata_immediately = false;

        loop {
            let allow_metadata_requests = self.metadata_join_set.is_empty();
            let metadata_fut = async {
                // We only allow one metadata refresh task to run at a time.
                if !allow_metadata_requests {
                    return None;
                }

                if retry_metadata_immediately {
                    return Some(None);
                }

                Some(tokio::select! {
                    _ = metadata_interval.tick() => None,
                    Some(req) = self.rx_topic_metadata.recv() => Some(req),
                })
            };

            let event = tokio::select! {
                biased;
                () = self.cancellation_token.cancelled() => break,
                // An err here means it panicked. There's no way to recover the original context, so let it go.
                Some(Ok(metadata_refreshed)) = self.metadata_join_set.join_next() => Event::RefreshComplete(metadata_refreshed),
                Some(req) = metadata_fut => Event::RefreshStart(req),
                Some(result) = self.join_set.join_next() => match result {
                    Ok(node_died) => Event::NodeDied(node_died),
                    Err(join_err) => {
                        tracing::error!("node connection task stopped unexpectedly, attempting to recover: {join_err}");
                        Event::RefreshStart(None)
                    }
                },
                // Either all handles are dropped, or we have no nodes to connect to in the cluster.
                else => break
            };

            match event {
                Event::RefreshComplete((mut ctx, metadata)) => {
                    let tx = ctx.request.map(|r| r.tx);

                    match metadata {
                        Ok(metadata) => {
                            bootstrap_sucessful = true;
                            retry_metadata_immediately = false;

                            let throttle_ms: u64 =
                                metadata.throttle_time_ms.try_into().unwrap_or_default();

                            self.update_metadata(metadata).await;

                            tracing::debug!(
                                broker_id = ctx.entry.node.id,
                                host = ?ctx.entry.node.host,
                                "successfully updated metadata {:?}",
                                self.hosts.list_nodes()
                            );

                            ctx.backoff.success();

                            if throttle_ms > 0 {
                                ctx.backoff
                                    .schedule_next(Duration::from_millis(throttle_ms), ());
                            }

                            self.metadata_backoff
                                .insert(ctx.entry.node.host, ctx.backoff);

                            drop(self.bootstrap_signal.take());

                            if let Some(tx) = tx {
                                let _ = tx.send(Ok(()));
                            }
                        }
                        Err(e) => {
                            // Check if we've exceeded our limit for bootstrap retries
                            if !bootstrap_sucessful
                                && self
                                    .config
                                    .bootstrap_max_retries
                                    .is_some_and(|max| ctx.backoff.count() >= max)
                            {
                                tracing::error!(
                                    broker_id = ctx.entry.node.id,
                                    host = ?ctx.entry.node.host,
                                    "retries exhausted while bootstrapping: {e}, retries: {}", ctx.backoff.count()
                                );
                                return Err(e);
                            }

                            // Send another attempt immediately if this is not from an upstream request.
                            // The task handles backoff.
                            retry_metadata_immediately = tx.is_none();

                            if matches!(e, KafkaError::Init(_)) {
                                // Error with establishing a connection, so don't back off and reset the attempts
                                // The node task handles backoff and logging in this case.
                                ctx.backoff.schedule_immediate((), true);
                                if let Some(tx) = tx {
                                    let _ = tx.send(Err(e));
                                }
                                continue;
                            }

                            let backoff = exponential_backoff(
                                self.config.metadata.backoff,
                                self.config.metadata.backoff_max,
                                ctx.backoff.count(),
                            );

                            ctx.backoff.failure(backoff, ());

                            tracing::error!(
                                broker_id = ctx.entry.node.id,
                                host = ?ctx.entry.node.host,
                                "failed to get metadata: {e}, backing off for {backoff:?}, retries: {}", ctx.backoff.count()
                            );

                            self.metadata_backoff
                                .insert(ctx.entry.node.host, ctx.backoff);

                            if let Some(tx) = tx {
                                let _ = tx.send(Err(e));
                            }
                            continue;
                        }
                    }
                }
                Event::RefreshStart(req) => {
                    let Some(entry_for_refresh) = self.cluster.load().brokers.get_best_connection()
                    else {
                        tracing::error!("no connections available for metadata refresh!");
                        break;
                    };

                    tracing::debug!(
                        broker_id = entry_for_refresh.node.id,
                        host = ?entry_for_refresh.node.host,
                        "attempting to refresh metadata"
                    );

                    let topics = req.as_ref().map(|r| r.topics.clone()).unwrap_or_else(|| {
                        Some(self.cluster.load().metadata.create_topics_for_refresh())
                    });

                    let backoff = self
                        .metadata_backoff
                        .remove(&entry_for_refresh.node.host)
                        .unwrap_or_default();

                    let task = MetadataRefreshTask {
                        context: MetadataRefreshContext {
                            entry: entry_for_refresh,
                            request: req,
                            backoff,
                        },
                        topics,
                    };

                    self.metadata_join_set.spawn(task.run());
                }
                Event::NodeDied(dead_task) => self.restart_if_needed(dead_task).await,
            }
        }

        for entry in self.hosts.drain() {
            entry.cancellation_token.cancel();
        }

        let mut clean_shutdown = true;

        self.metadata_join_set.abort_all();

        while let Some(result) = self.metadata_join_set.join_next().await {
            if let Err(join_err) = result {
                if join_err.is_panic() {
                    clean_shutdown = false;
                    tracing::error!("metadata refresh task stopped with an error: {join_err}");
                }
            }
        }

        while let Some(result) = self.join_set.join_next().await {
            if let Err(join_err) = result {
                if join_err.is_panic() {
                    clean_shutdown = false;
                    tracing::error!("a broker connection task stopped with an error: {join_err}");
                }
            }
        }

        self.cluster.store(Arc::new(Cluster::default()));

        if clean_shutdown {
            tracing::info!("shut down gracefully");
        } else {
            tracing::warn!("shut down with errors");
        }

        Ok(())
    }

    async fn update_metadata(&mut self, metadata: MetadataResponse) {
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
        self.hosts.retain(|entry| {
            let keep = new_broker_ids.contains_key(&entry.node.id);
            // Stop the broker task but keep the connection alive
            entry.cancellation_token.cancel();

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

        let mut new_state = self.cluster.load().as_ref().clone();

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
                        new_state.partitions.insert(tp.clone(), tx);
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

        // Restart each task
        for (broker_id, task) in tasks.drain() {
            let Some(entry) = self.hosts.get_mut(&broker_id) else {
                tracing::error!(
                    broker_id = broker_id,
                    "detected a broker task with no handle"
                );
                task.shutdown().await;
                continue;
            };

            let cancellation_token = self.cancellation_token.child_token();
            entry.cancellation_token = cancellation_token.clone();
            let node = task.get_node();

            tracing::debug!(
                broker_id = node.id,
                host = ?node.host,
                "starting broker task"
            );

            self.join_set
                .spawn(task.run(BrokerTaskContext { cancellation_token }));
        }

        let new_broker_hosts: FxHashSet<BrokerHost> =
            metadata.brokers.iter().map(BrokerHost::from).collect();

        // remove backoff state for nodes not in the cluster
        self.metadata_backoff
            .retain(|host, _| new_broker_hosts.contains(host));

        tracing::debug!("removing {} partitions", partition_streams.len());
        // remove partition queues which no longer exist
        for (tp, _) in partition_streams.drain() {
            new_state.partitions.remove(&tp);
        }

        new_state
            .metadata
            .update_with(metadata.clone(), Instant::now());
        new_state.brokers = self.hosts.clone();

        self.cluster.store(Arc::new(new_state));
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
        > = Default::default();
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
                Ok(task) => task,
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

            task.set_node(Node::from(*broker));
            tasks.insert(id, task);
        }

        (tasks, partition_streams)
    }

    fn create_new_task(&mut self, node: Node) -> (Task, CancellationToken) {
        tracing::debug!(
            broker_id = node.id,
            host = ?node.host,
            "creating new connection task"
        );

        let connector = NodeConnector::new(node.clone(), self.config.clone(), self.connect.clone());

        let (handle, task) = self.task_factory.new_task(connector);

        let cancellation_token = self.cancellation_token.child_token();

        self.hosts.insert(BrokerMapEntry {
            node,
            handle,
            cancellation_token: cancellation_token.clone(),
        });

        (task, cancellation_token)
    }

    async fn restart_if_needed(&mut self, mut dead_task: Task) {
        let task_node = dead_task.get_node();

        if let Some(mut entry) = self.hosts.remove(&task_node.id) {
            tracing::debug!(
                broker_id = task_node.id,
                host = ?entry.node.host,
                "restarting connection handle",
            );

            // create new cancellation token to not immediately exit when the task starts
            let cancellation_token = self.cancellation_token.child_token();
            entry.cancellation_token = cancellation_token.clone();

            // if the host is different, stop the existing connection
            if task_node.host != entry.node.host {
                tracing::debug!(
                    host = ?task_node.host,
                    broker_id = task_node.id,
                    "stopping existing connection"
                );
                dead_task = dead_task.shutdown().await;
            }

            dead_task.set_node(entry.node.clone());

            self.hosts.insert(entry);
            self.join_set
                .spawn(dead_task.run(BrokerTaskContext { cancellation_token }));
        }
    }
}

/// Handle to [`SelectorTask`] to pass commands to it.
///
/// The selector manages connections to each broker in the cluster.
///
/// The cluster state is exposed as a `watch` channel, which contains a mapping of broker id to a handle to send
/// requests to the broker.
///
/// This handle can queue requests for the broker even if the connection is not yet connected. It will also ensure that
/// queued requests will be sent to the specific broker by id, rather than host. If the cluster configuration changes
/// when the request is queued, it will remain queued and be sent to the new host for the broker. If the new cluster
/// config does not contain the broker id, the request will be dropped and the sender will receive an error indicating
/// the connection is closed.
pub(crate) struct SelectorTaskHandle<Task: BrokerTask, TaskHandle> {
    pub cluster: Arc<ArcSwap<Cluster<Task, TaskHandle>>>,
    pub tx_topic_metadata: mpsc::Sender<RefreshMetadataRequest>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
    config: KafkaConfig,
}

impl<Task: BrokerTask, TaskHandle> Clone for SelectorTaskHandle<Task, TaskHandle> {
    fn clone(&self) -> Self {
        Self {
            cluster: self.cluster.clone(),
            tx_topic_metadata: self.tx_topic_metadata.clone(),
            cancellation_token: self.cancellation_token.clone(),
            task_tracker: self.task_tracker.clone(),
            config: self.config.clone(),
        }
    }
}

impl<Task: BrokerTask, TaskHandle: BrokerTaskHandle> SelectorTaskHandle<Task, TaskHandle> {
    /// Create a new selector task handle using TCP without TLS.
    pub async fn try_new_tcp<Factory: BrokerTaskFactory<Tcp, Task = Task, Handle = TaskHandle>>(
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
        task_factory: Factory,
    ) -> Result<Self, KafkaError> {
        Self::try_new_with_connect(bootstrap, config, Tcp, task_factory).await
    }

    pub async fn await_shutdown(&self) {
        self.task_tracker.wait().await;
    }

    pub async fn shutdown(&self) {
        self.task_tracker.close();
        self.cancellation_token.cancel();
        self.await_shutdown().await;
    }

    async fn try_new_with_connect<
        Conn: Connect + Clone + Send + 'static,
        Factory: BrokerTaskFactory<Conn, Task = Task, Handle = TaskHandle>,
    >(
        bootstrap: &[BrokerHost],
        config: KafkaConfig,
        connect: Conn,
        task_factory: Factory,
    ) -> Result<Self, KafkaError> {
        let cancellation_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        let (tx_topic_metadata, rx_topic_metadata) =
            mpsc::channel(config.metadata.refresh_batch_count);

        let (tx_bootstrap, rx_bootstrap) = oneshot::channel();

        // start the selector task to manage broker connections
        let mut selector_task = SelectorTask {
            hosts: BrokerMap::default(),
            cluster: Default::default(),
            rx_topic_metadata,
            join_set: JoinSet::new(),
            config: config.clone(),
            metadata_backoff: Default::default(),
            metadata_join_set: JoinSet::new(),
            cancellation_token: cancellation_token.clone(),
            bootstrap_signal: Some(tx_bootstrap),
            connect,
            task_factory,
        };

        for (id, host) in bootstrap.iter().enumerate() {
            let node = Node {
                id: id as i32,
                host: host.clone(),
                rack: None,
            };

            let (task, cancellation_token) = selector_task.create_new_task(node);

            selector_task
                .join_set
                .spawn(task.run(BrokerTaskContext { cancellation_token }));
        }

        let cluster = selector_task.cluster.clone();

        cluster.store(Arc::new(Cluster::new(selector_task.hosts.clone())));

        let join_handle = task_tracker.spawn(selector_task.run());

        tokio::select! {
            // wait for bootstrap. Task will drop this channel when finished with bootstrap.
            _ = rx_bootstrap => Ok(()),
            // or failure to bootstrap
            result = join_handle => result.map_err(|join_err| {
                tracing::error!("bootstrapping stopped unexpectedly: {join_err}");
                KafkaError::Init(ConnectionInitError::Closed)
            })?,
        }?;

        Ok(Self {
            cluster,
            tx_topic_metadata,
            cancellation_token,
            task_tracker,
            config,
        })
    }

    async fn refresh_metadata_for_topics(
        &self,
        topics: Option<Vec<MetadataRequestTopic>>,
    ) -> Result<(), KafkaError> {
        let (tx, rx) = oneshot::channel();

        self.tx_topic_metadata
            .send(RefreshMetadataRequest { topics, tx })
            .await?;

        rx.await??;

        Ok(())
    }

    pub async fn check_topic_metadata(&self, topic: &TopicName) -> Result<(), KafkaError> {
        let cluster = self.cluster.load();
        let Some(topic_result) = cluster.metadata.get_topic_metadata_by_name(topic) else {
            self.refresh_metadata_for_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(topic.clone()))
            ]))
            .await?;
            return Ok(());
        };

        if topic_result.timestamp.elapsed() > self.config.metadata.max_age {
            self.refresh_metadata_for_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(topic.clone()))
            ]))
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    // TODO tests
}
