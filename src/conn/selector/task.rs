use std::{collections::HashMap, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use kafka_protocol::messages::{metadata_request::MetadataRequestTopic, TopicName};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    backoff::{exponential_backoff, BackoffSession},
    common::BrokerHost,
    config::KafkaConfig,
    conn::{
        broker::{
            init_error::ConnectionInitError,
            task::{BrokerTask, BrokerTaskContext, BrokerTaskFactory, BrokerTaskHandle},
        },
        connect::{Connect, Tcp},
        selector::{
            cluster::ClusterTaskManager,
            metadata::{MetadataRefreshContext, MetadataRefreshTask},
        },
    },
    error::KafkaError,
};

use super::{cluster_state::Cluster, metadata::MetadataRefreshResult};

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
    /// Manages running tasks to each broker
    cluster_manager: ClusterTaskManager<Conn, Task, TaskHandle, Factory>,
    /// Configuration
    config: KafkaConfig,
    /// Receiver for task commands
    rx: mpsc::Receiver<RefreshMetadataRequest>,
    /// Container to store metadata backoff state per-broker
    metadata_backoff: FxHashMap<BrokerHost, BackoffSession<()>>,
    /// Join set for the metadata refresh task. This should only have one task spawned at any time.
    metadata_join_set: JoinSet<MetadataRefreshResult<TaskHandle>>,
    /// Cancellation signal
    cancellation_token: CancellationToken,
    // The bootstrap signal is dropped when bootstrap is complete.
    bootstrap_signal: Option<oneshot::Sender<()>>,
    /// Token to flush partition queues then await shutdown
    flush: CancellationToken,
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
        let mut bootstrap_retries = 0;
        let mut retry_metadata_immediately = false;
        let mut flushing = false;

        loop {
            let allow_metadata_requests = self.metadata_join_set.is_empty();
            let command_fut = async {
                // We only allow one metadata refresh task to run at a time.
                if !allow_metadata_requests {
                    return None;
                }

                if retry_metadata_immediately {
                    return Some(None);
                }

                Some(tokio::select! {
                    _ = metadata_interval.tick() => None,
                    Some(req) = self.rx.recv() => Some(req),
                })
            };

            let event = tokio::select! {
                biased;
                () = self.cancellation_token.cancelled() => break,
                () = self.flush.cancelled() => {
                    tracing::debug!("flushing broker tasks");
                    flushing = true;
                    break;
                },
                // An err here means it panicked. There's no way to recover the original context, so let it go.
                Some(Ok(metadata_refreshed)) = self.metadata_join_set.join_next() => Event::RefreshComplete(metadata_refreshed),
                Some(req) = command_fut => Event::RefreshStart(req),
                Some(result) = self.cluster_manager.join_next() => match result {
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

                            let new_broker_hosts: FxHashSet<BrokerHost> =
                                metadata.brokers.iter().map(BrokerHost::from).collect();

                            // remove backoff state for nodes not in the cluster
                            self.metadata_backoff
                                .retain(|host, _| new_broker_hosts.contains(host));

                            let cluster = self.cluster_manager.update_metadata(metadata).await;

                            tracing::debug!(
                                broker_id = ctx.entry.node.id,
                                host = ?ctx.entry.node.host,
                                "successfully updated metadata {:?}",
                                cluster.brokers.list_nodes()
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
                            if !bootstrap_sucessful {
                                if self
                                    .config
                                    .bootstrap_max_retries
                                    .is_some_and(|max| bootstrap_retries >= max)
                                {
                                    tracing::error!(
                                        broker_id = ctx.entry.node.id,
                                        host = ?ctx.entry.node.host,
                                        "retries exhausted while bootstrapping: {e}, retries: {}", bootstrap_retries
                                    );
                                    return Err(e);
                                }

                                bootstrap_retries = bootstrap_retries.wrapping_add(1);
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
                    let Some(entry_for_refresh) = self.cluster_manager.get_best_connection() else {
                        tracing::error!("no connections available for metadata refresh!");
                        break;
                    };

                    tracing::debug!(
                        broker_id = entry_for_refresh.node.id,
                        host = ?entry_for_refresh.node.host,
                        "attempting to refresh metadata"
                    );

                    let topics = req.as_ref().map_or_else(
                        || Some(self.cluster_manager.create_topics_for_refresh()),
                        |r| r.topics.clone(),
                    );

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
                Event::NodeDied(dead_task) => {
                    if let Some(dead_task) = dead_task {
                        self.cluster_manager.restart_if_needed(dead_task).await;
                    }
                }
            }
        }

        if !flushing {
            self.cancellation_token.cancel();
        }

        self.await_shutdown().await;

        Ok(())
    }

    async fn await_shutdown(mut self) {
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

        self.cluster_manager.await_shutdown().await;

        if clean_shutdown {
            tracing::info!("shut down gracefully");
        } else {
            tracing::warn!("shut down with errors");
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
    tx_topic_metadata: mpsc::Sender<RefreshMetadataRequest>,
    cancellation_token: CancellationToken,
    flush: CancellationToken,
    task_tracker: TaskTracker,
    config: KafkaConfig,
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
        self.cancellation_token.cancel();
        self.await_shutdown().await;
    }

    pub async fn flush(self) {
        self.flush.cancel();
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
        let flush = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        let (tx_topic_metadata, rx_topic_metadata) =
            mpsc::channel(config.metadata.refresh_batch_count);

        let (tx_bootstrap, rx_bootstrap) = oneshot::channel();

        let context = BrokerTaskContext {
            cancellation_token: cancellation_token.clone(),
            flush: flush.clone(),
            tx: tx_topic_metadata.clone(),
        };

        // start the selector task to manage broker connections
        let selector_task = SelectorTask {
            cluster_manager: ClusterTaskManager::bootstrap(
                bootstrap,
                config.clone(),
                connect,
                task_factory,
                context,
            ),
            rx: rx_topic_metadata,
            config: config.clone(),
            metadata_backoff: HashMap::default(),
            metadata_join_set: JoinSet::new(),
            cancellation_token: cancellation_token.clone(),
            bootstrap_signal: Some(tx_bootstrap),
            flush: flush.clone(),
        };

        let cluster = selector_task.cluster_manager.shared_cluster.clone();

        let join_handle = task_tracker.spawn(selector_task.run());
        task_tracker.close();

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
            flush,
            task_tracker,
            config,
        })
    }

    async fn refresh_metadata_for_topic(&self, topic: &TopicName) -> Result<(), KafkaError> {
        let (tx, rx) = oneshot::channel();
        let topics = Some(vec![
            MetadataRequestTopic::default().with_name(Some(topic.clone()))
        ]);

        self.tx_topic_metadata
            .send(RefreshMetadataRequest { topics, tx })
            .await?;

        rx.await??;

        Ok(())
    }

    pub async fn check_topic_metadata(&self, topic: &TopicName) -> Result<(), KafkaError> {
        let cluster = self.cluster.load();
        let Some(topic_result) = cluster.metadata.get_topic_metadata_by_name(topic) else {
            self.refresh_metadata_for_topic(topic).await?;
            return Ok(());
        };

        if topic_result.timestamp.elapsed() > self.config.metadata.max_age {
            self.refresh_metadata_for_topic(topic).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    // TODO tests
}
