use std::sync::Arc;

use rustc_hash::FxHashSet;
use tokio::sync::{mpsc, oneshot};

use crate::{
    broker::{
        connector::{NodeConnector, VersionedConnection},
        init_error::ConnectionInitError,
        task::{BrokerTask, BrokerTaskContext},
    },
    cancel::OrCancelled,
    common::{Node, TopicPartition},
    conn::{connect::Connect, selector::ClusterMetadata},
};

#[derive(Debug)]
pub(super) struct NetworkTaskMessage {
    pub tx: oneshot::Sender<Result<Arc<VersionedConnection>, ConnectionInitError>>,
}

pub struct NetworkTask<Conn> {
    pub(super) connector: NodeConnector<Conn>,
    pub(super) rx: mpsc::Receiver<NetworkTaskMessage>,
    pub(super) partitions: FxHashSet<TopicPartition>,
}

impl<Conn: Connect + Send + 'static> BrokerTask for NetworkTask<Conn> {
    type PartitionState = ();
    type PublicPartitionState = ();

    async fn run(mut self, ctx: BrokerTaskContext, _cluster: ClusterMetadata) -> Option<Self> {
        loop {
            let Some(Some(Some(NetworkTaskMessage { tx }))) = self
                .rx
                .recv()
                .or_cancel(&ctx.cancellation_token)
                .or_cancel(&ctx.flush)
                .await
            else {
                break;
            };

            let Some(Some(conn)) = self
                .connector
                .connect()
                .or_cancel(&ctx.cancellation_token)
                .or_cancel(&ctx.flush)
                .await
            else {
                break;
            };

            let _ = tx.send(conn);
        }

        tracing::debug!(
            broker_id = self.connector.node.id,
            host = ?self.connector.node.host,
            "shutting down network task"
        );

        Some(self)
    }

    async fn shutdown(self) -> Self {
        Self {
            connector: self.connector.shutdown().await,
            rx: self.rx,
            partitions: self.partitions,
        }
    }

    fn get_node(&self) -> &Node {
        &self.connector.node
    }

    fn get_node_mut(&mut self) -> &mut Node {
        &mut self.connector.node
    }

    fn assign(&mut self, topic_partition: TopicPartition, _state: Self::PartitionState) {
        self.partitions.insert(topic_partition);
    }

    fn assign_new(&mut self, topic_partition: TopicPartition) -> Self::PublicPartitionState {
        self.assign(topic_partition, ());
    }

    fn revoke(&mut self, topic_partition: &TopicPartition) -> Option<Self::PartitionState> {
        if self.partitions.remove(topic_partition) {
            Some(())
        } else {
            None
        }
    }

    fn get_assignments(&self) -> Vec<TopicPartition> {
        self.partitions.iter().cloned().collect()
    }
}
