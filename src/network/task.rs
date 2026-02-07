use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::{
    cancel::OrCancelled,
    common::Node,
    conn::{
        broker::{
            connector::{NodeConnector, VersionedConnection},
            init_error::ConnectionInitError,
            partition_queue::PartitionQueueMap,
            task::{BrokerTask, BrokerTaskContext},
        },
        connect::Connect,
    },
};

#[derive(Debug)]
pub(super) struct NetworkTaskMessage {
    pub tx: oneshot::Sender<Result<Arc<VersionedConnection>, ConnectionInitError>>,
}

pub struct NetworkTask<Conn> {
    pub(super) connector: NodeConnector<Conn>,
    pub(super) rx: mpsc::Receiver<NetworkTaskMessage>,
    pub(super) partitions: PartitionQueueMap<()>,
}

impl<Conn: Connect + Send + 'static> BrokerTask for NetworkTask<Conn> {
    type PartitionMessage = ();

    async fn run(mut self, ctx: BrokerTaskContext) -> Option<Self> {
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

    fn get_partitions_mut(&mut self) -> &mut PartitionQueueMap<Self::PartitionMessage> {
        &mut self.partitions
    }

    fn get_node(&self) -> &Node {
        &self.connector.node
    }

    fn get_node_mut(&mut self) -> &mut Node {
        &mut self.connector.node
    }
}
