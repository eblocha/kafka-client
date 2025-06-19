use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::{
    cancel::OrCancelled,
    common::Node,
    conn::{
        broker::{
            connector::{NodeConnector, VersionedConnection},
            init_error::ConnectionInitError,
            task::{BrokerTask, BrokerTaskContext, PartitionQueueMap},
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

    async fn run(mut self, ctx: BrokerTaskContext) -> Self {
        loop {
            let Some(Some(NetworkTaskMessage { tx })) =
                self.rx.recv().or_cancel(&ctx.cancellation_token).await
            else {
                break;
            };

            let Some(conn) = self
                .connector
                .connect()
                .or_cancel(&ctx.cancellation_token)
                .await
            else {
                break;
            };

            let _ = tx.send(conn);
        }

        self
    }

    async fn shutdown(self) -> Self {
        let connector = self.connector.shutdown().await;

        Self {
            connector,
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

    fn set_node(&mut self, node: Node) {
        self.connector.node = node;
    }
}
