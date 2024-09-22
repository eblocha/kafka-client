use std::{future::Future, io, sync::Arc};
use tokio::{net::TcpStream, sync::mpsc};

use crate::conn::{
    channel::{KafkaChannel, KafkaChannelMessage},
    config::KafkaConnectionConfig,
    host::BrokerHost,
};

/// Creates a new async stream for the connection to a broker.
pub trait Connect {
    fn connect(
        &self,
        host: &BrokerHost,
    ) -> impl Future<Output = Result<mpsc::Sender<KafkaChannelMessage>, io::Error>> + Send;
}

/// [`Connect`] for creating a non-TLS [`TcpStream`].
#[derive(Debug, Clone)]
pub struct Tcp {
    /// Setting for `TCP_NODELAY`
    pub nodelay: bool,
    pub config: KafkaConnectionConfig,
}

impl Connect for Tcp {
    async fn connect(
        &self,
        host: &BrokerHost,
    ) -> Result<mpsc::Sender<KafkaChannelMessage>, io::Error> {
        let conn = TcpStream::connect((host.0.as_ref(), host.1)).await?;

        if let Err(err) = conn.set_nodelay(self.nodelay) {
            tracing::warn!(
                "failed to set TCP_NODELAY={} on stream: {err:?}",
                self.nodelay
            );
        };

        Ok(KafkaChannel::connect(conn, &self.config).sender().clone())
    }
}

impl<C: Connect> Connect for Arc<C> {
    fn connect(
        &self,
        host: &BrokerHost,
    ) -> impl Future<Output = Result<mpsc::Sender<KafkaChannelMessage>, io::Error>> + Send {
        self.as_ref().connect(host)
    }
}
