use std::{future::Future, io, sync::Arc};
use tokio::net::TcpStream;

use crate::{common::BrokerHost, config::KafkaConfig, conn::channel::KafkaChannel};

/// Creates a new async stream for the connection to a broker.
pub trait Connect {
    fn connect(
        &self,
        host: &BrokerHost,
    ) -> impl Future<Output = Result<KafkaChannel, io::Error>> + Send;
}

/// [`Connect`] for creating a non-TLS [`TcpStream`].
#[derive(Debug, Clone)]
pub struct Tcp {
    pub config: KafkaConfig,
}

impl Connect for Tcp {
    async fn connect(&self, host: &BrokerHost) -> Result<KafkaChannel, io::Error> {
        let conn = TcpStream::connect((host.0.as_ref(), host.1)).await?;

        if let Err(err) = conn.set_nodelay(self.config.socket.nodelay) {
            tracing::warn!(
                "failed to set TCP_NODELAY={} on stream: {err:?}",
                self.config.socket.nodelay
            );
        };

        Ok(KafkaChannel::connect(conn, &self.config))
    }
}

impl<C: Connect> Connect for Arc<C> {
    fn connect(
        &self,
        host: &BrokerHost,
    ) -> impl Future<Output = Result<KafkaChannel, io::Error>> + Send {
        self.as_ref().connect(host)
    }
}

impl Connect for KafkaChannel {
    async fn connect(&self, _host: &BrokerHost) -> Result<KafkaChannel, io::Error> {
        Ok(self.clone())
    }
}
