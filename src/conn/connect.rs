use std::{future::Future, io, sync::Arc};
use tokio::net::TcpStream;

use crate::{common::BrokerHost, config::KafkaConfig, conn::channel::KafkaChannel};

/// Creates a new async stream for the connection to a broker.
pub trait Connect {
    fn connect(
        &self,
        host: &BrokerHost,
        config: &KafkaConfig,
    ) -> impl Future<Output = Result<KafkaChannel, io::Error>> + Send;
}

/// [`Connect`] for creating a non-TLS [`TcpStream`].
#[derive(Debug, Clone)]
pub struct Tcp;

impl Connect for Tcp {
    async fn connect(
        &self,
        host: &BrokerHost,
        config: &KafkaConfig,
    ) -> Result<KafkaChannel, io::Error> {
        let conn = TcpStream::connect((host.0.as_ref(), host.1)).await?;

        if let Err(err) = conn.set_nodelay(config.socket.nodelay) {
            tracing::warn!(
                "failed to set TCP_NODELAY={} on stream: {err:?}",
                config.socket.nodelay
            );
        };

        Ok(KafkaChannel::connect(conn, config))
    }
}

impl<C: Connect> Connect for Arc<C> {
    fn connect(
        &self,
        host: &BrokerHost,
        config: &KafkaConfig,
    ) -> impl Future<Output = Result<KafkaChannel, io::Error>> + Send {
        self.as_ref().connect(host, config)
    }
}

impl Connect for KafkaChannel {
    async fn connect(
        &self,
        _host: &BrokerHost,
        _config: &KafkaConfig,
    ) -> Result<KafkaChannel, io::Error> {
        Ok(self.clone())
    }
}
