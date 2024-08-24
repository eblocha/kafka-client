use std::{future::Future, io, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

use crate::conn::host::BrokerHost;

/// Creates a new async stream for the connection to a broker.
pub trait Connect {
    type IO: AsyncRead + AsyncWrite + Send + 'static;

    fn connect(
        &self,
        host: &BrokerHost,
    ) -> impl Future<Output = Result<Self::IO, io::Error>> + Send;
}

/// [`Connect`] for creating a non-TLS [`TcpStream`].
#[derive(Debug, Clone, Copy)]
pub struct Tcp {
    /// Setting for `TCP_NODELAY`
    pub nodelay: bool,
}

impl Connect for Tcp {
    type IO = TcpStream;

    async fn connect(&self, host: &BrokerHost) -> Result<Self::IO, io::Error> {
        let conn = TcpStream::connect((host.0.as_ref(), host.1)).await?;

        if let Err(err) = conn.set_nodelay(self.nodelay) {
            tracing::warn!(
                "failed to set TCP_NODELAY={} on stream: {err:?}",
                self.nodelay
            );
        }

        Ok(conn)
    }
}

impl<C: Connect> Connect for Arc<C> {
    type IO = C::IO;

    fn connect(
        &self,
        host: &BrokerHost,
    ) -> impl Future<Output = Result<Self::IO, io::Error>> + Send {
        self.as_ref().connect(host)
    }
}
