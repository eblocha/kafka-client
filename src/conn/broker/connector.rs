use std::{io, sync::Arc};

use arc_swap::ArcSwapOption;
use derive_more::derive::From;
use kafka_protocol::{
    messages::{ApiVersionsRequest, ApiVersionsResponse},
    protocol::{Message, StrBytes, VersionRange},
};
use rustc_hash::FxHashMap;
use tokio::time::error::Elapsed;

use crate::{
    backoff::{exponential_backoff, BackoffSession},
    common::{BrokerHost, Node},
    conn::{
        broker::init_error::ConnectionInitError, channel::KafkaChannel,
        config::ConnectionRetryConfig, connect::Connect, Sendable,
    },
    error::{ErrorCode, KafkaError},
    proto::ver::{FromVersionRange, GetApiKey},
};

/// A connection with versioning information
#[derive(Debug)]
pub struct VersionedConnection {
    connection: KafkaChannel,
    versions: FxHashMap<i16, VersionRange>,
}

impl VersionedConnection {
    /// Send a request to the broker and wait for a response.
    pub async fn send<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<R::Response, KafkaError> {
        let (conn, req, version) = self.prepare_send(req)?;
        Ok(conn.send(req, version).await?)
    }

    /// Sends a request and returns a future that resolves when the message is sent.
    pub async fn send_and_forget<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<(), KafkaError> {
        let (conn, req, version) = self.prepare_send(req)?;
        Ok(conn.send_and_forget(req, version).await?)
    }

    pub fn capacity(&self) -> usize {
        self.connection.sender().capacity()
    }

    pub fn is_closed(&self) -> bool {
        self.connection.sender().is_closed()
    }

    fn prepare_send<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<(&KafkaChannel, R, i16), KafkaError> {
        let api_key = req.key();

        let Some(broker_versions) = self.versions.get(&api_key) else {
            return Err(KafkaError::ErrorCode(ErrorCode::UnsupportedVersion));
        };

        let Some((req, version)) = req.from_version_range(*broker_versions) else {
            return Err(KafkaError::ErrorCode(ErrorCode::UnsupportedVersion));
        };

        Ok((&self.connection, req, version))
    }
}

#[derive(Debug)]
/// Manages a connection to a broker node.
pub struct NodeConnector<Conn> {
    pub node: Node,
    pub retry_config: ConnectionRetryConfig,
    pub connection: Arc<ArcSwapOption<VersionedConnection>>,
    connect: Conn,
    backoff: BackoffSession<()>,
}

impl<Conn> NodeConnector<Conn> {
    pub fn new(node: Node, retry_config: ConnectionRetryConfig, connect: Conn) -> Self {
        Self {
            node,
            retry_config,
            connection: Arc::new(ArcSwapOption::empty()),
            connect,
            backoff: BackoffSession::default(),
        }
    }

    pub async fn shutdown(self) -> Self {
        if let Some(conn) = self.connection.swap(None) {
            conn.connection.shutdown().await;
        }

        tracing::debug!(
            broker_id = self.node.id,
            host = ?self.node.host,
            "shut down gracefully"
        );

        self
    }
}

#[derive(Debug, From)]
enum ConnectAttemptError {
    /// Timed out while waiting to connect
    Timeout,
    /// Initialization error
    Init(#[from] ConnectionInitError),
}

impl From<ConnectAttemptError> for ConnectionInitError {
    fn from(value: ConnectAttemptError) -> Self {
        match value {
            ConnectAttemptError::Timeout => Self::Io(io::ErrorKind::TimedOut.into()),
            ConnectAttemptError::Init(init) => init,
        }
    }
}

impl From<Elapsed> for ConnectAttemptError {
    fn from(_: Elapsed) -> Self {
        Self::Timeout
    }
}

impl From<io::Error> for ConnectAttemptError {
    fn from(value: io::Error) -> Self {
        Self::Init(value.into())
    }
}

impl<Conn: Connect> NodeConnector<Conn> {
    pub async fn connect(&mut self) -> Result<Arc<VersionedConnection>, ConnectionInitError> {
        self.backoff.wait_next().await;

        let conn_result = self.get_connection().await;

        if let Err(ref e) = conn_result {
            let (min, max) = (self.retry_config.min_backoff, self.retry_config.max_backoff);
            let backoff = exponential_backoff(min, max, self.backoff.count());

            tracing::error!(
                broker_id = self.node.id,
                host = ?self.node.host,
                retries = self.backoff.count(),
                backoff = ?backoff,
                "failed to connect: {e}",
            );

            self.backoff.failure(backoff, ());
        } else {
            self.backoff.success();
        }

        if conn_result.is_ok() {
            self.backoff.success();
        } else {
            let (min, max) = (self.retry_config.min_backoff, self.retry_config.max_backoff);
            let backoff = exponential_backoff(min, max, self.backoff.count());
            self.backoff.schedule_next(backoff, ());
        }

        conn_result
    }

    async fn get_connection(&mut self) -> Result<Arc<VersionedConnection>, ConnectionInitError> {
        if let Some(conn) = self
            .connection
            .load()
            .as_ref()
            .filter(|conn| !conn.is_closed())
        {
            return Ok(conn.clone());
        }

        // create a new connection to the broker

        tracing::debug!(
            broker_id = self.node.id,
            host = ?self.node.host,
            retries = self.backoff.count(),
            "connecting to broker"
        );

        let conn = self.try_connect().await?;

        let conn_arc = Arc::new(conn);

        self.connection.store(Some(conn_arc.clone()));

        Ok(conn_arc)
    }

    async fn try_connect(&mut self) -> Result<VersionedConnection, ConnectAttemptError> {
        let connect_fut = self.connect.connect(&self.node.host);

        let channel =
            tokio::time::timeout(self.retry_config.connection_timeout, connect_fut).await??;

        let versions = negotiate(self.node.id, &self.node.host, &channel)
            .await?
            .api_keys
            .into_iter()
            .map(|key| {
                (
                    key.api_key,
                    VersionRange {
                        min: key.min_version,
                        max: key.max_version,
                    },
                )
            })
            .collect();

        // TODO authenticate

        Ok(VersionedConnection {
            connection: channel.clone(),
            versions,
        })
    }
}

fn create_version_request() -> ApiVersionsRequest {
    let mut r = ApiVersionsRequest::default();
    r.client_software_name = StrBytes::from_static_str(env!("CARGO_PKG_NAME"));
    r.client_software_version = StrBytes::from_static_str(env!("CARGO_PKG_VERSION"));
    r
}

async fn negotiate(
    broker_id: i32,
    host: &BrokerHost,
    conn: &KafkaChannel,
) -> Result<ApiVersionsResponse, ConnectionInitError> {
    tracing::debug!(
        broker_id = broker_id,
        host = ?host,
        "negotiating api versions"
    );

    let api_versions_response = conn
        .send(
            create_version_request(),
            <ApiVersionsRequest as Message>::VERSIONS.max,
        )
        .await?;

    let api_versions_response =
        if api_versions_response.error_code == ErrorCode::UnsupportedVersion as i16 {
            tracing::debug!(
                broker_id = broker_id,
                host = ?host,
                "latest api versions request version is unsupported, falling back to version 0"
            );
            conn.send(
                create_version_request(),
                <ApiVersionsRequest as Message>::VERSIONS.min,
            )
            .await?
        } else {
            api_versions_response
        };

    let error_code: ErrorCode = api_versions_response.error_code.into();

    if error_code == ErrorCode::None {
        tracing::debug!(
            broker_id = broker_id,
            host = ?host,
            "version negotiation completed successfully"
        );
        Ok(api_versions_response)
    } else {
        let e = ConnectionInitError::NegotiationFailed(error_code);
        tracing::error!(
            broker_id = broker_id,
            host = ?host,
            "{e}"
        );
        Err(e)
    }
}
