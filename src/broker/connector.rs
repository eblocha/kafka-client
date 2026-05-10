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
    backoff::{BackoffSession, exponential_backoff},
    broker::init_error::ConnectionInitError,
    common::{BrokerHost, Node},
    config::KafkaConfig,
    conn::{Sendable, channel::KafkaChannel, connect::Connect},
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
    pub connection: Arc<ArcSwapOption<VersionedConnection>>,
    config: KafkaConfig,
    connect: Conn,
    backoff: BackoffSession<()>,
}

impl<Conn> NodeConnector<Conn> {
    pub fn new(node: Node, config: KafkaConfig, connect: Conn) -> Self {
        Self {
            node,
            config,
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
            let (min, max) = (
                self.config.socket.reconnect_backoff,
                self.config.socket.reconnect_backoff_max,
            );
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
            tracing::debug!(
                broker_id = self.node.id,
                host = ?self.node.host,
                retries = self.backoff.count(),
                "established connection"
            );

            self.backoff.success();
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
        let connect_fut = self.connect.connect(&self.node.host, &self.config);

        let channel =
            tokio::time::timeout(self.config.socket.connection_setup_timeout, connect_fut)
                .await??;

        let versions = tokio::time::timeout(
            self.config.api_version_request_timeout,
            negotiate(self.node.id, &self.node.host, &channel),
        )
        .await??
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

#[cfg(test)]
mod test {
    use std::{io, sync::Arc, time::Duration};

    use kafka_protocol::{
        messages::{ApiVersionsRequest, ApiVersionsResponse},
        protocol::Message,
    };
    use tokio_test::assert_err;

    use crate::{
        broker::{connector::NodeConnector, init_error::ConnectionInitError},
        common::{BrokerHost, Node},
        config::KafkaConfig,
        conn::{
            channel::KafkaChannel,
            testing::{NeverConnects, TestHarness, create_channel},
        },
        error::ErrorCode,
        proto::request::KafkaRequest,
    };

    fn create_connector(config: KafkaConfig) -> (TestHarness, NodeConnector<KafkaChannel>) {
        let (harness, channel) = create_channel();

        let connector = NodeConnector::new(
            Node {
                id: 0,
                host: BrokerHost("test".into(), 9092),
                rack: None,
            },
            config,
            channel,
        );

        (harness, connector)
    }

    fn create_never_connects(config: KafkaConfig) -> (TestHarness, NodeConnector<NeverConnects>) {
        let harness = TestHarness::new();
        let connector = NodeConnector::new(
            Node {
                id: 0,
                host: BrokerHost("test".into(), 9092),
                rack: None,
            },
            config,
            NeverConnects,
        );

        (harness, connector)
    }

    #[test]
    fn starts_not_connected() {
        let (_, handle) = create_connector(KafkaConfig::default());
        assert!(handle.connection.load().is_none())
    }

    #[tokio::test(start_paused = true)]
    async fn sends_api_versions_request() {
        let (mut harness, mut handle) = create_connector(KafkaConfig::default());

        tokio::spawn(async move { handle.connect().await });
        let req = harness.rx.recv().await.unwrap();

        assert_eq!(req.versioned.api_version, ApiVersionsRequest::VERSIONS.max);
        assert!(matches!(
            req.versioned.request,
            KafkaRequest::ApiVersions(_)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn sends_fallback_versions_request() {
        let (mut harness, mut handle) = create_connector(KafkaConfig::default());

        tokio::spawn(async move { handle.connect().await });
        let req = harness.rx.recv().await.unwrap();

        // Respond with an unsupported version error
        let response =
            ApiVersionsResponse::default().with_error_code(ErrorCode::UnsupportedVersion as i16);

        req.respond_with_version(response, 0);

        let req_2 = harness.rx.recv().await.unwrap();

        assert_eq!(
            req_2.versioned.api_version,
            ApiVersionsRequest::VERSIONS.min
        );
        assert!(matches!(
            req_2.versioned.request,
            KafkaRequest::ApiVersions(_)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn propagates_failure() {
        let (mut harness, mut handle) = create_connector(KafkaConfig::default());

        let join = tokio::spawn(async move { handle.connect().await });
        let req = harness.rx.recv().await.unwrap();

        req.tx.send_err(io::Error::from(io::ErrorKind::BrokenPipe));

        let result = join.await.unwrap();

        assert_err!(result.as_ref());

        let ConnectionInitError::Io(err) = result.as_ref().unwrap_err() else {
            panic!("did not respond with an io error: {result:?}");
        };

        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test(start_paused = true)]
    async fn reuses_connection() {
        let (harness, mut handle) = create_connector(KafkaConfig::default());

        harness.spawn_ok();

        let conn_1 = handle.connect().await.unwrap();
        let conn_2 = handle.connect().await.unwrap();

        assert!(Arc::ptr_eq(&conn_1, &conn_2));
    }

    #[tokio::test(start_paused = true)]
    async fn reconnects_after_closed() {
        let (harness_1, mut handle) = create_connector(KafkaConfig::default());

        harness_1.spawn_ok();

        let conn_1 = handle.connect().await.unwrap();
        conn_1.connection.shutdown().await;

        let (harness_2, channel_2) = create_channel();

        harness_2.spawn_ok();

        handle.connect = channel_2;

        let conn_2 = handle.connect().await.unwrap();

        assert!(conn_1.is_closed());
        assert!(!conn_2.is_closed());
        assert!(!Arc::ptr_eq(&conn_1, &conn_2));
    }

    #[tokio::test(start_paused = true)]
    async fn fails_on_socket_timeout() {
        let mut config = KafkaConfig::default();
        config.socket.connection_setup_timeout = Duration::from_secs(30);

        let (_, mut handle) = create_never_connects(config.clone());

        let result = handle.connect().await;

        let ConnectionInitError::Io(err) = result.as_ref().unwrap_err() else {
            panic!("did not respond with an io error: {result:?}");
        };

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn fails_on_versions_timeout() {
        let config = KafkaConfig {
            api_version_request_timeout: Duration::from_secs(30),
            ..Default::default()
        };

        let (harness, mut handle) = create_connector(config.clone());

        harness.spawn_never();

        let result = handle.connect().await;

        let ConnectionInitError::Io(err) = result.as_ref().unwrap_err() else {
            panic!("did not respond with an io error: {result:?}");
        };

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn waits_for_backoff() {
        let mut config = KafkaConfig::default();
        // Guarantee a specific backoff value
        config.socket.reconnect_backoff = Duration::from_secs(10);
        config.socket.reconnect_backoff_max = Duration::from_secs(10);
        config.socket.connection_setup_timeout = Duration::from_millis(100);
        config.api_version_request_timeout = Duration::from_millis(100);

        // Fail to connect
        let (_, mut handle) = create_connector(config.clone());
        let _ = handle.connect().await;

        let (harness, channel) = create_channel();

        handle.connect = channel;

        harness.spawn_ok();

        // Race the next connection with a timeout. Timeout should win.
        let result = tokio::time::timeout(Duration::from_secs(3), handle.connect()).await;

        assert_err!(result);
    }
}
