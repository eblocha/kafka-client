use arc_swap::ArcSwapOption;
use derive_more::derive::From;
use kafka_protocol::{
    messages::{ApiVersionsRequest, ApiVersionsResponse},
    protocol::{Message, StrBytes, VersionRange},
};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    time::error::Elapsed,
};
use tokio_util::sync::CancellationToken;

use crate::{
    backoff::exponential_backoff,
    conn::{
        channel::{KafkaChannel, KafkaChannelError},
        config::ConnectionRetryConfig,
        host::BrokerHost,
        Sendable,
    },
    proto::{
        error_codes::ErrorCode,
        ver::{FromVersionRange, GetApiKey},
    },
};

use super::connect::Connect;

/// Errors associated with establishing and preparing a Kafka connection.
#[derive(Debug, Error)]
pub enum ConnectionInitError {
    /// Indicates an IO problem. This could be a bad socket or an encoding problem.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// The client has stopped processing requests
    #[error("the connection is closed")]
    Closed,

    /// Failed to determine the API versions that the server supports.
    #[error("version negotiation returned an error code: {0:?}")]
    NegotiationFailed(ErrorCode),

    /// The broker's version range does not intersect with the client
    #[error("version mismatch")]
    Version,
}

#[derive(Debug, From)]
enum ConnectAttemptError {
    /// The node task was cancelled before a connection was established
    Cancelled,
    /// Timed out while waiting to connect
    Timeout,
    /// Initialization error
    Init(#[from] ConnectionInitError),
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

impl From<KafkaChannelError> for ConnectionInitError {
    fn from(value: KafkaChannelError) -> Self {
        match value {
            KafkaChannelError::Io(e) => Self::Io(e),
            KafkaChannelError::Closed => Self::Closed,
            KafkaChannelError::Version => Self::Version,
        }
    }
}

pub struct NodeTaskMessage {
    tx: oneshot::Sender<Arc<VersionedConnection>>,
}

/// A connection with versioning information
#[derive(Debug)]
pub struct VersionedConnection {
    connection: KafkaChannel,
    versions: ApiVersionsResponse,
}

/// A connection task to a broker.
#[derive(Debug)]
pub struct NodeTask<Conn> {
    /// The broker id this node is assigned to
    pub broker_id: i32,
    /// Host this task is connecting to
    pub host: BrokerHost,
    /// Message receiver
    pub rx: mpsc::Receiver<NodeTaskMessage>,
    /// Token to stop the task
    pub cancellation_token: CancellationToken,
    /// Options for connection retries
    pub retry_config: ConnectionRetryConfig,
    // The kafka channel with version information.
    // This will be None if no connection has ever been established.
    pub connection: Arc<ArcSwapOption<VersionedConnection>>,
    /// Creates the new IO stream
    connect: Conn,
}

impl<Conn: Connect + Send + 'static> NodeTask<Conn> {
    /// Attempts to connect after the specified delay, then starts accepting messages and forwarding to the Kafka stream.
    ///
    /// If the kafka stream has any problems, this will return `Self` to enable reuse of the message channel in a new
    /// connection.
    pub async fn run(mut self) -> Self {
        loop {
            let NodeTaskMessage { tx } = tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => break,
                Some(msg) = self.rx.recv() => msg,
                else => break
            };

            let Some(conn) = self.get_connection().await else {
                break;
            };

            let _ = tx.send(conn);
        }

        if let Some(conn) = self.connection.swap(None) {
            conn.connection.shutdown().await;
        }

        self
    }

    async fn try_connect(&mut self) -> Result<VersionedConnection, ConnectAttemptError> {
        let connect_fut = self.connect.connect(&self.host);

        let result = tokio::select! {
            biased;
            _ = self.cancellation_token.cancelled() => return Err(ConnectAttemptError::Cancelled),
            result = tokio::time::timeout(self.retry_config.connection_timeout, connect_fut) => result,
        };

        let conn = result??;

        let versions = negotiate(self.broker_id, &self.host, &conn).await?;

        // TODO authenticate

        Ok(VersionedConnection {
            connection: conn.clone(),
            versions,
        })
    }

    async fn get_connection(&mut self) -> Option<Arc<VersionedConnection>> {
        if let Some(conn) = self
            .connection
            .load()
            .as_ref()
            .filter(|conn| !conn.connection.sender().is_closed())
        {
            return Some(conn.clone());
        }

        // create a new connection to the broker
        let mut attempt = 0;

        let conn = loop {
            tracing::debug!(
                broker_id = self.broker_id,
                host = ?self.host,
                retries = attempt,
                "connecting to broker"
            );
            let backoff = match self.try_connect().await {
                Ok(conn) => break conn,
                Err(ConnectAttemptError::Cancelled) => return None,
                Err(e) => {
                    let (min, max) = (self.retry_config.min_backoff, self.retry_config.max_backoff);
                    let backoff = exponential_backoff(min, max, attempt);

                    macro_rules! log_err {
                        ($($msg:tt)*) => {
                            tracing::error!(
                                broker_id = self.broker_id,
                                host = ?self.host,
                                retries = attempt,
                                backoff = ?backoff,
                                $($msg)*,
                            )
                        };
                    }

                    match e {
                        ConnectAttemptError::Timeout => log_err!("connection timed out"),
                        ConnectAttemptError::Init(e) => log_err!("failed to connect: {e:?}"),
                        ConnectAttemptError::Cancelled => {}
                    }

                    backoff
                }
            };

            tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return None,
                _ = tokio::time::sleep(backoff) => {}
            }

            attempt += 1;

            if self
                .retry_config
                .max_retries
                .is_some_and(|max_retries| max_retries < attempt)
            {
                return None;
            }
        };

        let conn_arc = Arc::new(conn);

        self.connection.store(Some(conn_arc.clone()));

        Some(conn_arc)
    }
}

/// Handle to a [`NodeTask`], to control its lifecycle.
///
/// The [`NodeTask`] serves as an abstraction over a connection to a _broker_, as opposed to a _host_.
/// The task contains a message channel to send messages to the specific broker id, and maintains this channel across
/// retries and reconnects.
///
/// If a broker moves hosts, this channel will be maintained across that host change.
#[derive(Debug, Clone)]
pub struct NodeTaskHandle {
    /// Transmitter to send messages to the underlying connection
    pub tx: mpsc::Sender<NodeTaskMessage>,
    // The kafka channel with version information.
    // This will be None if no connection has ever been established.
    pub connection: Arc<ArcSwapOption<VersionedConnection>>,
    /// Token to stop the running task. This is not exposed, because on the [`crate::conn::selector::SelectorTask`]
    /// should stop the connection.
    pub(super) cancellation_token: CancellationToken,
    /// Number of requests waiting for a response
    in_flight: Arc<AtomicUsize>,
}

impl NodeTaskHandle {
    /// Send a request to the broker and wait for a response.
    ///
    /// Note that this will wait across reconnect retry loops.
    pub async fn send<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<R::Response, KafkaChannelError> {
        self.in_flight.fetch_add(1, Ordering::Acquire);

        let result = self.send_inner(req).await;

        self.in_flight.fetch_sub(1, Ordering::Release);

        result
    }

    async fn send_inner<R: Sendable, F: FromVersionRange<Req = R> + GetApiKey>(
        &self,
        req: F,
    ) -> Result<R::Response, KafkaChannelError> {
        let (tx, rx) = oneshot::channel();

        let msg = NodeTaskMessage { tx };

        self.tx
            .send(msg)
            .await
            .map_err(|_| KafkaChannelError::Closed)?;

        let conn = rx.await.map_err(|_| KafkaChannelError::Closed)?;

        let api_key = req.key();

        let Some(broker_versions) = conn.versions.api_keys.get(&api_key) else {
            return Err(KafkaChannelError::Version);
        };

        let broker_range = VersionRange {
            min: broker_versions.min_version,
            max: broker_versions.max_version,
        };

        let Some((req, version)) = req.from_version_range(broker_range) else {
            return Err(KafkaChannelError::Version);
        };

        conn.connection.send(req, version).await
    }

    /// Determine if this node has an open connection to the host.
    pub fn is_connected(&self) -> bool {
        self.connection
            .load()
            .as_ref()
            .is_some_and(|conn| !conn.connection.sender().is_closed())
    }

    /// Determine the number of in-flight requests to this broker
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Determine the capacity of the connection send buffer if connected.
    ///
    /// If the node is not connected, this will return None.
    pub fn capacity(&self) -> Option<usize> {
        self.connection
            .load()
            .as_ref()
            .map(|conn| conn.connection.sender().capacity())
    }
}

/// Create a new [`NodeTaskHandle`] and [`NodeTask`] pair.
///
/// This creates a new message channel, so is only appropriate to use when creating a connection to a new broker id, or
/// if the original task aborted or panicked, and the message channel was lost.
///
/// It does _not_ spawn the task, that must be handled by the selector task in a join set.
pub fn new_pair<Conn>(
    broker_id: i32,
    host: BrokerHost,
    retry_config: ConnectionRetryConfig,
    connect: Conn,
) -> (NodeTaskHandle, NodeTask<Conn>) {
    // We only need 1 slot because we are just waiting for a shared connection, not sending messages.
    let (tx, rx) = mpsc::channel(1);

    let connection = Arc::new(ArcSwapOption::empty());

    let handle = NodeTaskHandle {
        cancellation_token: CancellationToken::new(),
        connection,
        tx,
        in_flight: Arc::new(AtomicUsize::new(0)),
    };

    let task = NodeTask {
        broker_id,
        host,
        rx,
        cancellation_token: handle.cancellation_token.clone(),
        retry_config,
        connection: handle.connection.clone(),
        connect,
    };

    (handle, task)
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
    use std::sync::Arc;

    use bytes::BytesMut;
    use futures::future;
    use kafka_protocol::{
        messages::{
            api_versions_response::ApiVersion, ApiKey, ApiVersionsRequest, ApiVersionsResponse,
            MetadataRequest, MetadataResponse, ResponseHeader,
        },
        protocol::{Encodable, HeaderVersion, Message},
    };
    use tokio::sync::{mpsc, watch};
    use tokio_test::assert_ok;
    use tokio_util::{sync::CancellationToken, task::TaskTracker};
    use tracing_test::traced_test;

    use crate::{
        conn::{
            channel::{KafkaChannel, KafkaChannelMessage},
            codec::sendable::{DecodableResponse, RequestRecord},
            config::ConnectionRetryConfig,
            host::BrokerHost,
        },
        proto::request::KafkaRequest,
    };

    use super::*;

    fn encode_response<R: Encodable + HeaderVersion>(
        response: R,
        api_version: i16,
    ) -> DecodableResponse {
        let header = ResponseHeader::default();
        let header_version = R::header_version(api_version);

        let mut frame = BytesMut::new();

        header.encode(&mut frame, header_version).unwrap();
        response.encode(&mut frame, api_version).unwrap();

        DecodableResponse {
            record: RequestRecord {
                api_version,
                response_header_version: header_version,
            },
            frame,
        }
    }

    /// Start a task to a fake broker, and send a metadata request to it.
    ///
    /// Returns a reciever for messages that the broker would receive from the client.
    fn fire_and_forget_request() -> mpsc::Receiver<KafkaChannelMessage> {
        let (tx, rx) = mpsc::channel(1);
        let task_tracker = TaskTracker::new();
        let cancellation_token = CancellationToken::new();

        let (handle, task) = new_pair(
            0,
            BrokerHost(Arc::from("localhost"), 9092),
            ConnectionRetryConfig::default(),
            KafkaChannel::from_parts(tx, task_tracker, cancellation_token),
        );

        tokio::spawn(task.run());
        tokio::spawn(async move { handle.send(MetadataRequest::default()).await });

        rx
    }

    #[tokio::test(start_paused = true)]
    async fn starts_not_connected() {
        let (tx, _rx) = mpsc::channel(1);
        let task_tracker = TaskTracker::new();
        let cancellation_token = CancellationToken::new();

        let (handle, task) = new_pair(
            0,
            BrokerHost(Arc::from("localhost"), 9092),
            ConnectionRetryConfig::default(),
            KafkaChannel::from_parts(tx, task_tracker, cancellation_token.clone()),
        );

        tokio::spawn(task.run());

        assert!(!handle.is_connected());
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn connects_on_request() {
        let (tx, mut rx) = mpsc::channel(1);
        let task_tracker = TaskTracker::new();
        let cancellation_token = CancellationToken::new();

        let (handle, task) = new_pair(
            0,
            BrokerHost(Arc::from("localhost"), 9092),
            ConnectionRetryConfig::default(),
            KafkaChannel::from_parts(tx, task_tracker, cancellation_token.clone()),
        );

        tokio::spawn(task.run());

        let h_clone = handle.clone();

        let metadata_response_handle =
            tokio::spawn(async move { h_clone.send(MetadataRequest::default()).await });

        let channel_msg = rx.recv().await.unwrap();

        // the channel message should be an api versions request with max api version
        assert_eq!(
            channel_msg.versioned.api_version,
            ApiVersionsRequest::VERSIONS.max
        );

        assert!(
            matches!(channel_msg.versioned.request, KafkaRequest::ApiVersions(_)),
            "expected an ApiVersionsRequest, got {:?}",
            channel_msg.versioned.request
        );

        let response = encode_response(
            {
                let mut r = ApiVersionsResponse::default();

                r.api_keys.insert(ApiKey::MetadataKey as i16, {
                    let mut v = ApiVersion::default();
                    v.min_version = MetadataRequest::VERSIONS.min;
                    v.max_version = MetadataRequest::VERSIONS.max;
                    v
                });

                r
            },
            channel_msg.versioned.api_version,
        );

        let _ = channel_msg.tx.send(Ok(response));

        let channel_msg = rx.recv().await.unwrap();

        // the channel message should be our metadata request
        assert!(
            matches!(channel_msg.versioned.request, KafkaRequest::Metadata(_)),
            "expected an MetadataRequest, got {:?}",
            channel_msg.versioned.request
        );

        // we should advertise as connected now
        assert!(handle.is_connected());

        let response = encode_response(
            MetadataResponse::default(),
            channel_msg.versioned.api_version,
        );

        let _ = channel_msg.tx.send(Ok(response));

        // the sender should get a response
        let response = metadata_response_handle.await.unwrap();

        assert_ok!(response);
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn retry_on_fail() {
        let mut rx = fire_and_forget_request();

        let channel_msg = rx.recv().await.unwrap();

        let _ = channel_msg.tx.send(Err(io::Error::other("test")));

        // It will wait for backoff, then try to reconnect
        tokio::time::advance(ConnectionRetryConfig::default().min_backoff).await;

        let channel_msg = rx.recv().await.unwrap();

        assert!(
            matches!(channel_msg.versioned.request, KafkaRequest::ApiVersions(_)),
            "expected an ApiVersionsRequest, got {:?}",
            channel_msg.versioned.request
        );
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn retry_with_v0_if_unsupported_version() {
        let mut rx = fire_and_forget_request();

        let channel_msg = rx.recv().await.unwrap();

        let response = encode_response(
            {
                let mut r = ApiVersionsResponse::default();
                r.error_code = ErrorCode::UnsupportedVersion as i16;
                r
            },
            channel_msg.versioned.api_version,
        );

        let _ = channel_msg.tx.send(Ok(response));

        let channel_msg = rx.recv().await.unwrap();

        assert_eq!(
            channel_msg.versioned.api_version,
            ApiVersionsRequest::VERSIONS.min
        );

        assert!(
            matches!(channel_msg.versioned.request, KafkaRequest::ApiVersions(_)),
            "expected an ApiVersionsRequest, got {:?}",
            channel_msg.versioned.request
        );
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn retry_on_timeout() {
        #[derive(Debug, Clone)]
        struct RetryCountingConnect {
            attempts: watch::Sender<usize>,
        }

        impl Connect for RetryCountingConnect {
            async fn connect(&self, _: &BrokerHost) -> io::Result<KafkaChannel> {
                self.attempts.send_modify(|val| *val += 1);
                future::pending().await
            }
        }

        let retry_config = ConnectionRetryConfig::default();

        let (tx, mut rx) = watch::channel(0);

        let conn = RetryCountingConnect { attempts: tx };

        let (handle, task) = new_pair(
            0,
            BrokerHost(Arc::from("localhost"), 9092),
            retry_config.clone(),
            conn,
        );

        tokio::spawn(task.run());

        let h_clone = handle.clone();

        tokio::spawn(async move { h_clone.send(MetadataRequest::default()).await });

        let _ = rx.changed().await;

        // wait for timeout and backoff period
        tokio::time::advance(retry_config.connection_timeout).await;
        tokio::time::advance(retry_config.min_backoff).await;

        let _ = rx.changed().await;

        assert_eq!(rx.borrow().clone(), 2);
    }
}
