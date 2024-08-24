use kafka_protocol::{
    messages::{ApiVersionsRequest, ApiVersionsResponse},
    protocol::{Message, StrBytes, VersionRange},
};
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    conn::{
        codec::VersionedRequest,
        config::KafkaConnectionConfig,
        conn::{KafkaConnection, KafkaConnectionError, ResponseSender},
        host::BrokerHost,
        Sendable,
    },
    proto::{error_codes::ErrorCode, request::KafkaRequest, ver::Versionable},
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
}

impl From<KafkaConnectionError> for ConnectionInitError {
    fn from(value: KafkaConnectionError) -> Self {
        match value {
            KafkaConnectionError::Io(e) => Self::Io(e),
            KafkaConnectionError::Closed => Self::Closed,
        }
    }
}

pub struct NodeTaskMessage {
    request: KafkaRequest,
    tx: ResponseSender,
}

/// A connection task to a broker.
///
/// Contains a receiver that forwards requests to the connection.
#[derive(Debug)]
pub struct NodeTask<Conn> {
    /// The broker id this node is assigned to
    pub broker_id: i32,
    /// Host this task is connecting to
    pub host: BrokerHost,
    /// Message receiver
    pub rx: mpsc::Receiver<NodeTaskMessage>,
    /// The last message attempted which could not be sent
    pub last_message: Option<(VersionedRequest, ResponseSender)>,
    /// Token to stop the task
    pub cancellation_token: CancellationToken,
    /// Options for the io stream
    pub config: KafkaConnectionConfig,
    /// Timeout to establish a connection before exiting
    pub connection_timeout: Duration,
    /// Attempt count marker to compute next backoff
    pub retries: u32,
    /// Delay before attempting to connect
    pub delay: Option<Duration>,
    /// This node has acquired a connection
    pub connected: Arc<AtomicBool>,
    /// Creates the new IO stream
    connect: Conn,
}

impl<Conn: Connect + Send + 'static> NodeTask<Conn> {
    /// Attempts to connect after the specified delay, then starts accepting messages and forwarding to the Kafka stream.
    ///
    /// If the kafka stream has any problems, this will return `Self` to enable reuse of the message channel in a new
    /// connection.
    pub async fn run(mut self) -> Self {
        tracing::debug!(
            broker_id = self.broker_id,
            host = ?self.host,
            retries = self.retries,
            backoff = ?self.delay,
            "connecting to broker"
        );

        if let Some(delay) = self.delay {
            // back off
            tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return self,
                _ = tokio::time::sleep(delay) => {}
            }
        }

        let connect_fut = self.connect.connect(&self.host);

        let result = tokio::select! {
            biased;
            _ = self.cancellation_token.cancelled() => return self,
            result = tokio::time::timeout(self.connection_timeout, connect_fut) => result,
            else => return self
        };

        let conn = match result {
            Ok(Ok(conn)) => KafkaConnection::connect(conn, &self.config),
            Ok(Err(e)) => {
                tracing::error!(
                    broker_id = self.broker_id,
                    host = ?self.host,
                    retries = self.retries,
                    "failed to connect: {e:?}",
                );
                return self;
            }
            Err(_) => {
                tracing::error!(
                    broker_id = self.broker_id,
                    host = ?self.host,
                    retries = self.retries,
                    "connection timed out",
                );
                return self;
            }
        };

        let Ok(versions) = negotiate(self.broker_id, &self.host, self.retries, &conn).await else {
            return self;
        };

        // TODO authenticate

        self.connected.store(true, Ordering::SeqCst);

        // try to send the last message sent if it failed
        if let Some(last_message) = self.last_message.take() {
            if !last_message.1.is_closed() {
                if let Err(err) = conn.sender().send(last_message).await {
                    tracing::error!(
                        broker_id = self.broker_id,
                        host = ?self.host,
                        retries = self.retries,
                        "failed to re-send last sent message"
                    );
                    // if we're here, the connection we just created is already closed.
                    self.last_message = Some(err.0);
                    return self;
                }
            }
        }

        loop {
            let NodeTaskMessage { request, tx } = tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return self,
                _ = conn.closed() => {
                    tracing::error!(
                        broker_id = self.broker_id,
                        host = ?self.host,
                        retries = self.retries,
                        "connection closed unexpectedly"
                    );
                    return self;
                },
                Some(msg) = self.rx.recv() => msg,
                else => return self
            };

            let api_key = request.key();
            let range = request.versions();

            let versioned = VersionedRequest {
                request,
                api_version: determine_version(&versions, api_key, &range),
            };

            if tx.is_closed() {
                continue;
            }

            if let Err(err) = conn.sender().send((versioned, tx)).await {
                tracing::error!(
                    broker_id = self.broker_id,
                    host = ?self.host,
                    retries = self.retries,
                    "connection closed unexpectedly, storing last sent message"
                );
                self.last_message = Some(err.0);
                return self;
            }
        }
    }
}

/// Handle to a [`NodeTask`], to control its lifecycle.
///
/// The [`NodeTask`] serves as an abstraction over a connection to a _broker_, as opposed to a _host_.
/// The task contains a message channel to send messages to the specific broker id, and maintains this channel across
/// retries and reconnects.
///
/// If a broker moves hosts, this channel will be maintained across that host change.
///
/// It will also re-send the last message after a reconnect if the connection was closed when it was last attempted.
#[derive(Debug, Clone)]
pub struct NodeTaskHandle {
    /// Transmitter to send messages to the underlying connection
    pub tx: mpsc::Sender<NodeTaskMessage>,
    /// Atomic to determine if this node is _likely_ connected.
    ///
    /// Note that this may appear `true` when the connection closes unexpectedly, and the task has not yet been restarted.
    pub connected: Arc<AtomicBool>,
    /// Token to stop the running task. This is not exposed, because on the [`crate::conn::selector::SelectorTask`]
    /// should stop the connection.
    pub(super) cancellation_token: CancellationToken,
}

impl NodeTaskHandle {
    /// Send a request to the broker and wait for a response.
    ///
    /// Note that this will wait across reconnect retry loops.
    pub async fn send<R: Sendable>(&self, req: R) -> Result<R::Response, KafkaConnectionError> {
        let (tx, rx) = oneshot::channel();

        let msg = NodeTaskMessage {
            request: req.into(),
            tx,
        };

        self.tx
            .send(msg)
            .await
            .map_err(|_| KafkaConnectionError::Closed)?;

        let response = rx.await.map_err(|_| KafkaConnectionError::Closed)??;

        Ok(R::decode(response)?)
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
    connection_timeout: Duration,
    config: KafkaConnectionConfig,
    connect: Conn,
) -> (NodeTaskHandle, NodeTask<Conn>) {
    let (tx, rx) = mpsc::channel(config.send_buffer_size);

    let handle = NodeTaskHandle {
        cancellation_token: CancellationToken::new(),
        connected: Arc::new(AtomicBool::new(false)),
        tx,
    };

    let task = NodeTask {
        broker_id,
        host,
        rx,
        last_message: None,
        cancellation_token: handle.cancellation_token.clone(),
        config,
        connection_timeout,
        retries: 0,
        delay: None,
        connected: handle.connected.clone(),
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
    retries: u32,
    conn: &KafkaConnection,
) -> Result<ApiVersionsResponse, ConnectionInitError> {
    tracing::debug!(
        broker_id = broker_id,
        host = ?host,
        retries = retries,
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
                retries = retries,
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
            retries = retries,
            "version negotiation completed successfully"
        );
        Ok(api_versions_response)
    } else {
        let e = ConnectionInitError::NegotiationFailed(error_code);
        tracing::error!(
            broker_id = broker_id,
            host = ?host,
            retries = retries,
            "{e}"
        );
        Err(e)
    }
}

fn determine_version(response: &ApiVersionsResponse, api_key: i16, range: &VersionRange) -> i16 {
    let Some(broker_versions) = response.api_keys.get(&api_key) else {
        // if the server doesn't recognize the request, try sending anyways with the max version
        return range.max;
    };

    let intersection = range.intersect(&VersionRange {
        min: broker_versions.min_version,
        max: broker_versions.max_version,
    });

    if intersection.is_empty() {
        // if the server doesn't support our range, choose the closest one and try anyways
        return if broker_versions.max_version < range.min {
            // |--broker--| |--client--|
            //              ^
            range.min
        } else {
            // |--client--| |--broker--|
            //            ^
            range.max
        };
    }

    intersection.max
}
