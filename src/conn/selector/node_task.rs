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
    backoff::exponential_backoff,
    conn::{
        channel::{KafkaChannel, KafkaChannelError, KafkaChannelMessage, ResponseSender},
        codec::VersionedRequest,
        config::ConnectionConfig,
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

enum ConnectAttemptError {
    Cancelled,
    Failed,
}

impl From<KafkaChannelError> for ConnectionInitError {
    fn from(value: KafkaChannelError) -> Self {
        match value {
            KafkaChannelError::Io(e) => Self::Io(e),
            KafkaChannelError::Closed => Self::Closed,
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
    pub last_message: Option<KafkaChannelMessage>,
    /// Token to stop the task
    pub cancellation_token: CancellationToken,
    /// Options for the io stream
    pub config: ConnectionConfig,
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
        self.connected.store(false, Ordering::SeqCst);

        let mut attempt = 0;
        let (conn, versions) = loop {
            let (min, max) = (self.config.retry.min_backoff, self.config.retry.max_backoff);

            let backoff = if attempt == 0 {
                None
            } else {
                Some(exponential_backoff(min, max, attempt))
            };

            match self.try_connect(backoff, attempt).await {
                Ok(conn) => break conn,
                Err(ConnectAttemptError::Cancelled) => return self,
                Err(ConnectAttemptError::Failed) => {
                    attempt += 1;
                }
            }
        };

        // try to send the last message sent if it failed
        if let Some(last_message) = self.last_message.take() {
            if !last_message.tx.is_closed() {
                if let Err(err) = conn.sender().send(last_message).await {
                    tracing::error!(
                        broker_id = self.broker_id,
                        host = ?self.host,
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

            if let Err(err) = conn
                .sender()
                .send(KafkaChannelMessage { versioned, tx })
                .await
            {
                tracing::error!(
                    broker_id = self.broker_id,
                    host = ?self.host,
                    "connection closed unexpectedly, storing last sent message"
                );
                self.last_message = Some(err.0);
                return self;
            }
        }
    }

    async fn try_connect(
        &mut self,
        delay: Option<Duration>,
        retries: u32,
    ) -> Result<(KafkaChannel, ApiVersionsResponse), ConnectAttemptError> {
        tracing::debug!(
            broker_id = self.broker_id,
            host = ?self.host,
            retries = retries,
            backoff = ?delay,
            "connecting to broker"
        );

        if let Some(delay) = delay {
            // back off
            tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return Err(ConnectAttemptError::Cancelled),
                _ = tokio::time::sleep(delay) => {}
            }
        }

        let connect_fut = self.connect.connect(&self.host);

        let result = tokio::select! {
            biased;
            _ = self.cancellation_token.cancelled() => return Err(ConnectAttemptError::Cancelled),
            result = tokio::time::timeout(self.config.retry.connection_timeout, connect_fut) => result,
        };

        let conn = match result {
            Ok(Ok(conn)) => KafkaChannel::connect(conn, &self.config.io),
            Ok(Err(e)) => {
                tracing::error!(
                    broker_id = self.broker_id,
                    host = ?self.host,
                    retries = retries,
                    "failed to connect: {e:?}",
                );
                return Err(ConnectAttemptError::Failed);
            }
            Err(_) => {
                tracing::error!(
                    broker_id = self.broker_id,
                    host = ?self.host,
                    retries = retries,
                    "connection timed out",
                );
                return Err(ConnectAttemptError::Failed);
            }
        };

        let Ok(versions) = negotiate(self.broker_id, &self.host, retries, &conn).await else {
            return Err(ConnectAttemptError::Failed);
        };

        // TODO authenticate

        self.connected.store(true, Ordering::SeqCst);

        Ok((conn, versions))
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
    pub async fn send<R: Sendable>(&self, req: R) -> Result<R::Response, KafkaChannelError> {
        let (tx, rx) = oneshot::channel();

        let msg = NodeTaskMessage {
            request: req.into(),
            tx,
        };

        self.tx
            .send(msg)
            .await
            .map_err(|_| KafkaChannelError::Closed)?;

        let response = rx.await.map_err(|_| KafkaChannelError::Closed)??;

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
    config: ConnectionConfig,
    connect: Conn,
) -> (NodeTaskHandle, NodeTask<Conn>) {
    let (tx, rx) = mpsc::channel(config.io.send_buffer_size);

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
    conn: &KafkaChannel,
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

#[cfg(test)]
mod test {
    // TODO tests
    // it should wait for the delay before connecting
    // it should exit when failing to connect
    // it should time out the connection
    // it should negotiate versions
    //  it should try api version request v0 if highest version fails
    // it should exit if version negotiation fails
    // it should retry the last request if the connection closes
}
