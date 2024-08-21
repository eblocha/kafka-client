use futures::TryFutureExt;
use kafka_protocol::{
    messages::{ApiVersionsRequest, ApiVersionsResponse},
    protocol::{Message, StrBytes, VersionRange},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

use crate::{
    conn::{
        codec::VersionedRequest,
        config::KafkaConnectionConfig,
        conn::{KafkaConnection, KafkaConnectionError, ResponseSender},
        host::BrokerHost,
        PreparedConnectionInitError, Sendable, Versionable,
    },
    proto::{error_codes::ErrorCode, request::KafkaRequest},
};

pub struct NodeTaskMessage {
    request: KafkaRequest,
    tx: ResponseSender,
}

/// A connection task to a broker.
///
/// Contains a receiver that forwards requests to the connection.
#[derive(Debug)]
pub struct NodeTask {
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
    pub attempts: u32,
    /// Delay before attempting to connect
    pub delay: Option<Duration>,
    /// This node has acquired a connection
    pub connected: Arc<AtomicBool>,
}

impl NodeTask {
    /// Attempts to connect after the specified delay, then starts accepting messages and forwarding to the Kafka stream.
    ///
    /// If the kafka stream has any problems, this will return `Self` to enable reuse of the message channel in a new
    /// connection.
    pub async fn run(mut self) -> Self {
        self.attempts += 1;

        tracing::debug!(
            broker_id = self.broker_id,
            host = ?self.host,
            attempts = self.attempts,
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

        let connect_fut = TcpStream::connect((self.host.0.as_ref(), self.host.1))
            .and_then(|io| KafkaConnection::connect(io, &self.config));

        let result = tokio::select! {
            biased;
            _ = self.cancellation_token.cancelled() => return self,
            result = tokio::time::timeout(self.connection_timeout, connect_fut) => result,
            else => return self
        };

        let conn = match result {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                tracing::error!(
                    broker_id = self.broker_id,
                    host = ?self.host,
                    attempts = self.attempts,
                    "failed to connect: {e:?}",
                );
                return self;
            }
            Err(_) => {
                tracing::error!(
                    broker_id = self.broker_id,
                    host = ?self.host,
                    attempts = self.attempts,
                    "connection timed out",
                );
                return self;
            }
        };

        let Ok(versions) = negotiate(self.broker_id, &self.host, self.attempts, &conn).await else {
            return self;
        };

        // TODO authenticate

        self.connected.store(true, Ordering::SeqCst);

        // try to send the last message sent if it failed
        if let Some(last_message) = self.last_message.take() {
            if let Err(err) = conn.sender().send(last_message).await {
                tracing::error!(
                    broker_id = self.broker_id,
                    host = ?self.host,
                    attempts = self.attempts,
                    "failed to re-send last sent message"
                );
                // if we're here, the connection we just created is already closed.
                self.last_message = Some(err.0);
                return self;
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
                        attempts = self.attempts,
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

            if let Err(err) = conn.sender().send((versioned, tx)).await {
                tracing::error!(
                    broker_id = self.broker_id,
                    host = ?self.host,
                    attempts = self.attempts,
                    "connection closed unexpectedly, storing last sent message"
                );
                self.last_message = Some(err.0);
                return self;
            }
        }
    }
}

/// Handle to a [`NodeTask`], to control its lifecycle.
#[derive(Debug, Clone)]
pub struct NodeTaskHandle {
    pub tx: mpsc::Sender<NodeTaskMessage>,
    pub connected: Arc<AtomicBool>,
    pub cancellation_token: CancellationToken,
}

impl NodeTaskHandle {
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
pub fn new_pair(
    broker_id: i32,
    host: BrokerHost,
    connection_timeout: Duration,
    config: KafkaConnectionConfig,
) -> (NodeTaskHandle, NodeTask) {
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
        attempts: 0,
        delay: None,
        connected: handle.connected.clone(),
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
    attempts: u32,
    conn: &KafkaConnection,
) -> Result<ApiVersionsResponse, PreparedConnectionInitError> {
    tracing::debug!(
        broker_id = broker_id,
        host = ?host,
        attempts = attempts,
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
                attempts = attempts,
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
            attempts = attempts,
            "version negotiation completed successfully"
        );
        Ok(api_versions_response)
    } else {
        let e = PreparedConnectionInitError::NegotiationFailed(error_code);
        tracing::error!(
            broker_id = broker_id,
            host = ?host,
            attempts = attempts,
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
        return range.min;
    }

    intersection.max
}
