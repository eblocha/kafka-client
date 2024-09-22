use arc_swap::ArcSwapOption;
use kafka_protocol::{
    messages::{ApiVersionsRequest, ApiVersionsResponse},
    protocol::{Message, StrBytes, VersionRange},
};
use std::{io, sync::Arc};
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

#[derive(Debug)]
pub struct VersionedConnection {
    connection: KafkaChannel,
    versions: ApiVersionsResponse,
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
    /// Token to stop the task
    pub cancellation_token: CancellationToken,
    /// Options for the io stream
    pub config: ConnectionConfig,
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
            let NodeTaskMessage { request, tx } = tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return self,
                Some(msg) = self.rx.recv() => msg,
                else => return self
            };

            let Some(conn) = self.get_connection().await else {
                return self;
            };

            let api_key = request.key();
            let range = request.versions();

            let versioned = VersionedRequest {
                request,
                api_version: determine_version(&conn.versions, api_key, &range),
            };

            if tx.is_closed() {
                // the request was abandoned - no need to send it
                continue;
            }

            _ = conn
                .connection
                .sender()
                .send(KafkaChannelMessage { versioned, tx })
                .await;
        }
    }

    async fn try_connect(&mut self) -> Result<VersionedConnection, ConnectAttemptError> {
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
                    "failed to connect: {e:?}",
                );
                return Err(ConnectAttemptError::Failed);
            }
            Err(_) => {
                tracing::error!(
                    broker_id = self.broker_id,
                    host = ?self.host,
                    "connection timed out",
                );
                return Err(ConnectAttemptError::Failed);
            }
        };

        let Ok(versions) = negotiate(self.broker_id, &self.host, &conn).await else {
            return Err(ConnectAttemptError::Failed);
        };

        // TODO authenticate

        Ok(VersionedConnection {
            connection: conn,
            versions,
        })
    }

    async fn get_connection(&mut self) -> Option<Arc<VersionedConnection>> {
        if let Some(conn) = self
            .connection
            .load()
            .as_ref()
            .filter(|conn| !conn.connection.is_closed())
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

            match self.try_connect().await {
                Ok(conn) => break conn,
                Err(ConnectAttemptError::Cancelled) => return None,
                Err(ConnectAttemptError::Failed) => {}
            }

            let (min, max) = (self.config.retry.min_backoff, self.config.retry.max_backoff);

            let backoff = exponential_backoff(min, max, attempt);

            // back off
            tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return None,
                _ = tokio::time::sleep(backoff) => {}
            }

            attempt += 1;
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

    /// Determine if this node has an open connection to the host.
    pub fn is_connected(&self) -> bool {
        self.connection
            .load()
            .as_ref()
            .is_some_and(|conn| !conn.connection.is_closed())
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

    let connection = Arc::new(ArcSwapOption::empty());

    let handle = NodeTaskHandle {
        cancellation_token: CancellationToken::new(),
        connection,
        tx,
    };

    let task = NodeTask {
        broker_id,
        host,
        rx,
        cancellation_token: handle.cancellation_token.clone(),
        config,
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
