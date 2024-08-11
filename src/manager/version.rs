use kafka_protocol::{
    messages::{ApiVersionsRequest, ApiVersionsResponse},
    protocol::{Message, StrBytes, VersionRange},
};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::task::task_tracker::TaskTrackerWaitFuture;

use crate::{
    conn::{KafkaConnection, KafkaConnectionConfig, KafkaConnectionError, Sendable},
    proto::error_codes::ErrorCode,
};

/// A connection that will lazily negotiate api request versions with the server
pub struct VersionedConnection {
    api_versions: ApiVersionsResponse,
    conn: KafkaConnection,
}

#[derive(Debug, Error)]
pub enum VersionedConnectionInitializationError {
    #[error(transparent)]
    Client(#[from] KafkaConnectionError),

    #[error("negotiation returned an error code: {0:?}")]
    NegotiationFailed(i16),
}

#[derive(Debug, Error)]
pub enum VersionedConnectionError {
    /// There was an error from the kafka connection
    #[error(transparent)]
    Client(#[from] KafkaConnectionError),

    /// The server does not support any version in the request range
    #[error("the server does not support any version in the request range")]
    Version,
}

fn create_version_request() -> ApiVersionsRequest {
    let mut r = ApiVersionsRequest::default();
    // TODO client software name and version
    r.client_software_name = StrBytes::from_static_str("eblocha-kafka");
    r.client_software_version = StrBytes::from_static_str("1.0");
    r
}

async fn negotiate(
    conn: &KafkaConnection,
) -> Result<ApiVersionsResponse, VersionedConnectionInitializationError> {
    let api_versions_response = conn
        .send(create_version_request(), ApiVersionsRequest::VERSIONS.max)
        .await?;

    let api_versions_response =
        if api_versions_response.error_code == ErrorCode::UnsupportedVersion as i16 {
            // fall back to min version if version request version is unsupported
            conn.send(create_version_request(), ApiVersionsRequest::VERSIONS.min)
                .await?
        } else {
            api_versions_response
        };

    if api_versions_response.error_code == ErrorCode::None as i16 {
        Ok(api_versions_response)
    } else {
        Err(VersionedConnectionInitializationError::NegotiationFailed(
            api_versions_response.error_code,
        ))
    }
}

impl VersionedConnection {
    /// Wrap an io stream with a Kafka connection, and negotiate api version information
    pub async fn connect<IO: AsyncRead + AsyncWrite + Send + 'static>(
        io: IO,
        config: &KafkaConnectionConfig,
    ) -> Result<Self, VersionedConnectionInitializationError> {
        let conn = KafkaConnection::connect(io, config).await.map_err(|e| {
            VersionedConnectionInitializationError::Client(KafkaConnectionError::Io(e))
        })?;

        let api_versions_response = negotiate(&conn).await?;

        Ok(Self {
            api_versions: api_versions_response,
            conn,
        })
    }

    /// Send a response using the highest common version
    pub async fn send<R: Sendable>(
        &mut self,
        req: R,
    ) -> Result<R::Response, VersionedConnectionError> {
        let version = self.determine_version(R::KEY, &R::VERSIONS);

        let Some(version) = version else {
            return Err(VersionedConnectionError::Version);
        };

        Ok(self.conn.send(req, version).await?)
    }

    /// Determine the maximum supported version for an api key.
    ///
    /// Returns None if there is no version overlap or the server does not support the request type
    pub fn determine_version(&self, api_key: i16, range: &VersionRange) -> Option<i16> {
        let Some(broker_versions) = self.api_versions.api_keys.get(&api_key) else {
            return None;
        };

        let intersection = range.intersect(&VersionRange {
            min: broker_versions.min_version,
            max: broker_versions.max_version,
        });

        if intersection.is_empty() {
            return None;
        }

        Some(intersection.max)
    }

    /// Shut down the connection
    /// 
    /// Returns a future that can be awaited to wait for shutdown to complete.
    pub fn shutdown(&self) -> TaskTrackerWaitFuture<'_> {
        self.conn.shutdown()
    }
}
