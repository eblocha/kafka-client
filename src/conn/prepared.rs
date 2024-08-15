use kafka_protocol::{
    messages::{ApiVersionsRequest, ApiVersionsResponse},
    protocol::{Message, Request, StrBytes, VersionRange},
};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::task::task_tracker::TaskTrackerWaitFuture;

use crate::{
    conn::{KafkaConnection, KafkaConnectionConfig, KafkaConnectionError, Sendable},
    proto::error_codes::ErrorCode,
};

pub trait VersionedRequest {
    fn key(&self) -> i16;
    fn versions(&self) -> VersionRange;
}

impl<T: Request> VersionedRequest for T {
    #[inline]
    fn key(&self) -> i16 {
        T::KEY
    }

    #[inline]
    fn versions(&self) -> VersionRange {
        T::VERSIONS
    }
}

/// A connection that will lazily negotiate api request versions with the server
pub struct PreparedConnection {
    api_versions: ApiVersionsResponse,
    conn: KafkaConnection,
}

#[derive(Debug, Error)]
pub enum PreparedConnectionInitializationError {
    #[error(transparent)]
    Client(#[from] KafkaConnectionError),

    #[error("version negotiation returned an error code: {0:?}")]
    NegotiationFailed(i16),
}

#[derive(Debug, Error)]
pub enum PreparedConnectionError {
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
) -> Result<ApiVersionsResponse, PreparedConnectionInitializationError> {
    let api_versions_response = conn
        .send(
            create_version_request(),
            <ApiVersionsRequest as Message>::VERSIONS.max,
        )
        .await?;

    let api_versions_response =
        if api_versions_response.error_code == ErrorCode::UnsupportedVersion as i16 {
            // fall back to min version if version request version is unsupported
            conn.send(
                create_version_request(),
                <ApiVersionsRequest as Message>::VERSIONS.min,
            )
            .await?
        } else {
            api_versions_response
        };

    if api_versions_response.error_code == ErrorCode::None as i16 {
        Ok(api_versions_response)
    } else {
        Err(PreparedConnectionInitializationError::NegotiationFailed(
            api_versions_response.error_code,
        ))
    }
}

impl PreparedConnection {
    /// Wrap an io stream with a Kafka connection, and negotiate api version information
    pub async fn connect<IO: AsyncRead + AsyncWrite + Send + 'static>(
        io: IO,
        config: &KafkaConnectionConfig,
    ) -> Result<Self, PreparedConnectionInitializationError> {
        let conn = KafkaConnection::connect(io, config).await.map_err(|e| {
            PreparedConnectionInitializationError::Client(KafkaConnectionError::Io(e))
        })?;

        let api_versions_response = negotiate(&conn).await?;

        Ok(Self {
            api_versions: api_versions_response,
            conn,
        })
    }

    /// Send a request using the highest common version
    pub async fn send<R: Sendable + VersionedRequest>(
        &self,
        req: R,
    ) -> Result<R::Response, PreparedConnectionError> {
        let version = self.determine_version(req.key(), &req.versions());

        let Some(version) = version else {
            return Err(PreparedConnectionError::Version);
        };

        let res = self.conn.send(req, version).await;

        Ok(res?)
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

    /// Returns true if the connection is closed and will no longer process requests
    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    /// Waits until the connection is closed
    pub fn closed(&self) -> TaskTrackerWaitFuture<'_> {
        self.conn.closed()
    }

    pub fn capacity(&self) -> usize {
        self.conn.capacity()
    }

    pub fn max_capacity(&self) -> usize {
        self.conn.max_capacity()
    }
}
