use kafka_protocol::{
    messages::{ApiVersionsRequest, ApiVersionsResponse},
    protocol::{Message, StrBytes, VersionRange},
};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

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

fn determine_version(
    range: &VersionRange,
    api_key: i16,
    response: &ApiVersionsResponse,
) -> Option<i16> {
    let Some(broker_versions) = response.api_keys.get(&api_key) else {
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

    pub async fn send<R: Sendable>(
        &mut self,
        req: R,
    ) -> Result<R::Response, VersionedConnectionError> {
        let version = determine_version(&R::VERSIONS, R::KEY, &self.api_versions);

        let Some(version) = version else {
            return Err(VersionedConnectionError::Version);
        };

        Ok(self.conn.send(req, version).await?)
    }
}
