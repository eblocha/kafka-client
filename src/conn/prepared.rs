use std::io;

use kafka_protocol::protocol::{Request, VersionRange};
use thiserror::Error;

use crate::proto::error_codes::ErrorCode;

use super::conn::KafkaConnectionError;

/// Represents a request that can determine the api versions it supports.
pub trait Versionable {
    fn key(&self) -> i16;
    fn versions(&self) -> VersionRange;
}

impl<T: Request> Versionable for T {
    #[inline]
    fn key(&self) -> i16 {
        T::KEY
    }

    #[inline]
    fn versions(&self) -> VersionRange {
        T::VERSIONS
    }
}

/// Errors associated with establishing and preparing a Kafka connection.
#[derive(Debug, Error)]
pub enum PreparedConnectionInitError {
    /// Indicates an IO problem. This could be a bad socket or an encoding problem.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// The client has stopped processing requests
    #[error("the connection is closed")]
    Closed,

    #[error("version negotiation returned an error code: {0:?}")]
    NegotiationFailed(ErrorCode),
}

impl From<KafkaConnectionError> for PreparedConnectionInitError {
    fn from(value: KafkaConnectionError) -> Self {
        match value {
            KafkaConnectionError::Io(e) => Self::Io(e),
            KafkaConnectionError::Closed => Self::Closed,
        }
    }
}
