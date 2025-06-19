use std::io;

use thiserror::Error;

use crate::{conn::KafkaChannelError, error::ErrorCode};

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

impl From<KafkaChannelError> for ConnectionInitError {
    fn from(value: KafkaChannelError) -> Self {
        match value {
            KafkaChannelError::Io(e) => Self::Io(e),
            KafkaChannelError::Closed => Self::Closed,
        }
    }
}
