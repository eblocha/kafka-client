use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::{
    conn::{selector::ConnectionInitError, KafkaChannelError},
    proto::error_codes::ErrorCode,
};

/// All errors related to interacting with Kafka
#[derive(Debug, Error)]
pub enum KafkaError {
    /// Errors related to sending messages on a connected channel.
    ///
    /// These are typically fatal for the connection to the broker.
    #[error(transparent)]
    Channel(#[from] KafkaChannelError),

    /// Errors related to establishing a new connection.
    ///
    /// Typically these can be retried immediately, as the node task will handle backoff.
    #[error(transparent)]
    Init(#[from] ConnectionInitError),

    /// Errors with the data sent to or from a broker.
    ///
    /// Typically these mean the client needs to invalidate or update its state to align with the broker.
    #[error(transparent)]
    ErrorCode(#[from] ErrorCode),
}

impl<T> From<mpsc::error::SendError<T>> for KafkaError {
    fn from(_value: mpsc::error::SendError<T>) -> Self {
        KafkaChannelError::Closed.into()
    }
}

impl From<oneshot::error::RecvError> for KafkaError {
    fn from(_value: oneshot::error::RecvError) -> Self {
        KafkaChannelError::Closed.into()
    }
}
