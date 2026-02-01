use crate::{
    conn::KafkaChannelError,
    error::{ErrorCode, KafkaError},
};

pub struct ErrorClassification {
    /// The error is permanent.
    /// The producer should mark the topic as errored for future sends
    #[allow(unused)]
    pub permanent: bool,
    /// The topic metadata should be refreshed
    pub refresh: bool,
    /// The records should be retried
    pub retry: bool,
}

impl ErrorClassification {
    fn fatal_idempotent_error() -> Self {
        Self {
            permanent: true,
            refresh: false,
            retry: false,
        }
    }
}

/// Classify a ProduceRequest error to determine what action(s) should be taken in response
pub fn classify_error(error: &KafkaError) -> ErrorClassification {
    match error {
        KafkaError::Channel(KafkaChannelError::Io(_)) => ErrorClassification {
            permanent: false,
            refresh: true,
            retry: true,
        },
        KafkaError::ErrorCode(
            ErrorCode::UnknownTopicOrPartition | ErrorCode::KafkaStorageError,
        ) => ErrorClassification {
            permanent: false,
            refresh: true,
            retry: true,
        },
        KafkaError::ErrorCode(ErrorCode::TopicAuthorizationFailed) => ErrorClassification {
            permanent: true,
            refresh: false,
            retry: false,
        },
        KafkaError::ErrorCode(
            ErrorCode::NotEnoughReplicas | ErrorCode::NotEnoughReplicasAfterAppend,
        ) => ErrorClassification {
            permanent: false,
            refresh: false,
            retry: true,
        },
        // Idempotent producer errors (always permanent)
        KafkaError::ErrorCode(
            ErrorCode::OutOfOrderSequenceNumber
            | ErrorCode::DuplicateSequenceNumber
            | ErrorCode::UnknownProducerId
            | ErrorCode::InvalidProducerEpoch,
        ) => ErrorClassification::fatal_idempotent_error(),
        _ => ErrorClassification {
            permanent: false,
            refresh: false,
            retry: false,
        },
    }
}
