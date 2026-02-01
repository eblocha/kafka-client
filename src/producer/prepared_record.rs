use std::time::Instant;

use kafka_protocol::records::Record;
use tokio::sync::oneshot;

use crate::{
    backoff::exponential_backoff_due, config::RetryConfig, error::KafkaError,
    producer::record::RecordMetadata,
};

#[derive(Default)]
pub struct DeliveryMetadata {
    /// Number of attempts made to send the message to the broker.
    pub attempts: u32,
}

/// A [`Record`] and [`oneshot::Sender`] pair that will await the response for the record.
pub struct PreparedRecord {
    /// A [`Record`] ready to be encoded.
    pub record: Record,
    /// The [`oneshot::Sender`] that is expecting a response for the record.
    pub tx: oneshot::Sender<Result<RecordMetadata, KafkaError>>,
    /// Message delivery information
    pub delivery: DeliveryMetadata,
}

impl PreparedRecord {
    pub(super) fn increment_attempt(&mut self, config: &RetryConfig) -> Option<Instant> {
        self.delivery.attempts += 1;
        exponential_backoff_due(config.backoff, config.backoff_max, self.delivery.attempts)
    }
}
