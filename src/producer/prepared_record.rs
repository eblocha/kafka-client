use kafka_protocol::records::Record;
use tokio::sync::oneshot;

use crate::{error::KafkaError, producer::record::RecordMetadata};

/// A [`Record`] and [`oneshot::Sender`] pair that will await the response for the record.
pub struct PreparedRecord {
    /// A [`Record`] ready to be encoded.
    pub record: Record,
    /// The [`oneshot::Sender`] that is expecting a response for the record.
    pub tx: oneshot::Sender<Result<RecordMetadata, KafkaError>>,
}
