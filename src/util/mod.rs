use std::sync::Arc;

use extend::ext;
use kafka_protocol::{
    messages::{metadata_response::MetadataResponsePartition, TopicName},
    protocol::StrBytes,
};
use uuid::Uuid;

#[ext]
pub impl Uuid {
    /// Convert a uuid to [`None`] if the value is nil
    fn as_optional(&self) -> Option<Uuid> {
        if self.is_nil() {
            None
        } else {
            Some(*self)
        }
    }
}

#[ext]
pub impl TopicName {
    fn from_string(string: String) -> Self {
        Self(StrBytes::from_string(string))
    }
}

#[ext]
pub impl StrBytes {
    fn as_arc_str(&self) -> Arc<str> {
        Arc::from(self.as_str())
    }
}

/// Find a partition by partition index using binary search.
pub fn find_partition(
    partitions: &[MetadataResponsePartition],
    partition_index: i32,
) -> Option<&MetadataResponsePartition> {
    partitions
        .binary_search_by(|part| part.partition_index.cmp(&partition_index))
        .ok()
        .map(|idx| &partitions[idx])
}
