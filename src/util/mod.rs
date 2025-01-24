use std::sync::Arc;

use extend::ext;
use kafka_protocol::{messages::TopicName, protocol::StrBytes};
use uuid::Uuid;

#[ext]
pub impl Uuid {
    /// Convert a uuid to [`None`] if the value is nil
    fn as_optional(&self) -> Option<Uuid> {
        if self.is_nil() {
            None
        } else {
            Some(self.clone())
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
