pub mod chunks_timeout;

use std::{sync::Arc, time::Duration};

use extend::ext;
use futures::Stream;
use kafka_protocol::{messages::TopicName, protocol::StrBytes};
use uuid::Uuid;

use crate::util::chunks_timeout::ChunksTimeout;

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

pub trait StreamExt: Stream {
    fn chunks_timeout(self, capacity: usize, duration: Duration) -> ChunksTimeout<Self>
    where
        Self: Sized,
    {
        ChunksTimeout::new(self, capacity, duration)
    }
}

impl<St: ?Sized> StreamExt for St where St: Stream {}
