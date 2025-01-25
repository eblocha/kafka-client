use std::fmt::Display;

use kafka_protocol::messages::TopicName;

/// A topic name and partition pair
#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct TopicPartition(TopicName, i32);

impl TopicPartition {
    pub fn new(name: TopicName, partition: i32) -> Self {
        Self(name, partition)
    }

    pub fn name(&self) -> &TopicName {
        &self.0
    }

    pub fn partition(&self) -> i32 {
        self.1
    }
}

impl Display for TopicPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.name().as_str(), self.partition())
    }
}
