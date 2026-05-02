use crate::common::TopicPartition;

#[derive(Debug, Hash, Clone)]
#[non_exhaustive]
pub enum Subscription {
    /// Subscribe to all topics whose name matches the provided regular expression.
    ///
    /// Be warned: this will trigger the client to request metadata for all topics in the cluster.
    TopicPattern(String),
    /// Subscribe to a particular topic by name.
    Topic(String),
    /// Subscribe to a particular topic partition.
    TopicPartition(TopicPartition),
}
