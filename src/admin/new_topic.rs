/// Automatically assign the replica nodes for this topic's partitions.
#[derive(Debug, Clone)]
pub struct AutoAssignmentNewTopic {
    /// The topic name
    pub name: String,
    /// The partition count, or cluster default if None
    pub partitions: Option<i32>,
    /// The replication factor, or cluster default if None
    pub replication_factor: Option<i16>,
}

/// Assign replica nodes for each partition to an explicit set of broker nodes.
#[derive(Debug, Clone)]
pub struct ExplicitAssignmentNewTopic {
    /// The topic name
    pub name: String,
    /// Mapping of partition id to broker ids that will serve as replicas for the partition.
    pub replicas_assignments: indexmap::IndexMap<i32, Vec<i32>>,
}

/// Describes how to create a new topic
#[derive(Debug, Clone)]
pub enum NewTopic {
    /// Automatically assign the replica nodes for this topic's partitions.
    AutoAssignment(AutoAssignmentNewTopic),
    /// Assign replica nodes for each partition to an explicit set of broker nodes.
    ExplicitAssignment(ExplicitAssignmentNewTopic),
}
