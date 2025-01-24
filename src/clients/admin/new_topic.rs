#[derive(Debug, Clone)]
pub struct AutoAssignmentNewTopic {
    /// The topic name
    pub name: String,
    /// The partition count, or default if None
    pub partitions: Option<i32>,
    /// The replication factor, or default if None
    pub replication_factor: Option<i16>,
}

#[derive(Debug, Clone)]
pub struct ExplicitAssignmentNewTopic {
    /// The topic name
    pub name: String,
    /// Mapping of partition id to broker ids that will serve as replicas for the partition.
    pub replicas_assignments: indexmap::IndexMap<i32, Vec<i32>>,
}

#[derive(Debug, Clone)]
pub enum NewTopic {
    AutoAssignment(AutoAssignmentNewTopic),
    ExplicitAssignment(ExplicitAssignmentNewTopic),
}
