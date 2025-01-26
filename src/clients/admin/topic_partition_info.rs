use fnv::FnvHashMap;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition,
};

use crate::{common::Node, proto::error_codes::ErrorCode};

#[derive(Debug, Clone)]
pub struct TopicPartitionInfo {
    pub partition: i32,
    pub leader: Option<Node>,
    pub replicas: Vec<Node>,
    pub isr: Vec<Node>,
}

pub type ToTopicPartitionInfo<'m> = (
    MetadataResponsePartition,
    &'m FnvHashMap<i32, MetadataResponseBroker>,
);

impl<'m> TryFrom<ToTopicPartitionInfo<'m>> for TopicPartitionInfo {
    type Error = ErrorCode;

    fn try_from((partition, map): ToTopicPartitionInfo<'m>) -> Result<Self, Self::Error> {
        if partition.error_code != ErrorCode::None as i16 {
            return Err(partition.error_code.into());
        }

        let leader = map.get(&partition.leader_id).map(Node::from);

        let replicas = partition
            .replica_nodes
            .into_iter()
            .filter_map(|id| map.get(&id))
            .map(Node::from)
            .collect::<Vec<_>>();

        let isr = partition
            .isr_nodes
            .into_iter()
            .filter_map(|id| map.get(&id))
            .map(Node::from)
            .collect::<Vec<_>>();

        Ok(Self {
            partition: partition.partition_index,
            leader,
            replicas,
            isr,
        })
    }
}
