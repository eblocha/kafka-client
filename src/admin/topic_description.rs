use kafka_protocol::messages::{
    TopicName,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
};
use rustc_hash::{FxHashMap, FxHashSet};
use uuid::Uuid;

use crate::{
    common::acl::{AclOperation, acl_from_bitfield},
    proto::error_codes::ErrorCode,
    util::UuidExt,
};

use super::TopicPartitionInfo;

/// A detailed description of a single topic in the cluster.
#[derive(Debug, Clone)]
pub struct TopicDescription {
    pub name: Option<TopicName>,
    pub is_internal: bool,
    pub partitions: Vec<TopicPartitionInfo>,
    pub authorized_operations: Option<FxHashSet<AclOperation>>,
    pub id: Option<Uuid>,
}

pub type DescribeTopicsResult = Vec<Result<TopicDescription, ErrorCode>>;

pub type ToTopicDescription<'m> = (
    MetadataResponseTopic,
    &'m FxHashMap<i32, MetadataResponseBroker>,
);

impl<'m> TryFrom<ToTopicDescription<'m>> for TopicDescription {
    type Error = ErrorCode;

    fn try_from((topic, map): ToTopicDescription<'m>) -> Result<Self, Self::Error> {
        if topic.error_code != ErrorCode::None as i16 {
            return Err(topic.error_code.into());
        }

        let partitions: Vec<TopicPartitionInfo> = topic
            .partitions
            .into_iter()
            .map(|partition| TopicPartitionInfo::try_from((partition, map)))
            .collect::<Result<Vec<TopicPartitionInfo>, ErrorCode>>()?;

        Ok(Self {
            id: topic.topic_id.as_optional(),
            is_internal: topic.is_internal,
            name: topic.name,
            partitions,
            authorized_operations: acl_from_bitfield(topic.topic_authorized_operations),
        })
    }
}
