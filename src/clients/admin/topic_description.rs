use std::sync::Arc;

use fnv::FnvHashSet;
use kafka_protocol::{
    indexmap::IndexMap,
    messages::{
        metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
        BrokerId, TopicName,
    },
};
use uuid::Uuid;

use crate::{
    common::acl::{acl_from_bitfield, AclOperation},
    proto::error_codes::ErrorCode,
    util::{StrBytesExt, UuidExt},
};

use super::TopicPartitionInfo;

/// A detailed description of a single topic in the cluster.
#[derive(Debug, Clone)]
pub struct TopicDescription {
    pub name: Arc<str>,
    pub is_internal: bool,
    pub partitions: Vec<TopicPartitionInfo>,
    pub authorized_operations: Option<FnvHashSet<AclOperation>>,
    pub id: Option<Uuid>,
}

pub type DescribeTopicsResult = indexmap::IndexMap<Arc<str>, Result<TopicDescription, ErrorCode>>;

pub type ToTopicDescription<'m> = (
    TopicName,
    MetadataResponseTopic,
    &'m IndexMap<BrokerId, MetadataResponseBroker>,
);

impl<'m> TryFrom<ToTopicDescription<'m>> for TopicDescription {
    type Error = ErrorCode;

    fn try_from((name, topic, map): ToTopicDescription<'m>) -> Result<Self, Self::Error> {
        if topic.error_code != 0 {
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
            name: name.as_arc_str(),
            partitions,
            authorized_operations: acl_from_bitfield(topic.topic_authorized_operations),
        })
    }
}
