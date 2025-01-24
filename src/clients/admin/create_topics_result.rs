use std::sync::Arc;

use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use uuid::Uuid;

use crate::{proto::error_codes::ErrorCode, util::UuidExt};

pub struct TopicMetadataAndConfig {
    pub id: Option<Uuid>,
    pub partitions: i32,
    pub replication_factor: i16,
    // TODO config: Result<Config, ErrorCode>
}

/// Mapping of topic name to the create result for the topic
pub type CreateTopicsResult =
    indexmap::IndexMap<Arc<str>, Result<TopicMetadataAndConfig, ErrorCode>>;

impl TryFrom<CreatableTopicResult> for TopicMetadataAndConfig {
    type Error = ErrorCode;

    fn try_from(value: CreatableTopicResult) -> Result<Self, Self::Error> {
        if value.error_code != ErrorCode::None as i16 {
            return Err(value.error_code.into());
        }

        Ok(Self {
            id: value.topic_id.as_optional(),
            partitions: value.num_partitions,
            replication_factor: value.replication_factor,
        })
    }
}
