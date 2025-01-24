use std::sync::Arc;

use kafka_protocol::messages::{metadata_response::MetadataResponseTopic, TopicName};

use crate::proto::error_codes::ErrorCode;

#[derive(Debug, Clone)]
pub struct TopicListing {
    pub name: Arc<str>,
    pub is_internal: bool,
}

impl TryFrom<(TopicName, MetadataResponseTopic)> for TopicListing {
    type Error = ErrorCode;

    fn try_from((name, topic): (TopicName, MetadataResponseTopic)) -> Result<Self, Self::Error> {
        if topic.error_code != 0 {
            return Err(topic.error_code.into());
        }

        Ok(Self {
            name: Arc::from(name.as_str()),
            is_internal: topic.is_internal,
        })
    }
}
