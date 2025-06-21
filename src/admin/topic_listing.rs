use std::sync::Arc;

use kafka_protocol::messages::metadata_response::MetadataResponseTopic;

use crate::{proto::error_codes::ErrorCode, util::StrBytesExt};

#[derive(Debug, Clone)]
pub struct TopicListing {
    pub name: Arc<str>,
    pub is_internal: bool,
}

impl TryFrom<MetadataResponseTopic> for TopicListing {
    type Error = ErrorCode;

    fn try_from(topic: MetadataResponseTopic) -> Result<Self, Self::Error> {
        if topic.error_code != ErrorCode::None as i16 {
            return Err(topic.error_code.into());
        }

        Ok(Self {
            name: topic.name.unwrap_or_default().as_arc_str(),
            is_internal: topic.is_internal,
        })
    }
}
