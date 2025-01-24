use std::sync::Arc;

use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use uuid::Uuid;

use crate::{proto::error_codes::ErrorCode, util::UuidExt};

pub struct DeletedTopic {
    pub id: Option<Uuid>,
}

pub type DeleteTopicsResult = indexmap::IndexMap<Arc<str>, Result<DeletedTopic, ErrorCode>>;

impl TryFrom<DeletableTopicResult> for DeletedTopic {
    type Error = ErrorCode;

    fn try_from(value: DeletableTopicResult) -> Result<Self, Self::Error> {
        if value.error_code != ErrorCode::None as i16 {
            return Err(value.error_code.into());
        }

        Ok(Self {
            id: value.topic_id.as_optional(),
        })
    }
}
