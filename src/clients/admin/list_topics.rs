use std::{fmt::Display, future::Future};

use kafka_protocol::messages::MetadataRequest;
use uuid::Uuid;

use crate::manager::version::VersionedConnection;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicListing {
    pub name: String,
    pub id: Uuid,
    pub internal: bool,
}

impl Display for TopicListing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.internal {
            write!(f, "{} (internal)", self.name)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

pub fn list_topics_request() -> MetadataRequest {
    let mut req = MetadataRequest::default();
    req.topics = None;
    req.allow_auto_topic_creation = true;
    req
}

pub trait ListTopics {
    fn list_topics(self) -> impl Future<Output = anyhow::Result<Vec<TopicListing>>> + Send;
}

impl ListTopics for &VersionedConnection {
    async fn list_topics(self) -> anyhow::Result<Vec<TopicListing>> {
        let res = self.send(list_topics_request()).await?;

        Ok(res
            .topics
            .into_iter()
            .map(|(name, meta)| TopicListing {
                name: name.0.to_string(),
                id: meta.topic_id,
                internal: meta.is_internal,
            })
            .collect())
    }
}
