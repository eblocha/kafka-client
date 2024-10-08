use std::future::Future;

use extend::ext;
use kafka_protocol::messages::{MetadataRequest, MetadataResponse};

use crate::clients::network::NetworkClient;

pub fn list_topics_request() -> MetadataRequest {
    let mut req = MetadataRequest::default();
    req.topics = None;
    req.allow_auto_topic_creation = true;
    req
}

#[ext(pub, name = ListTopics)]
impl &NetworkClient {
    // using async fn will add it to the trait definition
    #[allow(clippy::manual_async_fn)]
    fn list_topics(self) -> impl Future<Output = anyhow::Result<MetadataResponse>> {
        async { Ok(self.send(list_topics_request()).await?) }
    }
}
