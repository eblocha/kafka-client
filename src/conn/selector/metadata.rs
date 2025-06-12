use kafka_protocol::messages::{
    metadata_request::MetadataRequestTopic, MetadataRequest, MetadataResponse,
};

use crate::{backoff::BackoffSession, error::KafkaError, proto::ver::with_max_version};

use super::{cluster::BrokerMapEntry, RefreshMetadataRequest};

fn create_metadata_request(
    version: i16,
    topics: Option<Vec<MetadataRequestTopic>>,
) -> MetadataRequest {
    let mut r = MetadataRequest::default();

    if version >= 4 {
        r.allow_auto_topic_creation = false;
    }

    if version >= 8 {
        if version <= 10 {
            r.include_cluster_authorized_operations = true;
        }

        r.include_topic_authorized_operations = true;
    }

    r.topics = topics;

    r
}

pub struct MetadataRefreshContext {
    pub entry: BrokerMapEntry,
    pub request: Option<RefreshMetadataRequest>,
    pub backoff: BackoffSession<()>,
}

pub struct MetadataRefreshTask {
    pub context: MetadataRefreshContext,
    pub topics: Option<Vec<MetadataRequestTopic>>,
}

pub type MetadataRefreshResult = (MetadataRefreshContext, Result<MetadataResponse, KafkaError>);

impl MetadataRefreshTask {
    pub async fn run(mut self) -> MetadataRefreshResult {
        self.context.backoff.wait_next().await;

        let metadata = self
            .context
            .entry
            .handle
            .send(with_max_version(move |ver| {
                Some(create_metadata_request(ver, self.topics))
            }))
            .await;

        (self.context, metadata)
    }
}
