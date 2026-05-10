use kafka_protocol::messages::{
    MetadataRequest, MetadataResponse, metadata_request::MetadataRequestTopic,
};

use crate::{
    backoff::BackoffSession, broker::task::BrokerTaskHandle, error::KafkaError,
    proto::ver::with_max_version,
};

use super::{RefreshMetadataRequest, cluster_state::BrokerMapEntry};

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

pub struct MetadataRefreshContext<TaskHandle> {
    pub entry: BrokerMapEntry<TaskHandle>,
    pub request: Option<RefreshMetadataRequest>,
    pub backoff: BackoffSession<()>,
}

pub struct MetadataRefreshTask<TaskHandle> {
    pub context: MetadataRefreshContext<TaskHandle>,
    pub topics: Option<Vec<MetadataRequestTopic>>,
}

pub type MetadataRefreshResult<TaskHandle> = (
    MetadataRefreshContext<TaskHandle>,
    Result<MetadataResponse, KafkaError>,
);

impl<TaskHandle: BrokerTaskHandle + Send + 'static> MetadataRefreshTask<TaskHandle> {
    pub async fn run(mut self) -> MetadataRefreshResult<TaskHandle> {
        tracing::debug!("refreshing metadata {:?}", self.topics);

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
