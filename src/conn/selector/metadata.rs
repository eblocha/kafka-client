use kafka_protocol::messages::{
    metadata_request::MetadataRequestTopic, MetadataRequest, MetadataResponse,
};

use crate::{conn::host::BrokerHost, error::KafkaError, proto::ver::with_max_version};

use super::{node_task::NodeTaskHandle, RefreshMetadataRequest};

fn create_metadata_request(
    version: i16,
    topics: Option<Vec<MetadataRequestTopic>>,
) -> Option<MetadataRequest> {
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

    Some(r)
}

pub struct MetadataRefreshContext {
    pub broker_id: i32,
    pub host: BrokerHost,
    pub node_handle: NodeTaskHandle,
    pub request: Option<RefreshMetadataRequest>,
}

pub struct MetadataRefreshTask {
    pub context: MetadataRefreshContext,
    pub topics: Option<Vec<MetadataRequestTopic>>,
}

pub type MetadataRefreshResult = (MetadataRefreshContext, Result<MetadataResponse, KafkaError>);

impl MetadataRefreshTask {
    pub async fn run(self) -> MetadataRefreshResult {
        tracing::info!(
            broker_id = self.context.broker_id,
            host = ?self.context.host,
            "attempting to refresh metadata"
        );

        let metadata = self
            .context
            .node_handle
            .send(with_max_version(move |ver| {
                create_metadata_request(ver, self.topics)
            }))
            .await;

        (self.context, metadata)
    }
}
