use std::{fmt::Display, sync::Arc};

use kafka_protocol::messages::{
    describe_cluster_response::DescribeClusterBroker, metadata_response::MetadataResponseBroker,
    BrokerId,
};

use super::BrokerHost;

#[derive(Debug, Clone)]
pub struct Node {
    pub id: i32,
    pub host: BrokerHost,
    pub rack: Option<Arc<str>>,
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let empty = Arc::from("");

        write!(
            f,
            "Node(id: {}, host: {:?}{}{})",
            self.id,
            self.host,
            if self.rack.is_some() { ", rack: " } else { "" },
            if let Some(ref rack) = self.rack {
                rack
            } else {
                &empty
            }
        )
    }
}

impl From<(BrokerId, &MetadataResponseBroker)> for Node {
    fn from((id, broker): (BrokerId, &MetadataResponseBroker)) -> Self {
        Self {
            id: id.0,
            host: BrokerHost::from(broker),
            rack: broker.rack.clone().map(|s| Arc::from(s.as_str())),
        }
    }
}

impl From<(BrokerId, &DescribeClusterBroker)> for Node {
    fn from((id, broker): (BrokerId, &DescribeClusterBroker)) -> Self {
        Self {
            id: id.0,
            host: BrokerHost::from(broker),
            rack: broker.rack.clone().map(|s| Arc::from(s.as_str())),
        }
    }
}
