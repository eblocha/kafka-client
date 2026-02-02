use std::{fmt::Display, sync::Arc};

use kafka_protocol::messages::{
    describe_cluster_response::DescribeClusterBroker, metadata_response::MetadataResponseBroker,
};

use crate::util::StrBytesExt;

use super::BrokerHost;

#[derive(Debug, Clone, PartialEq)]
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

impl From<&MetadataResponseBroker> for Node {
    fn from(broker: &MetadataResponseBroker) -> Self {
        Self {
            id: broker.node_id.0,
            host: BrokerHost::from(broker),
            rack: broker.rack.clone().map(|s| s.as_arc_str()),
        }
    }
}

impl From<&DescribeClusterBroker> for Node {
    fn from(broker: &DescribeClusterBroker) -> Self {
        Self {
            id: broker.broker_id.0,
            host: BrokerHost::from(broker),
            rack: broker.rack.clone().map(|s| s.as_arc_str()),
        }
    }
}
