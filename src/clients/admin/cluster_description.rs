use std::sync::Arc;

use fnv::FnvHashSet;
use kafka_protocol::messages::DescribeClusterResponse;

use crate::{
    common::{
        acl::{acl_from_bitfield, AclOperation},
        Node,
    },
    proto::error_codes::ErrorCode,
    util::StrBytesExt,
};

pub struct ClusterDescription {
    pub authorized_operations: Option<FnvHashSet<AclOperation>>,
    pub cluster_id: Arc<str>,
    pub controller: Option<Node>,
    pub nodes: Vec<Node>,
}

impl TryFrom<DescribeClusterResponse> for ClusterDescription {
    type Error = ErrorCode;

    fn try_from(value: DescribeClusterResponse) -> Result<Self, Self::Error> {
        if value.error_code != ErrorCode::None as i16 {
            return Err(value.error_code.into());
        }

        let controller = value
            .brokers
            .iter()
            .find(|broker| broker.broker_id == value.controller_id)
            .map(Node::from);

        let nodes = value.brokers.iter().map(Node::from).collect();

        Ok(Self {
            authorized_operations: acl_from_bitfield(value.cluster_authorized_operations),
            cluster_id: value.cluster_id.as_arc_str(),
            controller,
            nodes,
        })
    }
}
