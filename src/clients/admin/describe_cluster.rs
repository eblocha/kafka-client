use std::future::Future;

use extend::ext;
use kafka_protocol::messages::{DescribeClusterRequest, DescribeClusterResponse};

use crate::clients::network::NetworkClient;

pub fn describe_cluster_request() -> DescribeClusterRequest {
    let mut req = DescribeClusterRequest::default();

    req.endpoint_type = 1;
    req.include_cluster_authorized_operations = true;

    req
}

#[ext(pub, name = DescribeCluster)]
impl &NetworkClient {
    // using async fn will add it to the trait definition
    #[allow(clippy::manual_async_fn)]
    fn describe_cluster(self) -> impl Future<Output = anyhow::Result<DescribeClusterResponse>> {
        async { Ok(self.send(describe_cluster_request()).await?) }
    }
}
