use crate::clients::network::NetworkClient;

pub mod admin;
pub mod producer;

pub trait Run {
    type Response;

    async fn run(self, client: &NetworkClient) -> anyhow::Result<Self::Response>;
}
