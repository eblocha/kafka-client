use super::network::NetworkClient;

pub struct Producer {
    client: NetworkClient,
}

impl Producer {
    pub fn new(client: NetworkClient) -> Self {
        Self { client }
    }
}
