use crate::conn::PreparedConnection;

pub mod admin;

pub trait Run {
    type Response;

    async fn run(self, conn: &PreparedConnection) -> anyhow::Result<Self::Response>;
}
