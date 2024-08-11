use crate::manager::version::VersionedConnection;

pub mod admin;

pub trait Run {
    type Response;

    async fn run(self, conn: &VersionedConnection) -> anyhow::Result<Self::Response>;
}
