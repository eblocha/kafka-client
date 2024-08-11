use clap::Subcommand;

use crate::{clients::admin::list_topics::ListTopics, manager::version::VersionedConnection};

use super::Run;

#[derive(Subcommand)]
pub enum AdminCommands {
    ListTopics {
        #[arg(long, default_value_t = false)]
        exclude_internal: bool,
    },
}

impl Run for AdminCommands {
    type Response = ();

    async fn run(self, conn: &VersionedConnection) -> anyhow::Result<Self::Response> {
        match self {
            AdminCommands::ListTopics { exclude_internal } => {
                for topic in conn.list_topics().await? {
                    if !topic.internal || !exclude_internal {
                        println!("{topic}");
                    }
                }
            }
        }

        Ok(())
    }
}
