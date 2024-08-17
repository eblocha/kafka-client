use clap::Subcommand;

use crate::clients::{
    admin::{describe_cluster::DescribeCluster, list_topics::ListTopics},
    network::NetworkClient,
};

use super::Run;

#[derive(Subcommand)]
pub enum AdminCommands {
    ListTopics {
        #[arg(long, default_value_t = false)]
        exclude_internal: bool,
    },
    DescribeCluster {},
}

impl Run for AdminCommands {
    type Response = ();

    async fn run(self, conn: &NetworkClient) -> anyhow::Result<Self::Response> {
        match self {
            AdminCommands::ListTopics { exclude_internal } => {
                let res = conn.list_topics().await?;

                for (name, topic) in res.topics {
                    if !topic.is_internal || !exclude_internal {
                        println!(
                            "{}: {} {} {}",
                            name.to_string(),
                            topic.partitions.len(),
                            if topic.partitions.len() == 1 {
                                "partition"
                            } else {
                                "partitions"
                            },
                            if topic.is_internal { " (internal)" } else { "" }
                        );
                    }
                }
            }
            AdminCommands::DescribeCluster {} => {
                let res = conn.describe_cluster().await?;

                println!("Cluster ID: {}", res.cluster_id.to_string());

                for (broker_id, broker) in res.brokers {
                    println!(
                        "Broker {}: {}:{}{}",
                        broker_id.to_string(),
                        broker.host.to_string(),
                        broker.port,
                        if broker_id == res.controller_id {
                            " (controller)"
                        } else {
                            ""
                        }
                    );
                }
            }
        }

        Ok(())
    }
}
