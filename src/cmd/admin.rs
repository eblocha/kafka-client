use std::iter::zip;

use anyhow::bail;
use clap::Subcommand;

use kafka_client::{
    clients::{
        admin::{AdminClient, AutoAssignmentNewTopic, NewTopic},
        network::NetworkClient,
    },
    common::TopicCollection,
};

use super::Run;

#[derive(Subcommand)]
pub enum AdminCommands {
    /// List all topic names in the cluster.
    ListTopics {
        /// Exclude topics marked as internal.
        #[arg(long, default_value_t = false)]
        exclude_internal: bool,
    },
    /// Describe the partitions, replicas, and isr nodes for a set of topics.
    DescribeTopics {
        /// A comma-separated list of topic names to describe.
        #[arg(short, long, value_delimiter = ',', num_args = 1.., required = true)]
        topics: Vec<String>,
    },
    /// Describe the cluster's nodes, controller, and cluster id information.
    DescribeCluster {},
    /// Create a topic.
    CreateTopics {
        /// The topic name.
        #[arg(long, required = true)]
        name: String,
        /// Number of partitions. If omitted, this will use the server default.
        #[arg(short, long)]
        partitions: Option<i32>,
        /// Replication factor. If omitted, this will use the server default.
        #[arg(short, long)]
        replication_factor: Option<i16>,
    },
    /// Delete a set of topics, removing all data. This is irreversible.
    DeleteTopics {
        /// A comma-separated list of topic names to delete.
        #[arg(short, long, value_delimiter = ',', num_args = 1.., required = true)]
        topics: Vec<String>,
    },
}

impl AdminCommands {
    async fn run_inner(self, client: &AdminClient) -> anyhow::Result<()> {
        match self {
            AdminCommands::ListTopics { exclude_internal } => {
                let topics = client.list_topics().await?;

                for topic in topics {
                    if !topic.is_internal || !exclude_internal {
                        println!(
                            "{}{}",
                            topic.name,
                            if topic.is_internal { " (internal)" } else { "" }
                        );
                    }
                }
            }
            AdminCommands::DescribeTopics { topics } => {
                let mut some_failed = false;
                let results = client.describe_topics(topics.clone()).await?;

                for (name, result) in zip(topics, results) {
                    some_failed = result.is_err();
                    let topic = match result {
                        Ok(topic) => topic,
                        Err(e) => {
                            println!("{name}: ERROR: {e}");
                            continue;
                        }
                    };

                    let id_text = match topic.id {
                        Some(id) => format!(" (id: {id})"),
                        None => String::new(),
                    };

                    println!(
                        "{}{}{}",
                        topic
                            .name
                            .as_ref()
                            .map_or(name.as_str(), |name| name.as_str()),
                        id_text,
                        if topic.is_internal { " (internal)" } else { "" }
                    );

                    let acl_text = match topic.authorized_operations {
                        Some(acl) => format!("{acl:?}"),
                        None => "None".to_owned(),
                    };

                    println!("    authorized ops: {acl_text}");

                    for partition in topic.partitions {
                        let leader_text = match partition.leader {
                            Some(node) => format!("{node}"),
                            None => "Unknown".to_owned(),
                        };

                        println!(
                            "    partition {}, leader: {}",
                            partition.partition, leader_text
                        );
                        println!(
                            "        replicas: {}",
                            partition
                                .replicas
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                        println!(
                            "        isr: {}",
                            partition
                                .isr
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                    }
                }

                if some_failed {
                    bail!("failed to describe all topics")
                }
            }
            AdminCommands::DescribeCluster {} => {
                let cluster = client.describe_cluster().await?;

                let controller_id = cluster.controller.map(|node| node.id);

                println!("Cluster id: {}", cluster.cluster_id);

                for node in cluster.nodes {
                    println!(
                        "{node}{}",
                        if Some(node.id) == controller_id {
                            " (controller)"
                        } else {
                            ""
                        }
                    );
                }
            }
            AdminCommands::CreateTopics {
                name,
                partitions,
                replication_factor,
            } => {
                let mut some_failed = false;
                let results = client
                    .create_topics(vec![NewTopic::AutoAssignment(AutoAssignmentNewTopic {
                        name: name.clone(),
                        partitions,
                        replication_factor,
                    })])
                    .await?;

                for result in results {
                    some_failed = result.is_err();
                    let result = match result {
                        Ok(result) => result,
                        Err(e) => {
                            println!("topic {name}: ERROR: {e}");
                            continue;
                        }
                    };

                    let id_text = match result.id {
                        Some(id) => format!(" (id: {id})"),
                        None => String::new(),
                    };

                    println!(
                        "Created topic: {}{} with partitions {} and replication factor {}",
                        result.name.as_str(),
                        id_text,
                        result.partitions,
                        result.replication_factor
                    );
                }

                if some_failed {
                    bail!("failed to create some topics")
                }
            }
            AdminCommands::DeleteTopics { topics } => {
                let mut some_failed = false;
                let results = client
                    .delete_topics(TopicCollection::Names(topics.clone()))
                    .await?;

                for (name, result) in zip(topics, results.into_iter()) {
                    some_failed = result.is_err();
                    match result {
                        Ok(deleted) => {
                            println!(
                                "{}: OK",
                                deleted.name.as_ref().map_or(name.as_str(), |t| t.as_str())
                            );
                        }
                        Err(e) => {
                            println!("{name}: ERROR: {e}");
                        }
                    };
                }

                if some_failed {
                    bail!("failed to delete some topics")
                }
            }
        }

        Ok(())
    }
}

impl Run for AdminCommands {
    type Response = ();

    async fn run(self, conn: NetworkClient) -> anyhow::Result<Self::Response> {
        let client = AdminClient::new(conn);

        let result = self.run_inner(&client).await;

        client.shutdown().await;

        result
    }
}
