use clap::Subcommand;
use kafka_protocol::{
    indexmap::IndexMap,
    messages::{create_topics_request::CreatableTopic, CreateTopicsRequest, TopicName},
    protocol::StrBytes,
};

use crate::{
    clients::{
        admin::{describe_cluster::DescribeCluster, list_topics::ListTopics},
        network::NetworkClient,
    },
    proto::error_codes::ErrorCode,
};

use super::Run;

#[derive(Subcommand)]
pub enum AdminCommands {
    ListTopics {
        #[arg(long, default_value_t = false)]
        exclude_internal: bool,
    },
    DescribeCluster {},
    CreateTopic {
        #[arg(long, required = true)]
        name: String,
        #[arg(short, long)]
        partitions: Option<i32>,
        #[arg(short, long)]
        replication_factor: Option<i16>,
    },
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
                            name.0.as_str(),
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

                println!("Cluster ID: {}", res.cluster_id.as_str());

                for (broker_id, broker) in res.brokers {
                    println!(
                        "Broker {}: {}:{}{}",
                        broker_id.0,
                        broker.host.as_str(),
                        broker.port,
                        if broker_id == res.controller_id {
                            " (controller)"
                        } else {
                            ""
                        }
                    );
                }
            }
            AdminCommands::CreateTopic {
                name,
                partitions,
                replication_factor,
            } => {
                let res = conn
                    .send({
                        let mut topic = CreatableTopic::default();
                        topic.num_partitions = partitions.unwrap_or(-1);
                        topic.replication_factor = replication_factor.unwrap_or(-1);

                        let mut r = CreateTopicsRequest::default();
                        r.timeout_ms = 5000;
                        r.topics =
                            IndexMap::from_iter([(TopicName(StrBytes::from_string(name)), topic)]);
                        r.validate_only = false;
                        r
                    })
                    .await?;

                for (name, topic_result) in res.topics {
                    let error_code = ErrorCode::from(topic_result.error_code);

                    if error_code != ErrorCode::None {
                        println!("topic: {}: creation failed: {error_code:?}", name.as_str());
                        continue;
                    }

                    println!(
                        "Created topic {} with {} partitions and factor {}",
                        name.as_str(),
                        topic_result.num_partitions,
                        topic_result.replication_factor
                    );
                }
            }
        }

        Ok(())
    }
}
