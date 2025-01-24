use std::sync::Arc;

use kafka_protocol::{
    messages::{
        create_topics_request::{CreatableReplicaAssignment, CreatableTopic},
        metadata_request::MetadataRequestTopic,
        BrokerId, CreateTopicsRequest, DescribeClusterRequest, MetadataRequest, TopicName,
    },
    protocol::StrBytes,
};

use crate::{clients::network::NetworkClient, error::KafkaError, proto::ver::with_max_version};

use super::{
    ClusterDescription, CreateTopicsResult, NewTopic, TopicDescription, TopicListing,
    TopicMetadataAndConfig,
};

#[derive(Clone)]
pub struct AdminClient {
    client: NetworkClient,
}

impl AdminClient {
    pub fn new(client: NetworkClient) -> Self {
        Self { client }
    }

    pub async fn describe_cluster(&self) -> Result<ClusterDescription, KafkaError> {
        let response = self
            .client
            .send({
                let mut req = DescribeClusterRequest::default();

                req.include_cluster_authorized_operations = true;

                req
            })
            .await?;

        Ok(response.try_into()?)
    }

    pub async fn list_topics(&self) -> Result<Vec<TopicListing>, KafkaError> {
        let response = self
            .client
            .send({
                let mut req = MetadataRequest::default();
                req.topics = None;
                req
            })
            .await?;

        response
            .topics
            .into_iter()
            .map(|entry| TopicListing::try_from(entry).map_err(Into::into))
            .collect()
    }

    pub async fn describe_topics(
        &self,
        topics: Vec<String>,
    ) -> Result<Vec<TopicDescription>, KafkaError> {
        let response = self
            .client
            .send(with_max_version(|ver| {
                let mut req = MetadataRequest::default();

                if ver >= 4 {
                    req.allow_auto_topic_creation = false;
                }

                if ver >= 8 {
                    req.include_cluster_authorized_operations;
                }

                req.topics = Some(
                    topics
                        .into_iter()
                        .map(|name| TopicName(StrBytes::from_string(name)))
                        .map(|name| {
                            let mut topic = MetadataRequestTopic::default();
                            topic.name = Some(name);
                            topic
                        })
                        .collect(),
                );
                Some(req)
            }))
            .await?;

        response
            .topics
            .into_iter()
            .map(|(name, topic)| {
                TopicDescription::try_from((name, topic, &response.brokers)).map_err(Into::into)
            })
            .collect()
    }

    pub async fn create_topics(
        &self,
        topics: Vec<NewTopic>,
    ) -> Result<CreateTopicsResult, KafkaError> {
        let response = self
            .client
            .send(with_max_version(|_ver| {
                let mut req = CreateTopicsRequest::default();

                for topic in topics {
                    let mut new_topic = CreatableTopic::default();

                    match topic {
                        NewTopic::AutoAssignment(auto) => {
                            if let Some(partitions) = auto.partitions {
                                new_topic.num_partitions = partitions;
                            } else {
                                new_topic.num_partitions = -1;
                            }

                            if let Some(replicas) = auto.replication_factor {
                                new_topic.replication_factor = replicas;
                            } else {
                                new_topic.replication_factor = -1;
                            }

                            req.topics
                                .insert(TopicName(StrBytes::from_string(auto.name)), new_topic);
                        }
                        NewTopic::ExplicitAssignment(explicit) => {
                            new_topic.assignments = explicit
                                .replicas_assignments
                                .into_iter()
                                .map(|(partition, broker_ids)| {
                                    let broker_ids =
                                        broker_ids.into_iter().map(BrokerId::from).collect();
                                    let mut assignment = CreatableReplicaAssignment::default();
                                    assignment.broker_ids = broker_ids;

                                    (partition, assignment)
                                })
                                .collect();

                            req.topics
                                .insert(TopicName(StrBytes::from_string(explicit.name)), new_topic);
                        }
                    }
                }

                Some(req)
            }))
            .await?;

        Ok(response
            .topics
            .into_iter()
            .map(|(name, result)| {
                (
                    Arc::from(name.as_str()),
                    TopicMetadataAndConfig::try_from(result),
                )
            })
            .collect())
    }
}
