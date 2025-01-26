use kafka_protocol::messages::{
    create_topics_request::{CreatableReplicaAssignment, CreatableTopic},
    delete_topics_request::DeleteTopicState,
    metadata_request::MetadataRequestTopic,
    BrokerId, CreateTopicsRequest, DeleteTopicsRequest, DescribeClusterRequest, MetadataRequest,
    TopicName,
};

use crate::{
    clients::{admin::delete_topic_result::DeletedTopic, network::NetworkClient},
    common::TopicCollection,
    error::KafkaError,
    proto::ver::with_max_version,
    util::TopicNameExt,
};

use super::{
    delete_topic_result::DeleteTopicsResult, ClusterDescription, CreateTopicsResult,
    DescribeTopicsResult, NewTopic, TopicDescription, TopicListing, TopicMetadataAndConfig,
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
            .send(
                DescribeClusterRequest::default().with_include_cluster_authorized_operations(true),
            )
            .await?;

        Ok(response.try_into()?)
    }

    pub async fn list_topics(&self) -> Result<Vec<TopicListing>, KafkaError> {
        let response = self
            .client
            .send(MetadataRequest::default().with_topics(None))
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
    ) -> Result<DescribeTopicsResult, KafkaError> {
        let response = self
            .client
            .send(with_max_version(|ver| {
                let mut req = MetadataRequest::default();

                if ver >= 4 {
                    req.allow_auto_topic_creation = false;
                }

                if ver >= 8 && ver <= 10 {
                    req.include_cluster_authorized_operations = true;
                }

                req.topics = Some(
                    topics
                        .into_iter()
                        .map(TopicName::from_string)
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

        let broker_map = response
            .brokers
            .into_iter()
            .map(|broker| (broker.node_id.0, broker))
            .collect();

        Ok(response
            .topics
            .into_iter()
            .map(|topic| TopicDescription::try_from((topic, &broker_map)))
            .collect())
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
                            new_topic.name = TopicName::from_string(auto.name);
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

                            req.topics.push(new_topic);
                        }
                        NewTopic::ExplicitAssignment(explicit) => {
                            new_topic.name = TopicName::from_string(explicit.name);
                            new_topic.assignments = explicit
                                .replicas_assignments
                                .into_iter()
                                .map(|(partition, broker_ids)| {
                                    let broker_ids =
                                        broker_ids.into_iter().map(BrokerId::from).collect();
                                    let mut assignment = CreatableReplicaAssignment::default();
                                    assignment.partition_index = partition;
                                    assignment.broker_ids = broker_ids;

                                    assignment
                                })
                                .collect();

                            req.topics.push(new_topic);
                        }
                    }
                }

                Some(req)
            }))
            .await?;

        Ok(response
            .topics
            .into_iter()
            .map(TopicMetadataAndConfig::try_from)
            .collect())
    }

    pub async fn delete_topics(
        &self,
        topics: TopicCollection,
    ) -> Result<DeleteTopicsResult, KafkaError> {
        let response = self
            .client
            .send(with_max_version(move |ver| {
                let mut req = DeleteTopicsRequest::default();
                req.timeout_ms = 5000; // TODO config

                if matches!(topics, TopicCollection::Ids(_)) && ver < 6 {
                    return None;
                }

                match topics {
                    TopicCollection::Ids(ids) => {
                        for id in ids.into_iter() {
                            req.topics.push({
                                let mut topic = DeleteTopicState::default();
                                topic.topic_id = id;
                                topic
                            });
                        }
                    }
                    TopicCollection::Names(names) => {
                        if ver < 6 {
                            for name in names.into_iter() {
                                req.topic_names.push(TopicName::from_string(name));
                            }
                        } else {
                            for name in names.into_iter() {
                                req.topics.push({
                                    let mut topic = DeleteTopicState::default();
                                    topic.name = Some(TopicName::from_string(name));
                                    topic
                                });
                            }
                        }
                    }
                }

                Some(req)
            }))
            .await?;

        Ok(response
            .responses
            .into_iter()
            .map(DeletedTopic::try_from)
            .collect())
    }

    pub async fn shutdown(&self) {
        self.client.shutdown().await
    }

    pub async fn await_shutdown(&self) {
        self.client.await_shutdown().await
    }
}
