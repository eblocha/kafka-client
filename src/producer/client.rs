use tokio::sync::oneshot;

use crate::{
    common::{BrokerHost, TopicPartition},
    conn::{
        config::ConnectionManagerConfig,
        selector::{
            connect::{Connect, Tcp},
            SelectorTaskHandle,
        },
    },
    error::{ErrorCode, KafkaError},
    producer::{
        handle::{ProducerTaskFactory, ProducerTaskHandle},
        partitioner::{KeyHashPartitioner, Partitioner},
        record::ProducerRecord,
        task::{ProducerSendMessage, ProducerSendRecord, ProducerTask},
    },
};

pub struct Producer<Conn: Connect + Send + 'static, P> {
    selector: SelectorTaskHandle<ProducerTask<Conn>, ProducerTaskHandle>,
    partitioner: P,
}

impl Producer<Tcp, KeyHashPartitioner> {
    pub async fn try_new(
        bootstrap: &[BrokerHost],
        config: ConnectionManagerConfig,
    ) -> Result<Self, KafkaError> {
        let selector =
            SelectorTaskHandle::try_new_tcp(bootstrap, config, ProducerTaskFactory {}).await?;

        Ok(Self {
            selector,
            partitioner: KeyHashPartitioner,
        })
    }
}

impl<Conn: Connect + Send + 'static, P: Partitioner> Producer<Conn, P> {
    pub async fn produce(&self, mut record: ProducerRecord) -> Result<(), KafkaError> {
        let cluster = self.selector.cluster.load();

        // TODO: if err, refresh the metadata for the topic
        let topic_data = cluster.metadata.get_topic_metadata_by_name(&record.topic)?;

        if record.partition.is_none() {
            self.partitioner.partition(&mut record, topic_data);
        }

        let Some(partition) = record.partition else {
            return Err(ErrorCode::UnknownTopicOrPartition.into());
        };

        topic_data.get_partition_metadata(partition)?;

        let tp = TopicPartition::new(record.topic.clone(), partition);

        let Some(tx_partition) = cluster.partitions.get(&tp) else {
            return Err(ErrorCode::UnknownTopicOrPartition.into());
        };

        let (tx, rx) = oneshot::channel();

        tx_partition
            .send(ProducerSendMessage {
                record: ProducerSendRecord {
                    topic_partition: tp,
                    value: record.value,
                    key: record.key,
                    headers: record.headers,
                    timestamp: record.timestamp,
                },
                tx,
            })
            .await?;

        rx.await?
    }
}
