use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::{
    messages::{
        produce_request::{PartitionProduceData, TopicProduceData},
        ProduceRequest, TopicName,
    },
    protocol::StrBytes,
    records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType},
};

use crate::{
    conn::KafkaChannelError,
    error::{ErrorCode, KafkaError},
    proto::ver::with_max_version,
    util::TopicNameExt,
};

use super::network::NetworkClient;

pub struct ProducerRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub timestamp: Option<i64>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: indexmap::IndexMap<StrBytes, Option<Bytes>>,
}

pub struct Producer {
    client: NetworkClient,
}

impl Producer {
    pub fn new(client: NetworkClient) -> Self {
        Self { client }
    }

    pub async fn send(&mut self, record: ProducerRecord) -> Result<(), KafkaError> {
        let topic_name = TopicName::from_string(record.topic);

        self.client
            .load_topic_metadata([&topic_name].into_iter())
            .await?;

        let topic_map = &self.client.borrow_cluster().metadata.topics;

        let topic_data = topic_map
            .get(&topic_name)
            .ok_or(ErrorCode::UnknownTopicOrPartition)?;

        if topic_data.error_code != ErrorCode::None as i16 {
            return Err(KafkaError::ErrorCode(topic_data.error_code.into()));
        }

        if record.partition.is_some_and(|idx| idx < 0) {
            return Err(ErrorCode::UnknownTopicOrPartition.into());
        }

        let partition_index = record.partition.unwrap_or_default();

        let partition = topic_data
            .partitions
            .get(partition_index as usize)
            .ok_or(ErrorCode::UnknownTopicOrPartition)?;

        if partition.error_code != ErrorCode::None as i16 {
            return Err(KafkaError::ErrorCode(partition.error_code.into()));
        }

        let leader_epoch = partition.leader_epoch;
        let leader_id = partition.leader_id.0;

        let timestamp = record.timestamp.unwrap_or_else(|| {
            let start = SystemTime::now();
            let since_the_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            since_the_epoch.as_millis() as i64
        });

        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: leader_epoch,
            producer_epoch: -1,
            producer_id: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: 0,
            timestamp,
            key: record.key,
            value: record.value,
            headers: record.headers,
        };

        let mut records = BytesMut::new();

        RecordBatchEncoder::encode(
            &mut records,
            [record].iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .map_err(|e| KafkaError::Channel(KafkaChannelError::Io(io::Error::other(e))))?;

        let _response = self
            .client
            .send_to(
                with_max_version(|_ver| {
                    let mut req = ProduceRequest::default();

                    req.acks = 1;
                    req.timeout_ms = 1000;
                    req.transactional_id = None;

                    let mut data = TopicProduceData::default();

                    data.partition_data = vec![{
                        let mut partition_data = PartitionProduceData::default();

                        partition_data.index = partition.partition_index;
                        partition_data.records = Some(records.into());

                        partition_data
                    }];

                    req.topic_data = IndexMap::from_iter([(topic_name.clone(), data)]);

                    Some(req)
                }),
                leader_id,
            )
            .await?;

        Ok(())
    }
}
