mod client;

use client::{KafkaClient, KafkaClientConfig};

use kafka_protocol::{
    messages::{
        fetch_request::{FetchPartition, FetchTopic},
        FetchRequest, TopicName,
    },
    protocol::{Builder, StrBytes},
};

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let client = KafkaClient::connect("127.0.0.1:9092", &KafkaClientConfig::default()).await?;

    // let mut record_buf = BytesMut::new();

    // RecordBatchEncoder::encode(
    //     &mut record_buf,
    //     [Record {
    //         transactional: false,
    //         control: false,
    //         partition_leader_epoch: -1,
    //         producer_id: 0,
    //         producer_epoch: 0,
    //         timestamp_type: TimestampType::Creation,
    //         offset: 0,
    //         sequence: 0,
    //         timestamp: 1723270529927,
    //         key: Some(Bytes::from_static(b"key")),
    //         value: Some(Bytes::from_static(b"Hello world")),
    //         headers: IndexMap::new(),
    //     }]
    //     .iter(),
    //     &RecordEncodeOptions {
    //         version: 2,
    //         compression: Compression::None,
    //     },
    // )?;

    // let res = client
    //     .send(
    //         ProduceRequest::builder()
    //             .acks(1)
    //             .topic_data(IndexMap::from_iter([(
    //                 TopicName(StrBytes::from_static_str("test-topic")),
    //                 TopicProduceData::builder()
    //                     .partition_data(vec![PartitionProduceData::builder()
    //                         .index(0)
    //                         .records(Some(record_buf.into()))
    //                         .build()?])
    //                     .build()?,
    //             )]))
    //             .build()?
    //             .into(),
    //         9,
    //     )
    //     .await?;

    // println!("{res:#?}");

    let res = client
        .send(
            FetchRequest::builder()
                .cluster_id(Some(StrBytes::from_static_str("CvtEUt71RdKpIueT-oot1Q")))
                .isolation_level(0)
                .min_bytes(1)
                .max_wait_ms(200)
                .max_bytes(1024)
                .topics(vec![FetchTopic::builder()
                    .topic(TopicName(StrBytes::from_static_str("test-topic")))
                    .partitions(vec![FetchPartition::builder()
                        .partition(0)
                        .current_leader_epoch(4)
                        .partition_max_bytes(1024)
                        .fetch_offset(2)
                        .last_fetched_epoch(-1)
                        .build()
                        .unwrap()])
                    .build()?])
                .build()?,
            12,
        )
        .await
        .unwrap();

    println!("{res:#?}");

    client.shutdown();

    Ok(())
}
