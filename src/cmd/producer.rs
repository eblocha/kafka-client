use std::{
    path::PathBuf,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use kafka_protocol::{
    indexmap::IndexMap,
    messages::{
        produce_request::{PartitionProduceData, TopicProduceData},
        ProduceRequest, TopicName,
    },
    protocol::StrBytes,
    records::{self, Record, RecordBatchEncoder, RecordEncodeOptions},
};
use tokio::{
    fs::File,
    io::{self, AsyncBufReadExt},
};

use crate::{clients::network::NetworkClient, proto::error_codes::ErrorCode};

pub async fn produce_from_file(
    client: &NetworkClient,
    topic: String,
    file: PathBuf,
) -> anyhow::Result<()> {
    let file = File::open(file).await?;

    let mut reader = io::BufReader::new(file).lines();

    let topic = TopicName(StrBytes::from_string(topic));

    let cluster = client.read_cluster_snapshot();

    let Some(topic_data) = cluster.metadata.topics.get(&topic) else {
        return Ok(());
    };

    let partition = &topic_data.partitions[0];

    let leader_epoch = partition.leader_epoch;
    let leader_id = partition.leader_id.0;
    let mut iter: i32 = 0;

    let mut record_vec = Vec::new();
    let mut records = BytesMut::new();

    while let Some(line) = reader.next_line().await? {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let timestamp =
            since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000;

        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: leader_epoch,
            producer_id: 0,
            producer_epoch: -1,
            timestamp_type: records::TimestampType::Creation,
            offset: iter as i64,
            sequence: iter,
            timestamp: timestamp as i64,
            key: Some(Bytes::new()),
            value: Some(Bytes::from(line)),
            headers: Default::default(),
        };

        record_vec.push(record);

        iter += 1;
    }

    let now = Instant::now();

    RecordBatchEncoder::encode(
        &mut records,
        record_vec.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: records::Compression::None,
        },
    )?;

    let mut data = TopicProduceData::default();

    data.partition_data = vec![{
        let mut d = PartitionProduceData::default();
        d.index = partition.partition_index;
        d.records = Some(records.into());
        d
    }];

    let req = {
        let mut r = ProduceRequest::default();
        r.acks = 1;
        r.timeout_ms = 1000;
        r.transactional_id = None;
        r.topic_data = IndexMap::from_iter([(topic.clone(), data)]);
        r
    };

    let res = client.send_to(req, leader_id).await?;

    for (_, response) in res.responses {
        for response in response.partition_responses {
            let error_code = ErrorCode::from(response.error_code);

            if error_code != ErrorCode::None {
                println!("error: {error_code:?}");
                return Ok(());
            }
        }
    }

    let finish = now.elapsed();

    println!("produced {iter} messages in {finish:?}");

    Ok(())
}
