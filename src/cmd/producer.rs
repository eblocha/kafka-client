use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
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

    while let Some(line) = reader.next_line().await? {
        let mut records = BytesMut::new();

        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let timestamp =
            since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000;

        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: 0,
            producer_epoch: -1,
            timestamp_type: records::TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: timestamp as i64,
            key: None,
            value: Some(Bytes::from(line)),
            headers: Default::default(),
        };

        RecordBatchEncoder::encode(
            &mut records,
            [&record].into_iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: records::Compression::None,
            },
        )?;

        let mut data = TopicProduceData::default();

        data.partition_data = vec![{
            let mut d = PartitionProduceData::default();
            d.index = 0;
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

        let res = client.send(req).await?;

        for (_, response) in res.responses {
            for response in response.partition_responses {
                let error_code = ErrorCode::from(response.error_code);

                if error_code != ErrorCode::None {
                    println!("error: {error_code:?}");
                    return Ok(());
                }
            }
        }

        println!("Produced message!");
    }

    Ok(())
}
