use crate::{conn::selector::TopicMetadata, producer::record::ProducerRecord};

pub trait Partitioner {
    fn partition(&self, record: &mut ProducerRecord, topic_data: &TopicMetadata);
}

#[derive(Debug, Clone, Copy)]
pub struct KeyHashPartitioner;

impl Partitioner for KeyHashPartitioner {
    fn partition(&self, record: &mut ProducerRecord, topic_data: &TopicMetadata) {
        if topic_data.is_empty() {
            return;
        }

        let Some(ref key) = record.key else {
            record.partition = Some(0);
            return;
        };

        let index = crc32fast::hash(key.as_ref()) as usize % topic_data.len();

        record.partition = Some(index as i32);
    }
}
