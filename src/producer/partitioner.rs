use std::hash::{DefaultHasher, Hash, Hasher};

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

        let mut hasher = DefaultHasher::new();
        record.key.hash(&mut hasher);
        let index = hasher.finish() as i32 % topic_data.len() as i32;

        record.partition = Some(index);
    }
}
