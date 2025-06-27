//! These are here because `kafka_protocol` adds a generic to encode/decode that forces you to name the types.
//!
//! It doesn't _need_ to require this, but it does.

use bytes::BytesMut;
use kafka_protocol::{
    protocol::buf::ByteBufMut,
    records::{Compression, Record, RecordEncodeOptions},
};

// #[derive(Debug, Clone)]
// /// Batch decoder for Kafka records.
// pub struct RecordBatchDecoder;

// impl RecordBatchDecoder {
//     /// Decode the provided buffer into a vec of records.
//     pub fn decode<B: ByteBuf>(buf: &mut B) -> anyhow::Result<Vec<Record>> {
//         kafka_protocol::records::RecordBatchDecoder::decode_with_custom_compression(
//             buf,
//             None::<fn(&mut bytes::Bytes, Compression) -> anyhow::Result<B>>,
//         )
//     }
// }

#[derive(Debug, Clone)]
/// Batch encoder for Kafka records.
pub struct RecordBatchEncoder;

impl RecordBatchEncoder {
    /// Encode records into given buffer, using provided encoding options that select the encoding
    /// strategy based on version.
    pub fn encode<'a, B, I>(
        buf: &mut B,
        records: I,
        options: &RecordEncodeOptions,
    ) -> anyhow::Result<()>
    where
        B: ByteBufMut,
        I: IntoIterator<Item = &'a Record>,
        I::IntoIter: Clone,
    {
        kafka_protocol::records::RecordBatchEncoder::encode_with_custom_compression(
            buf,
            records,
            options,
            None::<fn(&mut BytesMut, &mut B, Compression) -> anyhow::Result<()>>,
        )
    }
}
