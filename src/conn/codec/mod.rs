use std::io;

use correlated::{CorrelatedDecoder, CorrelatedFrame};
use request::RequestEncoder;
use tokio_util::codec;

pub(crate) const LENGTH_FIELD_LENGTH: usize = 4;

mod correlated;
mod request;
pub mod sendable;

pub use correlated::CorrelationId;
pub use request::{EncodableRequest, VersionedRequest};

pub struct KafkaCodec {
    encoder: RequestEncoder,
    decoder: CorrelatedDecoder,
}

impl KafkaCodec {
    pub fn new(max_frame_length: usize) -> Self {
        Self {
            encoder: RequestEncoder::new(max_frame_length),
            decoder: CorrelatedDecoder::new(max_frame_length),
        }
    }
}

impl codec::Encoder<EncodableRequest> for KafkaCodec {
    type Error = io::Error;

    #[inline]
    fn encode(
        &mut self,
        item: EncodableRequest,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        self.encoder.encode(item, dst)
    }
}

impl codec::Decoder for KafkaCodec {
    type Item = CorrelatedFrame;

    type Error = io::Error;

    #[inline]
    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decoder.decode(src)
    }
}
