use std::io;

use bytes::{Buf, BytesMut};
use derive_more::derive::From;
use kafka_protocol::protocol::buf::ByteBuf;
use tokio_util::codec;

use super::LENGTH_FIELD_LENGTH;

#[derive(Debug, Clone, Copy, From, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CorrelationId(#[from] pub(crate) i32);

#[derive(Debug, Clone, PartialEq, Eq)]
/// A frame with a correlation id to map it to a request
pub struct CorrelatedFrame {
    /// The correlation id
    pub id: CorrelationId,
    /// The frame data, including the correlation id as the first 4 bytes    
    pub frame: BytesMut,
}

pub struct CorrelatedDecoder {
    raw_codec: codec::LengthDelimitedCodec,
}

impl CorrelatedDecoder {
    pub fn new(max_frame_length: usize) -> Self {
        Self {
            raw_codec: codec::LengthDelimitedCodec::builder()
                .length_field_length(LENGTH_FIELD_LENGTH)
                .max_frame_length(max_frame_length)
                .new_codec(),
        }
    }
}

impl codec::Decoder for CorrelatedDecoder {
    type Item = CorrelatedFrame;

    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Some(mut bytes) = self.raw_codec.decode(src)? else {
            return Ok(None);
        };

        let id: i32 = bytes
            .try_peek_bytes(0..4)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .get_i32();

        Ok(Some(CorrelatedFrame {
            id: CorrelationId(id),
            frame: bytes,
        }))
    }
}

#[cfg(test)]
mod test {
    use codec::Decoder;
    use io::ErrorKind;

    use super::*;

    #[test]
    fn includes_id() {
        let mut data: BytesMut = BytesMut::from_iter(b"\x00\x00\x00\x04\x00\x00\x00\x00");

        let mut codec = CorrelatedDecoder::new(8 * 1024 * 1024);

        let res = codec.decode(&mut data).unwrap();

        assert_eq!(
            res,
            Some(CorrelatedFrame {
                id: CorrelationId(0),
                frame: BytesMut::from_iter(b"\x00\x00\x00\x00")
            })
        );
    }

    #[test]
    fn invalid_id() {
        let mut data: BytesMut = BytesMut::from_iter(b"\x00\x00\x00\x03\x00\x00\x00");

        let mut codec = CorrelatedDecoder::new(8 * 1024 * 1024);

        let res = codec.decode(&mut data);

        let Err(e) = res else {
            panic!("result is not an error: {res:#?}");
        };

        assert_eq!(e.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn not_enough_data() {
        let mut data: BytesMut = BytesMut::from_iter(b"\x00\x00\x00\x04\x00\x00\x00");

        let mut codec = CorrelatedDecoder::new(8 * 1024 * 1024);

        let res = codec.decode(&mut data).unwrap();

        assert_eq!(res, None);
    }
}
