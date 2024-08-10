use std::io;

use bytes::BytesMut;
use kafka_protocol::{
    messages::{ApiKey, RequestHeader},
    protocol::{Encodable, StrBytes},
};
use tokio_util::codec;

use crate::client::request::KafkaRequest;

use super::{correlated::CorrelationId, LENGTH_FIELD_LENGTH};

#[derive(Debug, Clone)]
pub struct VersionedRequest {
    pub request: KafkaRequest,
    pub correlation_id: CorrelationId,
    pub api_version: i16,
}

#[derive(Debug, Clone)]
pub(crate) struct EncodableRequest {
    request: KafkaRequest,
    header: RequestHeader,
    api_key: ApiKey,
}

impl EncodableRequest {
    pub fn api_version(&self) -> i16 {
        self.header.request_api_version
    }

    pub fn api_key(&self) -> ApiKey {
        self.api_key
    }
}

impl From<VersionedRequest> for EncodableRequest {
    fn from(value: VersionedRequest) -> Self {
        let api_key = value.request.as_api_key();

        Self {
            api_key,
            request: value.request,
            header: {
                let mut h = RequestHeader::default();
                h.client_id = Some(StrBytes::from_static_str("eblocha"));
                h.correlation_id = value.correlation_id.0;
                h.request_api_key = api_key as i16;
                h.request_api_version = value.api_version;
                h
            },
        }
    }
}

pub struct RequestEncoder {
    raw_codec: codec::LengthDelimitedCodec,
}

impl RequestEncoder {
    pub fn new() -> Self {
        Self {
            raw_codec: codec::LengthDelimitedCodec::builder()
                .length_field_length(LENGTH_FIELD_LENGTH)
                .new_codec(),
        }
    }
}

#[inline]
fn into_invalid_input(error: anyhow::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}

impl codec::Encoder<EncodableRequest> for RequestEncoder {
    type Error = io::Error;

    fn encode(&mut self, item: EncodableRequest, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut bytes = BytesMut::new();

        item.header
            .encode(
                &mut bytes,
                item.api_key
                    .request_header_version(item.header.request_api_version),
            )
            .map_err(into_invalid_input)?;

        item.request
            .encode(&mut bytes, item.header.request_api_version)
            .map_err(into_invalid_input)?;

        self.raw_codec.encode(bytes.into(), dst)?;

        Ok(())
    }
}
