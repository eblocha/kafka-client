use std::{io, sync::Arc};

use bytes::{Buf, BytesMut};
use dashmap::DashMap;
use derive_more::derive::From;
use kafka_protocol::{
    messages::{ApiKey, RequestHeader, ResponseHeader},
    protocol::{buf::ByteBuf, Decodable, Encodable, StrBytes},
};
use tokio_util::codec;

use crate::{request::KafkaRequest, response::KafkaResponse};

#[derive(Debug, Clone)]
struct RequestRecord {
    api_key: ApiKey,
    api_version: i16,
    response_header_version: i16,
}

#[derive(Debug, Default, Clone)]
pub struct RequestRecords(Arc<DashMap<i32, RequestRecord>>);

#[derive(Debug, Clone)]
pub struct KafkaCodec {
    raw_codec: codec::LengthDelimitedCodec,
    request_records: RequestRecords,
}

impl KafkaCodec {
    #[inline]
    pub fn new(state: RequestRecords) -> Self {
        KafkaCodec {
            raw_codec: codec::LengthDelimitedCodec::builder()
                .length_field_length(4)
                .new_codec(),
            request_records: state,
        }
    }

    #[inline]
    pub fn discard(&self, correlation_id: i32) {
        self.request_records.0.remove(&correlation_id);
    }
}

#[inline]
fn into_invalid_input(error: anyhow::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}

#[inline]
fn into_invalid_data(error: anyhow::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[allow(unused)]
#[derive(Debug, From)]
pub enum DecodeError {
    Io(#[from] io::Error),
    MissingCorrelationId(i32),
}

impl codec::Decoder for KafkaCodec {
    type Item = (ResponseHeader, KafkaResponse);

    type Error = DecodeError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // TODO this errs when number of bytes is more than max frame length (default 8MB).
        // Make this configurable and provide more helpful error.
        let Some(mut bytes) = self.raw_codec.decode(src)? else {
            return Ok(None);
        };

        let correlation_id: i32 = bytes.peek_bytes(0..4).get_i32();

        let Some((_, record)) = self.request_records.0.remove(&correlation_id) else {
            return Err(DecodeError::MissingCorrelationId(correlation_id));
        };

        let header = ResponseHeader::decode(&mut bytes, record.response_header_version)
            .map_err(into_invalid_data)?;

        let response = KafkaResponse::decode(&mut bytes, record.api_version, record.api_key)
            .map_err(into_invalid_data)?;

        Ok(Some((header, response)))
    }
}

#[derive(Debug, Clone)]
pub struct VersionedRequest {
    pub request: KafkaRequest,
    pub correlation_id: i32,
    pub api_version: i16,
}

#[derive(Debug, Clone)]
struct EncodableRequest {
    request: KafkaRequest,
    header: RequestHeader,
    api_key: ApiKey,
}

impl From<VersionedRequest> for EncodableRequest {
    fn from(value: VersionedRequest) -> Self {
        let api_key = value.request.as_api_key();

        Self {
            request: value.request,
            header: {
                let mut h = RequestHeader::default();
                h.client_id = Some(StrBytes::from_static_str("eblocha"));
                h.correlation_id = value.correlation_id;
                h.request_api_key = api_key as i16;
                h.request_api_version = value.api_version;
                h
            },
            api_key,
        }
    }
}

impl codec::Encoder<VersionedRequest> for KafkaCodec {
    type Error = io::Error;

    fn encode(
        &mut self,
        item: VersionedRequest,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let req: EncodableRequest = item.into();

        let mut bytes = BytesMut::new();

        req.header
            .encode(
                &mut bytes,
                req.api_key
                    .request_header_version(req.header.request_api_version),
            )
            .map_err(into_invalid_input)?;

        req.request
            .encode(&mut bytes, req.header.request_api_version)
            .map_err(into_invalid_input)?;

        self.raw_codec.encode(bytes.get_bytes(bytes.len()), dst)?;

        // save request record
        self.request_records.0.insert(
            req.header.correlation_id,
            RequestRecord {
                api_key: req.api_key,
                api_version: req.header.request_api_version,
                response_header_version: req
                    .api_key
                    .response_header_version(req.header.request_api_version),
            },
        );

        Ok(())
    }
}
