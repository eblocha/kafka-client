use std::io;

use bytes::BytesMut;
use kafka_protocol::{
    messages::{ApiKey, ResponseHeader},
    protocol::{Decodable, Request},
};

use crate::conn::{request::KafkaRequest, response::KafkaResponse};

#[inline]
fn into_invalid_data(error: anyhow::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[derive(Debug, Clone)]
pub struct RequestRecord {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub response_header_version: i16,
}

pub trait Sendable: Into<KafkaRequest> {
    type Response;

    fn decode_frame(frame: BytesMut, record: RequestRecord) -> Result<Self::Response, io::Error>;
}

impl Sendable for KafkaRequest {
    type Response = KafkaResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        KafkaResponse::decode(&mut frame, record.api_version, record.api_key)
            .map_err(into_invalid_data)
    }
}

impl<T: Request + Into<KafkaRequest>> Sendable for T {
    type Response = <T as Request>::Response;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
