use std::io;

use bytes::BytesMut;
use kafka_protocol::{
    messages::ResponseHeader,
    protocol::{Decodable, Request},
};

use crate::conn::request::KafkaRequest;

#[inline]
fn into_invalid_data(error: anyhow::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[derive(Debug, Clone)]
pub struct RequestRecord {
    pub api_version: i16,
    pub response_header_version: i16,
}

pub trait Sendable: Into<KafkaRequest> {
    type Response;

    fn decode_frame(frame: BytesMut, record: RequestRecord) -> Result<Self::Response, io::Error>;
}

impl<T: Request + Into<KafkaRequest>> Sendable for T {
    type Response = <T as Request>::Response;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        tracing::trace!(
            version = record.api_version,
            header_version = record.response_header_version,
            "decoding response",
        );

        let h = ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        tracing::trace!(
            version = record.api_version,
            header_version = record.response_header_version,
            correlation_id = h.correlation_id,
            "recognized response header"
        );

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}

impl Sendable for KafkaRequest {
    type Response = (BytesMut, RequestRecord);

    fn decode_frame(frame: BytesMut, record: RequestRecord) -> Result<Self::Response, io::Error> {
        Ok((frame, record))
    }
}
