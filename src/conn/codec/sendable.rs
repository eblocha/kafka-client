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

pub trait Sendable: Into<KafkaRequest> + Request {
    fn decode_frame(frame: BytesMut, record: RequestRecord) -> Result<Self::Response, io::Error>;
}

impl<T: Request + Into<KafkaRequest>> Sendable for T {
    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
