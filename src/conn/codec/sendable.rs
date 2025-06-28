use std::io;

use bytes::BytesMut;
use kafka_protocol::{
    messages::{ApiKey, ResponseHeader},
    protocol::{Decodable, Request},
};

use crate::proto::request::KafkaRequest;

#[inline]
fn into_invalid_data(error: anyhow::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// Request context for decoding into a response type
#[derive(Debug, Clone)]
pub struct RequestRecord {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub response_header_version: i16,
}

/// A frame that can be decoded, includes the request context.
#[derive(Debug, Clone)]
pub struct DecodableResponse {
    pub record: RequestRecord,
    pub frame: BytesMut,
}

/// Represents a request that expects a specific response type, and defines how to decode the response into the type.
pub trait Sendable: Into<KafkaRequest> {
    type Response;

    fn decode(response: DecodableResponse) -> Result<Self::Response, io::Error>;
}

impl<T: Request + Into<KafkaRequest>> Sendable for T {
    type Response = <T as Request>::Response;

    fn decode(
        DecodableResponse { record, mut frame }: DecodableResponse,
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

        let mut frame_slice = frame.as_ref();

        let result =
            Self::Response::decode(&mut frame_slice, record.api_version).map_err(into_invalid_data);

        if result.is_err() && record.api_key == ApiKey::ApiVersions {
            tracing::trace!(
                correlation_id = h.correlation_id,
                "failed to decode ApiVersionsResponse, falling back to version 0"
            );
            // Try to parse as version 0 if it's an api versions request since the server may not support the version in the request
            Self::Response::decode(&mut frame, 0).map_err(into_invalid_data)
        } else {
            result
        }
    }
}
