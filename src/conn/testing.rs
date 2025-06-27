use bytes::BytesMut;
use kafka_protocol::{
    messages::{ApiKey, ResponseHeader},
    protocol::{Encodable, HeaderVersion},
};

use crate::conn::{codec::sendable::RequestRecord, DecodableResponse};

pub fn encode_response<R: Encodable + HeaderVersion>(
    response: R,
    api_key: ApiKey,
    api_version: i16,
) -> DecodableResponse {
    let header = ResponseHeader::default();
    let header_version = R::header_version(api_version);

    let mut frame = BytesMut::new();

    header.encode(&mut frame, header_version).unwrap();
    response.encode(&mut frame, api_version).unwrap();

    DecodableResponse {
        record: RequestRecord {
            api_key,
            api_version,
            response_header_version: header_version,
        },
        frame,
    }
}
