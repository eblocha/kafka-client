use std::{io, sync::Arc};

use bytes::{BufMut, BytesMut};
use kafka_protocol::{
    messages::{ApiKey, RequestHeader},
    protocol::{Encodable, StrBytes},
};
use tokio_util::codec;

use crate::{conn::codec::LENGTH_FIELD_LENGTH, proto::request::KafkaRequest};

use super::correlated::CorrelationId;

/// A request with version information
#[derive(Debug, Clone)]
pub struct VersionedRequest {
    pub request: KafkaRequest,
    pub api_version: i16,
}

/// A request that is ready to be encoded into a frame
#[derive(Debug, Clone)]
pub struct EncodableRequest {
    request: KafkaRequest,
    header: RequestHeader,
    api_key: ApiKey,
}

impl EncodableRequest {
    /// Create an encodable request from a versioned request and correlation id
    pub fn from_versioned(
        value: VersionedRequest,
        correlation_id: CorrelationId,
        client_id: Option<Arc<str>>,
    ) -> Self {
        let api_key = value.request.as_api_key();

        Self {
            api_key,
            request: value.request,
            header: {
                let mut h = RequestHeader::default();
                h.client_id = client_id
                    // FIXME there's no way around this copy until kafka-protocol supports Arc<str> (or better yet AsRef<str>)
                    .map(|id| StrBytes::from_string(id.as_ref().to_owned()));
                h.correlation_id = correlation_id.0;
                h.request_api_key = api_key as i16;
                h.request_api_version = value.api_version;
                h
            },
        }
    }

    pub fn api_version(&self) -> i16 {
        self.header.request_api_version
    }

    pub fn api_key(&self) -> ApiKey {
        self.api_key
    }

    pub fn correlation_id(&self) -> CorrelationId {
        CorrelationId(self.header.correlation_id)
    }
}

pub struct RequestEncoder {
    max_frame_length: usize,
}

impl RequestEncoder {
    pub fn new(max_frame_length: usize) -> Self {
        Self { max_frame_length }
    }
}

#[inline]
fn into_invalid_input(error: anyhow::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}

impl codec::Encoder<EncodableRequest> for RequestEncoder {
    type Error = io::Error;

    fn encode(&mut self, item: EncodableRequest, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let request_header_version = item
            .api_key
            .request_header_version(item.header.request_api_version);

        tracing::trace!(
            version = item.header.request_api_version,
            header_version = request_header_version,
            api_key = ?item.api_key,
            correlation_id = item.header.correlation_id,
            item = ?item,
            "encoding request"
        );

        let starting_pos = dst.len();

        // write space for the length header
        dst.put_bytes(0, LENGTH_FIELD_LENGTH);

        item.header
            .encode(dst, request_header_version)
            .map_err(into_invalid_input)?;

        item.request
            .encode(dst, item.header.request_api_version)
            .map_err(into_invalid_input)?;

        let len = dst.len() - starting_pos - LENGTH_FIELD_LENGTH;

        if len > self.max_frame_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame size too big",
            ));
        }

        dst[starting_pos..starting_pos + LENGTH_FIELD_LENGTH]
            .copy_from_slice(&(len as i32).to_be_bytes());

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use codec::Encoder;
    use kafka_protocol::messages::MetadataRequest;

    use crate::proto::request::KafkaRequest;

    use super::*;

    #[test]
    fn metadata_request() {
        let request = {
            let mut r = MetadataRequest::default();
            r.allow_auto_topic_creation = true;
            r.topics = None;
            r
        };

        let versioned = VersionedRequest {
            api_version: 12,
            request: KafkaRequest::Metadata(request),
        };

        let mut bytes = BytesMut::new();

        let expected = [
            //--length---|-key-|-ver--|----id-----|----message-------------|
            0u8, 0, 0, 15, 0, 3, 0, 12, 0, 0, 0, 1, 255, 255, 0, 0, 1, 0, 0,
        ];

        RequestEncoder::new(8 * 1024 * 1024)
            .encode(
                EncodableRequest::from_versioned(versioned, 1.into(), None),
                &mut bytes,
            )
            .unwrap();

        assert_eq!(bytes.into_iter().collect::<Vec<u8>>(), expected);
    }
}
