use kafka_protocol::protocol::{Request, VersionRange};

/// Represents a message that can determine the api versions it supports.
pub trait Versionable {
    /// The numeric representation of the API key, which identifies the message type.
    /// See https://kafka.apache.org/protocol#protocol_api_keys
    fn key(&self) -> i16;
    /// The range of API versions that this client supports for this message type.
    fn versions(&self) -> VersionRange;
}

impl<T: Request> Versionable for T {
    #[inline]
    fn key(&self) -> i16 {
        T::KEY
    }

    #[inline]
    fn versions(&self) -> VersionRange {
        T::VERSIONS
    }
}
