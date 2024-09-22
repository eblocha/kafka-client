use kafka_protocol::protocol::{Request, VersionRange};

pub trait GetApiKey {
    /// The numeric representation of the API key, which identifies the message type.
    /// See https://kafka.apache.org/protocol#protocol_api_keys
    fn key(&self) -> i16;
}

impl<T: Request> GetApiKey for T {
    #[inline]
    fn key(&self) -> i16 {
        T::KEY
    }
}

/// Represents a message that can determine the api versions it supports.
pub trait Versionable {
    /// The range of API versions that this client supports for this message type.
    fn versions(&self) -> VersionRange;
}

impl<T: Request> Versionable for T {
    #[inline]
    fn versions(&self) -> VersionRange {
        T::VERSIONS
    }
}

pub trait FromVersionRange {
    type Req;

    /// Create a request and api version from the version range supported by a target broker.
    ///
    /// Returns None if no message can be constructed
    fn from_version_range(self, range: VersionRange) -> Option<(Self::Req, i16)>;
}

impl<T: Versionable> FromVersionRange for T {
    type Req = Self;

    fn from_version_range(self, range: VersionRange) -> Option<(Self::Req, i16)> {
        let intersection = self.versions().intersect(&range);

        if intersection.is_empty() {
            None
        } else {
            Some((self, intersection.max))
        }
    }
}
