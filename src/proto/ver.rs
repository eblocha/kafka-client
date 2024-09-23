use kafka_protocol::protocol::{Request, VersionRange};

pub trait GetApiKey {
    /// The numeric representation of the API key, which identifies the message type.
    /// See https://kafka.apache.org/protocol#protocol_api_keys
    fn key(&self) -> i16;
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

/// Determine the maximum version in the intersection of two version ranges.
pub fn max_intersecting_version(client: &VersionRange, broker: &VersionRange) -> Option<i16> {
    let intersection = client.intersect(broker);

    if intersection.is_empty() {
        None
    } else {
        Some(intersection.max)
    }
}

impl<R, F: FnOnce(VersionRange) -> Option<(R, i16)>> FromVersionRange for F {
    type Req = R;

    fn from_version_range(self, range: VersionRange) -> Option<(Self::Req, i16)> {
        self(range)
    }
}

impl<R: Request, F: FnOnce(VersionRange) -> Option<(R, i16)>> GetApiKey for F {
    fn key(&self) -> i16 {
        R::KEY
    }
}

/// A strategy for constructing a request that uses the maximum version that intersects with the broker.
struct MaxVersionStrategy<R, F: FnOnce(i16) -> Option<R>> {
    func: F,
}

impl<R: Request, F: FnOnce(i16) -> Option<R>> GetApiKey for MaxVersionStrategy<R, F> {
    fn key(&self) -> i16 {
        R::KEY
    }
}

impl<R: Request, F: FnOnce(i16) -> Option<R>> FromVersionRange for MaxVersionStrategy<R, F> {
    type Req = R;

    fn from_version_range(self, range: VersionRange) -> Option<(Self::Req, i16)> {
        let ver = max_intersecting_version(&R::VERSIONS, &range)?;
        let req = (self.func)(ver)?;

        Some((req, ver))
    }
}

/// Create a struct that implements [`FromVersionRange`] using a function to create the request given the maximum
/// api version that intersects the client and broker ranges.
pub fn with_max_version<R: Request, F: FnOnce(i16) -> Option<R>>(
    func: F,
) -> impl FromVersionRange<Req = R> + GetApiKey {
    MaxVersionStrategy { func }
}

/// A strategy for selecting constructing a request that uses the intersection of client and broker versions
struct IntersectionStrategy<R, F: FnOnce(VersionRange) -> Option<(R, i16)>> {
    func: F,
}

impl<R: Request, F: FnOnce(VersionRange) -> Option<(R, i16)>> GetApiKey
    for IntersectionStrategy<R, F>
{
    fn key(&self) -> i16 {
        R::KEY
    }
}

impl<R: Request, F: FnOnce(VersionRange) -> Option<(R, i16)>> FromVersionRange
    for IntersectionStrategy<R, F>
{
    type Req = R;

    fn from_version_range(self, range: VersionRange) -> Option<(Self::Req, i16)> {
        let intersection = R::VERSIONS.intersect(&range);

        if intersection.is_empty() {
            None
        } else {
            (self.func)(intersection)
        }
    }
}

/// Create a struct that implements [`FromVersionRange`] using a function to create the request given the intersection
/// range of the client and broker versions.
pub fn with_intersection<R: Request, F: FnOnce(VersionRange) -> Option<(R, i16)>>(
    func: F,
) -> impl FromVersionRange<Req = R> + GetApiKey {
    IntersectionStrategy { func }
}
