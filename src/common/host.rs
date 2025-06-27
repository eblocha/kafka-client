use std::{fmt::Debug, str::FromStr, sync::Arc};

use kafka_protocol::messages::{
    describe_cluster_response::DescribeClusterBroker, metadata_response::MetadataResponseBroker,
};
use url::Url;

use crate::util::StrBytesExt;

/// A cheap-to-clone host:port pair for a Kafka broker
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BrokerHost(pub Arc<str>, pub u16);

impl Debug for BrokerHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}

impl From<&MetadataResponseBroker> for BrokerHost {
    fn from(broker: &MetadataResponseBroker) -> Self {
        BrokerHost(broker.host.as_arc_str(), broker.port as u16)
    }
}

impl From<&DescribeClusterBroker> for BrokerHost {
    fn from(broker: &DescribeClusterBroker) -> Self {
        BrokerHost(broker.host.as_arc_str(), broker.port as u16)
    }
}

impl FromStr for BrokerHost {
    type Err = url::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut url = Url::parse(s)?;

        if !url.has_host() {
            url = Url::parse(&format!("kafka://{s}"))?;
        }

        Ok(Self(
            Arc::from(url.host_str().ok_or(url::ParseError::EmptyHost)?),
            url.port().ok_or(url::ParseError::InvalidPort)?,
        ))
    }
}

/// Try to parse a slice of string-like items into a [`Vec`] of [`BrokerHost`].
///
/// # Errors
///
/// Fails if any hosts are invalid.
pub fn try_parse_hosts<S: AsRef<str>>(brokers: &[S]) -> Result<Vec<BrokerHost>, url::ParseError> {
    brokers
        .iter()
        .map(|h| h.as_ref().parse())
        .collect::<Result<Vec<_>, _>>()
}

#[cfg(test)]
mod test {

    use tokio_test::assert_err;

    use super::*;

    #[test]
    fn parses_typical_host() {
        let host = "localhost:9092";

        let broker_host: Result<BrokerHost, _> = host.parse();

        assert_eq!(broker_host, Ok(BrokerHost("localhost".into(), 9092)));
    }

    #[test]
    fn parses_with_protocol() {
        let host = "https://localhost:9092";

        let broker_host: Result<BrokerHost, _> = host.parse();

        assert_eq!(broker_host, Ok(BrokerHost("localhost".into(), 9092)));
    }

    #[test]
    fn fails_without_port() {
        let host = "localhost";

        let broker_host: Result<BrokerHost, _> = host.parse();

        assert_err!(broker_host);
    }

    #[test]
    fn fails_with_bad_port() {
        let host = "localhost:abcd";

        let broker_host: Result<BrokerHost, _> = host.parse();

        assert_err!(broker_host);
    }
}
