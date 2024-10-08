use std::{fmt::Debug, sync::Arc};

use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use url::Url;

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
        BrokerHost(Arc::from(broker.host.as_str()), broker.port as u16)
    }
}

impl TryFrom<&str> for BrokerHost {
    type Error = url::ParseError;

    fn try_from(broker: &str) -> Result<Self, Self::Error> {
        let mut url = Url::parse(broker)?;

        if !url.has_host() {
            url = Url::parse(&format!("kafka://{}", broker))?;
        }

        Ok(Self(
            Arc::from(url.host_str().ok_or(url::ParseError::EmptyHost)?),
            url.port().ok_or(url::ParseError::InvalidPort)?,
        ))
    }
}

/// Try to parse a slice of string-like items into a `Vec` of [`BrokerHost`].
///
/// Fails if any hosts are invalid.
pub fn try_parse_hosts<S: AsRef<str>>(brokers: &[S]) -> Result<Vec<BrokerHost>, url::ParseError> {
    brokers
        .iter()
        .map(|h| BrokerHost::try_from(h.as_ref()))
        .collect::<Result<Vec<_>, _>>()
}

#[cfg(test)]
mod test {

    use tokio_test::assert_err;

    use super::*;

    #[test]
    fn parses_typical_host() {
        let host = "localhost:9092";

        let broker_host = BrokerHost::try_from(host);

        assert_eq!(broker_host, Ok(BrokerHost("localhost".into(), 9092)));
    }

    #[test]
    fn parses_with_protocol() {
        let host = "https://localhost:9092";

        let broker_host = BrokerHost::try_from(host);

        assert_eq!(broker_host, Ok(BrokerHost("localhost".into(), 9092)));
    }

    #[test]
    fn fails_without_port() {
        let host = "localhost";

        let broker_host = BrokerHost::try_from(host);

        assert_err!(broker_host);
    }

    #[test]
    fn fails_with_bad_port() {
        let host = "localhost:abcd";

        let broker_host = BrokerHost::try_from(host);

        assert_err!(broker_host);
    }
}
