//! Configuration options for the Kafka client

use std::{sync::Arc, time::Duration};

use crate::config::KafkaConfig;

/// Configuration for the Kafka TCP connection
#[derive(Debug, Clone)]
pub struct KafkaConnectionConfig {
    /// Client id to include with every request.
    pub client_id: Option<Arc<str>>,
    /// Size of the request send buffer. Further requests will experience backpressure.
    pub send_buffer_size: usize,
    /// Maximum frame length allowed in the transport layer. If a request is larger than this, an error is returned.
    pub max_frame_length: usize,
}

impl Default for KafkaConnectionConfig {
    fn default() -> Self {
        Self {
            send_buffer_size: 512,
            max_frame_length: 8 * 1024 * 1024,
            client_id: None,
        }
    }
}

impl From<&KafkaConfig> for KafkaConnectionConfig {
    fn from(value: &KafkaConfig) -> Self {
        Self {
            client_id: value.client_id.clone(),
            send_buffer_size: value.send_buffer_size,
            max_frame_length: value.max_frame_length,
        }
    }
}

/// Controls how reconnection attempts are handled.
#[derive(Debug, Clone)]
pub struct ConnectionRetryConfig {
    /// Maximum number of connection retry attempts before returning an error.
    /// If None, the retries are infinite.
    ///
    /// Default None
    pub max_retries: Option<u32>,
    /// Minimum time to wait between connection attempts.
    ///
    /// Default 10ms
    pub min_backoff: Duration,
    /// Maximum time to wait between connection attempts.
    ///
    /// Default 30s
    pub max_backoff: Duration,
    /// Timeout to establish a connection before retrying.
    ///
    /// Default 10s
    pub connection_timeout: Duration,
}

impl Default for ConnectionRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: None,
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(10),
        }
    }
}

impl From<&KafkaConfig> for ConnectionRetryConfig {
    fn from(value: &KafkaConfig) -> Self {
        Self {
            max_retries: value.connection_max_retries,
            min_backoff: value.connection_min_backoff,
            max_backoff: value.connection_max_backoff,
            connection_timeout: value.connection_timeout,
        }
    }
}

/// Configuration options for a managed Kafka connection
#[derive(Debug, Clone, Default)]
pub struct ConnectionConfig {
    /// Connection retry configuration
    pub retry: ConnectionRetryConfig,
    /// IO stream configuration
    pub io: KafkaConnectionConfig,
}

impl From<&KafkaConfig> for ConnectionConfig {
    fn from(value: &KafkaConfig) -> Self {
        Self {
            retry: value.into(),
            io: value.into(),
        }
    }
}

/// Controls how metadata is refreshed, and retried.
#[derive(Debug, Clone)]
pub struct MetadataRefreshConfig {
    /// How often to refresh in the background
    pub interval: Duration,
    /// If refresh fails, the minimum time to wait before retrying
    pub min_backoff: Duration,
    /// If refresh fails, the maximum time to wait before retrying
    pub max_backoff: Duration,
}

impl From<&KafkaConfig> for MetadataRefreshConfig {
    fn from(value: &KafkaConfig) -> Self {
        Self {
            interval: value.metadata_refresh_interval,
            min_backoff: value.connection_min_backoff,
            max_backoff: value.connection_max_backoff,
        }
    }
}

impl Default for MetadataRefreshConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5 * 60),
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(30),
        }
    }
}

/// Configuration options for the Kafka connection manager
#[derive(Debug, Default, Clone)]
pub struct ConnectionManagerConfig {
    /// Options for new broker connections
    pub conn: ConnectionConfig,
    /// Cluster metadata refresh options
    pub metadata: MetadataRefreshConfig,
}

impl From<&KafkaConfig> for ConnectionManagerConfig {
    fn from(value: &KafkaConfig) -> Self {
        Self {
            conn: value.into(),
            metadata: value.into(),
        }
    }
}
