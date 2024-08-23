use std::{sync::Arc, time::Duration};

use crate::conn::config::ConnectionManagerConfig;

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Client id to include with every request.
    ///
    /// Default None
    pub client_id: Option<Arc<str>>,
    /// Size of the request send buffer. Further requests will experience backpressure.
    ///
    /// Default 512
    pub send_buffer_size: usize,
    /// Maximum frame length allowed in the transport layer. If a request is larger than this, an error is returned.
    ///
    /// Default 8 MiB
    pub max_frame_length: usize,
    /// Maximum number of connection retry attempts before returning an error.
    /// If None, the retries are infinite.
    ///
    /// Default None
    pub connection_max_retries: Option<u32>,
    /// Minimum time to wait between connection attempts.
    ///
    /// Default 10ms
    pub connection_min_backoff: Duration,
    /// Maximum time to wait between connection attempts.
    ///
    /// Default 30s
    pub connection_max_backoff: Duration,
    /// Timeout to establish a connection before retrying.
    ///
    /// Default 10s
    pub connection_timeout: Duration,
    /// How often to refresh cluster metadata
    ///
    /// Default 5min
    pub metadata_refresh_interval: Duration,
    /// Minimum time to wait between refresh attempts.
    ///
    /// Default 10ms
    pub metadata_refresh_min_backoff: Duration,
    /// Maximum time to wait between refresh attempts.
    ///
    /// Default 30s
    pub metadata_refresh_max_backoff: Duration,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        let mgr = ConnectionManagerConfig::default();

        Self {
            client_id: mgr.conn.io.client_id,
            send_buffer_size: mgr.conn.io.send_buffer_size,
            max_frame_length: mgr.conn.io.max_frame_length,
            connection_max_retries: mgr.conn.retry.max_retries,
            connection_min_backoff: mgr.conn.retry.min_backoff,
            connection_max_backoff: mgr.conn.retry.max_backoff,
            connection_timeout: mgr.conn.retry.connection_timeout,
            metadata_refresh_interval: mgr.metadata.interval,
            metadata_refresh_max_backoff: mgr.metadata.max_backoff,
            metadata_refresh_min_backoff: mgr.metadata.min_backoff,
        }
    }
}
