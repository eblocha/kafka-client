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
    /// Random noise to apply to backoff duration.
    /// Every backoff adds `min_backoff * 0..jitter` to its wait time.
    ///
    /// Default 10
    pub connection_jitter: u32,
    /// Timeout to establish a connection before retrying.
    ///
    /// Default 10s
    pub connection_timeout: Duration,
    /// How often to refresh cluster metadata
    ///
    /// Default 5min
    pub metadata_refresh_interval: Duration,
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
            connection_jitter: mgr.conn.retry.jitter,
            connection_timeout: mgr.conn.retry.connection_timeout,
            metadata_refresh_interval: mgr.metadata_refresh_interval,
        }
    }
}
