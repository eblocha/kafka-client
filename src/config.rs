use std::{fmt::Debug, time::Duration};

use kafka_protocol::records::Compression;

#[derive(Clone)]
pub struct SaslCredentials {
    pub username: String,
    pub password: String,
}

impl Debug for SaslCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslCredentials")
            .field("username", &self.username)
            .field("password", &"********")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub enum SaslMecahnism {
    Plain(SaslCredentials),
    ScramSha256(SaslCredentials),
    ScramSha512(SaslCredentials),
}

#[derive(Debug, Clone)]
pub struct SocketConfig {
    /// Size of the request send buffer. Further requests will experience backpressure.
    ///
    /// Default 512
    pub send_buffer_size: usize,
    /// Maximum frame length allowed in the transport layer. If a request is larger than this, an error is returned.
    ///
    /// Default 8 MiB
    pub max_frame_length: usize,
    /// Maximum time allowed for broker connection setup.
    ///
    /// Default 30s
    pub connection_setup_timeout: Duration,
    /// Enable SO_KEEPALIVE on broker sockets.
    ///
    /// Default false
    pub keepalive: bool,
    /// Enable TCP_NODELAY on broker sockets.
    ///
    /// Default true
    pub nodelay: bool,
    /// Maximum number of times to retry socket connection setup to each broker.
    /// Use None to retry infinitely.
    ///
    /// Default None
    pub max_retries: Option<u32>,
    /// Initial backoff duration to wait for a connection retry.
    ///
    /// Default 100ms
    pub reconnect_backoff: Duration,
    /// Maximum backoff duration to wait for a connection retry.
    ///
    /// Default 10s
    pub reconnect_backoff_max: Duration,
}

impl Default for SocketConfig {
    fn default() -> Self {
        Self {
            send_buffer_size: 512,
            max_frame_length: 8 * 1024 * 1024 * 1024,
            connection_setup_timeout: Duration::from_secs(30),
            keepalive: false,
            nodelay: true,
            max_retries: None,
            reconnect_backoff: Duration::from_millis(10),
            reconnect_backoff_max: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetadataConfig {
    /// How often to proactively refresh cluster metadata to detect new topics or brokers.
    ///
    /// Default 5min
    pub refresh_interval: Duration,
    /// Minimum time to wait between refresh attempts.
    ///
    /// Default 10ms
    pub backoff: Duration,
    /// Maximum time to wait between refresh attempts.
    ///
    /// Default 10s
    pub backoff_max: Duration,
    /// Maximum time to cache topic metadata.
    ///
    /// Default 15min
    pub max_age: Duration,
    /// Number of topic refresh requests to batch together when multiple clients request topic data while the background
    /// task is busy.
    ///
    /// Default 1
    pub refresh_batch_count: usize,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(5 * 60),
            backoff: Duration::from_millis(10),
            backoff_max: Duration::from_secs(10),
            max_age: Duration::from_secs(15 * 60),
            refresh_batch_count: 1,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompressionCodec {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl From<CompressionCodec> for Compression {
    fn from(value: CompressionCodec) -> Self {
        match value {
            CompressionCodec::None => Compression::None,
            CompressionCodec::Gzip => Compression::Gzip,
            CompressionCodec::Snappy => Compression::Snappy,
            CompressionCodec::Lz4 => Compression::Lz4,
            CompressionCodec::Zstd => Compression::Zstd,
        }
    }
}

impl Default for CompressionCodec {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Number of acknowledgements the leader must receive from ISR brokers before responding to a produce request.
    ///
    /// A value of `-1` means all ISR nodes must ack. A value of 0 means no acks are required.
    /// Note that 0 will cause the base_offset to be `-1` in the produce response.
    ///
    /// Default -1
    pub required_acks: i16,
    /// Ack timeout of the producer request.
    ///
    /// Default 30s
    pub request_timeout: Duration,
    /// Limits the time a produced message may take for successful delivery.
    ///
    /// Default 5min
    pub message_timeout: Duration,
    /// Compression codec for produce messages.
    ///
    /// Default [`CompressionCodec::None`]
    pub compression_codec: CompressionCodec,
    /// Number of produce records to batch into a single produce request.
    ///
    /// Default 2000
    pub batch_count: usize,
    /// Time to wait for produce messages to accumulate.
    ///
    /// Default 5ms
    pub linger: Duration,
    /// Enable transactional producer.
    ///
    /// Default None
    pub transactional_id: Option<String>,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            required_acks: -1,
            request_timeout: Duration::from_secs(30),
            message_timeout: Duration::from_secs(5 * 60),
            compression_codec: CompressionCodec::default(),
            batch_count: 2000,
            linger: Duration::from_millis(5),
            transactional_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Client id to include with every request.
    ///
    /// Default is the value of the CARGO_PKG_NAME environment variable at compile time.
    pub client_id: Option<String>,

    /// Configuration for broker sockets.
    pub socket: SocketConfig,

    /// Timeout for broker API version requests
    ///
    /// Default 10s
    pub api_version_request_timeout: Duration,

    /// SASL authentication configuration. Use None for no authentication.
    ///
    /// Default None
    pub sasl: Option<SaslMecahnism>,

    /// Maximum number of bootstrap retry attempts per-broker before returning an error.
    /// If None, the retries are infinite.
    ///
    /// Default None
    pub bootstrap_max_retries: Option<u32>,

    /// Cluster metadata configuration.
    pub metadata: MetadataConfig,

    /// Allow automatic topic creation when subscribing to or assigning non-existent topics.
    ///
    /// Default false
    pub allow_auto_create_topics: bool,

    /// Producer configuration.
    pub producer: ProducerConfig,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            client_id: option_env!("CARGO_PKG_NAME").map(String::from),
            socket: SocketConfig::default(),
            api_version_request_timeout: Duration::from_secs(10),
            sasl: None,
            bootstrap_max_retries: None,
            metadata: MetadataConfig::default(),
            allow_auto_create_topics: false,
            producer: ProducerConfig::default(),
        }
    }
}
