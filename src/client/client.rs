use std::{
    future::Future,
    io,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
};

use bytes::BytesMut;
use dashmap::DashMap;
use derive_more::derive::From;
use futures::{SinkExt, StreamExt};
use tokio::{
    net::{TcpStream, ToSocketAddrs},
    sync::{mpsc, oneshot},
};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::client::codec::sendable::RequestRecord;

use super::codec::{
    correlated::{CorrelatedDecoder, CorrelationId},
    request::{EncodableRequest, RequestEncoder, VersionedRequest},
    sendable::Sendable,
};

#[derive(Debug, Clone, Default)]
struct ClientState {
    next_correlation_id: Arc<AtomicI32>,
}

#[allow(unused)]
#[derive(Debug, From)]
pub enum KafkaClientError {
    /// Indicates an IO problem. This could be a bad socket or an encoding problem.
    Io(#[from] io::Error),
    /// The client has stopped processing requests
    Stopped,
}

type ResponseSender = oneshot::Sender<Result<BytesMut, KafkaClientError>>;

/// Configuration for the Kafka client
#[derive(Debug, Clone)]
pub struct KafkaClientConfig {
    /// Size of the request send buffer. Further requests will experience backpressure.
    pub send_buffer_size: usize,
    /// Maximum frame length allowed in the transport layer. If a request is larger than this, an error is returned.
    pub max_frame_length: usize,
    /// Client id to include with every request.
    pub client_id: Option<Arc<str>>,
}

impl Default for KafkaClientConfig {
    fn default() -> Self {
        Self {
            send_buffer_size: 512,
            max_frame_length: 8 * 1024 * 1024,
            client_id: None,
        }
    }
}

pub struct KafkaClient {
    state: ClientState,
    sender: mpsc::Sender<(EncodableRequest, ResponseSender)>,
    shutdown: oneshot::Sender<()>,
    client_id: Option<Arc<str>>,
}

impl KafkaClient {
    async fn run<S>(
        tcp: TcpStream,
        mut rx: mpsc::Receiver<(EncodableRequest, ResponseSender)>,
        shutdown: S,
        max_frame_length: usize,
    ) where
        S: Future + Send + 'static,
        S::Output: Send + 'static,
    {
        let (r, w) = tcp.into_split();
        let mut stream_in = FramedRead::new(r, CorrelatedDecoder::new(max_frame_length));
        let mut stream_out = FramedWrite::new(w, RequestEncoder::new(max_frame_length));

        // TODO is there a more efficient data structure?
        // We know the keys are created sequentially with an atomic i32
        let senders: Arc<DashMap<CorrelationId, ResponseSender>> = Arc::new(DashMap::new());

        let senders_1 = senders.clone();
        let write_fut = async move {
            while let Some((req, mut sender)) = rx.recv().await {
                let correlation_id = req.correlation_id();

                // send is cancel-safe for FramedWrite
                let result = tokio::select! {
                    res = stream_out.send(req) => res,
                    _ = sender.closed() => Ok(())
                };

                if let Err(e) = result {
                    // failed to push message to tcp stream.
                    // if this send fails, the request was abandoned.
                    let _ = sender.send(Err(e.into()));
                    continue;
                }

                senders_1.insert(correlation_id, sender);
            }
        };

        let senders_2 = senders.clone();
        let read_fut = async move {
            while let Some(read_result) = stream_in.next().await {
                let frame = match read_result {
                    Ok(frame) => frame,
                    Err(e) => {
                        // TODO should we set up another channel to receive these errors?
                        // This means the tcp socket is bad somehow, or the frame looked funky.
                        // Probably best to get a new connection.
                        eprintln!("encountered unrecoverable IO error {e:?}");
                        break;
                    }
                };

                let Some((_, sender)) = senders_2.remove(&frame.id) else {
                    eprintln!("discarding frame with correlation id {:?}", frame.id);
                    continue;
                };

                // ok to ignore since it just means the request was abandoned
                let _ = sender.send(Ok(frame.frame));
            }
        };

        tokio::select! {
            _ = write_fut => {},
            _ = read_fut => {}
            _ = shutdown => {}
        };
    }

    pub async fn connect<A: ToSocketAddrs>(
        addr: A,
        config: &KafkaClientConfig,
    ) -> io::Result<Self> {
        let tcp = TcpStream::connect(addr).await?;

        let client_id = config.client_id.clone();

        let (tx, rx) = mpsc::channel::<(EncodableRequest, ResponseSender)>(config.send_buffer_size);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        tokio::spawn(KafkaClient::run(
            tcp,
            rx,
            shutdown_rx,
            config.max_frame_length,
        ));

        let state = ClientState::default();

        Ok(Self {
            state,
            sender: tx,
            shutdown: shutdown_tx,
            client_id,
        })
    }

    pub fn shutdown(self) {
        // If this fails, it means the client has already dropped the receiver, so it's already shut down.
        let _ = self.shutdown.send(());
    }

    pub async fn send<R: Sendable>(
        &self,
        req: R,
        api_version: i16,
    ) -> Result<R::Response, KafkaClientError> {
        let (tx, rx) = oneshot::channel();

        let versioned = VersionedRequest {
            api_version,
            correlation_id: self
                .state
                .next_correlation_id
                .fetch_add(1, Ordering::Relaxed)
                .into(),
            request: req.into(),
            client_id: self.client_id.clone(),
        };

        let encodable_request: EncodableRequest = versioned.into();

        let api_key = encodable_request.api_key();
        let api_version = encodable_request.api_version();

        let record = RequestRecord {
            api_key,
            api_version,
            response_header_version: api_key.response_header_version(api_version),
        };

        self.sender
            .send((encodable_request, tx))
            .await
            .map_err(|_| KafkaClientError::Stopped)?;

        // error happens when the client dropped our sender before sending anything.
        let frame = rx.await.map_err(|_| KafkaClientError::Stopped)??;

        Ok(R::decode_frame(frame, record)?)
    }
}
