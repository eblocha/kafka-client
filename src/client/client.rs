use std::{
    io,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use derive_more::derive::From;
use futures::{SinkExt, TryStreamExt};
use kafka_protocol::{
    messages::{ApiKey, ResponseHeader},
    protocol::Decodable,
};
use tokio::{
    net::{TcpStream, ToSocketAddrs},
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};
use tokio_util::codec::{FramedRead, FramedWrite};

use super::{
    codec::{
        correlated::{CorrelatedDecoder, CorrelationId},
        request::{EncodableRequest, RequestEncoder, VersionedRequest},
    },
    request::KafkaRequest,
    response::KafkaResponse,
};

#[derive(Debug, Clone)]
struct RequestRecord {
    api_key: ApiKey,
    api_version: i16,
    response_header_version: i16,
}

#[derive(Debug, Clone, Default)]
struct ClientState {
    next_correlation_id: Arc<AtomicI32>,
}

#[derive(Debug, From)]
pub enum KafkaClientError {
    Io(#[from] io::Error),
    Rejected(VersionedRequest),
    Other,
}

type ResponseSender = oneshot::Sender<Result<KafkaResponse, KafkaClientError>>;

pub struct KafkaClient {
    state: ClientState,
    sender: UnboundedSender<(VersionedRequest, ResponseSender)>,
}

#[inline]
fn into_invalid_data(error: anyhow::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

impl KafkaClient {
    async fn run(tcp: TcpStream, mut rx: UnboundedReceiver<(VersionedRequest, ResponseSender)>) {
        let (r, w) = tcp.into_split();
        let mut stream_in = FramedRead::new(r, CorrelatedDecoder::new());
        let mut stream_out = FramedWrite::new(w, RequestEncoder::new());

        let senders: Arc<DashMap<CorrelationId, (RequestRecord, ResponseSender)>> =
            Arc::new(DashMap::new());

        let senders_1 = senders.clone();
        let write_fut = async move {
            while let Some((req, sender)) = rx.recv().await {
                let correlation_id = req.correlation_id;

                let encodable_request: EncodableRequest = req.into();
                let api_key = encodable_request.api_key();
                let api_version = encodable_request.api_version();

                let record = RequestRecord {
                    api_key,
                    api_version,
                    response_header_version: api_key.response_header_version(api_version),
                };

                if sender.is_closed() {
                    // abandonded request, no need to send it
                    continue;
                }

                if let Err(e) = stream_out.send(encodable_request).await {
                    // failed to push message to tcp stream
                    if !sender.is_closed() {
                        sender
                            .send(Err(e.into()))
                            .expect("Send failed on open oneshot channel. This is a bug.");
                    }
                    continue;
                }

                senders_1.insert(correlation_id, (record, sender));
            }
        };

        let senders_2 = senders.clone();
        let read_fut = async move {
            // TODO handle read error from tcp stream
            while let Some(mut frame) = stream_in.try_next().await.unwrap() {
                let Some((_, (record, sender))) = senders_2.remove(&frame.id) else {
                    eprintln!("discarding frame with correlation id {:?}", frame.id);
                    continue;
                };

                if let Err(e) =
                    ResponseHeader::decode(&mut frame.frame, record.response_header_version)
                {
                    if !sender.is_closed() {
                        sender
                            .send(Err(into_invalid_data(e).into()))
                            .expect("Send failed on open oneshot channel. This is a bug.");
                    }
                    continue;
                }

                match KafkaResponse::decode(&mut frame.frame, record.api_version, record.api_key) {
                    Ok(response) => {
                        if !sender.is_closed() {
                            sender
                                .send(Ok(response))
                                .expect("Send failed on open oneshot channel. This is a bug.")
                        }
                    }
                    Err(e) => {
                        if !sender.is_closed() {
                            sender
                                .send(Err(into_invalid_data(e).into()))
                                .expect("Send failed on open oneshot channel. This is a bug.")
                        }
                    }
                }
            }
        };

        // TODO shutdown signal
        tokio::select! {
            _ = write_fut => {},
            _ = read_fut => {}
        };
    }

    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let tcp = TcpStream::connect(addr).await?;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(VersionedRequest, ResponseSender)>();

        tokio::spawn(KafkaClient::run(tcp, rx));

        let state = ClientState::default();

        Ok(Self { state, sender: tx })
    }

    pub async fn send(
        &self,
        req: KafkaRequest,
        api_version: i16,
    ) -> Result<KafkaResponse, KafkaClientError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send((
                VersionedRequest {
                    api_version,
                    correlation_id: self
                        .state
                        .next_correlation_id
                        .fetch_add(1, Ordering::Relaxed)
                        .into(),
                    request: req,
                },
                tx,
            ))
            .map_err(|e| KafkaClientError::Rejected(e.0 .0))?;

        // error happens when the client dropped our sender before sending anything.
        rx.await.map_err(|_| KafkaClientError::Other)?
    }
}
