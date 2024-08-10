use std::{
    io,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use futures::{future::select_all, SinkExt, TryStreamExt};
use kafka_protocol::{
    messages::{ApiKey, ResponseHeader},
    protocol::Decodable,
};
use tokio::{
    net::{TcpStream, ToSocketAddrs},
    sync::{mpsc::UnboundedSender, oneshot},
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

type ResponseSender = oneshot::Sender<KafkaResponse>;

pub struct KafkaClient {
    state: ClientState,
    sender: UnboundedSender<(VersionedRequest, ResponseSender)>,
}

// #[inline]
// fn into_invalid_input(error: anyhow::Error) -> io::Error {
//     io::Error::new(io::ErrorKind::InvalidInput, error)
// }

// #[inline]
// fn into_invalid_data(error: anyhow::Error) -> io::Error {
//     io::Error::new(io::ErrorKind::InvalidData, error)
// }

impl KafkaClient {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let tcp = TcpStream::connect(addr).await?;

        let (tx, mut rx) =
            tokio::sync::mpsc::unbounded_channel::<(VersionedRequest, ResponseSender)>();

        let state = ClientState::default();

        let (r, w) = tcp.into_split();
        let mut stream_in = FramedRead::new(r, CorrelatedDecoder::new());
        let mut stream_out = FramedWrite::new(w, RequestEncoder::new());

        let senders: Arc<DashMap<CorrelationId, (RequestRecord, ResponseSender)>> =
            Arc::new(DashMap::new());

        let senders_1 = senders.clone();
        let write = tokio::spawn(async move {
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

                senders_1.insert(correlation_id, (record, sender));
                stream_out.send(encodable_request).await.unwrap();
            }
        });

        let senders_2 = senders.clone();
        let read = tokio::spawn(async move {
            while let Some(mut frame) = stream_in.try_next().await.unwrap() {
                if let Some((_, (record, sender))) = senders_2.remove(&frame.id) {
                    let _ =
                        ResponseHeader::decode(&mut frame.frame, record.response_header_version)
                            .unwrap();

                    let response =
                        KafkaResponse::decode(&mut frame.frame, record.api_version, record.api_key)
                            .unwrap();

                    sender
                        .send(response)
                        .expect("response could not be delivered");
                } else {
                    eprintln!("discarding frame with correlation id {:?}", frame.id)
                }
            }
        });

        // TODO shutdown
        tokio::spawn(select_all([read, write]));

        Ok(Self { state, sender: tx })
    }

    pub async fn send(&self, req: KafkaRequest, api_version: i16) -> anyhow::Result<KafkaResponse> {
        let (cb_tx, cb_rx) = tokio::sync::oneshot::channel();

        self.sender.send((
            VersionedRequest {
                api_version,
                correlation_id: self
                    .state
                    .next_correlation_id
                    .fetch_add(1, Ordering::Relaxed)
                    .into(),
                request: req,
            },
            cb_tx,
        ))?;

        Ok(cb_rx.await?)
    }
}
