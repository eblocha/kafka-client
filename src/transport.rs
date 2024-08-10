use futures::{future::select_all, SinkExt, TryStreamExt};
use std::{
    io,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use tokio::{
    net::{TcpStream, ToSocketAddrs},
    sync::{mpsc::UnboundedSender, oneshot},
};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    codec::{KafkaCodec, RequestRecords, VersionedRequest},
    request::KafkaRequest,
    response::KafkaResponse,
};

#[derive(Debug, Clone, Default)]
struct TransportState {
    records: RequestRecords,
    next_correlation_id: Arc<AtomicI32>,
}

pub struct KafkaTransport {
    state: TransportState,
    sender: UnboundedSender<(VersionedRequest, oneshot::Sender<KafkaResponse>)>,
}

impl KafkaTransport {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let tcp = TcpStream::connect(addr).await?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(
            VersionedRequest,
            oneshot::Sender<KafkaResponse>,
        )>();

        let state = TransportState::default();

        let (r, w) = tcp.into_split();
        let mut stream_in = FramedRead::new(r, KafkaCodec::new(state.records.clone()));
        let mut stream_out = FramedWrite::new(w, KafkaCodec::new(state.records.clone()));

        let senders: Arc<DashMap<i32, oneshot::Sender<KafkaResponse>>> = Arc::new(DashMap::new());

        let senders_1 = senders.clone();
        let write = tokio::spawn(async move {
            while let Some((req, sender)) = rx.recv().await {
                senders_1.insert(req.correlation_id, sender);
                stream_out.send(req).await.unwrap();
            }
        });

        let senders_2 = senders.clone();
        let read = tokio::spawn(async move {
            while let Some((header, response)) = stream_in.try_next().await.unwrap() {
                if let Some((_, sender)) = senders_2.remove(&header.correlation_id) {
                    sender
                        .send(response)
                        .expect("response could not be delivered");
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
                    .fetch_add(1, Ordering::Relaxed),
                request: req,
            },
            cb_tx,
        ))?;

        Ok(cb_rx.await?)
    }
}
