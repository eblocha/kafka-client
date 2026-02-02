use std::{future, io};

use bytes::BytesMut;
use kafka_protocol::{
    messages::{ApiKey, ApiVersionsResponse, ResponseHeader},
    protocol::{Encodable, HeaderVersion},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    cancel::OrCancelled,
    common::BrokerHost,
    config::KafkaConfig,
    conn::{
        channel::{KafkaChannel, KafkaChannelMessage},
        codec::sendable::RequestRecord,
        DecodableResponse,
    },
    connect::Connect,
};

pub fn encode_response<R: Encodable + HeaderVersion>(
    response: R,
    api_key: ApiKey,
    api_version: i16,
) -> DecodableResponse {
    let header = ResponseHeader::default();
    let header_version = R::header_version(api_version);

    let mut frame = BytesMut::new();

    header.encode(&mut frame, header_version).unwrap();
    response.encode(&mut frame, api_version).unwrap();

    DecodableResponse {
        record: RequestRecord {
            api_key,
            api_version,
            response_header_version: header_version,
        },
        frame,
    }
}

pub struct TestHarness {
    pub tx: mpsc::Sender<KafkaChannelMessage>,
    pub rx: mpsc::Receiver<KafkaChannelMessage>,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
}

impl TestHarness {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(1);
        let task_tracker = TaskTracker::new();
        let cancellation_token = CancellationToken::new();

        Self {
            tx,
            rx,
            task_tracker,
            cancellation_token,
        }
    }

    /// Spawn a task that immediately responds to a connection request with an empty successful versions response.
    pub fn spawn_ok(mut self) -> JoinHandle<Self> {
        let tracker = self.task_tracker.clone();

        tracker.spawn(async move {
            loop {
                let Some(Some(req)) = self.rx.recv().or_cancel(&self.cancellation_token).await
                else {
                    break;
                };

                let response = ApiVersionsResponse::default();

                req.respond(response)
            }

            self
        })
    }

    /// Spawn a task that never responds to any request.
    pub fn spawn_never(self) -> JoinHandle<Self> {
        let tracker = self.task_tracker.clone();

        tracker.spawn(async move {
            self.cancellation_token.cancelled().await;
            self
        })
    }
}

#[derive(Clone)]
pub struct NeverConnects;

impl Connect for NeverConnects {
    async fn connect(
        &self,
        _host: &BrokerHost,
        _config: &KafkaConfig,
    ) -> Result<KafkaChannel, io::Error> {
        future::pending().await
    }
}

pub fn create_channel() -> (TestHarness, KafkaChannel) {
    let harness = TestHarness::new();

    let channel = KafkaChannel::from_parts(
        harness.tx.clone(),
        harness.task_tracker.clone(),
        harness.cancellation_token.clone(),
    );

    (harness, channel)
}
