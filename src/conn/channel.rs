//! A low-level IO stream to a Kafka broker.

use std::{future::Future, io};

use futures::{SinkExt, StreamExt, future::Either};
use kafka_protocol::protocol::StrBytes;
#[cfg(test)]
use kafka_protocol::protocol::{Encodable, HeaderVersion};
use rustc_hash::{FxBuildHasher, FxHashMap};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
};
use tokio_util::{codec::Framed, sync::CancellationToken, task::TaskTracker};

use crate::{config::KafkaConfig, conn::codec::sendable::RequestRecord};

use super::codec::{
    CorrelationId, EncodableRequest, KafkaCodec, VersionedRequest,
    sendable::{DecodableResponse, Sendable},
};

#[derive(Debug, Error)]
pub enum KafkaChannelError {
    /// Indicates an IO problem. This could be a bad socket or an encoding problem.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// The client has stopped processing requests
    #[error("the connection is closed")]
    Closed,
}

impl From<oneshot::error::RecvError> for KafkaChannelError {
    #[inline]
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::Closed
    }
}

impl<T> From<mpsc::error::SendError<T>> for KafkaChannelError {
    #[inline]
    fn from(_: mpsc::error::SendError<T>) -> Self {
        Self::Closed
    }
}

pub type AwaitResponseSender = oneshot::Sender<Result<DecodableResponse, io::Error>>;

#[derive(Debug)]
pub enum ResponseSender {
    /// A response sender that needs the response object, so it should wait until a response has been received.
    OnResponse(AwaitResponseSender),
    /// A response sender that only wants to be notified when its request is flushed.
    OnFlush(oneshot::Sender<Result<(), io::Error>>),
}

impl ResponseSender {
    pub fn send_err(self, err: io::Error) {
        match self {
            ResponseSender::OnResponse(sender) => {
                let _ = sender.send(Err(err));
            }
            ResponseSender::OnFlush(sender) => {
                let _ = sender.send(Err(err));
            }
        }
    }

    #[cfg(test)]
    pub fn send_if_awaiter(self, response: DecodableResponse) {
        if let ResponseSender::OnResponse(sender) = self {
            let _ = sender.send(Ok(response));
        }
    }
}

#[derive(Debug)]
pub struct KafkaChannelMessage {
    pub versioned: VersionedRequest,
    pub tx: ResponseSender,
}

impl KafkaChannelMessage {
    #[cfg(test)]
    pub fn respond<R: Encodable + HeaderVersion>(self, response: R) {
        use crate::conn::testing::encode_response;

        let api_key = self.versioned.request.as_api_key();
        let api_version = self.versioned.api_version;
        self.tx
            .send_if_awaiter(encode_response(response, api_key, api_version));
    }

    #[cfg(test)]
    pub fn respond_with_version<R: Encodable + HeaderVersion>(self, response: R, api_version: i16) {
        use crate::conn::testing::encode_response;

        let api_key = self.versioned.request.as_api_key();
        self.tx
            .send_if_awaiter(encode_response(response, api_key, api_version));
    }
}

#[must_use]
struct KafkaChannelTask<IO> {
    io: IO,
    rx: mpsc::Receiver<KafkaChannelMessage>,
    cancellation_token: CancellationToken,
    config: KafkaConfig,
}

impl<IO> KafkaChannelTask<IO> {
    async fn run(mut self)
    where
        IO: AsyncRead + AsyncWrite,
    {
        let (mut sink, mut stream) = Framed::new(
            self.io,
            KafkaCodec::new(self.config.socket.max_frame_length),
        )
        .split();

        let mut in_flight =
            FxHashMap::<CorrelationId, (RequestRecord, AwaitResponseSender)>::with_capacity_and_hasher(self.config.socket.send_buffer_size, FxBuildHasher);

        let mut request_buffer = Vec::with_capacity(self.config.socket.send_buffer_size);
        let mut sender_batch = Vec::with_capacity(self.config.socket.send_buffer_size);

        let mut correlation_id = 0;

        let client_id = self.config.client_id.clone().map(StrBytes::from_string);

        loop {
            let either = tokio::select! {
                biased;
                () = self.cancellation_token.cancelled() => {
                    tracing::debug!("kafka channel task was cancelled, closing connection");
                    break
                },
                next_res = stream.next() => Either::Right(next_res),
                count = self.rx.recv_many(&mut request_buffer, self.config.socket.send_buffer_size) => Either::Left(count),
            };

            match either {
                Either::Left(count) => {
                    if count == 0 {
                        tracing::debug!("kafka channel was dropped, closing connection");
                        break;
                    }

                    tracing::trace!("sending {} frame(s)", request_buffer.len());
                    for message in request_buffer.drain(..) {
                        let id = CorrelationId(correlation_id);

                        let api_key = message.versioned.request.as_api_key();

                        let record = RequestRecord {
                            api_key,
                            api_version: message.versioned.api_version,
                            response_header_version: api_key
                                .response_header_version(message.versioned.api_version),
                        };

                        let encodable = EncodableRequest::from_versioned(
                            message.versioned,
                            id,
                            client_id.clone(),
                        );

                        let api_key = encodable.api_key();

                        match sink.feed(encodable).await {
                            Ok(()) => {
                                tracing::trace!(
                                    correlation_id = id.0,
                                    api_key = ?api_key,
                                    "io sink fed frame",
                                );
                                sender_batch.push((id, message.tx, record));
                            }
                            Err(e) => {
                                tracing::trace!(
                                    correlation_id = id.0,
                                    api_key = ?api_key,
                                    "io sink failed to feed frame: {:?}",
                                    e
                                );
                                message.tx.send_err(e);
                            }
                        }

                        correlation_id += 1;
                    }

                    if let Err(e) = sink.flush().await {
                        tracing::trace!("io sink failed to flush frames: {:?}", e);
                        // if the flush fails, notify all requests that they failed to send
                        for (_, sender, _) in sender_batch.drain(..) {
                            sender.send_err(e.kind().into());
                        }
                    } else {
                        tracing::trace!("io sink flushed frames");
                        for (correlation_id, sender, record) in sender_batch.drain(..) {
                            match sender {
                                ResponseSender::OnResponse(sender) => {
                                    in_flight.insert(correlation_id, (record, sender));
                                }
                                ResponseSender::OnFlush(sender) => {
                                    let _ = sender.send(Ok(()));
                                }
                            }
                        }
                    }
                }
                Either::Right(next_res) => match next_res {
                    Some(Ok(frame)) => {
                        tracing::trace!(
                            correlation_id = frame.id.0,
                            "read a frame from the io stream"
                        );
                        match in_flight.remove(&frame.id) {
                            Some((record, sender)) => {
                                // ok to ignore since it just means the request was abandoned
                                let _ = sender.send(Ok(DecodableResponse {
                                    record,
                                    frame: frame.frame,
                                }));
                            }
                            _ => {
                                tracing::warn!(
                                    correlation_id = frame.id.0,
                                    "read a frame that does not map to any pending request"
                                );
                            }
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("got an error from the io stream {e:?}");
                        for (_, (_, sender)) in in_flight {
                            let _ = sender.send(Err(e.kind().into()));
                        }
                        break;
                    }
                    None => {
                        tracing::debug!("connection closed by peer");
                        break;
                    }
                },
            }
        }

        tracing::debug!("closed io stream");
    }
}

/// A connection to a Kafka broker
///
/// This connection supports multiplexed async io.
#[derive(Debug, Clone)]
pub struct KafkaChannel {
    sender: mpsc::Sender<KafkaChannelMessage>,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
}

impl KafkaChannel {
    /// Wrap an IO stream to use as the transport for a Kafka connection.
    pub fn connect<IO: AsyncRead + AsyncWrite + Send + 'static>(
        io: IO,
        config: &KafkaConfig,
    ) -> Self {
        let cancellation_token = CancellationToken::new();

        let (tx, rx) = mpsc::channel(config.socket.send_buffer_size);

        let task_runner = KafkaChannelTask {
            io,
            rx,
            config: config.clone(),
            cancellation_token: cancellation_token.clone(),
        };

        let task_tracker = TaskTracker::new();

        task_tracker.spawn(task_runner.run());

        Self {
            sender: tx,
            task_tracker,
            cancellation_token,
        }
    }

    /// Create a channel from raw parts instead of using an IO stream. Useful for testing.
    #[cfg(test)]
    pub(crate) fn from_parts(
        sender: mpsc::Sender<KafkaChannelMessage>,
        task_tracker: TaskTracker,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            sender,
            task_tracker,
            cancellation_token,
        }
    }

    /// Sends a request and returns a future to await the response
    pub async fn send<R: Sendable>(
        &self,
        req: R,
        api_version: i16,
    ) -> Result<R::Response, KafkaChannelError> {
        send_on(&self.sender, req, api_version).await
    }

    /// Sends a request and returns a future that resolves when the message is sent.
    pub async fn send_and_forget<R: Sendable>(
        &self,
        req: R,
        api_version: i16,
    ) -> Result<(), KafkaChannelError> {
        send_on_and_forget(&self.sender, req, api_version).await
    }

    /// Obtain a new Sender to send and receive messages
    pub fn sender(&self) -> &mpsc::Sender<KafkaChannelMessage> {
        &self.sender
    }

    /// Shut down the connection. This is the preferred method to close a connection gracefully.
    ///
    /// Returns a future that can be awaited to wait for shutdown to complete.
    pub fn shutdown(&self) -> impl Future<Output = ()> + '_ {
        self.cancellation_token.cancel();
        self.task_tracker.close();
        self.task_tracker.wait()
    }
}

/// Send a message on the provided channel and await the response.
pub async fn send_on<R: Sendable>(
    sender: &mpsc::Sender<KafkaChannelMessage>,
    req: R,
    api_version: i16,
) -> Result<R::Response, KafkaChannelError> {
    let (tx, rx) = oneshot::channel();

    let versioned = VersionedRequest {
        api_version,
        request: req.into(),
    };

    sender
        .send(KafkaChannelMessage {
            versioned,
            tx: ResponseSender::OnResponse(tx),
        })
        .await?;

    // error happens when the client dropped our sender before sending anything.
    let response = rx.await??;

    Ok(R::decode(response)?)
}

/// Send a message on the provided channel and abandon it, do not wait for a response
pub async fn send_on_and_forget<R: Sendable>(
    sender: &mpsc::Sender<KafkaChannelMessage>,
    req: R,
    api_version: i16,
) -> Result<(), KafkaChannelError> {
    let (tx, rx) = oneshot::channel();

    let versioned = VersionedRequest {
        api_version,
        request: req.into(),
    };

    sender
        .send(KafkaChannelMessage {
            versioned,
            tx: ResponseSender::OnFlush(tx),
        })
        .await?;

    Ok(rx.await??)
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use bytes::{BufMut, BytesMut};
    use kafka_protocol::{
        messages::{
            ApiKey, BrokerId, MetadataRequest, MetadataResponse, RequestHeader, ResponseHeader,
            metadata_response::MetadataResponseBroker,
        },
        protocol::{Encodable, Message},
    };
    use tokio_test::{assert_err, assert_ok};

    use super::*;

    const REQ_VERSION: i16 = MetadataRequest::VERSIONS.max;

    fn create_kafka_config() -> KafkaConfig {
        KafkaConfig {
            client_id: None,
            ..KafkaConfig::default()
        }
    }

    fn create_request_response(
        correlation_id: i32,
    ) -> ((MetadataRequest, BytesMut), (MetadataResponse, BytesMut)) {
        let req_header_version = ApiKey::Metadata.request_header_version(REQ_VERSION);
        let res_header_version = ApiKey::Metadata.response_header_version(REQ_VERSION);

        // REQUEST
        let request = MetadataRequest::default()
            .with_allow_auto_topic_creation(true)
            .with_topics(None);

        let request_header = RequestHeader::default()
            .with_correlation_id(correlation_id)
            .with_request_api_key(ApiKey::Metadata as i16)
            .with_request_api_version(REQ_VERSION)
            .with_client_id(None);

        let size = (request_header.compute_size(req_header_version).unwrap()
            + request.compute_size(REQ_VERSION).unwrap()) as i32;

        let mut req_bytes = BytesMut::new();
        req_bytes.put(&(size.to_be_bytes()[..]));

        request_header
            .encode(&mut req_bytes, req_header_version)
            .unwrap();
        request.encode(&mut req_bytes, REQ_VERSION).unwrap();

        // RESPONSE
        let response = MetadataResponse::default()
            .with_brokers(vec![MetadataResponseBroker::default()])
            .with_controller_id(BrokerId(0));

        let response_header = ResponseHeader::default().with_correlation_id(correlation_id);

        let size = (response.compute_size(REQ_VERSION).unwrap()
            + response_header.compute_size(res_header_version).unwrap()) as i32;

        let mut res_bytes = BytesMut::new();
        res_bytes.put(&(size.to_be_bytes()[..]));

        response_header
            .encode(&mut res_bytes, res_header_version)
            .unwrap();
        response.encode(&mut res_bytes, REQ_VERSION).unwrap();

        ((request, req_bytes), (response, res_bytes))
    }

    #[tokio::test]
    async fn client_sends_request() {
        let ((request, req_bytes), (expected_response, res_bytes)) = create_request_response(0);

        let io = tokio_test::io::Builder::new()
            .write(&req_bytes)
            .read(&res_bytes)
            .build();

        let conn = KafkaChannel::connect(io, &create_kafka_config());

        let response =
            tokio::time::timeout(Duration::from_millis(500), conn.send(request, REQ_VERSION))
                .await
                .unwrap()
                .unwrap();

        conn.shutdown().await;

        assert_eq!(response, expected_response);
    }

    #[tokio::test]
    async fn multiplexing() {
        let ((request_1, req_bytes_1), (expected_response_1, res_bytes_1)) =
            create_request_response(0);
        let ((request_2, req_bytes_2), (expected_response_2, res_bytes_2)) =
            create_request_response(1);

        let io = tokio_test::io::Builder::new()
            .write(&req_bytes_1)
            .write(&req_bytes_2)
            .read(&res_bytes_2)
            .read(&res_bytes_1)
            .build();

        let conn = KafkaChannel::connect(io, &create_kafka_config());

        let (response_1, response_2) = tokio::join!(
            conn.send(request_1, REQ_VERSION),
            conn.send(request_2, REQ_VERSION)
        );

        conn.shutdown().await;

        assert_eq!(response_1.unwrap(), expected_response_1);
        assert_eq!(response_2.unwrap(), expected_response_2);
    }

    #[tokio::test]
    async fn shutdown() {
        let ((request, req_bytes), (_, _)) = create_request_response(0);

        let io = tokio_test::io::Builder::new().write(&req_bytes).build();

        let conn = Arc::new(KafkaChannel::connect(io, &create_kafka_config()));

        let conn_copy = conn.clone();

        // send a request but shut down before it's responded to
        let response = tokio::spawn(async move { conn_copy.send(request, REQ_VERSION).await });

        conn.shutdown().await;

        let response = response.await.unwrap();

        assert_err!(&response);

        match response.unwrap_err() {
            KafkaChannelError::Closed => {}
            e => panic!("expected closed error but got {e:?}"),
        };
    }

    #[tokio::test]
    async fn send_on_closed_connection() {
        let ((request, _), (_, _)) = create_request_response(0);

        let io = tokio_test::io::Builder::new().build();

        let conn = Arc::new(KafkaChannel::connect(io, &create_kafka_config()));

        conn.shutdown().await;

        let response = conn.send(request, REQ_VERSION).await;

        assert_err!(&response);

        match response.unwrap_err() {
            KafkaChannelError::Closed => {}
            e => panic!("expected closed error but got {e:?}"),
        };
    }

    #[tokio::test]
    async fn send_and_forget() {
        let ((request, req_bytes), (_, _)) = create_request_response(0);

        let io = tokio_test::io::Builder::new().write(&req_bytes).build();

        let conn = Arc::new(KafkaChannel::connect(io, &create_kafka_config()));

        let response = conn.send_and_forget(request, REQ_VERSION).await;

        assert_ok!(response)
    }
}
