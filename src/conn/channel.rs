//! A low-level IO stream to a Kafka broker.

use std::{future::Future, io};

use fnv::FnvHashMap;
use futures::{future::Either, SinkExt, StreamExt};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
};
use tokio_util::{codec::Framed, sync::CancellationToken, task::TaskTracker};

use crate::conn::codec::sendable::RequestRecord;

use super::{
    codec::{
        sendable::{DecodableResponse, Sendable},
        CorrelationId, EncodableRequest, KafkaCodec, VersionedRequest,
    },
    config::KafkaConnectionConfig,
};

#[derive(Debug, Error)]
pub enum KafkaChannelError {
    /// Indicates an IO problem. This could be a bad socket or an encoding problem.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// The client has stopped processing requests
    #[error("the connection is closed")]
    Closed,

    /// The broker's version range does not intersect with the client
    #[error("version mismatch")]
    Version,
}

pub type ResponseSender = oneshot::Sender<Result<DecodableResponse, io::Error>>;

#[derive(Debug)]
pub struct KafkaChannelMessage {
    pub versioned: VersionedRequest,
    pub tx: ResponseSender,
}

#[must_use]
struct KafkaChannelTask<IO> {
    io: IO,
    rx: mpsc::Receiver<KafkaChannelMessage>,
    cancellation_token: CancellationToken,
    config: KafkaConnectionConfig,
}

impl<IO> KafkaChannelTask<IO> {
    async fn run(mut self)
    where
        IO: AsyncRead + AsyncWrite,
    {
        let (mut sink, mut stream) =
            Framed::new(self.io, KafkaCodec::new(self.config.max_frame_length)).split();

        let mut in_flight: FnvHashMap<CorrelationId, (RequestRecord, ResponseSender)> =
            FnvHashMap::with_capacity_and_hasher(self.config.send_buffer_size, Default::default());

        let mut request_buffer = Vec::with_capacity(self.config.send_buffer_size);
        let mut sender_batch = Vec::with_capacity(self.config.send_buffer_size);

        let mut correlation_id = 0;

        loop {
            let either = tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => break,
                next_res = stream.next() => Either::Right(next_res),
                count = self.rx.recv_many(&mut request_buffer, self.config.send_buffer_size) => Either::Left(count),
            };

            match either {
                Either::Left(count) => match count {
                    // 0 means all senders dropped, and no remaining messages. This happens only when the connection is dropped.
                    0 => break,
                    _ => {
                        tracing::trace!("sending {} frame(s)", request_buffer.len());
                        for message in request_buffer.drain(..) {
                            let id = CorrelationId(correlation_id);

                            let api_key = message.versioned.request.as_api_key();

                            let record = RequestRecord {
                                api_version: message.versioned.api_version,
                                response_header_version: api_key
                                    .response_header_version(message.versioned.api_version),
                            };

                            let encodable = EncodableRequest::from_versioned(
                                message.versioned,
                                id,
                                self.config.client_id.clone(),
                            );

                            let api_key = encodable.api_key();

                            match sink.feed(encodable).await {
                                Ok(_) => {
                                    tracing::trace!(
                                        correlation_id = id.0,
                                        api_key = ?api_key,
                                        "io sink fed frame",
                                    );
                                    sender_batch.push((id, message.tx, record))
                                }
                                Err(e) => {
                                    tracing::trace!(
                                        correlation_id = id.0,
                                        api_key = ?api_key,
                                        "io sink failed to feed frame: {:?}",
                                        e
                                    );
                                    let _ = message.tx.send(Err(e));
                                }
                            }

                            correlation_id += 1;
                        }

                        match sink.flush().await {
                            Err(e) => {
                                tracing::trace!("io sink failed to flush frames: {:?}", e);
                                // if the flush fails, notify all requests that they failed to send
                                for (_, sender, _) in sender_batch.drain(..) {
                                    let _ = sender.send(Err(e.kind().into()));
                                }
                            }
                            Ok(_) => {
                                tracing::trace!("io sink flushed frames");
                                for (correlation_id, sender, record) in sender_batch.drain(..) {
                                    in_flight.insert(correlation_id, (record, sender));
                                }
                            }
                        }
                    }
                },
                Either::Right(next_res) => match next_res {
                    Some(Ok(frame)) => {
                        tracing::trace!(
                            correlation_id = frame.id.0,
                            "read a frame from the io stream"
                        );
                        if let Some((record, sender)) = in_flight.remove(&frame.id) {
                            // ok to ignore since it just means the request was abandoned
                            let _ = sender.send(Ok(DecodableResponse {
                                record,
                                frame: frame.frame,
                            }));
                        } else {
                            tracing::warn!(
                                correlation_id = frame.id.0,
                                "read a frame that does not map to any pending request"
                            );
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("got an error from the io stream {e:?}");
                        for (_, (_, sender)) in in_flight {
                            let _ = sender.send(Err(e.kind().into()));
                        }
                        break;
                    }
                    None => break,
                },
            }
        }

        tracing::debug!("closing io stream");
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
        config: &KafkaConnectionConfig,
    ) -> Self {
        let cancellation_token = CancellationToken::new();

        let (tx, rx) = mpsc::channel(config.send_buffer_size);

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
            cancellation_token: cancellation_token.clone(),
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
        .send(KafkaChannelMessage { versioned, tx })
        .await
        .map_err(|_| KafkaChannelError::Closed)?;

    // error happens when the client dropped our sender before sending anything.
    let response = rx.await.map_err(|_| KafkaChannelError::Closed)??;

    Ok(R::decode(response)?)
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use bytes::{BufMut, BytesMut};
    use kafka_protocol::{
        indexmap::IndexMap,
        messages::{
            metadata_response::MetadataResponseBroker, ApiKey, BrokerId, MetadataRequest,
            MetadataResponse, RequestHeader, ResponseHeader,
        },
        protocol::{Encodable, Message},
    };
    use tokio_test::assert_err;

    use super::*;

    const REQ_VERSION: i16 = MetadataRequest::VERSIONS.max;

    fn create_request_response(
        correlation_id: i32,
    ) -> ((MetadataRequest, BytesMut), (MetadataResponse, BytesMut)) {
        let req_header_version = ApiKey::MetadataKey.request_header_version(REQ_VERSION);
        let res_header_version = ApiKey::MetadataKey.response_header_version(REQ_VERSION);

        // REQUEST
        let request = {
            let mut r = MetadataRequest::default();
            r.allow_auto_topic_creation = true;
            r.topics = None;
            r
        };

        let request_header = {
            let mut h = RequestHeader::default();
            h.correlation_id = correlation_id;
            h.request_api_key = ApiKey::MetadataKey as i16;
            h.request_api_version = REQ_VERSION;
            h.client_id = None;
            h
        };

        let size = (request_header.compute_size(req_header_version).unwrap()
            + request.compute_size(REQ_VERSION).unwrap()) as i32;

        let mut req_bytes = BytesMut::new();
        req_bytes.put(&(size.to_be_bytes()[..]));

        request_header
            .encode(&mut req_bytes, req_header_version)
            .unwrap();
        request.encode(&mut req_bytes, REQ_VERSION).unwrap();

        // RESPONSE
        let response = {
            let mut r = MetadataResponse::default();
            r.brokers = IndexMap::from_iter([(BrokerId(0), MetadataResponseBroker::default())]);
            r.controller_id = BrokerId(0);
            r
        };

        let response_header = {
            let mut h = ResponseHeader::default();
            h.correlation_id = correlation_id;
            h
        };

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

        let conn = KafkaChannel::connect(io, &Default::default());

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

        let conn = KafkaChannel::connect(io, &Default::default());

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

        let conn = Arc::new(KafkaChannel::connect(io, &Default::default()));

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

        let conn = Arc::new(KafkaChannel::connect(io, &Default::default()));

        conn.shutdown().await;

        let response = conn.send(request, REQ_VERSION).await;

        assert_err!(&response);

        match response.unwrap_err() {
            KafkaChannelError::Closed => {}
            e => panic!("expected closed error but got {e:?}"),
        };
    }
}
