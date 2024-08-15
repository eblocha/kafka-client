use std::{
    io,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
};

use bytes::BytesMut;
use fnv::FnvHashMap;
use futures::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::{
    codec::Framed,
    sync::{CancellationToken, DropGuard},
    task::{task_tracker::TaskTrackerWaitFuture, TaskTracker},
};

use crate::conn::codec::sendable::RequestRecord;

use super::codec::{
    sendable::Sendable, CorrelationId, EncodableRequest, KafkaCodec, VersionedRequest,
};

#[derive(Debug, Default)]
struct ConnectionState {
    next_correlation_id: AtomicI32,
}

#[derive(Debug, Error)]
pub enum KafkaConnectionError {
    /// Indicates an IO problem. This could be a bad socket or an encoding problem.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The client has stopped processing requests
    #[error("the connection is closed")]
    Closed,
}

type ResponseSender = oneshot::Sender<Result<BytesMut, KafkaConnectionError>>;

/// Configuration for the Kafka client
#[derive(Debug, Clone)]
pub struct KafkaConnectionConfig {
    /// Size of the request send buffer. Further requests will experience backpressure.
    pub send_buffer_size: usize,
    /// Maximum frame length allowed in the transport layer. If a request is larger than this, an error is returned.
    pub max_frame_length: usize,
    /// Client id to include with every request.
    pub client_id: Option<Arc<str>>,
}

impl Default for KafkaConnectionConfig {
    fn default() -> Self {
        Self {
            send_buffer_size: 512,
            max_frame_length: 8 * 1024 * 1024,
            client_id: None,
        }
    }
}

#[must_use]
struct KafkaConnectionBackgroundTaskRunner<IO> {
    io: IO,
    rx: mpsc::Receiver<(EncodableRequest, ResponseSender)>,
    max_frame_length: usize,
    send_buffer_size: usize,
    cancellation_token: CancellationToken,
}

impl<IO> KafkaConnectionBackgroundTaskRunner<IO> {
    async fn run(mut self)
    where
        IO: AsyncRead + AsyncWrite,
    {
        let (mut sink, mut stream) =
            Framed::new(self.io, KafkaCodec::new(self.max_frame_length)).split();

        let mut senders: FnvHashMap<CorrelationId, ResponseSender> =
            FnvHashMap::with_capacity_and_hasher(self.send_buffer_size, Default::default());

        let mut request_buffer = Vec::with_capacity(self.send_buffer_size);
        let mut sender_batch = Vec::with_capacity(self.send_buffer_size);

        loop {
            tokio::select! {
                count = self.rx.recv_many(&mut request_buffer, self.send_buffer_size) => match count {
                    // 0 means all senders dropped, and no remaining messages. This happens only when the connection is dropped.
                    0 => break,
                    _ => {
                        for (req, sender) in request_buffer.drain(..) {
                            let correlation_id = req.correlation_id();

                            match sink.feed(req).await {
                                Ok(_) => sender_batch.push((correlation_id, sender)),
                                Err(e) => {
                                    let _ = sender.send(Err(e.into()));
                                },
                            }
                        }

                        match sink.flush().await {
                            Err(e) => {
                                // if the flush fails, notify all requests that they failed to send
                                for (_, sender) in sender_batch.drain(..) {
                                    let _ = sender.send(Err(KafkaConnectionError::Io(e.kind().into())));
                                }
                            },
                            Ok(_) => {
                                for (correlation_id, sender) in sender_batch.drain(..) {
                                    senders.insert(correlation_id, sender);
                                }
                            },
                        }
                    },
                },
                next_res = stream.next() => match next_res {
                    Some(Ok(frame)) => {
                        if let Some(sender) = senders.remove(&frame.id) {
                            // ok to ignore since it just means the request was abandoned
                            let _ = sender.send(Ok(frame.frame));
                        }
                    },
                    Some(Err(e)) => {
                        for (_, sender) in senders {
                            let _ = sender.send(Err(KafkaConnectionError::Io(e.kind().into())));
                        }
                        break
                    },
                    None => break
                },
                _ = self.cancellation_token.cancelled() => break
            }
        }
    }
}

/// A connection to a Kafka broker
///
/// This connection supports multiplexed async io.
/// The connection will be closed on drop.
pub struct KafkaConnection {
    state: ConnectionState,
    sender: mpsc::Sender<(EncodableRequest, ResponseSender)>,
    client_id: Option<Arc<str>>,
    task_tracker: TaskTracker,
    task_handle: JoinHandle<()>,
    cancellation_token: CancellationToken,
    _cancel_on_drop: DropGuard,
}

impl KafkaConnection {
    pub async fn connect<IO: AsyncRead + AsyncWrite + Send + 'static>(
        io: IO,
        config: &KafkaConnectionConfig,
    ) -> io::Result<Self> {
        let client_id = config.client_id.clone();

        let cancellation_token = CancellationToken::new();

        let (tx, rx) = mpsc::channel::<(EncodableRequest, ResponseSender)>(config.send_buffer_size);

        let task_runner = KafkaConnectionBackgroundTaskRunner {
            io,
            rx,
            max_frame_length: config.max_frame_length,
            send_buffer_size: config.send_buffer_size,
            cancellation_token: cancellation_token.clone(),
        };

        let task_tracker = TaskTracker::new();

        let task_handle = task_tracker.spawn(task_runner.run());

        task_tracker.close();

        let state = ConnectionState::default();

        let client = Self {
            state,
            sender: tx,
            client_id,
            task_tracker,
            task_handle,
            cancellation_token: cancellation_token.clone(),
            _cancel_on_drop: cancellation_token.drop_guard(),
        };

        Ok(client)
    }

    /// Sends a request and returns a future to await the response
    pub async fn send<R: Sendable>(
        &self,
        req: R,
        api_version: i16,
    ) -> Result<R::Response, KafkaConnectionError> {
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
            api_version,
            response_header_version: api_key.response_header_version(api_version),
        };

        self.sender
            .send((encodable_request, tx))
            .await
            .map_err(|_| KafkaConnectionError::Closed)?;

        // error happens when the client dropped our sender before sending anything.
        let frame = rx.await.map_err(|_| KafkaConnectionError::Closed)??;

        Ok(R::decode_frame(frame, record)?)
    }

    /// Shut down the connection. This is the preferred method to close a connection gracefully.
    ///
    /// Returns a future that can be awaited to wait for shutdown to complete.
    pub fn shutdown(&self) -> TaskTrackerWaitFuture<'_> {
        self.cancellation_token.cancel();
        self.task_tracker.wait()
    }

    /// Returns true if the connection is closed and will no longer process requests
    pub fn is_closed(&self) -> bool {
        self.task_handle.is_finished()
    }

    /// Waits until the connection is closed
    pub fn closed(&self) -> TaskTrackerWaitFuture<'_> {
        self.task_tracker.wait()
    }

    /// Returns the number of empty slots in the connection's send buffer
    pub fn capacity(&self) -> usize {
        self.sender.capacity()
    }

    /// Returns the maximum number of slots in the connection's send buffer
    pub fn max_capacity(&self) -> usize {
        self.sender.max_capacity()
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use bytes::BufMut;
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

        let conn = KafkaConnection::connect(io, &Default::default())
            .await
            .unwrap();

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

        let conn = KafkaConnection::connect(io, &Default::default())
            .await
            .unwrap();

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

        let conn = Arc::new(
            KafkaConnection::connect(io, &Default::default())
                .await
                .unwrap(),
        );

        let conn_copy = conn.clone();

        // send a request but shut down before it's responded to
        let response = tokio::spawn(async move { conn_copy.send(request, REQ_VERSION).await });

        conn.shutdown().await;

        let response = response.await.unwrap();

        assert_err!(&response);

        match response.unwrap_err() {
            KafkaConnectionError::Closed => {},
            e => panic!("expected closed error but got {e:?}")
        };
    }

    #[tokio::test]
    async fn send_on_closed_connection() {
        let ((request, _), (_, _)) = create_request_response(0);

        let io = tokio_test::io::Builder::new().build();

        let conn = Arc::new(
            KafkaConnection::connect(io, &Default::default())
                .await
                .unwrap(),
        );

        conn.shutdown().await;

        let response = conn.send(request, REQ_VERSION).await;

        assert_err!(&response);

        match response.unwrap_err() {
            KafkaConnectionError::Closed => {},
            e => panic!("expected closed error but got {e:?}")
        };
    }
}
