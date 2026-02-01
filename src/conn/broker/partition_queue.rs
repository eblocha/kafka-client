use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use futures::{ready, Stream};
use tokio::{sync::mpsc, time::Sleep};
use tokio_stream::StreamMap;

use crate::common::TopicPartition;

pub struct PartitionQueue<M> {
    retry_buffer: VecDeque<(Option<Pin<Box<Sleep>>>, M)>,
    rx: mpsc::Receiver<M>,
}

impl<M> PartitionQueue<M> {
    pub fn new(rx: mpsc::Receiver<M>) -> Self {
        Self {
            retry_buffer: VecDeque::new(),
            rx,
        }
    }

    /// Queue a message for retry
    ///
    /// If `due` is [`None`], the message will not have a retry delay.
    pub fn retry(&mut self, message: M, due: Option<Instant>) {
        self.retry_buffer.push_front((
            due.map(|deadline| Box::pin(tokio::time::sleep_until(deadline.into()))),
            message,
        ));
    }

    pub fn close(&mut self) {
        self.rx.close();
    }
}

impl<M> Unpin for PartitionQueue<M> {}

impl<M> Stream for PartitionQueue<M> {
    type Item = M;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some((Some(sleep), _msg)) = this.retry_buffer.get_mut(0) {
            // If the next message has a deadline, make sure we have passed it before continuing.
            ready!(sleep.as_mut().poll(cx));
        }

        if let Some((_, msg)) = this.retry_buffer.pop_front() {
            return Poll::Ready(Some(msg));
        }

        this.rx.poll_recv(cx)
    }
}

pub type PartitionQueueMap<M> = StreamMap<TopicPartition, PartitionQueue<M>>;

#[cfg(test)]
mod test {

    use std::time::{Duration, Instant};

    use futures::StreamExt;
    use tokio::sync::mpsc;

    use super::PartitionQueue;

    #[tokio::test]
    async fn test_order() {
        let (tx, rx) = mpsc::channel::<usize>(5);

        let queue = PartitionQueue::new(rx);

        for i in 0..5 {
            tx.try_send(i).unwrap();
        }

        drop(tx);

        let items = queue.collect::<Vec<_>>().await;

        assert_eq!(items, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_retry_lifo() {
        let (tx, rx) = mpsc::channel::<usize>(5);
        let mut queue = PartitionQueue::new(rx);

        tx.try_send(0).unwrap();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();

        let item1 = queue.next().await.unwrap();
        let item2 = queue.next().await.unwrap();

        queue.retry(item2, None);
        queue.retry(item1, None);

        let retried1 = queue.next().await.unwrap();
        let retried2 = queue.next().await.unwrap();
        let next = queue.next().await.unwrap();

        assert_eq!(retried1, 0);
        assert_eq!(retried2, 1);
        assert_eq!(next, 2);
    }

    #[tokio::test]
    async fn test_retry_after_sender_drop() {
        let (tx, rx) = mpsc::channel::<usize>(5);
        let mut queue = PartitionQueue::new(rx);

        tx.try_send(0).unwrap();
        tx.try_send(1).unwrap();

        let item = queue.next().await.unwrap();
        queue.retry(item, None);

        drop(tx);

        let retried = queue.next().await.unwrap();
        let next = queue.next().await.unwrap();

        assert_eq!(retried, 0);
        assert_eq!(next, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_delay_increasing_deadline() {
        let (tx, rx) = mpsc::channel::<usize>(5);
        let mut queue = PartitionQueue::new(rx);

        tx.try_send(0).unwrap();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();

        let item1 = queue.next().await.unwrap();
        let item2 = queue.next().await.unwrap();

        let now_tokio = tokio::time::Instant::now();
        let now = Instant::now();

        queue.retry(item2, now.checked_add(Duration::from_secs(2)));
        queue.retry(item1, now.checked_add(Duration::from_secs(1)));

        let retried = queue.next().await.unwrap();

        assert_eq!(now_tokio.elapsed().as_secs(), 1);
        assert_eq!(retried, 0);

        let retried = queue.next().await.unwrap();

        assert_eq!(now_tokio.elapsed().as_secs(), 2);
        assert_eq!(retried, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_delay_decreasing_deadline() {
        let (tx, rx) = mpsc::channel::<usize>(5);
        let mut queue = PartitionQueue::new(rx);

        tx.try_send(0).unwrap();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();

        let item1 = queue.next().await.unwrap();
        let item2 = queue.next().await.unwrap();

        let now_tokio = tokio::time::Instant::now();
        let now = Instant::now();

        queue.retry(item2, now.checked_add(Duration::from_secs(1)));
        queue.retry(item1, now.checked_add(Duration::from_secs(2)));

        let retried = queue.next().await.unwrap();

        assert_eq!(now_tokio.elapsed().as_secs(), 2);
        assert_eq!(retried, 0);

        let retried = queue.next().await.unwrap();

        assert_eq!(now_tokio.elapsed().as_secs(), 2);
        assert_eq!(retried, 1);
    }
}
