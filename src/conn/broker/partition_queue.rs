use std::{
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
    retry_buffer: Vec<(Option<Pin<Box<Sleep>>>, M)>,
    rx: mpsc::Receiver<M>,
}

impl<M> PartitionQueue<M> {
    pub fn new(rx: mpsc::Receiver<M>) -> Self {
        Self {
            retry_buffer: Vec::new(),
            rx,
        }
    }

    /// Queue a message for retry
    ///
    /// If `due` is [`None`], the message will not have a retry delay.
    pub fn retry(&mut self, message: M, due: Option<Instant>) {
        self.retry_buffer.push((
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

        if let Some((Some(sleep), _msg)) = this.retry_buffer.last_mut() {
            // If the next message has a deadline, make sure we have passed it before continuing.
            ready!(sleep.as_mut().poll(cx));
        }

        if let Some((_, msg)) = this.retry_buffer.pop() {
            return Poll::Ready(Some(msg));
        }

        this.rx.poll_recv(cx)
    }
}

pub type PartitionQueueMap<M> = StreamMap<TopicPartition, PartitionQueue<M>>;

#[cfg(test)]
mod test {

    use std::time::Duration;

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

        let item = queue.next().await.unwrap();

        drop(tx);

        queue.retry(item, None);

        let retried = queue.next().await.unwrap();
        let next = queue.next().await;

        assert_eq!(retried, 0);
        assert_eq!(next, None);
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

        let now = tokio::time::Instant::now();

        queue.retry(
            item2,
            now.checked_add(Duration::from_secs(2)).map(Into::into),
        );
        queue.retry(
            item1,
            now.checked_add(Duration::from_secs(1)).map(Into::into),
        );

        let retried = queue.next().await.unwrap();

        assert_eq!(now.elapsed(), Duration::from_secs(1));
        assert_eq!(retried, 0);

        let retried = queue.next().await.unwrap();

        assert_eq!(now.elapsed(), Duration::from_secs(2));
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

        let now = tokio::time::Instant::now();

        queue.retry(
            item2,
            now.checked_add(Duration::from_secs(1)).map(Into::into),
        );
        queue.retry(
            item1,
            now.checked_add(Duration::from_secs(2)).map(Into::into),
        );

        let retried = queue.next().await.unwrap();

        assert_eq!(now.elapsed().as_secs(), 2);
        assert_eq!(retried, 0);

        let retried = queue.next().await.unwrap();

        assert_eq!(now.elapsed().as_secs(), 2);
        assert_eq!(retried, 1);
    }
}
