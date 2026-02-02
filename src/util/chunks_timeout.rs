use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{Stream, StreamExt, ready, stream::Fuse};
use pin_project_lite::pin_project;
use tokio::time::{Sleep, sleep};

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct ChunksTimeout<St: Stream> {
        #[pin]
        stream: Fuse<St>,
        #[pin]
        deadline: Option<Sleep>,
        cap: usize,
        duration: Duration,
        items: Vec<St::Item>,
    }
}

impl<St: Stream> ChunksTimeout<St> {
    pub(super) fn new(stream: St, cap: usize, duration: Duration) -> Self {
        Self {
            stream: stream.fuse(),
            deadline: None,
            cap,
            duration,
            items: Vec::with_capacity(cap),
        }
    }

    pub fn get_pin_mut(self: Pin<&mut Self>) -> Pin<&mut St> {
        self.project().stream.get_pin_mut()
    }

    pub fn take_into(self: Pin<&mut Self>, other: &mut Vec<St::Item>) {
        other.append(self.project().items);
    }
}

impl<St: Stream> Stream for ChunksTimeout<St> {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut me = self.as_mut().project();

        loop {
            match me.stream.as_mut().poll_next(cx) {
                Poll::Pending => break,

                Poll::Ready(Some(item)) => {
                    if me.items.is_empty() {
                        me.deadline.set(Some(sleep(*me.duration)));

                        me.items.reserve_exact(*me.cap);
                    }

                    me.items.push(item);

                    if me.items.len() >= *me.cap {
                        return Poll::Ready(Some(()));
                    }
                }

                Poll::Ready(None) => {
                    // Returning Some here is only correct because we fuse the inner stream.

                    let last = if me.items.is_empty() { None } else { Some(()) };

                    return Poll::Ready(last);
                }
            }
        }

        if !me.items.is_empty() {
            if let Some(deadline) = me.deadline.as_pin_mut() {
                ready!(deadline.poll(cx));
            }

            return Poll::Ready(Some(()));
        }

        Poll::Pending
    }
}
