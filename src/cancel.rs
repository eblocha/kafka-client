use std::future::Future;

use tokio_util::sync::CancellationToken;

pub trait OrCancelled {
    type Output;

    /// Race a cancellation token.
    ///
    /// This will return [`None`] if the token cancels first, or [`Some`] if the future is ready first.
    ///
    /// The cancellation token is polled first.
    fn or_cancel(
        self,
        token: &CancellationToken,
    ) -> impl Future<Output = Option<Self::Output>> + Send;
}

impl<T, Fut: Future<Output = T> + Send> OrCancelled for Fut {
    type Output = T;

    async fn or_cancel(self, token: &CancellationToken) -> Option<Self::Output> {
        tokio::select! {
            biased;
            () = token.cancelled() => None,
            value = self => Some(value)
        }
    }
}
