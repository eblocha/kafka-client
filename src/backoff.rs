use std::time::Duration;

use tokio::time::Instant;

/// Compute an exponential backoff with full jitter
pub fn exponential_backoff(min: Duration, max: Duration, attempt: u32) -> Duration {
    let duration = std::cmp::min(max, min * 2u32.saturating_pow(attempt));
    duration.mul_f64(rand::random())
}

#[derive(Debug)]
pub struct BackoffAttempt<T> {
    pub delay_until: Option<Instant>,
    pub payload: T,
}

#[derive(Debug, Default)]
pub struct BackoffSession<T> {
    attempt: Option<BackoffAttempt<T>>,
    count: u32,
}

impl<T> BackoffSession<T> {
    /// Get the current attempt count
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Mark the backoff session as successful, resetting its state.
    pub fn success(&mut self) {
        self.attempt = None;
        self.count = 0;
    }

    /// Take the pending backoff attempt if one was requested
    pub fn take(&mut self) -> Option<BackoffAttempt<T>> {
        self.attempt.take()
    }

    /// Wait for the next backoff attempt to be ready.
    ///
    /// If there is no backoff attempt scheduled, this returns immediately with [`None`].
    ///
    /// If there is a backoff scheduled, this will sleep until its deadline, then return the payload for the attempt.
    pub async fn wait_next(&mut self) -> Option<T> {
        let attempt = self.take()?;
        if let Some(when) = attempt.delay_until {
            tokio::time::sleep_until(when).await;
        }
        Some(attempt.payload)
    }

    /// Mark a failed attempt, and schedule a retry with the provided payload after `delay`.
    pub fn failure(&mut self, delay: Duration, payload: T) {
        self.schedule_next(delay, payload);
        self.count += 1;
    }

    /// Schedule a retry with the provided payload after `delay`.
    ///
    /// This does _not_ increment the attempt count.
    pub fn schedule_next(&mut self, delay: Duration, payload: T) {
        self.attempt = Some(BackoffAttempt {
            delay_until: Instant::now().checked_add(delay),
            payload,
        });
    }
}
