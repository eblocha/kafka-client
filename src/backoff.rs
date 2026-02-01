use std::time::Duration;

use rand::Rng;
use tokio::time::Instant;

const JITTER: f64 = 0.2;

/// Compute an exponential backoff with jitter
pub fn exponential_backoff(min: Duration, max: Duration, attempt: u32) -> Duration {
    let duration = std::cmp::min(max, min * 2u32.saturating_pow(attempt));
    // take a random value between 80% and 120% of the duration
    duration.mul_f64(rand::thread_rng().gen_range((1.0 - JITTER)..(1.0 + JITTER)))
}

pub fn exponential_backoff_due(
    min: Duration,
    max: Duration,
    attempt: u32,
) -> Option<std::time::Instant> {
    std::time::Instant::now().checked_add(exponential_backoff(min, max, attempt))
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

    /// Schedule a retry with no delay, and optionally reset the retry count.
    ///
    /// This is useful for handling connection init errors, as those errors are backed off in the broker connection task.
    pub fn schedule_immediate(&mut self, payload: T, reset_count: bool) {
        self.attempt = Some(BackoffAttempt {
            delay_until: None,
            payload,
        });

        if reset_count {
            self.count = 0;
        }
    }

    /// Take the pending backoff attempt if one was requested
    fn take(&mut self) -> Option<BackoffAttempt<T>> {
        self.attempt.take()
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio_test::{assert_err, assert_ok};

    use crate::backoff::BackoffSession;

    #[test]
    fn failure_increments_count() {
        let mut backoff = BackoffSession::default();
        backoff.failure(Duration::from_secs(1), ());

        assert_eq!(backoff.count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn failure_waits_next_attempt() {
        let mut backoff = BackoffSession::default();

        backoff.failure(Duration::from_secs(1), ());

        let res = tokio::time::timeout(Duration::from_millis(900), backoff.wait_next()).await;

        assert_err!(res);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_starts_from_failure_instant() {
        let mut backoff = BackoffSession::default();
        backoff.schedule_next(Duration::from_secs(1), ());

        tokio::time::advance(Duration::from_secs(1)).await;

        let res = tokio::time::timeout(Duration::from_millis(10), backoff.wait_next()).await;

        assert_ok!(res);
    }

    #[test]
    fn success_resets_count() {
        let mut backoff = BackoffSession::default();
        backoff.failure(Duration::from_secs(1), ());
        backoff.failure(Duration::from_secs(1), ());

        backoff.success();

        assert_eq!(backoff.count(), 0);
    }

    #[test]
    fn schedule_next_does_not_increment_count() {
        let mut backoff = BackoffSession::default();
        backoff.schedule_next(Duration::from_secs(1), ());

        assert_eq!(backoff.count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn schedule_immediate_does_not_wait() {
        let mut backoff = BackoffSession::default();

        backoff.schedule_immediate((), false);

        let res = tokio::time::timeout(Duration::from_millis(10), backoff.wait_next()).await;

        assert_ok!(res);
    }

    #[test]
    fn schedule_immediate_resets_count() {
        let mut backoff = BackoffSession::default();
        backoff.failure(Duration::from_secs(1), ());
        backoff.failure(Duration::from_secs(1), ());

        backoff.schedule_immediate((), true);

        assert_eq!(backoff.count(), 0);
    }

    #[test]
    fn schedule_immediate_does_not_reset_count() {
        let mut backoff = BackoffSession::default();
        backoff.failure(Duration::from_secs(1), ());
        backoff.failure(Duration::from_secs(1), ());

        backoff.schedule_immediate((), false);

        assert_eq!(backoff.count(), 2);
    }
}
