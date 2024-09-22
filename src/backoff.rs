use std::time::Duration;

/// Compute an exponential backoff with full jitter
pub fn exponential_backoff(min: Duration, max: Duration, attempt: u32) -> Duration {
    let duration = std::cmp::min(max, min * 2u32.saturating_pow(attempt));
    duration.mul_f64(rand::random())
}
