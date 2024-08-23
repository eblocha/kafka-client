use std::time::Duration;

use rand::Rng;

/// Compute an exponential backoff with full jitter
pub fn exponential_backoff(min: Duration, max: Duration, attempt: u32) -> Duration {
    let duration = std::cmp::min(max, min * 2u32.saturating_pow(attempt));
    let frac = rand::thread_rng().gen_range(0.0..=1.0);
    Duration::from_secs_f64(duration.as_secs_f64() * frac)
}
