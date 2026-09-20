//! Waiting before trying again.
//!
//! # Why jitter, when the usual argument does not apply
//!
//! The textbook reason is a thundering herd: five hundred agents reconnecting
//! inside one fifty-millisecond window. **That is not this.** Three capture
//! processes on one host is not a herd.
//!
//! The reason here is narrower and real: the venue permits a bounded number of
//! **new connections a minute per IP**, shared across every process on the
//! host. A tight reconnect loop against a venue that is refusing can spend that
//! budget while learning nothing — and several processes restarting together
//! spend it at once, so that when the venue does come back none of them can
//! connect.
//!
//! The predecessor backs off without jitter. This costs ten lines and no
//! dependency, which is also why the `backoff` and `tokio-retry` crates were
//! not taken: a dependency whose value is ten lines is a dependency whose
//! failure modes are free.

use std::time::Duration;

/// How long to wait before the next attempt.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    initial: Duration,
    cap: Duration,
    /// The fraction each wait may vary by, either way.
    jitter: f64,
    current: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff::new(Duration::from_millis(250), Duration::from_secs(30), 0.30)
    }
}

impl Backoff {
    /// A policy.
    pub fn new(initial: Duration, cap: Duration, jitter: f64) -> Backoff {
        Backoff {
            initial,
            cap,
            jitter: jitter.clamp(0.0, 1.0),
            current: initial,
        }
    }

    /// The next wait, doubling toward the cap and jittered either way.
    ///
    /// `next_wait` rather than `next`: this is not an iterator, and a reader
    /// who assumed it was would expect it to end.
    pub fn next_wait(&mut self) -> Duration {
        let base = self.current;
        self.current = (self.current * 2).min(self.cap);
        jittered(base, self.jitter)
    }

    /// A connection succeeded. The next failure waits the initial interval
    /// again — otherwise one bad hour leaves the process waiting thirty seconds
    /// after a blip a week later.
    pub fn reset(&mut self) {
        self.current = self.initial;
    }

    /// What the next wait will be built from, before jitter. For a test that
    /// wants the growth without the randomness.
    pub fn base(&self) -> Duration {
        self.current
    }
}

fn jittered(base: Duration, jitter: f64) -> Duration {
    if jitter <= 0.0 {
        return base;
    }
    let factor = 1.0 + (rand::random::<f64>() * 2.0 - 1.0) * jitter;
    base.mul_f64(factor.max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wait_grows_to_a_cap() {
        let mut b = Backoff::new(
            Duration::from_millis(100),
            Duration::from_millis(800),
            0.0, // no jitter, so growth alone is what is asserted
        );
        let waits: Vec<u128> = (0..6).map(|_| b.next_wait().as_millis()).collect();
        assert_eq!(waits, vec![100, 200, 400, 800, 800, 800]);
    }

    #[test]
    fn two_waits_at_one_attempt_differ() {
        // Not a strict requirement that any two differ — randomness may
        // collide — but over many samples the spread must exist, or the
        // jitter is not doing anything.
        let seen: std::collections::BTreeSet<u128> = (0..64)
            .map(|_| {
                let mut b =
                    Backoff::new(Duration::from_millis(1_000), Duration::from_secs(30), 0.30);
                b.next_wait().as_millis()
            })
            .collect();
        assert!(seen.len() > 1, "jitter produced one value over 64 samples");
    }

    #[test]
    fn jitter_stays_inside_its_band() {
        for _ in 0..256 {
            let mut b = Backoff::new(Duration::from_millis(1_000), Duration::from_secs(30), 0.30);
            let ms = b.next_wait().as_millis();
            assert!((700..=1_300).contains(&ms), "{ms} is outside +/-30%");
        }
    }

    #[test]
    fn a_successful_connection_resets_the_wait() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_secs(30), 0.0);
        for _ in 0..5 {
            b.next_wait();
        }
        assert!(b.base() > Duration::from_millis(100));
        b.reset();
        assert_eq!(b.base(), Duration::from_millis(100));
        assert_eq!(b.next_wait().as_millis(), 100);
    }
}
