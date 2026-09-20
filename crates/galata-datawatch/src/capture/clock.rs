//! The clock the loop owns.
//!
//! **Every timestamp that reaches a payload, a gap or a status message
//! originates here.** Nothing below the loop reads one, and
//! `scripts/check-clock-discipline.sh` asserts it.
//!
//! That is what lets rotation, the flush timer and staleness be driven at
//! controlled times in a test. One `SystemTime::now()` in a helper below
//! silently removes that for whatever path it is on — and the test still
//! passes, so nothing says so.
//!
//! It acquires a second consequence later: a venue that signs its requests puts
//! a timestamp in the signature, so clock skew becomes a rejected request
//! rather than a flaky test.

use std::sync::atomic::{AtomicI64, Ordering};

/// A source of the current moment, in microseconds since the epoch.
pub trait Clock: Send + Sync {
    /// Now.
    fn now_micros(&self) -> i64;
}

/// The real one. **The only place in this crate permitted to read the system
/// time.**
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_micros(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_micros() as i64
    }
}

/// A clock that advances only when told.
///
/// Every timing invariant in this system is asserted by driving one of these:
/// a rotation at ten minutes, a flush at two seconds and a staleness window at
/// a minute are all one call rather than a wait.
#[derive(Debug)]
pub struct TestClock(AtomicI64);

impl TestClock {
    /// A clock starting at a moment.
    pub fn at(micros: i64) -> TestClock {
        TestClock(AtomicI64::new(micros))
    }

    /// Move it forward.
    pub fn advance(&self, micros: i64) {
        self.0.fetch_add(micros, Ordering::SeqCst);
    }

    /// Move it forward by whole seconds, which is what most of these tests
    /// mean.
    pub fn advance_secs(&self, secs: i64) {
        self.advance(secs * 1_000_000);
    }

    /// Put it somewhere.
    pub fn set(&self, micros: i64) {
        self.0.store(micros, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_micros(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_test_clock_advances_only_when_told() {
        let clock = TestClock::at(1_000);
        assert_eq!(clock.now_micros(), 1_000);
        // No wall-clock time passes here, and none is needed.
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(clock.now_micros(), 1_000);

        clock.advance_secs(10);
        assert_eq!(clock.now_micros(), 10_001_000);
        clock.set(0);
        assert_eq!(clock.now_micros(), 0);
    }

    #[test]
    fn the_system_clock_is_after_the_epoch() {
        assert!(SystemClock.now_micros() > 1_700_000_000_000_000);
    }
}
