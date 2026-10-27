//! The clock the engine reads, so tests can move time by hand.

use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};

/// A source of the current time, in milliseconds since the Unix epoch.
///
/// Leases and backoffs are written to the log as absolute times, so a
/// restarted server picks them up where they stood. That needs wall-clock
/// time and not a monotonic clock.
pub trait Clock: Send + Sync + Debug {
    /// Milliseconds since the Unix epoch.
    fn now_millis(&self) -> i64;
}

/// The system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> i64 {
        // A clock set before 1970 reads as the epoch.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| {
                i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
            })
    }
}

#[cfg(test)]
pub(crate) mod manual {
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;

    use super::Clock;

    /// A clock that moves only when told to.
    #[derive(Debug)]
    pub(crate) struct ManualClock(AtomicI64);

    impl ManualClock {
        pub(crate) fn new(start_millis: i64) -> Self {
            Self(AtomicI64::new(start_millis))
        }

        pub(crate) fn advance(&self, by: Duration) {
            let millis = i64::try_from(by.as_millis()).unwrap_or(i64::MAX);
            self.0.fetch_add(millis, Ordering::SeqCst);
        }
    }

    impl Clock for ManualClock {
        fn now_millis(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_system_clock_is_after_2026() {
        assert!(SystemClock.now_millis() > 1_767_225_600_000);
    }
}
