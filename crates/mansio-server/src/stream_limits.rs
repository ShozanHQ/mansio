//! How many event streams are open at once.
//!
//! Each open stream looks at the broker every `MANSIO_STREAM_POLL_MILLIS`, so
//! the count is bounded: per credential (`MANSIO_SSE_MAX_PER_CREDENTIAL`, 429
//! past it) and for the whole server (`MANSIO_SSE_MAX`, 503 past it). A
//! [`StreamPermit`] is one open stream; dropping it, when the client goes away
//! or the stream ends, gives the place back.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use mansio_core::PrincipalId;

use crate::auth::Actor;
use crate::error::ApiError;

/// Whose streams are counted together: one principal's credential, or the
/// bootstrap token, which has no principal id.
type Holder = Option<PrincipalId>;

#[derive(Debug, Default)]
struct Counts {
    total: usize,
    by_holder: HashMap<Holder, usize>,
}

/// The open event streams, and the limits on them.
#[derive(Debug)]
pub(crate) struct StreamLimiter {
    per_credential: usize,
    total: usize,
    counts: Mutex<Counts>,
}

impl StreamLimiter {
    pub(crate) fn new(per_credential: usize, total: usize) -> Arc<Self> {
        Arc::new(Self {
            per_credential,
            total,
            counts: Mutex::new(Counts::default()),
        })
    }

    fn counts(&self) -> MutexGuard<'_, Counts> {
        self.counts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Take a place for a stream of `actor`'s.
    ///
    /// # Errors
    ///
    /// 429 if the credential holds `MANSIO_SSE_MAX_PER_CREDENTIAL` streams
    /// already, else 503 if the server holds `MANSIO_SSE_MAX`.
    pub(crate) fn acquire(self: &Arc<Self>, actor: &Actor) -> Result<StreamPermit, ApiError> {
        let holder = actor.id();
        let mut counts = self.counts();
        if counts.by_holder.get(&holder).copied().unwrap_or(0) >= self.per_credential {
            return Err(ApiError::TooManyRequests(format!(
                "this credential already holds {} open event streams \
                 (MANSIO_SSE_MAX_PER_CREDENTIAL)",
                self.per_credential
            )));
        }
        if counts.total >= self.total {
            return Err(ApiError::Unavailable(format!(
                "the server already holds {} open event streams (MANSIO_SSE_MAX)",
                self.total
            )));
        }
        counts.total += 1;
        *counts.by_holder.entry(holder).or_insert(0) += 1;
        Ok(StreamPermit {
            limiter: Arc::clone(self),
            holder,
        })
    }
}

/// One open event stream. Dropping it gives the place back.
#[derive(Debug)]
pub(crate) struct StreamPermit {
    limiter: Arc<StreamLimiter>,
    holder: Holder,
}

impl Drop for StreamPermit {
    fn drop(&mut self) {
        let mut counts = self.limiter.counts();
        counts.total = counts.total.saturating_sub(1);
        if let Some(open) = counts.by_holder.get_mut(&self.holder) {
            *open = open.saturating_sub(1);
            if *open == 0 {
                counts.by_holder.remove(&self.holder);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin() -> Actor {
        Actor::admin("service:mansio".parse().unwrap())
    }

    #[test]
    fn a_holder_past_its_limit_is_refused_with_429_and_a_dropped_permit_frees_a_place() {
        let limiter = StreamLimiter::new(2, 10);
        let first = limiter.acquire(&admin()).unwrap();
        let _second = limiter.acquire(&admin()).unwrap();
        assert!(matches!(
            limiter.acquire(&admin()),
            Err(ApiError::TooManyRequests(_))
        ));
        drop(first);
        assert!(limiter.acquire(&admin()).is_ok());
    }

    #[test]
    fn the_server_past_its_total_is_refused_with_503() {
        let limiter = StreamLimiter::new(5, 1);
        let _held = limiter.acquire(&admin()).unwrap();
        assert!(matches!(
            limiter.acquire(&admin()),
            Err(ApiError::Unavailable(_))
        ));
    }
}
