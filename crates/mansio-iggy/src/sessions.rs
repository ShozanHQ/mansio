//! The clients signed in with a principal's credential.
//!
//! A request made for a credential goes over a client signed in as the user
//! the credential belongs to, so Iggy decides what it may do. A client answers
//! one request at a time, so a credential keeps a small pool of them: a
//! request takes an idle one, and a new client is signed in while all are busy
//! and the pool has room. The pool is closed when the user is deleted, when
//! Iggy stops accepting the credential, or when the credential has not been
//! used for the idle time. A table of them holds digests of credentials, not
//! the credentials.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use iggy::prelude::IggyClient;
use mansio_core::{BrokerUser, CredentialDigest};

/// A client signed in as a credential's user.
#[derive(Debug, Clone)]
pub(crate) struct Session {
    pub(crate) client: Arc<IggyClient>,
    pub(crate) user: BrokerUser,
}

/// A client taken for one request. While it is held the client counts as busy,
/// so that a second request for the same credential goes to another client.
#[derive(Debug)]
pub(crate) struct Lease {
    client: Arc<IggyClient>,
    busy: Arc<AtomicUsize>,
}

impl Deref for Lease {
    type Target = IggyClient;

    fn deref(&self) -> &IggyClient {
        &self.client
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.busy.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct Slot {
    client: Arc<IggyClient>,
    busy: Arc<AtomicUsize>,
}

impl Slot {
    fn new(client: Arc<IggyClient>) -> Self {
        Self {
            client,
            busy: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn lease(&self) -> Lease {
        self.busy.fetch_add(1, Ordering::AcqRel);
        Lease {
            client: Arc::clone(&self.client),
            busy: Arc::clone(&self.busy),
        }
    }

    fn is_busy(&self) -> bool {
        self.busy.load(Ordering::Acquire) > 0
    }
}

#[derive(Debug)]
struct Pool {
    user: BrokerUser,
    slots: Vec<Slot>,
    last_used: Instant,
}

impl Pool {
    fn new(user: BrokerUser, now: Instant) -> Self {
        Self {
            user,
            slots: Vec::new(),
            last_used: now,
        }
    }

    /// The slot with the fewest requests on it.
    fn least_busy(&self) -> Option<&Slot> {
        self.slots
            .iter()
            .min_by_key(|slot| slot.busy.load(Ordering::Acquire))
    }

    fn into_clients(self) -> impl Iterator<Item = Arc<IggyClient>> {
        self.slots.into_iter().map(|slot| slot.client)
    }
}

/// What asking for a credential's client came to.
#[derive(Debug)]
pub(crate) enum Checkout {
    /// A kept client: an idle one, or the least busy when the pool is full.
    Ready(Lease, BrokerUser),
    /// None is kept, or every kept client is busy and the pool has room: a new
    /// client should be signed in and passed to [`Sessions::add`].
    SignIn,
}

#[derive(Debug, Default)]
pub(crate) struct Sessions {
    open: Mutex<HashMap<CredentialDigest, Pool>>,
}

impl Sessions {
    fn lock(&self) -> MutexGuard<'_, HashMap<CredentialDigest, Pool>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Take a client of the credential for a request.
    pub(crate) fn checkout(&self, key: &CredentialDigest, max: usize, now: Instant) -> Checkout {
        let mut open = self.lock();
        let Some(pool) = open.get_mut(key) else {
            return Checkout::SignIn;
        };
        let Some(slot) = pool.least_busy() else {
            return Checkout::SignIn;
        };
        if slot.is_busy() && pool.slots.len() < max {
            return Checkout::SignIn;
        }
        let lease = slot.lease();
        let user = pool.user;
        pool.last_used = now;
        Checkout::Ready(lease, user)
    }

    /// The user a credential is signed in as, if it has a kept client.
    pub(crate) fn user(&self, key: &CredentialDigest) -> Option<BrokerUser> {
        self.lock().get(key).map(|pool| pool.user)
    }

    /// Keep the session for a request and lease it. If the pool is already
    /// full, because other requests signed in first, the session is not kept
    /// and is returned to be ended, and the lease is on the least busy client
    /// that is kept.
    pub(crate) fn add(
        &self,
        key: CredentialDigest,
        session: Session,
        max: usize,
        now: Instant,
    ) -> (Lease, BrokerUser, Option<Session>) {
        let mut open = self.lock();
        let pool = open
            .entry(key)
            .or_insert_with(|| Pool::new(session.user, now));
        pool.last_used = now;
        let user = pool.user;
        if pool.slots.len() >= max.max(1)
            && let Some(slot) = pool.least_busy()
        {
            return (slot.lease(), user, Some(session));
        }
        let slot = Slot::new(Arc::clone(&session.client));
        let lease = slot.lease();
        pool.slots.push(slot);
        (lease, user, None)
    }

    /// Keep a client that signed in again to check the credential, in place
    /// of an idle one when there is one. Only when every kept client is busy
    /// and the pool has room is it added as one more. Returns the clients to
    /// end: the old one that was replaced, or the new one if the pool is full
    /// of busy ones.
    pub(crate) fn refresh(
        &self,
        key: CredentialDigest,
        session: Session,
        max: usize,
        now: Instant,
    ) -> Vec<Arc<IggyClient>> {
        let mut open = self.lock();
        let pool = open
            .entry(key)
            .or_insert_with(|| Pool::new(session.user, now));
        pool.last_used = now;
        let slot = Slot::new(session.client);
        if let Some(index) = pool.slots.iter().position(|old| !old.is_busy()) {
            return vec![std::mem::replace(&mut pool.slots[index], slot).client];
        }
        if pool.slots.len() < max.max(1) {
            pool.slots.push(slot);
            return Vec::new();
        }
        vec![slot.client]
    }

    /// Take every client of the credential out of the table.
    pub(crate) fn remove(&self, key: &CredentialDigest) -> Vec<Arc<IggyClient>> {
        self.lock()
            .remove(key)
            .map(|pool| pool.into_clients().collect())
            .unwrap_or_default()
    }

    /// Take every client of a user out of the table.
    pub(crate) fn remove_user(&self, user: BrokerUser) -> Vec<Arc<IggyClient>> {
        let mut open = self.lock();
        let keys: Vec<CredentialDigest> = open
            .iter()
            .filter(|(_, pool)| pool.user == user)
            .map(|(key, _)| *key)
            .collect();
        keys.iter()
            .filter_map(|key| open.remove(key))
            .flat_map(Pool::into_clients)
            .collect()
    }

    /// Take out the clients of every credential that has not been used for
    /// longer than `idle` as of `now`, unless a request is still on one.
    pub(crate) fn evict_idle(&self, now: Instant, idle: Duration) -> Vec<Arc<IggyClient>> {
        let mut open = self.lock();
        let keys: Vec<CredentialDigest> = open
            .iter()
            .filter(|(_, pool)| {
                now.saturating_duration_since(pool.last_used) > idle
                    && !pool.slots.iter().any(Slot::is_busy)
            })
            .map(|(key, _)| *key)
            .collect();
        keys.iter()
            .filter_map(|key| open.remove(key))
            .flat_map(Pool::into_clients)
            .collect()
    }

    /// Take every client out of the table.
    pub(crate) fn drain(&self) -> Vec<Arc<IggyClient>> {
        self.lock()
            .drain()
            .flat_map(|(_, pool)| pool.into_clients())
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().values().map(|pool| pool.slots.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use iggy::prelude::IggyClientBuilder;
    use mansio_core::Credential;

    use super::*;

    /// A client that is built and not connected.
    fn session(user: u32) -> Session {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address("127.0.0.1:1".to_owned())
            .build()
            .unwrap();
        Session {
            client: Arc::new(client),
            user: BrokerUser(user),
        }
    }

    fn key(token: &str) -> CredentialDigest {
        Credential::new(token).digest()
    }

    #[test]
    fn a_busy_client_sends_the_next_request_to_a_new_one_until_the_pool_is_full() {
        let sessions = Sessions::default();
        let now = Instant::now();
        assert!(matches!(
            sessions.checkout(&key("a"), 2, now),
            Checkout::SignIn
        ));

        let (first, _, spare) = sessions.add(key("a"), session(1), 2, now);
        assert!(spare.is_none());
        // The first is busy and the pool has room.
        assert!(matches!(
            sessions.checkout(&key("a"), 2, now),
            Checkout::SignIn
        ));
        let (second, _, spare) = sessions.add(key("a"), session(1), 2, now);
        assert!(spare.is_none());
        assert!(!Arc::ptr_eq(&first.client, &second.client));

        // Full: a third request shares a client, and a session signed in by
        // a request that lost the race is handed back.
        let Checkout::Ready(third, user) = sessions.checkout(&key("a"), 2, now) else {
            panic!("the pool is full");
        };
        assert_eq!(user, BrokerUser(1));
        let (extra, _, spare) = sessions.add(key("a"), session(1), 2, now);
        assert!(spare.is_some());
        assert_eq!(sessions.len(), 2);

        // Once released, an idle client is taken instead of signing in.
        drop((first, second, third, extra));
        assert!(matches!(
            sessions.checkout(&key("a"), 2, now),
            Checkout::Ready(..)
        ));
    }

    /// SHO-10032: a long poll holds a lease; the next request for the same
    /// credential is not queued behind it when the pool has room, and is when
    /// the pool is one client.
    #[test]
    fn a_request_is_not_queued_behind_a_held_lease_while_the_pool_has_room() {
        let sessions = Sessions::default();
        let now = Instant::now();
        let (held, _, _) = sessions.add(key("a"), session(1), 2, now);

        assert!(matches!(
            sessions.checkout(&key("a"), 2, now),
            Checkout::SignIn
        ));
        let (next, _, spare) = sessions.add(key("a"), session(1), 2, now);
        assert!(spare.is_none());
        assert!(!Arc::ptr_eq(&held.client, &next.client));

        // A pool of one is the old behaviour: the request shares the client.
        let (single, _, _) = sessions.add(key("b"), session(2), 1, now);
        let Checkout::Ready(shared, _) = sessions.checkout(&key("b"), 1, now) else {
            panic!("a full pool shares");
        };
        assert!(Arc::ptr_eq(&single.client, &shared.client));
    }

    #[test]
    fn a_session_idle_for_longer_than_the_setting_is_closed_and_a_busy_one_is_not() {
        let sessions = Sessions::default();
        let start = Instant::now();
        let idle = Duration::from_secs(300);
        let (held, _, _) = sessions.add(key("a"), session(1), 2, start);
        drop(sessions.add(key("b"), session(2), 2, start));

        let later = start + idle + Duration::from_secs(1);
        // Not yet idle for longer than the setting.
        assert!(sessions.evict_idle(start + idle, idle).is_empty());
        // "a" has a request on it; "b" goes.
        assert_eq!(sessions.evict_idle(later, idle).len(), 1);
        assert!(sessions.user(&key("b")).is_none());
        assert!(sessions.user(&key("a")).is_some());

        // Used again, it is idle from then.
        drop(held);
        assert!(matches!(
            sessions.checkout(&key("a"), 2, later),
            Checkout::Ready(..)
        ));
        assert!(
            sessions
                .evict_idle(later + Duration::from_secs(1), idle)
                .is_empty()
        );
        assert_eq!(sessions.evict_idle(later + idle * 2, idle).len(), 1);
        // The next request finds nothing kept and signs in again.
        assert!(matches!(
            sessions.checkout(&key("a"), 2, later),
            Checkout::SignIn
        ));
        assert_eq!(sessions.len(), 0);
    }

    #[test]
    fn a_recheck_replaces_an_idle_client_instead_of_adding_one() {
        let sessions = Sessions::default();
        let now = Instant::now();
        drop(sessions.add(key("a"), session(1), 1, now));
        let ended = sessions.refresh(key("a"), session(1), 1, now);
        assert_eq!(ended.len(), 1);
        assert_eq!(sessions.len(), 1);

        // Room in the pool, but the kept client is idle: replaced, not added.
        assert_eq!(sessions.refresh(key("a"), session(1), 2, now).len(), 1);
        assert_eq!(sessions.len(), 1);

        // Every kept client busy and room left: kept as one more.
        let a = sessions.checkout(&key("a"), 2, now);
        assert!(sessions.refresh(key("a"), session(1), 2, now).is_empty());
        assert_eq!(sessions.len(), 2);

        // All busy and full: the checked client is the one ended.
        let b = sessions.checkout(&key("a"), 2, now);
        let a2 = sessions.checkout(&key("a"), 2, now);
        let fresh = session(1);
        let ended = sessions.refresh(key("a"), fresh.clone(), 2, now);
        assert!(Arc::ptr_eq(&ended[0], &fresh.client));
        assert_eq!(sessions.len(), 2);
        drop((a, b, a2));
    }

    #[test]
    fn removing_a_user_takes_all_its_sessions_and_only_those() {
        let sessions = Sessions::default();
        let now = Instant::now();
        drop(sessions.add(key("a"), session(1), 2, now));
        drop(sessions.add(key("b"), session(1), 2, now));
        drop(sessions.add(key("c"), session(2), 2, now));

        assert_eq!(sessions.remove_user(BrokerUser(1)).len(), 2);
        assert!(sessions.user(&key("a")).is_none());
        assert!(sessions.user(&key("b")).is_none());
        assert!(sessions.user(&key("c")).is_some());
        assert_eq!(sessions.remove(&key("c")).len(), 1);
        assert!(sessions.remove(&key("c")).is_empty());
    }

    #[test]
    fn draining_empties_the_table() {
        let sessions = Sessions::default();
        let now = Instant::now();
        drop(sessions.add(key("a"), session(1), 2, now));
        drop(sessions.add(key("c"), session(2), 2, now));
        assert_eq!(sessions.drain().len(), 2);
        assert_eq!(sessions.len(), 0);
    }
}
