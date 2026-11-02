//! The idempotency keys a tenant's publishes have used.
//!
//! Iggy deduplicates a client's retried requests and not message ids, so a
//! producer that sends again after a timeout would put the event on the bus
//! twice. A publish that carries an `idempotencykey` is remembered, per tenant
//! and topic, with where the first publish landed (offset, id and principal,
//! never its body). A repeat reads the envelope back from the topic. A second publish with the same key inside
//! the window ([`crate::WorkSettings::idempotency_window`]) is answered with
//! that first answer and publishes nothing.
//!
//! # Bound
//!
//! A tenant remembers at most
//! [`crate::WorkSettings::idempotency_max_keys`] keys. When one more would
//! pass the bound, the key that was remembered longest ago is forgotten, so a
//! key can be forgotten before its window ends, and never remembered after it.
//! Keys past their window are dropped on every timer tick.
//!
//! # Durability
//!
//! Each remembered key is also a record in the broker's log of the server's
//! own state ([`mansio_core::SystemLog::Idempotency`]), written after the
//! message is stored. A restarted server reads the log once, a page at a
//! time, keeping for each tenant the newest records that are still inside the
//! window and the bound, and answers as the server before it did. The read is
//! made when the server starts, not inside a request. The log is created with
//! an expiry equal to the window (fixed when the log is first made), so the
//! broker drops records once their window has passed and the log stays about a
//! window's worth of keyed publishes long.
//!
//! A record that cannot be written does not fail a publish that is already
//! stored: the key is kept in memory and is lost at the next restart. A
//! record that cannot be read is skipped with a warning.

use std::collections::{BTreeMap, HashMap};

use mansio_core::api::Published;
use mansio_core::{EventId, IdempotencyKey, Principal, TenantId, Topic};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::records;

type Slot = (Topic, IdempotencyKey);

/// Where the first publish with a key landed: enough to read it back from the
/// topic, and no message body, so that what a tenant remembers is a few
/// hundred bytes a key whatever it published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct First {
    /// The offset in the topic, if the broker said.
    pub(crate) offset: Option<u64>,
    /// The id of the envelope stored.
    pub(crate) id: EventId,
    /// Who published it.
    pub(crate) principal: Principal,
}

impl First {
    pub(crate) fn of(published: &Published) -> Self {
        Self {
            offset: published.offset,
            id: published.envelope.id,
            principal: published.envelope.principal.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct Remembered {
    seq: u64,
    /// When the publish was accepted, in milliseconds since the Unix epoch.
    at: i64,
    first: First,
}

/// The keys of one tenant, oldest first by when they were remembered.
#[derive(Debug, Default)]
pub(crate) struct Keys {
    by_slot: HashMap<Slot, Remembered>,
    order: BTreeMap<u64, Slot>,
    next: u64,
}

fn expired(at: i64, now: i64, window: i64) -> bool {
    now.saturating_sub(at) >= window
}

impl Keys {
    /// What the first publish with this key got, if it was accepted inside the
    /// window.
    pub(crate) fn get(
        &self,
        topic: &Topic,
        key: &IdempotencyKey,
        now: i64,
        window: i64,
    ) -> Option<&First> {
        self.by_slot
            .get(&(topic.clone(), key.clone()))
            .filter(|known| !expired(known.at, now, window))
            .map(|known| &known.first)
    }

    /// Remember a publish accepted at `at`. A key that was remembered before
    /// (and so was past its window) is replaced. Keys past their window are
    /// dropped, and then the oldest keys until no more than `max` are left.
    pub(crate) fn insert(
        &mut self,
        (topic, key): Slot,
        at: i64,
        first: First,
        now: i64,
        window: i64,
        max: usize,
    ) {
        let slot = (topic, key);
        if let Some(old) = self.by_slot.remove(&slot) {
            self.order.remove(&old.seq);
        }
        let seq = self.next;
        self.next = self.next.saturating_add(1);
        self.order.insert(seq, slot.clone());
        self.by_slot.insert(slot, Remembered { seq, at, first });
        while let Some((&seq, slot)) = self.order.first_key_value() {
            let drop = self.by_slot.len() > max
                || self
                    .by_slot
                    .get(slot)
                    .is_none_or(|known| expired(known.at, now, window));
            if !drop {
                break;
            }
            let slot = slot.clone();
            self.order.remove(&seq);
            self.by_slot.remove(&slot);
        }
    }

    /// Drop the keys past their window.
    pub(crate) fn prune(&mut self, now: i64, window: i64) {
        while let Some((&seq, slot)) = self.order.first_key_value() {
            if !self
                .by_slot
                .get(slot)
                .is_none_or(|known| expired(known.at, now, window))
            {
                break;
            }
            let slot = slot.clone();
            self.order.remove(&seq);
            self.by_slot.remove(&slot);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_slot.len()
    }
}

/// One record of the log: a key remembered.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum Entry {
    Seen {
        tenant: TenantId,
        topic: Topic,
        key: IdempotencyKey,
        /// When the publish was accepted, in milliseconds since the Unix epoch.
        at: i64,
        first: First,
    },
}

impl Entry {
    pub(crate) fn seen(
        tenant: TenantId,
        key: IdempotencyKey,
        at: i64,
        published: &Published,
    ) -> Self {
        Entry::Seen {
            tenant,
            topic: published.envelope.topic.clone(),
            key,
            at,
            first: First::of(published),
        }
    }
}

/// Add the records of one page of the log, in order, to the keys of each
/// tenant. Records outside the window are skipped and the keys evict by the
/// bound as they are added, so a long log is never held whole.
pub(crate) fn replay_into(
    tenants: &mut HashMap<TenantId, Keys>,
    log: &[mansio_core::SystemRecord],
    now: i64,
    window: i64,
    max: usize,
) {
    for record in log {
        match records::decode::<Entry>(&record.payload) {
            Ok(Entry::Seen {
                tenant,
                topic,
                key,
                at,
                first,
            }) => {
                if expired(at, now, window) {
                    continue;
                }
                tenants.entry(tenant).or_default().insert(
                    (topic, key),
                    at,
                    first,
                    now,
                    window,
                    max,
                );
            }
            Err(reason) => warn!(
                offset = record.offset,
                %reason,
                "an idempotency record could not be read and was left out"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use mansio_core::{Envelope, EventId};

    use super::*;

    fn topic() -> Topic {
        "facts.build".parse().unwrap()
    }

    fn key(n: u32) -> IdempotencyKey {
        format!("k{n}").parse().unwrap()
    }

    fn full(offset: u64) -> Published {
        Published {
            offset: Some(offset),
            envelope: Envelope::new(
                EventId::generate(),
                "test".parse().unwrap(),
                "t".parse().unwrap(),
                "acme".parse().unwrap(),
                topic(),
                "service:test".parse().unwrap(),
            ),
        }
    }

    fn published(offset: u64) -> First {
        First::of(&full(offset))
    }

    #[test]
    fn a_key_answers_inside_its_window_and_not_after() {
        let mut keys = Keys::default();
        let first = published(0);
        keys.insert((topic(), key(1)), 1_000, first.clone(), 1_000, 500, 10);
        assert_eq!(keys.get(&topic(), &key(1), 1_499, 500), Some(&first));
        assert_eq!(keys.get(&topic(), &key(1), 1_500, 500), None);
        assert_eq!(keys.get(&topic(), &key(2), 1_000, 500), None);
        let other: Topic = "facts.deploy".parse().unwrap();
        assert_eq!(keys.get(&other, &key(1), 1_000, 500), None);
    }

    #[test]
    fn the_oldest_key_is_forgotten_when_the_bound_is_passed() {
        let mut keys = Keys::default();
        for n in 0..5 {
            keys.insert((topic(), key(n)), 1_000, published(0), 1_000, 10_000, 3);
        }
        assert_eq!(keys.len(), 3);
        assert!(keys.get(&topic(), &key(0), 1_000, 10_000).is_none());
        assert!(keys.get(&topic(), &key(1), 1_000, 10_000).is_none());
        for n in 2..5 {
            assert!(keys.get(&topic(), &key(n), 1_000, 10_000).is_some());
        }
    }

    #[test]
    fn keys_past_their_window_are_dropped_on_the_next_insert() {
        let mut keys = Keys::default();
        keys.insert((topic(), key(1)), 0, published(0), 0, 100, 10);
        keys.insert((topic(), key(2)), 50, published(1), 50, 100, 10);
        keys.insert((topic(), key(3)), 120, published(2), 120, 100, 10);
        assert_eq!(keys.len(), 2);
        // A key past its window is replaced by the next publish with it.
        let again = published(3);
        keys.insert((topic(), key(2)), 200, again.clone(), 200, 100, 10);
        assert_eq!(keys.get(&topic(), &key(2), 200, 100), Some(&again));
        assert_eq!(keys.len(), 2);
        keys.insert((topic(), key(4)), 300, published(4), 300, 100, 10);
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn replay_keeps_the_newest_inside_the_window_and_the_bound() {
        let tenant: TenantId = "acme".parse().unwrap();
        let mut log = Vec::new();
        for (offset, (n, at)) in [(1, 0), (2, 900), (3, 950), (4, 990)]
            .into_iter()
            .enumerate()
        {
            let entry = Entry::seen(tenant.clone(), key(n), at, &full(offset as u64));
            log.push(mansio_core::SystemRecord {
                offset: offset as u64,
                payload: records::encode(&entry).unwrap(),
            });
        }
        log.push(mansio_core::SystemRecord {
            offset: 4,
            payload: b"not a record".to_vec(),
        });
        let mut tenants = HashMap::new();
        let (first, rest) = log.split_at(2);
        replay_into(&mut tenants, first, 1_000, 200, 2);
        replay_into(&mut tenants, rest, 1_000, 200, 2);
        let keys = tenants.get(&tenant).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.get(&topic(), &key(1), 1_000, 200).is_none());
        assert!(keys.get(&topic(), &key(2), 1_000, 200).is_none());
        assert!(keys.get(&topic(), &key(3), 1_000, 200).is_some());
        assert!(keys.get(&topic(), &key(4), 1_000, 200).is_some());
    }

    #[test]
    fn a_remembered_key_holds_no_message_body() {
        // The most the server accepts in one body is large; what a key holds
        // must not grow with it, in memory or in the log.
        let mut big = full(0);
        big.envelope.data = Some(serde_json::json!({ "blob": "x".repeat(1_000_000) }));
        let first = First::of(&big);
        let entry = Entry::seen("acme".parse().unwrap(), key(1), 0, &big);
        let written = records::encode(&entry).unwrap();
        assert!(written.len() < 1_000, "a record is {} bytes", written.len());
        assert!(std::mem::size_of_val(&first) < 1_000);
        let json = serde_json::to_string(&first).unwrap();
        assert!(!json.contains("blob"));
    }
}
