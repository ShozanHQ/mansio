//! What the engine holds in memory for one tenant.
//!
//! None of it is authoritative. It is a projection of the tenant's topics
//! (`work.<queue>`, `facts.work`, `dead`, `alerts`, `facts.alert`), and
//! [`crate::recover`] rebuilds it from them on start.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use mansio_core::{
    ConsumerName, Envelope, EventId, ItemId, LeaseFingerprint, LeaseId, Next, Principal, Segment,
};
use subtle::ConstantTimeEq;

use crate::error::ApiError;

/// A lease id the server still has, in memory only. It shows in no log, no
/// fact and no `Debug` output.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Remembered(pub(crate) LeaseId);

impl std::fmt::Debug for Remembered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Remembered(..)")
    }
}

/// The right to work on an item until `expires_at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Lease {
    /// Of the lease id, which only the holder has: the facts say which lease
    /// this is and nothing more.
    pub(crate) fingerprint: LeaseFingerprint,
    /// The lease id itself, while this process has been up since the lease was
    /// given. A consumer that asks again for the lease it holds is told it
    /// again. A lease read back from the facts has none, as the facts do not
    /// say it.
    pub(crate) id: Option<Remembered>,
    pub(crate) holder: Principal,
    pub(crate) consumer: ConsumerName,
    /// Milliseconds since the Unix epoch.
    pub(crate) expires_at: i64,
}

/// What becomes of an item once the fact it owes is recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Then {
    /// It is claimable again.
    Ready,
    /// It is finished and leaves the state.
    Gone,
}

/// Where an item stands.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Phase {
    /// Claimable.
    Ready,
    /// Claimed, until the lease ends.
    Leased(Lease),
    /// Failed or expired, waiting out its backoff. `due` is in milliseconds
    /// since the Unix epoch.
    Backoff { due: i64, next: Next },
    /// Its new message is published, but the fact that says so is not yet
    /// recorded. It is not claimable until it is.
    Owed { fact: Box<Envelope>, then: Then },
}

/// A work item: its current message and where it stands.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Item {
    /// The current version of the item: the message with the highest attempt.
    pub(crate) message: Envelope,
    /// Where the item stands in its queue's publish order. The queue sets it
    /// when the item is added, and again when a new version of it is
    /// published. It is not the offset: the broker does not always say where
    /// a message landed.
    order: u64,
    pub(crate) phase: Phase,
}

impl Item {
    /// The attempt the current message stands for.
    pub(crate) fn attempt(&self) -> u32 {
        self.message.attempt.unwrap_or(1)
    }
}

/// The ids of items that are done. An item is named by the id of its first
/// message, which is a UUID, so an id is kept as the 16 bytes of one; an id
/// that is not a UUID (it can only have come from outside) is kept as text.
#[derive(Debug, Default)]
struct Finished {
    uuids: HashSet<u128>,
    other: HashSet<ItemId>,
}

impl Finished {
    fn uuid(id: &ItemId) -> Option<u128> {
        id.as_str().parse::<EventId>().ok().map(|id| id.as_u128())
    }

    fn insert(&mut self, id: &ItemId) {
        match Self::uuid(id) {
            Some(uuid) => self.uuids.insert(uuid),
            None => self.other.insert(id.clone()),
        };
    }

    fn contains(&self, id: &ItemId) -> bool {
        match Self::uuid(id) {
            Some(uuid) => self.uuids.contains(&uuid),
            None => self.other.contains(id),
        }
    }
}

/// The items of one queue. Open items are kept whole; an item that is done
/// (acknowledged, or dead) leaves only its id, so that the id is not used
/// again.
#[derive(Debug, Default)]
pub(crate) struct Queue {
    items: HashMap<ItemId, Item>,
    finished: Finished,
    /// `(order, id)` of every item in [`Phase::Ready`], so that the oldest
    /// is the first.
    ready: BTreeSet<(u64, ItemId)>,
    /// The order the next addition gets.
    next_order: u64,
}

impl Queue {
    pub(crate) fn get(&self, id: &ItemId) -> Option<&Item> {
        self.items.get(id)
    }

    /// Whether the id belongs to an item, open or done.
    pub(crate) fn knows(&self, id: &ItemId) -> bool {
        self.items.contains_key(id) || self.finished.contains(id)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&ItemId, &Item)> {
        self.items.iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    /// The claimable item that was published first.
    pub(crate) fn oldest_ready(&self) -> Option<&ItemId> {
        self.ready.first().map(|(_, id)| id)
    }

    /// The item `holder` holds under the consumer name `consumer`, if its
    /// lease is still live at `now`, with that lease. If there are several,
    /// the one whose lease ends first. This looks through the open items, which
    /// costs no more than the broker write that a claim makes.
    pub(crate) fn held_by(
        &self,
        holder: &Principal,
        consumer: &ConsumerName,
        now: i64,
    ) -> Option<(&ItemId, &Item, &Lease)> {
        self.items
            .iter()
            .filter_map(|(id, item)| match &item.phase {
                Phase::Leased(lease)
                    if lease.holder == *holder
                        && lease.consumer == *consumer
                        && lease.expires_at > now =>
                {
                    Some((id, item, lease))
                }
                _ => None,
            })
            .min_by(|a, b| (a.2.expires_at, a.0).cmp(&(b.2.expires_at, b.0)))
    }

    fn take_order(&mut self) -> u64 {
        let order = self.next_order;
        self.next_order = self.next_order.saturating_add(1);
        order
    }

    /// Add an item at the end of the queue.
    pub(crate) fn insert(&mut self, id: ItemId, message: Envelope, phase: Phase) {
        self.remove(&id);
        let order = self.take_order();
        if phase == Phase::Ready {
            self.ready.insert((order, id.clone()));
        }
        self.items.insert(
            id,
            Item {
                message,
                order,
                phase,
            },
        );
    }

    fn remove(&mut self, id: &ItemId) {
        if let Some(old) = self.items.remove(id) {
            self.ready.remove(&(old.order, id.clone()));
        }
    }

    /// The item is done: drop it and remember its id.
    pub(crate) fn finish(&mut self, id: &ItemId) {
        self.remove(id);
        self.finished.insert(id);
    }

    pub(crate) fn set_phase(&mut self, id: &ItemId, phase: Phase) {
        let Some(item) = self.items.get_mut(id) else {
            return;
        };
        self.ready.remove(&(item.order, id.clone()));
        if phase == Phase::Ready {
            self.ready.insert((item.order, id.clone()));
        }
        item.phase = phase;
    }

    /// Put a newer version of the item in place, which moves it to the end
    /// of the queue. It becomes claimable only when its phase says so.
    pub(crate) fn set_message(&mut self, id: &ItemId, message: Envelope) {
        let order = self.take_order();
        if let Some(item) = self.items.get_mut(id) {
            self.ready.remove(&(item.order, id.clone()));
            item.message = message;
            item.order = order;
        }
    }
}

/// Everything the engine holds for one tenant.
#[derive(Debug, Default)]
pub(crate) struct TenantState {
    /// Whether the state matches the logs. It stops doing so when a publish
    /// fails, because the broker may have stored the message all the same;
    /// the next use rebuilds it.
    pub(crate) loaded: bool,
    pub(crate) queues: HashMap<Segment, Queue>,
    /// Alerts nobody has acknowledged, with when each was published or last
    /// surfaced, in milliseconds since the Unix epoch. The next surfacing is
    /// one interval later.
    pub(crate) alerts: HashMap<EventId, i64>,
    /// The ids of alerts that were acknowledged, which are all that is kept
    /// of them.
    pub(crate) acknowledged_alerts: HashSet<EventId>,
}

impl TenantState {
    pub(crate) fn queue_mut(&mut self, queue: &Segment) -> &mut Queue {
        self.queues.entry(queue.clone()).or_default()
    }
}

/// The wait after attempt `attempt` fails: `base * 2^(attempt-1)`, and never
/// more than `cap`.
pub(crate) fn backoff(attempt: u32, base: Duration, cap: Duration) -> Duration {
    let doublings = attempt.saturating_sub(1).min(31);
    base.checked_mul(1 << doublings)
        .map_or(cap, |wait| wait.min(cap))
}

/// Who says they hold a lease: the principal behind the credential, the
/// consumer name it consumes under, and the lease id it was given. The three
/// together are the holder.
#[derive(Debug, Clone, Copy)]
pub struct Holder<'a> {
    /// The principal behind the credential.
    pub principal: &'a Principal,
    /// The consumer name the lease was taken under.
    pub consumer: &'a ConsumerName,
    /// The lease id the holder was given.
    pub lease_id: &'a LeaseId,
}

/// Whether `holder` may act on `item`, and the lease it holds if so.
///
/// A lease that someone else holds, or that is not the one presented, is a
/// 403; an item with no live lease for the caller is a 409. Neither changes
/// anything.
pub(crate) fn held_lease<'a>(
    item: &'a Item,
    holder: Holder<'_>,
    now: i64,
) -> Result<&'a Lease, ApiError> {
    let Phase::Leased(lease) = &item.phase else {
        return Err(ApiError::Conflict(
            "the item is not claimed: its lease has ended or it was never claimed".to_owned(),
        ));
    };
    // The lease id is a secret the holder was given: what is kept is a
    // fingerprint of it, and the fingerprint of what is presented is compared
    // so that the time taken does not say how much of a guess was right.
    let same_lease = bool::from(
        lease
            .fingerprint
            .as_bytes()
            .ct_eq(holder.lease_id.fingerprint().as_bytes()),
    );
    if !same_lease || lease.holder != *holder.principal || lease.consumer != *holder.consumer {
        return Err(ApiError::Forbidden(
            "the item is claimed under a lease that is not yours".to_owned(),
        ));
    }
    if lease.expires_at <= now {
        return Err(ApiError::Conflict("the lease has expired".to_owned()));
    }
    Ok(lease)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn backoff_doubles_from_the_base_and_stops_at_the_cap() {
        let waits: Vec<u64> = (1..=8)
            .map(|attempt| backoff(attempt, secs(5), secs(100)).as_secs())
            .collect();
        assert_eq!(waits, [5, 10, 20, 40, 80, 100, 100, 100]);
    }

    #[test]
    fn backoff_does_not_overflow() {
        assert_eq!(backoff(u32::MAX, secs(5), secs(300)), secs(300));
        assert_eq!(backoff(40, Duration::MAX, secs(300)), secs(300));
        assert_eq!(backoff(0, secs(5), secs(300)), secs(5));
    }

    fn message(n: u64) -> (ItemId, Envelope) {
        let id: ItemId = format!("item-{n}").parse().unwrap();
        let mut message = Envelope::new(
            EventId::generate(),
            "test".parse().unwrap(),
            "t".parse().unwrap(),
            "acme".parse().unwrap(),
            "work.builds".parse().unwrap(),
            "service:test".parse().unwrap(),
        );
        message.item_id = Some(id.clone());
        message.attempt = Some(1);
        (id, message)
    }

    fn item(phase: Phase) -> Item {
        let mut queue = Queue::default();
        let (id, message) = message(0);
        queue.insert(id.clone(), message, phase);
        queue.get(&id).cloned().unwrap()
    }

    #[test]
    fn the_oldest_ready_item_is_first_and_phases_move_it_in_and_out() {
        let mut queue = Queue::default();
        let (a, message_a) = message(1);
        let (b, message_b) = message(2);
        let (c, message_c) = message(3);
        queue.insert(a.clone(), message_a, Phase::Ready);
        queue.insert(b.clone(), message_b, Phase::Ready);
        queue.insert(
            c.clone(),
            message_c,
            Phase::Backoff {
                due: 0,
                next: Next::Retry,
            },
        );
        assert_eq!(queue.oldest_ready(), Some(&a));

        let lease = Lease {
            fingerprint: LeaseId::generate().fingerprint(),
            id: None,
            holder: "service:test".parse().unwrap(),
            consumer: "w".parse().unwrap(),
            expires_at: 10,
        };
        queue.set_phase(&a, Phase::Leased(lease));
        assert_eq!(queue.oldest_ready(), Some(&b));
        queue.set_phase(&c, Phase::Ready);
        assert_eq!(queue.oldest_ready(), Some(&b));
        queue.remove(&b);
        assert_eq!(queue.oldest_ready(), Some(&c));
        queue.remove(&c);
        assert_eq!(queue.oldest_ready(), None);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn a_finished_item_leaves_its_id_and_nothing_else() {
        let mut queue = Queue::default();
        let (a, message_a) = message(1);
        let (b, _) = message(2);
        queue.insert(a.clone(), message_a, Phase::Ready);
        assert!(queue.knows(&a) && !queue.knows(&b));
        queue.finish(&a);
        assert!(queue.knows(&a), "an id is not used again");
        assert!(queue.get(&a).is_none());
        assert_eq!(queue.oldest_ready(), None);
        assert_eq!(queue.len(), 0);
        // Finishing what is not there still reserves the id.
        queue.finish(&b);
        assert!(queue.knows(&b));
    }

    #[test]
    fn the_ids_of_finished_items_are_kept_as_sixteen_bytes() {
        let mut queue = Queue::default();
        let uuid = EventId::generate();
        let id: ItemId = uuid.to_string().parse().unwrap();
        queue.finish(&id);
        assert!(queue.knows(&id));
        assert!(queue.finished.uuids.contains(&uuid.as_u128()));
        assert!(queue.finished.other.is_empty());
        assert!(!queue.knows(&EventId::generate().to_string().parse().unwrap()));

        // An id from outside that is not a UUID is still remembered.
        let odd: ItemId = "build-7".parse().unwrap();
        assert!(!queue.knows(&odd));
        queue.finish(&odd);
        assert!(queue.knows(&odd));
        assert_eq!(queue.finished.other.len(), 1);
    }

    #[test]
    fn a_new_version_of_an_item_goes_to_the_end_of_the_queue() {
        let mut queue = Queue::default();
        let (a, message_a) = message(1);
        let (b, message_b) = message(2);
        queue.insert(a.clone(), message_a, Phase::Ready);
        queue.insert(b.clone(), message_b, Phase::Ready);
        queue.set_phase(
            &a,
            Phase::Backoff {
                due: 0,
                next: Next::Retry,
            },
        );
        let (_, newer) = message(1);
        queue.set_message(&a, newer);
        queue.set_phase(&a, Phase::Ready);
        assert_eq!(queue.oldest_ready(), Some(&b));
        queue.remove(&b);
        assert_eq!(queue.oldest_ready(), Some(&a));
    }

    #[test]
    fn only_the_holder_of_a_live_lease_may_act() {
        let holder: Principal = "agent:one".parse().unwrap();
        let consumer: ConsumerName = "w".parse().unwrap();
        let id = LeaseId::generate();
        let lease = Lease {
            fingerprint: id.fingerprint(),
            id: None,
            holder: holder.clone(),
            consumer: consumer.clone(),
            expires_at: 1000,
        };
        let leased = item(Phase::Leased(lease));
        let who = |principal, consumer, lease_id| Holder {
            principal,
            consumer,
            lease_id,
        };
        assert!(held_lease(&leased, who(&holder, &consumer, &id), 999).is_ok());

        let other: Principal = "agent:two".parse().unwrap();
        assert!(matches!(
            held_lease(&leased, who(&other, &consumer, &id), 999),
            Err(ApiError::Forbidden(_))
        ));
        let stranger = LeaseId::generate();
        assert!(matches!(
            held_lease(&leased, who(&holder, &consumer, &stranger), 999),
            Err(ApiError::Forbidden(_))
        ));
        // Another consumer of the same principal, with the right lease id, is
        // not the holder either.
        let other_consumer: ConsumerName = "w2".parse().unwrap();
        assert!(matches!(
            held_lease(&leased, who(&holder, &other_consumer, &id), 999),
            Err(ApiError::Forbidden(_))
        ));
        assert!(matches!(
            held_lease(&leased, who(&holder, &consumer, &id), 1000),
            Err(ApiError::Conflict(_))
        ));

        let ready = item(Phase::Ready);
        assert!(matches!(
            held_lease(&ready, who(&holder, &consumer, &id), 0),
            Err(ApiError::Conflict(_))
        ));
    }
}
