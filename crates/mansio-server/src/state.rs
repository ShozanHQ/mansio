//! What the engine holds in memory for one tenant.
//!
//! None of it is authoritative. It is a projection of the tenant's topics
//! (`work.<queue>`, `facts.work`, `dead`, `alerts`, `facts.alert`), and
//! [`crate::recover`] rebuilds it from them on start.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use mansio_core::{
    ConsumerName, Envelope, EventId, ItemId, LeaseFingerprint, LeaseId, Next, Principal, Segment,
    Topic,
};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::error::ApiError;
use crate::idempotency::Keys;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Lease {
    /// Of the lease id, which only the holder has: the facts say which lease
    /// this is and nothing more.
    pub(crate) fingerprint: LeaseFingerprint,
    /// The lease id itself, while this process has been up since the lease was
    /// given. A consumer that asks again for the lease it holds is told it
    /// again. A lease read back from the facts has none, as the facts do not
    /// say it. A snapshot does not keep it either.
    #[serde(skip)]
    pub(crate) id: Option<Remembered>,
    pub(crate) holder: Principal,
    pub(crate) consumer: ConsumerName,
    /// When the lease ends, in milliseconds since the Unix epoch. An extension
    /// moves it later.
    pub(crate) expires_at: i64,
    /// When the item was claimed, in milliseconds since the Unix epoch. The
    /// longest a lease may run is counted from here, however often it is
    /// extended.
    pub(crate) claimed_at: i64,
}

/// What becomes of an item once the fact it owes is recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Then {
    /// It is claimable again.
    Ready,
    /// It is finished and leaves the state.
    Gone,
}

/// Where an item stands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// The due-time index of one queue: which items hold a lease, wait out a
/// backoff or owe a fact, ordered by when each is due. The timer reads the
/// front of each set instead of looking through every open item.
#[derive(Debug, Default)]
struct Timers {
    /// `(expires_at, id)` of every item in [`Phase::Leased`].
    leases: BTreeSet<(i64, ItemId)>,
    /// `(due, id)` of every item in [`Phase::Backoff`].
    backoffs: BTreeSet<(i64, ItemId)>,
    /// Every item in [`Phase::Owed`], which is due at once.
    owed: BTreeSet<ItemId>,
}

impl Timers {
    fn add(&mut self, id: &ItemId, phase: &Phase) {
        match phase {
            Phase::Leased(lease) => {
                self.leases.insert((lease.expires_at, id.clone()));
            }
            Phase::Backoff { due, .. } => {
                self.backoffs.insert((*due, id.clone()));
            }
            Phase::Owed { .. } => {
                self.owed.insert(id.clone());
            }
            Phase::Ready => {}
        }
    }

    fn remove(&mut self, id: &ItemId, phase: &Phase) {
        match phase {
            Phase::Leased(lease) => {
                self.leases.remove(&(lease.expires_at, id.clone()));
            }
            Phase::Backoff { due, .. } => {
                self.backoffs.remove(&(*due, id.clone()));
            }
            Phase::Owed { .. } => {
                self.owed.remove(id);
            }
            Phase::Ready => {}
        }
    }
}

/// What the timer has to do for one item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Due {
    /// The lease ran out.
    Lease,
    /// The backoff is over, or the item owes a fact.
    Advance,
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
    /// When each leased, backing-off and owing item is due.
    timers: Timers,
}

/// A queue as a snapshot keeps it: the open items in the order they stand in,
/// and the ids of the finished ones.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct QueueRecord {
    items: Vec<(ItemId, Item)>,
    finished: Vec<ItemId>,
    next_order: u64,
}

impl Queue {
    /// The queue as a record, to write in a snapshot.
    pub(crate) fn to_record(&self) -> QueueRecord {
        let mut items: Vec<(ItemId, Item)> = self
            .items
            .iter()
            .map(|(id, item)| (id.clone(), item.clone()))
            .collect();
        items.sort_by_key(|(_, item)| item.order);
        let mut finished: Vec<ItemId> = self.finished.other.iter().cloned().collect();
        finished.extend(self.finished.uuids.iter().filter_map(|uuid| {
            EventId::from_u128(*uuid)
                .ok()
                .and_then(|id| id.to_string().parse::<ItemId>().ok())
        }));
        finished.sort();
        QueueRecord {
            items,
            finished,
            next_order: self.next_order,
        }
    }

    /// The queue a record was made from.
    pub(crate) fn from_record(record: QueueRecord) -> Self {
        let mut queue = Self {
            next_order: record.next_order,
            ..Self::default()
        };
        for id in &record.finished {
            queue.finished.insert(id);
        }
        for (id, item) in record.items {
            // An order past `next_order` would let a later addition stand
            // before an earlier one.
            queue.next_order = queue.next_order.max(item.order.saturating_add(1));
            if item.phase == Phase::Ready {
                queue.ready.insert((item.order, id.clone()));
            }
            queue.timers.add(&id, &item.phase);
            queue.items.insert(id, item);
        }
        queue
    }

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

    /// What is due at `now`, as `(kind, due time, id)`, pushed onto `out`.
    /// Returns how many index entries were looked at: the due ones and at
    /// most one more per index, so the work does not depend on how many items
    /// are open.
    pub(crate) fn due(&self, now: i64, out: &mut Vec<(Due, i64, ItemId)>) -> usize {
        let mut looked = 0;
        for (at, id) in &self.timers.leases {
            looked += 1;
            if *at > now {
                break;
            }
            out.push((Due::Lease, *at, id.clone()));
        }
        for (at, id) in &self.timers.backoffs {
            looked += 1;
            if *at > now {
                break;
            }
            out.push((Due::Advance, *at, id.clone()));
        }
        for id in &self.timers.owed {
            looked += 1;
            out.push((Due::Advance, now, id.clone()));
        }
        looked
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
        self.timers.add(&id, &phase);
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
            self.timers.remove(id, &old.phase);
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
        self.timers.remove(id, &item.phase);
        self.timers.add(id, &phase);
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

/// The alerts nobody has acknowledged, each with when it was published or
/// last surfaced (milliseconds since the Unix epoch), and an index by that
/// time so that the ones due to surface again are found without looking at
/// the rest.
#[derive(Debug, Default)]
pub(crate) struct Alerts {
    surfaced: HashMap<EventId, i64>,
    by_time: BTreeSet<(i64, EventId)>,
}

impl Alerts {
    pub(crate) fn get(&self, alert: &EventId) -> Option<&i64> {
        self.surfaced.get(alert)
    }

    pub(crate) fn contains_key(&self, alert: &EventId) -> bool {
        self.surfaced.contains_key(alert)
    }

    #[cfg(test)]
    pub(crate) fn keys(&self) -> impl Iterator<Item = &EventId> {
        self.surfaced.keys()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&EventId, &i64)> {
        self.surfaced.iter()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.surfaced.is_empty()
    }

    /// Set when the alert was published or last surfaced.
    pub(crate) fn insert(&mut self, alert: EventId, at: i64) -> Option<i64> {
        let old = self.surfaced.insert(alert, at);
        if let Some(old) = old {
            self.by_time.remove(&(old, alert));
        }
        self.by_time.insert((at, alert));
        old
    }

    pub(crate) fn remove(&mut self, alert: &EventId) -> Option<i64> {
        let old = self.surfaced.remove(alert)?;
        self.by_time.remove(&(old, *alert));
        Some(old)
    }

    /// The alerts last surfaced at or before `cutoff`, oldest first, pushed
    /// onto `out`. Returns how many index entries were looked at: the due
    /// ones and at most one more.
    pub(crate) fn surfaced_by(&self, cutoff: i64, out: &mut Vec<(i64, EventId)>) -> usize {
        let mut looked = 0;
        for (at, alert) in &self.by_time {
            looked += 1;
            if *at > cutoff {
                break;
            }
            out.push((*at, *alert));
        }
        looked
    }
}

impl From<HashMap<EventId, i64>> for Alerts {
    fn from(surfaced: HashMap<EventId, i64>) -> Self {
        let by_time = surfaced.iter().map(|(alert, at)| (*at, *alert)).collect();
        Self { surfaced, by_time }
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
    pub(crate) alerts: Alerts,
    /// The ids of alerts that were acknowledged, which are all that is kept
    /// of them.
    pub(crate) acknowledged_alerts: HashSet<EventId>,
    /// The idempotency keys publishes have used. Unlike the rest of the
    /// state they are not rebuilt from the topics: they are read once from
    /// the log of keys, and a reload of the rest keeps them.
    pub(crate) keys: Keys,
    pub(crate) keys_loaded: bool,
    /// For each topic the state is made from, the offset the next message
    /// will have if the state covers every message before it. `None` when it
    /// is not known to, so that no snapshot is taken of it.
    pub(crate) covered: Option<HashMap<Topic, u64>>,
    /// When the last snapshot of this state was taken or read, in
    /// milliseconds since the Unix epoch.
    pub(crate) snapshot_at: Option<i64>,
    /// Whether the state has changed since that snapshot.
    pub(crate) changed: bool,
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
            claimed_at: 0,
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
            claimed_at: 0,
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

    fn leased(expires_at: i64) -> Phase {
        Phase::Leased(Lease {
            fingerprint: LeaseId::generate().fingerprint(),
            id: None,
            holder: "service:test".parse().unwrap(),
            consumer: "w".parse().unwrap(),
            expires_at,
            claimed_at: 0,
        })
    }

    /// Covers the indexes (`Queue::due`, `Alerts::surfaced_by`) and the number
    /// of entries each reports looking at. The engine test of the same name
    /// drives `Engine::due_counted` over a seeded tenant, and
    /// `tick_benchmark` times the tick at 1k, 10k and 100k open items.
    #[test]
    fn a_tick_with_nothing_due_does_constant_work_at_100k_open_items() {
        let mut queue = Queue::default();
        let mut alerts = Alerts::default();
        let far = 1_000_000;
        for n in 0..100_000_i64 {
            let (id, message) = message(u64::try_from(n).unwrap());
            let phase = match n % 3 {
                0 => Phase::Ready,
                1 => leased(far + n),
                _ => Phase::Backoff {
                    due: far + n,
                    next: Next::Retry,
                },
            };
            queue.insert(id, message, phase);
            alerts.insert(EventId::generate(), far + n);
        }
        assert_eq!(queue.len(), 100_000);

        let mut due = Vec::new();
        let looked = queue.due(0, &mut due);
        assert!(due.is_empty());
        // One look at the front of each of the leases and backoffs indexes
        // (the owed set is empty and costs no look), however many items are
        // open.
        assert_eq!(looked, 2);
        let mut surfaced = Vec::new();
        assert_eq!(alerts.surfaced_by(0, &mut surfaced), 1);
        assert!(surfaced.is_empty());

        // With some due, the work is the due items plus one look per index.
        let looked = queue.due(far + 9, &mut due);
        assert_eq!(due.len(), 6);
        assert_eq!(looked, due.len() + 2);
        assert_eq!(alerts.surfaced_by(far + 9, &mut surfaced), 11);
        assert_eq!(surfaced.len(), 10);
    }

    #[test]
    fn the_due_index_follows_every_change_of_phase() {
        let mut queue = Queue::default();
        let (a, message_a) = message(1);
        let (b, message_b) = message(2);
        queue.insert(a.clone(), message_a, leased(10));
        queue.insert(b.clone(), message_b, Phase::Ready);
        let due = |queue: &Queue, now| {
            let mut due = Vec::new();
            queue.due(now, &mut due);
            due.into_iter()
                .map(|(kind, at, id)| (kind, at, id.to_string()))
                .collect::<Vec<_>>()
        };
        assert!(due(&queue, 9).is_empty());
        assert_eq!(due(&queue, 10), [(Due::Lease, 10, a.to_string())]);

        // An extension moves the expiry.
        queue.set_phase(&a, leased(20));
        assert!(due(&queue, 10).is_empty());
        assert_eq!(due(&queue, 20), [(Due::Lease, 20, a.to_string())]);

        queue.set_phase(
            &a,
            Phase::Backoff {
                due: 30,
                next: Next::Retry,
            },
        );
        assert!(due(&queue, 29).is_empty());
        assert_eq!(due(&queue, 30), [(Due::Advance, 30, a.to_string())]);

        // An item that owes a fact is due at once.
        let (_, fact) = message(3);
        queue.set_phase(
            &a,
            Phase::Owed {
                fact: Box::new(fact),
                then: Then::Ready,
            },
        );
        assert_eq!(due(&queue, 0), [(Due::Advance, 0, a.to_string())]);

        queue.set_phase(&a, Phase::Ready);
        assert!(due(&queue, i64::MAX).is_empty());
        queue.set_phase(&b, leased(5));
        queue.finish(&b);
        assert!(due(&queue, i64::MAX).is_empty());
        queue.insert(a.clone(), message(1).1, leased(7));
        queue.insert(a.clone(), message(1).1, Phase::Ready);
        assert!(due(&queue, i64::MAX).is_empty());
    }

    #[test]
    fn alerts_are_indexed_by_when_they_were_last_surfaced() {
        let mut alerts = Alerts::default();
        let (x, y) = (EventId::generate(), EventId::generate());
        alerts.insert(x, 5);
        alerts.insert(y, 8);
        alerts.insert(x, 12);
        let mut out = Vec::new();
        alerts.surfaced_by(10, &mut out);
        assert_eq!(out, [(8, y)]);
        assert_eq!(alerts.remove(&y), Some(8));
        out.clear();
        alerts.surfaced_by(100, &mut out);
        assert_eq!(out, [(12, x)]);
        assert_eq!(alerts.get(&x), Some(&12));
    }
}
