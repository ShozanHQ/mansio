//! Rebuilding a tenant's engine state from its topics.
//!
//! The state is a projection of the logs, so a restart loses nothing: each
//! item's newest message says what the item is, and the transitions recorded
//! for that attempt say where it stands. A transition that was only half
//! written before the server stopped is finished here: an item whose retry
//! was published but not recorded owes its `retried` fact, and so does one
//! whose copy on `dead` was published but not recorded. The fact it owes is
//! the one the engine made when it published the message (see
//! [`Facts::owed`]).
//!
//! A message that cannot be read is skipped, with a warning naming where it
//! is, so that one bad message does not stop a tenant.

use std::collections::HashMap;
use std::sync::Arc;

use mansio_core::{
    AlertEvent, AlertFact, Broker, BrokerError, Caller, Envelope, EventId, ItemId, Next, Segment,
    Stored, TenantId, Timestamp, Topic, WorkEvent, WorkFact,
};
use std::collections::HashSet;
use tracing::warn;

use crate::error::ApiError;
use crate::facts::Facts;
use crate::state::{Item, Lease, Phase, Queue, Then};
use crate::tail::Tail;

/// Everything the engine reads from a tenant's topics, each in publish order.
#[derive(Debug, Default)]
pub(crate) struct Logs {
    pub(crate) work: HashMap<Segment, Vec<Stored>>,
    pub(crate) facts_work: Vec<Stored>,
    pub(crate) dead: Vec<Stored>,
    pub(crate) alerts: Vec<Stored>,
    pub(crate) facts_alert: Vec<Stored>,
    /// For each topic read, the offset after the last message read: where a
    /// reader that has all of it goes on from.
    pub(crate) ends: HashMap<Topic, u64>,
}

/// The state rebuilt from the logs.
#[derive(Debug, Default)]
pub(crate) struct Rebuilt {
    pub(crate) queues: HashMap<Segment, Queue>,
    /// Alerts nobody acknowledged, with when each was published or last
    /// surfaced.
    pub(crate) alerts: HashMap<EventId, i64>,
    pub(crate) acknowledged_alerts: HashSet<EventId>,
}

impl Logs {
    /// How many messages were read.
    pub(crate) fn len(&self) -> usize {
        self.work.values().map(Vec::len).sum::<usize>()
            + self.facts_work.len()
            + self.dead.len()
            + self.alerts.len()
            + self.facts_alert.len()
    }
}

/// Whether the engine replays the topic.
pub(crate) fn is_replayed(topic: &Topic) -> bool {
    match topic {
        Topic::Work(_) | Topic::Dead | Topic::Alerts => true,
        Topic::Facts(_) => *topic == Topic::facts_work() || *topic == Topic::facts_alert(),
        Topic::Presence => false,
    }
}

/// Read, in pages of `page`, every topic the engine replays, each from its
/// offset in `from` or from the start if it has none.
pub(crate) async fn read_logs(
    broker: &Arc<dyn Broker>,
    tenant: &TenantId,
    topics: &[Topic],
    page: u32,
    from: &HashMap<Topic, u64>,
) -> Result<Logs, BrokerError> {
    let mut logs = Logs::default();
    for topic in topics.iter().filter(|topic| is_replayed(topic)) {
        let start = from.get(topic).copied().unwrap_or(0);
        let (messages, end) = read_all(broker, tenant, topic, start, page).await?;
        logs.ends.insert(topic.clone(), end);
        let target = match topic {
            Topic::Work(queue) => {
                logs.work.insert(queue.clone(), messages);
                continue;
            }
            Topic::Dead => &mut logs.dead,
            Topic::Alerts => &mut logs.alerts,
            Topic::Facts(_) if *topic == Topic::facts_work() => &mut logs.facts_work,
            Topic::Facts(_) if *topic == Topic::facts_alert() => &mut logs.facts_alert,
            Topic::Facts(_) | Topic::Presence => continue,
        };
        *target = messages;
    }
    Ok(logs)
}

/// Every message of the topic from `from` on, in order, read page by page by
/// the reader the event streams and webhooks use, which steps over a message
/// that cannot be read, and the offset after the last.
async fn read_all(
    broker: &Arc<dyn Broker>,
    tenant: &TenantId,
    topic: &Topic,
    from: u64,
    page: u32,
) -> Result<(Vec<Stored>, u64), BrokerError> {
    let mut tail = Tail::new(
        Arc::clone(broker),
        tenant.clone(),
        topic.clone(),
        from,
        page,
    );
    let mut all = Vec::new();
    loop {
        let before = tail.next();
        let batch = match tail.read(&Caller::Admin).await {
            Ok(batch) => batch,
            Err(BrokerError::NotFound(_)) => break,
            Err(error) => return Err(error),
        };
        if batch.is_empty() || tail.next() <= before {
            break;
        }
        all.extend(batch);
    }
    Ok((all, tail.next()))
}

/// Whether the broker holds a message at or after `offset - 1` in each topic
/// the snapshot says it covers up to `offset`. A topic that ends before that
/// is not the one the snapshot was made from, and what the snapshot says about
/// it cannot be trusted.
///
/// # Errors
///
/// Returns the first topic that fails the check, with why, or the broker's
/// error as text.
pub(crate) async fn check_offsets(
    broker: &Arc<dyn Broker>,
    tenant: &TenantId,
    offsets: &HashMap<Topic, u64>,
) -> Result<(), String> {
    for (topic, next) in offsets {
        if *next == 0 {
            continue;
        }
        match broker
            .poll(&Caller::Admin, tenant, topic, next.saturating_sub(1), 1)
            .await
        {
            Ok(found) if found.is_empty() => {
                return Err(format!(
                    "{topic} ends before offset {next}, which the snapshot covers"
                ));
            }
            Ok(_) | Err(BrokerError::Corrupt(_)) => {}
            Err(BrokerError::NotFound(_)) => {
                return Err(format!("{topic} is gone, and the snapshot covers it"));
            }
            Err(error) => return Err(format!("{topic} could not be read: {error}")),
        }
    }
    Ok(())
}

/// A recorded transition of a work item.
struct Fact {
    event: WorkEvent,
    data: WorkFact,
    /// When it was recorded, in milliseconds since the Unix epoch.
    at: i64,
}

fn millis(time: Option<&Timestamp>, fallback: i64) -> i64 {
    time.and_then(Timestamp::unix_millis).unwrap_or(fallback)
}

/// The transitions on `facts.work`, grouped by queue and item, in order.
fn work_facts(stored: &[Stored], now: i64) -> HashMap<(Segment, ItemId), Vec<Fact>> {
    let mut grouped: HashMap<(Segment, ItemId), Vec<Fact>> = HashMap::new();
    for message in stored {
        let envelope = &message.envelope;
        let Some(event) = WorkEvent::from_event_type(envelope.event_type.as_str()) else {
            continue;
        };
        let item = envelope
            .subject
            .as_ref()
            .and_then(|subject| subject.as_str().parse::<ItemId>().ok());
        let data = envelope
            .data
            .clone()
            .and_then(|data| data.parse::<WorkFact>().ok());
        let (Some(item), Some(data)) = (item, data) else {
            warn!(offset = message.offset, "a work fact could not be read");
            continue;
        };
        let at = millis(envelope.time.as_ref(), now);
        grouped
            .entry((data.queue.clone(), item))
            .or_default()
            .push(Fact { event, data, at });
    }
    grouped
}

/// The newest version of each item in a queue: the message with the highest
/// attempt, and the first of them if a publish was repeated. In offset order.
fn newest_versions(messages: &[Stored]) -> Vec<(ItemId, &Stored)> {
    let mut newest: HashMap<ItemId, &Stored> = HashMap::new();
    for message in messages {
        let Some(item) = message.envelope.item_id.clone() else {
            warn!(offset = message.offset, "a work message has no item id");
            continue;
        };
        let attempt = message.envelope.attempt.unwrap_or(1);
        let newer = newest
            .get(&item)
            .is_none_or(|known| known.envelope.attempt.unwrap_or(1) < attempt);
        if newer {
            newest.insert(item, message);
        }
    }
    let mut versions: Vec<(ItemId, &Stored)> = newest.into_iter().collect();
    versions.sort_by_key(|(_, message)| message.offset);
    versions
}

/// The lease an attempt stands under, from the facts recorded for it. The
/// holder and the lease are those of the last `claimed` or `extended`, and so
/// is the end of the lease, which each extension moved later. The lease began
/// at the first `claimed`: a second one is the same lease given a new id by a
/// restarted server. `prior` is the lease the item held in a snapshot, whose
/// facts are not among `recorded`: it says when the lease began.
fn lease_of(recorded: &[&Fact], prior: Option<&Lease>, now: i64) -> Option<Lease> {
    let held = |fact: &&&Fact| matches!(fact.event, WorkEvent::Claimed | WorkEvent::Extended);
    let latest = recorded.iter().rev().find(held)?;
    let claimed_at = prior.map_or_else(
        || {
            recorded
                .iter()
                .find(|fact| fact.event == WorkEvent::Claimed)
                .unwrap_or(latest)
                .at
        },
        |lease| lease.claimed_at,
    );
    let data = &latest.data;
    Some(Lease {
        fingerprint: data.lease?,
        id: None,
        holder: data.holder.clone()?,
        consumer: data.consumer.clone()?,
        expires_at: data
            .expires_at
            .as_ref()
            .map_or(now, |at| millis(Some(at), now)),
        claimed_at,
    })
}

/// What the replay knows besides the logs.
struct Replay<'a> {
    tenant: &'a TenantId,
    facts: &'a Facts,
    /// The copies on `dead`, by the id of the message they came from.
    dead_by_cause: HashMap<String, &'a Stored>,
    now: i64,
}

impl Replay<'_> {
    /// Where the item stands, or `None` if it is finished.
    fn phase(
        &self,
        queue: &Segment,
        item: &ItemId,
        newest: &Stored,
        recorded: &[&Fact],
        prior: Option<&Lease>,
    ) -> Result<Option<Phase>, ApiError> {
        let message = &newest.envelope;
        let attempt = message.attempt.unwrap_or(1);
        let Some(last) = recorded.last() else {
            // A first attempt nobody has touched. A later one was made by a
            // retry, whose fact must come before anything else.
            return if attempt <= 1 {
                Ok(Some(Phase::Ready))
            } else {
                self.owed_retried(queue, message).map(Some)
            };
        };
        let phase = match last.event {
            WorkEvent::Retried => Phase::Ready,
            WorkEvent::Acked | WorkEvent::Dead => return Ok(None),
            WorkEvent::Claimed | WorkEvent::Extended => {
                lease_of(recorded, prior, self.now).map_or(Phase::Ready, Phase::Leased)
            }
            WorkEvent::Failed | WorkEvent::Expired => match last.data.next {
                Some(Next::Dead) => self.dead_phase(queue, item, message, last.at)?,
                Some(Next::Retry) | None => Phase::Backoff {
                    due: millis(last.data.retry_at.as_ref(), last.at),
                    next: Next::Retry,
                },
            },
        };
        Ok(Some(phase))
    }

    /// The `retried` fact of a message that was published without it.
    fn owed_retried(&self, queue: &Segment, message: &Envelope) -> Result<Phase, ApiError> {
        let previous = message
            .causation_id
            .as_ref()
            .and_then(|cause| cause.as_str().parse().ok());
        let fact = self
            .facts
            .owed(self.tenant, WorkEvent::Retried, queue, message, previous)?;
        Ok(Phase::Owed {
            fact: Box::new(fact),
            then: Then::Ready,
        })
    }

    /// An item that failed past the cap: either its copy on `dead` is
    /// published and the fact is owed, or the copy is still to be published.
    fn dead_phase(
        &self,
        queue: &Segment,
        item: &ItemId,
        message: &Envelope,
        failed_at: i64,
    ) -> Result<Phase, ApiError> {
        let Some(copy) = self
            .dead_by_cause
            .get(&message.id.to_string())
            .filter(|copy| copy.envelope.item_id.as_ref() == Some(item))
        else {
            return Ok(Phase::Backoff {
                due: failed_at,
                next: Next::Dead,
            });
        };
        let fact = self.facts.owed(
            self.tenant,
            WorkEvent::Dead,
            queue,
            &copy.envelope,
            Some(message.id),
        )?;
        Ok(Phase::Owed {
            fact: Box::new(fact),
            then: Then::Gone,
        })
    }
}

/// Rebuild the state from the logs, or bring `base`, the state a snapshot
/// holds, up to date with the messages after the snapshot.
///
/// With a `base`, `logs` hold only what came after it. A work message in them
/// is a new item or a new version of one the base holds, and a fact is about
/// an item the base holds or one the new messages made. Either way the item
/// ends up where a rebuild from offset 0 would put it.
///
/// # Errors
///
/// Returns [`ApiError::Internal`] if the engine cannot make a fact it owes.
pub(crate) fn replay(
    tenant: &TenantId,
    logs: &Logs,
    facts: &Facts,
    now: i64,
    base: Option<Rebuilt>,
) -> Result<Rebuilt, ApiError> {
    let recorded = work_facts(&logs.facts_work, now);
    let context = Replay {
        tenant,
        facts,
        dead_by_cause: logs
            .dead
            .iter()
            .filter_map(|copy| Some((copy.envelope.causation_id.as_ref()?.to_string(), copy)))
            .collect(),
        now,
    };

    let mut rebuilt = base.unwrap_or_default();
    let mut handled: HashSet<(Segment, ItemId)> = HashSet::new();
    for (name, messages) in &logs.work {
        let queue = rebuilt.queues.entry(name.clone()).or_default();
        for (item, newest) in newest_versions(messages) {
            let attempt = newest.envelope.attempt.unwrap_or(1);
            let held = queue.get(&item).map(Item::attempt);
            if held.is_some_and(|held| held >= attempt) || (held.is_none() && queue.knows(&item)) {
                // Finished, or the version the base holds: the facts say
                // where it stands.
                continue;
            }
            let about_this_attempt: Vec<&Fact> = recorded
                .get(&(name.clone(), item.clone()))
                .into_iter()
                .flatten()
                .filter(|fact| fact.data.attempt == attempt)
                .collect();
            match context.phase(name, &item, newest, &about_this_attempt, None)? {
                Some(phase) => queue.insert(item.clone(), newest.envelope.clone(), phase),
                None => queue.finish(&item),
            }
            handled.insert((name.clone(), item));
        }
    }

    // What the facts after the base say about the items it holds.
    for ((name, item), about_item) in &recorded {
        if handled.contains(&(name.clone(), item.clone())) {
            continue;
        }
        let Some(queue) = rebuilt.queues.get_mut(name) else {
            continue;
        };
        let Some(known) = queue.get(item) else {
            continue;
        };
        let about_this_attempt: Vec<&Fact> = about_item
            .iter()
            .filter(|fact| fact.data.attempt == known.attempt())
            .collect();
        if about_this_attempt.is_empty() {
            continue;
        }
        let prior = match &known.phase {
            Phase::Leased(lease) => Some(lease.clone()),
            _ => None,
        };
        let current = Stored {
            offset: 0,
            envelope: known.message.clone(),
        };
        match context.phase(name, item, &current, &about_this_attempt, prior.as_ref())? {
            Some(phase) => queue.set_phase(item, phase),
            None => queue.finish(item),
        }
    }

    // An item that failed past the cap waits for its copy on `dead`, which
    // may have come after the base.
    for (name, queue) in &mut rebuilt.queues {
        let waiting: Vec<(ItemId, Envelope, i64)> = queue
            .iter()
            .filter_map(|(id, item)| match item.phase {
                Phase::Backoff {
                    due,
                    next: Next::Dead,
                } => Some((id.clone(), item.message.clone(), due)),
                _ => None,
            })
            .collect();
        for (id, message, due) in waiting {
            let phase = context.dead_phase(name, &id, &message, due)?;
            if matches!(phase, Phase::Owed { .. }) {
                queue.set_phase(&id, phase);
            }
        }
    }

    for alert in &logs.alerts {
        if rebuilt.acknowledged_alerts.contains(&alert.envelope.id) {
            continue;
        }
        // The first copy of an alert is the alert. The time it carries is the
        // producer's, so one from the future counts from now.
        rebuilt
            .alerts
            .entry(alert.envelope.id)
            .or_insert_with(|| millis(alert.envelope.time.as_ref(), now).min(now));
    }
    for message in &logs.facts_alert {
        let envelope = &message.envelope;
        let Some(event) = AlertEvent::from_event_type(envelope.event_type.as_str()) else {
            continue;
        };
        let Some(data) = envelope
            .data
            .clone()
            .and_then(|data| data.parse::<AlertFact>().ok())
        else {
            warn!(offset = message.offset, "an alert fact could not be read");
            continue;
        };
        let at = millis(envelope.time.as_ref(), now);
        match event {
            // The first acknowledgement is the one that counts.
            AlertEvent::Acknowledged => {
                if rebuilt.alerts.remove(&data.alert_id).is_some() {
                    rebuilt.acknowledged_alerts.insert(data.alert_id);
                }
            }
            AlertEvent::Resurfaced => {
                if let Some(surfaced_at) = rebuilt.alerts.get_mut(&data.alert_id) {
                    *surfaced_at = (*surfaced_at).max(at);
                }
            }
        }
    }
    Ok(rebuilt)
}
