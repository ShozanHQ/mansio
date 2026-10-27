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

use mansio_core::{
    AlertEvent, AlertFact, Broker, BrokerError, Caller, Envelope, EventId, ItemId, Next, Segment,
    Stored, TenantId, Timestamp, Topic, WorkEvent, WorkFact,
};
use std::collections::HashSet;
use tracing::warn;

use crate::error::ApiError;
use crate::facts::Facts;
use crate::state::{Lease, Phase, Queue, Then};

/// Everything the engine reads from a tenant's topics, each in publish order.
#[derive(Debug, Default)]
pub(crate) struct Logs {
    pub(crate) work: HashMap<Segment, Vec<Stored>>,
    pub(crate) facts_work: Vec<Stored>,
    pub(crate) dead: Vec<Stored>,
    pub(crate) alerts: Vec<Stored>,
    pub(crate) facts_alert: Vec<Stored>,
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

/// Read, in pages of `page`, every topic the engine replays.
pub(crate) async fn read_logs(
    broker: &dyn Broker,
    tenant: &TenantId,
    topics: &[Topic],
    page: u32,
) -> Result<Logs, BrokerError> {
    let mut logs = Logs::default();
    for topic in topics {
        let target = match topic {
            Topic::Work(queue) => {
                let messages = read_all(broker, tenant, topic, page).await?;
                logs.work.insert(queue.clone(), messages);
                continue;
            }
            Topic::Dead => &mut logs.dead,
            Topic::Alerts => &mut logs.alerts,
            Topic::Facts(_) if *topic == Topic::facts_work() => &mut logs.facts_work,
            Topic::Facts(_) if *topic == Topic::facts_alert() => &mut logs.facts_alert,
            Topic::Facts(_) | Topic::Presence => continue,
        };
        *target = read_all(broker, tenant, topic, page).await?;
    }
    Ok(logs)
}

async fn read_all(
    broker: &dyn Broker,
    tenant: &TenantId,
    topic: &Topic,
    page: u32,
) -> Result<Vec<Stored>, BrokerError> {
    let mut all = Vec::new();
    let mut offset = 0;
    // Messages before this offset are read one at a time: the page that
    // failed has a message in it that cannot be read.
    let mut one_by_one_until = 0_u64;
    loop {
        let width = if offset < one_by_one_until { 1 } else { page };
        let batch = match broker
            .poll(&Caller::Admin, tenant, topic, offset, width)
            .await
        {
            Ok(batch) => batch,
            Err(BrokerError::NotFound(_)) => break,
            // A page with a message in it that is not an envelope fails as a
            // whole. Read it again one message at a time to find which, and
            // step over that one. The rest of the page is read the same way,
            // and the next page whole.
            Err(BrokerError::Corrupt(_)) if width > 1 => {
                one_by_one_until = offset.saturating_add(u64::from(page));
                continue;
            }
            Err(BrokerError::Corrupt(why)) => {
                warn!(%tenant, %topic, offset, %why, "a message that cannot be read is skipped");
                offset = offset.saturating_add(1);
                continue;
            }
            Err(error) => return Err(error),
        };
        let Some(last) = batch.last() else { break };
        let next = last.offset.saturating_add(1);
        if next <= offset {
            break;
        }
        offset = next;
        all.extend(batch);
    }
    Ok(all)
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
            .and_then(|data| serde_json::from_value::<WorkFact>(data).ok());
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

fn lease_of(data: &WorkFact, now: i64) -> Option<Lease> {
    Some(Lease {
        id: data.lease_id?,
        holder: data.holder.clone()?,
        consumer: data.consumer.clone()?,
        expires_at: data
            .expires_at
            .as_ref()
            .map_or(now, |at| millis(Some(at), now)),
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
            WorkEvent::Claimed => {
                lease_of(&last.data, self.now).map_or(Phase::Ready, Phase::Leased)
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

/// Rebuild the state from the logs.
///
/// # Errors
///
/// Returns [`ApiError::Internal`] if the engine cannot make a fact it owes.
pub(crate) fn replay(
    tenant: &TenantId,
    logs: &Logs,
    facts: &Facts,
    now: i64,
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

    let mut rebuilt = Rebuilt::default();
    for (name, messages) in &logs.work {
        let mut queue = Queue::default();
        for (item, newest) in newest_versions(messages) {
            let attempt = newest.envelope.attempt.unwrap_or(1);
            let about_this_attempt: Vec<&Fact> = recorded
                .get(&(name.clone(), item.clone()))
                .into_iter()
                .flatten()
                .filter(|fact| fact.data.attempt == attempt)
                .collect();
            match context.phase(name, &item, newest, &about_this_attempt)? {
                Some(phase) => queue.insert(item, newest.envelope.clone(), phase),
                None => queue.finish(&item),
            }
        }
        rebuilt.queues.insert(name.clone(), queue);
    }

    for alert in &logs.alerts {
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
            .and_then(|data| serde_json::from_value::<AlertFact>(data).ok())
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
