//! The transitions mansio records on `facts.work` and `facts.alert`, and the
//! `data` each one carries.
//!
//! The server's engine writes these and, on start, reads them back to rebuild
//! its state, so both sides use the types here.

use serde::{Deserialize, Serialize};

use crate::envelope::Timestamp;
use crate::id::{ConsumerName, EventId, LeaseFingerprint, Principal, Segment};

/// A transition of a work item, recorded as an envelope on `facts.work` with
/// the item id as its `subject`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkEvent {
    /// A consumer took the item under a lease.
    Claimed,
    /// The holder completed it.
    Acked,
    /// The holder gave it back as failed.
    Failed,
    /// The lease ran out before the holder answered.
    Expired,
    /// The item was published to its queue again, one attempt on.
    Retried,
    /// The item was published to `dead`, past the attempt cap.
    Dead,
}

impl WorkEvent {
    /// Every transition, in the order a healthy item meets them.
    pub const ALL: [WorkEvent; 6] = [
        WorkEvent::Claimed,
        WorkEvent::Acked,
        WorkEvent::Failed,
        WorkEvent::Expired,
        WorkEvent::Retried,
        WorkEvent::Dead,
    ];

    /// The CloudEvents `type` of the fact: `mansio.work.claimed` and so on.
    #[must_use]
    pub fn event_type(self) -> &'static str {
        match self {
            WorkEvent::Claimed => "mansio.work.claimed",
            WorkEvent::Acked => "mansio.work.acked",
            WorkEvent::Failed => "mansio.work.failed",
            WorkEvent::Expired => "mansio.work.expired",
            WorkEvent::Retried => "mansio.work.retried",
            WorkEvent::Dead => "mansio.work.dead",
        }
    }

    /// The transition a CloudEvents `type` names, if it is one.
    #[must_use]
    pub fn from_event_type(event_type: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|event| event.event_type() == event_type)
    }
}

/// A transition of an alert, recorded as an envelope on `facts.alert` with
/// the alert id as its `subject`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertEvent {
    /// A principal acknowledged the alert.
    Acknowledged,
    /// The alert went unacknowledged for another interval.
    Resurfaced,
}

impl AlertEvent {
    /// Both transitions.
    pub const ALL: [AlertEvent; 2] = [AlertEvent::Acknowledged, AlertEvent::Resurfaced];

    /// The CloudEvents `type` of the fact: `mansio.alert.acknowledged` and
    /// `mansio.alert.resurfaced`.
    #[must_use]
    pub fn event_type(self) -> &'static str {
        match self {
            AlertEvent::Acknowledged => "mansio.alert.acknowledged",
            AlertEvent::Resurfaced => "mansio.alert.resurfaced",
        }
    }

    /// The transition a CloudEvents `type` names, if it is one.
    #[must_use]
    pub fn from_event_type(event_type: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|event| event.event_type() == event_type)
    }
}

/// What happens to an item after a `failed` or `expired` fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Next {
    /// Published to its queue again, one attempt on, at `retry_at`.
    Retry,
    /// Published to `dead`: the attempt cap is reached.
    Dead,
}

/// The `data` of a `facts.work` envelope. Which fields are set depends on the
/// transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkFact {
    /// The queue, the `<queue>` of `work.<queue>`.
    pub queue: Segment,
    /// The work message this is about. For `retried` it is the new message,
    /// and for `dead` the message published to `dead`.
    pub message_id: EventId,
    /// `retried` and `dead`: the message this one replaces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_message_id: Option<EventId>,
    /// The attempt this is about, from 1.
    pub attempt: u32,
    /// The lease, on `claimed`, `acked`, `failed` and `expired`: a fingerprint
    /// of its id, which is the holder's proof and so is not recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<LeaseFingerprint>,
    /// The principal holding the lease.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holder: Option<Principal>,
    /// The consumer name the lease was taken under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer: Option<ConsumerName>,
    /// When the lease ends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
    /// `failed`: why, as the holder said.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// `failed` and `expired`: what happens next.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<Next>,
    /// `failed` and `expired` with [`Next::Retry`]: when it is republished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_at: Option<Timestamp>,
}

impl WorkFact {
    /// A fact about `message_id`, attempt `attempt` of a queue, with nothing
    /// else set.
    #[must_use]
    pub fn new(queue: Segment, message_id: EventId, attempt: u32) -> Self {
        Self {
            queue,
            message_id,
            previous_message_id: None,
            attempt,
            lease: None,
            holder: None,
            consumer: None,
            expires_at: None,
            reason: None,
            next: None,
            retry_at: None,
        }
    }
}

/// The `data` of a `facts.alert` envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertFact {
    /// The alert, which is the id of the message on `alerts`.
    pub alert_id: EventId,
    /// `acknowledged`: who.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_by: Option<Principal>,
    /// `acknowledged`: when.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_at: Option<Timestamp>,
    /// `resurfaced`: when.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surfaced_at: Option<Timestamp>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_event_type_maps_back() {
        for event in WorkEvent::ALL {
            assert!(event.event_type().starts_with("mansio.work."));
            assert_eq!(WorkEvent::from_event_type(event.event_type()), Some(event));
        }
        for event in AlertEvent::ALL {
            assert!(event.event_type().starts_with("mansio.alert."));
            assert_eq!(AlertEvent::from_event_type(event.event_type()), Some(event));
        }
        assert_eq!(WorkEvent::from_event_type("mansio.alert.resurfaced"), None);
        assert_eq!(AlertEvent::from_event_type("build.finished"), None);
    }

    #[test]
    fn a_work_fact_keeps_only_what_is_set() {
        let mut fact = WorkFact::new("builds".parse().unwrap(), EventId::generate(), 2);
        fact.next = Some(Next::Dead);
        fact.reason = Some("out of memory".to_owned());
        let json = serde_json::to_value(&fact).unwrap();
        let object = json.as_object().unwrap();
        let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["attempt", "message_id", "next", "queue", "reason"]);
        assert_eq!(json["next"], "dead");
        assert_eq!(serde_json::from_value::<WorkFact>(json).unwrap(), fact);
    }
}
