//! The JSON bodies of the HTTP API, shared by the server and its clients.

use serde::{Deserialize, Serialize};

use crate::broker::Stored;
use crate::envelope::{Envelope, Timestamp};
use crate::id::{ConsumerName, LeaseId};

/// The answer to a publish: the envelope as stored, and where it landed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Published {
    /// The offset in the topic. Absent when the broker accepted the message
    /// but could not say where it landed: it is stored, so do not publish it
    /// again; poll and match on the envelope's `id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    /// The envelope with the server-set attributes filled in.
    pub envelope: Envelope,
}

/// The answer to a poll.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Polled {
    /// Envelopes from the requested offset on, in publish order.
    pub messages: Vec<Stored>,
}

/// The body of every refusal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// What went wrong.
    pub error: ErrorDetail,
}

/// A machine-readable code and a message for a person.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorDetail {
    /// A stable code such as `bad_request` or `unauthorized`.
    pub code: String,
    /// What to fix.
    pub message: String,
}

/// The body of `work_pull`: who is asking. The principal comes from the
/// credential; the consumer name tells apart the consumers behind it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullRequest {
    /// The name this consumer pulls under.
    pub consumer: ConsumerName,
}

/// The answer to a `work_pull` that found an item: the item and the lease on
/// it. The lease holds until `expires_at`; `work_ack` and `work_fail` need
/// its id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Claimed {
    /// The id to present to acknowledge or fail the item.
    pub lease_id: LeaseId,
    /// When the lease ends, as RFC 3339.
    pub expires_at: Timestamp,
    /// The work message, with its `itemid` and `attempt`.
    pub item: Envelope,
}

/// The body of `work_ack`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckRequest {
    /// The lease the item was claimed under.
    pub lease_id: LeaseId,
}

/// The body of `work_fail`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailRequest {
    /// The lease the item was claimed under.
    pub lease_id: LeaseId,
    /// Why it failed. Recorded on the `failed` fact.
    pub reason: String,
}

/// The answer to `GET /v1/settings`: the settings the server runs with, none
/// of them secret. Durations are whole seconds, except the tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveSettings {
    /// `MANSIO_LEASE_SECONDS`: how long a claim holds.
    pub lease_seconds: u64,
    /// `MANSIO_MAX_ATTEMPTS`: attempts before an item goes to `dead`.
    pub max_attempts: u32,
    /// `MANSIO_BACKOFF_BASE_SECONDS`: the wait after the first failure.
    pub backoff_base_seconds: u64,
    /// `MANSIO_BACKOFF_CAP_SECONDS`: the longest wait.
    pub backoff_cap_seconds: u64,
    /// `MANSIO_ALERT_RESURFACE_SECONDS`: how often an unacknowledged alert
    /// is surfaced again.
    pub alert_resurface_seconds: u64,
    /// `MANSIO_TICK_MILLIS`: how often the timer looks for work to do.
    pub tick_millis: u64,
    /// `MANSIO_MAX_BODY_BYTES`.
    pub max_body_bytes: u64,
    /// `MANSIO_POLL_DEFAULT_COUNT`.
    pub poll_default_count: u32,
    /// `MANSIO_POLL_MAX_COUNT`.
    pub poll_max_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{EventId, Principal};

    fn envelope() -> Envelope {
        Envelope::new(
            EventId::generate(),
            "test".parse().unwrap(),
            "t".parse().unwrap(),
            "acme".parse().unwrap(),
            "alerts".parse().unwrap(),
            "service:test".parse::<Principal>().unwrap(),
        )
    }

    #[test]
    fn an_offset_the_broker_did_not_give_is_left_out() {
        let with = Published {
            offset: Some(3),
            envelope: envelope(),
        };
        let json = serde_json::to_value(&with).unwrap();
        assert_eq!(json["offset"], 3);
        assert_eq!(serde_json::from_value::<Published>(json).unwrap(), with);

        let without = Published {
            offset: None,
            envelope: envelope(),
        };
        let json = serde_json::to_value(&without).unwrap();
        assert!(json.get("offset").is_none(), "{json}");
        assert!(json.get("envelope").is_some());
        assert_eq!(serde_json::from_value::<Published>(json).unwrap(), without);
    }
}
