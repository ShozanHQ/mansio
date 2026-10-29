//! The JSON bodies of the HTTP API, shared by the server and its clients.

use serde::{Deserialize, Serialize};

use crate::access::Grant;
use crate::broker::Stored;
use crate::credential::Credential;
use crate::envelope::{Envelope, Timestamp};
use crate::id::{ConsumerName, LeaseId, PrincipalKind, PrincipalName, TenantId};
use crate::principal::PrincipalInfo;
use crate::topic::Topic;
use crate::webhook::{WebhookInfo, WebhookSecret};

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
    /// The id to present to acknowledge or fail the item. It is the holder's
    /// proof; the facts record a fingerprint of it, never the id.
    pub lease_id: LeaseId,
    /// The consumer name the lease was taken under, which `work_ack` and
    /// `work_fail` name too.
    pub consumer: ConsumerName,
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
    /// The consumer it was claimed as: the holder is the principal, the
    /// consumer and the lease together.
    pub consumer: ConsumerName,
}

/// The body of `work_fail`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailRequest {
    /// The lease the item was claimed under.
    pub lease_id: LeaseId,
    /// The consumer it was claimed as.
    pub consumer: ConsumerName,
    /// Why it failed. Recorded on the `failed` fact.
    pub reason: String,
}

/// The body of `principals_create`: who the credential is for and what it may
/// do.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatePrincipal {
    /// The kind. An `agent` credential always expires.
    pub kind: PrincipalKind,
    /// The name, unique within its kind and tenant.
    pub name: PrincipalName,
    /// The tenant the credential reaches, and no other.
    pub tenant: TenantId,
    /// What the credential may do. At least one.
    pub scopes: Vec<Grant>,
    /// Seconds until the credential stops working. Left out, an agent gets
    /// `MANSIO_AGENT_CREDENTIAL_TTL_SECONDS` and other kinds do not expire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
}

/// The answer to `principals_create`: the principal, and the credential. The
/// credential is shown this once and mansio keeps no copy of it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Issued {
    /// The principal as recorded.
    pub principal: PrincipalInfo,
    /// The bearer credential to present as `Authorization: Bearer ...`.
    pub credential: Credential,
}

/// The answer to `principals_list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Principals {
    /// The principals that have a credential, oldest first.
    pub principals: Vec<PrincipalInfo>,
}

/// Where a named consumer stands on a topic: the answer to `consumers_get`
/// and `consumers_commit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerState {
    /// The consumer's name.
    pub consumer: ConsumerName,
    /// The offset of the last message it committed. Absent if it has
    /// committed none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    /// The offset it reads next: one past `offset`, or 0.
    pub next: u64,
}

impl ConsumerState {
    /// The state of a consumer whose last committed offset is `committed`.
    #[must_use]
    pub fn new(consumer: ConsumerName, committed: Option<u64>) -> Self {
        Self {
            consumer,
            offset: committed,
            next: committed.map_or(0, |offset| offset.saturating_add(1)),
        }
    }
}

/// The body of `consumers_commit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    /// The offset of the last message the consumer has handled. Its next read
    /// starts one after it.
    pub offset: u64,
}

/// The body of `webhooks_create`: which topic is delivered where.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateWebhook {
    /// The tenant whose topic is delivered.
    pub tenant: TenantId,
    /// The topic delivered.
    pub topic: Topic,
    /// The `http` or `https` address each envelope is POSTed to.
    pub url: String,
}

/// The answer to `webhooks_create`: the webhook, and the secret its
/// deliveries are signed with. The secret is shown this once.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookCreated {
    /// The webhook as registered.
    pub webhook: WebhookInfo,
    /// The signing secret.
    pub secret: WebhookSecret,
}

/// The answer to `webhooks_list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Webhooks {
    /// The webhooks, oldest first, with how each is getting on. No secrets.
    pub webhooks: Vec<WebhookInfo>,
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
    /// `MANSIO_AGENT_CREDENTIAL_TTL_SECONDS`: how long an agent credential
    /// lasts when the request does not say.
    pub agent_credential_ttl_seconds: u64,
    /// `MANSIO_CREDENTIAL_RECHECK_SECONDS`: the longest a credential is
    /// trusted without asking the broker.
    pub credential_recheck_seconds: u64,
    /// `MANSIO_CREDENTIAL_SWEEP_SECONDS`: how often expired principals are
    /// removed.
    pub credential_sweep_seconds: u64,
    /// `MANSIO_STREAM_POLL_MILLIS`: how often a stream or a webhook looks
    /// for new messages.
    pub stream_poll_millis: u64,
    /// `MANSIO_SSE_KEEPALIVE_SECONDS`: how often an event stream sends a
    /// comment to keep the connection open.
    pub sse_keepalive_seconds: u64,
    /// `MANSIO_SHUTDOWN_GRACE_SECONDS`: how long a shutdown waits for the
    /// requests still being served once the event streams are ended.
    pub shutdown_grace_seconds: u64,
    /// `MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS`: the wait after a delivery's
    /// first failed attempt.
    pub webhook_backoff_base_seconds: u64,
    /// `MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS`: the longest wait.
    pub webhook_backoff_cap_seconds: u64,
    /// `MANSIO_WEBHOOK_MAX_ATTEMPTS`: attempts at one delivery before it is
    /// recorded as failed and the webhook moves on.
    pub webhook_max_attempts: u32,
    /// `MANSIO_WEBHOOK_TIMEOUT_SECONDS`: how long one attempt may take.
    pub webhook_timeout_seconds: u64,
    /// `MANSIO_WEBHOOK_URL_MAX_BYTES`: the longest address a webhook may have.
    pub webhook_url_max_bytes: u64,
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
    fn a_consumer_reads_next_from_one_past_what_it_committed() {
        let name: ConsumerName = "watcher".parse().unwrap();
        let fresh = ConsumerState::new(name.clone(), None);
        assert_eq!((fresh.offset, fresh.next), (None, 0));
        assert!(
            serde_json::to_value(&fresh)
                .unwrap()
                .get("offset")
                .is_none()
        );
        let first = ConsumerState::new(name.clone(), Some(0));
        assert_eq!((first.offset, first.next), (Some(0), 1));
        let json = serde_json::to_value(&first).unwrap();
        assert_eq!(json["offset"], 0);
        assert_eq!(json["next"], 1);
        assert_eq!(
            serde_json::from_value::<ConsumerState>(json).unwrap(),
            first
        );
        // The last offset there can be does not wrap round to 0.
        assert_eq!(ConsumerState::new(name, Some(u64::MAX)).next, u64::MAX);
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
