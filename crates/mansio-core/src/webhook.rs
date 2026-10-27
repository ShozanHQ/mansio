//! Webhooks: what is registered, what is shown about it, the signature on
//! each delivery, and the fact recorded when a delivery is given up on.
//!
//! A delivery is a POST of one envelope in the CloudEvents JSON format
//! (structured mode). Its header `mansio-signature` is
//! `t=<unix seconds>,v1=<hex>`, where the hex is the HMAC-SHA256 of
//! `<t>.<body>` under the webhook's signing secret. [`sign`] makes it and
//! [`verify`] checks it, so a receiver written in Rust need not write either.

use std::fmt;

use hmac::{Hmac, KeyInit, Mac};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::Sha256;
use uuid::Uuid;

use crate::envelope::Timestamp;
use crate::id::{EventId, TenantId, WebhookId};
use crate::topic::Topic;

/// The header that carries the signature of a delivery.
pub const SIGNATURE_HEADER: &str = "mansio-signature";

/// The content type of a delivery: a CloudEvent in structured mode.
pub const CONTENT_TYPE: &str = "application/cloudevents+json";

/// What every signing secret starts with, so that one is recognised where it
/// should not be.
const SECRET_PREFIX: &str = "whsec_";

/// The key a webhook's deliveries are signed with. It is returned once, when
/// the webhook is made, and kept only on the server's own log in the broker.
///
/// It never shows in `Debug` output. It serializes as its text because the
/// answer that makes the webhook has to carry it, once, and so does the log
/// that keeps it.
#[derive(Clone)]
pub struct WebhookSecret(SecretString);

impl WebhookSecret {
    /// A secret from its text.
    #[must_use]
    pub fn new(secret: impl Into<String>) -> Self {
        Self(SecretString::from(secret.into()))
    }

    /// A random secret of 244 bits from the operating system's generator:
    /// `whsec_` and 64 hex digits.
    #[must_use]
    pub fn random() -> Self {
        Self::new(format!(
            "{SECRET_PREFIX}{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        ))
    }

    /// The secret text: the HMAC key. Hand it to the receiver's owner and
    /// nowhere else.
    #[must_use]
    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for WebhookSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WebhookSecret(..)")
    }
}

impl Serialize for WebhookSecret {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.expose())
    }
}

impl<'de> Deserialize<'de> for WebhookSecret {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::new)
    }
}

type HmacSha256 = Hmac<Sha256>;

#[expect(
    clippy::expect_used,
    reason = "HMAC accepts a key of any length, so this cannot fail"
)]
fn mac_for(secret: &WebhookSecret) -> HmacSha256 {
    HmacSha256::new_from_slice(secret.expose().as_bytes()).expect("HMAC takes any key length")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if text.len() % 2 != 0 || !text.is_ascii() {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).ok())
        .collect()
}

/// The value of the signature header for a body sent at `timestamp` (unix
/// seconds): `t=<timestamp>,v1=<hex HMAC-SHA256 of "<timestamp>.<body>">`.
#[must_use]
pub fn sign(secret: &WebhookSecret, timestamp: i64, body: &[u8]) -> String {
    let mut mac = mac_for(secret);
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!("t={timestamp},v1={}", hex(&mac.finalize().into_bytes()))
}

/// Whether the header is a valid signature of `body` under `secret`, made
/// within `tolerance_seconds` of `now` (unix seconds). The comparison takes
/// the same time wherever the signatures differ. A header may carry several
/// `v1` signatures; any one that matches is enough.
#[must_use]
pub fn verify(
    secret: &WebhookSecret,
    header: &str,
    body: &[u8],
    now: i64,
    tolerance_seconds: i64,
) -> bool {
    let mut timestamp = None;
    let mut signatures = Vec::new();
    for part in header.split(',') {
        match part.trim().split_once('=') {
            Some(("t", value)) => timestamp = value.parse::<i64>().ok(),
            Some(("v1", value)) => signatures.extend(unhex(value)),
            _ => {}
        }
    }
    let Some(timestamp) = timestamp else {
        return false;
    };
    if now.saturating_sub(timestamp).abs() > tolerance_seconds {
        return false;
    }
    signatures.iter().any(|signature| {
        let mut mac = mac_for(secret);
        mac.update(timestamp.to_string().as_bytes());
        mac.update(b".");
        mac.update(body);
        mac.verify_slice(signature).is_ok()
    })
}

/// A webhook as registered: where a topic of a tenant is delivered to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Webhook {
    /// Names the webhook in the API, and its consumer in the broker.
    pub id: WebhookId,
    /// The tenant whose topic is delivered.
    pub tenant: TenantId,
    /// The topic delivered, every envelope in offset order.
    pub topic: Topic,
    /// The `http` or `https` address each envelope is POSTed to.
    pub url: String,
    /// When the webhook was made.
    pub created_at: Timestamp,
}

/// The last delivery that failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookError {
    /// When the attempt failed.
    pub at: Timestamp,
    /// The offset of the envelope that was being delivered.
    pub offset: u64,
    /// What went wrong, without the address.
    pub message: String,
}

/// How a webhook is getting on. The server keeps it in memory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookStatus {
    /// The offset of the last envelope the receiver accepted, or, after a
    /// restart before the next delivery, the last one the webhook moved past.
    /// Absent before the first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_delivered_offset: Option<u64>,
    /// The last failed attempt. It stays until another fails: a delivery that
    /// succeeds does not clear it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<WebhookError>,
}

/// A webhook and how it is getting on, as `webhooks_list` shows it. It holds
/// no secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookInfo {
    /// The webhook.
    #[serde(flatten)]
    pub webhook: Webhook,
    /// How it is getting on.
    #[serde(flatten)]
    pub status: WebhookStatus,
}

/// A change to a webhook that mansio records on `facts.webhook`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WebhookEvent {
    /// Every attempt at one envelope failed, and the webhook moved on.
    Failed,
}

impl WebhookEvent {
    /// Every event.
    pub const ALL: [WebhookEvent; 1] = [WebhookEvent::Failed];

    /// The CloudEvents `type` of the fact: `mansio.webhook.failed`.
    #[must_use]
    pub fn event_type(self) -> &'static str {
        match self {
            WebhookEvent::Failed => "mansio.webhook.failed",
        }
    }

    /// The event a CloudEvents `type` names, if it is one.
    #[must_use]
    pub fn from_event_type(event_type: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|event| event.event_type() == event_type)
    }
}

/// The `data` of a `facts.webhook` envelope. It names no address and no
/// secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookFact {
    /// The webhook that gave up.
    pub webhook_id: WebhookId,
    /// The topic it delivers.
    pub topic: Topic,
    /// The offset of the envelope it did not deliver.
    pub offset: u64,
    /// That envelope's id.
    pub message_id: EventId,
    /// How many times it was tried.
    pub attempts: u32,
    /// What the last attempt failed with.
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &[u8] = br#"{"specversion":"1.0","id":"0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3"}"#;

    fn secret() -> WebhookSecret {
        WebhookSecret::new("whsec_test-secret")
    }

    #[test]
    fn a_signature_matches_an_independent_hmac_sha256() {
        // Computed with Python's hmac and hashlib over "1792000000.<body>".
        assert_eq!(
            sign(&secret(), 1_792_000_000, BODY),
            "t=1792000000,v1=bc2a11368784fc25863ca8e376d93ec761cb9e42b17f71aeed9943a84ac3a07f"
        );
    }

    #[test]
    fn a_signature_verifies_within_the_tolerance_and_not_otherwise() {
        let header = sign(&secret(), 1_000, BODY);
        assert!(verify(&secret(), &header, BODY, 1_000, 300));
        assert!(verify(&secret(), &header, BODY, 1_300, 300));
        assert!(verify(&secret(), &header, BODY, 700, 300));
        assert!(!verify(&secret(), &header, BODY, 1_301, 300));
        assert!(!verify(&secret(), &header, BODY, 699, 300));
    }

    #[test]
    fn a_changed_body_secret_or_timestamp_does_not_verify() {
        let header = sign(&secret(), 1_000, BODY);
        assert!(!verify(&secret(), &header, b"{}", 1_000, 300));
        assert!(!verify(&WebhookSecret::new("other"), &header, BODY, 1_000, 300));
        let moved = header.replacen("t=1000", "t=1001", 1);
        assert!(!verify(&secret(), &moved, BODY, 1_000, 300));
    }

    #[test]
    fn a_malformed_header_does_not_verify() {
        for header in [
            "",
            "t=1000",
            "v1=abcd",
            "t=soon,v1=abcd",
            "t=1000,v1=zz",
            "t=1000,v1=abc",
            "t=1000,v1=",
            "nonsense",
        ] {
            assert!(!verify(&secret(), header, BODY, 1_000, 300), "{header:?}");
        }
    }

    #[test]
    fn any_one_of_several_signatures_is_enough() {
        let good = sign(&secret(), 1_000, BODY);
        let header = format!("t=1000,v1=00ff,{}", good.split_once(',').unwrap().1);
        assert!(verify(&secret(), &header, BODY, 1_000, 300));
    }

    #[test]
    fn a_secret_is_random_prefixed_and_never_shown() {
        let a = WebhookSecret::random();
        assert!(a.expose().starts_with("whsec_"));
        assert_eq!(a.expose().len(), 6 + 64);
        assert_ne!(a.expose(), WebhookSecret::random().expose());
        assert!(!format!("{a:?}").contains("whsec_"));
        let json = serde_json::to_string(&a).unwrap();
        assert_eq!(
            serde_json::from_str::<WebhookSecret>(&json).unwrap().expose(),
            a.expose()
        );
    }

    #[test]
    fn the_listing_of_a_webhook_has_no_secret_and_keeps_only_what_is_set() {
        let info = WebhookInfo {
            webhook: Webhook {
                id: WebhookId::generate(),
                tenant: "acme".parse().unwrap(),
                topic: "facts.build".parse().unwrap(),
                url: "https://example.com/hook".to_owned(),
                created_at: "2026-10-26T12:00:00Z".parse().unwrap(),
            },
            status: WebhookStatus::default(),
        };
        let json = serde_json::to_value(&info).unwrap();
        let mut keys: Vec<&str> = json.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["created_at", "id", "tenant", "topic", "url"]);
        assert_eq!(serde_json::from_value::<WebhookInfo>(json).unwrap(), info);

        let delivered = WebhookInfo {
            status: WebhookStatus {
                last_delivered_offset: Some(4),
                last_error: Some(WebhookError {
                    at: "2026-10-26T12:00:01Z".parse().unwrap(),
                    offset: 4,
                    message: "HTTP 500".to_owned(),
                }),
            },
            ..info
        };
        let json = serde_json::to_value(&delivered).unwrap();
        assert_eq!(json["last_delivered_offset"], 4);
        assert_eq!(json["last_error"]["message"], "HTTP 500");
        assert_eq!(
            serde_json::from_value::<WebhookInfo>(json).unwrap(),
            delivered
        );
    }

    #[test]
    fn the_event_type_maps_back() {
        for event in WebhookEvent::ALL {
            assert!(event.event_type().starts_with("mansio.webhook."));
            assert_eq!(
                WebhookEvent::from_event_type(event.event_type()),
                Some(event)
            );
        }
        assert_eq!(WebhookEvent::from_event_type("mansio.work.failed"), None);
    }
}
