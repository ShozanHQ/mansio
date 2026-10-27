//! Identifiers and other validated text, parsed once at the edge.
//!
//! Every type here is a newtype whose only constructor is `FromStr` (or
//! `Deserialize`, which calls it), so a value in hand has passed its rule.

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::error::ParseError;

/// Longest segment: one leading character plus 62 more.
const SEGMENT_MAX: usize = 63;
/// Longest principal kind.
const KIND_MAX: usize = 32;
/// Longest principal name.
const NAME_MAX: usize = 128;

fn invalid(what: &'static str, value: &str, rule: &'static str) -> ParseError {
    ParseError::Invalid {
        what,
        value: value.to_owned(),
        rule,
    }
}

/// `[a-z0-9][a-z0-9_-]{0,62}`: tenant ids and the parts of a topic name.
fn check_segment(what: &'static str, s: &str) -> Result<(), ParseError> {
    const RULE: &str = "must match [a-z0-9][a-z0-9_-]{0,62}";
    if s.is_empty() {
        return Err(ParseError::Empty { what });
    }
    if s.len() > SEGMENT_MAX {
        return Err(invalid(what, s, RULE));
    }
    let mut chars = s.chars();
    let first_ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    let rest_ok =
        chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if first_ok && rest_ok {
        Ok(())
    } else {
        Err(invalid(what, s, RULE))
    }
}

/// `[a-z][a-z0-9_-]{0,31}`: principal kinds and budget units.
pub(crate) fn check_token(what: &'static str, s: &str) -> Result<(), ParseError> {
    const RULE: &str = "must match [a-z][a-z0-9_-]{0,31}";
    if s.is_empty() {
        return Err(ParseError::Empty { what });
    }
    if s.len() > KIND_MAX {
        return Err(invalid(what, s, RULE));
    }
    let mut chars = s.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_lowercase());
    let rest_ok =
        chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if first_ok && rest_ok {
        Ok(())
    } else {
        Err(invalid(what, s, RULE))
    }
}

/// `[A-Za-z0-9][A-Za-z0-9._@:+-]{0,127}`: principal names.
fn check_name(what: &'static str, s: &str) -> Result<(), ParseError> {
    const RULE: &str = "must match [A-Za-z0-9][A-Za-z0-9._@:+-]{0,127}";
    if s.is_empty() {
        return Err(ParseError::Empty { what });
    }
    if s.len() > NAME_MAX {
        return Err(invalid(what, s, RULE));
    }
    let mut chars = s.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphanumeric());
    let rest_ok =
        chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '@' | ':' | '+' | '-'));
    if first_ok && rest_ok {
        Ok(())
    } else {
        Err(invalid(what, s, RULE))
    }
}

/// Free text with a length cap: not empty, no control characters.
fn check_text(what: &'static str, s: &str, max: usize) -> Result<(), ParseError> {
    if s.is_empty() {
        return Err(ParseError::Empty { what });
    }
    if s.len() > max {
        return Err(ParseError::TooLong { what, max });
    }
    if s.chars().any(char::is_control) {
        return Err(invalid(what, s, "must not contain control characters"));
    }
    Ok(())
}

/// Define a string newtype checked by `$check(what, s)` on every construction.
macro_rules! text_type {
    ($(#[$meta:meta])* $name:ident, $what:expr, $check:expr) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            /// The value as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = ParseError;

            fn from_str(s: &str) -> Result<Self, ParseError> {
                ($check)($what, s)?;
                Ok(Self(s.to_owned()))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let s = Cow::<str>::deserialize(deserializer)?;
                s.parse().map_err(D::Error::custom)
            }
        }
    };
}

text_type!(
    /// A tenant: an isolated set of topics, and one Iggy stream.
    TenantId,
    "tenant",
    check_segment
);
text_type!(
    /// The variable part of a topic name: the `<kind>` of `facts.<kind>` or
    /// the `<queue>` of `work.<queue>`.
    Segment,
    "topic segment",
    check_segment
);
text_type!(
    /// What sort of principal acted: `person`, `agent`, `connector`,
    /// `service`, or another lowercase token.
    PrincipalKind,
    "principal kind",
    check_token
);
text_type!(
    /// The name of a principal, unique within its kind.
    PrincipalName,
    "principal name",
    check_name
);
text_type!(
    /// Where the event happened (CloudEvents `source`).
    Source,
    "source",
    |what, s: &str| check_text(what, s, 1024)
);
text_type!(
    /// What happened (CloudEvents `type`).
    EventType,
    "type",
    |what, s: &str| check_text(what, s, 256)
);
text_type!(
    /// The subject of the event within its source (CloudEvents `subject`).
    Subject,
    "subject",
    |what, s: &str| check_text(what, s, 1024)
);
text_type!(
    /// The chain of work an event belongs to.
    CorrelationId,
    "correlation id",
    |what, s: &str| check_text(what, s, 256)
);
text_type!(
    /// The event that caused this one.
    CausationId,
    "causation id",
    |what, s: &str| check_text(what, s, 256)
);
text_type!(
    /// A producer's key, carried so consumers can deduplicate on it.
    IdempotencyKey,
    "idempotency key",
    |what, s: &str| check_text(what, s, 256)
);
text_type!(
    /// A work item's stable id across retries.
    ItemId,
    "item id",
    |what, s: &str| check_text(what, s, 256)
);
text_type!(
    /// The name a consumer pulls work under. A principal may run several
    /// consumers, and they are told apart by this name.
    ConsumerName,
    "consumer name",
    check_name
);
text_type!(
    /// An event's id in the host ledger it came from.
    RecordId,
    "record id",
    |what, s: &str| check_text(what, s, 256)
);

impl Segment {
    /// A segment from a name that is known to be valid, such as `work` in
    /// `facts.work`. The tests parse every name passed here.
    pub(crate) fn known(name: &'static str) -> Self {
        Self(name.to_owned())
    }
}

/// Read a lowercase hyphenated, non-nil UUID.
fn parse_uuid(what: &'static str, s: &str) -> Result<Uuid, ParseError> {
    const RULE: &str = "must be a lowercase hyphenated UUID";
    let uuid = Uuid::try_parse(s).map_err(|_| invalid(what, s, RULE))?;
    if uuid.hyphenated().to_string() != s {
        return Err(invalid(what, s, RULE));
    }
    if uuid.is_nil() {
        return Err(invalid(what, s, "must not be the nil UUID"));
    }
    Ok(uuid)
}

/// An event id: a lowercase hyphenated UUID, not nil. It is also the Iggy
/// message id, so it must be a nonzero 128-bit value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventId(Uuid);

impl EventId {
    /// A new time-ordered id (UUID v7).
    #[must_use]
    pub fn generate() -> Self {
        Self(Uuid::now_v7())
    }

    /// The id as a 128-bit number, the form Iggy stores.
    #[must_use]
    pub fn as_u128(&self) -> u128 {
        self.0.as_u128()
    }

    /// The id from its 128-bit form; zero is refused.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, which is the nil UUID.
    pub fn from_u128(value: u128) -> Result<Self, ParseError> {
        if value == 0 {
            return Err(invalid("event id", "0", "must not be the nil UUID"));
        }
        Ok(Self(Uuid::from_u128(value)))
    }
}

impl FromStr for EventId {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        parse_uuid("event id", s).map(Self)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.hyphenated())
    }
}

impl Serialize for EventId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for EventId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = Cow::<str>::deserialize(deserializer)?;
        s.parse().map_err(D::Error::custom)
    }
}

/// Define a random (version 4) UUID newtype, so that holding the value is
/// proof of having been given it.
macro_rules! random_uuid_type {
    ($(#[$meta:meta])* $name:ident, $what:expr) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Uuid);

        impl $name {
            /// A new unguessable id.
            #[must_use]
            pub fn generate() -> Self {
                Self(Uuid::new_v4())
            }

            /// The id as a 128-bit number, for comparing in constant time.
            #[must_use]
            pub fn as_u128(&self) -> u128 {
                self.0.as_u128()
            }
        }

        impl FromStr for $name {
            type Err = ParseError;

            fn from_str(s: &str) -> Result<Self, ParseError> {
                parse_uuid($what, s).map(Self)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0.hyphenated())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let s = Cow::<str>::deserialize(deserializer)?;
                s.parse().map_err(D::Error::custom)
            }
        }
    };
}

random_uuid_type!(
    /// The id of a lease on a work item. Only the holder presents it to
    /// acknowledge or fail the item.
    LeaseId,
    "lease id"
);
random_uuid_type!(
    /// The id of a principal the admin API issued a credential to. It names
    /// the principal in the API and in the broker, and is not a secret.
    PrincipalId,
    "principal id"
);
random_uuid_type!(
    /// The id of a webhook. It names the webhook in the API and its consumer
    /// in the broker, and is not a secret.
    WebhookId,
    "webhook id"
);

/// Lowercase hex digits of the bytes.
pub(crate) fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    bytes.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
}

/// The bytes written as exactly `2 * N` lowercase hex digits, if they are.
pub(crate) fn read_hex<const N: usize>(text: &str) -> Option<[u8; N]> {
    if text.len() != 2 * N || !text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let mut bytes = [0_u8; N];
    for (byte, pair) in bytes.iter_mut().zip(text.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(bytes)
}

/// What the facts record of a lease in place of its id: the first 16 bytes of
/// a BLAKE3 digest of the id, as 32 lowercase hex digits. The id is the
/// holder's proof, and every reader of `facts.work` sees the fact, so the fact
/// can say which lease it is about and cannot be used to act under it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaseFingerprint([u8; 16]);

impl LeaseFingerprint {
    /// The bytes, for comparing in constant time.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl LeaseId {
    /// The fingerprint the facts record for this lease.
    #[must_use]
    pub fn fingerprint(&self) -> LeaseFingerprint {
        let digest = blake3::hash(&self.as_u128().to_be_bytes());
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        LeaseFingerprint(bytes)
    }
}

impl fmt::Display for LeaseFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(f, &self.0)
    }
}

impl FromStr for LeaseFingerprint {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        read_hex(s)
            .map(Self)
            .ok_or_else(|| invalid("lease fingerprint", s, "must be 32 lowercase hex digits"))
    }
}

impl Serialize for LeaseFingerprint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for LeaseFingerprint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = Cow::<str>::deserialize(deserializer)?;
        s.parse().map_err(D::Error::custom)
    }
}

/// Who acted: a kind and a name, written `kind:name`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Principal {
    /// The kind of principal.
    pub kind: PrincipalKind,
    /// The name within that kind.
    pub name: PrincipalName,
}

impl Principal {
    /// A principal from its two parts.
    #[must_use]
    pub fn new(kind: PrincipalKind, name: PrincipalName) -> Self {
        Self { kind, name }
    }
}

impl FromStr for Principal {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        let (kind, name) = s
            .split_once(':')
            .ok_or_else(|| invalid("principal", s, "must be written kind:name"))?;
        Ok(Self {
            kind: kind.parse()?,
            name: name.parse()?,
        })
    }
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.kind, self.name)
    }
}

impl Serialize for Principal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Principal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = Cow::<str>::deserialize(deserializer)?;
        s.parse().map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_follow_the_rule() {
        for ok in ["a", "0", "build", "a-b_c", "x9", &"a".repeat(63)] {
            assert!(ok.parse::<TenantId>().is_ok(), "{ok} should parse");
        }
        for bad in ["", "A", "-a", "_a", "a.b", "a b", "é", &"a".repeat(64)] {
            assert!(
                bad.parse::<TenantId>().is_err(),
                "{bad:?} should be refused"
            );
        }
    }

    #[test]
    fn principal_kind_is_a_lowercase_token() {
        for ok in ["person", "agent", "connector", "service", "x-y_9"] {
            assert!(ok.parse::<PrincipalKind>().is_ok(), "{ok}");
        }
        for bad in ["", "Person", "9a", "-a", "a b", "a:b", &"a".repeat(33)] {
            assert!(bad.parse::<PrincipalKind>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn principal_round_trips() {
        let p: Principal = "service:mansio".parse().unwrap();
        assert_eq!(p.kind.as_str(), "service");
        assert_eq!(p.name.as_str(), "mansio");
        assert_eq!(p.to_string(), "service:mansio");

        let p: Principal = "person:paul@example.com".parse().unwrap();
        assert_eq!(p.to_string(), "person:paul@example.com");

        let p: Principal = "agent:a:b".parse().unwrap();
        assert_eq!(p.name.as_str(), "a:b");
    }

    #[test]
    fn principal_refuses_malformed_input() {
        for bad in [
            "",
            "service",
            ":x",
            "service:",
            "Service:x",
            "service: x",
            "service:-x",
        ] {
            assert!(bad.parse::<Principal>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn event_id_is_a_canonical_nonzero_uuid() {
        let id = "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3";
        assert_eq!(id.parse::<EventId>().unwrap().to_string(), id);

        for bad in [
            "",
            "0192F1C2-7D3E-7A4B-8C5D-6E7F8091A2B3",
            "0192f1c27d3e7a4b8c5d6e7f8091a2b3",
            "urn:uuid:0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3",
            "00000000-0000-0000-0000-000000000000",
            "not-a-uuid",
        ] {
            assert!(bad.parse::<EventId>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn lease_ids_are_random_canonical_uuids() {
        let a = LeaseId::generate();
        let b = LeaseId::generate();
        assert_ne!(a, b);
        assert_eq!(a.to_string().parse::<LeaseId>().unwrap(), a);
        // Version 4: the first digit of the third group.
        assert_eq!(a.to_string().as_bytes()[14], b'4');
        for bad in ["", "nope", "00000000-0000-0000-0000-000000000000"] {
            assert!(bad.parse::<LeaseId>().is_err(), "{bad:?}");
        }
        let json = serde_json::to_string(&a).unwrap();
        assert_eq!(serde_json::from_str::<LeaseId>(&json).unwrap(), a);
    }

    #[test]
    fn webhook_ids_are_random_canonical_uuids() {
        let a = WebhookId::generate();
        assert_ne!(a, WebhookId::generate());
        assert_eq!(a.to_string().parse::<WebhookId>().unwrap(), a);
        assert!("webhook".parse::<WebhookId>().is_err());
        // A consumer called after a webhook is a valid consumer name.
        assert!(format!("webhook-{a}").parse::<ConsumerName>().is_ok());
    }

    #[test]
    fn a_lease_fingerprint_names_the_lease_without_being_it() {
        let lease = LeaseId::generate();
        let fingerprint = lease.fingerprint();
        assert_eq!(fingerprint, lease.fingerprint());
        assert_ne!(fingerprint, LeaseId::generate().fingerprint());
        let text = fingerprint.to_string();
        assert_eq!(text.len(), 32);
        assert!(!lease.to_string().replace('-', "").contains(&text));
        assert_eq!(text.parse::<LeaseFingerprint>().unwrap(), fingerprint);
        let json = serde_json::to_string(&fingerprint).unwrap();
        assert_eq!(json, format!("\"{text}\""));
        assert_eq!(
            serde_json::from_str::<LeaseFingerprint>(&json).unwrap(),
            fingerprint
        );
        for bad in ["", "abc", &"A".repeat(32), &"g".repeat(32), &"a".repeat(33)] {
            assert!(bad.parse::<LeaseFingerprint>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn consumer_names_follow_the_principal_name_rule() {
        assert!("worker-1".parse::<ConsumerName>().is_ok());
        assert!("".parse::<ConsumerName>().is_err());
        assert!("-x".parse::<ConsumerName>().is_err());
        assert!("a b".parse::<ConsumerName>().is_err());
    }

    #[test]
    fn principals_serialize_as_kind_colon_name() {
        let p: Principal = "person:paul@example.com".parse().unwrap();
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "\"person:paul@example.com\"");
        assert_eq!(serde_json::from_str::<Principal>(&json).unwrap(), p);
        assert!(serde_json::from_str::<Principal>("\"nobody\"").is_err());
    }

    #[test]
    fn event_id_converts_to_and_from_u128() {
        let id = EventId::generate();
        assert_eq!(EventId::from_u128(id.as_u128()).unwrap(), id);
        assert!(EventId::from_u128(0).is_err());
    }

    #[test]
    fn text_types_refuse_empty_long_and_control_characters() {
        assert!("".parse::<Source>().is_err());
        assert!("a\nb".parse::<Source>().is_err());
        assert!("a".repeat(1025).parse::<Source>().is_err());
        assert!("a".repeat(1024).parse::<Source>().is_ok());
        assert!("https://example.com/build?x=1".parse::<Source>().is_ok());
        assert!("a".repeat(257).parse::<CorrelationId>().is_err());
    }

    #[test]
    fn serde_goes_through_the_rule() {
        let id: TenantId = serde_json::from_str("\"acme\"").unwrap();
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"acme\"");
        assert!(serde_json::from_str::<TenantId>("\"Acme\"").is_err());
        assert!(serde_json::from_str::<TenantId>("7").is_err());
    }
}
