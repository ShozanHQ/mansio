//! The envelope: CloudEvents 1.0 plus mansio's extension attributes.
//!
//! The wire form is the CloudEvents JSON format. Every extension attribute is
//! a scalar, as CloudEvents requires, so the optional [`Record`] travels as
//! the three attributes `recordid`, `recordprevhash` and `recordhash`.
//! Unknown attributes are refused.

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::error::{EnvelopeError, ParseError};
use crate::id::{
    CausationId, CorrelationId, EventId, EventType, IdempotencyKey, ItemId, Principal,
    PrincipalKind, PrincipalName, RecordId, Source, Subject, TenantId, check_token,
};
use crate::topic::Topic;

fn invalid(what: &'static str, value: &str, rule: &'static str) -> ParseError {
    ParseError::Invalid {
        what,
        value: value.to_owned(),
        rule,
    }
}

/// An RFC 3339 timestamp, kept exactly as written so that it survives the
/// bus byte for byte.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Timestamp(String);

impl Timestamp {
    /// The current time in UTC.
    ///
    /// # Errors
    ///
    /// Returns an error if the clock is outside the years RFC 3339 can write.
    pub fn now() -> Result<Self, ParseError> {
        OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|_| invalid("time", "now", "the clock is outside RFC 3339 range"))
            .map(Self)
    }

    /// The UTC time `millis` milliseconds after the Unix epoch.
    ///
    /// # Errors
    ///
    /// Returns an error if the time is outside the years RFC 3339 can write.
    pub fn from_unix_millis(millis: i64) -> Result<Self, ParseError> {
        let out_of_range = || invalid("time", &millis.to_string(), "is outside RFC 3339 range");
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)
            .map_err(|_| out_of_range())?
            .format(&Rfc3339)
            .map_err(|_| out_of_range())
            .map(Self)
    }

    /// Milliseconds since the Unix epoch, rounded down, or `None` if the
    /// timestamp is outside the range of an `i64` of milliseconds.
    #[must_use]
    pub fn unix_millis(&self) -> Option<i64> {
        let parsed = OffsetDateTime::parse(&self.0, &Rfc3339).ok()?;
        i64::try_from(parsed.unix_timestamp_nanos().div_euclid(1_000_000)).ok()
    }

    /// The timestamp as written.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Timestamp {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        OffsetDateTime::parse(s, &Rfc3339)
            .map_err(|_| invalid("time", s, "must be an RFC 3339 timestamp"))?;
        Ok(Self(s.to_owned()))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The content type of `data`. JSON is the only one mansio carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataContentType {
    /// `application/json`
    Json,
}

impl DataContentType {
    const JSON: &'static str = "application/json";
}

impl FromStr for DataContentType {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        if s == Self::JSON {
            Ok(Self::Json)
        } else {
            Err(invalid(
                "datacontenttype",
                s,
                "only application/json is carried",
            ))
        }
    }
}

impl fmt::Display for DataContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::JSON)
    }
}

/// The scope an action ran under, for example `work.builds:claim`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Scope(String);

impl Scope {
    /// The scope as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Scope {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        const RULE: &str = "must be 1 to 128 characters from [a-z0-9._*:-]";
        if s.is_empty() {
            return Err(ParseError::Empty { what: "scope" });
        }
        let ok = s.len() <= 128
            && s.chars().all(|c| {
                c.is_ascii_lowercase()
                    || c.is_ascii_digit()
                    || matches!(c, '.' | '_' | '*' | ':' | '-')
            });
        if ok {
            Ok(Self(s.to_owned()))
        } else {
            Err(invalid("scope", s, RULE))
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A spending limit: `<amount> <unit>`, for example `30 usd` or
/// `200000 tokens`. The amount is kept as written.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Budget {
    amount: String,
    unit: String,
}

impl Budget {
    /// The amount, a non-negative decimal number as written.
    #[must_use]
    pub fn amount(&self) -> &str {
        &self.amount
    }

    /// The unit, a lowercase token such as `usd` or `tokens`.
    #[must_use]
    pub fn unit(&self) -> &str {
        &self.unit
    }
}

impl FromStr for Budget {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        const RULE: &str = "must be \"<amount> <unit>\", for example \"30 usd\"";
        let (amount, unit) = s
            .split_once(' ')
            .ok_or_else(|| invalid("budget", s, RULE))?;
        let digits = |part: &str| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit());
        let amount_ok = amount.len() <= 24
            && match amount.split_once('.') {
                None => digits(amount),
                Some((whole, fraction)) => digits(whole) && digits(fraction),
            };
        if !amount_ok {
            return Err(invalid("budget", s, RULE));
        }
        check_token("budget unit", unit).map_err(|_| invalid("budget", s, RULE))?;
        Ok(Self {
            amount: amount.to_owned(),
            unit: unit.to_owned(),
        })
    }
}

impl fmt::Display for Budget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.amount, self.unit)
    }
}

/// Where an action stands in an approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApprovalState {
    /// No approval is involved.
    None,
    /// Approval has been asked for.
    Requested,
    /// Approval was given.
    Approved,
    /// Approval was refused.
    Rejected,
}

impl ApprovalState {
    /// The lowercase name used on the wire.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ApprovalState::None => "none",
            ApprovalState::Requested => "requested",
            ApprovalState::Approved => "approved",
            ApprovalState::Rejected => "rejected",
        }
    }
}

impl FromStr for ApprovalState {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        match s {
            "none" => Ok(ApprovalState::None),
            "requested" => Ok(ApprovalState::Requested),
            "approved" => Ok(ApprovalState::Approved),
            "rejected" => Ok(ApprovalState::Rejected),
            _ => Err(invalid(
                "approval state",
                s,
                "must be none, requested, approved or rejected",
            )),
        }
    }
}

impl fmt::Display for ApprovalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A hash as 64 lowercase hex digits (SHA-256).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Hash(String);

impl Hash {
    /// The hash as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Hash {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        let ok = s.len() == 64
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if ok {
            Ok(Self(s.to_owned()))
        } else {
            Err(invalid("hash", s, "must be 64 lowercase hex digits"))
        }
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where an event sits in the hash-chained log of the host it came from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Record {
    /// The event's id in the host's log.
    pub id: RecordId,
    /// The hash of the previous event in that log.
    pub prev_hash: Hash,
    /// This event's own hash.
    pub event_hash: Hash,
}

macro_rules! from_str_serde {
    ($($name:ident),+ $(,)?) => {$(
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
    )+};
}

from_str_serde!(
    Timestamp,
    DataContentType,
    Scope,
    Budget,
    ApprovalState,
    Hash
);

/// The only CloudEvents version mansio speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SpecVersion;

impl Serialize for SpecVersion {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("1.0")
    }
}

impl<'de> Deserialize<'de> for SpecVersion {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = Cow::<str>::deserialize(deserializer)?;
        if s == "1.0" {
            Ok(Self)
        } else {
            Err(D::Error::custom(format!(
                "unsupported specversion {s:?}, expected \"1.0\""
            )))
        }
    }
}

/// A message on the bus.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Wire", into = "Wire")]
pub struct Envelope {
    /// Unique id, also the Iggy message id.
    pub id: EventId,
    /// Where the event happened.
    pub source: Source,
    /// What happened.
    pub event_type: EventType,
    /// The subject within the source.
    pub subject: Option<Subject>,
    /// When it happened.
    pub time: Option<Timestamp>,
    /// The content type of `data`.
    pub datacontenttype: Option<DataContentType>,
    /// The payload.
    pub data: Option<Value>,
    /// The tenant, which is the Iggy stream.
    pub tenant: TenantId,
    /// The topic it was published to.
    pub topic: Topic,
    /// Who published it, taken from the credential and never from a request.
    pub principal: Principal,
    /// The chain of work this belongs to.
    pub correlation_id: Option<CorrelationId>,
    /// The event that caused this one.
    pub causation_id: Option<CausationId>,
    /// The producer's key for deduplication.
    pub idempotency_key: Option<IdempotencyKey>,
    /// The scope the action ran under.
    pub scope: Option<Scope>,
    /// The spending limit.
    pub budget: Option<Budget>,
    /// Where an approval stands.
    pub approval_state: Option<ApprovalState>,
    /// Where it sits in the host's chained log.
    pub record: Option<Record>,
    /// A work item's stable id across retries.
    pub item_id: Option<ItemId>,
    /// A work item's attempt number, from 1.
    pub attempt: Option<u32>,
}

impl Envelope {
    /// An envelope with the required attributes and nothing else.
    #[must_use]
    pub fn new(
        id: EventId,
        source: Source,
        event_type: EventType,
        tenant: TenantId,
        topic: Topic,
        principal: Principal,
    ) -> Self {
        Self {
            id,
            source,
            event_type,
            subject: None,
            time: None,
            datacontenttype: None,
            data: None,
            tenant,
            topic,
            principal,
            correlation_id: None,
            causation_id: None,
            idempotency_key: None,
            scope: None,
            budget: None,
            approval_state: None,
            record: None,
            item_id: None,
            attempt: None,
        }
    }

    /// Check the rules that span attributes. Each attribute is already valid
    /// by construction.
    ///
    /// # Errors
    ///
    /// Returns an error if `attempt` is zero, or is set without `itemid`.
    pub fn validate(&self) -> Result<(), EnvelopeError> {
        match (self.attempt, &self.item_id) {
            (Some(0), _) => Err(EnvelopeError::Inconsistent("attempt counts from 1")),
            (Some(_), None) => Err(EnvelopeError::Inconsistent("attempt needs an itemid")),
            _ => Ok(()),
        }
    }

    /// Read an envelope from CloudEvents JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the JSON is malformed, an attribute is missing,
    /// unknown or invalid, or the attributes contradict each other.
    pub fn from_json_slice(bytes: &[u8]) -> Result<Self, EnvelopeError> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// Read an envelope from a JSON value.
    ///
    /// # Errors
    ///
    /// As [`Envelope::from_json_slice`].
    pub fn from_json_value(value: Value) -> Result<Self, EnvelopeError> {
        Ok(serde_json::from_value(value)?)
    }

    /// Write the envelope as CloudEvents JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the envelope contradicts itself.
    pub fn to_json_vec(&self) -> Result<Vec<u8>, EnvelopeError> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    /// The envelope as a JSON value.
    ///
    /// # Errors
    ///
    /// Returns an error if the envelope contradicts itself.
    pub fn to_json_value(&self) -> Result<Value, EnvelopeError> {
        self.validate()?;
        Ok(serde_json::to_value(self)?)
    }
}

/// The flat CloudEvents form of an [`Envelope`].
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    specversion: SpecVersion,
    id: EventId,
    source: Source,
    #[serde(rename = "type")]
    event_type: EventType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    subject: Option<Subject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    time: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    datacontenttype: Option<DataContentType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
    tenant: TenantId,
    topic: Topic,
    principalkind: PrincipalKind,
    principalname: PrincipalName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    correlationid: Option<CorrelationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    causationid: Option<CausationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    idempotencykey: Option<IdempotencyKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scope: Option<Scope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    budget: Option<Budget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    approvalstate: Option<ApprovalState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recordid: Option<RecordId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recordprevhash: Option<Hash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recordhash: Option<Hash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    itemid: Option<ItemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attempt: Option<u32>,
}

impl TryFrom<Wire> for Envelope {
    type Error = EnvelopeError;

    fn try_from(wire: Wire) -> Result<Self, EnvelopeError> {
        let record = match (wire.recordid, wire.recordprevhash, wire.recordhash) {
            (Some(id), Some(prev_hash), Some(event_hash)) => Some(Record {
                id,
                prev_hash,
                event_hash,
            }),
            (None, None, None) => None,
            _ => {
                return Err(EnvelopeError::Inconsistent(
                    "recordid, recordprevhash and recordhash are set together or not at all",
                ));
            }
        };
        let envelope = Envelope {
            id: wire.id,
            source: wire.source,
            event_type: wire.event_type,
            subject: wire.subject,
            time: wire.time,
            datacontenttype: wire.datacontenttype,
            data: wire.data,
            tenant: wire.tenant,
            topic: wire.topic,
            principal: Principal::new(wire.principalkind, wire.principalname),
            correlation_id: wire.correlationid,
            causation_id: wire.causationid,
            idempotency_key: wire.idempotencykey,
            scope: wire.scope,
            budget: wire.budget,
            approval_state: wire.approvalstate,
            record,
            item_id: wire.itemid,
            attempt: wire.attempt,
        };
        envelope.validate()?;
        Ok(envelope)
    }
}

impl From<Envelope> for Wire {
    fn from(envelope: Envelope) -> Self {
        let (recordid, recordprevhash, recordhash) = match envelope.record {
            Some(record) => (
                Some(record.id),
                Some(record.prev_hash),
                Some(record.event_hash),
            ),
            None => (None, None, None),
        };
        Wire {
            specversion: SpecVersion,
            id: envelope.id,
            source: envelope.source,
            event_type: envelope.event_type,
            subject: envelope.subject,
            time: envelope.time,
            datacontenttype: envelope.datacontenttype,
            data: envelope.data,
            tenant: envelope.tenant,
            topic: envelope.topic,
            principalkind: envelope.principal.kind,
            principalname: envelope.principal.name,
            correlationid: envelope.correlation_id,
            causationid: envelope.causation_id,
            idempotencykey: envelope.idempotency_key,
            scope: envelope.scope,
            budget: envelope.budget,
            approvalstate: envelope.approval_state,
            recordid,
            recordprevhash,
            recordhash,
            itemid: envelope.item_id,
            attempt: envelope.attempt,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn full() -> Value {
        json!({
            "specversion": "1.0",
            "id": "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3",
            "source": "https://example.com/builder",
            "type": "build.finished",
            "subject": "build/42",
            "time": "2026-10-26T12:00:00.123456789Z",
            "datacontenttype": "application/json",
            "data": {"ok": true, "n": [1, 2, {"deep": null}], "s": "é"},
            "tenant": "acme",
            "topic": "work.builds",
            "principalkind": "agent",
            "principalname": "builder-1",
            "correlationid": "corr-1",
            "causationid": "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b0",
            "idempotencykey": "key-1",
            "scope": "work.builds:claim",
            "budget": "30.50 usd",
            "approvalstate": "requested",
            "recordid": "rec-9",
            "recordprevhash": HASH_A,
            "recordhash": HASH_B,
            "itemid": "item-7",
            "attempt": 2
        })
    }

    fn minimal() -> Value {
        json!({
            "specversion": "1.0",
            "id": "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3",
            "source": "s",
            "type": "t",
            "tenant": "acme",
            "topic": "facts.build",
            "principalkind": "service",
            "principalname": "mansio"
        })
    }

    #[test]
    fn a_full_envelope_round_trips_exactly() {
        let envelope = Envelope::from_json_value(full()).unwrap();
        assert_eq!(envelope.attempt, Some(2));
        assert_eq!(envelope.budget.as_ref().unwrap().unit(), "usd");
        assert_eq!(
            envelope.record.as_ref().unwrap().event_hash.as_str(),
            HASH_B
        );
        assert_eq!(envelope.to_json_value().unwrap(), full());

        let bytes = envelope.to_json_vec().unwrap();
        assert_eq!(Envelope::from_json_slice(&bytes).unwrap(), envelope);
    }

    #[test]
    fn a_minimal_envelope_round_trips_without_optional_attributes() {
        let envelope = Envelope::from_json_value(minimal()).unwrap();
        assert_eq!(envelope.to_json_value().unwrap(), minimal());
        assert!(envelope.record.is_none());
    }

    #[test]
    fn unknown_attributes_are_refused() {
        let mut value = minimal();
        value["surprise"] = json!(1);
        assert!(Envelope::from_json_value(value).is_err());
    }

    #[test]
    fn required_attributes_are_required() {
        for name in [
            "specversion",
            "id",
            "source",
            "type",
            "tenant",
            "topic",
            "principalkind",
            "principalname",
        ] {
            let mut value = minimal();
            value.as_object_mut().unwrap().remove(name);
            let error = Envelope::from_json_value(value).unwrap_err().to_string();
            assert!(error.contains(name), "{name}: {error}");
        }
    }

    #[test]
    fn specversion_must_be_1_0() {
        let mut value = minimal();
        value["specversion"] = json!("0.3");
        assert!(Envelope::from_json_value(value).is_err());
    }

    #[test]
    fn bad_attributes_are_refused() {
        let cases = [
            ("id", json!("not-a-uuid")),
            ("id", json!("00000000-0000-0000-0000-000000000000")),
            ("source", json!("")),
            ("type", json!("")),
            ("time", json!("yesterday")),
            ("time", json!("2026-10-26 12:00:00")),
            ("datacontenttype", json!("text/plain")),
            ("tenant", json!("Acme")),
            ("topic", json!("facts")),
            ("topic", json!("nope.x")),
            ("principalkind", json!("Agent")),
            ("principalname", json!("")),
            ("scope", json!("Work Builds")),
            ("budget", json!("30")),
            ("budget", json!("30 USD")),
            ("budget", json!("-3 usd")),
            ("budget", json!("3. usd")),
            ("budget", json!(".5 usd")),
            ("budget", json!("1e3 usd")),
            ("budget", json!("30  usd")),
            ("budget", json!("30 usd ")),
            ("approvalstate", json!("maybe")),
            ("approvalstate", json!("Approved")),
            ("attempt", json!("1")),
            ("attempt", json!(-1)),
            ("attempt", json!(1.5)),
        ];
        for (name, bad) in cases {
            let mut value = full();
            value[name] = bad.clone();
            assert!(
                Envelope::from_json_value(value).is_err(),
                "{name}={bad} should be refused"
            );
        }
    }

    #[test]
    fn hashes_are_64_lowercase_hex_digits() {
        for bad in [
            "",
            "abc",
            &"A".repeat(64),
            &"g".repeat(64),
            &"a".repeat(63),
            &"a".repeat(65),
        ] {
            for name in ["recordprevhash", "recordhash"] {
                let mut value = full();
                value[name] = json!(bad);
                assert!(Envelope::from_json_value(value).is_err(), "{name}={bad}");
            }
        }
    }

    #[test]
    fn record_attributes_come_together_or_not_at_all() {
        for name in ["recordid", "recordprevhash", "recordhash"] {
            let mut value = full();
            value.as_object_mut().unwrap().remove(name);
            let error = Envelope::from_json_value(value).unwrap_err().to_string();
            assert!(error.contains("together"), "{name}: {error}");
        }
    }

    #[test]
    fn attempt_counts_from_one_and_needs_an_item() {
        let mut value = full();
        value["attempt"] = json!(0);
        assert!(Envelope::from_json_value(value).is_err());

        let mut value = full();
        value.as_object_mut().unwrap().remove("itemid");
        assert!(Envelope::from_json_value(value).is_err());

        let mut value = full();
        value.as_object_mut().unwrap().remove("attempt");
        assert!(Envelope::from_json_value(value).is_ok());
    }

    #[test]
    fn budgets_keep_their_text() {
        for text in ["30 usd", "200000 tokens", "0.5 eur", "7 credits_v2"] {
            let budget: Budget = text.parse().unwrap();
            assert_eq!(budget.to_string(), text);
        }
        assert!("1".repeat(25).parse::<Budget>().is_err());
        assert!(format!("{} usd", "1".repeat(24)).parse::<Budget>().is_ok());
    }

    #[test]
    fn approval_states_round_trip() {
        for state in ["none", "requested", "approved", "rejected"] {
            assert_eq!(state.parse::<ApprovalState>().unwrap().to_string(), state);
        }
    }

    #[test]
    fn times_are_kept_as_written() {
        for text in [
            "2026-10-26T12:00:00Z",
            "2026-10-26T12:00:00+00:00",
            "2026-10-26T12:00:00.500+02:00",
        ] {
            assert_eq!(text.parse::<Timestamp>().unwrap().as_str(), text);
        }
        assert!(
            Timestamp::now()
                .unwrap()
                .as_str()
                .parse::<Timestamp>()
                .is_ok()
        );
    }

    #[test]
    fn times_convert_to_and_from_unix_milliseconds() {
        let at = Timestamp::from_unix_millis(1_792_000_000_123).unwrap();
        assert_eq!(at.as_str(), "2026-10-14T17:46:40.123Z");
        assert_eq!(at.unix_millis(), Some(1_792_000_000_123));
        assert_eq!(
            Timestamp::from_unix_millis(0).unwrap().as_str(),
            "1970-01-01T00:00:00Z"
        );
        let offset: Timestamp = "2026-10-26T12:00:00.500+02:00".parse().unwrap();
        assert_eq!(offset.unix_millis(), Some(1_793_008_800_500));
        assert!(Timestamp::from_unix_millis(i64::MAX).is_err());
    }

    #[test]
    fn validation_catches_a_hand_built_contradiction() {
        let mut envelope = Envelope::from_json_value(minimal()).unwrap();
        envelope.attempt = Some(1);
        assert!(envelope.to_json_vec().is_err());
        envelope.item_id = Some("i".parse().unwrap());
        assert!(envelope.to_json_vec().is_ok());
    }
}
