//! What a principal may do: scopes made of a topic pattern and verbs.
//!
//! A scope names the topics it covers (`work.builds`, `facts.*`, `alerts`) and
//! the verbs it grants there. [`allows`] answers whether scopes let a request
//! of some kind through. A broker enforces only reading for a credential:
//! every write goes through the server, which checks `publish` here, stamps the
//! principal from the credential and writes as its own admin identity.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::ParseError;
use crate::topic::Topic;

/// What a scope lets its principal do on the topics it covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verb {
    /// Publish through the server: `messages`, `work_publish`, `alerts_publish`.
    /// The server writes, as the principal the credential stands for. Not on a
    /// topic only mansio writes.
    Publish,
    /// Read messages: the broker's poll permission.
    Subscribe,
    /// Read messages, and claim items from a work queue, and ack or fail the
    /// leases the holder took there.
    Claim,
    /// Acknowledge alerts. Only on `alerts`.
    Ack,
    /// Claim items from `work.approvals`, and read it. Only there.
    Approve,
}

impl Verb {
    /// The lowercase name used in the API.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Verb::Publish => "publish",
            Verb::Subscribe => "subscribe",
            Verb::Claim => "claim",
            Verb::Ack => "ack",
            Verb::Approve => "approve",
        }
    }
}

impl fmt::Display for Verb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The topics a scope covers: one topic, or every `facts.<kind>` or every
/// `work.<queue>` of the tenant, including ones created later. There is no
/// pattern for the whole stream.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TopicPattern {
    /// Exactly this topic.
    Topic(Topic),
    /// `facts.*`
    AllFacts,
    /// `work.*`
    AllWork,
}

impl TopicPattern {
    /// Whether the topic is one the pattern covers.
    #[must_use]
    pub fn matches(&self, topic: &Topic) -> bool {
        match self {
            TopicPattern::Topic(own) => own == topic,
            TopicPattern::AllFacts => matches!(topic, Topic::Facts(_)),
            TopicPattern::AllWork => matches!(topic, Topic::Work(_)),
        }
    }
}

impl FromStr for TopicPattern {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        match s {
            "facts.*" => Ok(TopicPattern::AllFacts),
            "work.*" => Ok(TopicPattern::AllWork),
            _ => s
                .parse()
                .map(TopicPattern::Topic)
                .map_err(|_| ParseError::Invalid {
                    what: "topic pattern",
                    value: s.to_owned(),
                    rule: "must be a topic, facts.* or work.*",
                }),
        }
    }
}

impl fmt::Display for TopicPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TopicPattern::Topic(topic) => write!(f, "{topic}"),
            TopicPattern::AllFacts => f.write_str("facts.*"),
            TopicPattern::AllWork => f.write_str("work.*"),
        }
    }
}

impl Serialize for TopicPattern {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for TopicPattern {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = Cow::<str>::deserialize(deserializer)?;
        s.parse().map_err(D::Error::custom)
    }
}

/// What a request does, for the purpose of deciding whether a credential may.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Send a message to a topic.
    Send,
    /// Read messages from a topic.
    Poll,
    /// Claim an item from a work queue, and ack or fail it.
    Claim,
    /// Acknowledge an alert.
    Ack,
}

/// One scope of a principal: the topics it covers and the verbs granted there.
///
/// The verbs must make sense where they are granted: `claim` on work topics,
/// `approve` on `work.approvals`, `ack` on `alerts`. A scope that grants a verb
/// nowhere it could apply is refused when it is made, so a typo does not
/// become a credential that quietly lacks the access it was meant to have.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "GrantWire")]
pub struct Grant {
    topic: TopicPattern,
    verbs: BTreeSet<Verb>,
}

/// A [`Grant`] as it is written, before it is checked.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantWire {
    topic: TopicPattern,
    verbs: Vec<Verb>,
}

impl TryFrom<GrantWire> for Grant {
    type Error = ParseError;

    fn try_from(wire: GrantWire) -> Result<Self, ParseError> {
        Grant::new(wire.topic, wire.verbs)
    }
}

impl Grant {
    /// A scope over `topic` granting `verbs`.
    ///
    /// # Errors
    ///
    /// Returns an error if there are no verbs, or a verb cannot apply to the
    /// topics covered.
    pub fn new(
        topic: TopicPattern,
        verbs: impl IntoIterator<Item = Verb>,
    ) -> Result<Self, ParseError> {
        let verbs: BTreeSet<Verb> = verbs.into_iter().collect();
        let refuse = |rule: &'static str| ParseError::Invalid {
            what: "scope",
            value: format!("{topic} {}", verb_list(&verbs)),
            rule,
        };
        if verbs.is_empty() {
            return Err(refuse("must grant at least one verb"));
        }
        if verbs.contains(&Verb::Publish)
            && matches!(&topic, TopicPattern::Topic(named) if named.is_written_by_mansio())
        {
            return Err(refuse("only mansio publishes to this topic"));
        }
        let covers_work = matches!(
            topic,
            TopicPattern::AllWork | TopicPattern::Topic(Topic::Work(_))
        );
        if verbs.contains(&Verb::Claim) && !covers_work {
            return Err(refuse("claim applies to work topics"));
        }
        let covers_approvals =
            topic == TopicPattern::AllWork || topic == TopicPattern::Topic(Topic::work_approvals());
        if verbs.contains(&Verb::Approve) && !covers_approvals {
            return Err(refuse("approve applies to work.approvals"));
        }
        if verbs.contains(&Verb::Ack) && topic != TopicPattern::Topic(Topic::Alerts) {
            return Err(refuse("ack applies to alerts"));
        }
        Ok(Self { topic, verbs })
    }

    /// A scope read back from where it was kept, without checking it against
    /// today's rules: it was checked when it was made, and the rules may have
    /// changed since. Reading it back must not lose the principal that holds
    /// it. It still allows no more than [`allows`] lets through: a `publish`
    /// on a topic only mansio writes sends nothing.
    #[must_use]
    pub fn from_stored(topic: TopicPattern, verbs: impl IntoIterator<Item = Verb>) -> Self {
        Self {
            topic,
            verbs: verbs.into_iter().collect(),
        }
    }

    /// The topics the scope covers.
    #[must_use]
    pub fn topic(&self) -> &TopicPattern {
        &self.topic
    }

    /// The verbs the scope grants.
    #[must_use]
    pub fn verbs(&self) -> &BTreeSet<Verb> {
        &self.verbs
    }

    fn permits(&self, action: Action, topic: &Topic) -> bool {
        if !self.topic.matches(topic) {
            return false;
        }
        let has = |verb| self.verbs.contains(&verb);
        let approves = has(Verb::Approve) && *topic == Topic::work_approvals();
        match action {
            Action::Send => has(Verb::Publish) && !topic.is_written_by_mansio(),
            Action::Poll => has(Verb::Subscribe) || has(Verb::Claim) || approves,
            Action::Claim => matches!(topic, Topic::Work(_)) && (has(Verb::Claim) || approves),
            Action::Ack => *topic == Topic::Alerts && has(Verb::Ack),
        }
    }
}

fn verb_list(verbs: &BTreeSet<Verb>) -> String {
    verbs
        .iter()
        .map(|verb| verb.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

/// Whether the scopes let a request of this kind through on a topic.
#[must_use]
pub fn allows(grants: &[Grant], action: Action, topic: &Topic) -> bool {
    grants.iter().any(|grant| grant.permits(action, topic))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(name: &str) -> Topic {
        name.parse().unwrap()
    }

    fn grant(pattern: &str, verbs: &[Verb]) -> Grant {
        Grant::new(pattern.parse().unwrap(), verbs.iter().copied()).unwrap()
    }

    #[test]
    fn patterns_cover_one_topic_or_a_whole_family() {
        let exact: TopicPattern = "work.builds".parse().unwrap();
        assert!(exact.matches(&topic("work.builds")));
        assert!(!exact.matches(&topic("work.reviews")));
        assert!(!exact.matches(&topic("facts.builds")));

        let facts: TopicPattern = "facts.*".parse().unwrap();
        assert!(facts.matches(&topic("facts.build")));
        assert!(facts.matches(&topic("facts.signal")));
        assert!(!facts.matches(&topic("work.builds")));
        assert!(!facts.matches(&Topic::Alerts));

        let work: TopicPattern = "work.*".parse().unwrap();
        assert!(work.matches(&topic("work.approvals")));
        assert!(!work.matches(&topic("facts.work")));

        let alerts: TopicPattern = "alerts".parse().unwrap();
        assert!(alerts.matches(&Topic::Alerts));
        assert!(!alerts.matches(&Topic::Dead));
    }

    #[test]
    fn a_pattern_is_a_topic_or_facts_or_work_with_a_star() {
        for text in [
            "work.builds",
            "facts.*",
            "work.*",
            "alerts",
            "presence",
            "dead",
        ] {
            let pattern: TopicPattern = text.parse().unwrap();
            assert_eq!(pattern.to_string(), text);
        }
        for bad in [
            "", "*", "**", "facts.", "facts.b*", "alerts.*", "dead.*", "other.*", "work.a.*",
            "Facts.*",
        ] {
            assert!(bad.parse::<TopicPattern>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn publish_is_a_send_through_the_server_and_subscribe_and_claim_are_reads() {
        let grants = [
            grant("facts.build", &[Verb::Publish]),
            grant("facts.signal", &[Verb::Subscribe]),
            grant("work.builds", &[Verb::Claim]),
        ];
        let can = |action, name: &str| allows(&grants, action, &topic(name));
        assert!(can(Action::Send, "facts.build") && !can(Action::Poll, "facts.build"));
        assert!(can(Action::Poll, "facts.signal") && !can(Action::Send, "facts.signal"));
        assert!(can(Action::Poll, "work.builds") && !can(Action::Send, "work.builds"));
        for name in ["work.reviews", "alerts"] {
            assert!(
                !can(Action::Send, name) && !can(Action::Poll, name),
                "{name}"
            );
        }
    }

    #[test]
    fn claim_is_allowed_on_a_queue_only_with_claim_or_approve_there() {
        let grants = [
            grant("work.builds", &[Verb::Claim]),
            grant("work.reviews", &[Verb::Subscribe, Verb::Publish]),
            grant("work.approvals", &[Verb::Approve]),
        ];
        assert!(allows(&grants, Action::Claim, &topic("work.builds")));
        assert!(!allows(&grants, Action::Claim, &topic("work.reviews")));
        assert!(allows(&grants, Action::Claim, &topic("work.approvals")));
        assert!(!allows(&grants, Action::Claim, &topic("work.other")));
        assert!(!allows(&grants, Action::Claim, &topic("facts.build")));
    }

    #[test]
    fn approve_covers_work_approvals_and_nothing_else_under_work_star() {
        let grants = [grant("work.*", &[Verb::Approve])];
        assert!(allows(&grants, Action::Claim, &topic("work.approvals")));
        assert!(allows(&grants, Action::Poll, &topic("work.approvals")));
        assert!(!allows(&grants, Action::Claim, &topic("work.builds")));
        assert!(!allows(&grants, Action::Poll, &topic("work.builds")));
        assert!(!allows(&grants, Action::Send, &topic("work.approvals")));
    }

    #[test]
    fn ack_is_for_alerts_and_does_not_read_them() {
        let grants = [grant("alerts", &[Verb::Ack])];
        assert!(allows(&grants, Action::Ack, &Topic::Alerts));
        assert!(!allows(&grants, Action::Poll, &Topic::Alerts));
        assert!(!allows(&grants, Action::Ack, &Topic::Dead));
    }

    #[test]
    fn a_wildcard_covers_topics_made_later_and_nothing_outside_its_family() {
        let grants = [grant("facts.*", &[Verb::Publish, Verb::Subscribe])];
        assert!(allows(&grants, Action::Send, &topic("facts.later")));
        assert!(allows(&grants, Action::Poll, &topic("facts.later")));
        assert!(!allows(&grants, Action::Poll, &topic("work.later")));
        assert!(!allows(&grants, Action::Send, &Topic::Presence));
    }

    #[test]
    fn no_scopes_grant_nothing() {
        assert!(!allows(&[], Action::Poll, &topic("facts.build")));
        assert!(!allows(&[], Action::Send, &topic("facts.build")));
        assert!(!allows(&[], Action::Claim, &topic("work.builds")));
        assert!(!allows(&[], Action::Ack, &Topic::Alerts));
    }

    #[test]
    fn a_verb_that_cannot_apply_is_refused_when_the_scope_is_made() {
        let new = |pattern: &str, verbs: &[Verb]| {
            Grant::new(pattern.parse().unwrap(), verbs.iter().copied())
        };
        assert!(new("facts.build", &[]).is_err());
        assert!(new("facts.build", &[Verb::Claim]).is_err());
        assert!(new("facts.*", &[Verb::Claim]).is_err());
        assert!(new("alerts", &[Verb::Claim]).is_err());
        assert!(new("work.builds", &[Verb::Approve]).is_err());
        assert!(new("facts.build", &[Verb::Approve]).is_err());
        assert!(new("work.builds", &[Verb::Ack]).is_err());
        assert!(new("alerts", &[Verb::Ack, Verb::Subscribe]).is_ok());
        assert!(new("work.*", &[Verb::Claim, Verb::Approve]).is_ok());
        assert!(new("work.approvals", &[Verb::Approve]).is_ok());
        assert!(new("work.builds", &[Verb::Claim, Verb::Publish]).is_ok());
    }

    #[test]
    fn a_scope_reads_and_writes_as_json_and_is_checked_when_read() {
        let grant: Grant =
            serde_json::from_str(r#"{"topic":"work.builds","verbs":["claim","publish","claim"]}"#)
                .unwrap();
        assert_eq!(
            serde_json::to_value(&grant).unwrap(),
            serde_json::json!({"topic": "work.builds", "verbs": ["publish", "claim"]})
        );
        for bad in [
            r#"{"topic":"work.builds","verbs":[]}"#,
            r#"{"topic":"facts.build","verbs":["claim"]}"#,
            r#"{"topic":"facts.build","verbs":["admin"]}"#,
            r#"{"topic":"*","verbs":["publish"]}"#,
            r#"{"topic":"facts.build","verbs":["publish"],"extra":1}"#,
            r#"{"topic":"facts.build"}"#,
        ] {
            assert!(serde_json::from_str::<Grant>(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn facts_star_publish_does_not_reach_the_topics_only_mansio_writes() {
        let grants = [grant("facts.*", &[Verb::Publish, Verb::Subscribe])];
        for name in ["facts.work", "facts.alert", "facts.principal"] {
            assert!(!allows(&grants, Action::Send, &topic(name)), "{name}");
            assert!(
                allows(&grants, Action::Poll, &topic(name)),
                "{name} is readable"
            );
        }
        assert!(allows(&grants, Action::Send, &topic("facts.build")));
    }

    #[test]
    fn a_publish_scope_naming_a_topic_only_mansio_writes_is_refused() {
        for name in ["facts.work", "facts.alert", "facts.principal", "dead"] {
            let pattern: TopicPattern = name.parse().unwrap();
            assert!(
                Grant::new(pattern.clone(), [Verb::Publish]).is_err(),
                "{name} cannot be published to"
            );
            assert!(
                Grant::new(pattern, [Verb::Publish, Verb::Subscribe]).is_err(),
                "{name} with another verb too"
            );
            assert!(Grant::new(name.parse().unwrap(), [Verb::Subscribe]).is_ok());
        }
        // Work is published through work_publish, which publish allows.
        assert!(Grant::new("work.builds".parse().unwrap(), [Verb::Publish]).is_ok());
        assert!(Grant::new("work.*".parse().unwrap(), [Verb::Publish]).is_ok());
    }

    #[test]
    fn a_scope_read_back_is_not_checked_again_and_still_sends_nothing_mansio_writes() {
        // Refused by `Grant::new` today, as it might be by a later rule for a
        // scope that was fine when it was made.
        assert!(Grant::new("facts.work".parse().unwrap(), [Verb::Publish]).is_err());
        assert!(Grant::new("alerts".parse().unwrap(), [Verb::Claim]).is_err());
        let stored = Grant::from_stored("facts.work".parse().unwrap(), [Verb::Publish]);
        assert!(!allows(&[stored], Action::Send, &topic("facts.work")));
        let odd = Grant::from_stored("alerts".parse().unwrap(), [Verb::Claim, Verb::Subscribe]);
        assert!(allows(&[odd], Action::Poll, &topic("alerts")));
    }
}
