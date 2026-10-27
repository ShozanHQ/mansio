//! What a principal may do: scopes made of a topic pattern and verbs.
//!
//! A scope names the topics it covers (`work.builds`, `facts.*`, `alerts`) and
//! the verbs it grants there. [`topic_access`] turns scopes into what a broker
//! enforces on a topic, send and poll; [`allows`] answers what a request
//! needs. Both the brokers and the server use these two, so a scope means the
//! same everywhere.

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
    /// Send messages: the broker's send permission.
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
            Action::Send => has(Verb::Publish),
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

/// What a broker enforces on one topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TopicAccess {
    /// May send messages to the topic.
    pub send: bool,
    /// May read messages from the topic.
    pub poll: bool,
}

impl TopicAccess {
    /// Whether the access grants nothing.
    #[must_use]
    pub fn is_none(&self) -> bool {
        !self.send && !self.poll
    }
}

/// What the scopes grant on a topic, as the broker enforces it. Claiming,
/// approving and acknowledging are enforced by mansio and add only the right
/// to read.
#[must_use]
pub fn topic_access(grants: &[Grant], topic: &Topic) -> TopicAccess {
    TopicAccess {
        send: allows(grants, Action::Send, topic),
        poll: allows(grants, Action::Poll, topic),
    }
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
    fn publish_is_send_and_subscribe_and_claim_are_poll() {
        let grants = [
            grant("facts.build", &[Verb::Publish]),
            grant("facts.signal", &[Verb::Subscribe]),
            grant("work.builds", &[Verb::Claim]),
        ];
        let access = |name: &str| topic_access(&grants, &topic(name));
        assert_eq!(
            access("facts.build"),
            TopicAccess {
                send: true,
                poll: false
            }
        );
        assert_eq!(
            access("facts.signal"),
            TopicAccess {
                send: false,
                poll: true
            }
        );
        assert_eq!(
            access("work.builds"),
            TopicAccess {
                send: false,
                poll: true
            }
        );
        assert!(access("work.reviews").is_none());
        assert!(access("alerts").is_none());
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
        assert!(topic_access(&grants, &Topic::Alerts).is_none());
        assert!(!allows(&grants, Action::Ack, &Topic::Dead));
    }

    #[test]
    fn a_wildcard_covers_topics_made_later_and_nothing_outside_its_family() {
        let grants = [grant("facts.*", &[Verb::Publish, Verb::Subscribe])];
        assert!(topic_access(&grants, &topic("facts.later")).send);
        assert!(topic_access(&grants, &topic("facts.later")).poll);
        assert!(topic_access(&grants, &topic("work.later")).is_none());
        assert!(topic_access(&grants, &Topic::Presence).is_none());
    }

    #[test]
    fn no_scopes_grant_nothing() {
        assert!(topic_access(&[], &topic("facts.build")).is_none());
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
}
