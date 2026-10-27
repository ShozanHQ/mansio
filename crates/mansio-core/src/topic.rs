//! Topic names.

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::ParseError;
use crate::id::Segment;

/// A topic within a tenant's stream: `facts.<kind>`, `work.<queue>`,
/// `alerts`, `presence` or `dead`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Topic {
    /// Durable events: `facts.<kind>`.
    Facts(Segment),
    /// Claimable items: `work.<queue>`.
    Work(Segment),
    /// Escalations a person acknowledges.
    Alerts,
    /// Heartbeats and progress. Ephemeral.
    Presence,
    /// Work items past the attempt cap.
    Dead,
}

/// The five sorts of topic, without the variable part of a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopicKind {
    /// `facts.<kind>`
    Facts,
    /// `work.<queue>`
    Work,
    /// `alerts`
    Alerts,
    /// `presence`
    Presence,
    /// `dead`
    Dead,
}

impl TopicKind {
    /// Every kind, in a fixed order.
    pub const ALL: [TopicKind; 5] = [
        TopicKind::Facts,
        TopicKind::Work,
        TopicKind::Alerts,
        TopicKind::Presence,
        TopicKind::Dead,
    ];

    /// The lowercase name: `facts`, `work`, `alerts`, `presence`, `dead`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            TopicKind::Facts => "facts",
            TopicKind::Work => "work",
            TopicKind::Alerts => "alerts",
            TopicKind::Presence => "presence",
            TopicKind::Dead => "dead",
        }
    }
}

impl fmt::Display for TopicKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Topic {
    /// Which sort of topic this is.
    #[must_use]
    pub fn kind(&self) -> TopicKind {
        match self {
            Topic::Facts(_) => TopicKind::Facts,
            Topic::Work(_) => TopicKind::Work,
            Topic::Alerts => TopicKind::Alerts,
            Topic::Presence => TopicKind::Presence,
            Topic::Dead => TopicKind::Dead,
        }
    }

    /// `facts.work`: where mansio records every work item transition.
    #[must_use]
    pub fn facts_work() -> Self {
        Topic::Facts(Segment::known("work"))
    }

    /// `facts.alert`: where mansio records every alert transition.
    #[must_use]
    pub fn facts_alert() -> Self {
        Topic::Facts(Segment::known("alert"))
    }

    /// `facts.principal`: where mansio records every credential it issues
    /// and revokes.
    #[must_use]
    pub fn facts_principal() -> Self {
        Topic::Facts(Segment::known("principal"))
    }

    /// `facts.webhook`: where mansio records a delivery it gave up on.
    #[must_use]
    pub fn facts_webhook() -> Self {
        Topic::Facts(Segment::known("webhook"))
    }

    /// `work.approvals`: the queue of items a person approves.
    #[must_use]
    pub fn work_approvals() -> Self {
        Topic::Work(Segment::known("approvals"))
    }

    /// Whether what is published here is durable, and so goes through the
    /// sink first. Everything is, except `presence`.
    #[must_use]
    pub fn is_durable(&self) -> bool {
        !matches!(self, Topic::Presence)
    }
}

impl FromStr for Topic {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        const RULE: &str = "must be facts.<kind>, work.<queue>, alerts, presence or dead";
        match s {
            "alerts" => return Ok(Topic::Alerts),
            "presence" => return Ok(Topic::Presence),
            "dead" => return Ok(Topic::Dead),
            _ => {}
        }
        let unknown = || ParseError::Invalid {
            what: "topic",
            value: s.to_owned(),
            rule: RULE,
        };
        let (prefix, rest) = s.split_once('.').ok_or_else(unknown)?;
        match prefix {
            "facts" => Ok(Topic::Facts(rest.parse()?)),
            "work" => Ok(Topic::Work(rest.parse()?)),
            _ => Err(unknown()),
        }
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Topic::Facts(kind) => write!(f, "facts.{kind}"),
            Topic::Work(queue) => write!(f, "work.{queue}"),
            Topic::Alerts => f.write_str("alerts"),
            Topic::Presence => f.write_str("presence"),
            Topic::Dead => f.write_str("dead"),
        }
    }
}

impl Serialize for Topic {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Topic {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = Cow::<str>::deserialize(deserializer)?;
        s.parse().map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_valid_name_round_trips() {
        for name in [
            "facts.build",
            "facts.a",
            "facts.work-done_2",
            "work.builds",
            "work.0",
            "alerts",
            "presence",
            "dead",
        ] {
            let topic: Topic = name.parse().unwrap();
            assert_eq!(topic.to_string(), name);
            assert_eq!(topic.to_string().parse::<Topic>().unwrap(), topic);
        }
    }

    #[test]
    fn invalid_names_are_refused() {
        for name in [
            "",
            "facts",
            "work",
            "facts.",
            "work.",
            "facts.Build",
            "facts.a.b",
            "facts.-a",
            "work.a b",
            "alerts.x",
            "presence.x",
            "dead.x",
            "Alerts",
            "other.x",
            ".facts",
            "facts..x",
            "facts.é",
            "facts./x",
        ] {
            assert!(name.parse::<Topic>().is_err(), "{name:?} should be refused");
        }
    }

    #[test]
    fn segment_length_is_capped_at_63() {
        assert!(format!("facts.{}", "a".repeat(63)).parse::<Topic>().is_ok());
        assert!(
            format!("facts.{}", "a".repeat(64))
                .parse::<Topic>()
                .is_err()
        );
    }

    #[test]
    fn kind_and_durability() {
        let facts: Topic = "facts.build".parse().unwrap();
        assert_eq!(facts.kind(), TopicKind::Facts);
        assert!(facts.is_durable());
        assert_eq!(Topic::Presence.kind(), TopicKind::Presence);
        assert!(!Topic::Presence.is_durable());
        for kind in TopicKind::ALL {
            assert_eq!(kind.to_string(), kind.as_str());
        }
    }

    #[test]
    fn the_transition_topics_are_facts_work_and_facts_alert() {
        assert_eq!(Topic::facts_work().to_string(), "facts.work");
        assert_eq!(Topic::facts_alert().to_string(), "facts.alert");
        assert_eq!("facts.work".parse::<Topic>().unwrap(), Topic::facts_work());
        assert_eq!(
            "facts.alert".parse::<Topic>().unwrap(),
            Topic::facts_alert()
        );
    }

    #[test]
    fn the_webhook_topic_is_facts_webhook() {
        assert_eq!(Topic::facts_webhook().to_string(), "facts.webhook");
        assert_eq!(
            "facts.webhook".parse::<Topic>().unwrap(),
            Topic::facts_webhook()
        );
    }

    #[test]
    fn the_credential_topics_are_facts_principal_and_work_approvals() {
        assert_eq!(Topic::facts_principal().to_string(), "facts.principal");
        assert_eq!(Topic::work_approvals().to_string(), "work.approvals");
        assert_eq!(
            "facts.principal".parse::<Topic>().unwrap(),
            Topic::facts_principal()
        );
        assert_eq!(
            "work.approvals".parse::<Topic>().unwrap(),
            Topic::work_approvals()
        );
    }

    #[test]
    fn serde_uses_the_name() {
        let topic: Topic = serde_json::from_str("\"work.builds\"").unwrap();
        assert_eq!(serde_json::to_string(&topic).unwrap(), "\"work.builds\"");
        assert!(serde_json::from_str::<Topic>("\"work.\"").is_err());
    }
}
