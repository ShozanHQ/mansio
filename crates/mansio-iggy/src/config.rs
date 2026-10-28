//! How to reach Iggy and how to shape the topics mansio creates there.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use mansio_core::TopicKind;
use secrecy::SecretString;

/// Default Iggy TCP address.
pub const DEFAULT_ADDRESS: &str = "127.0.0.1:8090";
/// Default Iggy user, the root user of a development server.
pub const DEFAULT_USERNAME: &str = "iggy";
/// Default time to wait for Iggy at startup.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
/// Default number of recent messages searched for a publish that Iggy
/// accepted without reporting an offset.
pub const DEFAULT_LOCATE_WINDOW: u32 = 100;
/// Default retention of `presence`, which is ephemeral.
pub const DEFAULT_PRESENCE_RETENTION: Duration = Duration::from_secs(60 * 60);

/// How long Iggy keeps messages, per kind of topic. `None` keeps them
/// forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// `facts.<kind>`
    pub facts: Option<Duration>,
    /// `work.<queue>`
    pub work: Option<Duration>,
    /// `alerts`
    pub alerts: Option<Duration>,
    /// `dead`
    pub dead: Option<Duration>,
    /// `presence`
    pub presence: Option<Duration>,
}

impl Retention {
    /// The retention for a kind of topic.
    #[must_use]
    pub fn for_kind(&self, kind: TopicKind) -> Option<Duration> {
        match kind {
            TopicKind::Facts => self.facts,
            TopicKind::Work => self.work,
            TopicKind::Alerts => self.alerts,
            TopicKind::Dead => self.dead,
            TopicKind::Presence => self.presence,
        }
    }
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            facts: None,
            work: None,
            alerts: None,
            dead: None,
            presence: Some(DEFAULT_PRESENCE_RETENTION),
        }
    }
}

/// When Iggy answers that a write is done.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// After a quorum commit, with no stable-storage barrier. On one node that
    /// is a write to the operating system, so a crash of the host can lose
    /// the acknowledged tail.
    Replicated,
    /// After the data is on stable storage as well. Slower, and what a
    /// durable topic needs.
    Persisted,
}

impl fmt::Display for Durability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Durability::Replicated => "replicated",
            Durability::Persisted => "persisted",
        })
    }
}

impl FromStr for Durability {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("replicated") {
            Ok(Durability::Replicated)
        } else if s.eq_ignore_ascii_case("persisted") {
            Ok(Durability::Persisted)
        } else {
            Err("must be replicated or persisted")
        }
    }
}

/// The durability of the topics mansio creates, per kind of topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurabilityPolicy {
    /// `facts.<kind>`, `work.<queue>`, `alerts` and `dead`: everything that
    /// goes through the sink.
    pub durable: Durability,
    /// `presence`, which is ephemeral.
    pub presence: Durability,
}

impl DurabilityPolicy {
    /// The durability for a kind of topic.
    #[must_use]
    pub fn for_kind(&self, kind: TopicKind) -> Durability {
        match kind {
            TopicKind::Presence => self.presence,
            TopicKind::Facts | TopicKind::Work | TopicKind::Alerts | TopicKind::Dead => {
                self.durable
            }
        }
    }
}

impl Default for DurabilityPolicy {
    fn default() -> Self {
        Self {
            durable: Durability::Persisted,
            presence: Durability::Replicated,
        }
    }
}

/// Everything [`IggyBroker::connect`](crate::IggyBroker::connect) needs.
#[derive(Debug, Clone)]
pub struct IggyConfig {
    /// `host:port` of Iggy's TCP listener.
    pub address: String,
    /// The admin user mansio acts as.
    pub username: String,
    /// Its password.
    pub password: SecretString,
    /// How long to keep trying to reach Iggy at startup.
    pub connect_timeout: Duration,
    /// How many recent messages to search when Iggy accepts a publish
    /// without reporting where it landed. Zero turns the search off.
    pub locate_window: u32,
    /// Message retention per kind of topic.
    pub retention: Retention,
    /// When Iggy confirms a write, per kind of topic.
    pub durability: DurabilityPolicy,
}

impl IggyConfig {
    /// A configuration with the default timeout, search window, retention and
    /// durability.
    #[must_use]
    pub fn new(
        address: impl Into<String>,
        username: impl Into<String>,
        password: SecretString,
    ) -> Self {
        Self {
            address: address.into(),
            username: username.into(),
            password,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            locate_window: DEFAULT_LOCATE_WINDOW,
            retention: Retention::default(),
            durability: DurabilityPolicy::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_is_looked_up_by_kind() {
        let retention = Retention::default();
        assert_eq!(
            retention.for_kind(TopicKind::Presence),
            Some(DEFAULT_PRESENCE_RETENTION)
        );
        for kind in [
            TopicKind::Facts,
            TopicKind::Work,
            TopicKind::Alerts,
            TopicKind::Dead,
        ] {
            assert_eq!(retention.for_kind(kind), None);
        }
    }

    #[test]
    fn durability_parses_and_prints_its_names() {
        for (text, durability) in [
            ("persisted", Durability::Persisted),
            ("replicated", Durability::Replicated),
            ("Persisted", Durability::Persisted),
            ("REPLICATED", Durability::Replicated),
        ] {
            assert_eq!(text.parse::<Durability>(), Ok(durability));
            assert_eq!(durability.to_string().parse::<Durability>(), Ok(durability));
        }
        for text in ["", "fsync", "persist", " persisted"] {
            assert!(text.parse::<Durability>().is_err(), "{text:?}");
        }
    }

    #[test]
    fn everything_but_presence_is_persisted_by_default() {
        let policy = DurabilityPolicy::default();
        assert_eq!(policy.for_kind(TopicKind::Presence), Durability::Replicated);
        for kind in [
            TopicKind::Facts,
            TopicKind::Work,
            TopicKind::Alerts,
            TopicKind::Dead,
        ] {
            assert_eq!(policy.for_kind(kind), Durability::Persisted);
        }
    }

    #[test]
    fn debug_does_not_show_the_password() {
        let config = IggyConfig::new("host:1", "user", SecretString::from("hunter2".to_owned()));
        assert!(!format!("{config:?}").contains("hunter2"));
    }
}
