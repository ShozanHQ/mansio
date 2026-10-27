//! The binary's settings, read from the environment.
//!
//! Every limit, interval and switch is a named variable with a default. The
//! README lists them. Values live outside the repository; configuration names
//! variables, never values.

use std::fmt::Display;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use mansio_core::Principal;
use mansio_iggy::{
    DEFAULT_ADDRESS, DEFAULT_CONNECT_TIMEOUT, DEFAULT_LOCATE_WINDOW, DEFAULT_PRESENCE_RETENTION,
    DEFAULT_USERNAME, DurabilityPolicy, IggyConfig, ProvisionConfig, Retention, check_segment_size,
};
use secrecy::SecretString;
use thiserror::Error;

/// Default listen address: loopback only. The container image listens on all
/// interfaces.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:7070";
/// Default principal for the bearer token.
pub const DEFAULT_TOKEN_PRINCIPAL: &str = "service:mansio";
/// Default file for the JSON Lines sink.
pub const DEFAULT_SINK_PATH: &str = "mansio-sink.jsonl";

/// Limits on what the API accepts and returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest request body, in bytes (`MANSIO_MAX_BODY_BYTES`).
    pub max_body_bytes: usize,
    /// How many envelopes a poll returns when `count` is not given
    /// (`MANSIO_POLL_DEFAULT_COUNT`).
    pub poll_default_count: u32,
    /// The most a poll may ask for (`MANSIO_POLL_MAX_COUNT`).
    pub poll_max_count: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_body_bytes: 1024 * 1024,
            poll_default_count: 100,
            poll_max_count: 1000,
        }
    }
}

/// How the work and alert engine behaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkSettings {
    /// How long a claim holds (`MANSIO_LEASE_SECONDS`).
    pub lease: Duration,
    /// The longest a lease may be extended to, counted from the claim
    /// (`MANSIO_LEASE_MAX_SECONDS`).
    pub lease_max: Duration,
    /// Attempts an item gets before it goes to `dead`
    /// (`MANSIO_MAX_ATTEMPTS`).
    pub max_attempts: u32,
    /// The wait after a first failure; it doubles with each attempt
    /// (`MANSIO_BACKOFF_BASE_SECONDS`).
    pub backoff_base: Duration,
    /// The longest wait (`MANSIO_BACKOFF_CAP_SECONDS`).
    pub backoff_cap: Duration,
    /// How long an alert may go unacknowledged before it is surfaced again,
    /// and again after each further interval
    /// (`MANSIO_ALERT_RESURFACE_SECONDS`).
    pub alert_resurface: Duration,
    /// How often the timer looks for expiries, republishes and alerts to
    /// surface (`MANSIO_TICK_MILLIS`).
    pub tick: Duration,
}

impl Default for WorkSettings {
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(300),
            lease_max: Duration::from_secs(86_400),
            max_attempts: 5,
            backoff_base: Duration::from_secs(5),
            backoff_cap: Duration::from_secs(300),
            alert_resurface: Duration::from_secs(3600),
            tick: Duration::from_millis(500),
        }
    }
}

impl WorkSettings {
    /// Check the settings fit together.
    ///
    /// # Errors
    ///
    /// Returns an error naming the variable that is out of range.
    pub fn validate(&self) -> Result<(), SettingsError> {
        let invalid = |name: &'static str, reason: &str| SettingsError::Invalid {
            name,
            reason: reason.to_owned(),
        };
        if self.lease < Duration::from_secs(1) {
            return Err(invalid("MANSIO_LEASE_SECONDS", "must be at least 1"));
        }
        if self.lease_max < self.lease {
            return Err(invalid(
                "MANSIO_LEASE_MAX_SECONDS",
                "must be at least MANSIO_LEASE_SECONDS",
            ));
        }
        if self.max_attempts == 0 {
            return Err(invalid("MANSIO_MAX_ATTEMPTS", "must be at least 1"));
        }
        if self.backoff_base < Duration::from_secs(1) {
            return Err(invalid("MANSIO_BACKOFF_BASE_SECONDS", "must be at least 1"));
        }
        if self.backoff_cap < self.backoff_base {
            return Err(invalid(
                "MANSIO_BACKOFF_CAP_SECONDS",
                "must be at least MANSIO_BACKOFF_BASE_SECONDS",
            ));
        }
        if self.alert_resurface < Duration::from_secs(1) {
            return Err(invalid(
                "MANSIO_ALERT_RESURFACE_SECONDS",
                "must be at least 1",
            ));
        }
        if self.tick.is_zero() {
            return Err(invalid("MANSIO_TICK_MILLIS", "must be at least 1"));
        }
        Ok(())
    }
}

/// How credentials are issued and kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialSettings {
    /// How long an agent credential lasts when the request does not say
    /// (`MANSIO_AGENT_CREDENTIAL_TTL_SECONDS`).
    pub agent_ttl: Duration,
    /// The longest a credential is trusted without asking the broker again
    /// (`MANSIO_CREDENTIAL_RECHECK_SECONDS`). A revocation or an expiry that
    /// happened out of this server's sight takes effect within it.
    pub recheck: Duration,
    /// How often expired principals are revoked
    /// (`MANSIO_CREDENTIAL_SWEEP_SECONDS`).
    pub sweep: Duration,
}

impl Default for CredentialSettings {
    fn default() -> Self {
        Self {
            agent_ttl: Duration::from_secs(3600),
            recheck: Duration::from_secs(30),
            sweep: Duration::from_secs(60),
        }
    }
}

impl CredentialSettings {
    /// Check the settings are usable.
    ///
    /// # Errors
    ///
    /// Returns an error naming the variable that is out of range.
    pub fn validate(&self) -> Result<(), SettingsError> {
        let invalid = |name: &'static str| SettingsError::Invalid {
            name,
            reason: "must be at least 1".to_owned(),
        };
        let one = Duration::from_secs(1);
        if self.agent_ttl < one {
            return Err(invalid("MANSIO_AGENT_CREDENTIAL_TTL_SECONDS"));
        }
        if self.recheck < one {
            return Err(invalid("MANSIO_CREDENTIAL_RECHECK_SECONDS"));
        }
        if self.sweep < one {
            return Err(invalid("MANSIO_CREDENTIAL_SWEEP_SECONDS"));
        }
        Ok(())
    }
}

/// How event streams and webhooks follow a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamSettings {
    /// How often a stream or a webhook looks for new messages once it has
    /// caught up (`MANSIO_STREAM_POLL_MILLIS`).
    pub poll: Duration,
    /// How often an event stream that has nothing to send sends a comment,
    /// so that the connection is not taken for dead
    /// (`MANSIO_SSE_KEEPALIVE_SECONDS`).
    pub sse_keepalive: Duration,
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(500),
            sse_keepalive: Duration::from_secs(15),
        }
    }
}

impl StreamSettings {
    /// Check the settings are usable.
    ///
    /// # Errors
    ///
    /// Returns an error naming the variable that is out of range.
    pub fn validate(&self) -> Result<(), SettingsError> {
        let invalid = |name: &'static str| SettingsError::Invalid {
            name,
            reason: "must be at least 1".to_owned(),
        };
        if self.poll.is_zero() {
            return Err(invalid("MANSIO_STREAM_POLL_MILLIS"));
        }
        if self.sse_keepalive < Duration::from_secs(1) {
            return Err(invalid("MANSIO_SSE_KEEPALIVE_SECONDS"));
        }
        Ok(())
    }
}

/// How webhooks deliver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebhookSettings {
    /// The wait after the first failed attempt at a delivery; it doubles with
    /// each attempt (`MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS`).
    pub backoff_base: Duration,
    /// The longest wait (`MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS`).
    pub backoff_cap: Duration,
    /// Attempts at one delivery before it is recorded as failed and the
    /// webhook moves on (`MANSIO_WEBHOOK_MAX_ATTEMPTS`).
    pub max_attempts: u32,
    /// How long one attempt may take (`MANSIO_WEBHOOK_TIMEOUT_SECONDS`).
    pub timeout: Duration,
}

impl Default for WebhookSettings {
    fn default() -> Self {
        Self {
            backoff_base: Duration::from_secs(5),
            backoff_cap: Duration::from_secs(300),
            max_attempts: 8,
            timeout: Duration::from_secs(10),
        }
    }
}

impl WebhookSettings {
    /// Check the settings fit together.
    ///
    /// # Errors
    ///
    /// Returns an error naming the variable that is out of range.
    pub fn validate(&self) -> Result<(), SettingsError> {
        let invalid = |name: &'static str, reason: &str| SettingsError::Invalid {
            name,
            reason: reason.to_owned(),
        };
        let one = Duration::from_secs(1);
        if self.backoff_base < one {
            return Err(invalid(
                "MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS",
                "must be at least 1",
            ));
        }
        if self.backoff_cap < self.backoff_base {
            return Err(invalid(
                "MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS",
                "must be at least MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS",
            ));
        }
        if self.max_attempts == 0 {
            return Err(invalid("MANSIO_WEBHOOK_MAX_ATTEMPTS", "must be at least 1"));
        }
        if self.timeout < one {
            return Err(invalid(
                "MANSIO_WEBHOOK_TIMEOUT_SECONDS",
                "must be at least 1",
            ));
        }
        Ok(())
    }
}

/// Which broker to run on (`MANSIO_BROKER`), with what it needs.
#[derive(Debug)]
pub enum Backend {
    /// Apache Iggy.
    Iggy(IggyConfig),
    /// In memory, forgotten on exit.
    Memory,
}

impl Backend {
    /// The name `MANSIO_BROKER` gives it.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Backend::Iggy(_) => "iggy",
            Backend::Memory => "memory",
        }
    }
}

/// A setting that is missing or wrong.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SettingsError {
    /// A required variable is not set.
    #[error("{name} must be set")]
    Missing {
        /// The variable.
        name: &'static str,
    },
    /// A variable has a value that cannot be used. Secrets are never parsed,
    /// so they never appear here.
    #[error("{name} is not valid: {reason}")]
    Invalid {
        /// The variable.
        name: &'static str,
        /// What is wrong with it.
        reason: String,
    },
}

/// Everything the binary is configured with.
#[derive(Debug)]
pub struct Settings {
    /// Where to listen (`MANSIO_LISTEN`).
    pub listen: SocketAddr,
    /// Which broker (`MANSIO_BROKER`).
    pub broker: Backend,
    /// The bearer token clients present (`MANSIO_TOKEN`).
    pub token: SecretString,
    /// Who the token stands for (`MANSIO_TOKEN_PRINCIPAL`).
    pub token_principal: Principal,
    /// The JSON Lines sink file (`MANSIO_SINK_PATH`); `None` records nothing.
    pub sink_path: Option<PathBuf>,
    /// API limits.
    pub limits: Limits,
    /// The work and alert engine.
    pub work: WorkSettings,
    /// Credentials.
    pub credentials: CredentialSettings,
    /// Event streams.
    pub stream: StreamSettings,
    /// Webhook delivery.
    pub webhooks: WebhookSettings,
}

impl Settings {
    /// Read the settings from the process environment.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first variable that is missing or wrong.
    pub fn from_env() -> Result<Self, SettingsError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// Read the settings through `lookup`, which returns the value of a
    /// variable if it is set. An empty value counts as not set, except for
    /// `MANSIO_SINK_PATH`, where it turns the sink off.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first variable that is missing or wrong.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, SettingsError> {
        let env = Env(lookup);
        let defaults = Limits::default();
        let limits = Limits {
            max_body_bytes: env.parse("MANSIO_MAX_BODY_BYTES", defaults.max_body_bytes)?,
            poll_default_count: env
                .parse("MANSIO_POLL_DEFAULT_COUNT", defaults.poll_default_count)?,
            poll_max_count: env.parse("MANSIO_POLL_MAX_COUNT", defaults.poll_max_count)?,
        };
        if limits.poll_default_count == 0 || limits.poll_default_count > limits.poll_max_count {
            return Err(SettingsError::Invalid {
                name: "MANSIO_POLL_DEFAULT_COUNT",
                reason: "must be at least 1 and at most MANSIO_POLL_MAX_COUNT".to_owned(),
            });
        }

        let work_defaults = WorkSettings::default();
        let work = WorkSettings {
            lease: env.seconds("MANSIO_LEASE_SECONDS", work_defaults.lease)?,
            lease_max: env.seconds("MANSIO_LEASE_MAX_SECONDS", work_defaults.lease_max)?,
            max_attempts: env.parse("MANSIO_MAX_ATTEMPTS", work_defaults.max_attempts)?,
            backoff_base: env.seconds("MANSIO_BACKOFF_BASE_SECONDS", work_defaults.backoff_base)?,
            backoff_cap: env.seconds("MANSIO_BACKOFF_CAP_SECONDS", work_defaults.backoff_cap)?,
            alert_resurface: env.seconds(
                "MANSIO_ALERT_RESURFACE_SECONDS",
                work_defaults.alert_resurface,
            )?,
            tick: Duration::from_millis(env.parse(
                "MANSIO_TICK_MILLIS",
                u64::try_from(work_defaults.tick.as_millis()).unwrap_or(u64::MAX),
            )?),
        };
        work.validate()?;

        let credential_defaults = CredentialSettings::default();
        let credentials = CredentialSettings {
            agent_ttl: env.seconds(
                "MANSIO_AGENT_CREDENTIAL_TTL_SECONDS",
                credential_defaults.agent_ttl,
            )?,
            recheck: env.seconds(
                "MANSIO_CREDENTIAL_RECHECK_SECONDS",
                credential_defaults.recheck,
            )?,
            sweep: env.seconds("MANSIO_CREDENTIAL_SWEEP_SECONDS", credential_defaults.sweep)?,
        };
        credentials.validate()?;

        let stream_defaults = StreamSettings::default();
        let stream = StreamSettings {
            poll: Duration::from_millis(env.parse(
                "MANSIO_STREAM_POLL_MILLIS",
                u64::try_from(stream_defaults.poll.as_millis()).unwrap_or(u64::MAX),
            )?),
            sse_keepalive: env.seconds(
                "MANSIO_SSE_KEEPALIVE_SECONDS",
                stream_defaults.sse_keepalive,
            )?,
        };
        stream.validate()?;

        let webhook_defaults = WebhookSettings::default();
        let webhooks = WebhookSettings {
            backoff_base: env.seconds(
                "MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS",
                webhook_defaults.backoff_base,
            )?,
            backoff_cap: env.seconds(
                "MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS",
                webhook_defaults.backoff_cap,
            )?,
            max_attempts: env
                .parse("MANSIO_WEBHOOK_MAX_ATTEMPTS", webhook_defaults.max_attempts)?,
            timeout: env.seconds("MANSIO_WEBHOOK_TIMEOUT_SECONDS", webhook_defaults.timeout)?,
        };
        webhooks.validate()?;

        let broker = match env.string("MANSIO_BROKER", "iggy").as_str() {
            "iggy" => Backend::Iggy(Self::iggy_config(&env)?),
            "memory" => Backend::Memory,
            _ => {
                return Err(SettingsError::Invalid {
                    name: "MANSIO_BROKER",
                    reason: "must be iggy or memory".to_owned(),
                });
            }
        };

        Ok(Self {
            listen: env.parse("MANSIO_LISTEN", DEFAULT_LISTEN)?,
            broker,
            token: env.required_secret("MANSIO_TOKEN")?,
            token_principal: env.parse("MANSIO_TOKEN_PRINCIPAL", DEFAULT_TOKEN_PRINCIPAL)?,
            sink_path: match env.raw("MANSIO_SINK_PATH") {
                None => Some(PathBuf::from(DEFAULT_SINK_PATH)),
                Some(path) if path.is_empty() => None,
                Some(path) => Some(PathBuf::from(path)),
            },
            limits,
            work,
            credentials,
            stream,
            webhooks,
        })
    }

    /// Read what `mansio-server provision-admin` needs from the process
    /// environment.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first variable that is missing or wrong.
    pub fn provision_from_env() -> Result<ProvisionConfig, SettingsError> {
        Self::provision_from_lookup(|name| std::env::var(name).ok())
    }

    /// Read what `provision-admin` needs through `lookup`.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first variable that is missing or wrong.
    pub fn provision_from_lookup(
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<ProvisionConfig, SettingsError> {
        let env = Env(lookup);
        Ok(ProvisionConfig {
            address: env.string("MANSIO_IGGY_ADDRESS", DEFAULT_ADDRESS),
            root_username: env.string("IGGY_ROOT_USERNAME", "iggy"),
            root_password: env.required_secret("IGGY_ROOT_PASSWORD")?,
            admin_username: env.string("MANSIO_IGGY_USERNAME", DEFAULT_USERNAME),
            admin_password: env.required_secret("MANSIO_IGGY_PASSWORD")?,
            connect_timeout: Duration::from_secs(env.parse(
                "MANSIO_IGGY_CONNECT_TIMEOUT_SECONDS",
                DEFAULT_CONNECT_TIMEOUT.as_secs(),
            )?),
        })
    }

    fn iggy_config<F: Fn(&str) -> Option<String>>(
        env: &Env<F>,
    ) -> Result<IggyConfig, SettingsError> {
        let retention = |name: &'static str,
                         default: Option<Duration>|
         -> Result<Option<Duration>, SettingsError> {
            let seconds = env.parse(name, default.map_or(0, |d| d.as_secs()))?;
            Ok((seconds > 0).then(|| Duration::from_secs(seconds)))
        };
        let mut config = IggyConfig::new(
            env.string("MANSIO_IGGY_ADDRESS", DEFAULT_ADDRESS),
            env.string("MANSIO_IGGY_USERNAME", DEFAULT_USERNAME),
            env.required_secret("MANSIO_IGGY_PASSWORD")?,
        );
        config.connect_timeout = Duration::from_secs(env.parse(
            "MANSIO_IGGY_CONNECT_TIMEOUT_SECONDS",
            DEFAULT_CONNECT_TIMEOUT.as_secs(),
        )?);
        config.locate_window = env.parse("MANSIO_IGGY_LOCATE_WINDOW", DEFAULT_LOCATE_WINDOW)?;
        config.segment_size_bytes = env.parse("MANSIO_IGGY_SEGMENT_SIZE_BYTES", 0_u64)?;
        check_segment_size(config.segment_size_bytes).map_err(|reason| SettingsError::Invalid {
            name: "MANSIO_IGGY_SEGMENT_SIZE_BYTES",
            reason: reason.to_owned(),
        })?;
        let durability = DurabilityPolicy::default();
        config.durability = DurabilityPolicy {
            durable: env.parse("MANSIO_IGGY_DURABILITY", durability.durable)?,
            presence: env.parse("MANSIO_IGGY_PRESENCE_DURABILITY", durability.presence)?,
        };
        config.retention = Retention {
            facts: retention("MANSIO_RETENTION_FACTS_SECONDS", None)?,
            work: retention("MANSIO_RETENTION_WORK_SECONDS", None)?,
            alerts: retention("MANSIO_RETENTION_ALERTS_SECONDS", None)?,
            dead: retention("MANSIO_RETENTION_DEAD_SECONDS", None)?,
            presence: retention(
                "MANSIO_RETENTION_PRESENCE_SECONDS",
                Some(DEFAULT_PRESENCE_RETENTION),
            )?,
        };
        Ok(config)
    }
}

struct Env<F>(F);

impl<F: Fn(&str) -> Option<String>> Env<F> {
    fn raw(&self, name: &str) -> Option<String> {
        (self.0)(name)
    }

    fn set(&self, name: &str) -> Option<String> {
        self.raw(name).filter(|value| !value.is_empty())
    }

    fn string(&self, name: &str, default: &str) -> String {
        self.set(name).unwrap_or_else(|| default.to_owned())
    }

    fn required_secret(&self, name: &'static str) -> Result<SecretString, SettingsError> {
        self.set(name)
            .map(SecretString::from)
            .ok_or(SettingsError::Missing { name })
    }

    /// A whole number of seconds, or `default` if it is not set.
    fn seconds(&self, name: &'static str, default: Duration) -> Result<Duration, SettingsError> {
        self.parse(name, default.as_secs()).map(Duration::from_secs)
    }

    /// The parsed value of `name`, or of `default` if it is not set.
    fn parse<T>(&self, name: &'static str, default: impl ToString) -> Result<T, SettingsError>
    where
        T: FromStr,
        T::Err: Display,
    {
        let text = self.set(name).unwrap_or_else(|| default.to_string());
        text.parse().map_err(|e: T::Err| SettingsError::Invalid {
            name,
            reason: e.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use mansio_iggy::Durability;
    use secrecy::ExposeSecret;

    use super::*;

    fn settings(pairs: &[(&str, &str)]) -> Result<Settings, SettingsError> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        Settings::from_lookup(|name| map.get(name).cloned())
    }

    #[test]
    fn defaults_apply_when_only_the_required_variables_are_set() {
        let settings = settings(&[("MANSIO_TOKEN", "t"), ("MANSIO_IGGY_PASSWORD", "p")]).unwrap();
        assert_eq!(settings.listen, "127.0.0.1:7070".parse().unwrap());
        assert_eq!(settings.token_principal.to_string(), "service:mansio");
        assert_eq!(settings.sink_path, Some(PathBuf::from("mansio-sink.jsonl")));
        assert_eq!(settings.limits, Limits::default());
        assert_eq!(settings.work, WorkSettings::default());
        assert_eq!(settings.credentials, CredentialSettings::default());
        assert_eq!(settings.stream, StreamSettings::default());
        assert_eq!(settings.webhooks, WebhookSettings::default());
        let Backend::Iggy(iggy) = settings.broker else {
            panic!("expected the Iggy broker");
        };
        assert_eq!(iggy.address, "127.0.0.1:8090");
        assert_eq!(iggy.username, "mansio");
        assert_eq!(iggy.password.expose_secret(), "p");
        assert_eq!(iggy.connect_timeout, Duration::from_secs(60));
        assert_eq!(iggy.locate_window, 100);
        assert_eq!(iggy.segment_size_bytes, 0);
        assert_eq!(iggy.retention, Retention::default());
        assert_eq!(iggy.durability.durable, Durability::Persisted);
        assert_eq!(iggy.durability.presence, Durability::Replicated);
    }

    #[test]
    fn the_token_is_required() {
        let error = settings(&[("MANSIO_BROKER", "memory")]).unwrap_err();
        assert_eq!(
            error,
            SettingsError::Missing {
                name: "MANSIO_TOKEN"
            }
        );
        let error = settings(&[("MANSIO_BROKER", "memory"), ("MANSIO_TOKEN", "")]).unwrap_err();
        assert_eq!(
            error,
            SettingsError::Missing {
                name: "MANSIO_TOKEN"
            }
        );
    }

    #[test]
    fn iggy_needs_its_password_and_memory_does_not() {
        let error = settings(&[("MANSIO_TOKEN", "t")]).unwrap_err();
        assert_eq!(
            error,
            SettingsError::Missing {
                name: "MANSIO_IGGY_PASSWORD"
            }
        );
        let settings = settings(&[("MANSIO_TOKEN", "t"), ("MANSIO_BROKER", "memory")]).unwrap();
        assert!(matches!(settings.broker, Backend::Memory));
    }

    #[test]
    fn values_override_defaults() {
        let settings = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_LISTEN", "0.0.0.0:9000"),
            ("MANSIO_TOKEN_PRINCIPAL", "agent:builder"),
            ("MANSIO_SINK_PATH", "/data/sink.jsonl"),
            ("MANSIO_MAX_BODY_BYTES", "2048"),
            ("MANSIO_POLL_DEFAULT_COUNT", "10"),
            ("MANSIO_POLL_MAX_COUNT", "50"),
        ])
        .unwrap();
        assert_eq!(settings.listen.port(), 9000);
        assert_eq!(settings.token_principal.to_string(), "agent:builder");
        assert_eq!(settings.sink_path, Some(PathBuf::from("/data/sink.jsonl")));
        assert_eq!(
            settings.limits,
            Limits {
                max_body_bytes: 2048,
                poll_default_count: 10,
                poll_max_count: 50
            }
        );
    }

    #[test]
    fn the_defaults_for_work_are_the_documented_ones() {
        let work = WorkSettings::default();
        assert_eq!(work.lease, Duration::from_secs(300));
        assert_eq!(work.lease_max, Duration::from_secs(86_400));
        assert_eq!(work.max_attempts, 5);
        assert_eq!(work.backoff_base, Duration::from_secs(5));
        assert_eq!(work.backoff_cap, Duration::from_secs(300));
        assert_eq!(work.alert_resurface, Duration::from_secs(3600));
        assert_eq!(work.tick, Duration::from_millis(500));
        assert_eq!(work.validate(), Ok(()));
    }

    #[test]
    fn work_settings_are_read_from_the_environment() {
        let settings = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_LEASE_SECONDS", "2"),
            ("MANSIO_LEASE_MAX_SECONDS", "60"),
            ("MANSIO_MAX_ATTEMPTS", "3"),
            ("MANSIO_BACKOFF_BASE_SECONDS", "1"),
            ("MANSIO_BACKOFF_CAP_SECONDS", "4"),
            ("MANSIO_ALERT_RESURFACE_SECONDS", "7"),
            ("MANSIO_TICK_MILLIS", "100"),
        ])
        .unwrap();
        assert_eq!(
            settings.work,
            WorkSettings {
                lease: Duration::from_secs(2),
                lease_max: Duration::from_secs(60),
                max_attempts: 3,
                backoff_base: Duration::from_secs(1),
                backoff_cap: Duration::from_secs(4),
                alert_resurface: Duration::from_secs(7),
                tick: Duration::from_millis(100),
            }
        );
    }

    #[test]
    fn work_settings_that_do_not_fit_are_refused_by_name() {
        for (name, value) in [
            ("MANSIO_LEASE_SECONDS", "0"),
            ("MANSIO_LEASE_MAX_SECONDS", "soon"),
            ("MANSIO_MAX_ATTEMPTS", "0"),
            ("MANSIO_BACKOFF_BASE_SECONDS", "0"),
            ("MANSIO_ALERT_RESURFACE_SECONDS", "0"),
            ("MANSIO_TICK_MILLIS", "0"),
            ("MANSIO_LEASE_SECONDS", "soon"),
            ("MANSIO_TICK_MILLIS", "-5"),
        ] {
            let error = settings(&[
                ("MANSIO_TOKEN", "t"),
                ("MANSIO_BROKER", "memory"),
                (name, value),
            ])
            .unwrap_err();
            assert!(
                matches!(&error, SettingsError::Invalid { name: reported, .. } if *reported == name),
                "{name}={value}: {error:?}"
            );
        }
        // The longest lease may not be below the lease.
        let error = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_LEASE_SECONDS", "30"),
            ("MANSIO_LEASE_MAX_SECONDS", "29"),
        ])
        .unwrap_err();
        assert!(matches!(
            error,
            SettingsError::Invalid {
                name: "MANSIO_LEASE_MAX_SECONDS",
                ..
            }
        ));
        // The cap may not be below the base.
        let error = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_BACKOFF_BASE_SECONDS", "10"),
            ("MANSIO_BACKOFF_CAP_SECONDS", "5"),
        ])
        .unwrap_err();
        assert!(matches!(
            error,
            SettingsError::Invalid {
                name: "MANSIO_BACKOFF_CAP_SECONDS",
                ..
            }
        ));
    }

    #[test]
    fn the_credential_defaults_are_the_documented_ones() {
        let credentials = CredentialSettings::default();
        assert_eq!(credentials.agent_ttl, Duration::from_secs(3600));
        assert_eq!(credentials.recheck, Duration::from_secs(30));
        assert_eq!(credentials.sweep, Duration::from_secs(60));
        assert_eq!(credentials.validate(), Ok(()));
    }

    #[test]
    fn credential_settings_are_read_from_the_environment() {
        let settings = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_AGENT_CREDENTIAL_TTL_SECONDS", "120"),
            ("MANSIO_CREDENTIAL_RECHECK_SECONDS", "5"),
            ("MANSIO_CREDENTIAL_SWEEP_SECONDS", "7"),
        ])
        .unwrap();
        assert_eq!(
            settings.credentials,
            CredentialSettings {
                agent_ttl: Duration::from_secs(120),
                recheck: Duration::from_secs(5),
                sweep: Duration::from_secs(7),
            }
        );
    }

    #[test]
    fn credential_settings_that_cannot_work_are_refused_by_name() {
        for (name, value) in [
            ("MANSIO_AGENT_CREDENTIAL_TTL_SECONDS", "0"),
            ("MANSIO_CREDENTIAL_RECHECK_SECONDS", "0"),
            ("MANSIO_CREDENTIAL_SWEEP_SECONDS", "0"),
            ("MANSIO_CREDENTIAL_SWEEP_SECONDS", "often"),
        ] {
            let error = settings(&[
                ("MANSIO_TOKEN", "t"),
                ("MANSIO_BROKER", "memory"),
                (name, value),
            ])
            .unwrap_err();
            assert!(
                matches!(&error, SettingsError::Invalid { name: reported, .. } if *reported == name),
                "{name}={value}: {error:?}"
            );
        }
    }

    #[test]
    fn the_defaults_for_streams_and_webhooks_are_the_documented_ones() {
        let stream = StreamSettings::default();
        assert_eq!(stream.poll, Duration::from_millis(500));
        assert_eq!(stream.sse_keepalive, Duration::from_secs(15));
        assert_eq!(stream.validate(), Ok(()));
        let webhooks = WebhookSettings::default();
        assert_eq!(webhooks.backoff_base, Duration::from_secs(5));
        assert_eq!(webhooks.backoff_cap, Duration::from_secs(300));
        assert_eq!(webhooks.max_attempts, 8);
        assert_eq!(webhooks.timeout, Duration::from_secs(10));
        assert_eq!(webhooks.validate(), Ok(()));
    }

    #[test]
    fn stream_and_webhook_settings_are_read_from_the_environment() {
        let settings = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_STREAM_POLL_MILLIS", "50"),
            ("MANSIO_SSE_KEEPALIVE_SECONDS", "3"),
            ("MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS", "1"),
            ("MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS", "4"),
            ("MANSIO_WEBHOOK_MAX_ATTEMPTS", "5"),
            ("MANSIO_WEBHOOK_TIMEOUT_SECONDS", "2"),
        ])
        .unwrap();
        assert_eq!(
            settings.stream,
            StreamSettings {
                poll: Duration::from_millis(50),
                sse_keepalive: Duration::from_secs(3),
            }
        );
        assert_eq!(
            settings.webhooks,
            WebhookSettings {
                backoff_base: Duration::from_secs(1),
                backoff_cap: Duration::from_secs(4),
                max_attempts: 5,
                timeout: Duration::from_secs(2),
            }
        );
    }

    #[test]
    fn stream_and_webhook_settings_that_cannot_work_are_refused_by_name() {
        for (name, value) in [
            ("MANSIO_STREAM_POLL_MILLIS", "0"),
            ("MANSIO_STREAM_POLL_MILLIS", "often"),
            ("MANSIO_SSE_KEEPALIVE_SECONDS", "0"),
            ("MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS", "0"),
            ("MANSIO_WEBHOOK_MAX_ATTEMPTS", "0"),
            ("MANSIO_WEBHOOK_TIMEOUT_SECONDS", "0"),
            ("MANSIO_WEBHOOK_TIMEOUT_SECONDS", "-1"),
        ] {
            let error = settings(&[
                ("MANSIO_TOKEN", "t"),
                ("MANSIO_BROKER", "memory"),
                (name, value),
            ])
            .unwrap_err();
            assert!(
                matches!(&error, SettingsError::Invalid { name: reported, .. } if *reported == name),
                "{name}={value}: {error:?}"
            );
        }
        // The cap may not be below the base.
        let error = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS", "10"),
            ("MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS", "5"),
        ])
        .unwrap_err();
        assert!(matches!(
            error,
            SettingsError::Invalid {
                name: "MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS",
                ..
            }
        ));
    }

    #[test]
    fn provisioning_reads_the_root_and_the_admin_credentials_from_named_variables() {
        let pairs = [
            ("IGGY_ROOT_PASSWORD", "root-secret"),
            ("MANSIO_IGGY_PASSWORD", "admin-secret"),
        ];
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let config = Settings::provision_from_lookup(|name| map.get(name).cloned()).unwrap();
        assert_eq!(config.root_username, "iggy");
        assert_eq!(config.admin_username, "mansio");
        assert_eq!(config.address, "127.0.0.1:8090");
        assert_eq!(config.root_password.expose_secret(), "root-secret");
        assert_eq!(config.admin_password.expose_secret(), "admin-secret");
        let shown = format!("{config:?}");
        assert!(!shown.contains("root-secret") && !shown.contains("admin-secret"));

        for missing in ["IGGY_ROOT_PASSWORD", "MANSIO_IGGY_PASSWORD"] {
            let error = Settings::provision_from_lookup(|name| {
                (name != missing).then(|| map.get(name).cloned()).flatten()
            })
            .unwrap_err();
            assert_eq!(error, SettingsError::Missing { name: missing });
        }
    }

    #[test]
    fn an_empty_sink_path_turns_the_sink_off() {
        let settings = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_SINK_PATH", ""),
        ])
        .unwrap();
        assert_eq!(settings.sink_path, None);
    }

    #[test]
    fn retention_zero_means_forever() {
        let settings = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_IGGY_PASSWORD", "p"),
            ("MANSIO_RETENTION_FACTS_SECONDS", "86400"),
            ("MANSIO_RETENTION_PRESENCE_SECONDS", "0"),
        ])
        .unwrap();
        let Backend::Iggy(iggy) = settings.broker else {
            panic!("expected the Iggy broker");
        };
        let retention = iggy.retention;
        assert_eq!(retention.facts, Some(Duration::from_secs(86400)));
        assert_eq!(retention.presence, None);
        assert_eq!(retention.work, None);
    }

    #[test]
    fn the_segment_size_is_read_and_checked_against_what_iggy_admits() {
        let read = |value: &str| {
            settings(&[
                ("MANSIO_TOKEN", "t"),
                ("MANSIO_IGGY_PASSWORD", "p"),
                ("MANSIO_IGGY_SEGMENT_SIZE_BYTES", value),
            ])
        };
        let Backend::Iggy(iggy) = read("1048576").unwrap().broker else {
            panic!("expected the Iggy broker");
        };
        assert_eq!(iggy.segment_size_bytes, 1_048_576);
        for bad in ["1024", "1048577", "2147483648", "big"] {
            let error = read(bad).unwrap_err();
            assert!(
                matches!(
                    error,
                    SettingsError::Invalid {
                        name: "MANSIO_IGGY_SEGMENT_SIZE_BYTES",
                        ..
                    }
                ),
                "{bad}: {error:?}"
            );
        }
    }

    #[test]
    fn durability_is_set_for_durable_topics_and_for_presence() {
        let settings = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_IGGY_PASSWORD", "p"),
            ("MANSIO_IGGY_DURABILITY", "replicated"),
            ("MANSIO_IGGY_PRESENCE_DURABILITY", "Persisted"),
        ])
        .unwrap();
        let Backend::Iggy(iggy) = settings.broker else {
            panic!("expected the Iggy broker");
        };
        assert_eq!(iggy.durability.durable, Durability::Replicated);
        assert_eq!(iggy.durability.presence, Durability::Persisted);
    }

    #[test]
    fn bad_values_name_the_variable() {
        for (name, value) in [
            ("MANSIO_BROKER", "kafka"),
            ("MANSIO_LISTEN", "nowhere"),
            ("MANSIO_TOKEN_PRINCIPAL", "nobody"),
            ("MANSIO_MAX_BODY_BYTES", "-1"),
            ("MANSIO_POLL_MAX_COUNT", "many"),
            ("MANSIO_POLL_DEFAULT_COUNT", "0"),
            ("MANSIO_POLL_DEFAULT_COUNT", "2000"),
            ("MANSIO_IGGY_CONNECT_TIMEOUT_SECONDS", "soon"),
            ("MANSIO_IGGY_DURABILITY", "fsync"),
            ("MANSIO_IGGY_PRESENCE_DURABILITY", "never"),
        ] {
            let error = settings(&[
                ("MANSIO_TOKEN", "t"),
                ("MANSIO_IGGY_PASSWORD", "p"),
                (name, value),
            ])
            .unwrap_err();
            let SettingsError::Invalid {
                name: reported,
                reason,
            } = &error
            else {
                panic!("{name}={value}: {error:?}");
            };
            assert_eq!(*reported, name);
            assert!(!reason.is_empty());
        }
    }

    #[test]
    fn debug_does_not_show_secrets() {
        let settings = settings(&[
            ("MANSIO_TOKEN", "tok3n-value"),
            ("MANSIO_IGGY_PASSWORD", "p4ss-value"),
        ])
        .unwrap();
        let shown = format!("{settings:?}");
        assert!(!shown.contains("tok3n-value") && !shown.contains("p4ss-value"));
    }
}
