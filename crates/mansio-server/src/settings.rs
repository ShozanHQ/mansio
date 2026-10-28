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
    DEFAULT_USERNAME, DurabilityPolicy, IggyConfig, Retention,
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
        let Backend::Iggy(iggy) = settings.broker else {
            panic!("expected the Iggy broker");
        };
        assert_eq!(iggy.address, "127.0.0.1:8090");
        assert_eq!(iggy.username, "iggy");
        assert_eq!(iggy.password.expose_secret(), "p");
        assert_eq!(iggy.connect_timeout, Duration::from_secs(60));
        assert_eq!(iggy.locate_window, 100);
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
