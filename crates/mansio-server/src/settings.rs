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
    DEFAULT_PRESENCE_SEGMENT_SIZE_BYTES, DEFAULT_SESSION_CLIENTS, DEFAULT_SESSION_IDLE,
    DEFAULT_SIGNIN_TIMEOUT, DEFAULT_USERNAME, DurabilityPolicy, IggyConfig, ProvisionConfig,
    Retention, check_segment_size,
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
/// Whether the file sink syncs each record to disk by default.
pub const DEFAULT_SINK_SYNC: bool = true;
/// Whether `GET /metrics` is served and counted by default (`MANSIO_METRICS`).
pub const DEFAULT_METRICS: bool = true;
/// Default directory of the snapshots (`MANSIO_SNAPSHOT_DIR`).
pub const DEFAULT_SNAPSHOT_DIR: &str = "mansio-snapshots";
/// Default interval between snapshots, in seconds
/// (`MANSIO_SNAPSHOT_INTERVAL_SECONDS`).
pub const DEFAULT_SNAPSHOT_INTERVAL_SECONDS: u64 = 300;
/// Most Iggy clients one credential may keep (`MANSIO_IGGY_SESSION_CLIENTS`).
pub const MAX_SESSION_CLIENTS: usize = 64;

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

/// The longest a lease, or the longest a lease may be extended to, can be set
/// to: ten years. A lease is an absolute time in the facts, and one far enough
/// away would pass the range of an RFC 3339 timestamp, which a restart could
/// not read back as it was.
pub const MAX_LEASE: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);

/// The longest an idempotency window can be set to: ten years, so that a time
/// plus a window stays in range of a timestamp.
pub const MAX_IDEMPOTENCY_WINDOW: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);

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
    /// How long a publish's idempotency key is remembered: a second publish
    /// to the same tenant and topic with the same key inside this window is
    /// answered with the first and publishes nothing
    /// (`MANSIO_IDEMPOTENCY_WINDOW_SECONDS`).
    pub idempotency_window: Duration,
    /// The most keys a tenant remembers; past it the one remembered longest
    /// ago is forgotten, even inside its window
    /// (`MANSIO_IDEMPOTENCY_MAX_KEYS`).
    pub idempotency_max_keys: usize,
    /// How often a recovery that keeps failing is logged, and the longest wait
    /// between its attempts, which back off from one tick to this
    /// (`MANSIO_RECOVERY_LOG_SECONDS`).
    pub recovery_log: Duration,
    /// How often the state of a tenant that has changed is written to a
    /// snapshot, which a start reads instead of replaying every topic from
    /// offset 0 (`MANSIO_SNAPSHOT_INTERVAL_SECONDS`). Zero turns snapshots
    /// off.
    pub snapshot_interval: Duration,
    /// How long a server may go without renewing a tenant before another
    /// server takes the tenant over (`MANSIO_OWNER_TTL_SECONDS`). Zero, the
    /// default, means no ownership: the server serves every tenant, and it
    /// must be the only one on its broker. Set it on every server that shares
    /// a broker.
    pub owner_ttl: Duration,
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
            idempotency_window: Duration::from_secs(86_400),
            idempotency_max_keys: 10_000,
            recovery_log: Duration::from_secs(60),
            snapshot_interval: Duration::from_secs(DEFAULT_SNAPSHOT_INTERVAL_SECONDS),
            owner_ttl: Duration::ZERO,
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
        if self.lease > MAX_LEASE {
            return Err(invalid(
                "MANSIO_LEASE_SECONDS",
                "must be at most 315360000 (ten years)",
            ));
        }
        if self.lease_max > MAX_LEASE {
            return Err(invalid(
                "MANSIO_LEASE_MAX_SECONDS",
                "must be at most 315360000 (ten years)",
            ));
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
        if !self.owner_ttl.is_zero() && self.owner_ttl < Duration::from_secs(3) {
            return Err(invalid(
                "MANSIO_OWNER_TTL_SECONDS",
                "must be 0 (no ownership) or at least 3",
            ));
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
        if self.idempotency_window < Duration::from_secs(1) {
            return Err(invalid(
                "MANSIO_IDEMPOTENCY_WINDOW_SECONDS",
                "must be at least 1",
            ));
        }
        if self.idempotency_window > MAX_IDEMPOTENCY_WINDOW {
            return Err(invalid(
                "MANSIO_IDEMPOTENCY_WINDOW_SECONDS",
                "must be at most 315360000 (ten years)",
            ));
        }
        if self.idempotency_max_keys == 0 {
            return Err(invalid("MANSIO_IDEMPOTENCY_MAX_KEYS", "must be at least 1"));
        }
        if self.recovery_log < Duration::from_secs(1) {
            return Err(invalid("MANSIO_RECOVERY_LOG_SECONDS", "must be at least 1"));
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
    /// The soonest the principals log is read again for a token the registry
    /// does not know (`MANSIO_CREDENTIAL_FOLLOW_MILLIS`).
    pub follow: Duration,
}

impl Default for CredentialSettings {
    fn default() -> Self {
        Self {
            agent_ttl: Duration::from_secs(3600),
            recheck: Duration::from_secs(30),
            sweep: Duration::from_secs(60),
            follow: Duration::from_millis(1000),
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
        if self.follow.is_zero() {
            return Err(invalid("MANSIO_CREDENTIAL_FOLLOW_MILLIS"));
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
    /// How long a shutdown waits, once it has ended every event stream, for
    /// the requests still being served to finish before the server stops
    /// anyway (`MANSIO_SHUTDOWN_GRACE_SECONDS`).
    pub shutdown_grace: Duration,
    /// The most event streams one credential may hold open; the next gets
    /// 429 (`MANSIO_SSE_MAX_PER_CREDENTIAL`).
    pub sse_max_per_credential: usize,
    /// The most event streams open on the server in all; the next gets 503
    /// (`MANSIO_SSE_MAX`).
    pub sse_max: usize,
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(500),
            sse_keepalive: Duration::from_secs(15),
            shutdown_grace: Duration::from_secs(10),
            sse_max_per_credential: 10,
            sse_max: 1000,
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
        if self.shutdown_grace < Duration::from_secs(1) {
            return Err(invalid("MANSIO_SHUTDOWN_GRACE_SECONDS"));
        }
        if self.sse_max_per_credential == 0 {
            return Err(invalid("MANSIO_SSE_MAX_PER_CREDENTIAL"));
        }
        if self.sse_max == 0 {
            return Err(invalid("MANSIO_SSE_MAX"));
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
    /// The longest address a webhook may have, in bytes
    /// (`MANSIO_WEBHOOK_URL_MAX_BYTES`).
    pub url_max_bytes: usize,
}

impl Default for WebhookSettings {
    fn default() -> Self {
        Self {
            backoff_base: Duration::from_secs(5),
            backoff_cap: Duration::from_secs(300),
            max_attempts: 8,
            timeout: Duration::from_secs(10),
            url_max_bytes: 2048,
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
        if self.url_max_bytes == 0 {
            return Err(invalid(
                "MANSIO_WEBHOOK_URL_MAX_BYTES",
                "must be at least 1",
            ));
        }
        Ok(())
    }
}

/// Which broker to run on (`MANSIO_BROKER`), with what it needs.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "one value is built at startup; boxing would change a public field"
)]
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

/// What is wrong with a `facts` retention shorter than that of `work`, or
/// `None` if nothing is.
///
/// The engine rebuilds a work item from its message on `work.<queue>` and the
/// transitions recorded on `facts.work`. When Iggy has deleted the facts of an
/// item and kept its message, a rebuild finds an item nobody has touched and
/// offers it again, though it was finished; a snapshot reads the facts after
/// its offsets and can do nothing for the ones that are gone.
#[must_use]
pub fn retention_warning(retention: &Retention) -> Option<String> {
    let facts = retention.facts?;
    match retention.work {
        Some(work) if facts >= work => None,
        work => Some(format!(
            "MANSIO_RETENTION_FACTS_SECONDS ({}) is shorter than MANSIO_RETENTION_WORK_SECONDS ({}), so facts.work can lose the transitions of an item whose message work.<queue> still holds; \
             a start then offers a finished item again. Keep facts at least as long as work",
            facts.as_secs(),
            work.map_or_else(|| "forever".to_owned(), |work| work.as_secs().to_string()),
        )),
    }
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
    /// Whether the sink syncs each record to disk before the publish goes on
    /// (`MANSIO_SINK_SYNC`, default `true`). Turn it off for development only.
    pub sink_sync: bool,
    /// Whether `GET /metrics` is served and what it reports is counted
    /// (`MANSIO_METRICS`, default `true`).
    pub metrics: bool,
    /// The directory of the snapshots of each tenant's state
    /// (`MANSIO_SNAPSHOT_DIR`); `None` keeps none, and every start replays
    /// every topic from offset 0.
    pub snapshot_dir: Option<PathBuf>,
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
            idempotency_window: env.seconds(
                "MANSIO_IDEMPOTENCY_WINDOW_SECONDS",
                work_defaults.idempotency_window,
            )?,
            idempotency_max_keys: env.parse(
                "MANSIO_IDEMPOTENCY_MAX_KEYS",
                work_defaults.idempotency_max_keys,
            )?,
            recovery_log: env.seconds("MANSIO_RECOVERY_LOG_SECONDS", work_defaults.recovery_log)?,
            snapshot_interval: env.seconds(
                "MANSIO_SNAPSHOT_INTERVAL_SECONDS",
                work_defaults.snapshot_interval,
            )?,
            owner_ttl: env.seconds("MANSIO_OWNER_TTL_SECONDS", work_defaults.owner_ttl)?,
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
            follow: Duration::from_millis(env.parse(
                "MANSIO_CREDENTIAL_FOLLOW_MILLIS",
                u64::try_from(credential_defaults.follow.as_millis()).unwrap_or(u64::MAX),
            )?),
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
            shutdown_grace: env.seconds(
                "MANSIO_SHUTDOWN_GRACE_SECONDS",
                stream_defaults.shutdown_grace,
            )?,
            sse_max_per_credential: env.parse(
                "MANSIO_SSE_MAX_PER_CREDENTIAL",
                stream_defaults.sse_max_per_credential,
            )?,
            sse_max: env.parse("MANSIO_SSE_MAX", stream_defaults.sse_max)?,
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
            url_max_bytes: env.parse(
                "MANSIO_WEBHOOK_URL_MAX_BYTES",
                webhook_defaults.url_max_bytes,
            )?,
        };
        webhooks.validate()?;

        let broker = match env.string("MANSIO_BROKER", "iggy").as_str() {
            "iggy" => Backend::Iggy(Self::iggy_config(&env, work.idempotency_window)?),
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
            sink_sync: env.parse("MANSIO_SINK_SYNC", DEFAULT_SINK_SYNC)?,
            metrics: env.parse("MANSIO_METRICS", DEFAULT_METRICS)?,
            snapshot_dir: match env.raw("MANSIO_SNAPSHOT_DIR") {
                None => Some(PathBuf::from(DEFAULT_SNAPSHOT_DIR)),
                Some(dir) if dir.is_empty() => None,
                Some(dir) => Some(PathBuf::from(dir)),
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
        idempotency_window: Duration,
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
        let invalid = |name: &'static str, reason: &str| SettingsError::Invalid {
            name,
            reason: reason.to_owned(),
        };
        let signin_seconds = env.parse(
            "MANSIO_IGGY_SIGNIN_TIMEOUT_SECONDS",
            DEFAULT_SIGNIN_TIMEOUT.as_secs(),
        )?;
        if signin_seconds == 0 {
            return Err(invalid(
                "MANSIO_IGGY_SIGNIN_TIMEOUT_SECONDS",
                "must be at least 1",
            ));
        }
        config.signin_timeout = Duration::from_secs(signin_seconds);
        let idle_seconds = env.parse(
            "MANSIO_IGGY_SESSION_IDLE_SECONDS",
            DEFAULT_SESSION_IDLE.as_secs(),
        )?;
        if idle_seconds == 0 {
            return Err(invalid(
                "MANSIO_IGGY_SESSION_IDLE_SECONDS",
                "must be at least 1",
            ));
        }
        config.session_idle = Duration::from_secs(idle_seconds);
        let clients = env.parse("MANSIO_IGGY_SESSION_CLIENTS", DEFAULT_SESSION_CLIENTS)?;
        if !(1..=MAX_SESSION_CLIENTS).contains(&clients) {
            return Err(invalid(
                "MANSIO_IGGY_SESSION_CLIENTS",
                "must be between 1 and 64",
            ));
        }
        config.session_clients = clients;
        config.idempotency_expiry = idempotency_window;
        config.locate_window = env.parse("MANSIO_IGGY_LOCATE_WINDOW", DEFAULT_LOCATE_WINDOW)?;
        config.segment_size_bytes = env.parse("MANSIO_IGGY_SEGMENT_SIZE_BYTES", 0_u64)?;
        check_segment_size(config.segment_size_bytes).map_err(|reason| SettingsError::Invalid {
            name: "MANSIO_IGGY_SEGMENT_SIZE_BYTES",
            reason: reason.to_owned(),
        })?;
        config.presence_segment_size_bytes = env.parse(
            "MANSIO_IGGY_PRESENCE_SEGMENT_SIZE_BYTES",
            DEFAULT_PRESENCE_SEGMENT_SIZE_BYTES,
        )?;
        check_segment_size(config.presence_segment_size_bytes).map_err(|reason| {
            SettingsError::Invalid {
                name: "MANSIO_IGGY_PRESENCE_SEGMENT_SIZE_BYTES",
                reason: reason.to_owned(),
            }
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
        assert_eq!(iggy.signin_timeout, Duration::from_secs(10));
        assert_eq!(iggy.session_idle, Duration::from_secs(300));
        assert_eq!(iggy.session_clients, 2);
        assert_eq!(iggy.locate_window, 100);
        assert_eq!(iggy.segment_size_bytes, 0);
        assert_eq!(iggy.presence_segment_size_bytes, 1_048_576);
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
        assert_eq!(work.recovery_log, Duration::from_secs(60));
        assert_eq!(work.snapshot_interval, Duration::from_secs(300));
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
            ("MANSIO_IDEMPOTENCY_WINDOW_SECONDS", "90"),
            ("MANSIO_IDEMPOTENCY_MAX_KEYS", "50"),
            ("MANSIO_RECOVERY_LOG_SECONDS", "9"),
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
                idempotency_window: Duration::from_secs(90),
                idempotency_max_keys: 50,
                recovery_log: Duration::from_secs(9),
                snapshot_interval: Duration::from_secs(300),
                owner_ttl: Duration::ZERO,
            }
        );
    }

    #[test]
    fn the_snapshot_interval_and_directory_are_settings() {
        let defaults = settings(&[("MANSIO_TOKEN", "t"), ("MANSIO_BROKER", "memory")]).unwrap();
        assert_eq!(defaults.work.snapshot_interval, Duration::from_secs(300));
        assert_eq!(
            defaults.snapshot_dir,
            Some(PathBuf::from("mansio-snapshots"))
        );

        let set = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_SNAPSHOT_INTERVAL_SECONDS", "0"),
            ("MANSIO_SNAPSHOT_DIR", "/data/snapshots"),
        ])
        .unwrap();
        assert_eq!(set.work.snapshot_interval, Duration::ZERO);
        assert_eq!(set.snapshot_dir, Some(PathBuf::from("/data/snapshots")));

        let off = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_SNAPSHOT_DIR", ""),
        ])
        .unwrap();
        assert_eq!(off.snapshot_dir, None);
    }

    #[test]
    fn servers_share_a_broker_only_when_an_owner_ttl_is_set() {
        let base = [("MANSIO_TOKEN", "t"), ("MANSIO_BROKER", "memory")];
        let defaults = settings(&base).unwrap();
        assert_eq!(defaults.work.owner_ttl, Duration::ZERO);

        let with =
            |ttl: &'static str| settings(&[base[0], base[1], ("MANSIO_OWNER_TTL_SECONDS", ttl)]);
        assert_eq!(with("15").unwrap().work.owner_ttl, Duration::from_secs(15));
        assert_eq!(with("0").unwrap().work.owner_ttl, Duration::ZERO);
        // Too short to renew inside: renewals are a quarter of it apart.
        assert!(with("2").is_err());
    }

    #[test]
    fn a_facts_retention_shorter_than_work_is_a_warning() {
        let hours = |n: u64| Some(Duration::from_secs(n * 3600));
        let with = |facts, work| Retention {
            facts,
            work,
            ..Retention::default()
        };
        assert!(retention_warning(&with(None, None)).is_none());
        assert!(retention_warning(&with(None, hours(1))).is_none());
        assert!(retention_warning(&with(hours(2), hours(1))).is_none());
        assert!(retention_warning(&with(hours(1), hours(1))).is_none());
        assert!(retention_warning(&with(hours(1), hours(2))).is_some());
        assert!(retention_warning(&with(hours(1), None)).is_some());
    }

    #[test]
    fn work_settings_that_do_not_fit_are_refused_by_name() {
        for (name, value) in [
            ("MANSIO_LEASE_SECONDS", "0"),
            ("MANSIO_LEASE_MAX_SECONDS", "soon"),
            // Past ten years, where a lease would pass the range of a time.
            ("MANSIO_LEASE_SECONDS", "315360001"),
            ("MANSIO_LEASE_MAX_SECONDS", "315360001"),
            ("MANSIO_LEASE_MAX_SECONDS", "18446744073709551615"),
            ("MANSIO_LEASE_SECONDS", "18446744073709551615"),
            ("MANSIO_MAX_ATTEMPTS", "0"),
            ("MANSIO_BACKOFF_BASE_SECONDS", "0"),
            ("MANSIO_ALERT_RESURFACE_SECONDS", "0"),
            ("MANSIO_RECOVERY_LOG_SECONDS", "0"),
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
    fn a_lease_of_ten_years_is_accepted_and_its_end_is_still_a_time() {
        let settings = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_LEASE_SECONDS", "315360000"),
            ("MANSIO_LEASE_MAX_SECONDS", "315360000"),
        ])
        .unwrap();
        assert_eq!(settings.work.lease, MAX_LEASE);
        assert_eq!(settings.work.lease_max, MAX_LEASE);
        // Ten years from a date well after now still reads back from a fact.
        let ceiling = i64::try_from(MAX_LEASE.as_millis()).unwrap();
        assert!(mansio_core::Timestamp::from_unix_millis(4_000_000_000_000 + ceiling).is_ok());
        // One second more is refused, naming the variable.
        let error = self::settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_LEASE_MAX_SECONDS", "315360001"),
        ])
        .unwrap_err();
        assert!(
            error.to_string().contains("MANSIO_LEASE_MAX_SECONDS"),
            "{error}"
        );
        assert!(error.to_string().contains("ten years"), "{error}");
    }

    #[test]
    fn the_credential_defaults_are_the_documented_ones() {
        let credentials = CredentialSettings::default();
        assert_eq!(credentials.agent_ttl, Duration::from_secs(3600));
        assert_eq!(credentials.recheck, Duration::from_secs(30));
        assert_eq!(credentials.sweep, Duration::from_secs(60));
        assert_eq!(credentials.follow, Duration::from_millis(1000));
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
            ("MANSIO_CREDENTIAL_FOLLOW_MILLIS", "250"),
        ])
        .unwrap();
        assert_eq!(
            settings.credentials,
            CredentialSettings {
                agent_ttl: Duration::from_secs(120),
                recheck: Duration::from_secs(5),
                sweep: Duration::from_secs(7),
                follow: Duration::from_millis(250),
            }
        );
    }

    #[test]
    fn credential_settings_that_cannot_work_are_refused_by_name() {
        for (name, value) in [
            ("MANSIO_AGENT_CREDENTIAL_TTL_SECONDS", "0"),
            ("MANSIO_CREDENTIAL_RECHECK_SECONDS", "0"),
            ("MANSIO_CREDENTIAL_SWEEP_SECONDS", "0"),
            ("MANSIO_CREDENTIAL_FOLLOW_MILLIS", "0"),
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
        assert_eq!(stream.shutdown_grace, Duration::from_secs(10));
        assert_eq!(stream.sse_max_per_credential, 10);
        assert_eq!(stream.sse_max, 1000);
        assert_eq!(stream.validate(), Ok(()));
        let webhooks = WebhookSettings::default();
        assert_eq!(webhooks.backoff_base, Duration::from_secs(5));
        assert_eq!(webhooks.backoff_cap, Duration::from_secs(300));
        assert_eq!(webhooks.max_attempts, 8);
        assert_eq!(webhooks.timeout, Duration::from_secs(10));
        assert_eq!(webhooks.url_max_bytes, 2048);
        assert_eq!(webhooks.validate(), Ok(()));
    }

    #[test]
    fn stream_and_webhook_settings_are_read_from_the_environment() {
        let settings = settings(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_BROKER", "memory"),
            ("MANSIO_STREAM_POLL_MILLIS", "50"),
            ("MANSIO_SSE_KEEPALIVE_SECONDS", "3"),
            ("MANSIO_SHUTDOWN_GRACE_SECONDS", "4"),
            ("MANSIO_SSE_MAX_PER_CREDENTIAL", "2"),
            ("MANSIO_SSE_MAX", "7"),
            ("MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS", "1"),
            ("MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS", "4"),
            ("MANSIO_WEBHOOK_MAX_ATTEMPTS", "5"),
            ("MANSIO_WEBHOOK_TIMEOUT_SECONDS", "2"),
            ("MANSIO_WEBHOOK_URL_MAX_BYTES", "512"),
        ])
        .unwrap();
        assert_eq!(
            settings.stream,
            StreamSettings {
                poll: Duration::from_millis(50),
                sse_keepalive: Duration::from_secs(3),
                shutdown_grace: Duration::from_secs(4),
                sse_max_per_credential: 2,
                sse_max: 7,
            }
        );
        assert_eq!(
            settings.webhooks,
            WebhookSettings {
                backoff_base: Duration::from_secs(1),
                backoff_cap: Duration::from_secs(4),
                max_attempts: 5,
                timeout: Duration::from_secs(2),
                url_max_bytes: 512,
            }
        );
    }

    #[test]
    fn stream_and_webhook_settings_that_cannot_work_are_refused_by_name() {
        for (name, value) in [
            ("MANSIO_STREAM_POLL_MILLIS", "0"),
            ("MANSIO_STREAM_POLL_MILLIS", "often"),
            ("MANSIO_SSE_KEEPALIVE_SECONDS", "0"),
            ("MANSIO_SHUTDOWN_GRACE_SECONDS", "0"),
            ("MANSIO_SSE_MAX_PER_CREDENTIAL", "0"),
            ("MANSIO_SSE_MAX", "0"),
            ("MANSIO_SSE_MAX", "many"),
            ("MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS", "0"),
            ("MANSIO_WEBHOOK_MAX_ATTEMPTS", "0"),
            ("MANSIO_WEBHOOK_TIMEOUT_SECONDS", "0"),
            ("MANSIO_WEBHOOK_TIMEOUT_SECONDS", "-1"),
            ("MANSIO_WEBHOOK_URL_MAX_BYTES", "0"),
            ("MANSIO_WEBHOOK_URL_MAX_BYTES", "long"),
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
    fn the_sink_syncs_unless_told_not_to() {
        let base = [("MANSIO_TOKEN", "t"), ("MANSIO_BROKER", "memory")];
        assert!(settings(&base).unwrap().sink_sync);
        let mut off = base.to_vec();
        off.push(("MANSIO_SINK_SYNC", "false"));
        assert!(!settings(&off).unwrap().sink_sync);
        let mut bad = base.to_vec();
        bad.push(("MANSIO_SINK_SYNC", "maybe"));
        assert!(matches!(
            settings(&bad).unwrap_err(),
            SettingsError::Invalid {
                name: "MANSIO_SINK_SYNC",
                ..
            }
        ));
    }

    #[test]
    fn metrics_are_on_unless_told_not_to() {
        let base = [("MANSIO_TOKEN", "t"), ("MANSIO_BROKER", "memory")];
        assert!(settings(&base).unwrap().metrics);
        let mut off = base.to_vec();
        off.push(("MANSIO_METRICS", "false"));
        assert!(!settings(&off).unwrap().metrics);
        let mut bad = base.to_vec();
        bad.push(("MANSIO_METRICS", "maybe"));
        assert!(matches!(
            settings(&bad).unwrap_err(),
            SettingsError::Invalid {
                name: "MANSIO_METRICS",
                ..
            }
        ));
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
    fn the_presence_segment_size_has_its_own_setting_and_checks() {
        let read = |value: &str| {
            settings(&[
                ("MANSIO_TOKEN", "t"),
                ("MANSIO_IGGY_PASSWORD", "p"),
                ("MANSIO_IGGY_PRESENCE_SEGMENT_SIZE_BYTES", value),
            ])
        };
        let Backend::Iggy(iggy) = read("2097152").unwrap().broker else {
            panic!("expected the Iggy broker");
        };
        assert_eq!(iggy.presence_segment_size_bytes, 2_097_152);
        assert_eq!(iggy.segment_size_bytes, 0);
        for bad in ["1024", "1048577", "big"] {
            assert!(
                matches!(
                    read(bad).unwrap_err(),
                    SettingsError::Invalid {
                        name: "MANSIO_IGGY_PRESENCE_SEGMENT_SIZE_BYTES",
                        ..
                    }
                ),
                "{bad}"
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
            ("MANSIO_IGGY_SIGNIN_TIMEOUT_SECONDS", "0"),
            ("MANSIO_IGGY_SESSION_IDLE_SECONDS", "0"),
            ("MANSIO_IGGY_SESSION_CLIENTS", "0"),
            ("MANSIO_IGGY_SESSION_CLIENTS", "65"),
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
