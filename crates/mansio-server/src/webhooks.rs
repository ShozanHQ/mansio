//! Webhooks: software that will not hold a connection is sent each envelope
//! of a topic as an HTTP POST.
//!
//! A webhook is registered by the admin API and kept in a log in the broker
//! that no credential can reach ([`mansio_core::SystemLog::Webhooks`]), which
//! is also the only place its signing secret is kept. Each webhook has a task
//! that follows its topic from a position the server keeps (see
//! [`crate::cursors`]), so a restarted server resumes where the last one
//! stopped, and delivers in offset order,
//! one envelope at a time. A delivery that the receiver does not accept with
//! a 2xx, or does not answer in time, is tried again after a doubling
//! backoff. Past `MANSIO_WEBHOOK_MAX_ATTEMPTS` it is recorded as
//! `mansio.webhook.failed` on `facts.webhook` and the webhook moves on.
//!
//! The log fails loud, as the principals' does: a record of it that cannot be
//! read (damaged, or written by a newer version of the format) keeps the
//! registry unloaded. Skipping it could revive a webhook that was deleted, which
//! would deliver to an address the admin took away, or lose the only copy of a
//! secret. Until the registry loads, the admin API answers 503, `/healthz`
//! says `degraded`, and no delivery task starts.
//!
//! A new webhook starts at the topic's first message or, if it asks for
//! `next`, after the last one the topic holds when it is made: the position
//! the task resumes from is set before the webhook is registered.
//!
//! A delivery goes only to an address outside the network the server runs in,
//! unless `MANSIO_WEBHOOK_ALLOW_PRIVATE_DESTINATIONS` says otherwise (see
//! [`crate::destination`]).
//!
//! `webhooks_rotate` replaces the signing secret. Until
//! `MANSIO_WEBHOOK_ROTATION_GRACE_SECONDS` have passed a delivery is signed
//! with both the new secret and the old, two `v1` values in one header, so a
//! receiver is moved from one to the other without a missed delivery; after
//! that, with the new one only.
//!
//! The creation, deletion and rotation of a webhook are recorded as
//! `mansio.webhook.created`, `.deleted` and `.rotated` in the sink and on
//! `facts.webhook` before the registry's log is written, so a change nobody
//! recorded is never made. They name no secret, and of the address only its
//! origin.
//!
//! Delivery is at least once: a server that stops between the receiver's
//! answer and the commit of the offset sends that envelope again. Its `id` is
//! the same, so a receiver deduplicates on it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, PoisonError};
use std::time::Duration;

use mansio_core::api::{CreateWebhook, WebhookCreated, WebhookRotated};
use mansio_core::webhook::{CONTENT_TYPE, SIGNATURE_HEADER, sign_many};
use mansio_core::{
    Broker, BrokerError, Caller, Principal, StartPosition, Stored, SystemLog, TenantId, Timestamp,
    Topic, Webhook, WebhookChange, WebhookError, WebhookEvent, WebhookFact, WebhookId, WebhookInfo,
    WebhookSecret, WebhookStatus,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, OnceCell};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::clock::Clock;
use crate::cursors::{Cursor, Cursors};
use crate::destination;
use crate::engine::Engine;
use crate::error::ApiError;
use crate::facts::Facts;
use crate::records;
use crate::settings::{StreamSettings, WebhookSettings};
use crate::state::backoff;
use crate::tail::Tail;

/// One record of the webhook log.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Entry {
    /// A webhook was made, with the secret its deliveries are signed with.
    Created {
        webhook: Webhook,
        secret: WebhookSecret,
    },
    /// The webhook was deleted.
    Deleted { id: WebhookId, at: Timestamp },
    /// The webhook's secret was replaced. The one it replaced signs
    /// deliveries too until `previous_until`.
    Rotated {
        id: WebhookId,
        secret: WebhookSecret,
        previous_until: Timestamp,
        at: Timestamp,
    },
}

impl Entry {
    fn encode(&self) -> Result<Vec<u8>, ApiError> {
        records::encode(self)
    }
}

/// Why the registry could not be loaded.
#[derive(Debug)]
enum LoadError {
    /// The broker could not be read.
    Broker(BrokerError),
    /// A record of the log exists and cannot be read.
    Unreadable { offset: u64, reason: String },
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Broker(error) => write!(f, "the broker failed: {error}"),
            LoadError::Unreadable { offset, reason } => write!(
                f,
                "the record at offset {offset} of the webhooks' log cannot be read: {reason}"
            ),
        }
    }
}

impl From<BrokerError> for LoadError {
    fn from(error: BrokerError) -> Self {
        LoadError::Broker(error)
    }
}

/// What a request is told while the registry is not loaded. The cause is in the
/// server's log.
fn not_loaded() -> ApiError {
    ApiError::Unavailable(
        "the registry of webhooks is not loaded, so no webhook is delivered; see the server log"
            .to_owned(),
    )
}

/// How the load of the registry is going: how many attempts in a row have
/// failed, and when the next may be made.
#[derive(Debug, Default)]
struct Loading {
    failures: u32,
    retry_at: Option<i64>,
    /// What the last failure said, so that a failure with another cause is
    /// logged again.
    cause: Option<String>,
}

/// A run of failures of one thing a task does, in the log and in the wait. The
/// first failure of a run is logged, and so is the end of it; the failures
/// between are not, so a broker that is down costs a webhook two lines and not
/// two a pass. The wait grows with the run, from the webhooks' backoff base to
/// its cap.
struct Streak {
    what: &'static str,
    webhook: WebhookId,
    failures: u32,
}

impl Streak {
    fn new(what: &'static str, webhook: WebhookId) -> Self {
        Self {
            what,
            webhook,
            failures: 0,
        }
    }

    /// A failure. How long to wait before trying again.
    fn failed(&mut self, cause: &dyn std::fmt::Display, settings: &WebhookSettings) -> Duration {
        self.failures = self.failures.saturating_add(1);
        let wait = backoff(self.failures, settings.backoff_base, settings.backoff_cap);
        if self.failures == 1 {
            warn!(
                webhook = %self.webhook,
                what = self.what,
                %cause,
                "a webhook is failing; it tries again with a growing wait and logs when it recovers"
            );
        }
        wait
    }

    /// Success. Logs the end of a run of failures, if there was one.
    fn ok(&mut self) {
        if self.failures > 0 {
            info!(
                webhook = %self.webhook,
                what = self.what,
                failures = self.failures,
                "a webhook has recovered"
            );
            self.failures = 0;
        }
    }
}

/// The secrets a webhook's deliveries are signed with.
#[derive(Debug, Clone)]
struct Signing {
    current: WebhookSecret,
    /// The secret that was replaced, and until when (unix milliseconds) it
    /// signs too.
    previous: Option<(WebhookSecret, i64)>,
}

impl Signing {
    /// The signature header for `body` sent at `now` (unix milliseconds).
    fn header(&self, now: i64, body: &[u8]) -> String {
        let mut secrets = vec![&self.current];
        if let Some((previous, until)) = &self.previous
            && now < *until
        {
            secrets.push(previous);
        }
        sign_many(&secrets, now.div_euclid(1000), body)
    }
}

/// A registered webhook and its secrets.
#[derive(Debug, Clone)]
struct Record {
    webhook: Webhook,
    signing: Signing,
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// What a failed request says, without the address it was sent to: an address
/// can carry a secret of the receiver's own.
fn describe(error: &reqwest::Error, timeout: Duration) -> String {
    if destination::was_refused(error) {
        destination::REFUSED.to_owned()
    } else if error.is_timeout() {
        format!("no answer within {}s", timeout.as_secs())
    } else if error.is_connect() {
        "could not connect".to_owned()
    } else {
        "the request failed".to_owned()
    }
}

/// The scheme, host and port of an address, without a user name, path, query
/// or fragment, any of which can hold a secret of the receiver's own.
fn origin_of(url: &str) -> String {
    reqwest::Url::parse(url)
        .map(|parsed| parsed.origin().ascii_serialization())
        .unwrap_or_default()
}

/// Check an address a webhook may be sent to: at most `max` bytes, `http` or
/// `https`, with a host.
fn check_url(url: &str, max: usize) -> Result<(), ApiError> {
    let bad = |why: String| ApiError::BadRequest(format!("url {why}"));
    if url.len() > max {
        return Err(bad(format!("is longer than {max} bytes")));
    }
    let parsed = reqwest::Url::parse(url).map_err(|e| bad(format!("is not valid: {e}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(bad("must be http or https".to_owned()));
    }
    if parsed.host_str().is_none() {
        return Err(bad("must name a host".to_owned()));
    }
    Ok(())
}

/// What the tasks that deliver need, shared among them.
struct Delivery {
    broker: Arc<dyn Broker>,
    engine: Arc<Engine>,
    cursors: Arc<Cursors>,
    clock: Arc<dyn Clock>,
    facts: Facts,
    /// `None` if the client could not be built, in which case every attempt
    /// fails and says so.
    http: Option<reqwest::Client>,
    settings: WebhookSettings,
    stream: StreamSettings,
    /// How many messages to read at a time.
    page: u32,
    status: StdMutex<HashMap<WebhookId, WebhookStatus>>,
    /// What each webhook's deliveries are signed with now, which a rotation
    /// changes under a task that is running.
    signing: StdMutex<HashMap<WebhookId, Signing>>,
}

impl Delivery {
    fn signing(&self) -> MutexGuard<'_, HashMap<WebhookId, Signing>> {
        self.signing.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn status(&self) -> MutexGuard<'_, HashMap<WebhookId, WebhookStatus>> {
        self.status.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn delivered(&self, id: WebhookId, offset: u64) {
        self.status().entry(id).or_default().last_delivered_offset = Some(offset);
    }

    fn failed(&self, id: WebhookId, offset: u64, message: String) {
        let Ok(at) = Timestamp::from_unix_millis(self.clock.now_millis()) else {
            return;
        };
        self.status().entry(id).or_default().last_error = Some(WebhookError {
            at,
            offset,
            message,
        });
    }

    /// Follow the webhook's topic for as long as the task lives: start where
    /// its position says it stopped, and deliver what comes in order.
    async fn follow(self: Arc<Self>, webhook: Webhook) {
        let poll = self.stream.poll;
        let cursor = Cursor::Webhook(webhook.id);
        let mut streak = Streak::new("reading where it stopped", webhook.id);
        let start = loop {
            match self.cursors.get(&cursor).await {
                Ok(position) => {
                    streak.ok();
                    match position {
                        Some(committed) => {
                            self.delivered(webhook.id, committed);
                            break committed.saturating_add(1);
                        }
                        // Nothing committed: from the first message.
                        None => break 0,
                    }
                }
                Err(error) => {
                    let wait = streak.failed(&format!("{error:?}"), &self.settings);
                    tokio::time::sleep(wait).await;
                }
            }
        };
        let mut streak = Streak::new("reading its topic", webhook.id);
        let mut tail = Tail::new(
            Arc::clone(&self.broker),
            webhook.tenant.clone(),
            webhook.topic.clone(),
            start,
            self.page,
        );
        loop {
            match tail.read(&Caller::Admin).await {
                Ok(batch) if batch.is_empty() => {
                    streak.ok();
                    tokio::time::sleep(poll).await;
                }
                Ok(batch) => {
                    streak.ok();
                    for stored in batch {
                        self.deliver(&webhook, &stored).await;
                    }
                }
                // The topic is made by the first publish to it, which may
                // come after the webhook.
                Err(BrokerError::NotFound(_)) => {
                    streak.ok();
                    tokio::time::sleep(poll).await;
                }
                Err(error) => {
                    let wait = streak.failed(&error, &self.settings);
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    /// Deliver one envelope: try until the receiver accepts it or the
    /// attempts are used, then move past it either way.
    async fn deliver(&self, webhook: &Webhook, stored: &Stored) {
        let outcome = match stored.envelope.to_json_vec() {
            Ok(body) => self.attempts(webhook, stored, &body).await,
            Err(error) => Err((1, format!("the envelope cannot be written: {error}"))),
        };
        if let Err((attempts, error)) = &outcome {
            self.record_failure(webhook, stored, *attempts, error).await;
        }
        let committed = self
            .cursors
            .set(Cursor::Webhook(webhook.id), stored.offset)
            .await;
        if let Err(error) = committed {
            // The next commit covers this one; a stop before it sends this
            // envelope again.
            warn!(webhook = %webhook.id, offset = stored.offset, ?error, "the webhook's position could not be stored");
        }
        if outcome.is_ok() {
            self.delivered(webhook.id, stored.offset);
        }
    }

    /// Post the body until it is accepted. On giving up, the number of
    /// attempts made and why the last one failed.
    async fn attempts(
        &self,
        webhook: &Webhook,
        stored: &Stored,
        body: &[u8],
    ) -> Result<(), (u32, String)> {
        let mut attempt = 0_u32;
        loop {
            attempt += 1;
            let Err(message) = self.post(webhook, body).await else {
                return Ok(());
            };
            self.failed(webhook.id, stored.offset, message.clone());
            if attempt >= self.settings.max_attempts {
                return Err((attempt, message));
            }
            let wait = backoff(
                attempt,
                self.settings.backoff_base,
                self.settings.backoff_cap,
            );
            warn!(
                webhook = %webhook.id,
                offset = stored.offset,
                attempt,
                error = %message,
                retry_in_seconds = wait.as_secs(),
                "a webhook delivery failed"
            );
            tokio::time::sleep(wait).await;
        }
    }

    /// One attempt: whether the receiver answered with a 2xx.
    async fn post(&self, webhook: &Webhook, body: &[u8]) -> Result<(), String> {
        let Some(http) = &self.http else {
            return Err("the HTTP client could not be made".to_owned());
        };
        // A host name is judged when the client resolves it; an IP address
        // is not resolved, so it is judged here.
        if !self.settings.allow_private_destinations
            && destination::literal_is_internal(&webhook.url)
        {
            return Err(destination::REFUSED.to_owned());
        }
        let signing = self.signing().get(&webhook.id).cloned();
        let Some(signing) = signing else {
            return Err("the webhook has no signing secret".to_owned());
        };
        let signature = signing.header(self.clock.now_millis(), body);
        let response = http
            .post(&webhook.url)
            .header(reqwest::header::CONTENT_TYPE, CONTENT_TYPE)
            .header(SIGNATURE_HEADER, signature)
            .body(body.to_vec())
            .send()
            .await
            .map_err(|error| describe(&error, self.settings.timeout))?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(format!("HTTP {}", status.as_u16()))
        }
    }

    /// Record that the webhook gave up on an envelope. A webhook that cannot
    /// say so does not move on: it tries again until it can.
    async fn record_failure(&self, webhook: &Webhook, stored: &Stored, attempts: u32, error: &str) {
        let data = WebhookFact {
            webhook_id: webhook.id,
            topic: webhook.topic.clone(),
            offset: stored.offset,
            message_id: stored.envelope.id,
            attempts,
            error: error.to_owned(),
        };
        warn!(
            webhook = %webhook.id,
            offset = stored.offset,
            attempts,
            %error,
            "a webhook gave up on a delivery"
        );
        let mut streak = Streak::new("recording a delivery it gave up on", webhook.id);
        loop {
            let recorded = match self.facts.webhook(
                &webhook.tenant,
                WebhookEvent::Failed,
                self.clock.now_millis(),
                &data,
            ) {
                Ok(fact) => self.engine.record(fact).await.map(|_| ()),
                Err(error) => Err(error),
            };
            match recorded {
                Ok(()) => {
                    streak.ok();
                    return;
                }
                Err(failure) => {
                    let wait = streak.failed(&format!("{failure:?}"), &self.settings);
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }
}

/// The webhooks of a server: the registry, and the tasks that deliver.
pub struct Webhooks {
    broker: Arc<dyn Broker>,
    clock: Arc<dyn Clock>,
    delivery: Arc<Delivery>,
    /// How many records to read at a time when rebuilding the registry.
    page: u32,
    registry: StdMutex<HashMap<WebhookId, Record>>,
    loaded: OnceCell<()>,
    /// How the load is going, held while it is tried.
    loading: AsyncMutex<Loading>,
    workers: StdMutex<HashMap<WebhookId, JoinHandle<()>>>,
    /// Held while a secret is rotated, so that two rotations of a webhook are
    /// written to the log in the order they take effect.
    rotating: AsyncMutex<()>,
}

impl std::fmt::Debug for Webhooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Webhooks")
            .field("settings", &self.delivery.settings)
            .finish_non_exhaustive()
    }
}

impl Drop for Webhooks {
    fn drop(&mut self) {
        for task in self
            .workers
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
        {
            task.abort();
        }
    }
}

impl Webhooks {
    /// Webhooks over `broker`, whose failures are recorded through `engine`.
    /// `page` is how many messages and records to read at a time.
    #[must_use]
    pub(crate) fn new(
        broker: Arc<dyn Broker>,
        engine: Arc<Engine>,
        cursors: Arc<Cursors>,
        clock: Arc<dyn Clock>,
        settings: WebhookSettings,
        stream: StreamSettings,
        page: u32,
    ) -> Self {
        // A client that follows no redirect: a delivery is signed for the
        // address it was made for, and goes nowhere else.
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(settings.timeout);
        if !settings.allow_private_destinations {
            // The guard judges the address the webhook's host resolves to, so
            // the request must be made to that host and not through a proxy
            // named in the environment, which would resolve it instead.
            builder = builder.no_proxy().dns_resolver(destination::Guard);
        }
        let http = builder
            .build()
            .inspect_err(
                |reason| tracing::error!(%reason, "the webhook HTTP client could not be made"),
            )
            .ok();
        let page = page.max(1);
        Self {
            broker: Arc::clone(&broker),
            clock: Arc::clone(&clock),
            delivery: Arc::new(Delivery {
                broker,
                engine,
                cursors,
                clock,
                facts: Facts::new(),
                http,
                settings,
                stream,
                page,
                status: StdMutex::default(),
                signing: StdMutex::default(),
            }),
            page,
            registry: StdMutex::default(),
            loaded: OnceCell::new(),
            loading: AsyncMutex::default(),
            workers: StdMutex::default(),
            rotating: AsyncMutex::default(),
        }
    }

    /// The settings webhooks deliver with.
    #[must_use]
    pub fn settings(&self) -> &WebhookSettings {
        &self.delivery.settings
    }

    fn registry(&self) -> MutexGuard<'_, HashMap<WebhookId, Record>> {
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn workers(&self) -> MutexGuard<'_, HashMap<WebhookId, JoinHandle<()>>> {
        self.workers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // ---- start ------------------------------------------------------------

    /// Rebuild the registry from the log, once, and start the tasks.
    ///
    /// If the log cannot be read, or holds a record that cannot be (damaged,
    /// or written by a newer version of the format), nothing is started: the
    /// failure is logged at `error` once, and every call is refused with 503
    /// until a wait that grows from `MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS` to
    /// its cap is over, when the next call reads the log again.
    ///
    /// # Errors
    ///
    /// Returns 503 if the registry is not loaded.
    pub async fn load(&self) -> Result<(), ApiError> {
        if self.loaded.initialized() {
            self.sync();
            return Ok(());
        }
        let mut loading = self.loading.lock().await;
        if self.loaded.initialized() {
            self.sync();
            return Ok(());
        }
        let now = self.clock.now_millis();
        if loading.retry_at.is_some_and(|at| now < at) {
            return Err(not_loaded());
        }
        match self.rebuild().await {
            Ok(()) => {
                // Held under `loading`, so no other attempt is racing this one.
                let _ = self.loaded.set(());
                if loading.failures > 0 {
                    info!(
                        failures = loading.failures,
                        "the registry of webhooks is loaded"
                    );
                }
                *loading = Loading::default();
                self.sync();
                Ok(())
            }
            Err(cause) => {
                loading.failures = loading.failures.saturating_add(1);
                let settings = &self.delivery.settings;
                let wait = backoff(
                    loading.failures,
                    settings.backoff_base,
                    settings.backoff_cap,
                );
                loading.retry_at =
                    Some(now.saturating_add(i64::try_from(wait.as_millis()).unwrap_or(i64::MAX)));
                // Logged when a run of failures starts, and again when what
                // goes wrong changes: a broker that is down and a record that
                // cannot be read are different things to put right.
                let said = cause.to_string();
                if loading.cause.as_deref() != Some(said.as_str()) {
                    tracing::error!(
                        %cause,
                        retry_seconds = wait.as_secs(),
                        "the registry of webhooks could not be loaded; no webhook is delivered until it is"
                    );
                    loading.cause = Some(said);
                }
                Err(not_loaded())
            }
        }
    }

    async fn rebuild(&self) -> Result<(), LoadError> {
        let mut records: HashMap<WebhookId, Record> = HashMap::new();
        let mut offset = 0;
        loop {
            let batch = self
                .broker
                .system_read(SystemLog::Webhooks, offset, self.page)
                .await?;
            let Some(last) = batch.last() else { break };
            let next = last.offset.saturating_add(1);
            for record in &batch {
                let entry = records::decode::<Entry>(&record.payload).map_err(|reason| {
                    LoadError::Unreadable {
                        offset: record.offset,
                        reason: reason.to_string(),
                    }
                })?;
                match entry {
                    Entry::Created { webhook, secret } => {
                        records.insert(
                            webhook.id,
                            Record {
                                webhook,
                                signing: Signing {
                                    current: secret,
                                    previous: None,
                                },
                            },
                        );
                    }
                    Entry::Deleted { id, .. } => {
                        records.remove(&id);
                    }
                    Entry::Rotated {
                        id,
                        secret,
                        previous_until,
                        ..
                    } => {
                        if let Some(record) = records.get_mut(&id) {
                            let until = previous_until.unix_millis().unwrap_or(0);
                            let old = std::mem::replace(&mut record.signing.current, secret);
                            record.signing.previous = Some((old, until));
                        }
                    }
                }
            }
            if next <= offset {
                break;
            }
            offset = next;
        }
        info!(webhooks = records.len(), "webhooks loaded");
        if self.delivery.engine.shared() && !records.is_empty() {
            warn!(
                webhooks = records.len(),
                "servers share this broker (MANSIO_OWNER_TTL_SECONDS is set) and webhooks exist: every server that holds a webhook delivers it, so a receiver gets each delivery once per server, and a position committed on one server is not seen by the others"
            );
        }
        if self.delivery.engine.shared()
            && let Some(named @ 1..) = self.delivery.cursors.named().await
        {
            warn!(
                named_consumers = named,
                "servers share this broker (MANSIO_OWNER_TTL_SECONDS is set) and named consumers have positions: a position committed on one server is not seen by the others, so a consumer that moves between servers may read again from an older position"
            );
        }
        *self.registry() = records;
        Ok(())
    }

    /// Run until the task is dropped or aborted: load the registry, then keep
    /// one task per webhook alive.
    pub async fn run(self: Arc<Self>) {
        let poll = self.delivery.stream.poll;
        // A load that fails has logged why, once, and refuses until its wait is
        // over, so this asks every poll without costing a line each time.
        while self.load().await.is_err() {
            tokio::time::sleep(poll).await;
        }
        loop {
            tokio::time::sleep(poll).await;
            self.sync();
        }
    }

    /// Make the tasks match the registry: one for each webhook, and none for
    /// one that is gone.
    fn sync(&self) {
        let registry = self.registry();
        let mut workers = self.workers();
        workers.retain(|id, task| {
            let keep = registry.contains_key(id);
            if !keep {
                task.abort();
            }
            keep
        });
        // The secrets first, so that a task started below finds its own.
        {
            let mut signing = self.delivery.signing();
            signing.retain(|id, _| registry.contains_key(id));
            for (id, record) in registry.iter() {
                signing.insert(*id, record.signing.clone());
            }
        }
        for (id, record) in registry.iter() {
            if workers.get(id).is_some_and(|task| !task.is_finished()) {
                continue;
            }
            let task = tokio::spawn(Arc::clone(&self.delivery).follow(record.webhook.clone()));
            workers.insert(*id, task);
        }
        let mut status = self.delivery.status();
        status.retain(|id, _| registry.contains_key(id));
    }

    /// Stop every task, and wait until it has gone.
    pub async fn shutdown(&self) {
        let tasks: Vec<JoinHandle<()>> = self.workers().drain().map(|(_, task)| task).collect();
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
    }

    // ---- the admin API ------------------------------------------------------

    /// The next offset of the topic: where a webhook that starts at `next`
    /// begins. It reads the topic through, a page at a time.
    async fn topic_end(&self, webhook: &Webhook) -> Result<u64, ApiError> {
        let mut tail = Tail::new(
            Arc::clone(&self.broker),
            webhook.tenant.clone(),
            webhook.topic.clone(),
            0,
            self.page,
        );
        loop {
            match tail.read(&Caller::Admin).await {
                Ok(batch) if batch.is_empty() => return Ok(tail.next()),
                Ok(_) => {}
                // A topic nobody has published to yet: the next message is
                // the first.
                Err(BrokerError::NotFound(_)) => return Ok(tail.next()),
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Record the change to a webhook in the sink and on `facts.webhook`.
    async fn record_change(
        &self,
        by: &Principal,
        event: WebhookEvent,
        webhook: &Webhook,
        start: Option<StartPosition>,
        previous_secret_valid_until: Option<Timestamp>,
    ) -> Result<(), ApiError> {
        let data = WebhookChange {
            webhook_id: webhook.id,
            topic: webhook.topic.clone(),
            origin: origin_of(&webhook.url),
            start,
            previous_secret_valid_until,
        };
        let fact = self.delivery.facts.webhook_change(
            &webhook.tenant,
            event,
            by,
            self.clock.now_millis(),
            &data,
        )?;
        self.delivery.engine.record(fact).await.map(|_| ())
    }

    /// Make a webhook: `webhooks_create`. The secret it returns is shown this
    /// once.
    ///
    /// The creation is recorded as `mansio.webhook.created` before the
    /// registry is written. A webhook that starts at `next` gets its position
    /// set to the last message the topic holds, before it is registered.
    ///
    /// # Errors
    ///
    /// Returns 400 for an address that cannot be used or a topic that must not
    /// be delivered, and 502 or 503 if the registry cannot be written. Nothing
    /// is made then.
    pub(crate) async fn create(
        &self,
        by: &Principal,
        request: CreateWebhook,
    ) -> Result<WebhookCreated, ApiError> {
        self.load().await?;
        check_url(&request.url, self.delivery.settings.url_max_bytes)?;
        if request.topic == Topic::facts_webhook() {
            return Err(ApiError::BadRequest(
                "facts.webhook cannot be delivered: a delivery that failed would be delivered to itself"
                    .to_owned(),
            ));
        }
        let webhook = Webhook {
            id: WebhookId::generate(),
            tenant: request.tenant,
            topic: request.topic,
            url: request.url,
            created_at: Timestamp::from_unix_millis(self.clock.now_millis())
                .map_err(|_| ApiError::Internal)?,
        };
        if request.start == StartPosition::Next
            && let Some(last) = self.topic_end(&webhook).await?.checked_sub(1)
        {
            // The position is the last message handled, so the task starts
            // at the one after it. Set before the webhook is registered, so
            // that no task of it ever reads from the first.
            self.delivery
                .cursors
                .set(Cursor::Webhook(webhook.id), last)
                .await?;
        }
        self.record_change(
            by,
            WebhookEvent::Created,
            &webhook,
            Some(request.start),
            None,
        )
        .await?;
        let secret = WebhookSecret::random();
        let entry = Entry::Created {
            webhook: webhook.clone(),
            secret: secret.clone(),
        };
        self.broker
            .system_append(SystemLog::Webhooks, &entry.encode()?)
            .await?;
        self.registry().insert(
            webhook.id,
            Record {
                webhook: webhook.clone(),
                signing: Signing {
                    current: secret.clone(),
                    previous: None,
                },
            },
        );
        self.sync();
        info!(
            webhook = %webhook.id,
            tenant = %webhook.tenant,
            topic = %webhook.topic,
            start = %request.start,
            "webhook made"
        );
        Ok(WebhookCreated {
            webhook: self.info(&webhook),
            secret,
        })
    }

    fn info(&self, webhook: &Webhook) -> WebhookInfo {
        let status = self
            .delivery
            .status()
            .get(&webhook.id)
            .cloned()
            .unwrap_or_default();
        WebhookInfo {
            webhook: webhook.clone(),
            status,
        }
    }

    /// The webhooks of the tenant, or of every tenant, oldest first, with how
    /// each is getting on: `webhooks_list`. No secret is in it.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry cannot be loaded.
    pub(crate) async fn list(
        &self,
        tenant: Option<&TenantId>,
    ) -> Result<Vec<WebhookInfo>, ApiError> {
        self.load().await?;
        let mut webhooks: Vec<Webhook> = self
            .registry()
            .values()
            .map(|record| record.webhook.clone())
            .filter(|webhook| tenant.is_none_or(|tenant| webhook.tenant == *tenant))
            .collect();
        webhooks.sort_by(|a, b| {
            (a.created_at.unix_millis(), a.id).cmp(&(b.created_at.unix_millis(), b.id))
        });
        Ok(webhooks.iter().map(|webhook| self.info(webhook)).collect())
    }

    /// Delete a webhook: `webhooks_delete`. Its task stops at once; what was
    /// being delivered may or may not have arrived. The deletion is recorded
    /// as `mansio.webhook.deleted` before the registry is written.
    ///
    /// # Errors
    ///
    /// Returns 404 if there is no such webhook, and 502 or 503 if the
    /// registry cannot be written, in which case the webhook stays.
    pub(crate) async fn delete(&self, by: &Principal, id: WebhookId) -> Result<Webhook, ApiError> {
        self.load().await?;
        let found = self
            .registry()
            .get(&id)
            .map(|record| record.webhook.clone())
            .ok_or_else(|| ApiError::NotFound(format!("no webhook {id}")))?;
        let entry = Entry::Deleted {
            id,
            at: Timestamp::from_unix_millis(self.clock.now_millis())
                .map_err(|_| ApiError::Internal)?,
        };
        self.record_change(by, WebhookEvent::Deleted, &found, None, None)
            .await?;
        self.broker
            .system_append(SystemLog::Webhooks, &entry.encode()?)
            .await?;
        self.registry().remove(&id);
        self.sync();
        info!(webhook = %id, "webhook deleted");
        Ok(found)
    }

    /// Replace a webhook's signing secret: `webhooks_rotate`. The new secret
    /// is returned this once. Deliveries are signed with the new secret and
    /// the old until `MANSIO_WEBHOOK_ROTATION_GRACE_SECONDS` have passed, then
    /// with the new one only. The rotation is recorded as
    /// `mansio.webhook.rotated` before the registry is written.
    ///
    /// # Errors
    ///
    /// Returns 404 if there is no such webhook, and 502 or 503 if the
    /// registry cannot be written, in which case the secret stays.
    pub(crate) async fn rotate(
        &self,
        by: &Principal,
        id: WebhookId,
    ) -> Result<WebhookRotated, ApiError> {
        self.load().await?;
        let _rotating = self.rotating.lock().await;
        let found = self
            .registry()
            .get(&id)
            .map(|record| record.webhook.clone())
            .ok_or_else(|| ApiError::NotFound(format!("no webhook {id}")))?;
        let now = self.clock.now_millis();
        let at = Timestamp::from_unix_millis(now).map_err(|_| ApiError::Internal)?;
        let until_millis = now.saturating_add(millis(self.delivery.settings.rotation_grace));
        let until = Timestamp::from_unix_millis(until_millis).map_err(|_| ApiError::Internal)?;
        let secret = WebhookSecret::random();
        self.record_change(by, WebhookEvent::Rotated, &found, None, Some(until.clone()))
            .await?;
        let entry = Entry::Rotated {
            id,
            secret: secret.clone(),
            previous_until: until.clone(),
            at,
        };
        self.broker
            .system_append(SystemLog::Webhooks, &entry.encode()?)
            .await?;
        {
            let mut registry = self.registry();
            let Some(record) = registry.get_mut(&id) else {
                // Deleted while the rotation was written: nothing to sign for.
                return Err(ApiError::NotFound(format!("no webhook {id}")));
            };
            let old = std::mem::replace(&mut record.signing.current, secret.clone());
            record.signing.previous = Some((old, until_millis));
        }
        self.sync();
        info!(webhook = %id, "webhook secret rotated");
        Ok(WebhookRotated {
            webhook: self.info(&found),
            secret,
            previous_secret_valid_until: until,
        })
    }
}

/// Read a record of the webhooks log. For the fuzz targets.
#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_record(bytes: &[u8]) -> bool {
    records::decode::<Entry>(bytes).is_ok()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    use async_trait::async_trait;
    use mansio_core::{Envelope, MemoryBroker, NoSink, SystemRecord};

    use super::*;
    use crate::clock::manual::ManualClock;
    use crate::settings::WorkSettings;
    use crate::testing::Logs;

    #[test]
    fn only_http_and_https_addresses_with_a_host_are_accepted() {
        for ok in [
            "http://127.0.0.1:9000/hook",
            "https://example.com/a/b?c=d",
            "http://localhost/",
        ] {
            assert!(check_url(ok, 2048).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "example.com/hook",
            "ftp://example.com/",
            "file:///etc/passwd",
            "http://",
            "javascript:alert(1)",
            &format!("https://example.com/{}", "a".repeat(2048)),
        ] {
            assert!(
                matches!(check_url(bad, 2048), Err(ApiError::BadRequest(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn the_longest_address_is_the_setting() {
        let url = "https://example.com/hook";
        assert!(check_url(url, url.len()).is_ok());
        let error = check_url(url, url.len() - 1).unwrap_err();
        assert!(
            matches!(&error, ApiError::BadRequest(why) if why.contains("longer than 23 bytes")),
            "{error:?}"
        );
    }

    fn a_webhook() -> Webhook {
        Webhook {
            id: WebhookId::generate(),
            tenant: "acme".parse().unwrap(),
            topic: "facts.build".parse().unwrap(),
            url: "https://example.com/hook".to_owned(),
            created_at: "2026-10-26T12:00:00Z".parse().unwrap(),
        }
    }

    #[test]
    fn a_record_of_the_log_round_trips_carries_its_version_and_shows_no_secret() {
        let webhook = a_webhook();
        let secret = WebhookSecret::random();
        for entry in [
            Entry::Created {
                webhook: webhook.clone(),
                secret: secret.clone(),
            },
            Entry::Deleted {
                id: webhook.id,
                at: "2026-10-26T13:00:00Z".parse().unwrap(),
            },
        ] {
            let bytes = entry.encode().unwrap();
            let back = records::decode::<Entry>(&bytes).unwrap();
            assert_eq!(back.encode().unwrap(), bytes);
            assert!(!format!("{entry:?}").contains(secret.expose()));
        }
        let created = Entry::Created { webhook, secret };
        let text = String::from_utf8(created.encode().unwrap()).unwrap();
        assert!(text.contains(r#""op":"created""#), "{text}");
        assert!(text.contains(r#""v":1"#), "{text}");
    }

    /// A memory broker whose reads of the logs and of a topic fail as told.
    #[derive(Default)]
    struct Flaky {
        inner: MemoryBroker,
        /// How many reads of the webhooks' log fail before one does not.
        webhook_log_reads_to_fail: AtomicU32,
        webhook_log_reads: AtomicU32,
        cursor_log_down: AtomicBool,
        topics_down: AtomicBool,
    }

    #[async_trait]
    impl Broker for Flaky {
        async fn tenants(&self) -> Result<Vec<TenantId>, BrokerError> {
            self.inner.tenants().await
        }

        async fn topics(&self, tenant: &TenantId) -> Result<Vec<Topic>, BrokerError> {
            self.inner.topics(tenant).await
        }

        async fn ensure_tenant(&self, tenant: &TenantId) -> Result<(), BrokerError> {
            self.inner.ensure_tenant(tenant).await
        }

        async fn ensure_topic(&self, tenant: &TenantId, topic: &Topic) -> Result<(), BrokerError> {
            self.inner.ensure_topic(tenant, topic).await
        }

        async fn publish(
            &self,
            caller: &Caller<'_>,
            envelope: &Envelope,
        ) -> Result<Option<u64>, BrokerError> {
            self.inner.publish(caller, envelope).await
        }

        async fn poll(
            &self,
            caller: &Caller<'_>,
            tenant: &TenantId,
            topic: &Topic,
            offset: u64,
            max: u32,
        ) -> Result<Vec<Stored>, BrokerError> {
            if self.topics_down.load(Ordering::SeqCst) {
                return Err(BrokerError::Unavailable("down".to_owned()));
            }
            self.inner.poll(caller, tenant, topic, offset, max).await
        }

        async fn system_append(&self, log: SystemLog, record: &[u8]) -> Result<(), BrokerError> {
            self.inner.system_append(log, record).await
        }

        async fn system_read(
            &self,
            log: SystemLog,
            offset: u64,
            max: u32,
        ) -> Result<Vec<SystemRecord>, BrokerError> {
            match log {
                SystemLog::Webhooks => {
                    self.webhook_log_reads.fetch_add(1, Ordering::SeqCst);
                    let failing = self.webhook_log_reads_to_fail.load(Ordering::SeqCst);
                    if failing > 0 {
                        self.webhook_log_reads_to_fail
                            .store(failing - 1, Ordering::SeqCst);
                        return Err(BrokerError::Unavailable("down".to_owned()));
                    }
                }
                SystemLog::Cursors if self.cursor_log_down.load(Ordering::SeqCst) => {
                    return Err(BrokerError::Unavailable("down".to_owned()));
                }
                _ => {}
            }
            self.inner.system_read(log, offset, max).await
        }
    }

    struct Rig {
        broker: Arc<Flaky>,
        clock: Arc<ManualClock>,
        webhooks: Arc<Webhooks>,
    }

    /// Webhooks over a flaky broker and a clock moved by hand, with waits of
    /// a few milliseconds so that a test of what a task does over time is fast.
    fn rig(base: Duration, cap: Duration) -> Rig {
        rig_with(base, cap, true)
    }

    /// As [`rig`], with delivery to private addresses allowed or refused.
    fn rig_with(base: Duration, cap: Duration, allow_private_destinations: bool) -> Rig {
        let broker = Arc::new(Flaky::default());
        let clock = Arc::new(ManualClock::new(1_792_000_000_000));
        let engine = Arc::new(Engine::new(
            broker.clone(),
            Arc::new(NoSink),
            clock.clone(),
            WorkSettings::default(),
            10,
        ));
        let cursors = Arc::new(Cursors::new(broker.clone(), 10));
        let webhooks = Arc::new(Webhooks::new(
            broker.clone(),
            engine,
            cursors,
            clock.clone(),
            WebhookSettings {
                backoff_base: base,
                backoff_cap: cap,
                max_attempts: 3,
                timeout: Duration::from_secs(1),
                url_max_bytes: 2048,
                allow_private_destinations,
                rotation_grace: Duration::from_secs(60),
            },
            StreamSettings {
                poll: Duration::from_millis(5),
                ..StreamSettings::default()
            },
            10,
        ));
        Rig {
            broker,
            clock,
            webhooks,
        }
    }

    fn admin() -> Principal {
        "service:admin".parse().unwrap()
    }

    fn a_request(url: &str) -> CreateWebhook {
        CreateWebhook {
            tenant: "acme".parse().unwrap(),
            topic: "facts.build".parse().unwrap(),
            url: url.to_owned(),
            start: StartPosition::First,
        }
    }

    fn created(webhook: &Webhook) -> Vec<u8> {
        Entry::Created {
            webhook: webhook.clone(),
            secret: WebhookSecret::random(),
        }
        .encode()
        .unwrap()
    }

    fn is_unavailable<T: std::fmt::Debug>(result: Result<T, ApiError>) -> bool {
        matches!(result, Err(ApiError::Unavailable(_)))
    }

    /// Publish `n` envelopes to `facts.build` of `acme`.
    async fn publish(rig: &Rig, n: u32) {
        let tenant: TenantId = "acme".parse().unwrap();
        let topic: Topic = "facts.build".parse().unwrap();
        rig.broker.ensure_tenant(&tenant).await.unwrap();
        rig.broker.ensure_topic(&tenant, &topic).await.unwrap();
        for _ in 0..n {
            let envelope = Envelope::new(
                mansio_core::EventId::generate(),
                "test".parse().unwrap(),
                "t".parse().unwrap(),
                tenant.clone(),
                topic.clone(),
                admin(),
            );
            rig.broker.publish(&Caller::Admin, &envelope).await.unwrap();
        }
    }

    async fn position(rig: &Rig, id: WebhookId) -> Option<u64> {
        rig.webhooks
            .delivery
            .cursors
            .get(&Cursor::Webhook(id))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_webhook_starts_at_the_first_message_or_after_the_last_one() {
        let _serial = crate::testing::serial().await;
        let rig = rig(Duration::from_secs(5), Duration::from_secs(8));
        publish(&rig, 3).await;
        let url = "http://127.0.0.1:9/hook";
        let first = rig.webhooks.create(&admin(), a_request(url)).await.unwrap();
        let next = rig
            .webhooks
            .create(
                &admin(),
                CreateWebhook {
                    start: StartPosition::Next,
                    ..a_request(url)
                },
            )
            .await
            .unwrap();
        // A topic nobody has published to: the next message is the first.
        let empty = rig
            .webhooks
            .create(
                &admin(),
                CreateWebhook {
                    topic: "facts.deploy".parse().unwrap(),
                    start: StartPosition::Next,
                    ..a_request(url)
                },
            )
            .await
            .unwrap();
        // The position is the last message handled: the task starts at the one
        // after it.
        assert_eq!(position(&rig, first.webhook.webhook.id).await, None);
        assert_eq!(position(&rig, next.webhook.webhook.id).await, Some(2));
        assert_eq!(position(&rig, empty.webhook.webhook.id).await, None);
        rig.webhooks.shutdown().await;
    }

    #[tokio::test]
    async fn a_rotated_secret_signs_beside_the_old_for_the_grace_and_alone_after() {
        use mansio_core::webhook::verify;

        let _serial = crate::testing::serial().await;
        let rig = rig(Duration::from_secs(5), Duration::from_secs(8));
        let created = rig
            .webhooks
            .create(&admin(), a_request("https://example.com/hook"))
            .await
            .unwrap();
        let id = created.webhook.webhook.id;
        let old = created.secret;
        let rotated = rig.webhooks.rotate(&admin(), id).await.unwrap();
        assert!(
            rotated.secret.expose() != old.expose(),
            "the rotated secret is the same as the old one"
        );
        assert!(rotated.secret.expose().starts_with("whsec_"));

        let now = rig.clock.now_millis();
        let until = rotated.previous_secret_valid_until.unix_millis().unwrap();
        assert_eq!(until, now + 60_000, "the grace is the setting");
        let body = b"{}";
        let signing = rig.webhooks.delivery.signing().get(&id).cloned().unwrap();

        let inside = signing.header(now, body);
        assert_eq!(inside.matches("v1=").count(), 2, "{inside}");
        for secret in [&old, &rotated.secret] {
            assert!(verify(secret, &inside, body, now / 1000, 300));
        }
        let after = signing.header(until, body);
        assert_eq!(after.matches("v1=").count(), 1, "{after}");
        assert!(verify(&rotated.secret, &after, body, until / 1000, 300));
        assert!(!verify(&old, &after, body, until / 1000, 300));

        // A server that starts again reads the rotation from the log.
        rig.webhooks.rebuild().await.unwrap();
        let rebuilt = rig.webhooks.registry().get(&id).unwrap().signing.clone();
        assert_eq!(rebuilt.header(now, body), inside);
        assert_eq!(rebuilt.header(until, body), after);

        // A second rotation moves the first secret out of the grace.
        let second = rig.webhooks.rotate(&admin(), id).await.unwrap();
        let signing = rig.webhooks.delivery.signing().get(&id).cloned().unwrap();
        let header = signing.header(now, body);
        assert!(verify(&second.secret, &header, body, now / 1000, 300));
        assert!(verify(&rotated.secret, &header, body, now / 1000, 300));
        assert!(!verify(&old, &header, body, now / 1000, 300));

        assert!(matches!(
            rig.webhooks.rotate(&admin(), WebhookId::generate()).await,
            Err(ApiError::NotFound(_))
        ));
        rig.webhooks.shutdown().await;
    }

    #[tokio::test]
    async fn the_facts_of_a_webhook_name_no_secret_and_no_secret_part_of_the_address() {
        let _serial = crate::testing::serial().await;
        let rig = rig(Duration::from_secs(5), Duration::from_secs(8));
        let url = "https://user:hunter2@example.com:8443/hooks/path-secret?token=query-secret#fragment-secret";
        let created = rig
            .webhooks
            .create(
                &admin(),
                CreateWebhook {
                    start: StartPosition::Next,
                    ..a_request(url)
                },
            )
            .await
            .unwrap();
        let id = created.webhook.webhook.id;
        let rotated = rig.webhooks.rotate(&admin(), id).await.unwrap();
        rig.webhooks.delete(&admin(), id).await.unwrap();

        let facts = rig
            .broker
            .poll(
                &Caller::Admin,
                &"acme".parse().unwrap(),
                &Topic::facts_webhook(),
                0,
                10,
            )
            .await
            .unwrap();
        let values: Vec<serde_json::Value> = facts
            .iter()
            .map(|stored| serde_json::to_value(&stored.envelope).unwrap())
            .collect();
        let types: Vec<&str> = values
            .iter()
            .map(|value| value["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            [
                "mansio.webhook.created",
                "mansio.webhook.rotated",
                "mansio.webhook.deleted"
            ]
        );
        for value in &values {
            assert_eq!(value["subject"], id.to_string());
            assert_eq!(value["principalkind"], "service");
            assert_eq!(value["principalname"], "admin");
            assert_eq!(value["data"]["webhook_id"], id.to_string());
            assert_eq!(value["data"]["origin"], "https://example.com:8443");
        }
        assert_eq!(values[0]["data"]["start"], "next");
        assert!(values[1]["data"]["previous_secret_valid_until"].is_string());

        let everything = serde_json::to_string(&values).unwrap();
        let searched = [
            created.secret.expose(),
            rotated.secret.expose(),
            "whsec_",
            "hunter2",
            "path-secret",
            "query-secret",
            "fragment-secret",
        ];
        for (n, secret) in searched.into_iter().enumerate() {
            assert!(
                !everything.contains(secret),
                "searched item {n} is in a webhook fact"
            );
        }
        rig.webhooks.shutdown().await;
    }

    /// Answer one request with a 200 and hand back what its head said.
    fn answer_once(listener: tokio::net::TcpListener) -> JoinHandle<String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut seen = Vec::new();
            let mut chunk = [0_u8; 1024];
            let head = loop {
                let n = socket.read(&mut chunk).await.unwrap();
                seen.extend_from_slice(&chunk[..n]);
                if let Some(end) = seen.windows(4).position(|w| w == b"\r\n\r\n") {
                    break String::from_utf8_lossy(&seen[..end]).into_owned();
                }
                assert!(n > 0, "the request ended inside its head");
            };
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
            head
        })
    }

    #[tokio::test]
    async fn a_delivery_to_a_loopback_link_local_or_private_address_is_refused() {
        let _serial = crate::testing::serial().await;
        // The address as written, and a name that resolves to the loopback
        // when the delivery is made. The webhook itself is accepted: the
        // address is judged at delivery. Nothing listens on the port, so a
        // delivery that was tried and not refused would say "could not
        // connect".
        for url in [
            "http://127.0.0.1:9/hook",
            "http://localhost:9/hook",
            "http://[::1]:9/hook",
            "http://169.254.169.254/latest/meta-data",
            "http://10.1.2.3/hook",
            "http://192.168.0.7:8080/hook",
            "http://[fd00::1]/hook",
        ] {
            let rig = rig_with(Duration::from_secs(5), Duration::from_secs(8), false);
            let created = rig.webhooks.create(&admin(), a_request(url)).await.unwrap();
            let refused = rig
                .webhooks
                .delivery
                .post(&created.webhook.webhook, b"{}")
                .await;
            assert_eq!(
                refused,
                Err(destination::REFUSED.to_owned()),
                "a destination case was not refused"
            );
            assert!(!destination::REFUSED.contains("127.0.0.1"));
            rig.webhooks.shutdown().await;
        }

        // Allowed by the setting, the same address is tried (and, with nothing
        // listening, cannot be connected to).
        let rig = rig_with(Duration::from_secs(5), Duration::from_secs(8), true);
        let created = rig
            .webhooks
            .create(&admin(), a_request("http://127.0.0.1:9/hook"))
            .await
            .unwrap();
        let tried = rig
            .webhooks
            .delivery
            .post(&created.webhook.webhook, b"{}")
            .await;
        assert_eq!(tried, Err("could not connect".to_owned()));
        rig.webhooks.shutdown().await;
    }

    /// The test binary is run again with a proxy in its environment, which a
    /// test cannot set itself (`set_var` is unsafe), and `proxy_child` runs.
    #[test]
    fn a_proxy_in_the_environment_does_not_get_a_delivery_past_the_guard() {
        let exe = std::env::current_exe().unwrap();
        let output = std::process::Command::new(exe)
            .args([
                "--exact",
                "webhooks::tests::proxy_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("MANSIO_TEST_PROXY_CHILD", "1")
            .env("HTTP_PROXY", "http://203.0.113.9:3128")
            .env("http_proxy", "http://203.0.113.9:3128")
            .env("ALL_PROXY", "http://203.0.113.9:3128")
            .env_remove("NO_PROXY")
            .env_remove("no_proxy")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Run by the test above, with a public proxy named in the environment: a
    /// name that resolves to the loopback is still refused.
    #[tokio::test]
    async fn proxy_child() {
        if std::env::var_os("MANSIO_TEST_PROXY_CHILD").is_none() {
            return;
        }
        let rig = rig_with(Duration::from_secs(5), Duration::from_secs(8), false);
        let created = rig
            .webhooks
            .create(&admin(), a_request("http://localhost:9/hook"))
            .await
            .unwrap();
        let refused = rig
            .webhooks
            .delivery
            .post(&created.webhook.webhook, b"{}")
            .await;
        assert_eq!(refused, Err(destination::REFUSED.to_owned()));
        rig.webhooks.shutdown().await;
    }

    #[tokio::test]
    async fn a_delivery_that_is_allowed_is_signed_with_both_secrets_in_the_grace() {
        let _serial = crate::testing::serial().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let rig = rig_with(Duration::from_secs(5), Duration::from_secs(8), true);
        let created = rig
            .webhooks
            .create(
                &admin(),
                a_request(&format!("http://127.0.0.1:{port}/hook")),
            )
            .await
            .unwrap();
        rig.webhooks
            .rotate(&admin(), created.webhook.webhook.id)
            .await
            .unwrap();
        let answered = answer_once(listener);
        let sent = rig
            .webhooks
            .delivery
            .post(&created.webhook.webhook, b"{}")
            .await;
        assert_eq!(sent, Ok(()));
        let head = answered.await.unwrap().to_ascii_lowercase();
        assert_eq!(head.matches("v1=").count(), 2, "{head}");
        rig.webhooks.shutdown().await;
    }

    #[tokio::test]
    async fn a_record_that_cannot_be_read_keeps_the_registry_unloaded_and_starts_nothing() {
        let (logs, _guard) = Logs::capture().await;
        let rig = rig(Duration::from_secs(2), Duration::from_secs(8));
        let webhook = a_webhook();
        rig.broker
            .system_append(SystemLog::Webhooks, &created(&webhook))
            .await
            .unwrap();
        // A deletion by a newer version of the format. Skipped, it would leave
        // the webhook it deleted delivering to an address the admin took away.
        let deletion = format!(
            r#"{{"op":"deleted","v":2,"id":"{}","at":"2026-10-26T13:00:00Z"}}"#,
            webhook.id
        );
        rig.broker
            .system_append(SystemLog::Webhooks, deletion.as_bytes())
            .await
            .unwrap();

        assert!(is_unavailable(rig.webhooks.load().await));
        assert!(is_unavailable(rig.webhooks.list(None).await));
        assert!(is_unavailable(
            rig.webhooks.delete(&admin(), webhook.id).await
        ));
        assert!(is_unavailable(
            rig.webhooks.rotate(&admin(), webhook.id).await
        ));
        assert!(is_unavailable(
            rig.webhooks
                .create(&admin(), a_request("https://example.com/hook"))
                .await
        ));
        assert!(rig.webhooks.workers().is_empty(), "nothing was started");
        assert!(rig.webhooks.registry().is_empty());

        // One line, at error, with where and why.
        let logged = logs.text();
        assert_eq!(logged.matches("could not be loaded").count(), 1, "{logged}");
        for expected in ["ERROR", "offset 1", "version 2", "retry_seconds=2"] {
            assert!(logged.contains(expected), "{expected} is not in {logged}");
        }
    }

    #[tokio::test]
    async fn a_load_failure_is_logged_again_when_its_cause_changes_and_not_when_it_repeats() {
        let (logs, _guard) = Logs::capture().await;
        let rig = rig(Duration::from_secs(2), Duration::from_secs(2));
        let attempt = || async {
            rig.clock.advance(Duration::from_secs(2));
            assert!(is_unavailable(rig.webhooks.load().await));
        };
        let said = || logs.text().matches("could not be loaded").count();

        // The broker fails twice: logged once.
        rig.broker
            .webhook_log_reads_to_fail
            .store(2, Ordering::SeqCst);
        attempt().await;
        attempt().await;
        assert_eq!(said(), 1, "{}", logs.text());

        // Then the broker answers, and the log holds a record that cannot be
        // read: another cause, logged again, and once however often it repeats.
        rig.broker
            .system_append(SystemLog::Webhooks, b"not a record")
            .await
            .unwrap();
        attempt().await;
        assert_eq!(said(), 2, "{}", logs.text());
        attempt().await;
        attempt().await;
        assert_eq!(said(), 2, "{}", logs.text());
        assert!(logs.text().contains("offset 0"));
    }

    #[tokio::test]
    async fn a_record_without_a_version_is_version_one_and_is_read() {
        let _serial = crate::testing::serial().await;
        let rig = rig(Duration::from_secs(2), Duration::from_secs(8));
        let webhook = a_webhook();
        let mut value: serde_json::Value = serde_json::from_slice(&created(&webhook)).unwrap();
        value.as_object_mut().unwrap().remove("v").unwrap();
        rig.broker
            .system_append(SystemLog::Webhooks, value.to_string().as_bytes())
            .await
            .unwrap();
        rig.webhooks.load().await.unwrap();
        let listed = rig.webhooks.list(None).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].webhook.id, webhook.id);
        assert_eq!(rig.webhooks.workers().len(), 1, "its task is started");
        rig.webhooks.shutdown().await;
    }

    #[tokio::test]
    async fn a_load_that_failed_is_tried_again_after_a_wait_that_grows() {
        let (logs, _guard) = Logs::capture().await;
        let rig = rig(Duration::from_secs(2), Duration::from_secs(8));
        let webhook = a_webhook();
        rig.broker
            .system_append(SystemLog::Webhooks, &created(&webhook))
            .await
            .unwrap();
        rig.broker
            .webhook_log_reads_to_fail
            .store(2, Ordering::SeqCst);
        let reads = || rig.broker.webhook_log_reads.load(Ordering::SeqCst);

        // The first attempt reads and fails; until 2 s have passed the rest
        // are refused without a read.
        assert!(is_unavailable(rig.webhooks.load().await));
        assert_eq!(reads(), 1);
        for _ in 0..3 {
            assert!(is_unavailable(rig.webhooks.load().await));
        }
        assert_eq!(reads(), 1, "not read again inside the wait");
        assert!(rig.webhooks.workers().is_empty());

        // The second attempt fails too, and the wait is longer: 4 s.
        rig.clock.advance(Duration::from_secs(2));
        assert!(is_unavailable(rig.webhooks.load().await));
        assert_eq!(reads(), 2);
        rig.clock.advance(Duration::from_secs(3));
        assert!(is_unavailable(rig.webhooks.load().await));
        assert_eq!(reads(), 2, "still inside the 4 s");

        // The third reads, loads, and starts the task.
        rig.clock.advance(Duration::from_secs(1));
        rig.webhooks.load().await.unwrap();
        assert_eq!(reads(), 4, "the third attempt reads the record and the end");
        assert_eq!(rig.webhooks.workers().len(), 1);
        rig.webhooks.shutdown().await;

        // One error for the run of failures, and one line when it ended.
        let logged = logs.text();
        assert_eq!(logged.matches("could not be loaded").count(), 1, "{logged}");
        assert_eq!(
            logged.matches("registry of webhooks is loaded").count(),
            1,
            "{logged}"
        );
    }

    #[tokio::test]
    async fn a_broker_that_is_down_costs_a_webhook_a_line_when_it_fails_and_a_line_when_it_recovers()
     {
        let (logs, _guard) = Logs::capture().await;
        let rig = rig(Duration::from_millis(20), Duration::from_millis(40));
        let created = rig
            .webhooks
            .create(&admin(), a_request("http://127.0.0.1:9/hook"))
            .await
            .unwrap();
        let lines = |what: &str| logs.text().matches(what).count();
        let pause = |millis| tokio::time::sleep(Duration::from_millis(millis));

        // The positions cannot be read: the task fails for twenty or more
        // waits and says so once.
        rig.broker.cursor_log_down.store(true, Ordering::SeqCst);
        pause(600).await;
        assert_eq!(lines("a webhook is failing"), 1, "{}", logs.text());
        assert!(logs.text().contains("reading where it stopped"));
        assert_eq!(lines("has recovered"), 0);

        // They can, and the topic cannot be read: it recovers from the first
        // and fails at the second, once each.
        rig.broker.topics_down.store(true, Ordering::SeqCst);
        rig.broker.cursor_log_down.store(false, Ordering::SeqCst);
        pause(600).await;
        assert_eq!(lines("a webhook is failing"), 2, "{}", logs.text());
        assert!(logs.text().contains("reading its topic"));
        assert_eq!(lines("has recovered"), 1);

        // Both are back.
        rig.broker.topics_down.store(false, Ordering::SeqCst);
        pause(300).await;
        assert_eq!(lines("a webhook is failing"), 2);
        assert_eq!(lines("has recovered"), 2, "{}", logs.text());
        assert!(
            logs.text()
                .contains(&created.webhook.webhook.id.to_string())
        );
        rig.webhooks.shutdown().await;
    }
}
