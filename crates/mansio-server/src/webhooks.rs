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
//! Delivery is at least once: a server that stops between the receiver's
//! answer and the commit of the offset sends that envelope again. Its `id` is
//! the same, so a receiver deduplicates on it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, PoisonError};
use std::time::Duration;

use mansio_core::api::{CreateWebhook, WebhookCreated};
use mansio_core::webhook::{CONTENT_TYPE, SIGNATURE_HEADER, sign};
use mansio_core::{
    Broker, BrokerError, Caller, Stored, SystemLog, TenantId, Timestamp, Topic, Webhook,
    WebhookError, WebhookEvent, WebhookFact, WebhookId, WebhookInfo, WebhookSecret, WebhookStatus,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, OnceCell};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::clock::Clock;
use crate::cursors::{Cursor, Cursors};
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

/// A registered webhook and its secret.
#[derive(Debug, Clone)]
struct Record {
    webhook: Webhook,
    secret: WebhookSecret,
}

/// What a failed request says, without the address it was sent to: an address
/// can carry a secret of the receiver's own.
fn describe(error: &reqwest::Error, timeout: Duration) -> String {
    if error.is_timeout() {
        format!("no answer within {}s", timeout.as_secs())
    } else if error.is_connect() {
        "could not connect".to_owned()
    } else {
        "the request failed".to_owned()
    }
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
}

impl Delivery {
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
    async fn follow(self: Arc<Self>, webhook: Webhook, secret: WebhookSecret) {
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
                        self.deliver(&webhook, &secret, &stored).await;
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
    async fn deliver(&self, webhook: &Webhook, secret: &WebhookSecret, stored: &Stored) {
        let outcome = match stored.envelope.to_json_vec() {
            Ok(body) => self.attempts(webhook, secret, stored, &body).await,
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
        secret: &WebhookSecret,
        stored: &Stored,
        body: &[u8],
    ) -> Result<(), (u32, String)> {
        let mut attempt = 0_u32;
        loop {
            attempt += 1;
            let Err(message) = self.post(webhook, secret, body).await else {
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
    async fn post(
        &self,
        webhook: &Webhook,
        secret: &WebhookSecret,
        body: &[u8],
    ) -> Result<(), String> {
        let Some(http) = &self.http else {
            return Err("the HTTP client could not be made".to_owned());
        };
        let signature = sign(secret, self.clock.now_millis().div_euclid(1000), body);
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
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(settings.timeout)
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
            }),
            page,
            registry: StdMutex::default(),
            loaded: OnceCell::new(),
            loading: AsyncMutex::default(),
            workers: StdMutex::default(),
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
                        records.insert(webhook.id, Record { webhook, secret });
                    }
                    Entry::Deleted { id, .. } => {
                        records.remove(&id);
                    }
                }
            }
            if next <= offset {
                break;
            }
            offset = next;
        }
        info!(webhooks = records.len(), "webhooks loaded");
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
        for (id, record) in registry.iter() {
            if workers.get(id).is_some_and(|task| !task.is_finished()) {
                continue;
            }
            let task = tokio::spawn(
                Arc::clone(&self.delivery).follow(record.webhook.clone(), record.secret.clone()),
            );
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

    /// Make a webhook: `webhooks_create`. The secret it returns is shown this
    /// once.
    ///
    /// # Errors
    ///
    /// Returns 400 for an address that cannot be used or a topic that must not
    /// be delivered, and 502 or 503 if the registry cannot be written. Nothing
    /// is made then.
    pub(crate) async fn create(&self, request: CreateWebhook) -> Result<WebhookCreated, ApiError> {
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
                secret: secret.clone(),
            },
        );
        self.sync();
        info!(
            webhook = %webhook.id,
            tenant = %webhook.tenant,
            topic = %webhook.topic,
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
    /// being delivered may or may not have arrived.
    ///
    /// # Errors
    ///
    /// Returns 404 if there is no such webhook, and 502 or 503 if the
    /// registry cannot be written, in which case the webhook stays.
    pub(crate) async fn delete(&self, id: WebhookId) -> Result<Webhook, ApiError> {
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
        self.broker
            .system_append(SystemLog::Webhooks, &entry.encode()?)
            .await?;
        self.registry().remove(&id);
        self.sync();
        info!(webhook = %id, "webhook deleted");
        Ok(found)
    }
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
        assert!(is_unavailable(rig.webhooks.delete(webhook.id).await));
        let request = CreateWebhook {
            tenant: "acme".parse().unwrap(),
            topic: "facts.build".parse().unwrap(),
            url: "https://example.com/hook".to_owned(),
        };
        assert!(is_unavailable(rig.webhooks.create(request).await));
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
            .create(CreateWebhook {
                tenant: "acme".parse().unwrap(),
                topic: "facts.build".parse().unwrap(),
                url: "http://127.0.0.1:9/hook".to_owned(),
            })
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
