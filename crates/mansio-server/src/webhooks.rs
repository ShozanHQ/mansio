//! Webhooks: software that will not hold a connection is sent each envelope
//! of a topic as an HTTP POST.
//!
//! A webhook is registered by the admin API and kept in a log in the broker
//! that no credential can reach ([`mansio_core::SystemLog::Webhooks`]), which
//! is also the only place its signing secret is kept. Each webhook has a task
//! that follows its topic as a durable consumer of the broker, so a restarted
//! server resumes where the last one stopped, and delivers in offset order,
//! one envelope at a time. A delivery that the receiver does not accept with
//! a 2xx, or does not answer in time, is tried again after a doubling
//! backoff. Past `MANSIO_WEBHOOK_MAX_ATTEMPTS` it is recorded as
//! `mansio.webhook.failed` on `facts.webhook` and the webhook moves on.
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
    Broker, BrokerError, Caller, ConsumerName, Stored, SystemLog, TenantId, Timestamp, Topic,
    Webhook, WebhookError, WebhookEvent, WebhookFact, WebhookId, WebhookInfo, WebhookSecret,
    WebhookStatus,
};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::clock::Clock;
use crate::engine::Engine;
use crate::error::ApiError;
use crate::facts::Facts;
use crate::settings::{StreamSettings, WebhookSettings};
use crate::state::backoff;
use crate::tail::Tail;

/// The longest address a webhook may have.
const URL_MAX: usize = 2048;

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
        serde_json::to_vec(self).map_err(|reason| {
            tracing::error!(%reason, "a webhook record did not serialize");
            ApiError::Internal
        })
    }
}

/// A registered webhook and its secret.
#[derive(Debug, Clone)]
struct Record {
    webhook: Webhook,
    secret: WebhookSecret,
}

/// The consumer a webhook follows its topic as.
fn consumer_of(id: WebhookId) -> Result<ConsumerName, ApiError> {
    format!("webhook-{id}").parse().map_err(|reason| {
        tracing::error!(%reason, "a webhook consumer name did not parse");
        ApiError::Internal
    })
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

/// Check an address a webhook may be sent to.
fn check_url(url: &str) -> Result<(), ApiError> {
    let bad = |why: String| ApiError::BadRequest(format!("url {why}"));
    if url.len() > URL_MAX {
        return Err(bad(format!("is longer than {URL_MAX} bytes")));
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
    /// the broker says the webhook's consumer stopped, and deliver what comes
    /// in order.
    async fn follow(self: Arc<Self>, webhook: Webhook, secret: WebhookSecret) {
        let Ok(consumer) = consumer_of(webhook.id) else {
            return;
        };
        let poll = self.stream.poll;
        let start = loop {
            match self
                .broker
                .consumer_offset(&webhook.tenant, &webhook.topic, &consumer)
                .await
            {
                Ok(Some(committed)) => {
                    self.delivered(webhook.id, committed);
                    break committed.saturating_add(1);
                }
                // Nothing committed, or no topic yet: from the first message.
                Ok(None) | Err(BrokerError::NotFound(_)) => break 0,
                Err(error) => {
                    warn!(webhook = %webhook.id, %error, "where the webhook stopped could not be read");
                    tokio::time::sleep(poll).await;
                }
            }
        };
        let mut tail = Tail::new(
            Arc::clone(&self.broker),
            webhook.tenant.clone(),
            webhook.topic.clone(),
            start,
            self.page,
        );
        loop {
            match tail.read(&Caller::Admin).await {
                Ok(batch) if batch.is_empty() => tokio::time::sleep(poll).await,
                Ok(batch) => {
                    for stored in batch {
                        self.deliver(&webhook, &secret, &consumer, &stored).await;
                    }
                }
                // The topic is made by the first publish to it, which may
                // come after the webhook.
                Err(BrokerError::NotFound(_)) => tokio::time::sleep(poll).await,
                Err(error) => {
                    warn!(webhook = %webhook.id, %error, "the webhook's topic could not be read");
                    tokio::time::sleep(poll).await;
                }
            }
        }
    }

    /// Deliver one envelope: try until the receiver accepts it or the
    /// attempts are used, then move past it either way.
    async fn deliver(
        &self,
        webhook: &Webhook,
        secret: &WebhookSecret,
        consumer: &ConsumerName,
        stored: &Stored,
    ) {
        let outcome = match stored.envelope.to_json_vec() {
            Ok(body) => self.attempts(webhook, secret, stored, &body).await,
            Err(error) => Err((1, format!("the envelope cannot be written: {error}"))),
        };
        if let Err((attempts, error)) = &outcome {
            self.record_failure(webhook, stored, *attempts, error).await;
        }
        let committed = self
            .broker
            .commit_offset(&webhook.tenant, &webhook.topic, consumer, stored.offset)
            .await;
        if let Err(error) = committed {
            // The next commit covers this one; a stop before it sends this
            // envelope again.
            warn!(webhook = %webhook.id, offset = stored.offset, %error, "the webhook's offset could not be stored");
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
                Ok(()) => return,
                Err(failure) => {
                    warn!(webhook = %webhook.id, ?failure, "the failed delivery could not be recorded; trying again");
                    tokio::time::sleep(self.stream.poll).await;
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
            .inspect_err(|reason| tracing::error!(%reason, "the webhook HTTP client could not be made"))
            .ok();
        let page = page.max(1);
        Self {
            broker: Arc::clone(&broker),
            clock: Arc::clone(&clock),
            delivery: Arc::new(Delivery {
                broker,
                engine,
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
    /// # Errors
    ///
    /// Returns an error if the log cannot be read. The next call tries again.
    pub async fn load(&self) -> Result<(), ApiError> {
        self.loaded.get_or_try_init(|| self.rebuild()).await?;
        self.sync();
        Ok(())
    }

    async fn rebuild(&self) -> Result<(), ApiError> {
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
                match serde_json::from_slice::<Entry>(&record.payload) {
                    Ok(Entry::Created { webhook, secret }) => {
                        records.insert(webhook.id, Record { webhook, secret });
                    }
                    Ok(Entry::Deleted { id, .. }) => {
                        records.remove(&id);
                    }
                    Err(reason) => warn!(
                        offset = record.offset,
                        %reason,
                        "a webhook record could not be read and was left out"
                    ),
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
        loop {
            match self.load().await {
                Ok(()) => break,
                Err(error) => {
                    warn!(?error, "the webhooks could not be loaded; trying again");
                    tokio::time::sleep(poll).await;
                }
            }
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
            let task = tokio::spawn(Arc::clone(&self.delivery).follow(
                record.webhook.clone(),
                record.secret.clone(),
            ));
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
        check_url(&request.url)?;
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
    use super::*;

    #[test]
    fn only_http_and_https_addresses_with_a_host_are_accepted() {
        for ok in [
            "http://127.0.0.1:9000/hook",
            "https://example.com/a/b?c=d",
            "http://localhost/",
        ] {
            assert!(check_url(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "example.com/hook",
            "ftp://example.com/",
            "file:///etc/passwd",
            "http://",
            "javascript:alert(1)",
            &format!("https://example.com/{}", "a".repeat(URL_MAX)),
        ] {
            assert!(
                matches!(check_url(bad), Err(ApiError::BadRequest(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_record_of_the_log_round_trips_and_a_debug_shows_no_secret() {
        let webhook = Webhook {
            id: WebhookId::generate(),
            tenant: "acme".parse().unwrap(),
            topic: "facts.build".parse().unwrap(),
            url: "https://example.com/hook".to_owned(),
            created_at: "2026-10-26T12:00:00Z".parse().unwrap(),
        };
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
            let back = serde_json::from_slice::<Entry>(&bytes).unwrap();
            assert_eq!(back.encode().unwrap(), bytes);
            assert!(!format!("{entry:?}").contains(secret.expose()));
        }
        let created = Entry::Created { webhook, secret };
        let text = String::from_utf8(created.encode().unwrap()).unwrap();
        assert!(text.contains(r#""op":"created""#), "{text}");
    }

    #[test]
    fn a_webhooks_consumer_is_named_after_it() {
        let id = WebhookId::generate();
        assert_eq!(
            consumer_of(id).unwrap().as_str(),
            format!("webhook-{id}")
        );
    }
}
