//! A server to test against, and the means to talk to it.
//!
//! With `MANSIO_E2E_URL` unset, each test starts a server in this process on
//! a `MemoryBroker`, with a file sink of its own and short work settings.
//! With it set, the tests use that server (CI runs them against
//! `docker compose` and Iggy), and `MANSIO_TOKEN` must be the token it
//! accepts. The work settings are then whatever `GET /v1/settings` says. The
//! sink can be read only if `MANSIO_E2E_SINK_PATH` names the file the server
//! writes it to.
//!
//! Every test uses a tenant of its own, so tests do not see each other's
//! messages on a shared server.
//!
//! The tests of credentials also talk to the broker without going through the
//! server. In process that is the `MemoryBroker`. Against a live server it is
//! Iggy, at `MANSIO_E2E_IGGY_ADDRESS`, which is then required: without it
//! those tests fail.

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail, ensure};
use iggy::prelude::{
    Client, IggyClientBuilder, IggyError, IggyMessage, MessageClient, Partitioning,
    PersonalAccessTokenClient,
};
use mansio_core::api::{Claimed, EffectiveSettings, Polled, Published};
use mansio_core::{
    Broker, BrokerError, Caller, Credential, Envelope, EventId, MemoryBroker, Timestamp,
};
use mansio_server::{
    Auth, CredentialSettings, FileSink, Limits, Server, StreamSettings, WebhookSettings,
    WorkSettings,
};
use reqwest::{Method, StatusCode};
use secrecy::SecretString;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The token the in-process server accepts.
const LOCAL_TOKEN: &str = "e2e-local-token";
/// The principal the server's token stands for, by default.
pub const PRINCIPAL: &str = "service:mansio";
/// How often a test looks again while it waits for something.
const LOOK_AGAIN: Duration = Duration::from_millis(50);
/// How long a server may take to shut down before it is aborted.
const STOP_TIME: Duration = Duration::from_secs(5);
/// What a wait may take beyond what the settings call for, for a slow runner.
pub const SLACK: Duration = Duration::from_secs(15);

/// The work settings of the in-process server: seconds-scale, so that a test
/// of a lease or a backoff takes seconds.
fn local_work_settings() -> WorkSettings {
    WorkSettings {
        lease: Duration::from_secs(2),
        lease_max: Duration::from_secs(60),
        max_attempts: 3,
        backoff_base: Duration::from_secs(1),
        backoff_cap: Duration::from_secs(4),
        alert_resurface: Duration::from_secs(2),
        tick: Duration::from_millis(100),
    }
}

/// The credential settings of the in-process server: seconds-scale, so that
/// an expiry, a recheck and a sweep take seconds.
fn local_credential_settings() -> CredentialSettings {
    CredentialSettings {
        agent_ttl: Duration::from_secs(3),
        recheck: Duration::from_secs(1),
        sweep: Duration::from_secs(1),
    }
}

/// The stream settings of the in-process server: a quick look for new
/// messages, and a comment every second on a quiet event stream.
fn local_stream_settings() -> StreamSettings {
    StreamSettings {
        poll: Duration::from_millis(50),
        sse_keepalive: Duration::from_secs(1),
    }
}

/// The webhook settings of the in-process server: seconds-scale, so that a
/// retry, a timeout and giving up take seconds.
fn local_webhook_settings() -> WebhookSettings {
    WebhookSettings {
        backoff_base: Duration::from_secs(1),
        backoff_cap: Duration::from_secs(4),
        max_attempts: 3,
        timeout: Duration::from_secs(2),
    }
}

/// What the in-process server runs with: the API limits, the work settings,
/// the credential settings, and how streams and webhooks behave.
#[derive(Debug, Clone, Copy)]
pub struct LocalSettings {
    pub limits: Limits,
    pub work: WorkSettings,
    pub credentials: CredentialSettings,
    pub streams: StreamSettings,
    pub webhooks: WebhookSettings,
}

impl Default for LocalSettings {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            work: local_work_settings(),
            credentials: local_credential_settings(),
            streams: local_stream_settings(),
            webhooks: local_webhook_settings(),
        }
    }
}

/// A server running in this process. It stops when dropped.
struct LocalServer {
    base: String,
    /// Tells the server to shut down; it also does when this is dropped.
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<io::Result<()>>,
}

impl LocalServer {
    async fn start(
        broker: &Arc<MemoryBroker>,
        sink: &Path,
        settings: LocalSettings,
    ) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let auth = Auth::new(
            SecretString::from(LOCAL_TOKEN.to_owned()),
            PRINCIPAL.parse()?,
        );
        let server = Server::new(broker.clone(), auth)
            .with_sink(Arc::new(FileSink::open(sink).await?))
            .with_limits(settings.limits)
            .with_work_settings(settings.work)?
            .with_credential_settings(settings.credentials)
            .with_stream_settings(settings.streams)?
            .with_webhook_settings(settings.webhooks)?;
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(server.serve(listener, async move {
            let _ = stopped.await;
        }));
        Ok(Self {
            base,
            stop: Some(stop),
            task,
        })
    }

    /// Shut the server down and wait until it has gone, its timer too, so
    /// that nothing of it is still running when a new one starts. A server
    /// that does not stop in time is aborted.
    async fn stop(mut self) -> anyhow::Result<()> {
        drop(self.stop.take());
        if tokio::time::timeout(STOP_TIME, &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
            let _ = (&mut self.task).await;
        }
        Ok(())
    }
}

impl Drop for LocalServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A server to test against.
pub struct Target {
    pub base: String,
    pub token: String,
    pub http: reqwest::Client,
    pub tenant: String,
    pub settings: EffectiveSettings,
    sink: Option<PathBuf>,
    /// The in-process broker and server, if there is one, and what the server
    /// runs with.
    local: Option<(Arc<MemoryBroker>, Option<LocalServer>, LocalSettings)>,
}

impl Drop for Target {
    fn drop(&mut self) {
        if let (Some(path), Some(_)) = (&self.sink, &self.local) {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Target {
    /// A server to test against, and a tenant of its own on it: the live
    /// server if `MANSIO_E2E_URL` is set, else one in this process.
    pub async fn start() -> anyhow::Result<Self> {
        Self::open(None, LocalSettings::default(), false).await
    }

    /// As [`Target::start`], with `settings` for the server in this process.
    /// A live server runs with what it was started with.
    pub async fn start_with(settings: LocalSettings) -> anyhow::Result<Self> {
        Self::open(None, settings, false).await
    }

    /// The live server, which must be there: a test that needs one fails,
    /// rather than passing without testing anything, when `MANSIO_E2E_URL` is
    /// not set. With `tenant` it uses that one, as a second pass does after
    /// a first on the same server; else a new one.
    pub async fn live(tenant: Option<&str>) -> anyhow::Result<Self> {
        Self::open(tenant.map(str::to_owned), LocalSettings::default(), true).await
    }

    async fn open(
        tenant: Option<String>,
        settings: LocalSettings,
        live_only: bool,
    ) -> anyhow::Result<Self> {
        let tenant = tenant
            .unwrap_or_else(|| format!("e2e-{}", EventId::generate().to_string().replace('-', "")));
        let http = reqwest::Client::new();
        if let Some(url) = std::env::var("MANSIO_E2E_URL")
            .ok()
            .filter(|url| !url.is_empty())
        {
            let token = std::env::var("MANSIO_TOKEN")
                .context("MANSIO_TOKEN must be set when MANSIO_E2E_URL is")?;
            let base = url.trim_end_matches('/').to_owned();
            let effective = read_settings(&http, &base, &token).await?;
            let sink = std::env::var_os("MANSIO_E2E_SINK_PATH")
                .filter(|path| !path.is_empty())
                .map(PathBuf::from);
            return Ok(Self {
                base,
                token,
                http,
                tenant,
                settings: effective,
                sink,
                local: None,
            });
        }
        ensure!(
            !live_only,
            "MANSIO_E2E_URL is not set: this test runs against a live server and Iggy"
        );
        let sink = std::env::temp_dir().join(format!("mansio-{tenant}.jsonl"));
        let broker = Arc::new(MemoryBroker::new());
        let server = LocalServer::start(&broker, &sink, settings).await?;
        let effective = read_settings(&http, &server.base, LOCAL_TOKEN).await?;
        Ok(Self {
            base: server.base.clone(),
            token: LOCAL_TOKEN.to_owned(),
            http,
            tenant,
            settings: effective,
            sink: Some(sink),
            local: Some((broker, Some(server), settings)),
        })
    }

    /// Whether the server runs in this process, and so can be stopped and
    /// started again.
    pub fn is_local(&self) -> bool {
        self.local.is_some()
    }

    /// Stop the in-process server, and wait until it and its timer are gone.
    /// The broker and the sink file stay.
    pub async fn stop_server(&mut self) -> anyhow::Result<()> {
        let (_, server, _) = self.local.as_mut().context("the server is not local")?;
        server
            .take()
            .context("the server is already stopped")?
            .stop()
            .await
    }

    /// Start a new in-process server on the broker and the sink file that
    /// the last one used.
    pub async fn start_server(&mut self) -> anyhow::Result<()> {
        let sink = self.sink.clone().context("no sink file")?;
        let (broker, server, settings) = self.local.as_mut().context("the server is not local")?;
        ensure!(server.is_none(), "the server is running");
        let started = LocalServer::start(broker, &sink, *settings).await?;
        self.base = started.base.clone();
        *server = Some(started);
        Ok(())
    }

    pub fn messages_uri(&self, topic: &str) -> String {
        format!(
            "{}/v1/tenants/{}/topics/{topic}/messages",
            self.base, self.tenant
        )
    }

    pub fn tenant_uri(&self, rest: &str) -> String {
        format!("{}/v1/tenants/{}/{rest}", self.base, self.tenant)
    }

    /// Send a request, with the token unless `token` says otherwise, and
    /// return the status and the JSON body (`null` if there is none).
    pub async fn call(
        &self,
        method: Method,
        uri: &str,
        token: Option<&str>,
        body: Option<&Value>,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let mut request = self.http.request(method, uri);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        Ok((
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        ))
    }

    /// A request with the right token.
    pub async fn authed(
        &self,
        method: Method,
        uri: &str,
        body: Option<&Value>,
    ) -> anyhow::Result<(StatusCode, Value)> {
        self.call(method, uri, Some(&self.token), body).await
    }

    pub async fn publish(&self, topic: &str, body: &Value) -> anyhow::Result<Published> {
        let (status, response) = self
            .authed(Method::POST, &self.messages_uri(topic), Some(body))
            .await?;
        ensure!(
            status == StatusCode::CREATED,
            "publish answered {status}: {response}"
        );
        Ok(serde_json::from_value(response)?)
    }

    pub async fn poll(&self, topic: &str, query: &str) -> anyhow::Result<Polled> {
        let uri = format!("{}?{query}", self.messages_uri(topic));
        let (status, response) = self.authed(Method::GET, &uri, None).await?;
        ensure!(
            status == StatusCode::OK,
            "poll answered {status}: {response}"
        );
        Ok(serde_json::from_value(response)?)
    }

    /// Everything in a topic, in order. A topic nobody has published to is
    /// not found by the server, and holds nothing.
    pub async fn read(&self, topic: &str) -> anyhow::Result<Vec<Envelope>> {
        let mut all = Vec::new();
        let mut offset = 0;
        loop {
            let query = format!("offset={offset}&count={}", self.settings.poll_max_count);
            let uri = format!("{}?{query}", self.messages_uri(topic));
            let (status, response) = self.authed(Method::GET, &uri, None).await?;
            if status == StatusCode::NOT_FOUND {
                return Ok(all);
            }
            ensure!(
                status == StatusCode::OK,
                "poll answered {status}: {response}"
            );
            let page: Polled = serde_json::from_value(response)?;
            let Some(last) = page.messages.last() else {
                return Ok(all);
            };
            offset = last.offset + 1;
            all.extend(page.messages.into_iter().map(|stored| stored.envelope));
        }
    }

    /// The types of the transitions on `facts.work`, as `claimed`, `acked`
    /// and so on, in order.
    pub async fn work_transitions(&self) -> anyhow::Result<Vec<String>> {
        Ok(type_names(&self.read("facts.work").await?, "mansio.work."))
    }

    /// The same for `facts.alert`.
    pub async fn alert_transitions(&self) -> anyhow::Result<Vec<String>> {
        Ok(type_names(
            &self.read("facts.alert").await?,
            "mansio.alert.",
        ))
    }

    /// `work_publish`: the body is an envelope without `itemid` and
    /// `attempt`.
    pub async fn work_publish(&self, queue: &str, body: &Value) -> anyhow::Result<Published> {
        let uri = self.tenant_uri(&format!("work/{queue}/items"));
        let (status, response) = self.authed(Method::POST, &uri, Some(body)).await?;
        ensure!(
            status == StatusCode::CREATED,
            "work_publish answered {status}: {response}"
        );
        Ok(serde_json::from_value(response)?)
    }

    /// `work_pull`: the claim, or `None` if nothing can be claimed.
    pub async fn pull(&self, queue: &str, consumer: &str) -> anyhow::Result<Option<Claimed>> {
        let uri = self.tenant_uri(&format!("work/{queue}/pull"));
        let (status, response) = self
            .authed(Method::POST, &uri, Some(&json!({ "consumer": consumer })))
            .await?;
        match status {
            StatusCode::OK => Ok(Some(serde_json::from_value(response)?)),
            StatusCode::NO_CONTENT => Ok(None),
            other => bail!("work_pull answered {other}: {response}"),
        }
    }

    /// `work_ack`, answering whatever the server says.
    pub async fn ack(
        &self,
        queue: &str,
        item: &str,
        consumer: &str,
        lease: &str,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(&format!("work/{queue}/items/{item}/ack"));
        let body = json!({ "lease_id": lease, "consumer": consumer });
        self.authed(Method::POST, &uri, Some(&body)).await
    }

    /// `work_extend`, answering whatever the server says. Without `seconds`
    /// the lease runs for the server's lease setting from now.
    pub async fn extend(
        &self,
        queue: &str,
        item: &str,
        consumer: &str,
        lease: &str,
        seconds: Option<u64>,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(&format!("work/{queue}/items/{item}/extend"));
        let mut body = json!({ "lease_id": lease, "consumer": consumer });
        if let Some(seconds) = seconds {
            body["seconds"] = json!(seconds);
        }
        self.authed(Method::POST, &uri, Some(&body)).await
    }

    /// `work_fail`, answering whatever the server says.
    pub async fn fail(
        &self,
        queue: &str,
        item: &str,
        consumer: &str,
        lease: &str,
        reason: &str,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(&format!("work/{queue}/items/{item}/fail"));
        let body = json!({ "lease_id": lease, "consumer": consumer, "reason": reason });
        self.authed(Method::POST, &uri, Some(&body)).await
    }

    /// Ack a claim, which must be accepted.
    pub async fn ack_claim(&self, queue: &str, claim: &Claimed) -> anyhow::Result<Published> {
        let item = item_of(claim)?;
        let (status, response) = self
            .ack(
                queue,
                &item,
                claim.consumer.as_str(),
                &claim.lease_id.to_string(),
            )
            .await?;
        ensure!(
            status == StatusCode::OK,
            "work_ack answered {status}: {response}"
        );
        Ok(serde_json::from_value(response)?)
    }

    /// Extend a claim, which must be accepted: the `extended` fact.
    pub async fn extend_claim(
        &self,
        queue: &str,
        claim: &Claimed,
        seconds: Option<u64>,
    ) -> anyhow::Result<Published> {
        let item = item_of(claim)?;
        let (status, response) = self
            .extend(
                queue,
                &item,
                claim.consumer.as_str(),
                &claim.lease_id.to_string(),
                seconds,
            )
            .await?;
        ensure!(
            status == StatusCode::OK,
            "work_extend answered {status}: {response}"
        );
        Ok(serde_json::from_value(response)?)
    }

    /// Fail a claim, which must be accepted.
    pub async fn fail_claim(
        &self,
        queue: &str,
        claim: &Claimed,
        reason: &str,
    ) -> anyhow::Result<Published> {
        let item = item_of(claim)?;
        let (status, response) = self
            .fail(
                queue,
                &item,
                claim.consumer.as_str(),
                &claim.lease_id.to_string(),
                reason,
            )
            .await?;
        ensure!(
            status == StatusCode::OK,
            "work_fail answered {status}: {response}"
        );
        Ok(serde_json::from_value(response)?)
    }

    /// `alerts_publish`.
    pub async fn alert_publish(&self, body: &Value) -> anyhow::Result<Published> {
        let (status, response) = self
            .authed(Method::POST, &self.tenant_uri("alerts"), Some(body))
            .await?;
        ensure!(
            status == StatusCode::CREATED,
            "alerts_publish answered {status}: {response}"
        );
        Ok(serde_json::from_value(response)?)
    }

    /// `alerts_ack`, answering whatever the server says.
    pub async fn alert_ack(&self, alert: &str) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(&format!("alerts/{alert}/ack"));
        self.authed(Method::POST, &uri, None).await
    }

    /// What the sink recorded for this tenant, in order, or `None` if the
    /// sink cannot be read from here.
    pub fn sink_records(&self) -> anyhow::Result<Option<Vec<Envelope>>> {
        let Some(path) = &self.sink else {
            return Ok(None);
        };
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading the sink {}", path.display()))?;
        // The server may be part way through a line; the rest comes later.
        let complete = text.rfind('\n').map_or("", |end| &text[..=end]);
        let mut records = Vec::new();
        for line in complete.lines() {
            let envelope = Envelope::from_json_slice(line.as_bytes())?;
            if envelope.tenant.as_str() == self.tenant {
                records.push(envelope);
            }
        }
        Ok(Some(records))
    }

    /// Look again every moment until `probe` finds something or `timeout`
    /// passes.
    pub async fn until<T, Fut>(
        &self,
        what: &str,
        timeout: Duration,
        mut probe: impl FnMut() -> Fut,
    ) -> anyhow::Result<T>
    where
        Fut: Future<Output = anyhow::Result<Option<T>>>,
    {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(found) = probe().await? {
                return Ok(found);
            }
            ensure!(
                Instant::now() < deadline,
                "gave up after {timeout:?} waiting for {what}"
            );
            tokio::time::sleep(LOOK_AGAIN).await;
        }
    }

    /// Pull until something is claimed.
    pub async fn claim(
        &self,
        queue: &str,
        consumer: &str,
        timeout: Duration,
    ) -> anyhow::Result<Claimed> {
        self.until("a claim", timeout, || self.pull(queue, consumer))
            .await
    }

    /// The wait after attempt `attempt` fails, from the settings.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let settings = &self.settings;
        let doublings = attempt.saturating_sub(1).min(31);
        Duration::from_secs(settings.backoff_base_seconds)
            .saturating_mul(1 << doublings)
            .min(Duration::from_secs(settings.backoff_cap_seconds))
    }

    pub fn lease(&self) -> Duration {
        Duration::from_secs(self.settings.lease_seconds)
    }

    /// The longest a lease may run from its claim, from the settings.
    pub fn lease_max(&self) -> Duration {
        Duration::from_secs(self.settings.lease_max_seconds)
    }

    /// One timer tick, and a little for a round trip: how long to wait to
    /// show that something does not happen.
    pub fn settle(&self) -> Duration {
        Duration::from_millis(self.settings.tick_millis.saturating_mul(3) + 300)
    }

    // ---- consumers, webhooks ------------------------------------------------

    /// The address of a named consumer of a topic of this tenant.
    pub fn consumer_uri(&self, topic: &str, consumer: &str) -> String {
        format!(
            "{}/v1/tenants/{}/topics/{topic}/consumers/{consumer}",
            self.base, self.tenant
        )
    }

    /// `webhooks_create` for a topic of this tenant, answering whatever the
    /// server says.
    pub async fn create_webhook(
        &self,
        topic: &str,
        url: &str,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let body = json!({"tenant": self.tenant, "topic": topic, "url": url});
        self.authed(
            Method::POST,
            &format!("{}/v1/webhooks", self.base),
            Some(&body),
        )
        .await
    }

    /// `webhooks_create`, which must be accepted: the answer with the
    /// webhook and its secret.
    pub async fn webhook(&self, topic: &str, url: &str) -> anyhow::Result<Value> {
        let (status, response) = self.create_webhook(topic, url).await?;
        ensure!(
            status == StatusCode::CREATED,
            "webhooks_create answered {status}: {response}"
        );
        Ok(response)
    }

    /// `webhooks_list` for this tenant.
    pub async fn webhooks(&self) -> anyhow::Result<Vec<Value>> {
        let uri = format!("{}/v1/webhooks?tenant={}", self.base, self.tenant);
        let (status, response) = self.authed(Method::GET, &uri, None).await?;
        ensure!(
            status == StatusCode::OK,
            "webhooks_list answered {status}: {response}"
        );
        Ok(response["webhooks"].as_array().context("a list")?.clone())
    }

    /// Delete every webhook of this tenant, so that a server that goes on after
    /// the test does not go on delivering for it.
    pub async fn delete_webhooks(&self) -> anyhow::Result<()> {
        for webhook in self.webhooks().await? {
            let id = webhook["id"].as_str().context("a webhook has an id")?;
            let uri = format!("{}/v1/webhooks/{id}", self.base);
            let (status, response) = self.authed(Method::DELETE, &uri, None).await?;
            ensure!(
                status == StatusCode::OK,
                "webhooks_delete answered {status}: {response}"
            );
        }
        Ok(())
    }

    /// The wait after attempt `attempt` at a webhook delivery fails, from the
    /// settings.
    pub fn webhook_backoff(&self, attempt: u32) -> Duration {
        let settings = &self.settings;
        let doublings = attempt.saturating_sub(1).min(31);
        Duration::from_secs(settings.webhook_backoff_base_seconds)
            .saturating_mul(1 << doublings)
            .min(Duration::from_secs(settings.webhook_backoff_cap_seconds))
    }

    /// How long a webhook takes to give up on one delivery: every attempt
    /// that is made after waiting for the one before.
    pub fn webhook_give_up(&self) -> Duration {
        (1..self.settings.webhook_max_attempts)
            .map(|attempt| self.webhook_backoff(attempt))
            .sum::<Duration>()
            + Duration::from_secs(self.settings.webhook_timeout_seconds)
    }

    // ---- credentials ------------------------------------------------------

    /// `principals_create`, answering whatever the server says.
    pub async fn create_principal(&self, body: &Value) -> anyhow::Result<(StatusCode, Value)> {
        let uri = format!("{}/v1/principals", self.base);
        self.authed(Method::POST, &uri, Some(body)).await
    }

    /// Issue a credential, which must be accepted. An agent's lasts ten
    /// minutes unless `ttl_seconds` says otherwise, so that a test that is not
    /// about expiry never meets one; other kinds do not expire unless asked.
    pub async fn issue(
        &self,
        kind: &str,
        name: &str,
        scopes: &Value,
        ttl_seconds: Option<u64>,
    ) -> anyhow::Result<Issued> {
        let ttl = ttl_seconds.or((kind == "agent").then_some(600));
        self.issue_with(kind, name, scopes, ttl).await
    }

    /// Issue a credential with exactly the `ttl_seconds` given, or none, which
    /// leaves the expiry to the server's settings.
    pub async fn issue_with(
        &self,
        kind: &str,
        name: &str,
        scopes: &Value,
        ttl_seconds: Option<u64>,
    ) -> anyhow::Result<Issued> {
        let mut body = json!({
            "kind": kind,
            "name": name,
            "tenant": self.tenant,
            "scopes": scopes,
        });
        if let Some(ttl) = ttl_seconds {
            body["ttl_seconds"] = json!(ttl);
        }
        let (status, response) = self.create_principal(&body).await?;
        ensure!(
            status == StatusCode::CREATED,
            "principals_create answered {status}: {response}"
        );
        let token = response["credential"]
            .as_str()
            .context("the answer carries the credential")?
            .to_owned();
        Ok(Issued {
            id: response["principal"]["id"]
                .as_str()
                .context("the answer carries the principal id")?
                .to_owned(),
            principal: response["principal"].clone(),
            token,
            http: self.http.clone(),
            base: self.base.clone(),
            tenant: self.tenant.clone(),
        })
    }

    /// `principals_revoke`, answering whatever the server says.
    pub async fn revoke(&self, id: &str) -> anyhow::Result<(StatusCode, Value)> {
        let uri = format!("{}/v1/principals/{id}", self.base);
        self.authed(Method::DELETE, &uri, None).await
    }

    /// `principals_list` for this tenant.
    pub async fn principals(&self) -> anyhow::Result<Vec<Value>> {
        let uri = format!("{}/v1/principals?tenant={}", self.base, self.tenant);
        let (status, response) = self.authed(Method::GET, &uri, None).await?;
        ensure!(
            status == StatusCode::OK,
            "principals_list answered {status}: {response}"
        );
        Ok(response["principals"].as_array().context("a list")?.clone())
    }

    /// Make sure a topic of a tenant exists, by publishing to it as the admin.
    pub async fn seed(&self, tenant: &str, topic: &str) -> anyhow::Result<()> {
        let uri = format!("{}/v1/tenants/{tenant}/topics/{topic}/messages", self.base);
        let body = json!({"source": "e2e", "type": "e2e.seed"});
        let (status, response) = self.authed(Method::POST, &uri, Some(&body)).await?;
        ensure!(
            status == StatusCode::CREATED,
            "seeding {topic} answered {status}: {response}"
        );
        Ok(())
    }

    /// Seed a work queue of this tenant with an item.
    pub async fn seed_queue(&self, queue: &str) -> anyhow::Result<()> {
        self.work_publish(queue, &job(0)).await.map(|_| ())
    }

    // ---- the broker, not through the server ---------------------------------

    /// Sign in to the broker with the credential, asked of the broker itself
    /// and not of mansio-server: `None` if the broker will not have it. In
    /// process the broker is the memory broker; against a live server it is
    /// Iggy at `MANSIO_E2E_IGGY_ADDRESS`, which must be set.
    pub async fn broker_session(&self, token: &str) -> anyhow::Result<Option<Direct>> {
        if let Some((broker, _, _)) = &self.local {
            let credential = Credential::new(token);
            return Ok(match broker.identify(&credential).await {
                Ok(_) => Some(Direct(DirectKind::Memory {
                    broker: Arc::clone(broker),
                    credential,
                })),
                Err(_) => None,
            });
        }
        let client = iggy_client(&iggy_address()?).await?;
        Ok(match client.login_with_personal_access_token(token).await {
            Ok(_) => Some(Direct(DirectKind::Iggy(client))),
            Err(_) => {
                let _ = client.shutdown().await;
                None
            }
        })
    }

    /// Whether the broker still signs in as the credential:
    /// [`Outcome::Accepted`] if it does.
    pub async fn broker_login(&self, token: &str) -> anyhow::Result<Outcome> {
        Ok(match self.broker_session(token).await? {
            Some(session) => {
                session.close().await;
                Outcome::Accepted
            }
            None => Outcome::Unauthenticated,
        })
    }

    /// Whether the broker lets the credential read the topic of the tenant.
    pub async fn broker_poll(
        &self,
        token: &str,
        tenant: &str,
        topic: &str,
    ) -> anyhow::Result<Outcome> {
        let Some(session) = self.broker_session(token).await? else {
            return Ok(Outcome::Unauthenticated);
        };
        let outcome = session.poll(tenant, topic).await?;
        session.close().await;
        Ok(outcome)
    }

    /// Whether the broker lets the credential send a message to the topic of
    /// the tenant. It never does: a credential reads, and the server writes.
    pub async fn broker_send(
        &self,
        token: &str,
        tenant: &str,
        topic: &str,
    ) -> anyhow::Result<Outcome> {
        let Some(session) = self.broker_session(token).await? else {
            return Ok(Outcome::Unauthenticated);
        };
        let outcome = session.send(tenant, topic).await?;
        session.close().await;
        Ok(outcome)
    }

    /// Whether the broker lets the credential read the server's own record of
    /// principals. See [`Target::broker_read_system`].
    pub async fn broker_read_registry(&self, token: &str) -> anyhow::Result<Option<Outcome>> {
        self.broker_read_system(token, mansio_iggy::SYSTEM_TOPIC)
            .await
    }

    /// Whether the broker lets the credential read a log of the server's own
    /// records: `principals`, or `webhooks`, which holds signing secrets. Only
    /// Iggy can be asked: in process the records have no reader but the
    /// server, and `None` comes back.
    pub async fn broker_read_system(
        &self,
        token: &str,
        topic: &str,
    ) -> anyhow::Result<Option<Outcome>> {
        if self.local.is_some() {
            return Ok(None);
        }
        let Some(session) = self.broker_session(token).await? else {
            return Ok(Some(Outcome::Unauthenticated));
        };
        let outcome = session.poll(mansio_iggy::SYSTEM_STREAM, topic).await?;
        session.close().await;
        Ok(Some(outcome))
    }
}

/// A session with the broker opened with a credential, kept open to show what
/// the broker does with it later.
pub struct Direct(DirectKind);

enum DirectKind {
    Memory {
        broker: Arc<MemoryBroker>,
        credential: Credential,
    },
    Iggy(iggy::prelude::IggyClient),
}

impl Direct {
    /// Read one message of the topic.
    pub async fn poll(&self, tenant: &str, topic: &str) -> anyhow::Result<Outcome> {
        Ok(match &self.0 {
            DirectKind::Memory { broker, credential } => {
                match broker
                    .poll(
                        &Caller::Credential(credential),
                        &tenant.parse()?,
                        &topic.parse()?,
                        0,
                        1,
                    )
                    .await
                {
                    Ok(_) => Outcome::Accepted,
                    Err(error) => outcome_of(&error),
                }
            }
            DirectKind::Iggy(client) => {
                match client
                    .poll_messages(
                        &iggy::prelude::Identifier::named(tenant)?,
                        &iggy::prelude::Identifier::named(topic)?,
                        Some(0),
                        &iggy::prelude::Consumer::default(),
                        &iggy::prelude::PollingStrategy::offset(0),
                        1,
                        false,
                    )
                    .await
                {
                    Ok(_) => Outcome::Accepted,
                    Err(error) => iggy_outcome(error),
                }
            }
        })
    }

    /// Send a message to the topic.
    pub async fn send(&self, tenant: &str, topic: &str) -> anyhow::Result<Outcome> {
        let envelope = Envelope::new(
            EventId::generate(),
            "e2e".parse()?,
            "e2e.direct".parse()?,
            tenant.parse()?,
            topic.parse()?,
            "agent:direct".parse()?,
        );
        Ok(match &self.0 {
            DirectKind::Memory { broker, credential } => {
                match broker
                    .publish(&Caller::Credential(credential), &envelope)
                    .await
                {
                    Ok(_) => Outcome::Accepted,
                    Err(error) => outcome_of(&error),
                }
            }
            DirectKind::Iggy(client) => {
                let mut message = IggyMessage::builder()
                    .id(envelope.id.as_u128())
                    .payload(envelope.to_json_vec()?.into())
                    .build()?;
                match client
                    .send_messages(
                        &iggy::prelude::Identifier::named(tenant)?,
                        &iggy::prelude::Identifier::named(topic)?,
                        &Partitioning::partition_id(0),
                        std::slice::from_mut(&mut message),
                    )
                    .await
                {
                    Ok(_) => Outcome::Accepted,
                    Err(error) => iggy_outcome(error),
                }
            }
        })
    }

    /// Make a token that never expires for the signed-in user, which Iggy lets
    /// any signed-in user do. `None` in process, where the memory broker has no
    /// such call, or if Iggy refuses.
    pub async fn mint_token(&self) -> Option<String> {
        match &self.0 {
            DirectKind::Memory { .. } => None,
            DirectKind::Iggy(client) => client
                .create_personal_access_token("mine", iggy::prelude::IggyExpiry::NeverExpire)
                .await
                .ok()
                .map(|raw| raw.token),
        }
    }

    /// End the session.
    pub async fn close(self) {
        if let DirectKind::Iggy(client) = self.0 {
            let _ = client.shutdown().await;
        }
    }
}

/// What a broker said to a credential. A credential that cannot sign in is
/// not authenticated, whatever the reason Iggy gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// It did what was asked.
    Accepted,
    /// The credential is valid and the broker's permissions refuse it.
    Forbidden,
    /// The credential is not valid: unknown, expired or revoked.
    Unauthenticated,
    /// Anything else, which a test treats as a failure to be explained.
    Other(String),
}

fn outcome_of(error: &BrokerError) -> Outcome {
    match error {
        BrokerError::Forbidden(_) => Outcome::Forbidden,
        BrokerError::Unauthenticated(_) => Outcome::Unauthenticated,
        other => Outcome::Other(other.to_string()),
    }
}

fn iggy_outcome(error: IggyError) -> Outcome {
    match error {
        IggyError::Unauthorized => Outcome::Forbidden,
        IggyError::Unauthenticated
        | IggyError::InvalidCredentials
        | IggyError::InvalidPersonalAccessToken
        | IggyError::PersonalAccessTokenExpired(..) => Outcome::Unauthenticated,
        other => Outcome::Other(other.to_string()),
    }
}

/// Iggy's address for the tests that talk to it. Without it those tests
/// fail: a run against a live server that cannot reach Iggy has not shown that
/// Iggy refuses anything.
fn iggy_address() -> anyhow::Result<String> {
    std::env::var("MANSIO_E2E_IGGY_ADDRESS")
        .ok()
        .filter(|address| !address.is_empty())
        .context(
            "MANSIO_E2E_IGGY_ADDRESS must be set when MANSIO_E2E_URL is: these tests ask Iggy itself",
        )
}

/// A client connected to Iggy and signed in as nobody.
async fn iggy_client(address: &str) -> anyhow::Result<iggy::prelude::IggyClient> {
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(address.to_owned())
        .build()?;
    tokio::time::timeout(SLACK, client.connect())
        .await
        .context("Iggy did not answer in time")??;
    Ok(client)
}

/// A credential that was issued, and the means to use it.
pub struct Issued {
    pub id: String,
    pub principal: Value,
    pub token: String,
    http: reqwest::Client,
    base: String,
    tenant: String,
}

impl Issued {
    /// The same credential, used against the server the target runs now: after
    /// the server is restarted it listens elsewhere.
    pub fn at(mut self, target: &Target) -> Self {
        self.base = target.base.clone();
        self
    }

    async fn call(
        &self,
        method: Method,
        uri: &str,
        body: Option<&Value>,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let mut request = self.http.request(method, uri).bearer_auth(&self.token);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        Ok((
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        ))
    }

    fn tenant_uri(&self, tenant: &str, rest: &str) -> String {
        format!("{}/v1/tenants/{tenant}/{rest}", self.base)
    }

    /// The generic publish route.
    pub async fn publish(&self, topic: &str) -> anyhow::Result<(StatusCode, Value)> {
        self.publish_to(&self.tenant, topic).await
    }

    /// The generic publish route, in a tenant of the caller's choosing.
    pub async fn publish_to(
        &self,
        tenant: &str,
        topic: &str,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(tenant, &format!("topics/{topic}/messages"));
        let body = json!({"source": "e2e", "type": "e2e.credential"});
        self.call(Method::POST, &uri, Some(&body)).await
    }

    /// The generic poll route.
    pub async fn poll(&self, topic: &str) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(&self.tenant, &format!("topics/{topic}/messages?offset=0"));
        self.call(Method::GET, &uri, None).await
    }

    /// `work_publish`.
    pub async fn work_publish(&self, queue: &str) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(&self.tenant, &format!("work/{queue}/items"));
        self.call(Method::POST, &uri, Some(&job(1))).await
    }

    /// `work_pull`.
    pub async fn pull(&self, queue: &str) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(&self.tenant, &format!("work/{queue}/pull"));
        self.call(Method::POST, &uri, Some(&json!({"consumer": "worker"})))
            .await
    }

    /// `work_ack` of a claim that `pull` answered with.
    pub async fn ack(&self, queue: &str, claim: &Value) -> anyhow::Result<(StatusCode, Value)> {
        let item = claim["item"]["itemid"]
            .as_str()
            .context("a claim names its item")?;
        let uri = self.tenant_uri(&self.tenant, &format!("work/{queue}/items/{item}/ack"));
        let body = json!({"lease_id": claim["lease_id"], "consumer": claim["consumer"]});
        self.call(Method::POST, &uri, Some(&body)).await
    }

    /// `work_extend` of a claim that `pull` answered with.
    pub async fn extend(&self, queue: &str, claim: &Value) -> anyhow::Result<(StatusCode, Value)> {
        let item = claim["item"]["itemid"]
            .as_str()
            .context("a claim names its item")?;
        let uri = self.tenant_uri(&self.tenant, &format!("work/{queue}/items/{item}/extend"));
        let body = json!({"lease_id": claim["lease_id"], "consumer": claim["consumer"]});
        self.call(Method::POST, &uri, Some(&body)).await
    }

    /// `alerts_ack`.
    pub async fn alert_ack(&self, alert: &str) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(&self.tenant, &format!("alerts/{alert}/ack"));
        self.call(Method::POST, &uri, None).await
    }

    /// `alerts_publish`.
    pub async fn alert_publish(&self) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(&self.tenant, "alerts");
        let body = json!({"source": "e2e", "type": "disk.full"});
        self.call(Method::POST, &uri, Some(&body)).await
    }

    /// The admin API with this credential: `principals_list`.
    pub async fn list_principals(&self) -> anyhow::Result<(StatusCode, Value)> {
        let uri = format!("{}/v1/principals", self.base);
        self.call(Method::GET, &uri, None).await
    }

    /// The admin API with this credential: `principals_create`.
    pub async fn create_principal(&self) -> anyhow::Result<(StatusCode, Value)> {
        let uri = format!("{}/v1/principals", self.base);
        let body = json!({
            "kind": "agent",
            "name": "sneaky",
            "tenant": self.tenant,
            "scopes": [{"topic": "facts.*", "verbs": ["publish", "subscribe"]}]
        });
        self.call(Method::POST, &uri, Some(&body)).await
    }

    /// The admin API with this credential: `principals_revoke`.
    pub async fn revoke_principal(&self, id: &str) -> anyhow::Result<(StatusCode, Value)> {
        let uri = format!("{}/v1/principals/{id}", self.base);
        self.call(Method::DELETE, &uri, None).await
    }

    /// `GET /v1/settings`.
    pub async fn settings(&self) -> anyhow::Result<(StatusCode, Value)> {
        let uri = format!("{}/v1/settings", self.base);
        self.call(Method::GET, &uri, None).await
    }
}

async fn read_settings(
    http: &reqwest::Client,
    base: &str,
    token: &str,
) -> anyhow::Result<EffectiveSettings> {
    let response = http
        .get(format!("{base}/v1/settings"))
        .bearer_auth(token)
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::OK,
        "GET /v1/settings answered {}",
        response.status()
    );
    Ok(response.json().await?)
}

/// The item id of a claim.
pub fn item_of(claim: &Claimed) -> anyhow::Result<String> {
    Ok(claim
        .item
        .item_id
        .as_ref()
        .context("a claimed item has an item id")?
        .to_string())
}

/// The event types of envelopes, without the prefix they share.
pub fn type_names(envelopes: &[Envelope], prefix: &str) -> Vec<String> {
    envelopes
        .iter()
        .map(|envelope| envelope.event_type.as_str().replace(prefix, ""))
        .collect()
}

/// Milliseconds since the epoch of an RFC 3339 timestamp.
pub fn millis(timestamp: &Timestamp) -> anyhow::Result<i64> {
    timestamp
        .unix_millis()
        .context("a timestamp inside the range of milliseconds")
}

/// Where the lease ends, in milliseconds since the epoch, as an `extended`
/// fact says.
pub fn extended_to(fact: &Published) -> anyhow::Result<i64> {
    ensure!(
        fact.envelope.event_type.as_str() == "mansio.work.extended",
        "not an extended fact: {}",
        fact.envelope.event_type
    );
    let ends = fact
        .envelope
        .data
        .as_ref()
        .and_then(|data| data.get("expires_at"))
        .context("an extended fact says where the lease ends")?;
    millis(&serde_json::from_value(ends.clone())?)
}

/// A work item body with a type and a payload.
pub fn job(n: u32) -> Value {
    json!({
        "source": "e2e",
        "type": "job.requested",
        "subject": format!("job-{n}"),
        "datacontenttype": "application/json",
        "data": {"n": n},
        "correlationid": "run-1"
    })
}
