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

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail, ensure};
use mansio_core::api::{Claimed, EffectiveSettings, Polled, Published};
use mansio_core::{Envelope, EventId, MemoryBroker, Timestamp};
use mansio_server::{Auth, FileSink, Server, WorkSettings};
use reqwest::{Method, StatusCode};
use secrecy::SecretString;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// The token the in-process server accepts.
const LOCAL_TOKEN: &str = "e2e-local-token";
/// The principal the server's token stands for, by default.
pub const PRINCIPAL: &str = "service:mansio";
/// How often a test looks again while it waits for something.
const LOOK_AGAIN: Duration = Duration::from_millis(50);
/// What a wait may take beyond what the settings call for, for a slow runner.
pub const SLACK: Duration = Duration::from_secs(15);

/// The work settings of the in-process server: seconds-scale, so that a test
/// of a lease or a backoff takes seconds.
fn local_work_settings() -> WorkSettings {
    WorkSettings {
        lease: Duration::from_secs(2),
        max_attempts: 3,
        backoff_base: Duration::from_secs(1),
        backoff_cap: Duration::from_secs(4),
        alert_resurface: Duration::from_secs(2),
        tick: Duration::from_millis(100),
    }
}

/// A server running in this process. It stops when dropped.
struct LocalServer {
    base: String,
    task: JoinHandle<io::Result<()>>,
}

impl LocalServer {
    async fn start(broker: &Arc<MemoryBroker>, sink: &Path) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let auth = Auth::new(
            SecretString::from(LOCAL_TOKEN.to_owned()),
            PRINCIPAL.parse()?,
        );
        let server = Server::new(broker.clone(), auth)
            .with_sink(Arc::new(FileSink::open(sink).await?))
            .with_work_settings(local_work_settings());
        let task = tokio::spawn(server.serve(listener, std::future::pending()));
        Ok(Self { base, task })
    }
}

impl Drop for LocalServer {
    /// Stopping is not graceful: the listener and the timer go at once, as
    /// they would if the process died.
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
    /// The in-process broker and server, if there is one.
    local: Option<(Arc<MemoryBroker>, Option<LocalServer>)>,
}

impl Drop for Target {
    fn drop(&mut self) {
        if let (Some(path), Some(_)) = (&self.sink, &self.local) {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Target {
    pub async fn start() -> anyhow::Result<Self> {
        let tenant = format!("e2e-{}", EventId::generate().to_string().replace('-', ""));
        let http = reqwest::Client::new();
        if let Some(url) = std::env::var("MANSIO_E2E_URL")
            .ok()
            .filter(|url| !url.is_empty())
        {
            let token = std::env::var("MANSIO_TOKEN")
                .context("MANSIO_TOKEN must be set when MANSIO_E2E_URL is")?;
            let base = url.trim_end_matches('/').to_owned();
            let settings = read_settings(&http, &base, &token).await?;
            let sink = std::env::var_os("MANSIO_E2E_SINK_PATH")
                .filter(|path| !path.is_empty())
                .map(PathBuf::from);
            return Ok(Self {
                base,
                token,
                http,
                tenant,
                settings,
                sink,
                local: None,
            });
        }
        let sink = std::env::temp_dir().join(format!("mansio-{tenant}.jsonl"));
        let broker = Arc::new(MemoryBroker::new());
        let server = LocalServer::start(&broker, &sink).await?;
        let settings = read_settings(&http, &server.base, LOCAL_TOKEN).await?;
        Ok(Self {
            base: server.base.clone(),
            token: LOCAL_TOKEN.to_owned(),
            http,
            tenant,
            settings,
            sink: Some(sink),
            local: Some((broker, Some(server))),
        })
    }

    /// Whether the server runs in this process, and so can be stopped and
    /// started again.
    pub fn is_local(&self) -> bool {
        self.local.is_some()
    }

    /// Stop the in-process server as a crash would. The broker and the sink
    /// file stay.
    pub async fn crash_server(&mut self) -> anyhow::Result<()> {
        let (_, server) = self.local.as_mut().context("the server is not local")?;
        let mut stopped = server.take().context("the server is already stopped")?;
        stopped.task.abort();
        // Wait until it is gone, timer included, so that nothing of the old
        // server is still running when a new one starts.
        let _ = (&mut stopped.task).await;
        Ok(())
    }

    /// Start a new in-process server on the broker and the sink file that
    /// the last one used.
    pub async fn start_server(&mut self) -> anyhow::Result<()> {
        let sink = self.sink.clone().context("no sink file")?;
        let (broker, server) = self.local.as_mut().context("the server is not local")?;
        ensure!(server.is_none(), "the server is running");
        let started = LocalServer::start(broker, &sink).await?;
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

    /// Everything in a topic, in order.
    pub async fn read(&self, topic: &str) -> anyhow::Result<Vec<Envelope>> {
        let mut all = Vec::new();
        let mut offset = 0;
        loop {
            let query = format!("offset={offset}&count={}", self.settings.poll_max_count);
            let page = self.poll(topic, &query).await?;
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
        lease: &str,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(&format!("work/{queue}/items/{item}/ack"));
        self.authed(Method::POST, &uri, Some(&json!({ "lease_id": lease })))
            .await
    }

    /// `work_fail`, answering whatever the server says.
    pub async fn fail(
        &self,
        queue: &str,
        item: &str,
        lease: &str,
        reason: &str,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let uri = self.tenant_uri(&format!("work/{queue}/items/{item}/fail"));
        let body = json!({ "lease_id": lease, "reason": reason });
        self.authed(Method::POST, &uri, Some(&body)).await
    }

    /// Ack a claim, which must be accepted.
    pub async fn ack_claim(&self, queue: &str, claim: &Claimed) -> anyhow::Result<Published> {
        let item = item_of(claim)?;
        let (status, response) = self.ack(queue, &item, &claim.lease_id.to_string()).await?;
        ensure!(
            status == StatusCode::OK,
            "work_ack answered {status}: {response}"
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
            .fail(queue, &item, &claim.lease_id.to_string(), reason)
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
        let mut records = Vec::new();
        for line in text.lines() {
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

    /// One timer tick, and a little for a round trip: how long to wait to
    /// show that something does not happen.
    pub fn settle(&self) -> Duration {
        Duration::from_millis(self.settings.tick_millis.saturating_mul(3) + 300)
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
