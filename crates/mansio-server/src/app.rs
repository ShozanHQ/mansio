//! The HTTP application: routes, handlers, and the `Server` builder a host
//! application uses to run it in-process.

use std::future::Future;
use std::io;
use std::str::FromStr;
use std::sync::Arc;

use axum::extract::rejection::{BytesRejection, PathRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use bytes::Bytes;
use mansio_core::api::{EffectiveSettings, Polled, Published};
use mansio_core::{
    Action, Broker, Envelope, EventId, NoSink, ParseError, Principal, Sink, TenantId, Timestamp,
    Topic,
};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::auth::{Auth, Authenticated};
use crate::clock::{Clock, SystemClock};
use crate::credentials::Credentials;
use crate::engine::Engine;
use crate::error::ApiError;
use crate::principals_api;
use crate::settings::{CredentialSettings, Limits, WorkSettings};
use crate::work_api;

/// Attributes the server sets itself, from the path and the credential. A
/// body that carries one is refused.
const SERVER_SET: [&str; 4] = ["tenant", "topic", "principalkind", "principalname"];

/// Attributes `work_publish` sets itself: the item is named by the id of its
/// first message, and starts at attempt 1.
const WORK_SET: [&str; 2] = ["itemid", "attempt"];

/// Everything a handler needs.
pub struct AppState {
    pub(crate) broker: Arc<dyn Broker>,
    pub(crate) engine: Arc<Engine>,
    pub(crate) credentials: Arc<Credentials>,
    pub(crate) auth: Auth,
    pub(crate) limits: Limits,
}

/// What [`Server::build`] makes: the routes, and the two things that have to
/// be run alongside them.
pub struct Parts {
    /// The routes.
    pub router: Router,
    /// The work and alert engine. Run [`Engine::run`] alongside the routes.
    pub engine: Arc<Engine>,
    /// Credentials. Run [`Credentials::run`] alongside the routes.
    pub credentials: Arc<Credentials>,
}

/// The mansio HTTP server, ready to run.
///
/// A host application builds one with its own [`Broker`] and [`Sink`] and
/// calls [`Server::serve`], which also runs the timer that expires leases,
/// republishes items and surfaces alerts again, and the sweep that revokes
/// expired credentials. A host that mounts the [`Router`] itself takes it from
/// [`Server::build`] together with the [`Engine`] and [`Credentials`], and
/// runs [`Engine::run`] and [`Credentials::run`] alongside.
pub struct Server {
    broker: Arc<dyn Broker>,
    sink: Arc<dyn Sink>,
    clock: Arc<dyn Clock>,
    auth: Auth,
    limits: Limits,
    work: WorkSettings,
    credentials: CredentialSettings,
}

/// Aborts a task when dropped, so that a server that is dropped or stopped
/// does not leave its timer running.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Server {
    /// A server over `broker` that accepts `auth`, records nothing in a sink,
    /// reads the system clock and uses the default limits and work settings.
    #[must_use]
    pub fn new(broker: Arc<dyn Broker>, auth: Auth) -> Self {
        Self {
            broker,
            sink: Arc::new(NoSink),
            clock: Arc::new(SystemClock),
            auth,
            limits: Limits::default(),
            work: WorkSettings::default(),
            credentials: CredentialSettings::default(),
        }
    }

    /// Record every durable publish in `sink` first.
    #[must_use]
    pub fn with_sink(mut self, sink: Arc<dyn Sink>) -> Self {
        self.sink = sink;
        self
    }

    /// Use these limits instead of the defaults.
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Use these work and alert settings instead of the defaults.
    #[must_use]
    pub fn with_work_settings(mut self, work: WorkSettings) -> Self {
        self.work = work;
        self
    }

    /// Use these credential settings instead of the defaults.
    #[must_use]
    pub fn with_credential_settings(mut self, credentials: CredentialSettings) -> Self {
        self.credentials = credentials;
        self
    }

    /// Read time from `clock` instead of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The routes, and the engine and credentials behind them. Nothing drives
    /// the engine's timer or the credential sweep until [`Engine::run`] and
    /// [`Credentials::run`] are, or [`Engine::tick`] and [`Credentials::sweep`]
    /// are called.
    pub fn build(self) -> Parts {
        let max_body = self.limits.max_body_bytes;
        let engine = Arc::new(Engine::new(
            Arc::clone(&self.broker),
            self.sink,
            Arc::clone(&self.clock),
            self.work,
            self.limits.poll_max_count,
        ));
        let credentials = Arc::new(Credentials::new(
            Arc::clone(&self.broker),
            Arc::clone(&engine),
            self.clock,
            self.credentials,
            self.auth.principal().clone(),
            self.limits.poll_max_count,
        ));
        let state = AppState {
            broker: self.broker,
            engine: Arc::clone(&engine),
            credentials: Arc::clone(&credentials),
            auth: self.auth,
            limits: self.limits,
        };
        let router = Router::new()
            .route("/healthz", get(healthz))
            .route("/v1/settings", get(settings))
            .route(
                "/v1/tenants/{tenant}/topics/{topic}/messages",
                get(poll).post(publish),
            )
            .route(
                "/v1/tenants/{tenant}/work/{queue}/items",
                post(work_api::work_publish),
            )
            .route(
                "/v1/tenants/{tenant}/work/{queue}/pull",
                post(work_api::work_pull),
            )
            .route(
                "/v1/tenants/{tenant}/work/{queue}/items/{item}/ack",
                post(work_api::work_ack),
            )
            .route(
                "/v1/tenants/{tenant}/work/{queue}/items/{item}/fail",
                post(work_api::work_fail),
            )
            .route(
                "/v1/tenants/{tenant}/alerts",
                post(work_api::alerts_publish),
            )
            .route(
                "/v1/tenants/{tenant}/alerts/{alert}/ack",
                post(work_api::alerts_ack),
            )
            .route(
                "/v1/principals",
                post(principals_api::principals_create).get(principals_api::principals_list),
            )
            .route(
                "/v1/principals/{id}",
                delete(principals_api::principals_revoke),
            )
            .fallback(not_found)
            .layer(DefaultBodyLimit::max(max_body))
            .with_state(Arc::new(state));
        Parts {
            router,
            engine,
            credentials,
        }
    }

    /// The routes. The engine's timer and the credential sweep do not run;
    /// see [`Server::build`].
    pub fn router(self) -> Router {
        self.build().router
    }

    /// Serve on `listener` until `shutdown` completes. Before it accepts a
    /// request the engine takes up what the broker holds and the registry of
    /// principals is loaded, and while it runs the timer drives expiries,
    /// republishes and alerts, and the sweep revokes expired credentials.
    /// Dropping the future stops both.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener fails.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> io::Result<()> {
        let Parts {
            router,
            engine,
            credentials,
        } = self.build();
        if let Err(error) = engine.recover().await {
            warn!(
                ?error,
                "the tenants could not be listed; the timer will try again"
            );
        }
        if let Err(error) = credentials.load().await {
            warn!(
                ?error,
                "the registry of principals could not be loaded; the next request will try again"
            );
        }
        let _timer = AbortOnDrop(tokio::spawn(engine.run()));
        let _sweep = AbortOnDrop(tokio::spawn(credentials.run()));
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
    }
}

async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn settings(
    State(state): State<Arc<AppState>>,
    _auth: Authenticated,
) -> Json<EffectiveSettings> {
    let work = state.engine.settings();
    let credentials = state.credentials.settings();
    let limits = &state.limits;
    let seconds = |d: std::time::Duration| d.as_secs();
    Json(EffectiveSettings {
        lease_seconds: seconds(work.lease),
        max_attempts: work.max_attempts,
        backoff_base_seconds: seconds(work.backoff_base),
        backoff_cap_seconds: seconds(work.backoff_cap),
        alert_resurface_seconds: seconds(work.alert_resurface),
        tick_millis: u64::try_from(work.tick.as_millis()).unwrap_or(u64::MAX),
        max_body_bytes: u64::try_from(limits.max_body_bytes).unwrap_or(u64::MAX),
        poll_default_count: limits.poll_default_count,
        poll_max_count: limits.poll_max_count,
        agent_credential_ttl_seconds: seconds(credentials.agent_ttl),
        credential_recheck_seconds: seconds(credentials.recheck),
        credential_sweep_seconds: seconds(credentials.sweep),
    })
}

async fn not_found() -> ApiError {
    ApiError::NotFound("no such route".to_owned())
}

/// Parse a piece of the path.
pub(crate) fn parse_part<T: FromStr<Err = ParseError>>(text: &str) -> Result<T, ApiError> {
    text.parse()
        .map_err(|e: ParseError| ApiError::BadRequest(e.to_string()))
}

fn parse_target(
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<(TenantId, Topic), ApiError> {
    let Path((tenant, topic)) = path.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    Ok((parse_part(&tenant)?, parse_part(&topic)?))
}

/// The request body, or the refusal for a body that is too large or cannot be
/// read.
pub(crate) fn read_body(
    state: &AppState,
    body: Result<Bytes, BytesRejection>,
) -> Result<Bytes, ApiError> {
    body.map_err(|e| match e.status() {
        StatusCode::PAYLOAD_TOO_LARGE => ApiError::PayloadTooLarge(format!(
            "body is larger than {} bytes",
            state.limits.max_body_bytes
        )),
        _ => ApiError::BadRequest(e.body_text()),
    })
}

/// A request body that is a JSON object of a known shape.
pub(crate) fn parse_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|e| ApiError::BadRequest(format!("body: {e}")))
}

/// What the envelope being built is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    /// A message to any topic.
    Message,
    /// A work item: the server names it and sets its attempt.
    WorkItem,
}

/// Make the envelope to store from a request body.
///
/// The body is an envelope without the attributes the server sets. `id`,
/// `time` and `specversion` are filled in when absent. Everything else is
/// checked by the envelope's own rules. A work item is named by its `id`
/// (`itemid`) and starts at `attempt` 1, and a body that sets either is
/// refused.
pub(crate) fn build_envelope(
    body: &[u8],
    tenant: &TenantId,
    topic: &Topic,
    principal: &Principal,
    now: &Timestamp,
    origin: Origin,
) -> Result<Envelope, ApiError> {
    let bad = |message: String| ApiError::BadRequest(message);
    let Value::Object(mut attributes) =
        serde_json::from_slice::<Value>(body).map_err(|e| bad(format!("body is not JSON: {e}")))?
    else {
        return Err(bad("body must be a JSON object".to_owned()));
    };

    let refused: Vec<&str> = match origin {
        Origin::Message => SERVER_SET.to_vec(),
        Origin::WorkItem => SERVER_SET.into_iter().chain(WORK_SET).collect(),
    };
    let present: Vec<&str> = refused
        .into_iter()
        .filter(|name| attributes.contains_key(*name))
        .collect();
    if !present.is_empty() {
        return Err(bad(format!(
            "{} are set by the server and must not be in the body",
            present.join(", ")
        )));
    }

    attributes
        .entry("specversion")
        .or_insert_with(|| "1.0".into());
    attributes
        .entry("id")
        .or_insert_with(|| EventId::generate().to_string().into());
    if !attributes.contains_key("time") {
        attributes.insert("time".to_owned(), now.to_string().into());
    }
    if origin == Origin::WorkItem
        && let Some(Value::String(id)) = attributes.get("id").cloned()
    {
        attributes.insert("itemid".to_owned(), id.into());
        attributes.insert("attempt".to_owned(), 1.into());
    }
    attributes.insert("tenant".to_owned(), tenant.to_string().into());
    attributes.insert("topic".to_owned(), topic.to_string().into());
    attributes.insert(
        "principalkind".to_owned(),
        principal.kind.to_string().into(),
    );
    attributes.insert(
        "principalname".to_owned(),
        principal.name.to_string().into(),
    );

    Envelope::from_json_value(Value::Object(attributes)).map_err(|e| bad(e.to_string()))
}

async fn publish(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<Published>), ApiError> {
    let (tenant, topic) = parse_target(path)?;
    actor.authorize(Action::Send, &tenant, &topic)?;
    let body = read_body(&state, body)?;
    let now = state.engine.now()?;
    let envelope = build_envelope(
        &body,
        &tenant,
        &topic,
        actor.principal(),
        &now,
        Origin::Message,
    )?;
    let published = state
        .engine
        .publish(&actor.caller(), envelope)
        .await
        .map_err(|error| error.logged(&tenant, &topic))?;
    Ok((StatusCode::CREATED, Json(published)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PollQuery {
    offset: Option<u64>,
    count: Option<u32>,
}

async fn poll(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String)>, PathRejection>,
    query: Result<Query<PollQuery>, QueryRejection>,
) -> Result<Json<Polled>, ApiError> {
    let (tenant, topic) = parse_target(path)?;
    actor.authorize(Action::Poll, &tenant, &topic)?;
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let limits = &state.limits;
    let count = query
        .count
        .unwrap_or_else(|| limits.poll_default_count.min(limits.poll_max_count));
    if count == 0 || count > limits.poll_max_count {
        return Err(ApiError::BadRequest(format!(
            "count must be between 1 and {}",
            limits.poll_max_count
        )));
    }

    // A read never makes a stream or a topic: a topic nobody has published
    // to is not found.
    let messages = state
        .broker
        .poll(
            &actor.caller(),
            &tenant,
            &topic,
            query.offset.unwrap_or(0),
            count,
        )
        .await
        .map_err(|error| ApiError::from(error).logged(&tenant, &topic))?;
    Ok(Json(Polled { messages }))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Method, Request, header};
    use http_body_util::BodyExt;
    use mansio_core::MemoryBroker;
    use mansio_core::api::ErrorBody;
    use secrecy::SecretString;
    use tower::ServiceExt;

    use super::*;

    const TOKEN: &str = "test-token";

    fn router() -> Router {
        let auth = Auth::new(
            SecretString::from(TOKEN.to_owned()),
            "service:mansio".parse().unwrap(),
        );
        Server::new(Arc::new(MemoryBroker::new()), auth)
            .with_limits(Limits {
                max_body_bytes: 2048,
                poll_default_count: 3,
                poll_max_count: 5,
            })
            .router()
    }

    async fn send(
        router: &Router,
        method: Method,
        uri: &str,
        token: Option<&str>,
        body: &str,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder().method(method).uri(uri);
        if let Some(token) = token {
            request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        let request = request.body(Body::from(body.to_owned())).unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    const URI: &str = "/v1/tenants/acme/topics/facts.build/messages";
    const MINIMAL: &str = r#"{"source":"s","type":"t"}"#;

    fn error_code(body: &Value) -> String {
        serde_json::from_value::<ErrorBody>(body.clone())
            .unwrap()
            .error
            .code
    }

    #[tokio::test]
    async fn healthz_needs_no_token() {
        let (status, body) = send(&router(), Method::GET, "/healthz", None, "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"status": "ok"}));
    }

    #[tokio::test]
    async fn the_api_needs_the_token() {
        let router = router();
        for token in [None, Some("wrong"), Some("")] {
            let (status, body) = send(&router, Method::POST, URI, token, MINIMAL).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{token:?}");
            assert_eq!(error_code(&body), "unauthorized");
            let (status, _) = send(&router, Method::GET, URI, token, "").await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
        }
    }

    #[tokio::test]
    async fn publish_fills_in_and_poll_returns_it() {
        let router = router();
        let (status, body) = send(&router, Method::POST, URI, Some(TOKEN), MINIMAL).await;
        assert_eq!(status, StatusCode::CREATED);
        let published: Published = serde_json::from_value(body).unwrap();
        assert_eq!(published.offset, Some(0));
        assert_eq!(published.envelope.principal.to_string(), "service:mansio");
        assert_eq!(published.envelope.tenant.as_str(), "acme");
        assert_eq!(published.envelope.topic.to_string(), "facts.build");
        assert!(published.envelope.time.is_some());

        let (status, body) = send(&router, Method::GET, URI, Some(TOKEN), "").await;
        assert_eq!(status, StatusCode::OK);
        let polled: Polled = serde_json::from_value(body).unwrap();
        assert_eq!(polled.messages.len(), 1);
        assert_eq!(polled.messages[0].envelope, published.envelope);
    }

    #[tokio::test]
    async fn a_body_with_server_set_attributes_is_refused() {
        let router = router();
        for name in SERVER_SET {
            let body = format!(r#"{{"source":"s","type":"t","{name}":"x"}}"#);
            let (status, body) = send(&router, Method::POST, URI, Some(TOKEN), &body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{name}");
            assert!(body["error"]["message"].as_str().unwrap().contains(name));
        }
    }

    #[tokio::test]
    async fn malformed_requests_are_refused() {
        let router = router();
        let cases = [
            (Method::POST, URI, "not json"),
            (Method::POST, URI, "[1]"),
            (Method::POST, URI, r#"{"source":"s"}"#),
            (Method::POST, URI, r#"{"source":"s","type":"t","extra":1}"#),
            (
                Method::POST,
                URI,
                r#"{"source":"s","type":"t","id":"nope"}"#,
            ),
            (
                Method::POST,
                "/v1/tenants/acme/topics/nope/messages",
                MINIMAL,
            ),
            (
                Method::POST,
                "/v1/tenants/ACME/topics/alerts/messages",
                MINIMAL,
            ),
            (Method::GET, "/v1/tenants/acme/topics/facts./messages", ""),
            (
                Method::GET,
                "/v1/tenants/acme/topics/alerts/messages?count=0",
                "",
            ),
            (
                Method::GET,
                "/v1/tenants/acme/topics/alerts/messages?count=6",
                "",
            ),
            (
                Method::GET,
                "/v1/tenants/acme/topics/alerts/messages?count=x",
                "",
            ),
            (
                Method::GET,
                "/v1/tenants/acme/topics/alerts/messages?offset=-1",
                "",
            ),
            (
                Method::GET,
                "/v1/tenants/acme/topics/alerts/messages?other=1",
                "",
            ),
        ];
        for (method, uri, body) in cases {
            let (status, response) = send(&router, method.clone(), uri, Some(TOKEN), body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {uri} {body}");
            assert_eq!(error_code(&response), "bad_request");
        }
    }

    #[tokio::test]
    async fn an_oversized_body_is_refused() {
        let body = format!(
            r#"{{"source":"s","type":"t","data":"{}"}}"#,
            "x".repeat(4096)
        );
        let (status, response) = send(&router(), Method::POST, URI, Some(TOKEN), &body).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(error_code(&response), "payload_too_large");
    }

    #[tokio::test]
    async fn unknown_routes_answer_in_json() {
        let (status, body) = send(&router(), Method::GET, "/nope", Some(TOKEN), "").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(error_code(&body), "not_found");
    }

    #[tokio::test]
    async fn polling_a_topic_nobody_published_to_is_a_404_and_makes_nothing() {
        let broker = Arc::new(MemoryBroker::new());
        let auth = Auth::new(
            SecretString::from(TOKEN.to_owned()),
            "service:mansio".parse().unwrap(),
        );
        let router = Server::new(broker.clone(), auth).router();
        let uri = "/v1/tenants/acme/topics/alerts/messages";
        for _ in 0..2 {
            let (status, body) = send(&router, Method::GET, uri, Some(TOKEN), "").await;
            assert_eq!(status, StatusCode::NOT_FOUND);
            assert_eq!(error_code(&body), "not_found");
        }
        // The read made no stream and no topic.
        assert!(broker.tenants().await.unwrap().is_empty());

        // Once something is published the topic is there, and a read of it
        // before any message beyond the end is empty.
        let (status, _) = send(&router, Method::POST, URI, Some(TOKEN), MINIMAL).await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, body) = send(
            &router,
            Method::GET,
            &format!("{URI}?offset=1"),
            Some(TOKEN),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"messages": []}));
    }

    #[tokio::test]
    async fn the_work_and_alert_routes_need_the_token() {
        let router = router();
        let routes = [
            (Method::GET, "/v1/settings"),
            (Method::POST, "/v1/tenants/acme/work/builds/items"),
            (Method::POST, "/v1/tenants/acme/work/builds/pull"),
            (Method::POST, "/v1/tenants/acme/work/builds/items/i/ack"),
            (Method::POST, "/v1/tenants/acme/work/builds/items/i/fail"),
            (Method::POST, "/v1/tenants/acme/alerts"),
            (
                Method::POST,
                "/v1/tenants/acme/alerts/0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3/ack",
            ),
        ];
        for (method, uri) in routes {
            let (status, body) = send(&router, method.clone(), uri, None, "{}").await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri}");
            assert_eq!(error_code(&body), "unauthorized");
        }
    }

    #[tokio::test]
    async fn work_requests_are_checked_before_anything_happens() {
        let router = router();
        let item = "/v1/tenants/acme/work/builds/items";
        let lease = "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3";
        let cases = [
            // Work goes through work_publish.
            (
                Method::POST,
                URI.replace("facts.build", "work.builds"),
                MINIMAL.to_owned(),
            ),
            (
                Method::POST,
                item.to_owned(),
                r#"{"source":"s","type":"t","itemid":"x"}"#.to_owned(),
            ),
            (
                Method::POST,
                item.to_owned(),
                r#"{"source":"s","type":"t","attempt":2}"#.to_owned(),
            ),
            (
                Method::POST,
                item.to_owned(),
                r#"{"source":"s","type":"t","topic":"alerts"}"#.to_owned(),
            ),
            (
                Method::POST,
                "/v1/tenants/acme/work/Bad/items".to_owned(),
                MINIMAL.to_owned(),
            ),
            // A pull names its consumer, and nothing else.
            (
                Method::POST,
                "/v1/tenants/acme/work/builds/pull".to_owned(),
                "{}".to_owned(),
            ),
            (
                Method::POST,
                "/v1/tenants/acme/work/builds/pull".to_owned(),
                r#"{"consumer":"a b"}"#.to_owned(),
            ),
            (
                Method::POST,
                "/v1/tenants/acme/work/builds/pull".to_owned(),
                r#"{"consumer":"w","x":1}"#.to_owned(),
            ),
            // An ack and a fail carry a lease id, and a fail a reason.
            (Method::POST, format!("{item}/i/ack"), "{}".to_owned()),
            (
                Method::POST,
                format!("{item}/i/ack"),
                r#"{"lease_id":"nope"}"#.to_owned(),
            ),
            (
                Method::POST,
                format!("{item}/i/fail"),
                format!(r#"{{"lease_id":"{lease}"}}"#),
            ),
            (
                Method::POST,
                format!("{item}/i/fail"),
                format!(r#"{{"lease_id":"{lease}","reason":" "}}"#),
            ),
            (
                Method::POST,
                "/v1/tenants/acme/alerts/nope/ack".to_owned(),
                String::new(),
            ),
        ];
        for (method, uri, body) in cases {
            let (status, response) = send(&router, method.clone(), &uri, Some(TOKEN), &body).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{method} {uri} {body}: {response}"
            );
            assert_eq!(error_code(&response), "bad_request");
        }
    }

    #[tokio::test]
    async fn an_empty_queue_answers_204_and_an_unknown_item_404() {
        let router = router();
        let (status, _) = send(
            &router,
            Method::POST,
            "/v1/tenants/acme/work/builds/pull",
            Some(TOKEN),
            r#"{"consumer":"w"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, body) = send(
            &router,
            Method::POST,
            "/v1/tenants/acme/work/builds/items/i/ack",
            Some(TOKEN),
            r#"{"lease_id":"0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(error_code(&body), "not_found");
    }

    #[tokio::test]
    async fn the_settings_route_reports_what_the_server_runs_with() {
        let (status, body) = send(&router(), Method::GET, "/v1/settings", Some(TOKEN), "").await;
        assert_eq!(status, StatusCode::OK);
        let settings: mansio_core::api::EffectiveSettings = serde_json::from_value(body).unwrap();
        assert_eq!(settings.lease_seconds, 300);
        assert_eq!(settings.max_attempts, 5);
        assert_eq!(settings.backoff_base_seconds, 5);
        assert_eq!(settings.backoff_cap_seconds, 300);
        assert_eq!(settings.alert_resurface_seconds, 3600);
        assert_eq!(settings.tick_millis, 500);
        assert_eq!(settings.max_body_bytes, 2048);
        assert_eq!(settings.poll_default_count, 3);
        assert_eq!(settings.poll_max_count, 5);
    }

    #[tokio::test]
    async fn a_client_chosen_id_is_kept() {
        let id = "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3";
        let body =
            format!(r#"{{"id":"{id}","source":"s","type":"t","time":"2026-10-26T12:00:00Z"}}"#);
        let (status, response) = send(&router(), Method::POST, URI, Some(TOKEN), &body).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(response["envelope"]["id"], id);
        assert_eq!(response["envelope"]["time"], "2026-10-26T12:00:00Z");
    }

    /// A sink that notes what it was given and answers as told.
    struct ScriptedSink {
        seen: std::sync::Mutex<Vec<String>>,
        answer: Result<Option<mansio_core::Record>, ()>,
    }

    #[async_trait::async_trait]
    impl Sink for ScriptedSink {
        async fn record(
            &self,
            envelope: &Envelope,
        ) -> Result<Option<mansio_core::Record>, mansio_core::SinkError> {
            self.seen.lock().unwrap().push(envelope.topic.to_string());
            self.answer
                .clone()
                .map_err(|()| mansio_core::SinkError::Failed("disk full".to_owned()))
        }
    }

    fn router_with(sink: Arc<ScriptedSink>) -> Router {
        let auth = Auth::new(
            SecretString::from(TOKEN.to_owned()),
            "service:mansio".parse().unwrap(),
        );
        Server::new(Arc::new(MemoryBroker::new()), auth)
            .with_sink(sink)
            .router()
    }

    #[tokio::test]
    async fn durable_publishes_pass_through_the_sink_and_take_its_record() {
        let record = mansio_core::Record {
            id: "rec-1".parse().unwrap(),
            prev_hash: "a".repeat(64).parse().unwrap(),
            event_hash: "b".repeat(64).parse().unwrap(),
        };
        let sink = Arc::new(ScriptedSink {
            seen: Default::default(),
            answer: Ok(Some(record.clone())),
        });
        let router = router_with(sink.clone());

        let (status, body) = send(&router, Method::POST, URI, Some(TOKEN), MINIMAL).await;
        assert_eq!(status, StatusCode::CREATED);
        let published: Published = serde_json::from_value(body).unwrap();
        assert_eq!(published.envelope.record, Some(record));

        // Presence is ephemeral and never reaches the sink.
        let presence = "/v1/tenants/acme/topics/presence/messages";
        let (status, _) = send(&router, Method::POST, presence, Some(TOKEN), MINIMAL).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(*sink.seen.lock().unwrap(), ["facts.build"]);
    }

    #[tokio::test]
    async fn a_failing_sink_stops_the_publish() {
        let sink = Arc::new(ScriptedSink {
            seen: Default::default(),
            answer: Err(()),
        });
        let router = router_with(sink);
        let (status, body) = send(&router, Method::POST, URI, Some(TOKEN), MINIMAL).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code(&body), "unavailable");

        let (_, body) = send(&router, Method::GET, URI, Some(TOKEN), "").await;
        assert_eq!(body, json!({"messages": []}));
    }

    /// A broker over [`MemoryBroker`] whose publish answers as told.
    struct ScriptedBroker {
        inner: MemoryBroker,
        publish: fn() -> Result<Option<u64>, mansio_core::BrokerError>,
    }

    #[async_trait::async_trait]
    impl Broker for ScriptedBroker {
        async fn tenants(&self) -> Result<Vec<TenantId>, mansio_core::BrokerError> {
            self.inner.tenants().await
        }

        async fn topics(&self, tenant: &TenantId) -> Result<Vec<Topic>, mansio_core::BrokerError> {
            self.inner.topics(tenant).await
        }

        async fn ensure_tenant(&self, tenant: &TenantId) -> Result<(), mansio_core::BrokerError> {
            self.inner.ensure_tenant(tenant).await
        }

        async fn ensure_topic(
            &self,
            tenant: &TenantId,
            topic: &Topic,
        ) -> Result<(), mansio_core::BrokerError> {
            self.inner.ensure_topic(tenant, topic).await
        }

        async fn publish(
            &self,
            _caller: &mansio_core::Caller<'_>,
            _envelope: &Envelope,
        ) -> Result<Option<u64>, mansio_core::BrokerError> {
            (self.publish)()
        }

        async fn poll(
            &self,
            caller: &mansio_core::Caller<'_>,
            tenant: &TenantId,
            topic: &Topic,
            offset: u64,
            max: u32,
        ) -> Result<Vec<mansio_core::Stored>, mansio_core::BrokerError> {
            self.inner.poll(caller, tenant, topic, offset, max).await
        }
    }

    fn router_over(publish: fn() -> Result<Option<u64>, mansio_core::BrokerError>) -> Router {
        let auth = Auth::new(
            SecretString::from(TOKEN.to_owned()),
            "service:mansio".parse().unwrap(),
        );
        let broker = ScriptedBroker {
            inner: MemoryBroker::new(),
            publish,
        };
        Server::new(Arc::new(broker), auth).router()
    }

    /// What the tracing events emitted while it is in scope say.
    #[derive(Clone, Default)]
    struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Logs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Logs {
        fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
            let logs = Self::default();
            let writer = logs.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish();
            (logs, tracing::subscriber::set_default(subscriber))
        }

        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    #[tokio::test]
    async fn a_stored_message_without_an_offset_is_still_a_201() {
        let router = router_over(|| Ok(None));
        let (status, body) = send(&router, Method::POST, URI, Some(TOKEN), MINIMAL).await;
        assert_eq!(status, StatusCode::CREATED);
        assert!(body.get("offset").is_none(), "{body}");
        let published: Published = serde_json::from_value(body).unwrap();
        assert_eq!(published.offset, None);
        assert_eq!(published.envelope.tenant.as_str(), "acme");
    }

    #[tokio::test]
    async fn a_refusal_by_the_broker_is_a_502_and_is_logged_with_where_and_why() {
        let (logs, _guard) = Logs::capture();
        let router = router_over(|| Err(mansio_core::BrokerError::Rejected("no room".to_owned())));
        let (status, body) = send(&router, Method::POST, URI, Some(TOKEN), MINIMAL).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(error_code(&body), "broker_refused");
        let logged = logs.text();
        for expected in [
            "WARN",
            "tenant=acme",
            "topic=facts.build",
            "status=502",
            "no room",
        ] {
            assert!(logged.contains(expected), "{expected} is not in {logged}");
        }
    }

    #[tokio::test]
    async fn a_refusal_by_the_brokers_permissions_is_a_403_and_a_bad_credential_a_401() {
        let (logs, _guard) = Logs::capture();
        let router = router_over(|| {
            Err(mansio_core::BrokerError::Forbidden(
                "the credential may not send to facts.build".to_owned(),
            ))
        });
        let (status, body) = send(&router, Method::POST, URI, Some(TOKEN), MINIMAL).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(error_code(&body), "forbidden");

        let router = router_over(|| {
            Err(mansio_core::BrokerError::Unauthenticated(
                "revoked".to_owned(),
            ))
        });
        let (status, body) = send(&router, Method::POST, URI, Some(TOKEN), MINIMAL).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(error_code(&body), "unauthorized");
        assert!(!logs.text().contains("WARN"), "a refusal is not a failure");
    }

    #[tokio::test]
    async fn an_unavailable_broker_is_a_503_and_is_logged() {
        let (logs, _guard) = Logs::capture();
        let router = router_over(|| {
            Err(mansio_core::BrokerError::Unavailable(
                "no answer".to_owned(),
            ))
        });
        let (status, _) = send(&router, Method::POST, URI, Some(TOKEN), MINIMAL).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let logged = logs.text();
        for expected in ["WARN", "status=503", "no answer"] {
            assert!(logged.contains(expected), "{expected} is not in {logged}");
        }
    }

    #[tokio::test]
    async fn a_failing_sink_is_logged_without_the_envelope() {
        let (logs, _guard) = Logs::capture();
        let sink = Arc::new(ScriptedSink {
            seen: Default::default(),
            answer: Err(()),
        });
        let router = router_with(sink);
        let body = r#"{"source":"s","type":"t","data":"private-payload"}"#;
        let (status, _) = send(&router, Method::POST, URI, Some(TOKEN), body).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let logged = logs.text();
        for expected in ["WARN", "tenant=acme", "topic=facts.build", "disk full"] {
            assert!(logged.contains(expected), "{expected} is not in {logged}");
        }
        assert!(!logged.contains("private-payload"), "{logged}");
    }

    #[tokio::test]
    async fn a_missing_topic_is_a_404_and_is_not_logged_as_a_failure() {
        let (logs, _guard) = Logs::capture();
        let router = router_over(|| Err(mansio_core::BrokerError::NotFound("topic".to_owned())));
        let (status, _) = send(&router, Method::POST, URI, Some(TOKEN), MINIMAL).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(!logs.text().contains("WARN"), "{}", logs.text());
    }
}
