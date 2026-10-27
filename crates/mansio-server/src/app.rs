//! The HTTP application: routes, handlers, and the `Server` builder a host
//! application uses to run it in-process.

use std::future::Future;
use std::io;
use std::sync::Arc;

use axum::extract::rejection::{BytesRejection, PathRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use mansio_core::api::{Polled, Published};
use mansio_core::{
    Broker, Envelope, EventId, NoSink, ParseError, Principal, Sink, TenantId, Timestamp, Topic,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::auth::{Auth, Authenticated};
use crate::error::ApiError;
use crate::settings::Limits;

/// Attributes the server sets itself, from the path and the credential. A
/// body that carries one is refused.
const SERVER_SET: [&str; 4] = ["tenant", "topic", "principalkind", "principalname"];

/// Everything a handler needs.
pub struct AppState {
    pub(crate) broker: Arc<dyn Broker>,
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) auth: Auth,
    pub(crate) limits: Limits,
}

/// The mansio HTTP server, ready to run.
///
/// A host application builds one with its own [`Broker`] and [`Sink`] and
/// calls [`Server::serve`], or takes the [`Router`] and mounts it itself.
pub struct Server {
    state: AppState,
}

impl Server {
    /// A server over `broker` that accepts `auth`, records nothing in a sink
    /// and uses the default limits.
    #[must_use]
    pub fn new(broker: Arc<dyn Broker>, auth: Auth) -> Self {
        Self {
            state: AppState {
                broker,
                sink: Arc::new(NoSink),
                auth,
                limits: Limits::default(),
            },
        }
    }

    /// Record every durable publish in `sink` first.
    #[must_use]
    pub fn with_sink(mut self, sink: Arc<dyn Sink>) -> Self {
        self.state.sink = sink;
        self
    }

    /// Use these limits instead of the defaults.
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.state.limits = limits;
        self
    }

    /// The routes, as a [`Router`].
    pub fn router(self) -> Router {
        let max_body = self.state.limits.max_body_bytes;
        Router::new()
            .route("/healthz", get(healthz))
            .route(
                "/v1/tenants/{tenant}/topics/{topic}/messages",
                get(poll).post(publish),
            )
            .fallback(not_found)
            .layer(DefaultBodyLimit::max(max_body))
            .with_state(Arc::new(self.state))
    }

    /// Serve on `listener` until `shutdown` completes.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener fails.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> io::Result<()> {
        axum::serve(listener, self.router())
            .with_graceful_shutdown(shutdown)
            .await
    }
}

async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn not_found() -> ApiError {
    ApiError::NotFound("no such route".to_owned())
}

fn parse_target(
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<(TenantId, Topic), ApiError> {
    let Path((tenant, topic)) = path.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let tenant = tenant
        .parse()
        .map_err(|e: ParseError| ApiError::BadRequest(e.to_string()))?;
    let topic = topic
        .parse()
        .map_err(|e: ParseError| ApiError::BadRequest(e.to_string()))?;
    Ok((tenant, topic))
}

/// Make the envelope to store from a request body.
///
/// The body is an envelope without the attributes the server sets. `id`,
/// `time` and `specversion` are filled in when absent. Everything else is
/// checked by the envelope's own rules.
fn build_envelope(
    body: &[u8],
    tenant: &TenantId,
    topic: &Topic,
    principal: &Principal,
) -> Result<Envelope, ApiError> {
    let bad = |message: String| ApiError::BadRequest(message);
    let Value::Object(mut attributes) =
        serde_json::from_slice::<Value>(body).map_err(|e| bad(format!("body is not JSON: {e}")))?
    else {
        return Err(bad("body must be a JSON object".to_owned()));
    };

    let present: Vec<&str> = SERVER_SET
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
        let now = Timestamp::now().map_err(|_| ApiError::Internal)?;
        attributes.insert("time".to_owned(), now.to_string().into());
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
    Authenticated(principal): Authenticated,
    path: Result<Path<(String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<Published>), ApiError> {
    let (tenant, topic) = parse_target(path)?;
    let body = body.map_err(|e| match e.status() {
        StatusCode::PAYLOAD_TOO_LARGE => ApiError::PayloadTooLarge(format!(
            "body is larger than {} bytes",
            state.limits.max_body_bytes
        )),
        _ => ApiError::BadRequest(e.body_text()),
    })?;
    let mut envelope = build_envelope(&body, &tenant, &topic, &principal)?;

    state.broker.ensure_tenant(&tenant).await?;
    state.broker.ensure_topic(&tenant, &topic).await?;
    if topic.is_durable()
        && let Some(record) = state.sink.record(&envelope).await?
    {
        envelope.record = Some(record);
    }
    let offset = state.broker.publish(&envelope).await?;
    Ok((StatusCode::CREATED, Json(Published { offset, envelope })))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PollQuery {
    offset: Option<u64>,
    count: Option<u32>,
}

async fn poll(
    State(state): State<Arc<AppState>>,
    _auth: Authenticated,
    path: Result<Path<(String, String)>, PathRejection>,
    query: Result<Query<PollQuery>, QueryRejection>,
) -> Result<Json<Polled>, ApiError> {
    let (tenant, topic) = parse_target(path)?;
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

    state.broker.ensure_tenant(&tenant).await?;
    state.broker.ensure_topic(&tenant, &topic).await?;
    let messages = state
        .broker
        .poll(&tenant, &topic, query.offset.unwrap_or(0), count)
        .await?;
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
        assert_eq!(published.offset, 0);
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
    async fn polling_an_empty_topic_returns_nothing() {
        let (status, body) = send(
            &router(),
            Method::GET,
            "/v1/tenants/acme/topics/alerts/messages",
            Some(TOKEN),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"messages": []}));
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
}
