//! One log line per request, and the request metrics.
//!
//! The line is written when the answer is ready, at `info`, with the method,
//! the route pattern (never the path, which holds ids), the tenant the path
//! or the query names, the status, the duration, and the kind and name of the
//! principal the credential stands for. It holds no header and so no token.
//! A request that did not present a valid credential has `-` for the
//! principal. For an event stream the duration is the time to the start of
//! the answer, not the life of the stream.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::http::Uri;
use axum::middleware::Next;
use axum::response::Response;
use mansio_core::{Principal, TenantId};
use tracing::info;

use crate::app::AppState;

/// What a request learns about itself on the way to its answer. The
/// extractor of the credential fills in the principal.
#[derive(Debug, Default)]
pub(crate) struct Seen(Mutex<Option<Principal>>);

impl Seen {
    pub(crate) fn principal(&self, principal: &Principal) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(principal.clone());
    }

    fn take(&self) -> Option<Principal> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

/// The route label of a request no route matched.
const UNMATCHED: &str = "unmatched";

/// The tenant a request names: the one in `/v1/tenants/{tenant}/...`, or the
/// `tenant` of the query. Only a value that parses as a tenant is a tenant.
fn tenant_of(uri: &Uri) -> Option<TenantId> {
    let mut parts = uri.path().split('/').skip(1);
    if parts.next() == Some("v1") && parts.next() == Some("tenants") {
        return parts.next().and_then(|tenant| tenant.parse().ok());
    }
    uri.query()?
        .split('&')
        .find_map(|pair| pair.strip_prefix("tenant="))
        .and_then(|tenant| tenant.parse().ok())
}

/// Wrap a request: time it, and when it is answered log it and count it.
pub(crate) async fn observe(
    State(state): State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let method = request.method().as_str().to_owned();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or(UNMATCHED, MatchedPath::as_str)
        .to_owned();
    let tenant = tenant_of(request.uri());
    let seen = Arc::new(Seen::default());
    request.extensions_mut().insert(Arc::clone(&seen));

    let response = next.run(request).await;

    let took = started.elapsed();
    let status = response.status().as_u16();
    state.metrics.request(&method, &route, status, took);
    let (kind, name) = seen.take().map_or_else(
        || ("-".to_owned(), "-".to_owned()),
        |principal| (principal.kind.to_string(), principal.name.to_string()),
    );
    info!(
        method = %method,
        route = %route,
        tenant = %tenant.as_ref().map_or("-", TenantId::as_str),
        status,
        duration_ms = (took.as_secs_f64() * 1_000_000.0).round() / 1000.0,
        principal_kind = %kind,
        principal_name = %name,
        "request"
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(uri: &str) -> Option<String> {
        tenant_of(&uri.parse().unwrap()).map(|tenant| tenant.to_string())
    }

    #[test]
    fn the_tenant_comes_from_the_path_or_the_query_and_only_when_it_is_one() {
        assert_eq!(
            tenant("/v1/tenants/acme/topics/x/messages").as_deref(),
            Some("acme")
        );
        assert_eq!(
            tenant("/v1/events?tenant=acme&topic=x").as_deref(),
            Some("acme")
        );
        assert_eq!(tenant("/v1/webhooks?tenant=acme").as_deref(), Some("acme"));
        assert_eq!(tenant("/healthz"), None);
        assert_eq!(tenant("/v1/tenants/NOT%20A%20TENANT/alerts"), None);
        assert_eq!(tenant("/v1/events?tenant=bad%0Aline"), None);
    }
}
