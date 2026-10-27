//! What the API says when it refuses.

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use mansio_core::api::{ErrorBody, ErrorDetail};
use mansio_core::{BrokerError, SinkError, TenantId, Topic};
use tracing::{error, warn};

/// A refusal, with the status it maps to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    /// 400: the request is malformed or breaks a rule.
    BadRequest(String),
    /// 401: no valid credential.
    Unauthorized,
    /// 413: the body is larger than `MANSIO_MAX_BODY_BYTES`.
    PayloadTooLarge(String),
    /// 403: the caller does not hold what it presented, for example a lease.
    Forbidden(String),
    /// 404: no such route, tenant, topic, item or alert.
    NotFound(String),
    /// 409: the request is well formed but the state it acts on does not
    /// allow it, for example an alert already acknowledged.
    Conflict(String),
    /// 502: the broker named a refusal that it makes before it stores
    /// anything. Nothing was stored.
    BadGateway(String),
    /// 503: the broker or the sink is not available, or the broker's answer
    /// does not say whether it stored the request. A client sends it again
    /// with the same id.
    Unavailable(String),
    /// 500: a fault in the server. The message is generic; the detail is
    /// logged.
    Internal,
}

impl ApiError {
    fn status(&self) -> StatusCode {
        match self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::BadGateway(_) => StatusCode::BAD_GATEWAY,
            ApiError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            ApiError::BadRequest(_) => "bad_request",
            ApiError::Unauthorized => "unauthorized",
            ApiError::PayloadTooLarge(_) => "payload_too_large",
            ApiError::Forbidden(_) => "forbidden",
            ApiError::NotFound(_) => "not_found",
            ApiError::Conflict(_) => "conflict",
            ApiError::BadGateway(_) => "broker_refused",
            ApiError::Unavailable(_) => "unavailable",
            ApiError::Internal => "internal",
        }
    }

    /// Log a 502 or 503 with the tenant and topic it happened for and the
    /// error from the broker or sink, then hand the refusal back unchanged.
    /// The client sees the same message, so nothing here is a secret.
    pub(crate) fn logged(self, tenant: &TenantId, topic: &Topic) -> Self {
        if let ApiError::BadGateway(why) | ApiError::Unavailable(why) = &self {
            warn!(
                %tenant,
                %topic,
                status = self.status().as_u16(),
                error = %why,
                "the broker or the sink failed a request"
            );
        }
        self
    }

    fn message(&self) -> String {
        match self {
            ApiError::BadRequest(m)
            | ApiError::PayloadTooLarge(m)
            | ApiError::Forbidden(m)
            | ApiError::NotFound(m)
            | ApiError::Conflict(m)
            | ApiError::BadGateway(m)
            | ApiError::Unavailable(m) => m.clone(),
            ApiError::Unauthorized => "missing or invalid bearer token".to_owned(),
            ApiError::Internal => "internal error".to_owned(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: ErrorDetail {
                code: self.code().to_owned(),
                message: self.message(),
            },
        };
        let mut response = (self.status(), Json(body)).into_response();
        if self == ApiError::Unauthorized {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}

impl From<BrokerError> for ApiError {
    fn from(error: BrokerError) -> Self {
        match error {
            BrokerError::NotFound(what) => ApiError::NotFound(what),
            BrokerError::Unavailable(why) => ApiError::Unavailable(why),
            BrokerError::Rejected(why) => ApiError::BadGateway(why),
            BrokerError::Corrupt(_) | BrokerError::Envelope(_) => {
                error!(%error, "broker fault");
                ApiError::Internal
            }
        }
    }
}

impl From<SinkError> for ApiError {
    fn from(error: SinkError) -> Self {
        ApiError::Unavailable(error.to_string())
    }
}
