//! What the API says when it refuses.

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use mansio_core::api::{ErrorBody, ErrorDetail};
use mansio_core::{BrokerError, SinkError};
use tracing::error;

/// A refusal, with the status it maps to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    /// 400: the request is malformed or breaks a rule.
    BadRequest(String),
    /// 401: no valid credential.
    Unauthorized,
    /// 413: the body is larger than `MANSIO_MAX_BODY_BYTES`.
    PayloadTooLarge(String),
    /// 404: no such route, tenant or topic.
    NotFound(String),
    /// 502: the broker refused.
    BadGateway(String),
    /// 503: the broker or the sink is not available.
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
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
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
            ApiError::NotFound(_) => "not_found",
            ApiError::BadGateway(_) => "broker_refused",
            ApiError::Unavailable(_) => "unavailable",
            ApiError::Internal => "internal",
        }
    }

    fn message(&self) -> String {
        match self {
            ApiError::BadRequest(m)
            | ApiError::PayloadTooLarge(m)
            | ApiError::NotFound(m)
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
