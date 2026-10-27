//! The bearer token and the principal it stands for.
//!
//! One token maps to one principal. Per-principal credentials replace this
//! in a later change.

use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use mansio_core::Principal;
use secrecy::{ExposeSecret, SecretString};
use subtle::ConstantTimeEq;

use crate::app::AppState;
use crate::error::ApiError;

/// The credential the server accepts.
#[derive(Debug, Clone)]
pub struct Auth {
    token: SecretString,
    principal: Principal,
}

impl Auth {
    /// Accept `token`, and treat whoever presents it as `principal`.
    #[must_use]
    pub fn new(token: SecretString, principal: Principal) -> Self {
        Self { token, principal }
    }

    /// The principal for a presented token, if it is the right one. The
    /// comparison takes the same time wherever the tokens differ.
    fn principal_for(&self, presented: &str) -> Option<&Principal> {
        let same = presented
            .as_bytes()
            .ct_eq(self.token.expose_secret().as_bytes());
        bool::from(same).then_some(&self.principal)
    }
}

/// The principal behind a request's bearer token. Extracting it refuses the
/// request with 401 otherwise.
#[derive(Debug, Clone)]
pub struct Authenticated(pub Principal);

fn bearer(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then_some(token.trim_start())
}

impl FromRequestParts<Arc<AppState>> for Authenticated {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, ApiError> {
        let presented = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(bearer)
            .ok_or(ApiError::Unauthorized)?;
        state
            .auth
            .principal_for(presented)
            .cloned()
            .map(Authenticated)
            .ok_or(ApiError::Unauthorized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth() -> Auth {
        Auth::new(
            SecretString::from("s3cret".to_owned()),
            "service:mansio".parse().unwrap(),
        )
    }

    #[test]
    fn only_the_exact_token_is_accepted() {
        let auth = auth();
        assert!(auth.principal_for("s3cret").is_some());
        for wrong in ["", "s3cre", "s3crets", "S3CRET", "s3cret "] {
            assert!(auth.principal_for(wrong).is_none(), "{wrong:?}");
        }
    }

    #[test]
    fn bearer_scheme_is_case_insensitive() {
        assert_eq!(bearer("Bearer abc"), Some("abc"));
        assert_eq!(bearer("bearer abc"), Some("abc"));
        assert_eq!(bearer("BEARER   abc"), Some("abc"));
        assert_eq!(bearer("Basic abc"), None);
        assert_eq!(bearer("abc"), None);
        assert_eq!(bearer(""), None);
    }

    #[test]
    fn debug_does_not_show_the_token() {
        assert!(!format!("{:?}", auth()).contains("s3cret"));
    }
}
