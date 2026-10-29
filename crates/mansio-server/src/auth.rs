//! Who a request is from.
//!
//! The bootstrap token (`MANSIO_TOKEN`) is the admin's credential and stands
//! for one principal in every tenant. Any other bearer is the credential of a
//! principal the admin API issued it to, and is checked with the broker.
//! Identity comes from the credential and from nowhere in the request.

use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use mansio_core::{
    Action, Caller, Credential, Grant, Principal, PrincipalId, TenantId, Topic, allows,
};
use secrecy::{ExposeSecret, SecretString};
use subtle::ConstantTimeEq;

use crate::app::AppState;
use crate::error::ApiError;

/// The admin's credential.
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

    /// The principal the token stands for.
    #[must_use]
    pub fn principal(&self) -> &Principal {
        &self.principal
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

/// Who is acting, and what that allows.
#[derive(Debug, Clone)]
pub struct Actor {
    principal: Principal,
    access: Access,
}

#[derive(Debug, Clone)]
enum Access {
    /// The bootstrap token: every tenant, every topic, and the admin API.
    Admin,
    /// A principal's credential: one tenant, and what its scopes allow.
    Scoped(Arc<Scoped>),
}

#[derive(Debug)]
struct Scoped {
    id: PrincipalId,
    tenant: TenantId,
    grants: Vec<Grant>,
    credential: Credential,
}

impl Actor {
    /// The holder of the bootstrap token, acting as `principal`.
    pub(crate) fn admin(principal: Principal) -> Self {
        Self {
            principal,
            access: Access::Admin,
        }
    }

    /// The holder of a principal's credential.
    pub(crate) fn scoped(
        id: PrincipalId,
        principal: Principal,
        tenant: TenantId,
        grants: Vec<Grant>,
        credential: Credential,
    ) -> Self {
        Self {
            principal,
            access: Access::Scoped(Arc::new(Scoped {
                id,
                tenant,
                grants,
                credential,
            })),
        }
    }

    /// The `kind:name` that stands for the actor on what it publishes and in
    /// the leases it holds.
    #[must_use]
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Whether the actor is the admin.
    #[must_use]
    pub fn is_admin(&self) -> bool {
        matches!(self.access, Access::Admin)
    }

    /// The id of the principal the credential was issued to. The admin has
    /// none.
    #[must_use]
    pub fn id(&self) -> Option<PrincipalId> {
        match &self.access {
            Access::Admin => None,
            Access::Scoped(scoped) => Some(scoped.id),
        }
    }

    /// Whom the broker acts for when the request reads or writes.
    #[must_use]
    pub fn caller(&self) -> Caller<'_> {
        match &self.access {
            Access::Admin => Caller::Admin,
            Access::Scoped(scoped) => Caller::Credential(&scoped.credential),
        }
    }

    /// Refuse the request with 403 unless the actor is the admin.
    pub(crate) fn require_admin(&self) -> Result<(), ApiError> {
        if self.is_admin() {
            Ok(())
        } else {
            Err(ApiError::Forbidden(
                "only the admin credential may do this".to_owned(),
            ))
        }
    }

    /// Refuse the request with 403 unless the actor may do this to the topic.
    /// This runs before anything is recorded or created; the broker checks
    /// again on every read and write made for a credential.
    pub(crate) fn authorize(
        &self,
        action: Action,
        tenant: &TenantId,
        topic: &Topic,
    ) -> Result<(), ApiError> {
        let Access::Scoped(scoped) = &self.access else {
            return Ok(());
        };
        if scoped.tenant == *tenant && allows(&scoped.grants, action, topic) {
            return Ok(());
        }
        let what = match action {
            Action::Send => "send to",
            Action::Poll => "read",
            Action::Claim => "claim from",
            Action::Ack => "acknowledge on",
        };
        Err(ApiError::Forbidden(format!(
            "the credential may not {what} {topic} in {tenant}"
        )))
    }
}

/// The actor behind a request's bearer token. Extracting it refuses the
/// request with 401 otherwise.
#[derive(Debug, Clone)]
pub struct Authenticated(pub Actor);

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
        if let Some(principal) = state.auth.principal_for(presented) {
            return Ok(Authenticated(Actor::admin(principal.clone())));
        }
        state
            .credentials
            .authenticate(presented)
            .await
            .map(Authenticated)
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

    fn scoped(grants: &[(&str, &[mansio_core::Verb])]) -> Actor {
        let grants = grants
            .iter()
            .map(|(pattern, verbs)| {
                Grant::new(pattern.parse().unwrap(), verbs.iter().copied()).unwrap()
            })
            .collect();
        Actor::scoped(
            PrincipalId::generate(),
            "agent:one".parse().unwrap(),
            "acme".parse().unwrap(),
            grants,
            Credential::new("tok3n"),
        )
    }

    #[test]
    fn the_admin_may_do_everything_and_a_credential_only_what_its_scopes_allow() {
        use mansio_core::Verb;
        let admin = Actor::admin("service:mansio".parse().unwrap());
        let tenant: TenantId = "acme".parse().unwrap();
        let other: TenantId = "other".parse().unwrap();
        let topic: Topic = "work.builds".parse().unwrap();
        assert!(admin.authorize(Action::Send, &other, &topic).is_ok());
        assert!(admin.require_admin().is_ok());
        assert!(matches!(admin.caller(), Caller::Admin));
        assert_eq!(admin.id(), None);

        let agent = scoped(&[("work.builds", &[Verb::Claim])]);
        assert!(agent.authorize(Action::Claim, &tenant, &topic).is_ok());
        assert!(agent.authorize(Action::Poll, &tenant, &topic).is_ok());
        for refused in [
            agent.authorize(Action::Send, &tenant, &topic),
            agent.authorize(Action::Claim, &other, &topic),
            agent.authorize(Action::Claim, &tenant, &"work.reviews".parse().unwrap()),
            agent.authorize(Action::Ack, &tenant, &Topic::Alerts),
            agent.require_admin(),
        ] {
            assert!(
                matches!(refused, Err(ApiError::Forbidden(_))),
                "{refused:?}"
            );
        }
        assert!(matches!(agent.caller(), Caller::Credential(_)));
        assert!(agent.id().is_some());
    }

    #[test]
    fn an_actor_never_shows_its_credential() {
        let agent = scoped(&[]);
        assert!(!format!("{agent:?}").contains("tok3n"));
    }
}
