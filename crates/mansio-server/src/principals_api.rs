//! The admin API for credentials: `principals_create`, `principals_list` and
//! `principals_revoke`. Only the admin credential may call them.

use std::sync::Arc;

use axum::Json;
use axum::extract::rejection::{BytesRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use bytes::Bytes;
use mansio_core::api::{CreatePrincipal, Issued, Principals};
use mansio_core::{PrincipalInfo, RevokeReason, TenantId};
use serde::Deserialize;

use crate::app::{AppState, parse_body, parse_part, read_body};
use crate::auth::Authenticated;
use crate::error::ApiError;

/// `POST /v1/principals`: issue a credential. 201 with the principal and the
/// credential, which is shown this once.
pub(crate) async fn principals_create(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<Issued>), ApiError> {
    actor.require_admin()?;
    let request: CreatePrincipal = parse_body(&read_body(&state, body)?)?;
    let issued = state
        .credentials
        .create(actor.principal(), request)
        .await
        .map_err(|error| error.logged_in("principals_create"))?;
    Ok((StatusCode::CREATED, Json(issued)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListQuery {
    tenant: Option<TenantId>,
}

/// `GET /v1/principals?tenant=`: the principals that have a credential, of
/// one tenant or of all.
pub(crate) async fn principals_list(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Json<Principals>, ApiError> {
    actor.require_admin()?;
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let principals = state
        .credentials
        .list(query.tenant.as_ref())
        .await
        .map_err(|error| error.logged_in("principals_list"))?;
    Ok(Json(Principals { principals }))
}

/// `DELETE /v1/principals/{id}`: withdraw a credential. 200 with the principal
/// it belonged to.
pub(crate) async fn principals_revoke(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<PrincipalInfo>, ApiError> {
    actor.require_admin()?;
    let Path(id) = path.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let revoked = state
        .credentials
        .revoke(actor.principal(), parse_part(&id)?, RevokeReason::Requested)
        .await
        .map_err(|error| error.logged_in("principals_revoke"))?;
    Ok(Json(revoked))
}
