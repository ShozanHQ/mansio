//! The admin API for webhooks: `webhooks_create`, `webhooks_list` and
//! `webhooks_delete`. Only the admin credential may call them.

use std::sync::Arc;

use axum::Json;
use axum::extract::rejection::{BytesRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use bytes::Bytes;
use mansio_core::api::{CreateWebhook, WebhookCreated, Webhooks};
use mansio_core::{TenantId, Webhook};
use serde::Deserialize;

use crate::app::{AppState, parse_body, parse_part, read_body};
use crate::auth::Authenticated;
use crate::error::ApiError;

/// `POST /v1/webhooks`: make a webhook. 201 with the webhook and its signing
/// secret, which is shown this once.
pub(crate) async fn webhooks_create(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<WebhookCreated>), ApiError> {
    actor.require_admin()?;
    let request: CreateWebhook = parse_body(&read_body(&state, body)?)?;
    let created = state
        .webhooks
        .create(request)
        .await
        .map_err(|error| error.logged_in("webhooks_create"))?;
    Ok((StatusCode::CREATED, Json(created)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListQuery {
    tenant: Option<TenantId>,
}

/// `GET /v1/webhooks?tenant=`: the webhooks of one tenant or of all, with
/// their last delivered offset and last error. No secret is in it.
pub(crate) async fn webhooks_list(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Json<Webhooks>, ApiError> {
    actor.require_admin()?;
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let webhooks = state
        .webhooks
        .list(query.tenant.as_ref())
        .await
        .map_err(|error| error.logged_in("webhooks_list"))?;
    Ok(Json(Webhooks { webhooks }))
}

/// `DELETE /v1/webhooks/{id}`: delete a webhook. 200 with the webhook.
pub(crate) async fn webhooks_delete(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<Webhook>, ApiError> {
    actor.require_admin()?;
    let Path(id) = path.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let deleted = state
        .webhooks
        .delete(parse_part(&id)?)
        .await
        .map_err(|error| error.logged_in("webhooks_delete"))?;
    Ok(Json(deleted))
}
