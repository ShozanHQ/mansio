//! The HTTP handlers for work items and alerts: `work_publish`, `work_pull`,
//! `work_ack`, `work_fail`, `alerts_publish` and `alerts_ack`.

use std::sync::Arc;

use axum::Json;
use axum::extract::rejection::{BytesRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use mansio_core::Topic;
use mansio_core::api::{AckRequest, FailRequest, Published, PullRequest};

use crate::app::{AppState, Origin, build_envelope, parse_body, parse_part, read_body};
use crate::auth::Authenticated;
use crate::error::ApiError;

fn path_error(rejection: &PathRejection) -> ApiError {
    ApiError::BadRequest(rejection.body_text())
}

/// `POST /v1/tenants/{tenant}/work/{queue}/items`: publish a work item. The
/// body is an envelope without the attributes the server sets; the item is
/// named by its `id` and starts at attempt 1.
pub(crate) async fn work_publish(
    State(state): State<Arc<AppState>>,
    Authenticated(principal): Authenticated,
    path: Result<Path<(String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<Published>), ApiError> {
    let Path((tenant, queue)) = path.map_err(|e| path_error(&e))?;
    let (tenant, queue) = (parse_part(&tenant)?, parse_part(&queue)?);
    let body = read_body(&state, body)?;
    let now = state.engine.now()?;
    let topic = Topic::Work(queue);
    let envelope = build_envelope(&body, &tenant, &topic, &principal, &now, Origin::WorkItem)?;
    let published = state
        .engine
        .work_publish(envelope)
        .await
        .map_err(|error| error.logged(&tenant, &topic))?;
    Ok((StatusCode::CREATED, Json(published)))
}

/// `POST /v1/tenants/{tenant}/work/{queue}/pull`: claim the oldest claimable
/// item. 200 with the item and its lease, or 204 if there is nothing to claim.
pub(crate) async fn work_pull(
    State(state): State<Arc<AppState>>,
    Authenticated(principal): Authenticated,
    path: Result<Path<(String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let Path((tenant, queue)) = path.map_err(|e| path_error(&e))?;
    let (tenant, queue) = (parse_part(&tenant)?, parse_part(&queue)?);
    let request: PullRequest = parse_body(&read_body(&state, body)?)?;
    let claimed = state
        .engine
        .work_pull(&tenant, &queue, &principal, &request.consumer)
        .await
        .map_err(|error| error.logged(&tenant, &Topic::Work(queue.clone())))?;
    Ok(match claimed {
        Some(claimed) => Json(claimed).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

/// `POST /v1/tenants/{tenant}/work/{queue}/items/{item}/ack`.
pub(crate) async fn work_ack(
    State(state): State<Arc<AppState>>,
    Authenticated(principal): Authenticated,
    path: Result<Path<(String, String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<Published>, ApiError> {
    let Path((tenant, queue, item)) = path.map_err(|e| path_error(&e))?;
    let (tenant, queue, item) = (
        parse_part(&tenant)?,
        parse_part(&queue)?,
        parse_part(&item)?,
    );
    let request: AckRequest = parse_body(&read_body(&state, body)?)?;
    let published = state
        .engine
        .work_ack(&tenant, &queue, &item, &principal, &request.lease_id)
        .await
        .map_err(|error| error.logged(&tenant, &Topic::Work(queue.clone())))?;
    Ok(Json(published))
}

/// `POST /v1/tenants/{tenant}/work/{queue}/items/{item}/fail`.
pub(crate) async fn work_fail(
    State(state): State<Arc<AppState>>,
    Authenticated(principal): Authenticated,
    path: Result<Path<(String, String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<Published>, ApiError> {
    let Path((tenant, queue, item)) = path.map_err(|e| path_error(&e))?;
    let (tenant, queue, item) = (
        parse_part(&tenant)?,
        parse_part(&queue)?,
        parse_part(&item)?,
    );
    let request: FailRequest = parse_body(&read_body(&state, body)?)?;
    if request.reason.trim().is_empty() {
        return Err(ApiError::BadRequest("reason must not be empty".to_owned()));
    }
    let published = state
        .engine
        .work_fail(
            &tenant,
            &queue,
            &item,
            &principal,
            &request.lease_id,
            &request.reason,
        )
        .await
        .map_err(|error| error.logged(&tenant, &Topic::Work(queue.clone())))?;
    Ok(Json(published))
}

/// `POST /v1/tenants/{tenant}/alerts`: publish an alert to `alerts`. The body
/// is an envelope without the attributes the server sets.
pub(crate) async fn alerts_publish(
    State(state): State<Arc<AppState>>,
    Authenticated(principal): Authenticated,
    path: Result<Path<String>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<Published>), ApiError> {
    let Path(tenant) = path.map_err(|e| path_error(&e))?;
    let tenant = parse_part(&tenant)?;
    let body = read_body(&state, body)?;
    let now = state.engine.now()?;
    let envelope = build_envelope(
        &body,
        &tenant,
        &Topic::Alerts,
        &principal,
        &now,
        Origin::Message,
    )?;
    let published = state
        .engine
        .publish(envelope)
        .await
        .map_err(|error| error.logged(&tenant, &Topic::Alerts))?;
    Ok((StatusCode::CREATED, Json(published)))
}

/// `POST /v1/tenants/{tenant}/alerts/{alert}/ack`: acknowledge an alert, once.
/// The principal is the one behind the credential.
pub(crate) async fn alerts_ack(
    State(state): State<Arc<AppState>>,
    Authenticated(principal): Authenticated,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<Json<Published>, ApiError> {
    let Path((tenant, alert)) = path.map_err(|e| path_error(&e))?;
    let (tenant, alert) = (parse_part(&tenant)?, parse_part(&alert)?);
    let published = state
        .engine
        .alerts_ack(&tenant, alert, &principal)
        .await
        .map_err(|error| error.logged(&tenant, &Topic::facts_alert()))?;
    Ok(Json(published))
}
