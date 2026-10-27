//! The HTTP handlers for work items and alerts: `work_publish`, `work_pull`,
//! `work_extend`, `work_ack`, `work_fail`, `alerts_publish` and `alerts_ack`.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::rejection::{BytesRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use mansio_core::api::{AckRequest, ExtendRequest, FailRequest, Published, PullRequest};
use mansio_core::{Action, BrokerError, ConsumerName, ItemId, LeaseId, Segment, TenantId, Topic};

use crate::app::{AppState, Origin, build_envelope, parse_body, parse_part, read_body};
use crate::auth::{Actor, Authenticated};
use crate::engine::Holder;
use crate::error::ApiError;

/// Who the request says holds the lease.
fn holder<'a>(actor: &'a Actor, consumer: &'a ConsumerName, lease_id: &'a LeaseId) -> Holder<'a> {
    Holder {
        principal: actor.principal(),
        consumer,
        lease_id,
    }
}

fn path_error(rejection: &PathRejection) -> ApiError {
    ApiError::BadRequest(rejection.body_text())
}

/// `POST /v1/tenants/{tenant}/work/{queue}/items`: publish a work item. The
/// body is an envelope without the attributes the server sets; the item is
/// named by its `id` and starts at attempt 1.
pub(crate) async fn work_publish(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<Published>), ApiError> {
    let Path((tenant, queue)) = path.map_err(|e| path_error(&e))?;
    let (tenant, queue) = (parse_part(&tenant)?, parse_part(&queue)?);
    let topic = Topic::Work(queue);
    actor.authorize(Action::Send, &tenant, &topic)?;
    let body = read_body(&state, body)?;
    let now = state.engine.now()?;
    let envelope = build_envelope(
        &body,
        &tenant,
        &topic,
        actor.principal(),
        &now,
        Origin::WorkItem,
    )?;
    let published = state
        .engine
        .work_publish(envelope)
        .await
        .map_err(|error| error.logged(&tenant, &topic))?;
    Ok((StatusCode::CREATED, Json(published)))
}

/// `POST /v1/tenants/{tenant}/work/{queue}/pull`: claim the oldest claimable
/// item. 200 with the item and its lease, or 204 if there is nothing to claim.
///
/// The caller's own session reads the queue first, so the broker decides
/// whether the credential may take from it before the engine hands out a
/// lease. A queue that does not exist has nothing to claim.
pub(crate) async fn work_pull(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let Path((tenant, queue)) = path.map_err(|e| path_error(&e))?;
    let (tenant, queue): (TenantId, Segment) = (parse_part(&tenant)?, parse_part(&queue)?);
    let topic = Topic::Work(queue.clone());
    actor.authorize(Action::Claim, &tenant, &topic)?;
    let request: PullRequest = parse_body(&read_body(&state, body)?)?;
    if !actor.is_admin() {
        match state
            .broker
            .poll(&actor.caller(), &tenant, &topic, 0, 0)
            .await
        {
            Ok(_) => {}
            Err(BrokerError::NotFound(_)) => return Ok(StatusCode::NO_CONTENT.into_response()),
            Err(error) => return Err(ApiError::from(error).logged(&tenant, &topic)),
        }
    }
    let claimed = state
        .engine
        .work_pull(&tenant, &queue, actor.principal(), &request.consumer)
        .await
        .map_err(|error| error.logged(&tenant, &Topic::Work(queue.clone())))?;
    Ok(match claimed {
        Some(claimed) => Json(claimed).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

/// `POST /v1/tenants/{tenant}/work/{queue}/items/{item}/extend`: move the end
/// of the holder's lease later. 200 with the `extended` fact, whose
/// `data.expires_at` is the new end.
pub(crate) async fn work_extend(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<Published>, ApiError> {
    let Path((tenant, queue, item)) = path.map_err(|e| path_error(&e))?;
    let (tenant, queue, item): (TenantId, Segment, ItemId) = (
        parse_part(&tenant)?,
        parse_part(&queue)?,
        parse_part(&item)?,
    );
    actor.authorize(Action::Claim, &tenant, &Topic::Work(queue.clone()))?;
    let request: ExtendRequest = parse_body(&read_body(&state, body)?)?;
    if request.seconds == Some(0) {
        return Err(ApiError::BadRequest(
            "seconds must be at least 1".to_owned(),
        ));
    }
    let published = state
        .engine
        .work_extend(
            &tenant,
            &queue,
            &item,
            holder(&actor, &request.consumer, &request.lease_id),
            request.seconds.map(Duration::from_secs),
        )
        .await
        .map_err(|error| error.logged(&tenant, &Topic::Work(queue.clone())))?;
    Ok(Json(published))
}

/// `POST /v1/tenants/{tenant}/work/{queue}/items/{item}/ack`.
pub(crate) async fn work_ack(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<Published>, ApiError> {
    let Path((tenant, queue, item)) = path.map_err(|e| path_error(&e))?;
    let (tenant, queue, item): (TenantId, Segment, ItemId) = (
        parse_part(&tenant)?,
        parse_part(&queue)?,
        parse_part(&item)?,
    );
    actor.authorize(Action::Claim, &tenant, &Topic::Work(queue.clone()))?;
    let request: AckRequest = parse_body(&read_body(&state, body)?)?;
    let published = state
        .engine
        .work_ack(
            &tenant,
            &queue,
            &item,
            holder(&actor, &request.consumer, &request.lease_id),
        )
        .await
        .map_err(|error| error.logged(&tenant, &Topic::Work(queue.clone())))?;
    Ok(Json(published))
}

/// `POST /v1/tenants/{tenant}/work/{queue}/items/{item}/fail`.
pub(crate) async fn work_fail(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<Published>, ApiError> {
    let Path((tenant, queue, item)) = path.map_err(|e| path_error(&e))?;
    let (tenant, queue, item): (TenantId, Segment, ItemId) = (
        parse_part(&tenant)?,
        parse_part(&queue)?,
        parse_part(&item)?,
    );
    actor.authorize(Action::Claim, &tenant, &Topic::Work(queue.clone()))?;
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
            holder(&actor, &request.consumer, &request.lease_id),
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
    Authenticated(actor): Authenticated,
    path: Result<Path<String>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<Published>), ApiError> {
    let Path(tenant) = path.map_err(|e| path_error(&e))?;
    let tenant = parse_part(&tenant)?;
    actor.authorize(Action::Send, &tenant, &Topic::Alerts)?;
    let body = read_body(&state, body)?;
    let now = state.engine.now()?;
    let envelope = build_envelope(
        &body,
        &tenant,
        &Topic::Alerts,
        actor.principal(),
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
/// The principal is the one behind the credential, which needs the `ack` verb
/// on `alerts`.
pub(crate) async fn alerts_ack(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<Json<Published>, ApiError> {
    let Path((tenant, alert)) = path.map_err(|e| path_error(&e))?;
    let (tenant, alert) = (parse_part(&tenant)?, parse_part(&alert)?);
    actor.authorize(Action::Ack, &tenant, &Topic::Alerts)?;
    let published = state
        .engine
        .alerts_ack(&tenant, alert, actor.principal())
        .await
        .map_err(|error| error.logged(&tenant, &Topic::facts_alert()))?;
    Ok(Json(published))
}
