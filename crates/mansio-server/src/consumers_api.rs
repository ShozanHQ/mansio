//! The handlers for named durable consumers: `consumers_get` and
//! `consumers_commit`. The broker keeps the offset, so it survives a restart
//! of the server, and holds the caller to its right to read the topic.

use std::sync::Arc;

use axum::Json;
use axum::extract::rejection::{BytesRejection, PathRejection};
use axum::extract::{Path, State};
use bytes::Bytes;
use mansio_core::api::{CommitRequest, ConsumerState};
use mansio_core::{Action, ConsumerName, TenantId, Topic};

use crate::app::{AppState, parse_body, parse_part, read_body};
use crate::auth::Authenticated;
use crate::error::ApiError;

type Target = (TenantId, Topic, ConsumerName);

fn target(path: Result<Path<(String, String, String)>, PathRejection>) -> Result<Target, ApiError> {
    let Path((tenant, topic, consumer)) =
        path.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    Ok((
        parse_part(&tenant)?,
        parse_part(&topic)?,
        parse_part(&consumer)?,
    ))
}

/// `GET /v1/tenants/{tenant}/topics/{topic}/consumers/{consumer}`: where the
/// consumer stands. A consumer that has committed nothing reads from 0.
pub(crate) async fn consumers_get(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String, String)>, PathRejection>,
) -> Result<Json<ConsumerState>, ApiError> {
    let (tenant, topic, consumer) = target(path)?;
    actor.authorize(Action::Poll, &tenant, &topic)?;
    let committed = state
        .broker
        .consumer_offset(&actor.caller(), &tenant, &topic, &consumer)
        .await
        .map_err(|error| ApiError::from(error).logged(&tenant, &topic))?;
    Ok(Json(ConsumerState::new(consumer, committed)))
}

/// `POST /v1/tenants/{tenant}/topics/{topic}/consumers/{consumer}/commit`:
/// store the offset of the last message the consumer has handled. Its next
/// read starts one after it. Any offset may be stored, so a consumer can be
/// set back.
pub(crate) async fn consumers_commit(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ConsumerState>, ApiError> {
    let (tenant, topic, consumer) = target(path)?;
    actor.authorize(Action::Poll, &tenant, &topic)?;
    let request: CommitRequest = parse_body(&read_body(&state, body)?)?;
    state
        .broker
        .commit_offset(&actor.caller(), &tenant, &topic, &consumer, request.offset)
        .await
        .map_err(|error| ApiError::from(error).logged(&tenant, &topic))?;
    Ok(Json(ConsumerState::new(consumer, Some(request.offset))))
}
