//! The handlers for named durable consumers: `consumers_get` and
//! `consumers_commit`. mansio keeps the positions itself ([`crate::cursors`]),
//! and a consumer belongs to the principal behind the credential, so two
//! principals that use one name have two positions. A credential reads the
//! topic first, which the broker holds it to.

use std::sync::Arc;

use axum::Json;
use axum::extract::rejection::{BytesRejection, PathRejection};
use axum::extract::{Path, State};
use bytes::Bytes;
use mansio_core::api::{CommitRequest, ConsumerState};
use mansio_core::{Action, Caller, ConsumerName, TenantId, Topic};

use crate::app::{AppState, parse_body, parse_part, read_body};
use crate::auth::{Actor, Authenticated};
use crate::cursors::Cursor;
use crate::error::ApiError;

type Target = (TenantId, Topic, ConsumerName);

fn target(path: Result<Path<(String, String, String)>, PathRejection>) -> Result<Target, ApiError> {
    let Path((tenant, topic, consumer)) = path.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    Ok((
        parse_part(&tenant)?,
        parse_part(&topic)?,
        parse_part(&consumer)?,
    ))
}

/// Make the broker check that the caller may read the topic, which also finds
/// out that the topic is there, before anything is asked about a consumer.
async fn may_read(
    state: &AppState,
    actor: &Actor,
    tenant: &TenantId,
    topic: &Topic,
) -> Result<(), ApiError> {
    state
        .broker
        .poll(&actor.caller(), tenant, topic, 0, 0)
        .await
        .map(|_| ())
        .map_err(|error| ApiError::from(error).logged(tenant, topic))
}

/// The caller's own consumer of that name.
fn cursor(actor: &Actor, tenant: TenantId, topic: Topic, consumer: ConsumerName) -> Cursor {
    Cursor::Named {
        tenant,
        topic,
        principal: actor.principal().clone(),
        consumer,
    }
}

/// `GET /v1/tenants/{tenant}/topics/{topic}/consumers/{consumer}`: where the
/// caller's consumer of that name stands. One that has committed nothing reads
/// from 0.
pub(crate) async fn consumers_get(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String, String)>, PathRejection>,
) -> Result<Json<ConsumerState>, ApiError> {
    let (tenant, topic, consumer) = target(path)?;
    actor.authorize(Action::Poll, &tenant, &topic)?;
    may_read(&state, &actor, &tenant, &topic).await?;
    let committed = state
        .cursors
        .get(&cursor(
            &actor,
            tenant.clone(),
            topic.clone(),
            consumer.clone(),
        ))
        .await
        .map_err(|error| error.logged(&tenant, &topic))?;
    Ok(Json(ConsumerState::new(consumer, committed)))
}

/// `POST /v1/tenants/{tenant}/topics/{topic}/consumers/{consumer}/commit`:
/// store the offset of the last message the caller's consumer has handled. Its
/// next read starts one after it. The message must exist, or be one that
/// retention has since deleted: an offset past the last message of the topic
/// is a 409, so a typo cannot make a consumer skip what is published next. An
/// earlier offset sets the consumer back.
pub(crate) async fn consumers_commit(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    path: Result<Path<(String, String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ConsumerState>, ApiError> {
    let (tenant, topic, consumer) = target(path)?;
    actor.authorize(Action::Poll, &tenant, &topic)?;
    let request: CommitRequest = parse_body(&read_body(&state, body)?)?;
    may_read(&state, &actor, &tenant, &topic).await?;
    let found = state
        .broker
        .poll(&Caller::Admin, &tenant, &topic, request.offset, 1)
        .await
        .map_err(|error| ApiError::from(error).logged(&tenant, &topic))?;
    if found.is_empty() {
        return Err(ApiError::Conflict(format!(
            "offset {} is past the last message of {topic}: commit the offset of a message that exists",
            request.offset
        )));
    }
    state
        .cursors
        .set(
            cursor(&actor, tenant.clone(), topic.clone(), consumer.clone()),
            request.offset,
        )
        .await
        .map_err(|error| error.logged(&tenant, &topic))?;
    Ok(Json(ConsumerState::new(consumer, Some(request.offset))))
}
