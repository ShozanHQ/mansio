//! `GET /v1/events`: a topic as server-sent events.
//!
//! Each event's `id` is the offset of the message, its `event` the envelope's
//! `type` and its `data` the envelope as CloudEvents JSON. A client that
//! reconnects sends `Last-Event-ID` and gets the messages after it, so
//! nothing is missed and nothing is sent twice. `?from=` starts a first
//! connection at an offset, and `Last-Event-ID` wins when both are given.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream;
use futures_util::{Stream, StreamExt};
use mansio_core::{Action, Stored, TenantId, Topic};
use serde::Deserialize;
use tracing::warn;

use crate::app::AppState;
use crate::auth::{Actor, Authenticated};
use crate::error::ApiError;
use crate::tail::Tail;

/// The header a reconnecting client sends: the `id` of the last event it saw.
const LAST_EVENT_ID: &str = "last-event-id";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EventsQuery {
    tenant: TenantId,
    topic: Topic,
    /// The offset to start at: the first event sent is this one.
    from: Option<u64>,
}

/// Where the stream starts: after the last event the client saw, else at
/// `from`, else at the first message.
fn start(query: &EventsQuery, headers: &HeaderMap) -> Result<u64, ApiError> {
    let Some(value) = headers.get(LAST_EVENT_ID) else {
        return Ok(query.from.unwrap_or(0));
    };
    let last: u64 = value
        .to_str()
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .ok_or_else(|| {
            ApiError::BadRequest("Last-Event-ID must be the offset of an event".to_owned())
        })?;
    Ok(last.saturating_add(1))
}

/// One envelope as an event, or `None` if it cannot be written.
fn event(stored: &Stored) -> Option<Event> {
    let data = serde_json::to_string(&stored.envelope).ok()?;
    Some(
        Event::default()
            .id(stored.offset.to_string())
            .event(stored.envelope.event_type.as_str())
            .data(data),
    )
}

/// What the stream carries between events.
struct Following {
    tail: Tail,
    actor: Actor,
    poll: std::time::Duration,
    waiting: VecDeque<Stored>,
}

fn events(following: Following) -> impl Stream<Item = Result<Event, Infallible>> {
    stream::unfold(following, |mut following| async move {
        loop {
            if let Some(stored) = following.waiting.pop_front() {
                let Some(event) = event(&stored) else {
                    warn!(
                        offset = stored.offset,
                        "an envelope could not be written as an event"
                    );
                    return None;
                };
                return Some((Ok(event), following));
            }
            match following.tail.read(&following.actor.caller()).await {
                Ok(batch) if batch.is_empty() => tokio::time::sleep(following.poll).await,
                Ok(batch) => following.waiting.extend(batch),
                // The client reconnects with `Last-Event-ID` and is told
                // why, if it is not allowed any more.
                Err(error) => {
                    warn!(%error, "an event stream ended: the topic could not be read");
                    return None;
                }
            }
        }
    })
}

/// `GET /v1/events?tenant=&topic=[&from=]`: serve the topic as
/// `text/event-stream`. The caller needs the right to read the topic, as for
/// `poll`; it is checked before the first byte is sent, and the broker holds
/// the credential to it on every read after.
pub(crate) async fn events_stream(
    State(state): State<Arc<AppState>>,
    Authenticated(actor): Authenticated,
    headers: HeaderMap,
    query: Result<Query<EventsQuery>, QueryRejection>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    actor.authorize(Action::Poll, &query.tenant, &query.topic)?;
    let from = start(&query, &headers)?;
    let mut tail = Tail::new(
        Arc::clone(&state.broker),
        query.tenant.clone(),
        query.topic.clone(),
        from,
        state.limits.poll_default_count,
    );
    // The first read decides whether there is a stream to send: a topic that
    // does not exist, or one the credential may not read, is an answer, not
    // an event stream that ends.
    let first = tail
        .read(&actor.caller())
        .await
        .map_err(|error| ApiError::from(error).logged(&query.tenant, &query.topic))?;
    let following = Following {
        tail,
        actor,
        poll: state.streams.poll,
        waiting: first.into(),
    };
    // A server that is shutting down ends the stream, so that a client that is
    // connected does not hold the shutdown up.
    let mut ending = state.ending.clone();
    let ended = async move {
        // A host that dropped its `Ender` ends nothing.
        if ending.wait_for(|ending| *ending).await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    Ok(Sse::new(events(following).take_until(ended)).keep_alive(
        KeepAlive::new()
            .interval(state.streams.sse_keepalive)
            .text("keep-alive"),
    ))
}
