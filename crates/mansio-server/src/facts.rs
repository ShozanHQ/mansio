//! Making the envelopes the engine publishes: the transitions on
//! `facts.work` and `facts.alert`, and the copies of an item it republishes.

use std::str::FromStr;

use mansio_core::{
    AlertEvent, AlertFact, CausationId, DataContentType, Envelope, EventId, EventType, ItemId,
    Principal, Source, Subject, TenantId, Timestamp, Topic, WorkEvent, WorkFact,
};
use tracing::error;

use crate::error::ApiError;

/// A value the engine writes itself must parse; if it does not, that is a
/// fault in the server and not in the request.
fn own<T: FromStr>(what: &str, text: &str) -> Result<T, ApiError>
where
    T::Err: std::fmt::Display,
{
    text.parse().map_err(|reason| {
        error!(what, text, %reason, "the engine made an invalid value");
        ApiError::Internal
    })
}

fn time(millis: i64) -> Result<Timestamp, ApiError> {
    Timestamp::from_unix_millis(millis).map_err(|reason| {
        error!(millis, %reason, "the clock is out of range");
        ApiError::Internal
    })
}

/// Who the engine is when it acts without a caller: expiring a lease,
/// republishing, surfacing an alert again.
pub(crate) const ENGINE_PRINCIPAL: &str = "service:mansio-engine";

/// The `source` of every fact the engine publishes.
pub(crate) const SOURCE: &str = "mansio";

/// Makes the engine's envelopes.
#[derive(Debug)]
pub(crate) struct Facts {
    source: Source,
    engine: Principal,
}

impl Facts {
    #[expect(clippy::expect_used, reason = "two constants that a test parses")]
    pub(crate) fn new() -> Self {
        Self {
            source: SOURCE.parse().expect("SOURCE is a valid source"),
            engine: ENGINE_PRINCIPAL
                .parse()
                .expect("ENGINE_PRINCIPAL is a valid principal"),
        }
    }

    /// The principal for transitions nobody asked for.
    pub(crate) fn engine(&self) -> &Principal {
        &self.engine
    }

    fn envelope(
        &self,
        tenant: &TenantId,
        topic: Topic,
        event_type: &str,
        principal: &Principal,
        at: i64,
        subject: &str,
    ) -> Result<Envelope, ApiError> {
        let mut envelope = Envelope::new(
            EventId::generate(),
            self.source.clone(),
            own::<EventType>("event type", event_type)?,
            tenant.clone(),
            topic,
            principal.clone(),
        );
        envelope.subject = Some(own::<Subject>("subject", subject)?);
        envelope.time = Some(time(at)?);
        envelope.datacontenttype = Some(DataContentType::Json);
        Ok(envelope)
    }

    /// A transition of a work item, on `facts.work`. It carries the item id
    /// and the attempt as attributes too, so a consumer can follow one item
    /// without reading `data`.
    pub(crate) fn work(
        &self,
        tenant: &TenantId,
        event: WorkEvent,
        principal: &Principal,
        at: i64,
        item: &ItemId,
        data: &WorkFact,
    ) -> Result<Envelope, ApiError> {
        let value = serde_json::to_value(data).map_err(|reason| {
            error!(%reason, "a work fact did not serialize");
            ApiError::Internal
        })?;
        let mut envelope = self.envelope(
            tenant,
            Topic::facts_work(),
            event.event_type(),
            principal,
            at,
            item.as_str(),
        )?;
        envelope.data = Some(value);
        envelope.item_id = Some(item.clone());
        envelope.attempt = Some(data.attempt);
        Ok(envelope)
    }

    /// A transition of an alert, on `facts.alert`.
    pub(crate) fn alert(
        &self,
        tenant: &TenantId,
        event: AlertEvent,
        principal: &Principal,
        at: i64,
        data: &AlertFact,
    ) -> Result<Envelope, ApiError> {
        let value = serde_json::to_value(data).map_err(|reason| {
            error!(%reason, "an alert fact did not serialize");
            ApiError::Internal
        })?;
        let mut envelope = self.envelope(
            tenant,
            Topic::facts_alert(),
            event.event_type(),
            principal,
            at,
            &data.alert_id.to_string(),
        )?;
        envelope.data = Some(value);
        Ok(envelope)
    }

    /// The next attempt of an item: the same message with a new id, one more
    /// attempt and the old message as its cause, published by the engine.
    pub(crate) fn retry_message(&self, current: &Envelope, at: i64) -> Result<Envelope, ApiError> {
        let mut next = current.clone();
        next.id = EventId::generate();
        next.time = Some(time(at)?);
        next.causation_id = Some(own::<CausationId>("causation id", &current.id.to_string())?);
        next.principal = self.engine.clone();
        next.record = None;
        next.attempt = Some(current.attempt.unwrap_or(1).saturating_add(1));
        Ok(next)
    }

    /// The item as it goes to `dead`: a new message of the same attempt, the
    /// last one made, with the old message as its cause.
    pub(crate) fn dead_message(&self, current: &Envelope, at: i64) -> Result<Envelope, ApiError> {
        let mut dead = current.clone();
        dead.id = EventId::generate();
        dead.time = Some(time(at)?);
        dead.topic = Topic::Dead;
        dead.causation_id = Some(own::<CausationId>("causation id", &current.id.to_string())?);
        dead.principal = self.engine.clone();
        dead.record = None;
        Ok(dead)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn work_message() -> Envelope {
        let mut message = Envelope::new(
            EventId::generate(),
            "https://example.com/ci".parse().unwrap(),
            "build.requested".parse().unwrap(),
            "acme".parse().unwrap(),
            "work.builds".parse().unwrap(),
            "agent:planner".parse().unwrap(),
        );
        message.item_id = Some("item-1".parse().unwrap());
        message.attempt = Some(2);
        message.correlation_id = Some("run-9".parse().unwrap());
        message.data = Some(serde_json::json!({"sha": "abc"}));
        message.record = Some(mansio_core::Record {
            id: "rec-1".parse().unwrap(),
            prev_hash: "a".repeat(64).parse().unwrap(),
            event_hash: "b".repeat(64).parse().unwrap(),
        });
        message
    }

    #[test]
    fn the_constants_parse() {
        let facts = Facts::new();
        assert_eq!(facts.engine().to_string(), ENGINE_PRINCIPAL);
    }

    #[test]
    fn a_retry_is_the_same_item_one_attempt_on() {
        let facts = Facts::new();
        let current = work_message();
        let next = facts.retry_message(&current, 1_792_000_000_000).unwrap();
        assert_ne!(next.id, current.id);
        assert_eq!(next.attempt, Some(3));
        assert_eq!(next.item_id, current.item_id);
        assert_eq!(
            next.causation_id.as_ref().unwrap().as_str(),
            current.id.to_string()
        );
        assert_eq!(next.topic, current.topic);
        assert_eq!(next.data, current.data);
        assert_eq!(next.correlation_id, current.correlation_id);
        assert_eq!(next.source, current.source);
        assert_eq!(next.principal.to_string(), ENGINE_PRINCIPAL);
        assert_eq!(next.record, None);
        assert!(next.to_json_vec().is_ok());
    }

    #[test]
    fn a_dead_item_keeps_its_attempt_and_leaves_the_queue() {
        let facts = Facts::new();
        let current = work_message();
        let dead = facts.dead_message(&current, 0).unwrap();
        assert_eq!(dead.topic, Topic::Dead);
        assert_eq!(dead.attempt, current.attempt);
        assert_eq!(dead.item_id, current.item_id);
        assert_eq!(
            dead.causation_id.as_ref().unwrap().as_str(),
            current.id.to_string()
        );
        assert!(dead.to_json_vec().is_ok());
    }

    #[test]
    fn a_work_fact_names_the_item_and_the_attempt() {
        let facts = Facts::new();
        let item: ItemId = "item-1".parse().unwrap();
        let data = WorkFact::new("builds".parse().unwrap(), EventId::generate(), 2);
        let tenant: TenantId = "acme".parse().unwrap();
        let principal: Principal = "agent:one".parse().unwrap();
        let fact = facts
            .work(&tenant, WorkEvent::Claimed, &principal, 0, &item, &data)
            .unwrap();
        assert_eq!(fact.topic.to_string(), "facts.work");
        assert_eq!(fact.event_type.as_str(), "mansio.work.claimed");
        assert_eq!(fact.subject.as_ref().unwrap().as_str(), "item-1");
        assert_eq!(fact.item_id, Some(item));
        assert_eq!(fact.attempt, Some(2));
        assert_eq!(fact.principal, principal);
        assert_eq!(fact.time.unwrap().as_str(), "1970-01-01T00:00:00Z");
    }
}
