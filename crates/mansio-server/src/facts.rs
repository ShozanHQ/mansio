//! Making the envelopes the engine publishes: the transitions on
//! `facts.work` and `facts.alert`, and the copies of an item it republishes.

use std::str::FromStr;

use mansio_core::{
    AlertEvent, AlertFact, CausationId, DataContentType, Envelope, EventId, EventType, ItemId,
    Principal, PrincipalEvent, PrincipalFact, Segment, Source, Subject, TenantId, Timestamp, Topic,
    WebhookEvent, WebhookFact, WorkEvent, WorkFact,
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

/// Mixed into a message's id to make the id of the fact that is owed for it.
const OWED_SALT: u128 = 0x6d61_6e73_696f_2d6f_7765_6420_6661_6374;

/// Mixed into a webhook's id and a message's to make the id of the fact that
/// the webhook gave up on the message.
const WEBHOOK_FAILED_SALT: u128 = 0x6d61_6e73_696f_2d77_6562_686f_6f6b_2166;

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

    /// The fact owed for a message the engine has published: `retried` for
    /// the next attempt of an item, or `dead` for its copy on `dead`.
    ///
    /// It is made from the message alone: its id is the message's id with a
    /// fixed pattern mixed in, and its time is the message's time. So the
    /// engine that publishes the message and a restarted one that finds it
    /// without its fact make the same fact. If the first recorded it in the
    /// sink before it stopped, the second records the same id again, and a
    /// consumer that deduplicates on `id` sees one fact.
    pub(crate) fn owed(
        &self,
        tenant: &TenantId,
        event: WorkEvent,
        queue: &Segment,
        message: &Envelope,
        previous: Option<EventId>,
    ) -> Result<Envelope, ApiError> {
        let item = message.item_id.clone().ok_or(ApiError::Internal)?;
        let attempt = message.attempt.unwrap_or(1);
        let mut data = WorkFact::new(queue.clone(), message.id, attempt);
        data.previous_message_id = previous;
        let at = message
            .time
            .as_ref()
            .and_then(Timestamp::unix_millis)
            .unwrap_or(0);
        let mut fact = self.work(tenant, event, &self.engine, at, &item, &data)?;
        fact.time.clone_from(&message.time);
        fact.id = EventId::from_u128(message.id.as_u128() ^ OWED_SALT)
            .unwrap_or_else(|_| EventId::generate());
        Ok(fact)
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

    /// A change to a principal, on `facts.principal`, with the principal id
    /// as its subject. The data names no secret: the credential is not part of
    /// a principal.
    pub(crate) fn principal(
        &self,
        event: PrincipalEvent,
        by: &Principal,
        at: i64,
        data: &PrincipalFact,
    ) -> Result<Envelope, ApiError> {
        let value = serde_json::to_value(data).map_err(|reason| {
            error!(%reason, "a principal fact did not serialize");
            ApiError::Internal
        })?;
        let mut envelope = self.envelope(
            &data.principal.tenant,
            Topic::facts_principal(),
            event.event_type(),
            by,
            at,
            &data.principal.id.to_string(),
        )?;
        envelope.data = Some(value);
        Ok(envelope)
    }

    /// A change to a webhook, on `facts.webhook`, with the webhook id as its
    /// subject. The engine is who made it: nobody asked.
    ///
    /// Its id is made from the webhook's and the message's, as an owed fact's
    /// is from its message's: a webhook that gives up on a message, is stopped
    /// before the fact is stored, and gives up on it again after a restart makes
    /// the very same fact, which a consumer that deduplicates on `id` sees
    /// once.
    pub(crate) fn webhook(
        &self,
        tenant: &TenantId,
        event: WebhookEvent,
        at: i64,
        data: &WebhookFact,
    ) -> Result<Envelope, ApiError> {
        let value = serde_json::to_value(data).map_err(|reason| {
            error!(%reason, "a webhook fact did not serialize");
            ApiError::Internal
        })?;
        let mut envelope = self.envelope(
            tenant,
            Topic::facts_webhook(),
            event.event_type(),
            &self.engine,
            at,
            &data.webhook_id.to_string(),
        )?;
        envelope.data = Some(value);
        envelope.id = EventId::from_u128(
            data.webhook_id.as_u128() ^ data.message_id.as_u128() ^ WEBHOOK_FAILED_SALT,
        )
        .unwrap_or_else(|_| EventId::generate());
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
    fn a_principal_fact_names_the_principal_and_who_made_the_change() {
        let facts = Facts::new();
        let info = mansio_core::PrincipalInfo {
            id: mansio_core::PrincipalId::generate(),
            kind: "agent".parse().unwrap(),
            name: "builder-1".parse().unwrap(),
            tenant: "acme".parse().unwrap(),
            scopes: vec![
                mansio_core::Grant::new("work.builds".parse().unwrap(), [mansio_core::Verb::Claim])
                    .unwrap(),
            ],
            created_at: "2026-10-26T12:00:00Z".parse().unwrap(),
            expires_at: None,
        };
        let by: Principal = "service:mansio".parse().unwrap();
        let data = PrincipalFact {
            principal: info.clone(),
            reason: None,
        };
        let fact = facts
            .principal(PrincipalEvent::Created, &by, 0, &data)
            .unwrap();
        assert_eq!(fact.topic.to_string(), "facts.principal");
        assert_eq!(fact.tenant, info.tenant);
        assert_eq!(fact.event_type.as_str(), "mansio.principal.created");
        assert_eq!(fact.subject.as_ref().unwrap().as_str(), info.id.to_string());
        assert_eq!(fact.principal, by);
        let back: PrincipalFact = serde_json::from_value(fact.data.unwrap()).unwrap();
        assert_eq!(back, data);
    }

    #[test]
    fn a_webhook_fact_names_the_webhook_and_what_it_gave_up_on() {
        let facts = Facts::new();
        let tenant: TenantId = "acme".parse().unwrap();
        let data = WebhookFact {
            webhook_id: mansio_core::WebhookId::generate(),
            topic: "facts.build".parse().unwrap(),
            offset: 7,
            message_id: EventId::generate(),
            attempts: 5,
            error: "HTTP 500".to_owned(),
        };
        let fact = facts
            .webhook(&tenant, WebhookEvent::Failed, 0, &data)
            .unwrap();
        assert_eq!(fact.topic.to_string(), "facts.webhook");
        assert_eq!(fact.event_type.as_str(), "mansio.webhook.failed");
        assert_eq!(
            fact.subject.as_ref().unwrap().as_str(),
            data.webhook_id.to_string()
        );
        assert_eq!(fact.principal.to_string(), ENGINE_PRINCIPAL);
        let back: WebhookFact = serde_json::from_value(fact.data.clone().unwrap()).unwrap();
        assert_eq!(back, data);

        // Giving up on the same message again makes the same fact, and another
        // message or another webhook makes another.
        let again = facts
            .webhook(&tenant, WebhookEvent::Failed, 5_000, &data)
            .unwrap();
        assert_eq!(again.id, fact.id);
        let other_message = WebhookFact {
            message_id: EventId::generate(),
            ..data.clone()
        };
        let other_webhook = WebhookFact {
            webhook_id: mansio_core::WebhookId::generate(),
            ..data
        };
        for other in [other_message, other_webhook] {
            let made = facts
                .webhook(&tenant, WebhookEvent::Failed, 0, &other)
                .unwrap();
            assert_ne!(made.id, fact.id);
        }
    }

    #[test]
    fn an_owed_fact_is_made_from_its_message_alone() {
        let facts = Facts::new();
        let tenant: TenantId = "acme".parse().unwrap();
        let queue: Segment = "builds".parse().unwrap();
        let current = work_message();
        let next = facts.retry_message(&current, 1_792_000_000_123).unwrap();
        let owed = |facts: &Facts| {
            facts
                .owed(&tenant, WorkEvent::Retried, &queue, &next, Some(current.id))
                .unwrap()
        };
        let (first, second) = (owed(&facts), owed(&Facts::new()));
        assert_eq!(first, second, "the same message always owes the same fact");
        assert_ne!(first.id, next.id);
        assert_eq!(first.time, next.time);
        assert_eq!(first.attempt, next.attempt);
        assert_eq!(first.item_id, next.item_id);
        assert_eq!(first.event_type.as_str(), "mansio.work.retried");
        assert_eq!(
            first.data.as_ref().unwrap()["message_id"],
            next.id.to_string()
        );
        assert_eq!(
            first.data.as_ref().unwrap()["previous_message_id"],
            current.id.to_string()
        );
        // And another message owes another fact.
        let other = facts.retry_message(&current, 1_792_000_000_123).unwrap();
        let other_fact = facts
            .owed(
                &tenant,
                WorkEvent::Retried,
                &queue,
                &other,
                Some(current.id),
            )
            .unwrap();
        assert_ne!(other_fact.id, first.id);
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
