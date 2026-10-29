//! An in-memory broker, for tests, development and `MANSIO_BROKER=memory`.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;

use crate::broker::{Broker, Stored};
use crate::envelope::Envelope;
use crate::error::BrokerError;
use crate::id::TenantId;
use crate::topic::Topic;

type Topics = HashMap<Topic, Vec<Envelope>>;

/// A [`Broker`] that keeps everything in memory and forgets it on drop.
#[derive(Debug, Default)]
pub struct MemoryBroker {
    tenants: Mutex<HashMap<TenantId, Topics>>,
}

impl MemoryBroker {
    /// An empty broker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<TenantId, Topics>> {
        self.tenants.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn missing_topic(tenant: &TenantId, topic: &Topic) -> BrokerError {
    BrokerError::NotFound(format!("topic {topic} in tenant {tenant}"))
}

#[async_trait]
impl Broker for MemoryBroker {
    async fn tenants(&self) -> Result<Vec<TenantId>, BrokerError> {
        Ok(self.lock().keys().cloned().collect())
    }

    async fn topics(&self, tenant: &TenantId) -> Result<Vec<Topic>, BrokerError> {
        self.lock()
            .get(tenant)
            .map(|topics| topics.keys().cloned().collect())
            .ok_or_else(|| BrokerError::NotFound(format!("tenant {tenant}")))
    }

    async fn ensure_tenant(&self, tenant: &TenantId) -> Result<(), BrokerError> {
        self.lock().entry(tenant.clone()).or_default();
        Ok(())
    }

    async fn ensure_topic(&self, tenant: &TenantId, topic: &Topic) -> Result<(), BrokerError> {
        let mut tenants = self.lock();
        let topics = tenants
            .get_mut(tenant)
            .ok_or_else(|| BrokerError::NotFound(format!("tenant {tenant}")))?;
        topics.entry(topic.clone()).or_default();
        Ok(())
    }

    async fn publish(&self, envelope: &Envelope) -> Result<Option<u64>, BrokerError> {
        envelope.validate()?;
        let mut tenants = self.lock();
        let log = tenants
            .get_mut(&envelope.tenant)
            .and_then(|topics| topics.get_mut(&envelope.topic))
            .ok_or_else(|| missing_topic(&envelope.tenant, &envelope.topic))?;
        log.push(envelope.clone());
        Ok(Some(u64::try_from(log.len() - 1).unwrap_or(u64::MAX)))
    }

    async fn poll(
        &self,
        tenant: &TenantId,
        topic: &Topic,
        offset: u64,
        max: u32,
    ) -> Result<Vec<Stored>, BrokerError> {
        let tenants = self.lock();
        let log = tenants
            .get(tenant)
            .and_then(|topics| topics.get(topic))
            .ok_or_else(|| missing_topic(tenant, topic))?;
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let stored = log
            .iter()
            .enumerate()
            .skip(start)
            .take(usize::try_from(max).unwrap_or(usize::MAX))
            .map(|(index, envelope)| Stored {
                offset: u64::try_from(index).unwrap_or(u64::MAX),
                envelope: envelope.clone(),
            })
            .collect();
        Ok(stored)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{EventId, Principal};

    fn envelope(tenant: &str, topic: &str, kind: &str) -> Envelope {
        Envelope::new(
            EventId::generate(),
            "test".parse().unwrap(),
            kind.parse().unwrap(),
            tenant.parse().unwrap(),
            topic.parse().unwrap(),
            "service:test".parse::<Principal>().unwrap(),
        )
    }

    async fn broker_with(tenant: &str, topic: &str) -> MemoryBroker {
        let broker = MemoryBroker::new();
        broker
            .ensure_tenant(&tenant.parse().unwrap())
            .await
            .unwrap();
        broker
            .ensure_topic(&tenant.parse().unwrap(), &topic.parse().unwrap())
            .await
            .unwrap();
        broker
    }

    #[tokio::test]
    async fn publish_then_poll_returns_the_same_envelope() {
        let broker = broker_with("acme", "facts.build").await;
        let sent = envelope("acme", "facts.build", "build.finished");
        let offset = broker.publish(&sent).await.unwrap();
        assert_eq!(offset, Some(0));

        let tenant: TenantId = "acme".parse().unwrap();
        let topic: Topic = "facts.build".parse().unwrap();
        let got = broker.poll(&tenant, &topic, 0, 10).await.unwrap();
        assert_eq!(
            got,
            vec![Stored {
                offset: 0,
                envelope: sent
            }]
        );
    }

    #[tokio::test]
    async fn offsets_follow_publish_order_and_poll_pages() {
        let broker = broker_with("acme", "alerts").await;
        for n in 0..5 {
            let offset = broker
                .publish(&envelope("acme", "alerts", &format!("t{n}")))
                .await
                .unwrap();
            assert_eq!(offset, Some(n));
        }
        let tenant: TenantId = "acme".parse().unwrap();
        let page = broker.poll(&tenant, &Topic::Alerts, 1, 2).await.unwrap();
        let offsets: Vec<u64> = page.iter().map(|s| s.offset).collect();
        assert_eq!(offsets, [1, 2]);
        assert_eq!(page[0].envelope.event_type.as_str(), "t1");

        assert!(
            broker
                .poll(&tenant, &Topic::Alerts, 5, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            broker
                .poll(&tenant, &Topic::Alerts, u64::MAX, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            broker
                .poll(&tenant, &Topic::Alerts, 0, 0)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn tenants_and_topics_are_isolated() {
        let broker = broker_with("acme", "alerts").await;
        broker
            .ensure_tenant(&"other".parse().unwrap())
            .await
            .unwrap();
        broker
            .ensure_topic(&"other".parse().unwrap(), &Topic::Alerts)
            .await
            .unwrap();
        broker
            .publish(&envelope("acme", "alerts", "t"))
            .await
            .unwrap();

        let other: TenantId = "other".parse().unwrap();
        assert!(
            broker
                .poll(&other, &Topic::Alerts, 0, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn missing_tenants_and_topics_are_not_found() {
        let broker = MemoryBroker::new();
        let tenant: TenantId = "acme".parse().unwrap();

        let error = broker
            .ensure_topic(&tenant, &Topic::Dead)
            .await
            .unwrap_err();
        assert!(matches!(error, BrokerError::NotFound(_)));
        let error = broker
            .publish(&envelope("acme", "dead", "t"))
            .await
            .unwrap_err();
        assert!(matches!(error, BrokerError::NotFound(_)));
        let error = broker.poll(&tenant, &Topic::Dead, 0, 1).await.unwrap_err();
        assert!(matches!(error, BrokerError::NotFound(_)));

        broker.ensure_tenant(&tenant).await.unwrap();
        let error = broker.poll(&tenant, &Topic::Dead, 0, 1).await.unwrap_err();
        assert!(matches!(error, BrokerError::NotFound(_)));
    }

    #[tokio::test]
    async fn tenants_and_topics_can_be_listed() {
        let broker = broker_with("acme", "alerts").await;
        broker
            .ensure_topic(&"acme".parse().unwrap(), &Topic::Dead)
            .await
            .unwrap();
        broker
            .ensure_tenant(&"other".parse().unwrap())
            .await
            .unwrap();

        let mut tenants = broker.tenants().await.unwrap();
        tenants.sort();
        assert_eq!(tenants, ["acme".parse().unwrap(), "other".parse().unwrap()]);
        let mut topics: Vec<String> = broker
            .topics(&"acme".parse().unwrap())
            .await
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect();
        topics.sort();
        assert_eq!(topics, ["alerts", "dead"]);
        assert!(
            broker
                .topics(&"other".parse().unwrap())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            broker.topics(&"nope".parse().unwrap()).await,
            Err(BrokerError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn ensure_is_idempotent_and_keeps_messages() {
        let broker = broker_with("acme", "presence").await;
        broker
            .publish(&envelope("acme", "presence", "t"))
            .await
            .unwrap();
        let tenant: TenantId = "acme".parse().unwrap();
        broker.ensure_tenant(&tenant).await.unwrap();
        broker
            .ensure_topic(&tenant, &Topic::Presence)
            .await
            .unwrap();
        assert_eq!(
            broker
                .poll(&tenant, &Topic::Presence, 0, 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
