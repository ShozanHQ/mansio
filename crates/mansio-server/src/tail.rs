//! Reading a topic page by page from an offset: the one reader that the event
//! stream, webhooks and the rebuild of the engine's state all use, which steps
//! over a message that cannot be read instead of stopping at it.

use std::sync::Arc;

use mansio_core::{Broker, BrokerError, Caller, Stored, TenantId, Topic};
use tracing::warn;

/// A position in a topic, and the means to read on from it.
pub(crate) struct Tail {
    broker: Arc<dyn Broker>,
    tenant: TenantId,
    topic: Topic,
    /// The offset the next read starts at.
    next: u64,
    /// How many messages one read asks for.
    page: u32,
    /// Messages before this offset are read one at a time, because the page
    /// that failed has a message in it that cannot be read.
    one_by_one_until: u64,
}

impl Tail {
    /// A tail of `topic` that starts at offset `from` and reads `page`
    /// messages at a time.
    pub(crate) fn new(
        broker: Arc<dyn Broker>,
        tenant: TenantId,
        topic: Topic,
        from: u64,
        page: u32,
    ) -> Self {
        Self {
            broker,
            tenant,
            topic,
            next: from,
            page: page.max(1),
            one_by_one_until: 0,
        }
    }

    /// The offset the next read starts at.
    pub(crate) fn next(&self) -> u64 {
        self.next
    }

    /// The next messages, in publish order, as `caller`; none if it has caught
    /// up. A stored message that is not an envelope is skipped with a warning
    /// that says where it is, so that one bad message does not stop the tail
    /// for good.
    ///
    /// # Errors
    ///
    /// Returns what the broker returns, `NotFound` for a topic that does not
    /// exist (yet) among it. The tail stays where it was.
    pub(crate) async fn read(&mut self, caller: &Caller<'_>) -> Result<Vec<Stored>, BrokerError> {
        loop {
            let width = if self.next < self.one_by_one_until {
                1
            } else {
                self.page
            };
            match self
                .broker
                .poll(caller, &self.tenant, &self.topic, self.next, width)
                .await
            {
                Ok(batch) => {
                    if let Some(last) = batch.last() {
                        self.next = self.next.max(last.offset.saturating_add(1));
                    }
                    return Ok(batch);
                }
                Err(BrokerError::Corrupt(_)) if width > 1 => {
                    self.one_by_one_until = self.next.saturating_add(u64::from(self.page));
                }
                Err(BrokerError::Corrupt(why)) => {
                    warn!(
                        tenant = %self.tenant,
                        topic = %self.topic,
                        offset = self.next,
                        %why,
                        "a message that cannot be read is skipped"
                    );
                    self.next = self.next.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use mansio_core::{Envelope, EventId, MemoryBroker};

    use super::*;

    /// A memory broker one of whose messages cannot be read, so that a poll
    /// that covers it fails, as one does when a stored message is not an
    /// envelope.
    struct OneBadMessage {
        inner: MemoryBroker,
        offset: u64,
    }

    #[async_trait]
    impl Broker for OneBadMessage {
        async fn tenants(&self) -> Result<Vec<TenantId>, BrokerError> {
            self.inner.tenants().await
        }

        async fn topics(&self, tenant: &TenantId) -> Result<Vec<Topic>, BrokerError> {
            self.inner.topics(tenant).await
        }

        async fn ensure_tenant(&self, tenant: &TenantId) -> Result<(), BrokerError> {
            self.inner.ensure_tenant(tenant).await
        }

        async fn ensure_topic(&self, tenant: &TenantId, topic: &Topic) -> Result<(), BrokerError> {
            self.inner.ensure_topic(tenant, topic).await
        }

        async fn publish(
            &self,
            caller: &Caller<'_>,
            envelope: &Envelope,
        ) -> Result<Option<u64>, BrokerError> {
            self.inner.publish(caller, envelope).await
        }

        async fn poll(
            &self,
            caller: &Caller<'_>,
            tenant: &TenantId,
            topic: &Topic,
            offset: u64,
            max: u32,
        ) -> Result<Vec<Stored>, BrokerError> {
            if offset <= self.offset && self.offset < offset + u64::from(max) {
                return Err(BrokerError::Corrupt("not an envelope".to_owned()));
            }
            self.inner.poll(caller, tenant, topic, offset, max).await
        }
    }

    fn tenant() -> TenantId {
        "acme".parse().unwrap()
    }

    fn topic() -> Topic {
        "facts.build".parse().unwrap()
    }

    async fn broker_with(count: u64) -> Arc<MemoryBroker> {
        let broker = Arc::new(MemoryBroker::new());
        broker.ensure_tenant(&tenant()).await.unwrap();
        broker.ensure_topic(&tenant(), &topic()).await.unwrap();
        for n in 0..count {
            let envelope = Envelope::new(
                EventId::generate(),
                "test".parse().unwrap(),
                format!("t{n}").parse().unwrap(),
                tenant(),
                topic(),
                "service:test".parse().unwrap(),
            );
            broker.publish(&Caller::Admin, &envelope).await.unwrap();
        }
        broker
    }

    fn offsets(batch: &[Stored]) -> Vec<u64> {
        batch.iter().map(|stored| stored.offset).collect()
    }

    #[tokio::test]
    async fn a_tail_reads_on_from_where_it_started_page_by_page() {
        let broker = broker_with(5).await;
        let mut tail = Tail::new(broker.clone(), tenant(), topic(), 1, 2);
        assert_eq!(offsets(&tail.read(&Caller::Admin).await.unwrap()), [1, 2]);
        assert_eq!(offsets(&tail.read(&Caller::Admin).await.unwrap()), [3, 4]);
        assert!(tail.read(&Caller::Admin).await.unwrap().is_empty());

        // What is published later is read next, and nothing twice.
        let late = Envelope::new(
            EventId::generate(),
            "test".parse().unwrap(),
            "late".parse().unwrap(),
            tenant(),
            topic(),
            "service:test".parse().unwrap(),
        );
        broker.publish(&Caller::Admin, &late).await.unwrap();
        assert_eq!(offsets(&tail.read(&Caller::Admin).await.unwrap()), [5]);
        assert!(tail.read(&Caller::Admin).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_message_that_cannot_be_read_is_stepped_over_and_the_rest_is_read() {
        let inner = Arc::try_unwrap(broker_with(7).await).unwrap();
        let broker = Arc::new(OneBadMessage { inner, offset: 3 });
        let mut tail = Tail::new(broker, tenant(), topic(), 0, 4);
        let mut seen = Vec::new();
        for _ in 0..20 {
            seen.extend(offsets(&tail.read(&Caller::Admin).await.unwrap()));
        }
        assert_eq!(seen, [0, 1, 2, 4, 5, 6]);
    }

    #[tokio::test]
    async fn a_tail_of_a_topic_that_is_not_there_yet_says_so_and_stays_put() {
        let broker = Arc::new(MemoryBroker::new());
        let mut tail = Tail::new(broker.clone(), tenant(), topic(), 0, 10);
        assert!(matches!(
            tail.read(&Caller::Admin).await,
            Err(BrokerError::NotFound(_))
        ));
        broker.ensure_tenant(&tenant()).await.unwrap();
        broker.ensure_topic(&tenant(), &topic()).await.unwrap();
        assert!(tail.read(&Caller::Admin).await.unwrap().is_empty());
    }
}
