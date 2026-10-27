//! The engine against a memory broker and a clock moved by hand.

use std::collections::HashSet;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use mansio_core::{MemoryBroker, Record, SinkError, Stored};
use serde_json::{Value, json};

use super::*;
use crate::clock::manual::ManualClock;

const START: i64 = 1_792_000_000_000;

fn settings() -> WorkSettings {
    WorkSettings {
        lease: Duration::from_secs(10),
        max_attempts: 3,
        backoff_base: Duration::from_secs(2),
        backoff_cap: Duration::from_secs(5),
        alert_resurface: Duration::from_secs(30),
        tick: Duration::from_millis(100),
    }
}

fn tenant() -> TenantId {
    "acme".parse().unwrap()
}

fn queue() -> Segment {
    "builds".parse().unwrap()
}

fn agent(name: &str) -> Principal {
    format!("agent:{name}").parse().unwrap()
}

fn consumer(name: &str) -> ConsumerName {
    name.parse().unwrap()
}

/// A broker that refuses publishes to the topics and of the event types it is
/// told to, and otherwise is a memory broker.
#[derive(Debug, Default)]
struct FlakyBroker {
    inner: MemoryBroker,
    failing: StdMutex<HashSet<String>>,
}

impl FlakyBroker {
    fn fail(&self, topic: &str) {
        self.failing.lock().unwrap().insert(topic.to_owned());
    }

    fn heal(&self) {
        self.failing.lock().unwrap().clear();
    }
}

#[async_trait]
impl Broker for FlakyBroker {
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

    async fn publish(&self, envelope: &Envelope) -> Result<Option<u64>, BrokerError> {
        let refused = {
            let failing = self.failing.lock().unwrap();
            failing.contains(&envelope.topic.to_string())
                || failing.contains(envelope.event_type.as_str())
        };
        if refused {
            return Err(BrokerError::Unavailable("injected".to_owned()));
        }
        self.inner.publish(envelope).await
    }

    async fn poll(
        &self,
        tenant: &TenantId,
        topic: &Topic,
        offset: u64,
        max: u32,
    ) -> Result<Vec<Stored>, BrokerError> {
        self.inner.poll(tenant, topic, offset, max).await
    }
}

/// A sink that notes what it was given, in order. With `slow_start` it is slow
/// to answer earlier calls and quick to answer later ones, which reorders
/// concurrent publishes unless something keeps them in line.
#[derive(Debug, Default)]
struct LogSink {
    seen: StdMutex<Vec<(String, EventId)>>,
    calls: AtomicU64,
    slow_start: bool,
}

#[async_trait]
impl Sink for LogSink {
    async fn record(&self, envelope: &Envelope) -> Result<Option<Record>, SinkError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .unwrap()
            .push((envelope.topic.to_string(), envelope.id));
        if self.slow_start {
            // The record is made; the answer is slow. Without a lock, a later
            // publish overtakes an earlier one on its way to the broker.
            tokio::time::sleep(Duration::from_millis(40_u64.saturating_sub(call * 3))).await;
        }
        Ok(None)
    }
}

impl LogSink {
    fn topic(&self, topic: &str) -> Vec<EventId> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _)| name == topic)
            .map(|(_, id)| *id)
            .collect()
    }

    fn all(&self) -> Vec<(String, EventId)> {
        self.seen.lock().unwrap().clone()
    }
}

struct Rig {
    broker: Arc<FlakyBroker>,
    sink: Arc<LogSink>,
    clock: Arc<ManualClock>,
    engine: Arc<Engine>,
}

impl Rig {
    fn new() -> Self {
        Self::over(
            Arc::default(),
            Arc::default(),
            Arc::new(ManualClock::new(START)),
        )
    }

    /// A server on a broker, sink and clock that other servers may have used
    /// before. Nothing is rebuilt until the first request or [`Engine::recover`].
    fn over(broker: Arc<FlakyBroker>, sink: Arc<LogSink>, clock: Arc<ManualClock>) -> Self {
        let engine = Arc::new(Engine::new(
            broker.clone(),
            sink.clone(),
            clock.clone(),
            settings(),
            3,
        ));
        Self {
            broker,
            sink,
            clock,
            engine,
        }
    }

    /// The same broker, sink and clock under a new engine: a restart.
    async fn restart(self) -> Self {
        let Self {
            broker,
            sink,
            clock,
            ..
        } = self;
        let restarted = Self::over(broker, sink, clock);
        restarted.engine.recover().await.unwrap();
        restarted
    }

    async fn pass(&self, seconds: u64) {
        self.clock.advance(Duration::from_secs(seconds));
        self.engine.tick().await;
    }

    async fn publish_item(&self, name: &str) -> ItemId {
        let id = EventId::generate();
        let mut message = Envelope::new(
            id,
            "test".parse().unwrap(),
            "job.requested".parse().unwrap(),
            tenant(),
            Topic::Work(queue()),
            agent("planner"),
        );
        let item: ItemId = id.to_string().parse().unwrap();
        message.item_id = Some(item.clone());
        message.attempt = Some(1);
        message.subject = Some(name.parse().unwrap());
        message.data = Some(json!({"name": name}));
        self.engine.work_publish(message).await.unwrap();
        item
    }

    async fn pull(&self, who: &str) -> Option<Claimed> {
        self.engine
            .work_pull(&tenant(), &queue(), &agent(who), &consumer("w1"))
            .await
            .unwrap()
    }

    async fn ack(&self, who: &str, item: &ItemId, lease: &LeaseId) -> Result<Published, ApiError> {
        self.engine
            .work_ack(&tenant(), &queue(), item, &agent(who), lease)
            .await
    }

    async fn fail(&self, who: &str, item: &ItemId, lease: &LeaseId) -> Result<Published, ApiError> {
        self.engine
            .work_fail(&tenant(), &queue(), item, &agent(who), lease, "it broke")
            .await
    }

    async fn read(&self, topic: &Topic) -> Vec<Envelope> {
        match self.broker.poll(&tenant(), topic, 0, 1000).await {
            Ok(stored) => stored.into_iter().map(|s| s.envelope).collect(),
            Err(BrokerError::NotFound(_)) => Vec::new(),
            Err(error) => panic!("{error}"),
        }
    }

    /// The transitions on `facts.work`, as `claimed`, `expired` and so on.
    async fn transitions(&self) -> Vec<String> {
        self.read(&Topic::facts_work())
            .await
            .iter()
            .map(|fact| fact.event_type.as_str().replace("mansio.work.", ""))
            .collect()
    }
}

fn data(fact: &Envelope) -> Value {
    fact.data.clone().unwrap()
}

#[tokio::test]
async fn a_claim_hands_out_the_oldest_item_and_hides_it_from_everyone_else() {
    let rig = Rig::new();
    let first = rig.publish_item("first").await;
    let second = rig.publish_item("second").await;

    let one = rig.pull("one").await.unwrap();
    assert_eq!(one.item.item_id, Some(first));
    assert_eq!(one.item.attempt, Some(1));
    assert_eq!(
        one.expires_at.unix_millis(),
        Some(START + 10_000),
        "the lease runs for the lease setting"
    );
    let two = rig.pull("two").await.unwrap();
    assert_eq!(two.item.item_id, Some(second));
    assert_ne!(one.lease_id, two.lease_id);
    assert!(rig.pull("three").await.is_none());

    assert_eq!(rig.transitions().await, ["claimed", "claimed"]);
    let facts = rig.read(&Topic::facts_work()).await;
    let claimed = data(&facts[0]);
    assert_eq!(claimed["queue"], "builds");
    assert_eq!(claimed["holder"], "agent:one");
    assert_eq!(claimed["consumer"], "w1");
    assert_eq!(claimed["attempt"], 1);
    assert_eq!(claimed["lease_id"], one.lease_id.to_string());
    assert_eq!(claimed["message_id"], one.item.id.to_string());
    assert_eq!(facts[0].principal, agent("one"));
}

#[tokio::test]
async fn an_ack_by_the_holder_completes_the_item() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull("one").await.unwrap();

    let acked = rig.ack("one", &item, &claim.lease_id).await.unwrap();
    assert_eq!(acked.envelope.event_type.as_str(), "mansio.work.acked");
    assert_eq!(acked.envelope.principal, agent("one"));
    assert_eq!(acked.envelope.item_id, Some(item.clone()));

    rig.pass(100).await;
    assert!(rig.pull("two").await.is_none());
    assert_eq!(rig.transitions().await, ["claimed", "acked"]);
    assert_eq!(rig.read(&Topic::Work(queue())).await.len(), 1);
    assert!(rig.read(&Topic::Dead).await.is_empty());
}

#[tokio::test]
async fn only_the_holder_with_the_lease_id_can_ack_or_fail_and_a_refusal_changes_nothing() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull("one").await.unwrap();
    let before = rig.sink.all().len();

    let stranger = LeaseId::generate();
    assert!(matches!(
        rig.ack("one", &item, &stranger).await,
        Err(ApiError::Forbidden(_))
    ));
    assert!(matches!(
        rig.fail("one", &item, &stranger).await,
        Err(ApiError::Forbidden(_))
    ));
    // Someone else with the right lease id.
    assert!(matches!(
        rig.ack("two", &item, &claim.lease_id).await,
        Err(ApiError::Forbidden(_))
    ));
    let unknown: ItemId = "no-such-item".parse().unwrap();
    assert!(matches!(
        rig.ack("one", &unknown, &claim.lease_id).await,
        Err(ApiError::NotFound(_))
    ));
    assert_eq!(rig.sink.all().len(), before);
    assert_eq!(rig.transitions().await, ["claimed"]);

    // The holder is unharmed, until the lease ends; then even it is refused,
    // before the timer has noticed.
    rig.clock.advance(Duration::from_secs(10));
    assert!(matches!(
        rig.ack("one", &item, &claim.lease_id).await,
        Err(ApiError::Conflict(_))
    ));
    assert!(matches!(
        rig.fail("one", &item, &claim.lease_id).await,
        Err(ApiError::Conflict(_))
    ));
    assert_eq!(rig.transitions().await, ["claimed"]);

    // After the timer has, and the item is claimed by someone else.
    rig.engine.tick().await;
    rig.pass(2).await;
    let second = rig.pull("two").await.unwrap();
    assert!(matches!(
        rig.ack("one", &item, &claim.lease_id).await,
        Err(ApiError::Forbidden(_))
    ));
    rig.ack("two", &item, &second.lease_id).await.unwrap();
    // And once it is done it is gone.
    assert!(matches!(
        rig.ack("two", &item, &second.lease_id).await,
        Err(ApiError::NotFound(_))
    ));
}

#[tokio::test]
async fn an_unacked_claim_expires_and_the_item_comes_back_one_attempt_on_after_the_backoff() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let first = rig.pull("one").await.unwrap();

    // Not yet.
    rig.pass(9).await;
    assert_eq!(rig.transitions().await, ["claimed"]);
    assert!(rig.pull("two").await.is_none());

    // The lease ends: expired is recorded and the backoff (base 2 s) starts.
    rig.pass(1).await;
    assert_eq!(rig.transitions().await, ["claimed", "expired"]);
    let facts = rig.read(&Topic::facts_work()).await;
    assert_eq!(facts[1].principal.to_string(), "service:mansio-engine");
    assert_eq!(data(&facts[1])["lease_id"], first.lease_id.to_string());
    assert_eq!(data(&facts[1])["holder"], "agent:one");
    assert_eq!(data(&facts[1])["next"], "retry");
    assert_eq!(
        data(&facts[1])["retry_at"],
        Timestamp::from_unix_millis(START + 12_000)
            .unwrap()
            .to_string()
    );
    assert!(rig.pull("two").await.is_none());

    rig.pass(1).await;
    assert!(rig.pull("two").await.is_none());
    rig.pass(1).await;
    assert_eq!(rig.transitions().await, ["claimed", "expired", "retried"]);

    let second = rig.pull("two").await.unwrap();
    assert_eq!(second.item.item_id, Some(item.clone()));
    assert_eq!(second.item.attempt, Some(2));
    assert_ne!(second.item.id, first.item.id);
    assert_eq!(
        second.item.causation_id.as_ref().unwrap().as_str(),
        first.item.id.to_string()
    );
    assert_ne!(second.lease_id, first.lease_id);
    // Two messages on the queue, the second made by the engine.
    let queue_log = rig.read(&Topic::Work(queue())).await;
    assert_eq!(queue_log.len(), 2);
    assert_eq!(queue_log[1].id, second.item.id);

    let facts = rig.read(&Topic::facts_work()).await;
    let retried = &facts[2];
    assert_eq!(retried.attempt, Some(2));
    assert_eq!(data(retried)["message_id"], second.item.id.to_string());
    assert_eq!(
        data(retried)["previous_message_id"],
        first.item.id.to_string()
    );
}

#[tokio::test]
async fn a_failure_is_retried_after_a_doubling_backoff_and_past_the_cap_the_item_is_dead_only() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;

    // Attempt 1 fails: wait 2 s.
    let claim = rig.pull("one").await.unwrap();
    let failed = rig.fail("one", &item, &claim.lease_id).await.unwrap();
    assert_eq!(failed.envelope.event_type.as_str(), "mansio.work.failed");
    assert_eq!(data(&failed.envelope)["reason"], "it broke");
    assert_eq!(data(&failed.envelope)["next"], "retry");
    rig.pass(1).await;
    assert!(rig.pull("one").await.is_none());
    rig.pass(1).await;

    // Attempt 2 fails: wait 4 s.
    let claim = rig.pull("one").await.unwrap();
    assert_eq!(claim.item.attempt, Some(2));
    rig.fail("one", &item, &claim.lease_id).await.unwrap();
    rig.pass(3).await;
    assert!(rig.pull("one").await.is_none());
    rig.pass(1).await;

    // Attempt 3 is the last: its failure sends the item to dead at once.
    let claim = rig.pull("one").await.unwrap();
    assert_eq!(claim.item.attempt, Some(3));
    let failed = rig.fail("one", &item, &claim.lease_id).await.unwrap();
    assert_eq!(data(&failed.envelope)["next"], "dead");
    assert!(data(&failed.envelope).get("retry_at").is_none());

    assert_eq!(
        rig.transitions().await,
        [
            "claimed", "failed", "retried", "claimed", "failed", "retried", "claimed", "failed",
            "dead"
        ]
    );
    rig.pass(1_000).await;
    assert!(rig.pull("one").await.is_none());
    assert_eq!(rig.read(&Topic::Work(queue())).await.len(), 3);
    let dead = rig.read(&Topic::Dead).await;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].item_id, Some(item));
    assert_eq!(dead[0].attempt, Some(3));
    assert_eq!(dead[0].data, claim.item.data);
    assert_eq!(
        dead[0].causation_id.as_ref().unwrap().as_str(),
        claim.item.id.to_string()
    );
}

#[tokio::test]
async fn an_item_that_expires_on_its_last_attempt_goes_to_dead() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    for _ in 0..3 {
        rig.pull("one").await.unwrap();
        rig.pass(10).await;
        rig.pass(5).await;
    }
    assert!(rig.pull("one").await.is_none());
    assert_eq!(
        rig.transitions().await,
        [
            "claimed", "expired", "retried", "claimed", "expired", "retried", "claimed", "expired",
            "dead"
        ]
    );
    let dead = rig.read(&Topic::Dead).await;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].item_id, Some(item));
}

#[tokio::test]
async fn concurrent_pulls_never_get_the_same_item() {
    let rig = Rig::new();
    let mut published = Vec::new();
    for n in 0..20 {
        published.push(rig.publish_item(&format!("job-{n}")).await);
    }
    let mut pulls = Vec::new();
    for n in 0..60 {
        let engine = Arc::clone(&rig.engine);
        pulls.push(tokio::spawn(async move {
            engine
                .work_pull(
                    &tenant(),
                    &queue(),
                    &agent("one"),
                    &consumer(&format!("w{n}")),
                )
                .await
                .unwrap()
        }));
    }
    let mut got = Vec::new();
    for pull in pulls {
        if let Some(claimed) = pull.await.unwrap() {
            got.push(claimed.item.item_id.unwrap());
        }
    }
    assert_eq!(got.len(), 20);
    let distinct: HashSet<&ItemId> = got.iter().collect();
    assert_eq!(distinct.len(), 20);
    assert_eq!(distinct, published.iter().collect::<HashSet<_>>());
}

#[tokio::test]
async fn a_claim_that_cannot_be_recorded_leaves_the_item_claimable() {
    let rig = Rig::new();
    rig.publish_item("job").await;
    rig.broker.fail("facts.work");
    let refused = rig
        .engine
        .work_pull(&tenant(), &queue(), &agent("one"), &consumer("w1"))
        .await;
    assert!(matches!(refused, Err(ApiError::Unavailable(_))));
    rig.broker.heal();
    assert!(rig.pull("one").await.is_some());
    assert_eq!(rig.transitions().await, ["claimed"]);
}

#[tokio::test]
async fn publishing_work_through_the_generic_route_is_refused() {
    let rig = Rig::new();
    let mut message = Envelope::new(
        EventId::generate(),
        "test".parse().unwrap(),
        "job".parse().unwrap(),
        tenant(),
        Topic::Work(queue()),
        agent("planner"),
    );
    for topic in ["work.builds", "facts.work", "facts.alert"] {
        message.topic = topic.parse().unwrap();
        assert!(
            matches!(
                rig.engine.publish(message.clone()).await,
                Err(ApiError::BadRequest(_))
            ),
            "{topic}"
        );
    }
    assert!(rig.sink.all().is_empty());
    message.topic = "facts.build".parse().unwrap();
    assert!(rig.engine.publish(message).await.is_ok());
}

#[tokio::test]
async fn an_item_id_that_is_still_open_cannot_be_published_again() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let published = rig.read(&Topic::Work(queue())).await;
    let again = published[0].clone();
    assert_eq!(again.item_id, Some(item));
    let error = rig.engine.work_publish(again).await.unwrap_err();
    assert!(matches!(error, ApiError::Conflict(_)));
    assert_eq!(rig.read(&Topic::Work(queue())).await.len(), 1);
}

async fn publish_alert(rig: &Rig) -> EventId {
    let id = EventId::generate();
    let mut alert = Envelope::new(
        id,
        "test".parse().unwrap(),
        "disk.full".parse().unwrap(),
        tenant(),
        Topic::Alerts,
        agent("monitor"),
    );
    alert.time = Some(Timestamp::from_unix_millis(rig.clock.now_millis()).unwrap());
    rig.engine.publish(alert).await.unwrap();
    id
}

async fn alert_facts(rig: &Rig) -> Vec<String> {
    rig.read(&Topic::facts_alert())
        .await
        .iter()
        .map(|fact| fact.event_type.as_str().replace("mansio.alert.", ""))
        .collect()
}

#[tokio::test]
async fn an_alert_is_acknowledged_once_and_the_ack_names_the_principal() {
    let rig = Rig::new();
    let alert = publish_alert(&rig).await;

    let first = rig
        .engine
        .alerts_ack(&tenant(), alert, &agent("pat"))
        .await
        .unwrap();
    assert_eq!(
        first.envelope.event_type.as_str(),
        "mansio.alert.acknowledged"
    );
    assert_eq!(first.envelope.principal, agent("pat"));
    assert_eq!(
        first.envelope.subject.as_ref().unwrap().as_str(),
        alert.to_string()
    );
    let fact = data(&first.envelope);
    assert_eq!(fact["alert_id"], alert.to_string());
    assert_eq!(fact["acknowledged_by"], "agent:pat");
    assert_eq!(
        fact["acknowledged_at"],
        Timestamp::from_unix_millis(START).unwrap().to_string()
    );

    let second = rig.engine.alerts_ack(&tenant(), alert, &agent("sam")).await;
    assert!(matches!(second, Err(ApiError::Conflict(message)) if message.contains("agent:pat")));
    let unknown = rig
        .engine
        .alerts_ack(&tenant(), EventId::generate(), &agent("pat"))
        .await;
    assert!(matches!(unknown, Err(ApiError::NotFound(_))));
    assert_eq!(alert_facts(&rig).await, ["acknowledged"]);
}

#[tokio::test]
async fn an_unacknowledged_alert_is_surfaced_once_per_interval_until_acknowledged() {
    let rig = Rig::new();
    let alert = publish_alert(&rig).await;

    rig.pass(29).await;
    assert!(alert_facts(&rig).await.is_empty());
    rig.pass(1).await;
    assert_eq!(alert_facts(&rig).await, ["resurfaced"]);
    // Ticking again within the interval adds nothing.
    rig.pass(0).await;
    rig.pass(29).await;
    assert_eq!(alert_facts(&rig).await, ["resurfaced"]);
    rig.pass(1).await;
    assert_eq!(alert_facts(&rig).await, ["resurfaced", "resurfaced"]);

    let facts = rig.read(&Topic::facts_alert()).await;
    assert_eq!(facts[0].principal.to_string(), "service:mansio-engine");
    assert_eq!(data(&facts[0])["alert_id"], alert.to_string());
    assert_eq!(
        facts[0].subject.as_ref().unwrap().as_str(),
        alert.to_string()
    );

    rig.engine
        .alerts_ack(&tenant(), alert, &agent("pat"))
        .await
        .unwrap();
    rig.pass(300).await;
    assert_eq!(
        alert_facts(&rig).await,
        ["resurfaced", "resurfaced", "acknowledged"]
    );
}

#[tokio::test]
async fn a_restart_mid_lease_keeps_the_lease_and_the_item_comes_back_exactly_once() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull("one").await.unwrap();
    rig.pass(4).await;

    let rig = rig.restart().await;
    // Still leased to the same holder.
    assert!(rig.pull("two").await.is_none());
    rig.pass(5).await;
    assert!(rig.pull("two").await.is_none());
    assert_eq!(rig.transitions().await, ["claimed"]);

    // The lease ends on the new server's clock reading.
    rig.pass(1).await;
    assert_eq!(rig.transitions().await, ["claimed", "expired"]);
    rig.pass(2).await;
    assert_eq!(rig.transitions().await, ["claimed", "expired", "retried"]);

    let again = rig.pull("two").await.unwrap();
    assert_eq!(again.item.item_id, Some(item));
    assert_eq!(again.item.attempt, Some(2));
    assert_eq!(
        again.item.causation_id.as_ref().unwrap().as_str(),
        claim.item.id.to_string()
    );
    assert!(rig.pull("three").await.is_none());
    rig.pass(1).await;
    assert_eq!(rig.read(&Topic::Work(queue())).await.len(), 2);

    // And the holder it had is recognised by the restarted server.
    let kept = rig.restart().await;
    assert!(matches!(
        kept.ack("one", &again.item.item_id.clone().unwrap(), &claim.lease_id)
            .await,
        Err(ApiError::Forbidden(_))
    ));
    kept.ack("two", &again.item.item_id.clone().unwrap(), &again.lease_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_restart_during_a_backoff_still_republishes_once() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull("one").await.unwrap();
    rig.fail("one", &item, &claim.lease_id).await.unwrap();
    rig.pass(1).await;

    let rig = rig.restart().await;
    assert!(rig.pull("two").await.is_none());
    rig.pass(1).await;
    rig.pass(1).await;
    rig.pass(1).await;
    assert_eq!(rig.transitions().await, ["claimed", "failed", "retried"]);
    assert_eq!(rig.read(&Topic::Work(queue())).await.len(), 2);
    assert_eq!(rig.pull("two").await.unwrap().item.attempt, Some(2));
    assert!(rig.pull("three").await.is_none());
}

#[tokio::test]
async fn a_restart_after_the_message_but_before_the_fact_records_the_fact_once() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull("one").await.unwrap();
    rig.fail("one", &item, &claim.lease_id).await.unwrap();

    // The republish gets as far as the queue and no further.
    rig.broker.fail("mansio.work.retried");
    rig.clock.advance(Duration::from_secs(2));
    rig.engine.tick().await;
    assert_eq!(rig.read(&Topic::Work(queue())).await.len(), 2);
    assert_eq!(rig.transitions().await, ["claimed", "failed"]);
    // The item is not claimable while its fact is owed.
    rig.broker.heal();
    // (Stop here, as a crash would.)
    let rig = rig.restart().await;
    assert!(rig.pull("two").await.is_none());
    rig.pass(1).await;
    assert_eq!(rig.transitions().await, ["claimed", "failed", "retried"]);
    assert_eq!(rig.read(&Topic::Work(queue())).await.len(), 2);
    let retried = &rig.read(&Topic::facts_work()).await[2];
    assert_eq!(retried.attempt, Some(2));

    let claim = rig.pull("two").await.unwrap();
    assert_eq!(claim.item.attempt, Some(2));
    assert_eq!(data(retried)["message_id"], claim.item.id.to_string());
    assert!(rig.pull("three").await.is_none());
    rig.pass(5).await;
    assert_eq!(rig.read(&Topic::Work(queue())).await.len(), 2);
}

#[tokio::test]
async fn the_same_half_written_retry_is_finished_by_the_timer_without_a_restart() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull("one").await.unwrap();
    rig.fail("one", &item, &claim.lease_id).await.unwrap();

    rig.broker.fail("mansio.work.retried");
    rig.pass(2).await;
    assert_eq!(rig.transitions().await, ["claimed", "failed"]);
    assert!(rig.pull("two").await.is_none());
    rig.broker.heal();
    rig.pass(0).await;
    assert_eq!(rig.transitions().await, ["claimed", "failed", "retried"]);
    assert_eq!(rig.read(&Topic::Work(queue())).await.len(), 2);
    assert!(rig.pull("two").await.is_some());
}

/// An item whose last attempt fails while publishes matching `refused` (a
/// topic or an event type) are refused, so that sending it to `dead` stops
/// half way.
async fn failing_last_attempt(refused: &str) -> Rig {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    for _ in 0..2 {
        let claim = rig.pull("one").await.unwrap();
        rig.fail("one", &item, &claim.lease_id).await.unwrap();
        rig.pass(5).await;
    }
    let claim = rig.pull("one").await.unwrap();
    rig.broker.fail(refused);
    let failed = rig.fail("one", &item, &claim.lease_id).await.unwrap();
    assert_eq!(data(&failed.envelope)["next"], "dead");
    assert_eq!(rig.transitions().await.last().unwrap(), "failed");
    rig
}

async fn assert_dead_once(rig: &Rig) {
    assert!(rig.pull("two").await.is_none());
    assert_eq!(rig.read(&Topic::Dead).await.len(), 1);
    let transitions = rig.transitions().await;
    assert_eq!(transitions.last().unwrap(), "dead");
    assert_eq!(transitions.iter().filter(|t| *t == "dead").count(), 1);
}

#[tokio::test]
async fn a_restart_between_the_dead_message_and_its_fact_records_the_fact_once() {
    let rig = failing_last_attempt("mansio.work.dead").await;
    assert_eq!(rig.read(&Topic::Dead).await.len(), 1, "the copy is out");
    rig.broker.heal();
    let rig = rig.restart().await;
    rig.pass(1).await;
    rig.pass(100).await;
    assert_dead_once(&rig).await;
}

#[tokio::test]
async fn the_timer_finishes_a_half_written_dead_letter_without_a_restart() {
    let rig = failing_last_attempt("mansio.work.dead").await;
    rig.pass(1).await;
    assert_eq!(rig.transitions().await.last().unwrap(), "failed");
    rig.broker.heal();
    rig.pass(1).await;
    rig.pass(100).await;
    assert_dead_once(&rig).await;
}

#[tokio::test]
async fn a_restart_before_the_dead_message_is_published_publishes_it_once() {
    let rig = failing_last_attempt("dead").await;
    assert!(rig.read(&Topic::Dead).await.is_empty());
    rig.broker.heal();
    let rig = rig.restart().await;
    rig.pass(1).await;
    rig.pass(100).await;
    assert_dead_once(&rig).await;
}

#[tokio::test]
async fn finished_items_and_acknowledged_alerts_stay_finished_across_a_restart() {
    let rig = Rig::new();
    let done = rig.publish_item("done").await;
    let claim = rig.pull("one").await.unwrap();
    rig.ack("one", &done, &claim.lease_id).await.unwrap();
    let alert = publish_alert(&rig).await;
    rig.engine
        .alerts_ack(&tenant(), alert, &agent("pat"))
        .await
        .unwrap();

    let rig = rig.restart().await;
    rig.pass(1_000).await;
    assert!(rig.pull("two").await.is_none());
    assert_eq!(rig.transitions().await, ["claimed", "acked"]);
    assert_eq!(alert_facts(&rig).await, ["acknowledged"]);
    let again = rig.engine.alerts_ack(&tenant(), alert, &agent("sam")).await;
    assert!(matches!(again, Err(ApiError::Conflict(_))));
}

#[tokio::test]
async fn an_alert_surfaced_before_a_restart_is_surfaced_again_one_interval_after_that() {
    let rig = Rig::new();
    let alert = publish_alert(&rig).await;
    rig.pass(30).await;
    assert_eq!(alert_facts(&rig).await, ["resurfaced"]);

    let rig = rig.restart().await;
    rig.pass(29).await;
    assert_eq!(alert_facts(&rig).await, ["resurfaced"]);
    rig.pass(1).await;
    assert_eq!(alert_facts(&rig).await, ["resurfaced", "resurfaced"]);
    // It can still be acknowledged.
    rig.engine
        .alerts_ack(&tenant(), alert, &agent("pat"))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_fresh_server_finds_the_tenants_the_broker_already_holds() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    rig.pull("one").await.unwrap();

    // A second tenant, to be sure each is taken up.
    let other: TenantId = "other".parse().unwrap();
    rig.broker.ensure_tenant(&other).await.unwrap();

    let restarted = rig.restart().await;
    restarted.pass(10).await;
    restarted.pass(2).await;
    assert_eq!(
        restarted.transitions().await,
        ["claimed", "expired", "retried"],
        "the timer needed no request to expire the lease of {item}"
    );
}

#[tokio::test]
async fn concurrent_publishes_reach_the_sink_in_the_order_they_reach_the_topic() {
    let sink = Arc::new(LogSink {
        slow_start: true,
        ..LogSink::default()
    });
    let rig = Rig::over(
        Arc::default(),
        Arc::clone(&sink),
        Arc::new(ManualClock::new(START)),
    );
    let mut publishes = Vec::new();
    for n in 0..12 {
        let engine = Arc::clone(&rig.engine);
        publishes.push(tokio::spawn(async move {
            let mut message = Envelope::new(
                EventId::generate(),
                "test".parse().unwrap(),
                format!("t{n}").parse().unwrap(),
                tenant(),
                "facts.build".parse().unwrap(),
                agent("one"),
            );
            message.data = Some(json!({"n": n}));
            engine.publish(message).await.unwrap();
        }));
    }
    for publish in publishes {
        publish.await.unwrap();
    }
    let topic: Vec<EventId> = rig
        .read(&"facts.build".parse().unwrap())
        .await
        .iter()
        .map(|e| e.id)
        .collect();
    assert_eq!(topic.len(), 12);
    assert_eq!(sink.topic("facts.build"), topic);
}

#[tokio::test]
async fn the_sink_sees_work_and_facts_in_the_order_they_were_published() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull("one").await.unwrap();
    rig.fail("one", &item, &claim.lease_id).await.unwrap();
    rig.pass(2).await;
    let claim = rig.pull("one").await.unwrap();
    rig.ack("one", &item, &claim.lease_id).await.unwrap();

    let topics: Vec<String> = rig.sink.all().into_iter().map(|(topic, _)| topic).collect();
    assert_eq!(
        topics,
        [
            "work.builds", // the item
            "facts.work",  // claimed
            "facts.work",  // failed
            "work.builds", // attempt 2
            "facts.work",  // retried
            "facts.work",  // claimed
            "facts.work",  // acked
        ]
    );
    let facts: Vec<EventId> = rig
        .read(&Topic::facts_work())
        .await
        .iter()
        .map(|e| e.id)
        .collect();
    assert_eq!(rig.sink.topic("facts.work"), facts);
}

#[test]
fn the_engine_has_a_debug_form_that_names_no_state() {
    let rig = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async { Rig::new() });
    assert!(format!("{:?}", rig.engine).contains("Engine"));
}
