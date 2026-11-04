//! The engine against a memory broker and a clock moved by hand.

use std::collections::HashSet;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use mansio_core::{Data, MemoryBroker, Record, SinkError, Stored, SystemRecord};
use serde_json::{Value, json};

use super::*;
use crate::clock::manual::ManualClock;

const START: i64 = 1_792_000_000_000;

fn settings() -> WorkSettings {
    WorkSettings {
        lease: Duration::from_secs(10),
        lease_max: Duration::from_secs(120),
        max_attempts: 3,
        backoff_base: Duration::from_secs(2),
        backoff_cap: Duration::from_secs(5),
        alert_resurface: Duration::from_secs(30),
        tick: Duration::from_millis(100),
        idempotency_window: Duration::from_secs(60),
        idempotency_max_keys: 3,
        recovery_log: Duration::from_secs(30),
        snapshot_interval: Duration::from_secs(60),
        owner_ttl: Duration::ZERO,
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
    refused: StdMutex<HashSet<String>>,
    lost: StdMutex<HashSet<String>>,
    corrupt: StdMutex<Option<(Topic, u64)>>,
    /// Appends to the server's own logs fail.
    system_down: std::sync::atomic::AtomicBool,
    /// Reads of the server's own logs are refused.
    system_unreadable: std::sync::atomic::AtomicBool,
    /// The broker refuses to list its tenants, and counts the asks.
    tenants_refused: std::sync::atomic::AtomicBool,
    tenants_asked: AtomicU64,
    /// How many messages polls have returned.
    read: AtomicU64,
}

impl FlakyBroker {
    /// Appends to the server's own logs fail, as when only that stream is down.
    fn fail_system_appends(&self) {
        self.system_down.store(true, Ordering::SeqCst);
    }

    /// Publishes to the topic, or of the event type, fail as if the broker
    /// could not be reached: whether the message was stored is not known.
    fn fail(&self, what: &str) {
        self.failing.lock().unwrap().insert(what.to_owned());
    }

    /// Publishes to the topic, or of the event type, are refused outright:
    /// the message was not stored.
    fn refuse(&self, what: &str) {
        self.refused.lock().unwrap().insert(what.to_owned());
    }

    /// The next publish to the topic, or of the event type, is stored and then
    /// reported as an error, as a connection that drops after the write would.
    fn lose_the_answer(&self, what: &str) {
        self.lost.lock().unwrap().insert(what.to_owned());
    }

    /// A poll that covers the message at `offset` of `topic` fails, as one
    /// does when a stored message is not an envelope.
    fn corrupt(&self, topic: &str, offset: u64) {
        *self.corrupt.lock().unwrap() = Some((topic.parse().unwrap(), offset));
    }

    fn heal(&self) {
        self.failing.lock().unwrap().clear();
        self.refused.lock().unwrap().clear();
        self.lost.lock().unwrap().clear();
        *self.corrupt.lock().unwrap() = None;
        self.system_down.store(false, Ordering::SeqCst);
    }
}

#[async_trait]
impl Broker for FlakyBroker {
    async fn tenants(&self) -> Result<Vec<TenantId>, BrokerError> {
        self.tenants_asked.fetch_add(1, Ordering::SeqCst);
        if self.tenants_refused.load(Ordering::SeqCst) {
            return Err(BrokerError::Forbidden("may not list streams".to_owned()));
        }
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
        let refused = {
            let failing = self.failing.lock().unwrap();
            failing.contains(&envelope.topic.to_string())
                || failing.contains(envelope.event_type.as_str())
        };
        if refused {
            return Err(BrokerError::Unavailable("injected".to_owned()));
        }
        let rejected = {
            let refused = self.refused.lock().unwrap();
            refused.contains(&envelope.topic.to_string())
                || refused.contains(envelope.event_type.as_str())
        };
        if rejected {
            return Err(BrokerError::Rejected("refused".to_owned()));
        }
        let stored = self.inner.publish(caller, envelope).await;
        let lost = {
            let mut lost = self.lost.lock().unwrap();
            lost.remove(&envelope.topic.to_string()) || lost.remove(envelope.event_type.as_str())
        };
        if lost {
            return Err(BrokerError::Unavailable(
                "the connection dropped after the write".to_owned(),
            ));
        }
        stored
    }

    async fn poll(
        &self,
        caller: &Caller<'_>,
        tenant: &TenantId,
        topic: &Topic,
        offset: u64,
        max: u32,
    ) -> Result<Vec<Stored>, BrokerError> {
        if let Some((bad, at)) = &*self.corrupt.lock().unwrap()
            && bad == topic
            && (offset..offset.saturating_add(u64::from(max))).contains(at)
        {
            return Err(BrokerError::Corrupt(format!(
                "offset {at} of {topic} in {tenant}: injected"
            )));
        }
        let found = self.inner.poll(caller, tenant, topic, offset, max).await?;
        self.read.fetch_add(
            u64::try_from(found.len()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
        Ok(found)
    }

    async fn system_append(&self, log: SystemLog, record: &[u8]) -> Result<(), BrokerError> {
        if self.system_down.load(Ordering::SeqCst) {
            return Err(BrokerError::Unavailable("injected".to_owned()));
        }
        self.inner.system_append(log, record).await
    }

    async fn system_read(
        &self,
        log: SystemLog,
        offset: u64,
        max: u32,
    ) -> Result<Vec<SystemRecord>, BrokerError> {
        if self.system_unreadable.load(Ordering::SeqCst) {
            return Err(BrokerError::Rejected("injected".to_owned()));
        }
        self.inner.system_read(log, offset, max).await
    }
}

/// A sink that notes what it was given, in order. With `slow_start` it is slow
/// to answer earlier calls and quick to answer later ones, which reorders
/// concurrent publishes unless something keeps them in line.
#[derive(Debug, Default)]
struct LogSink {
    seen: StdMutex<Vec<(String, EventId)>>,
    envelopes: StdMutex<Vec<Envelope>>,
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
        self.envelopes.lock().unwrap().push(envelope.clone());
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

    fn all_envelopes(&self) -> Vec<Envelope> {
        self.envelopes.lock().unwrap().clone()
    }
}

/// The consumer name each lease was taken under, so that an ack or a fail can
/// name it as the holder must. Lease ids are random, so tests do not meet each
/// other's.
static TAKEN_AS: std::sync::LazyLock<StdMutex<std::collections::HashMap<LeaseId, ConsumerName>>> =
    std::sync::LazyLock::new(Default::default);

fn taken_as(lease: &LeaseId) -> ConsumerName {
    TAKEN_AS
        .lock()
        .unwrap()
        .get(lease)
        .cloned()
        .unwrap_or_else(|| consumer("w1"))
}

struct Rig {
    broker: Arc<FlakyBroker>,
    sink: Arc<LogSink>,
    clock: Arc<ManualClock>,
    engine: Arc<Engine>,
    /// How many pulls have been made, so that each can be a consumer of its own.
    pulls: AtomicU64,
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
            pulls: AtomicU64::new(0),
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
        self.engine
            .work_publish(item_envelope(id, name))
            .await
            .unwrap();
        id.to_string().parse().unwrap()
    }

    /// A pull by `who` under a consumer name nobody has used yet, so that it
    /// is a claim of its own: `w1`, `w2` and so on.
    async fn pull(&self, who: &str) -> Option<Claimed> {
        let n = self.pulls.fetch_add(1, Ordering::SeqCst) + 1;
        self.pull_as(who, &format!("w{n}")).await
    }

    /// A pull by `who` under the consumer name given.
    async fn pull_as(&self, who: &str, name: &str) -> Option<Claimed> {
        let claim = self
            .engine
            .work_pull(&tenant(), &queue(), &agent(who), &consumer(name))
            .await
            .unwrap()?;
        TAKEN_AS
            .lock()
            .unwrap()
            .insert(claim.lease_id, claim.consumer.clone());
        Some(claim)
    }

    async fn ack(&self, who: &str, item: &ItemId, lease: &LeaseId) -> Result<Published, ApiError> {
        self.engine
            .work_ack(
                &tenant(),
                &queue(),
                item,
                Holder {
                    principal: &agent(who),
                    consumer: &taken_as(lease),
                    lease_id: lease,
                },
            )
            .await
    }

    async fn fail(&self, who: &str, item: &ItemId, lease: &LeaseId) -> Result<Published, ApiError> {
        self.engine
            .work_fail(
                &tenant(),
                &queue(),
                item,
                Holder {
                    principal: &agent(who),
                    consumer: &taken_as(lease),
                    lease_id: lease,
                },
                "it broke",
            )
            .await
    }

    async fn extend(
        &self,
        who: &str,
        item: &ItemId,
        lease: &LeaseId,
        seconds: Option<u64>,
    ) -> Result<Published, ApiError> {
        self.engine
            .work_extend(
                &tenant(),
                &queue(),
                item,
                Holder {
                    principal: &agent(who),
                    consumer: &taken_as(lease),
                    lease_id: lease,
                },
                seconds.map(Duration::from_secs),
                None,
            )
            .await
    }

    async fn read(&self, topic: &Topic) -> Vec<Envelope> {
        match self
            .broker
            .poll(&Caller::Admin, &tenant(), topic, 0, 1000)
            .await
        {
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

/// The message `work_publish` is given: named after its id, at attempt 1.
fn item_envelope(id: EventId, name: &str) -> Envelope {
    let mut message = Envelope::new(
        id,
        "test".parse().unwrap(),
        "job.requested".parse().unwrap(),
        tenant(),
        Topic::Work(queue()),
        agent("planner"),
    );
    message.item_id = Some(id.to_string().parse().unwrap());
    message.attempt = Some(1);
    message.subject = Some(name.parse().unwrap());
    message.data = Some(Data::new(&json!({"name": name})).unwrap());
    message
}

fn data(fact: &Envelope) -> Value {
    fact.data.as_ref().unwrap().parse().unwrap()
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
    assert_eq!(claimed["lease"], one.lease_id.fingerprint().to_string());
    assert!(claimed.get("lease_id").is_none(), "the id is not recorded");
    assert_eq!(claimed["message_id"], one.item.id.to_string());
    assert_eq!(facts[0].principal, agent("one"));
}

#[tokio::test]
async fn a_consumer_that_holds_a_live_lease_is_given_that_lease_again() {
    let rig = Rig::new();
    let first = rig.publish_item("first").await;
    let second = rig.publish_item("second").await;

    let claim = rig.pull_as("one", "worker").await.unwrap();
    assert_eq!(claim.item.item_id, Some(first.clone()));
    let facts = rig.sink.all().len();

    // The answer was lost and the client asks again: the same lease, not the
    // next item, and nothing is recorded.
    rig.clock.advance(Duration::from_secs(3));
    let again = rig.pull_as("one", "worker").await.unwrap();
    assert_eq!(again, claim, "the lease keeps its id and its expiry");
    assert_eq!(rig.sink.all().len(), facts);
    assert_eq!(rig.transitions().await, ["claimed"]);

    // A lease is held by a principal and a consumer name together. Another
    // name gets the next item, and another principal under the same name
    // finds nothing left.
    let other_name = rig.pull_as("one", "worker-2").await.unwrap();
    assert_eq!(other_name.item.item_id, Some(second));
    assert!(rig.pull_as("two", "worker").await.is_none());
    assert_eq!(rig.pull_as("one", "worker").await.unwrap(), claim);

    // Finished, the lease is not given again: the next pull claims anew.
    rig.ack("one", &first, &claim.lease_id).await.unwrap();
    let third = rig.publish_item("third").await;
    let next = rig.pull_as("one", "worker").await.unwrap();
    assert_eq!(next.item.item_id, Some(third));
    assert_ne!(next.lease_id, claim.lease_id);
}

#[tokio::test]
async fn a_lease_that_has_run_out_is_not_given_again() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();

    // The lease ended and the timer has not noticed yet.
    rig.clock.advance(Duration::from_secs(10));
    assert!(rig.pull_as("one", "worker").await.is_none());

    // After the backoff the item is claimed anew, under a lease of its own.
    rig.engine.tick().await;
    rig.pass(2).await;
    let again = rig.pull_as("one", "worker").await.unwrap();
    assert_eq!(again.item.item_id, Some(item));
    assert_eq!(again.item.attempt, Some(2));
    assert_ne!(again.lease_id, claim.lease_id);
}

#[tokio::test]
async fn a_restarted_server_gives_a_consumer_a_new_id_for_the_lease_it_held() {
    let rig = Rig::new();
    let item = rig.publish_item("first").await;
    rig.publish_item("second").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();

    // The facts keep a fingerprint of the lease id and not the id, so a server
    // that has only read them cannot say it again. It gives the same item, the
    // same expiry and a new id, and records that.
    let rig = rig.restart().await;
    let again = rig.pull_as("one", "worker").await.unwrap();
    assert_eq!(again.item, claim.item);
    assert_eq!(again.expires_at, claim.expires_at);
    assert_ne!(again.lease_id, claim.lease_id);
    assert_eq!(rig.transitions().await, ["claimed", "claimed"]);
    assert!(matches!(
        rig.ack("one", &item, &claim.lease_id).await,
        Err(ApiError::Forbidden(_))
    ));

    // Asked once more it is the same lease, and nothing more is recorded.
    assert_eq!(rig.pull_as("one", "worker").await.unwrap(), again);
    assert_eq!(rig.transitions().await, ["claimed", "claimed"]);

    // The new id is the lease, and survives another restart as a fingerprint.
    let rig = rig.restart().await;
    rig.ack("one", &item, &again.lease_id).await.unwrap();
    assert!(rig.pull_as("one", "worker").await.is_some_and(|next| {
        next.item.subject.as_ref().map(ToString::to_string) == Some("second".to_owned())
    }));
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
    assert_eq!(
        data(&facts[1])["lease"],
        first.lease_id.fingerprint().to_string()
    );
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
    for topic in ["work.builds", "facts.work", "facts.alert", "dead"] {
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

/// The replay takes a copy on `dead` for an item's own dead letter, so a copy
/// a caller could write there would finish an open item without running it.
#[tokio::test]
async fn a_crafted_copy_on_dead_cannot_finish_an_open_item() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let original = rig.read(&Topic::Work(queue())).await.remove(0);
    let mut crafted = original.clone();
    crafted.id = EventId::generate();
    crafted.topic = Topic::Dead;
    crafted.causation_id = Some(original.id.to_string().parse().unwrap());
    assert!(matches!(
        rig.engine.publish(crafted).await,
        Err(ApiError::BadRequest(_))
    ));
    assert!(rig.read(&Topic::Dead).await.is_empty());

    let rig = rig.restart().await;
    let claimed = rig.pull("one").await.expect("the item is still open");
    assert_eq!(claimed.item.item_id, Some(item));
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

fn alert_envelope(id: EventId, at: i64) -> Envelope {
    let mut alert = Envelope::new(
        id,
        "test".parse().unwrap(),
        "disk.full".parse().unwrap(),
        tenant(),
        Topic::Alerts,
        agent("monitor"),
    );
    alert.time = Some(Timestamp::from_unix_millis(at).unwrap());
    alert
}

async fn publish_alert(rig: &Rig) -> EventId {
    let id = EventId::generate();
    rig.engine
        .publish(alert_envelope(id, rig.clock.now_millis()))
        .await
        .unwrap();
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
    assert!(
        matches!(second, Err(ApiError::Conflict(message)) if message.contains("already acknowledged"))
    );
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
            message.data = Some(Data::new(&json!({"n": n})).unwrap());
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

// ---- an error does not say whether the message was stored -------------------

#[tokio::test]
async fn a_work_item_that_was_stored_but_whose_publish_errored_is_still_claimable() {
    let rig = Rig::new();
    rig.broker.lose_the_answer("work.builds");
    let id = EventId::generate();
    let refused = rig.engine.work_publish(item_envelope(id, "job")).await;
    assert!(matches!(refused, Err(ApiError::Unavailable(_))));
    assert_eq!(
        rig.read(&Topic::Work(queue())).await.len(),
        1,
        "it is stored"
    );

    let claim = rig.pull("one").await.expect("a stored item can be claimed");
    assert_eq!(claim.item.id, id);
}

#[tokio::test]
async fn an_alert_ack_that_was_stored_but_errored_cannot_be_made_twice() {
    let rig = Rig::new();
    let alert = publish_alert(&rig).await;
    rig.broker.lose_the_answer("mansio.alert.acknowledged");
    let refused = rig.engine.alerts_ack(&tenant(), alert, &agent("pat")).await;
    assert!(matches!(refused, Err(ApiError::Unavailable(_))));

    let again = rig.engine.alerts_ack(&tenant(), alert, &agent("sam")).await;
    assert!(matches!(again, Err(ApiError::Conflict(_))), "{again:?}");
    assert_eq!(alert_facts(&rig).await, ["acknowledged"]);
}

#[tokio::test]
async fn a_claim_that_was_stored_but_errored_holds_the_item() {
    let rig = Rig::new();
    rig.publish_item("job").await;
    rig.broker.lose_the_answer("mansio.work.claimed");
    let refused = rig
        .engine
        .work_pull(&tenant(), &queue(), &agent("one"), &consumer("w1"))
        .await;
    assert!(matches!(refused, Err(ApiError::Unavailable(_))));

    // The claim is on the log, so the item is leased, and comes back after it.
    assert!(rig.pull("two").await.is_none());
    rig.pass(10).await;
    rig.pass(2).await;
    assert_eq!(rig.pull("two").await.unwrap().item.attempt, Some(2));
}

#[tokio::test]
async fn a_dead_letter_that_was_stored_but_errored_is_not_sent_twice() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    for _ in 0..2 {
        let claim = rig.pull("one").await.unwrap();
        rig.fail("one", &item, &claim.lease_id).await.unwrap();
        rig.pass(5).await;
    }
    let claim = rig.pull("one").await.unwrap();
    rig.broker.lose_the_answer("dead");
    rig.fail("one", &item, &claim.lease_id).await.unwrap();
    assert_eq!(rig.read(&Topic::Dead).await.len(), 1, "the copy is out");

    rig.pass(1).await;
    rig.pass(100).await;
    assert_dead_once(&rig).await;
}

// ---- ids are not reused ----------------------------------------------------

#[tokio::test]
async fn an_item_id_is_not_reused_after_the_item_is_done() {
    let rig = Rig::new();
    let id = EventId::generate();
    rig.engine
        .work_publish(item_envelope(id, "job"))
        .await
        .unwrap();
    let item: ItemId = id.to_string().parse().unwrap();
    let claim = rig.pull("one").await.unwrap();
    rig.ack("one", &item, &claim.lease_id).await.unwrap();

    let again = rig.engine.work_publish(item_envelope(id, "job")).await;
    assert!(matches!(again, Err(ApiError::Conflict(_))), "{again:?}");
    assert!(
        rig.pull("two").await.is_none(),
        "done work does not run twice"
    );

    let rig = rig.restart().await;
    assert!(rig.pull("two").await.is_none());
    let again = rig.engine.work_publish(item_envelope(id, "job")).await;
    assert!(matches!(again, Err(ApiError::Conflict(_))), "{again:?}");
    assert_eq!(rig.read(&Topic::Work(queue())).await.len(), 1);
}

#[tokio::test]
async fn an_item_id_is_not_reused_after_the_item_is_dead() {
    let rig = Rig::new();
    let id = EventId::generate();
    rig.engine
        .work_publish(item_envelope(id, "job"))
        .await
        .unwrap();
    let item: ItemId = id.to_string().parse().unwrap();
    for _ in 0..3 {
        let claim = rig.pull("one").await.unwrap();
        rig.fail("one", &item, &claim.lease_id).await.unwrap();
        rig.pass(5).await;
    }
    assert_eq!(rig.read(&Topic::Dead).await.len(), 1);

    let refused = rig.engine.work_publish(item_envelope(id, "job")).await;
    assert!(matches!(refused, Err(ApiError::Conflict(_))));
    let rig = rig.restart().await;
    let refused = rig.engine.work_publish(item_envelope(id, "job")).await;
    assert!(matches!(refused, Err(ApiError::Conflict(_))));
    assert!(rig.pull("two").await.is_none());
}

#[tokio::test]
async fn a_resent_alert_is_refused_and_does_not_reopen_it() {
    let rig = Rig::new();
    let id = EventId::generate();
    rig.engine.publish(alert_envelope(id, START)).await.unwrap();
    // Unacknowledged, a resend is refused and publishes nothing.
    let resent = rig.engine.publish(alert_envelope(id, START)).await;
    assert!(matches!(resent, Err(ApiError::Conflict(_))), "{resent:?}");
    assert_eq!(rig.read(&Topic::Alerts).await.len(), 1);

    rig.engine
        .alerts_ack(&tenant(), id, &agent("pat"))
        .await
        .unwrap();
    // Acknowledged, a resend does not make it acknowledgeable again.
    let resent = rig.engine.publish(alert_envelope(id, START)).await;
    assert!(matches!(resent, Err(ApiError::Conflict(_))), "{resent:?}");
    let again = rig.engine.alerts_ack(&tenant(), id, &agent("sam")).await;
    assert!(matches!(again, Err(ApiError::Conflict(_))));
    rig.pass(300).await;
    assert_eq!(alert_facts(&rig).await, ["acknowledged"]);

    let rig = rig.restart().await;
    let again = rig.engine.alerts_ack(&tenant(), id, &agent("kim")).await;
    assert!(matches!(again, Err(ApiError::Conflict(_))));
    rig.pass(300).await;
    assert_eq!(alert_facts(&rig).await, ["acknowledged"]);
}

// ---- one failure does not stop the rest --------------------------------------

#[tokio::test]
async fn one_item_whose_retry_keeps_failing_does_not_stall_the_rest_of_the_tenant() {
    let rig = Rig::new();
    let poison = EventId::generate();
    let mut message = item_envelope(poison, "poison");
    message.event_type = "poison.job".parse().unwrap();
    rig.engine.work_publish(message).await.unwrap();
    let healthy = rig.publish_item("healthy").await;
    let alert = publish_alert(&rig).await;

    let poisoned = rig.pull("one").await.unwrap();
    let ok = rig.pull("two").await.unwrap();
    rig.fail(
        "one",
        &poison.to_string().parse().unwrap(),
        &poisoned.lease_id,
    )
    .await
    .unwrap();
    rig.broker.refuse("poison.job");
    rig.fail("two", &healthy, &ok.lease_id).await.unwrap();

    for _ in 0..20 {
        rig.pass(10).await;
    }
    let again = rig.pull("three").await.expect("the healthy item came back");
    assert_eq!(again.item.item_id, Some(healthy));
    assert!(
        alert_facts(&rig).await.iter().any(|t| t == "resurfaced"),
        "alert {alert} was surfaced again"
    );
    // And the poison item is not lost: it comes back once the broker does.
    rig.broker.heal();
    rig.pass(10).await;
    let poisoned = rig.pull("four").await.unwrap();
    assert_eq!(poisoned.item.attempt, Some(2));
}

// ---- time and the clock ------------------------------------------------------

#[tokio::test]
async fn an_alert_with_a_time_in_the_future_is_still_surfaced_again() {
    let rig = Rig::new();
    let id = EventId::generate();
    let tomorrow = START + 86_400_000;
    rig.engine
        .publish(alert_envelope(id, tomorrow))
        .await
        .unwrap();
    rig.pass(29).await;
    assert!(alert_facts(&rig).await.is_empty());
    rig.pass(1).await;
    assert_eq!(alert_facts(&rig).await, ["resurfaced"]);

    // After a restart too: timed from the restart if its own time is later.
    let rig = rig.restart().await;
    rig.pass(30).await;
    assert_eq!(alert_facts(&rig).await, ["resurfaced", "resurfaced"]);
    rig.engine
        .alerts_ack(&tenant(), id, &agent("pat"))
        .await
        .unwrap();
}

// ---- a message that cannot be read -------------------------------------------

#[tokio::test]
async fn a_message_that_cannot_be_read_is_skipped_when_the_state_is_rebuilt() {
    let rig = Rig::new();
    let first = rig.publish_item("first").await;
    let claim = rig.pull("one").await.unwrap(); // facts.work 0: claimed
    rig.ack("one", &first, &claim.lease_id).await.unwrap(); // 1: acked
    rig.publish_item("second").await;
    rig.pull("one").await.unwrap(); // 2: claimed
    rig.publish_item("third").await;

    // The ack cannot be read back. Everything else still can.
    rig.broker.corrupt("facts.work", 1);
    let rig = rig.restart().await;
    // The third is claimable, the second is leased, and no request fails.
    assert_eq!(rig.pull("two").await.unwrap().item.attempt, Some(1));
    assert!(rig.pull("two").await.is_none());
}

// ---- what a restart keeps --------------------------------------------------

#[tokio::test]
async fn an_acknowledged_alert_leaves_only_its_id_in_memory() {
    fn check(held: &TenantState, open: EventId, done: EventId) {
        assert_eq!(held.alerts.keys().copied().collect::<Vec<_>>(), [open]);
        assert!(held.acknowledged_alerts.contains(&done));
    }

    let rig = Rig::new();
    let open = publish_alert(&rig).await;
    let done = publish_alert(&rig).await;
    rig.engine
        .alerts_ack(&tenant(), done, &agent("pat"))
        .await
        .unwrap();
    check(&rig.engine.lock(&tenant()).await.unwrap(), open, done);
    let rig = rig.restart().await;
    check(&rig.engine.lock(&tenant()).await.unwrap(), open, done);
}

#[tokio::test]
async fn a_fact_rebuilt_after_a_restart_is_the_one_the_crashed_server_wrote() {
    // A retry whose fact the sink recorded and the broker never got.
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull("one").await.unwrap();
    rig.fail("one", &item, &claim.lease_id).await.unwrap();
    rig.broker.fail("mansio.work.retried");
    rig.pass(2).await;
    let recorded = *rig.sink.topic("facts.work").last().unwrap();
    let republished = rig.read(&Topic::Work(queue())).await.remove(1);

    rig.broker.heal();
    let rig = rig.restart().await;
    rig.pass(1).await;
    let facts = rig.read(&Topic::facts_work()).await;
    let retried = facts.last().unwrap();
    assert_eq!(retried.event_type.as_str(), "mansio.work.retried");
    assert_eq!(
        retried.id, recorded,
        "the same fact, so a consumer sees one"
    );
    assert_eq!(retried.time, republished.time);
    assert_eq!(rig.sink.topic("facts.work").last(), Some(&recorded));
}

#[tokio::test]
async fn a_dead_fact_rebuilt_after_a_restart_is_the_one_the_crashed_server_wrote() {
    let rig = failing_last_attempt("mansio.work.dead").await;
    let recorded = *rig.sink.topic("facts.work").last().unwrap();
    rig.broker.heal();
    let rig = rig.restart().await;
    rig.pass(1).await;
    let facts = rig.read(&Topic::facts_work()).await;
    let dead = facts.last().unwrap();
    assert_eq!(dead.event_type.as_str(), "mansio.work.dead");
    assert_eq!(dead.id, recorded);
}

// ---- what a failure costs ------------------------------------------------------

/// Counts the rebuilds of a tenant's state (one `topics` call each) and the
/// polls, and otherwise leaves everything to the broker under it.
#[derive(Debug)]
struct Counting {
    inner: Arc<FlakyBroker>,
    rebuilds: AtomicU64,
    polls: AtomicU64,
}

impl Counting {
    fn new(inner: Arc<FlakyBroker>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            rebuilds: AtomicU64::new(0),
            polls: AtomicU64::new(0),
        })
    }

    fn rebuilds(&self) -> u64 {
        self.rebuilds.load(Ordering::SeqCst)
    }

    fn polls(&self) -> u64 {
        self.polls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Broker for Counting {
    async fn tenants(&self) -> Result<Vec<TenantId>, BrokerError> {
        self.inner.tenants().await
    }

    async fn topics(&self, tenant: &TenantId) -> Result<Vec<Topic>, BrokerError> {
        self.rebuilds.fetch_add(1, Ordering::SeqCst);
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
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.inner.poll(caller, tenant, topic, offset, max).await
    }
}

/// An engine over a counting broker and a clock moved by hand.
fn counted_engine(page: u32) -> (Engine, Arc<FlakyBroker>, Arc<Counting>, Arc<ManualClock>) {
    let flaky: Arc<FlakyBroker> = Arc::default();
    let counting = Counting::new(Arc::clone(&flaky));
    let clock = Arc::new(ManualClock::new(START));
    let engine = Engine::new(
        counting.clone(),
        Arc::new(LogSink::default()),
        clock.clone(),
        settings(),
        page,
    );
    (engine, flaky, counting, clock)
}

#[tokio::test]
async fn a_broker_that_cannot_be_reached_costs_at_most_one_rebuild_per_tick() {
    let (engine, flaky, counting, clock) = counted_engine(100);
    for n in 0..50 {
        let id = EventId::generate();
        engine
            .work_publish(item_envelope(id, &format!("job-{n}")))
            .await
            .unwrap();
    }
    for n in 0..50 {
        engine
            .work_pull(
                &tenant(),
                &queue(),
                &agent("a"),
                &consumer(&format!("w{n}")),
            )
            .await
            .unwrap()
            .unwrap();
    }

    // Every lease runs out, and nothing can be written.
    flaky.fail("facts.work");
    clock.advance(Duration::from_secs(11));
    for tick in 0..5 {
        let before = counting.rebuilds();
        engine.tick().await;
        let rebuilt = counting.rebuilds() - before;
        assert!(
            rebuilt <= 1,
            "tick {tick}: {rebuilt} rebuilds with 50 tasks failing"
        );
        clock.advance(Duration::from_secs(1));
    }

    // Once it can, every lease is expired and every item comes back.
    flaky.heal();
    for _ in 0..3 {
        engine.tick().await;
        clock.advance(Duration::from_secs(5));
    }
    engine.tick().await;
    let mut claimed = 0;
    while engine
        .work_pull(
            &tenant(),
            &queue(),
            &agent("b"),
            &consumer(&format!("w{claimed}")),
        )
        .await
        .unwrap()
        .is_some()
    {
        claimed += 1;
    }
    assert_eq!(claimed, 50);
}

#[tokio::test]
async fn a_refusal_rebuilds_nothing_and_a_lost_answer_at_most_once_per_tick() {
    for (unknown, most) in [(false, 0), (true, 1)] {
        let (engine, flaky, counting, clock) = counted_engine(100);
        let poison = EventId::generate();
        let mut message = item_envelope(poison, "poison");
        message.event_type = "poison.job".parse().unwrap();
        engine.work_publish(message).await.unwrap();
        let claim = engine
            .work_pull(&tenant(), &queue(), &agent("a"), &consumer("w"))
            .await
            .unwrap()
            .unwrap();
        engine
            .work_fail(
                &tenant(),
                &queue(),
                &poison.to_string().parse().unwrap(),
                Holder {
                    principal: &agent("a"),
                    consumer: &consumer("w"),
                    lease_id: &claim.lease_id,
                },
                "x",
            )
            .await
            .unwrap();
        if unknown {
            flaky.fail("poison.job");
        } else {
            flaky.refuse("poison.job");
        }
        let before = counting.rebuilds();
        let mut worst = 0;
        for _ in 0..20 {
            clock.advance(Duration::from_millis(500));
            let at = counting.rebuilds();
            engine.tick().await;
            worst = worst.max(counting.rebuilds() - at);
        }
        let total = counting.rebuilds() - before;
        assert!(
            worst <= most,
            "unknown outcome {unknown}: {worst} rebuilds in one tick, {total} in 20"
        );
        if !unknown {
            assert_eq!(total, 0, "a refusal is a definite answer");
        }
    }
}

#[tokio::test]
async fn stepping_over_an_unreadable_message_does_not_read_the_page_again() {
    let page = 100;
    let (writer, flaky, counting, _) = counted_engine(page);
    for n in 0..page {
        writer
            .work_publish(item_envelope(EventId::generate(), &format!("job-{n}")))
            .await
            .unwrap();
    }
    // The last message of the first page cannot be read.
    flaky.corrupt("work.builds", u64::from(page) - 1);
    drop(writer);

    let before = counting.polls();
    let reader = Engine::new(
        counting.clone(),
        Arc::new(LogSink::default()),
        Arc::new(ManualClock::new(START)),
        settings(),
        page,
    );
    reader.recover().await.unwrap();
    let polls = counting.polls() - before;
    // The page that failed, one poll for each message in it, and the end.
    assert!(polls <= u64::from(page) + 2, "{polls} polls");
    let claimed = {
        let mut n = 0;
        while reader
            .work_pull(
                &tenant(),
                &queue(),
                &agent("a"),
                &consumer(&format!("w{n}")),
            )
            .await
            .unwrap()
            .is_some()
        {
            n += 1;
        }
        n
    };
    assert_eq!(claimed, page - 1, "every message but the unreadable one");
}

#[tokio::test]
async fn an_alert_keeps_the_time_it_was_accepted_when_the_state_is_rebuilt() {
    let rig = Rig::new();
    let id = EventId::generate();
    // A producer's time from long ago, accepted now.
    rig.engine
        .publish(alert_envelope(id, START - 86_400_000))
        .await
        .unwrap();

    // Something unrelated fails in a way that leaves the outcome unknown, so
    // the state is rebuilt.
    rig.broker.lose_the_answer("work.builds");
    let lost = rig
        .engine
        .work_publish(item_envelope(EventId::generate(), "x"))
        .await;
    assert!(lost.is_err());
    rig.pass(1).await;
    assert!(
        alert_facts(&rig).await.is_empty(),
        "the alert was accepted a second ago"
    );
    rig.pass(28).await;
    assert!(alert_facts(&rig).await.is_empty());
    rig.pass(1).await;
    assert_eq!(alert_facts(&rig).await, ["resurfaced"]);
}

/// The lease id is the holder's proof, and `facts.work` is readable by anyone
/// who may subscribe to `facts.*`: neither it nor the sink holds an id.
#[tokio::test]
async fn no_fact_and_no_sink_record_holds_a_lease_id() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let first = rig.pull("one").await.unwrap();
    rig.fail("one", &item, &first.lease_id).await.unwrap();
    rig.pass(100).await;
    let second = rig.pull("one").await.unwrap();
    rig.extend("one", &item, &second.lease_id, None)
        .await
        .unwrap();
    rig.pass(100).await; // the second lease expires unanswered
    rig.pass(10).await; // and its backoff passes
    let third = rig.pull("one").await.unwrap();
    rig.ack("one", &item, &third.lease_id).await.unwrap();

    let facts = rig.read(&Topic::facts_work()).await;
    let transitions = rig.transitions().await;
    for kind in ["claimed", "extended", "failed", "expired", "acked"] {
        assert!(
            transitions.iter().any(|t| t == kind),
            "{kind} in {transitions:?}"
        );
    }
    for claim in [&first, &second, &third] {
        let id = claim.lease_id.to_string();
        let bare = id.replace('-', "");
        let seen = serde_json::to_string(&facts).unwrap();
        assert!(
            !seen.contains(&id) && !seen.contains(&bare),
            "a fact holds {id}"
        );
        let recorded = serde_json::to_string(&rig.sink.all_envelopes()).unwrap();
        assert!(
            !recorded.contains(&id) && !recorded.contains(&bare),
            "the sink holds {id}"
        );
    }
    // The facts do say which lease each is about.
    let leased: Vec<String> = facts
        .iter()
        .filter_map(|fact| data(fact)["lease"].as_str().map(str::to_owned))
        .collect();
    assert!(leased.contains(&first.lease_id.fingerprint().to_string()));
    assert!(leased.contains(&second.lease_id.fingerprint().to_string()));
    assert!(leased.contains(&third.lease_id.fingerprint().to_string()));
}

/// A lease is held by a principal consuming under a name: another consumer of
/// the same principal, with the right lease id, cannot ack or fail it.
#[tokio::test]
async fn another_consumer_of_the_same_principal_cannot_ack_or_fail_the_lease() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull("one").await.unwrap();
    let before = rig.sink.all().len();
    let principal = agent("one");
    let holder = |consumer| Holder {
        principal: &principal,
        consumer,
        lease_id: &claim.lease_id,
    };
    let others = [consumer("w2"), consumer("W1"), consumer("w")];
    for other in &others {
        let refused = rig
            .engine
            .work_ack(&tenant(), &queue(), &item, holder(other))
            .await;
        assert!(matches!(refused, Err(ApiError::Forbidden(_))), "{other}");
        let refused = rig
            .engine
            .work_fail(&tenant(), &queue(), &item, holder(other), "x")
            .await;
        assert!(matches!(refused, Err(ApiError::Forbidden(_))), "{other}");
    }
    assert_eq!(rig.sink.all().len(), before, "a refusal records nothing");
    rig.ack("one", &item, &claim.lease_id).await.unwrap();
}

// ---- idempotency keys ------------------------------------------------------

fn keyed(key: &str, topic: &str) -> Envelope {
    let mut message = Envelope::new(
        EventId::generate(),
        "test".parse().unwrap(),
        "build.finished".parse().unwrap(),
        tenant(),
        topic.parse().unwrap(),
        agent("one"),
    );
    message.idempotency_key = Some(key.parse().unwrap());
    message
}

async fn publish_keyed(rig: &Rig, key: &str, topic: &str) -> (Published, bool) {
    rig.engine.publish_once(keyed(key, topic)).await.unwrap()
}

#[tokio::test]
async fn a_repeated_key_gets_the_first_answer_and_stores_nothing() {
    let rig = Rig::new();
    let (first, repeat) = publish_keyed(&rig, "k", "facts.build").await;
    assert!(!repeat);
    let (again, repeat) = publish_keyed(&rig, "k", "facts.build").await;
    assert!(repeat);
    assert_eq!(again, first);
    assert_eq!(rig.read(&"facts.build".parse().unwrap()).await.len(), 1);
    assert_eq!(rig.sink.topic("facts.build").len(), 1);

    // Another topic, another key, and no key at all, publish.
    let mut unkeyed = keyed("k", "facts.build");
    unkeyed.idempotency_key = None;
    for message in [
        keyed("k", "facts.deploy"),
        keyed("other", "facts.build"),
        unkeyed.clone(),
        unkeyed,
    ] {
        let (_, repeat) = rig.engine.publish_once(message).await.unwrap();
        assert!(!repeat);
    }
}

#[tokio::test]
async fn a_key_is_forgotten_after_the_window() {
    let rig = Rig::new();
    publish_keyed(&rig, "k", "facts.build").await;
    rig.clock.advance(Duration::from_secs(59));
    assert!(publish_keyed(&rig, "k", "facts.build").await.1);
    rig.clock.advance(Duration::from_secs(1));
    assert!(!publish_keyed(&rig, "k", "facts.build").await.1);
    assert_eq!(rig.read(&"facts.build".parse().unwrap()).await.len(), 2);
}

#[tokio::test]
async fn the_oldest_key_is_forgotten_past_the_bound() {
    let rig = Rig::new();
    for key in ["a", "b", "c", "d"] {
        publish_keyed(&rig, key, "facts.build").await;
    }
    // The bound is 3: "a" is gone, "d" is not.
    assert!(publish_keyed(&rig, "d", "facts.build").await.1);
    assert!(!publish_keyed(&rig, "a", "facts.build").await.1);
}

#[tokio::test]
async fn a_restarted_server_answers_with_the_first_inside_the_window_and_the_bound() {
    let rig = Rig::new();
    let (first, _) = publish_keyed(&rig, "k", "facts.build").await;
    rig.clock.advance(Duration::from_secs(10));
    let rig = rig.restart().await;
    let (again, repeat) = publish_keyed(&rig, "k", "facts.build").await;
    assert!(repeat);
    assert_eq!(again, first);
    assert_eq!(rig.read(&"facts.build".parse().unwrap()).await.len(), 1);

    // The window counts from the first publish, across the restart.
    rig.clock.advance(Duration::from_secs(50));
    let rig = rig.restart().await;
    assert!(!publish_keyed(&rig, "k", "facts.build").await.1);

    // Past the bound, the oldest did not survive and the newest did.
    for key in ["a", "b", "c", "d"] {
        publish_keyed(&rig, key, "facts.deploy").await;
    }
    let rig = rig.restart().await;
    assert!(publish_keyed(&rig, "d", "facts.deploy").await.1);
    assert!(!publish_keyed(&rig, "a", "facts.deploy").await.1);
}

#[tokio::test]
async fn a_log_that_cannot_be_read_fails_keyed_publishes_and_forgets_nothing() {
    let rig = Rig::new();
    let (first, _) = publish_keyed(&rig, "k", "facts.build").await;
    rig.broker.system_unreadable.store(true, Ordering::SeqCst);
    let restarted = Rig::over(rig.broker.clone(), rig.sink.clone(), rig.clock.clone());
    restarted.engine.recover().await.unwrap();
    let error = restarted
        .engine
        .publish_once(keyed("k", "facts.build"))
        .await
        .unwrap_err();
    assert!(matches!(error, ApiError::BadGateway(_)), "{error:?}");
    // A publish without a key is not affected.
    let mut unkeyed = keyed("unused", "facts.build");
    unkeyed.idempotency_key = None;
    restarted.engine.publish(unkeyed).await.unwrap();
    restarted
        .broker
        .system_unreadable
        .store(false, Ordering::SeqCst);
    let (again, repeat) = publish_keyed(&restarted, "k", "facts.build").await;
    assert!(repeat);
    assert_eq!(again, first);
}

#[tokio::test]
async fn a_repeat_reads_the_first_body_back_and_the_server_keeps_none() {
    let rig = Rig::new();
    let mut message = keyed("big", "facts.build");
    message.data = Some(Data::new(&serde_json::json!({ "blob": "x".repeat(200_000) })).unwrap());
    let (first, _) = rig.engine.publish_once(message.clone()).await.unwrap();
    // What is remembered is a pointer, however large the body.
    let held = rig.engine.lock(&tenant()).await.unwrap();
    let remembered = held
        .keys
        .get(
            &"facts.build".parse().unwrap(),
            &"big".parse().unwrap(),
            rig.clock.now_millis(),
            60_000,
        )
        .unwrap()
        .clone();
    drop(held);
    assert!(format!("{remembered:?}").len() < 1_000);
    // The repeat still answers with the whole first envelope.
    let mut again = message;
    again.id = EventId::generate();
    again.data = None;
    let (answer, repeat) = rig.engine.publish_once(again).await.unwrap();
    assert!(repeat);
    assert_eq!(answer, first);
    assert!(answer.envelope.data.is_some());
}

#[tokio::test]
async fn the_timer_drops_keys_past_their_window() {
    let rig = Rig::new();
    publish_keyed(&rig, "k", "facts.build").await;
    let tenant = tenant();
    assert_eq!(rig.engine.lock(&tenant).await.unwrap().keys.len(), 1);
    rig.clock.advance(Duration::from_secs(61));
    rig.engine.tick().await;
    assert_eq!(rig.engine.lock(&tenant).await.unwrap().keys.len(), 0);
}

#[tokio::test]
async fn concurrent_publishes_with_one_key_store_one_message() {
    let rig = Rig::new();
    let mut publishes = Vec::new();
    for _ in 0..8 {
        let engine = Arc::clone(&rig.engine);
        publishes.push(tokio::spawn(async move {
            engine
                .publish_once(keyed("k", "facts.build"))
                .await
                .unwrap()
        }));
    }
    let mut firsts = 0;
    for publish in publishes {
        let (_, repeat) = publish.await.unwrap();
        firsts += usize::from(!repeat);
    }
    assert_eq!(firsts, 1);
    assert_eq!(rig.read(&"facts.build".parse().unwrap()).await.len(), 1);
}

#[tokio::test]
async fn another_principal_never_gets_the_first_envelope() {
    let rig = Rig::new();
    let (first, _) = publish_keyed(&rig, "k", "facts.build").await;
    let mut other = keyed("k", "facts.build");
    other.principal = agent("two");
    let error = rig.engine.publish_once(other).await.unwrap_err();
    assert!(matches!(error, ApiError::Conflict(_)), "{error:?}");
    let text = format!("{error:?}");
    assert!(
        !text.contains(&first.envelope.id.to_string()),
        "the refusal must not carry the first envelope: {text}"
    );
    assert_eq!(rig.read(&"facts.build".parse().unwrap()).await.len(), 1);
    // The first publisher still gets its own answer.
    assert!(publish_keyed(&rig, "k", "facts.build").await.1);
}

#[tokio::test]
async fn a_key_whose_record_cannot_be_written_is_answered_and_kept_in_memory() {
    let rig = Rig::new();
    rig.broker.fail_system_appends();
    let (first, repeat) = publish_keyed(&rig, "k", "facts.build").await;
    assert!(!repeat);
    let (again, repeat) = publish_keyed(&rig, "k", "facts.build").await;
    assert!(repeat);
    assert_eq!(again, first);
    assert_eq!(rig.read(&"facts.build".parse().unwrap()).await.len(), 1);
}

#[tokio::test]
async fn a_publish_that_was_refused_does_not_use_up_its_key() {
    let rig = Rig::new();
    rig.broker.refuse("facts.build");
    assert!(rig.engine.publish(keyed("k", "facts.build")).await.is_err());
    rig.broker.heal();
    assert!(!publish_keyed(&rig, "k", "facts.build").await.1);
}

// ---- extending a lease -----------------------------------------------------

/// Where the lease ends, as an `extended` fact says.
fn ends_at(fact: &Envelope) -> i64 {
    serde_json::from_value::<Timestamp>(data(fact)["expires_at"].clone())
        .unwrap()
        .unix_millis()
        .unwrap()
}

fn secs_after_start(seconds: i64) -> i64 {
    START + seconds * 1000
}

#[tokio::test]
async fn the_holder_extends_its_lease_and_the_new_end_is_recorded() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();
    assert_eq!(claim.expires_at.unix_millis(), Some(secs_after_start(10)));

    // By the lease setting from now.
    rig.clock.advance(Duration::from_secs(5));
    let extended = rig
        .extend("one", &item, &claim.lease_id, None)
        .await
        .unwrap();
    assert_eq!(
        extended.envelope.event_type.as_str(),
        "mansio.work.extended"
    );
    assert_eq!(extended.envelope.principal, agent("one"));
    assert_eq!(extended.envelope.item_id, Some(item.clone()));
    assert_eq!(extended.envelope.attempt, Some(1));
    assert_eq!(ends_at(&extended.envelope), secs_after_start(15));
    let fact = data(&extended.envelope);
    assert_eq!(fact["queue"], "builds");
    assert_eq!(fact["holder"], "agent:one");
    assert_eq!(fact["consumer"], "worker");
    assert_eq!(fact["lease"], claim.lease_id.fingerprint().to_string());
    assert_eq!(fact["message_id"], claim.item.id.to_string());

    // The sink has it, and it is the fact on the topic.
    let facts = rig.read(&Topic::facts_work()).await;
    assert_eq!(facts.last(), Some(&extended.envelope));
    let recorded = rig.sink.all_envelopes();
    let in_sink = recorded
        .iter()
        .find(|e| e.id == extended.envelope.id)
        .expect("the sink recorded the fact");
    assert_eq!(ends_at(in_sink), secs_after_start(15));

    // The holder is told the new end when it asks for its lease again, and
    // nobody else can take the item at the old end.
    let again = rig.pull_as("one", "worker").await.unwrap();
    assert_eq!(again.lease_id, claim.lease_id);
    assert_eq!(again.expires_at.unix_millis(), Some(secs_after_start(15)));
    rig.pass(6).await;
    assert!(rig.pull("two").await.is_none());
    assert_eq!(rig.transitions().await, ["claimed", "extended"]);

    // A duration of its own counts from now.
    let extended = rig
        .extend("one", &item, &claim.lease_id, Some(40))
        .await
        .unwrap();
    assert_eq!(ends_at(&extended.envelope), secs_after_start(11 + 40));
    // An extension never shortens a lease.
    let extended = rig
        .extend("one", &item, &claim.lease_id, Some(1))
        .await
        .unwrap();
    assert_eq!(ends_at(&extended.envelope), secs_after_start(11 + 40));

    // Acking ends it as ever, with the extended lease's fingerprint.
    let acked = rig.ack("one", &item, &claim.lease_id).await.unwrap();
    assert_eq!(ends_at(&acked.envelope), secs_after_start(11 + 40));
    assert_eq!(
        rig.transitions().await,
        ["claimed", "extended", "extended", "extended", "acked"]
    );
}

#[tokio::test]
async fn an_extension_never_runs_past_the_end_of_the_holders_credential() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();
    let credential_ends = secs_after_start(30);
    let extend = |seconds: u64, not_after: Option<i64>| {
        let (engine, item, lease) = (rig.engine.clone(), item.clone(), claim.lease_id);
        async move {
            engine
                .work_extend(
                    &tenant(),
                    &queue(),
                    &item,
                    Holder {
                        principal: &agent("one"),
                        consumer: &taken_as(&lease),
                        lease_id: &lease,
                    },
                    Some(Duration::from_secs(seconds)),
                    not_after,
                )
                .await
                .unwrap()
        }
    };

    // Within the credential's life it is as asked.
    let extended = extend(20, Some(credential_ends)).await;
    assert_eq!(ends_at(&extended.envelope), secs_after_start(20));
    // Asking for more is capped at the credential's end, not the lease max.
    let extended = extend(100, Some(credential_ends)).await;
    assert_eq!(ends_at(&extended.envelope), credential_ends);
    // Without an expiry only the lease max caps it.
    let extended = extend(100, None).await;
    assert_eq!(ends_at(&extended.envelope), secs_after_start(100));
    // An end that is already behind the lease never shortens it.
    let extended = extend(5, Some(credential_ends)).await;
    assert_eq!(ends_at(&extended.envelope), secs_after_start(100));
}

#[tokio::test]
async fn ending_a_holders_leases_records_them_expired_as_revoked_and_retries_the_items() {
    let rig = Rig::new();
    let first = rig.publish_item("one").await;
    let second = rig.publish_item("two").await;
    let third = rig.publish_item("three").await;
    let a = rig.pull_as("gone", "w-a").await.unwrap();
    let b = rig.pull_as("gone", "w-b").await.unwrap();
    let kept = rig.pull_as("other", "w-c").await.unwrap();
    // One of them asked for a long extension.
    rig.extend("gone", &first, &a.lease_id, Some(100))
        .await
        .unwrap();

    let ended = rig
        .engine
        .end_leases_of(&tenant(), &agent("gone"))
        .await
        .unwrap();
    assert_eq!(ended, 2);

    let facts = rig.read(&Topic::facts_work()).await;
    let expired: Vec<&Envelope> = facts
        .iter()
        .filter(|fact| fact.event_type.as_str() == "mansio.work.expired")
        .collect();
    assert_eq!(expired.len(), 2);
    for fact in &expired {
        let fact = data(fact);
        assert_eq!(fact["reason"], "revoked");
        assert_eq!(fact["holder"], "agent:gone");
        assert_eq!(fact["next"], "retry");
    }

    // The other principal's lease stands, and the revoked holder cannot use
    // its own any more.
    assert!(matches!(
        rig.ack("gone", &first, &a.lease_id).await,
        Err(ApiError::Conflict(_))
    ));
    assert!(matches!(
        rig.extend("gone", &second, &b.lease_id, None).await,
        Err(ApiError::Conflict(_))
    ));
    rig.ack("other", &third, &kept.lease_id).await.unwrap();

    // Nothing is claimable until the backoff is over, and then both are.
    assert!(rig.pull("two").await.is_none());
    rig.pass(3).await;
    let again = rig.pull("two").await.unwrap();
    assert_eq!(again.item.attempt, Some(2));
    let more = rig.pull("two").await.unwrap();
    assert_eq!(more.item.attempt, Some(2));
    assert!(rig.pull("two").await.is_none());

    // Nothing is left to end.
    assert_eq!(
        rig.engine
            .end_leases_of(&tenant(), &agent("gone"))
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn a_lease_that_ran_out_is_left_to_the_timer_when_its_holder_is_revoked() {
    let rig = Rig::new();
    rig.publish_item("job").await;
    rig.pull_as("gone", "worker").await.unwrap();
    rig.clock.advance(Duration::from_secs(11));
    let ended = rig
        .engine
        .end_leases_of(&tenant(), &agent("gone"))
        .await
        .unwrap();
    assert_eq!(ended, 0);
    rig.engine.tick().await;
    let facts = rig.read(&Topic::facts_work()).await;
    let expired = facts
        .iter()
        .find(|fact| fact.event_type.as_str() == "mansio.work.expired")
        .unwrap();
    assert!(data(expired).get("reason").is_none());
}

#[tokio::test]
async fn anyone_but_the_holder_is_refused_an_extension_as_for_an_ack_and_nothing_changes() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();
    let before = rig.sink.all().len();

    let stranger = LeaseId::generate();
    assert!(matches!(
        rig.extend("one", &item, &stranger, None).await,
        Err(ApiError::Forbidden(_))
    ));
    // Someone else with the right lease id.
    assert!(matches!(
        rig.extend("two", &item, &claim.lease_id, None).await,
        Err(ApiError::Forbidden(_))
    ));
    // The same principal under another consumer name.
    let principal = agent("one");
    let other = consumer("worker-2");
    let refused = rig
        .engine
        .work_extend(
            &tenant(),
            &queue(),
            &item,
            Holder {
                principal: &principal,
                consumer: &other,
                lease_id: &claim.lease_id,
            },
            None,
            None,
        )
        .await;
    assert!(
        matches!(refused, Err(ApiError::Forbidden(_))),
        "{refused:?}"
    );
    let unknown: ItemId = "no-such-item".parse().unwrap();
    assert!(matches!(
        rig.extend("one", &unknown, &claim.lease_id, None).await,
        Err(ApiError::NotFound(_))
    ));
    assert_eq!(rig.sink.all().len(), before, "a refusal records nothing");
    assert_eq!(rig.transitions().await, ["claimed"]);

    // None of it moved the end: the item is expired and claimed again on time.
    rig.pass(10).await;
    rig.pass(2).await;
    assert_eq!(
        rig.transitions().await,
        ["claimed", "expired", "retried"],
        "the lease ended when it was given to"
    );
}

#[tokio::test]
async fn a_lease_that_has_ended_is_not_extended_and_neither_is_one_nobody_holds() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();

    // The end has passed and the timer has not yet noticed: 409, and the
    // item is not saved.
    rig.clock.advance(Duration::from_secs(10));
    assert!(matches!(
        rig.extend("one", &item, &claim.lease_id, None).await,
        Err(ApiError::Conflict(_))
    ));
    assert_eq!(rig.transitions().await, ["claimed"]);
    rig.engine.tick().await;
    assert_eq!(rig.transitions().await, ["claimed", "expired"]);

    // Waiting out its backoff: nobody holds it.
    assert!(matches!(
        rig.extend("one", &item, &claim.lease_id, None).await,
        Err(ApiError::Conflict(_))
    ));
    rig.pass(2).await;
    assert!(matches!(
        rig.extend("one", &item, &claim.lease_id, None).await,
        Err(ApiError::Conflict(_))
    ));

    // Once it is done it is gone.
    let second = rig.pull_as("two", "worker").await.unwrap();
    rig.ack("two", &item, &second.lease_id).await.unwrap();
    assert!(matches!(
        rig.extend("two", &item, &second.lease_id, None).await,
        Err(ApiError::NotFound(_))
    ));
    assert_eq!(
        rig.transitions().await,
        ["claimed", "expired", "retried", "claimed", "acked"]
    );
}

#[tokio::test]
async fn an_extension_is_capped_at_the_longest_lease_counted_from_the_claim() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();
    let cap = secs_after_start(120);

    // Asking for more than there is to give is capped, not refused.
    rig.clock.advance(Duration::from_secs(5));
    let extended = rig
        .extend("one", &item, &claim.lease_id, Some(1_000_000))
        .await
        .unwrap();
    assert_eq!(ends_at(&extended.envelope), cap);
    let huge = rig
        .extend("one", &item, &claim.lease_id, Some(u64::MAX))
        .await
        .unwrap();
    assert_eq!(ends_at(&huge.envelope), cap);

    // Extending again, later, does not move the cap, which is from the claim.
    rig.clock.advance(Duration::from_secs(100));
    let near = rig
        .extend("one", &item, &claim.lease_id, None)
        .await
        .unwrap();
    assert_eq!(ends_at(&near.envelope), cap, "10 s more would be 115 s");
    rig.clock.advance(Duration::from_secs(10));
    let last = rig
        .extend("one", &item, &claim.lease_id, None)
        .await
        .unwrap();
    assert_eq!(ends_at(&last.envelope), cap);

    // At the cap the lease ends like any other: the holder is refused and the
    // timer expires the item.
    rig.clock.advance(Duration::from_secs(5));
    assert!(matches!(
        rig.extend("one", &item, &claim.lease_id, None).await,
        Err(ApiError::Conflict(_))
    ));
    rig.engine.tick().await;
    assert_eq!(
        rig.transitions().await,
        [
            "claimed", "extended", "extended", "extended", "extended", "expired"
        ]
    );
}

#[tokio::test]
async fn a_consumer_that_extends_every_half_lease_keeps_its_item_for_many_leases_and_acks_it_once()
{
    let rig = Rig::new();
    let item = rig.publish_item("build").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();

    // Ten lease lengths, each half lease extended. Nobody else is given it,
    // and it is not expired, however often the timer looks.
    for half in 1..=20 {
        rig.pass(5).await;
        assert!(
            rig.pull_as("two", "other").await.is_none(),
            "another consumer was given the item at half lease {half}"
        );
        rig.extend("one", &item, &claim.lease_id, None)
            .await
            .unwrap();
    }
    let transitions = rig.transitions().await;
    assert_eq!(transitions.len(), 21, "{transitions:?}");
    assert_eq!(transitions[0], "claimed");
    assert!(transitions[1..].iter().all(|t| t == "extended"));

    // And it acks it once.
    rig.ack("one", &item, &claim.lease_id).await.unwrap();
    assert!(matches!(
        rig.ack("one", &item, &claim.lease_id).await,
        Err(ApiError::NotFound(_))
    ));
    assert_eq!(rig.transitions().await.last().unwrap(), "acked");
    assert_eq!(
        rig.read(&Topic::Work(queue())).await.len(),
        1,
        "the item was never republished"
    );
}

#[tokio::test]
async fn a_restart_keeps_the_extended_lease_and_the_cap_counted_from_the_claim() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();
    rig.pass(5).await;
    rig.extend("one", &item, &claim.lease_id, None)
        .await
        .unwrap();
    rig.pass(5).await;
    let extended = rig
        .extend("one", &item, &claim.lease_id, Some(20))
        .await
        .unwrap();
    assert_eq!(ends_at(&extended.envelope), secs_after_start(30));

    // The replay takes the latest end: the lease first ran out at 10 s, and
    // the first extension ended it at 15 s.
    let rig = rig.restart().await;
    rig.pass(10).await; // 20 s
    assert!(rig.pull("two").await.is_none());
    assert_eq!(rig.transitions().await, ["claimed", "extended", "extended"]);

    // The holder is recognised by what the facts hold, and its lease runs to
    // the end it was extended to.
    let again = rig.pull_as("one", "worker").await.unwrap();
    assert_eq!(again.item, claim.item);
    assert_eq!(again.expires_at.unix_millis(), Some(secs_after_start(30)));
    assert_ne!(
        again.lease_id, claim.lease_id,
        "a restart cannot say the id"
    );

    // The cap is still counted from the claim, not from the restart.
    let capped = rig
        .extend("one", &item, &again.lease_id, Some(1_000_000))
        .await
        .unwrap();
    assert_eq!(ends_at(&capped.envelope), secs_after_start(120));

    // And that holds across another restart, with the new lease's fingerprint.
    let rig = rig.restart().await;
    rig.pass(90).await; // 110 s
    assert!(rig.pull("two").await.is_none());
    let more = rig
        .extend("one", &item, &again.lease_id, Some(1_000_000))
        .await
        .unwrap();
    assert_eq!(ends_at(&more.envelope), secs_after_start(120));
    rig.pass(10).await; // 120 s: ended
    assert_eq!(
        rig.transitions().await.last().map(String::as_str),
        Some("expired")
    );
}

#[tokio::test]
async fn a_restart_finds_a_lease_that_was_extended_and_then_ran_out_expired_once() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();
    rig.pass(8).await;
    rig.extend("one", &item, &claim.lease_id, None)
        .await
        .unwrap();

    // Down past the original end but not the extended one: still held. The
    // old id still works, as the fingerprint is the same.
    let rig = rig.restart().await;
    rig.pass(5).await; // 13 s
    assert!(rig.pull("two").await.is_none());
    rig.extend("one", &item, &claim.lease_id, None)
        .await
        .unwrap();

    // Down past the extended end: expired on start, once.
    let rig = rig.restart().await;
    rig.pass(20).await;
    rig.pass(2).await;
    assert_eq!(
        rig.transitions().await,
        ["claimed", "extended", "extended", "expired", "retried"]
    );
}

#[tokio::test]
async fn an_extension_that_cannot_be_recorded_changes_nothing() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();
    rig.pass(5).await;

    rig.broker.refuse("mansio.work.extended");
    let refused = rig.extend("one", &item, &claim.lease_id, None).await;
    assert!(
        matches!(refused, Err(ApiError::BadGateway(_))),
        "{refused:?}"
    );
    rig.broker.fail("mansio.work.extended");
    let refused = rig.extend("one", &item, &claim.lease_id, None).await;
    assert!(
        matches!(refused, Err(ApiError::Unavailable(_))),
        "{refused:?}"
    );
    rig.broker.heal();
    assert_eq!(rig.transitions().await, ["claimed"]);
    // The sink is written first: it has both attempts, which the topic does
    // not, as it does for every transition.
    assert_eq!(rig.sink.topic("facts.work").len(), 3);

    // The lease ends where it did.
    rig.pass(5).await;
    assert_eq!(rig.transitions().await, ["claimed", "expired"]);
}

#[tokio::test]
async fn an_extension_that_was_stored_but_errored_still_extends_the_lease() {
    let rig = Rig::new();
    let item = rig.publish_item("job").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();
    rig.pass(5).await;

    rig.broker.lose_the_answer("mansio.work.extended");
    let refused = rig.extend("one", &item, &claim.lease_id, None).await;
    assert!(matches!(refused, Err(ApiError::Unavailable(_))));

    // The state is rebuilt from the logs, which have the extension: the lease
    // runs to 15 s, and the holder is still the holder.
    rig.pass(7).await;
    assert_eq!(rig.transitions().await, ["claimed", "extended"]);
    assert!(rig.pull("two").await.is_none());
    rig.ack("one", &item, &claim.lease_id).await.unwrap();
}

/// The longest lease is counted from the claim, and a restart finds the claim
/// in the `claimed` fact. A clock that moves between two reads must not put
/// the fact a millisecond after the moment the lease says it began.
#[tokio::test]
async fn a_fresh_claim_is_recorded_at_the_moment_its_lease_began_and_the_cap_survives_a_restart() {
    let rig = Rig::over(
        Arc::default(),
        Arc::default(),
        Arc::new(ManualClock::ticking(START)),
    );
    let item = rig.publish_item("job").await;
    let claim = rig.pull_as("one", "worker").await.unwrap();
    let claimed_at = claim.expires_at.unix_millis().unwrap() - 10_000;

    let facts = rig.read(&Topic::facts_work()).await;
    assert_eq!(
        facts[0].time.as_ref().and_then(Timestamp::unix_millis),
        Some(claimed_at),
        "the claimed fact is timed at the claim"
    );

    let rig = rig.restart().await;
    let capped = rig
        .extend("one", &item, &claim.lease_id, Some(1_000_000))
        .await
        .unwrap();
    assert_eq!(ends_at(&capped.envelope), claimed_at + 120_000);

    // A lease read back and given a new id is recorded again, and the cap
    // still counts from the first claim.
    let rig = rig.restart().await;
    let again = rig.pull_as("one", "worker").await.unwrap();
    let capped = rig
        .extend("one", &item, &again.lease_id, Some(1_000_000))
        .await
        .unwrap();
    assert_eq!(ends_at(&capped.envelope), claimed_at + 120_000);
}

#[tokio::test]
async fn a_recovery_that_keeps_failing_is_logged_once_per_interval_and_backs_off() {
    // The setting is 30 s and the tick 100 ms.
    let rig = Rig::new();
    rig.broker.tenants_refused.store(true, Ordering::SeqCst);
    let mut retry = RecoveryRetry::default();
    let asked = || rig.broker.tenants_asked.load(Ordering::SeqCst);

    // The first failure is logged, and the broker is not asked again at once.
    assert_eq!(
        rig.engine.try_recover(&mut retry).await,
        RecoveryAttempt::Failed { logged: true }
    );
    assert_eq!(
        rig.engine.try_recover(&mut retry).await,
        RecoveryAttempt::Waiting
    );
    assert_eq!(asked(), 1);

    // Ticks of 100 ms for two minutes: asks back off and logs are 30 s apart.
    let mut logs = 1;
    let mut last_wait = 0;
    let mut previous_ask = rig.clock.now_millis();
    for _ in 0..1200 {
        rig.clock.advance(Duration::from_millis(100));
        let before = asked();
        match rig.engine.try_recover(&mut retry).await {
            RecoveryAttempt::Failed { logged } => {
                logs += i32::from(logged);
                let wait = rig.clock.now_millis() - previous_ask;
                assert!(
                    wait >= last_wait,
                    "waits do not shrink: {wait} < {last_wait}"
                );
                last_wait = wait;
                previous_ask = rig.clock.now_millis();
            }
            RecoveryAttempt::Waiting => assert_eq!(asked(), before),
            RecoveryAttempt::Recovered => panic!("the broker still refuses"),
        }
    }
    assert!(logs <= 5, "{logs} logs in two minutes of a 30 s interval");
    assert!(logs >= 4, "{logs} logs: one per interval is still said");
    assert!(
        asked() < 40,
        "{} asks in 1200 ticks: not backed off",
        asked()
    );

    // Once the broker lists its tenants again, the recovery completes.
    rig.broker.tenants_refused.store(false, Ordering::SeqCst);
    rig.clock.advance(Duration::from_secs(31));
    assert_eq!(
        rig.engine.try_recover(&mut retry).await,
        RecoveryAttempt::Recovered
    );
}

/// Seed a tenant with `n` open items (a third claimable, a third leased and a
/// third in backoff, all with nothing due) and `n / 10` alerts, directly in
/// the engine's state.
fn seed_idle_tenant(engine: &Engine, n: usize) {
    let mut state = TenantState {
        loaded: true,
        keys_loaded: true,
        ..TenantState::default()
    };
    let far = START + 3_600_000;
    for i in 0..n {
        let id = EventId::generate();
        let item: ItemId = id.to_string().parse().unwrap();
        let phase = match i % 3 {
            0 => Phase::Ready,
            1 => Phase::Leased(Lease {
                fingerprint: LeaseId::generate().fingerprint(),
                id: None,
                holder: agent("worker"),
                consumer: consumer("w1"),
                expires_at: far + i64::try_from(i).unwrap(),
                claimed_at: START,
            }),
            _ => Phase::Backoff {
                due: far + i64::try_from(i).unwrap(),
                next: Next::Retry,
            },
        };
        state
            .queue_mut(&queue())
            .insert(item, item_envelope(id, "bench"), phase);
    }
    for _ in 0..n / 10 {
        state.alerts.insert(EventId::generate(), START);
    }
    engine
        .tenants
        .lock()
        .unwrap()
        .insert(tenant(), Arc::new(Mutex::new(state)));
}

/// A tick with nothing due finds nothing through the due-time indexes and
/// looks at one entry per index, with 100k open items: a return to scanning
/// the items would raise the count.
#[tokio::test]
async fn a_tick_with_nothing_due_does_constant_work_at_100k_open_items() {
    let rig = Rig::new();
    seed_idle_tenant(&rig.engine, 100_000);
    let cell = rig
        .engine
        .tenants
        .lock()
        .unwrap()
        .get(&tenant())
        .cloned()
        .unwrap();
    let mut held = cell.lock_owned().await;
    let now = rig.clock.now_millis();
    let (tasks, looked) = rig.engine.due_counted(&held, now);
    assert!(tasks.is_empty());
    // Leases and backoffs of the one queue, and the alerts. Owed entries
    // count as due work, since an owed fact is due at once; none are owed
    // here, so they add nothing.
    assert_eq!(looked, 3);
    rig.engine.tick_tenant(&mut held, &tenant()).await;
}

/// Tick time and lock hold time of a tick with nothing due, at three queue
/// depths. Run with
/// `cargo test -p mansio-server --release -- --ignored --nocapture tick_benchmark`.
#[tokio::test]
#[ignore = "a benchmark: run it by name"]
async fn tick_benchmark() {
    use std::time::Instant;
    const TICKS: u32 = 50;
    for n in [1_000, 10_000, 100_000] {
        let rig = Rig::new();
        seed_idle_tenant(&rig.engine, n);
        rig.engine.tick().await;
        let started = Instant::now();
        for _ in 0..TICKS {
            rig.engine.tick().await;
        }
        let tick = started.elapsed() / TICKS;

        // The tick's critical section: the tenant lock, held from taking it
        // to the end of the pass.
        let cell = rig
            .engine
            .tenants
            .lock()
            .unwrap()
            .get(&tenant())
            .cloned()
            .unwrap();
        let mut hold = Duration::ZERO;
        for _ in 0..TICKS {
            let mut held = cell.clone().lock_owned().await;
            let taken = Instant::now();
            rig.engine.tick_tenant(&mut held, &tenant()).await;
            hold += taken.elapsed();
        }
        println!(
            "open_items={n} tick_nanos={} lock_hold_nanos={}",
            tick.as_nanos(),
            (hold / TICKS).as_nanos()
        );
    }
}

// ---- snapshots -------------------------------------------------------------

/// Snapshots kept in memory.
#[derive(Debug, Default)]
struct MemorySnapshots {
    kept: StdMutex<std::collections::HashMap<TenantId, Vec<u8>>>,
}

impl MemorySnapshots {
    fn put(&self, bytes: &[u8]) {
        self.kept.lock().unwrap().insert(tenant(), bytes.to_vec());
    }

    fn get(&self) -> Option<Vec<u8>> {
        self.kept.lock().unwrap().get(&tenant()).cloned()
    }
}

#[async_trait]
impl Snapshots for MemorySnapshots {
    async fn read(&self, tenant: &TenantId) -> std::io::Result<Option<Vec<u8>>> {
        Ok(self.kept.lock().unwrap().get(tenant).cloned())
    }

    async fn write(&self, tenant: &TenantId, bytes: &[u8]) -> std::io::Result<()> {
        self.kept
            .lock()
            .unwrap()
            .insert(tenant.clone(), bytes.to_vec());
        Ok(())
    }
}

impl Rig {
    /// A server on a broker, sink and clock that others may have used, which
    /// keeps its snapshots in `store` if it is given one.
    fn over_with(
        broker: Arc<FlakyBroker>,
        sink: Arc<LogSink>,
        clock: Arc<ManualClock>,
        store: Option<Arc<MemorySnapshots>>,
    ) -> Self {
        let mut engine = Engine::new(broker.clone(), sink.clone(), clock.clone(), settings(), 3);
        if let Some(store) = store {
            engine = engine.with_snapshots(store);
        }
        Self {
            broker,
            sink,
            clock,
            engine: Arc::new(engine),
            pulls: AtomicU64::new(0),
        }
    }

    /// Everything the engine holds for the tenant, without what only the
    /// process remembers and without the numbers that stand for a position in
    /// a queue: the queues in order, the alerts and the ids of the finished.
    async fn picture(&self) -> Value {
        let held = self.engine.lock(&tenant()).await.unwrap();
        let mut queues = serde_json::Map::new();
        for (name, queue) in &held.queues {
            let mut record = serde_json::to_value(queue.to_record()).unwrap();
            record.as_object_mut().unwrap().remove("next_order");
            for pair in record["items"].as_array_mut().unwrap() {
                pair[1].as_object_mut().unwrap().remove("order");
            }
            queues.insert(name.to_string(), record);
        }
        let mut alerts: Vec<_> = held.alerts.iter().collect();
        alerts.sort();
        let mut acknowledged: Vec<_> = held.acknowledged_alerts.iter().collect();
        acknowledged.sort();
        json!({ "queues": queues, "alerts": alerts, "acknowledged": acknowledged })
    }

    /// How many messages of the topics the engine replays the broker holds.
    async fn replayable(&self) -> u64 {
        let mut all = 0;
        for topic in [
            Topic::Work(queue()),
            Topic::facts_work(),
            Topic::Dead,
            Topic::Alerts,
            Topic::facts_alert(),
        ] {
            all += self.read(&topic).await.len() as u64;
        }
        all
    }
}

/// A restart: the same broker, sink and clock under a new engine, which
/// rebuilds the tenant's state with the snapshots in `store` if it is given
/// them. The picture of the state and the number of messages it read.
async fn restarted(rig: &Rig, store: Option<&Arc<MemorySnapshots>>) -> (Value, u64) {
    let before = rig.broker.read.load(Ordering::SeqCst);
    let engine = Rig::over_with(
        rig.broker.clone(),
        rig.sink.clone(),
        rig.clock.clone(),
        store.cloned(),
    );
    engine.engine.recover().await.unwrap();
    let read = rig.broker.read.load(Ordering::SeqCst) - before;
    (engine.picture().await, read)
}

/// A rig with a lot in it: an item acknowledged, one failed and waiting out
/// its backoff, one under a long lease, one claimable, one dead, an alert
/// acknowledged and one still open. Then a snapshot is taken.
async fn rig_with_a_snapshot(store: &Arc<MemorySnapshots>) -> (Rig, ItemId, ItemId, ItemId) {
    let rig = Rig::over_with(
        Arc::default(),
        Arc::default(),
        Arc::new(ManualClock::new(START)),
        Some(store.clone()),
    );
    // An item that fails past the cap and goes to `dead`.
    let dead = rig.publish_item("dead").await;
    for backoff in [2, 4, 0] {
        let claim = rig.pull("two").await.unwrap();
        rig.fail("two", &dead, &claim.lease_id).await.unwrap();
        rig.pass(backoff).await;
    }
    rig.pass(1).await;
    assert_eq!(rig.read(&Topic::Dead).await.len(), 1);

    let acked = rig.publish_item("acked").await;
    let claim = rig.pull("one").await.unwrap();
    rig.ack("one", &acked, &claim.lease_id).await.unwrap();

    let failing = rig.publish_item("failing").await;
    let claim = rig.pull("one").await.unwrap();
    rig.fail("one", &failing, &claim.lease_id).await.unwrap();

    let held = rig.publish_item("held").await;
    let claim = rig.pull("one").await.unwrap();
    rig.extend("one", &held, &claim.lease_id, Some(100))
        .await
        .unwrap();

    rig.publish_item("waiting").await;

    publish_alert(&rig).await;
    let answered = publish_alert(&rig).await;
    rig.engine
        .alerts_ack(&tenant(), answered, &agent("pat"))
        .await
        .unwrap();

    // No snapshot until an interval has passed.
    assert!(store.get().is_none());
    rig.pass(61).await;
    assert!(store.get().is_some(), "a snapshot after an interval");
    (rig, held, failing, dead)
}

/// What happens after the snapshot, which a restart must read: an item the
/// snapshot holds as claimable is claimed and acknowledged, one that was
/// waiting out a backoff comes back and fails again, the lease the snapshot
/// holds is extended, new items arrive and one is claimed, an alert comes and
/// one goes, and time passes enough for a retry and an expiry.
async fn more_happens(rig: &Rig, held: &ItemId, failing: &ItemId) {
    let claim = rig.pull("three").await.unwrap();
    let id = claim.item.item_id.clone().unwrap();
    assert!(&id != held && &id != failing);
    rig.ack("three", &id, &claim.lease_id).await.unwrap();

    let claim = rig.pull("three").await.unwrap();
    assert_eq!(claim.item.item_id.as_ref(), Some(failing));
    assert_eq!(claim.item.attempt, Some(2));
    rig.fail("three", failing, &claim.lease_id).await.unwrap();

    rig.publish_item("new one").await;
    rig.publish_item("new two").await;
    let _taken = rig.pull("four").await.unwrap();
    let alert = publish_alert(rig).await;
    rig.pass(5).await;
    rig.pass(12).await;
    rig.engine
        .alerts_ack(&tenant(), alert, &agent("pat"))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_restart_from_a_snapshot_reads_fewer_messages_and_reaches_the_same_state() {
    let store = Arc::new(MemorySnapshots::default());
    let (rig, held, failing, _dead) = rig_with_a_snapshot(&store).await;
    more_happens(&rig, &held, &failing).await;
    let total = rig.replayable().await;
    assert!(total > 20, "{total} messages is not much of a history");

    let (from_start, read_all) = restarted(&rig, None).await;
    let (from_snapshot, read_some) = restarted(&rig, Some(&store)).await;

    assert!(
        read_all >= total,
        "a rebuild from the start reads all {total}, read {read_all}"
    );
    assert!(
        read_some < total,
        "a rebuild from a snapshot reads fewer than {total}, read {read_some}"
    );
    assert_eq!(from_snapshot, from_start);
    // And the engine that is still running agrees with both.
    assert_eq!(rig.picture().await, from_start);
}

/// A restart from a snapshot finds the leases and backoffs the snapshot holds
/// through the due-time index: the lease expires and the retry comes back
/// once the clock passes them, with no message replayed to put them there.
#[tokio::test]
async fn a_restart_from_a_snapshot_still_expires_its_leases_and_retries_its_backoffs() {
    let store = Arc::new(MemorySnapshots::default());
    let rig = Rig::over_with(
        Arc::default(),
        Arc::default(),
        Arc::new(ManualClock::new(START)),
        Some(store.clone()),
    );
    let failing = rig.publish_item("failing").await;
    let held = rig.publish_item("held").await;
    rig.pass(59).await;
    let claim = rig.pull("one").await.unwrap();
    assert_eq!(claim.item.item_id.as_ref(), Some(&failing));
    rig.fail("one", &failing, &claim.lease_id).await.unwrap();
    let claim = rig.pull("one").await.unwrap();
    assert_eq!(claim.item.item_id.as_ref(), Some(&held));
    // The snapshot is taken at the interval, with one item backing off and
    // the other leased.
    rig.pass(1).await;
    assert!(store.get().is_some(), "a snapshot at the interval");
    let restored = Rig::over_with(
        rig.broker.clone(),
        rig.sink.clone(),
        rig.clock.clone(),
        Some(store.clone()),
    );
    restored.engine.recover().await.unwrap();
    {
        let state = restored.engine.lock(&tenant()).await.unwrap();
        let item_queue = &state.queues[&queue()];
        assert!(matches!(
            item_queue.get(&held).unwrap().phase,
            Phase::Leased(_)
        ));
        assert!(matches!(
            item_queue.get(&failing).unwrap().phase,
            Phase::Backoff { .. }
        ));
    }
    // Past the long lease and every backoff.
    restored.pass(400).await;
    let state = restored.engine.lock(&tenant()).await.unwrap();
    let item_queue = &state.queues[&queue()];
    assert!(
        !matches!(item_queue.get(&held).unwrap().phase, Phase::Leased(_)),
        "the restored lease expired"
    );
    assert!(
        !matches!(
            item_queue.get(&failing).unwrap().phase,
            Phase::Backoff { .. }
        ),
        "the restored retry was republished"
    );
}

#[tokio::test]
async fn a_snapshot_keeps_when_a_lease_began_across_an_extension_after_it() {
    let store = Arc::new(MemorySnapshots::default());
    let (rig, held, failing, _) = rig_with_a_snapshot(&store).await;
    more_happens(&rig, &held, &failing).await;
    // The lease is extended after the snapshot, and the item is rebuilt.
    let lease = {
        let state = rig.engine.lock(&tenant()).await.unwrap();
        match &state.queues[&queue()].get(&held).unwrap().phase {
            Phase::Leased(lease) => lease.clone(),
            other => panic!("{other:?}"),
        }
    };
    let (picture, _) = restarted(&rig, Some(&store)).await;
    let (full, _) = restarted(&rig, None).await;
    assert_eq!(picture, full);
    let text = picture.to_string();
    assert!(text.contains(&format!("\"claimed_at\":{}", lease.claimed_at)));
}

#[tokio::test]
async fn a_snapshot_that_cannot_be_read_is_replaced_by_a_full_rebuild() {
    let store = Arc::new(MemorySnapshots::default());
    let (rig, held, failing, _) = rig_with_a_snapshot(&store).await;
    more_happens(&rig, &held, &failing).await;
    let total = rig.replayable().await;
    let (full, _) = restarted(&rig, None).await;

    for broken in [
        b"not a snapshot".to_vec(),
        Vec::new(),
        br#"{"format":99,"tenant":"acme","taken_at":0,"offsets":[],"queues":[],"alerts":[],"acknowledged_alerts":[]}"#.to_vec(),
        br#"{"format":1,"tenant":"other","taken_at":0,"offsets":[],"queues":[],"alerts":[],"acknowledged_alerts":[]}"#.to_vec(),
        br#"{"format":1,"tenant":"acme","taken_at":0,"offsets":[["not a topic",1]],"queues":[],"alerts":[],"acknowledged_alerts":[]}"#.to_vec(),
    ] {
        store.put(&broken);
        let (picture, read) = restarted(&rig, Some(&store)).await;
        assert_eq!(picture, full, "{}", String::from_utf8_lossy(&broken));
        assert!(read >= total, "a full rebuild reads all {total}: {read}");
    }
}

#[tokio::test]
async fn a_snapshot_of_topics_that_are_not_the_ones_there_now_is_not_used() {
    let store = Arc::new(MemorySnapshots::default());
    let (rig, _, _, _) = rig_with_a_snapshot(&store).await;

    // The same snapshot over a broker that holds nothing: it covers offsets
    // that are not there.
    let empty = Rig::over_with(
        Arc::default(),
        Arc::default(),
        rig.clock.clone(),
        Some(store.clone()),
    );
    empty.engine.recover().await.unwrap();
    let held = empty.engine.lock(&tenant()).await.unwrap();
    assert!(held.queues.values().all(|queue| queue.len() == 0));
    assert!(held.alerts.is_empty());
}

#[tokio::test]
async fn nothing_is_written_when_nothing_changed_or_the_interval_has_not_passed() {
    let store = Arc::new(MemorySnapshots::default());
    let rig = Rig::over_with(
        Arc::default(),
        Arc::default(),
        Arc::new(ManualClock::new(START)),
        Some(store.clone()),
    );
    rig.publish_item("job").await;
    rig.pass(30).await;
    assert!(store.get().is_none(), "not before the interval is over");
    rig.pass(31).await;
    let first = store.get().expect("a snapshot once the interval is over");

    // Nothing changed: no new snapshot, however long it is.
    rig.pass(100).await;
    rig.pass(100).await;
    assert!(store.get().unwrap() == first);

    // A change after a long quiet is written at the next tick, as the
    // interval is long over.
    rig.publish_item("later").await;
    rig.pass(1).await;
    let second = store.get().unwrap();
    assert!(second != first);

    // A change is not written before the interval since the last is over.
    rig.publish_item("later still").await;
    rig.pass(10).await;
    assert!(store.get().unwrap() == second);
    rig.pass(60).await;
    assert!(store.get().unwrap() != second);
}

#[tokio::test]
async fn a_snapshot_taken_while_an_item_waits_to_go_dead_finds_its_copy_after_it() {
    let store = Arc::new(MemorySnapshots::default());
    let rig = Rig::over_with(
        Arc::default(),
        Arc::default(),
        Arc::new(ManualClock::new(START)),
        Some(store.clone()),
    );
    let item = rig.publish_item("doomed").await;
    for backoff in [2, 4, 0] {
        let claim = rig.pull("one").await.unwrap();
        // The last failure sends it to `dead`, which refuses the copy.
        if backoff == 0 {
            rig.broker.refuse("dead");
        }
        rig.fail("one", &item, &claim.lease_id).await.unwrap();
        rig.pass(backoff).await;
    }
    rig.pass(61).await;
    let kept = store.get().expect("a snapshot");
    assert!(
        String::from_utf8_lossy(&kept).contains(r#""next":"dead""#),
        "the snapshot holds the item waiting for its copy to be published"
    );
    assert!(rig.read(&Topic::Dead).await.is_empty());

    // The copy is published after the snapshot, and the fact for it.
    rig.broker.heal();
    rig.pass(1).await;
    assert_eq!(rig.read(&Topic::Dead).await.len(), 1);
    assert_eq!(rig.transitions().await.last().unwrap(), "dead");

    let (from_snapshot, read) = restarted(&rig, Some(&store)).await;
    let (from_start, _) = restarted(&rig, None).await;
    assert_eq!(from_snapshot, from_start);
    assert!(read < rig.replayable().await);
    let text = from_snapshot.to_string();
    assert!(text.contains(&item.to_string()), "its id is kept: {text}");
    assert!(!text.contains("Backoff"), "and it is finished: {text}");
}

#[tokio::test]
async fn an_engine_without_a_store_keeps_no_snapshot_and_still_restarts() {
    let rig = Rig::new();
    rig.publish_item("job").await;
    rig.pass(1_000).await;
    let (picture, _) = restarted(&rig, None).await;
    assert_eq!(picture, rig.picture().await);
}

#[tokio::test]
async fn a_server_that_lost_a_tenant_reports_no_gauges_for_it() {
    let (a, b) = sharing_pair();
    a.publish_item("job").await;
    run_for(&a, 1).await;
    let before = a.engine.queue_gauges().await;
    assert_eq!(before.len(), 1, "{before:?}");
    assert_eq!(before[0].open, 1);
    assert!(b.engine.queue_gauges().await.is_empty());

    // The first server stops renewing; the other takes the tenant over.
    let mut seconds = 0;
    while b.engine.queue_gauges().await.is_empty() {
        seconds += 1;
        assert!(seconds < 40, "the tenant was not taken over");
        run_for(&b, 1).await;
        let _ = b
            .engine
            .work_pull(
                &tenant(),
                &queue(),
                &agent("y"),
                &consumer(&format!("g{seconds}")),
            )
            .await;
    }
    // The old owner's state is still loaded, and it reports nothing for it.
    a.engine.keep_ownership().await;
    a.engine.tick().await;
    assert!(a.engine.queue_gauges().await.is_empty());
    assert_eq!(b.engine.queue_gauges().await.len(), 1);
}

#[tokio::test]
async fn expiries_retries_dead_letters_and_the_queue_gauges_are_counted() {
    let metrics = Arc::new(Metrics::new(true));
    let clock = Arc::new(ManualClock::new(START));
    let engine = Engine::new(
        Arc::<FlakyBroker>::default(),
        Arc::new(LogSink::default()),
        clock.clone(),
        settings(),
        3,
    )
    .with_metrics(Arc::clone(&metrics));
    let pull = async |name: &str| {
        engine
            .work_pull(&tenant(), &queue(), &agent("one"), &consumer(name))
            .await
    };

    engine
        .work_publish(item_envelope(EventId::generate(), "job"))
        .await
        .unwrap();
    // Every attempt's lease runs out: the first two are retried, the third
    // goes to dead.
    for name in ["w1", "w2", "w3"] {
        pull(name).await.unwrap().unwrap();
        clock.advance(Duration::from_secs(10));
        engine.tick().await;
        clock.advance(Duration::from_secs(5));
        engine.tick().await;
    }
    let text = metrics.render(&engine.queue_gauges().await);
    for expected in [
        "mansio_queue_lease_expiries_total{tenant=\"acme\",queue=\"builds\"} 3",
        "mansio_queue_retries_total{tenant=\"acme\",queue=\"builds\"} 2",
        "mansio_queue_dead_letters_total{tenant=\"acme\",queue=\"builds\"} 1",
        "mansio_queue_open_items{tenant=\"acme\",queue=\"builds\"} 0",
        "mansio_queue_active_leases{tenant=\"acme\",queue=\"builds\"} 0",
        "mansio_published_messages_total{tenant=\"acme\",topic=\"dead\"} 1",
    ] {
        assert!(text.contains(expected), "{expected} is not in {text}");
    }

    // A claimed item is open and leased.
    engine
        .work_publish(item_envelope(EventId::generate(), "next"))
        .await
        .unwrap();
    pull("w4").await.unwrap().unwrap();
    let text = metrics.render(&engine.queue_gauges().await);
    for expected in [
        "mansio_queue_open_items{tenant=\"acme\",queue=\"builds\"} 1",
        "mansio_queue_active_leases{tenant=\"acme\",queue=\"builds\"} 1",
    ] {
        assert!(text.contains(expected), "{expected} is not in {text}");
    }
}

// ---- servers that share a broker ---------------------------------------------

const OWNER_TTL: Duration = Duration::from_secs(8);

/// Two servers on one broker, sink and clock, each told to share tenants.
fn sharing_pair() -> (Rig, Rig) {
    let broker: Arc<FlakyBroker> = Arc::default();
    let sink: Arc<LogSink> = Arc::default();
    let clock = Arc::new(ManualClock::new(START));
    let server = || Rig {
        broker: broker.clone(),
        sink: sink.clone(),
        clock: clock.clone(),
        engine: Arc::new(Engine::new(
            broker.clone(),
            sink.clone(),
            clock.clone(),
            WorkSettings {
                owner_ttl: OWNER_TTL,
                ..settings()
            },
            3,
        )),
        pulls: AtomicU64::new(0),
    };
    (server(), server())
}

fn is_unavailable<T>(result: &Result<T, ApiError>) -> bool {
    matches!(result, Err(ApiError::Unavailable(_)))
}

/// A pull by `who` under the consumer name, with whatever the engine answers.
async fn raw_pull(rig: &Rig, who: &str, name: String) -> Result<Option<Claimed>, ApiError> {
    rig.engine
        .work_pull(&tenant(), &queue(), &agent(who), &consumer(&name))
        .await
}

/// What the timer of a server does in `seconds` of the clock's time.
async fn run_for(rig: &Rig, seconds: u64) {
    for _ in 0..seconds {
        rig.clock.advance(Duration::from_secs(1));
        rig.engine.keep_ownership().await;
        rig.engine.tick().await;
    }
}

#[tokio::test]
async fn a_tenant_is_served_by_one_server_and_no_item_is_claimed_by_two() {
    let (a, b) = sharing_pair();
    let first = a.publish_item("job").await;
    for n in 0..9 {
        a.publish_item(&format!("job-{n}")).await;
    }

    // The other server refuses anything for the tenant.
    let refused = b
        .engine
        .work_publish(item_envelope(EventId::generate(), "elsewhere"))
        .await;
    assert!(is_unavailable(&refused), "{refused:?}");

    // Pulls on both servers at once, under names of their own.
    let mut claimed = Vec::new();
    let mut refusals = 0;
    for round in 0..8 {
        let ((a1, a2, a3), (b1, b2, b3)) = tokio::join!(
            async {
                tokio::join!(
                    raw_pull(&a, "x", format!("a{round}x1")),
                    raw_pull(&a, "x", format!("a{round}x2")),
                    raw_pull(&a, "x", format!("a{round}x3")),
                )
            },
            async {
                tokio::join!(
                    raw_pull(&b, "y", format!("b{round}x1")),
                    raw_pull(&b, "y", format!("b{round}x2")),
                    raw_pull(&b, "y", format!("b{round}x3")),
                )
            }
        );
        for result in [a1, a2, a3] {
            if let Some(claim) = result.unwrap() {
                claimed.push(claim.item.item_id.unwrap());
            }
        }
        for result in [b1, b2, b3] {
            assert!(is_unavailable(&result), "{result:?}");
            refusals += 1;
        }
    }
    assert_eq!(refusals, 24);
    let unique: HashSet<_> = claimed.iter().collect();
    assert_eq!(unique.len(), claimed.len(), "an item was claimed twice");
    assert_eq!(claimed.len(), 10, "every item was claimed");
    assert!(claimed.contains(&first.to_string().parse().unwrap()));
    assert_eq!(
        a.transitions()
            .await
            .iter()
            .filter(|t| *t == "claimed")
            .count(),
        10
    );
}

#[tokio::test]
async fn the_other_server_takes_a_tenant_over_when_its_server_stops_and_no_item_is_dropped() {
    let (a, b) = sharing_pair();
    for n in 0..4 {
        a.publish_item(&format!("job-{n}")).await;
    }
    let done = a.pull("worker").await.unwrap();
    a.ack(
        "worker",
        done.item.item_id.as_ref().unwrap(),
        &done.lease_id,
    )
    .await
    .unwrap();
    // One is leased when the server stops, which is the end of it: it neither
    // ticks nor gives anything up.
    let held = a.pull("worker").await.unwrap();
    let held_id = held.item.item_id.clone().unwrap();
    drop(a);

    // Until the time is up, the tenant is not the other's.
    run_for(&b, 4).await;
    let early = b
        .engine
        .work_pull(&tenant(), &queue(), &agent("y"), &consumer("early"))
        .await;
    assert!(is_unavailable(&early), "{early:?}");

    // Within the ttl and an interval after that, it is.
    let mut seconds = 4;
    let mut again = Vec::new();
    loop {
        run_for(&b, 1).await;
        seconds += 1;
        assert!(seconds <= 8 + 2 + 10 + 2 + 5, "the items did not come back");
        let name = consumer(&format!("late{seconds}"));
        if let Ok(Some(claim)) = b
            .engine
            .work_pull(&tenant(), &queue(), &agent("y"), &name)
            .await
        {
            let id = claim.item.item_id.clone().unwrap();
            TAKEN_AS
                .lock()
                .unwrap()
                .insert(claim.lease_id, claim.consumer.clone());
            b.ack("y", &id, &claim.lease_id).await.unwrap();
            again.push(id);
        }
        if again.len() == 3 {
            break;
        }
    }
    // The leased one came back after its lease, and nothing came back twice.
    let unique: HashSet<_> = again.iter().collect();
    assert_eq!(unique.len(), 3);
    assert!(again.contains(&held_id));
    assert!(!again.contains(done.item.item_id.as_ref().unwrap()));
    let transitions = b.transitions().await;
    assert_eq!(
        transitions.iter().filter(|t| *t == "acked").count(),
        4,
        "{transitions:?}"
    );
}

#[tokio::test]
async fn a_server_that_stops_in_order_gives_its_tenants_up_at_once() {
    let (a, b) = sharing_pair();
    a.publish_item("job").await;
    let waiting = b
        .engine
        .work_pull(&tenant(), &queue(), &agent("y"), &consumer("w"))
        .await;
    assert!(is_unavailable(&waiting), "{waiting:?}");

    a.engine.release_ownership().await;
    // One round later, which is a quarter of the ttl, not the whole of it.
    run_for(&b, 2).await;
    let claim = b
        .engine
        .work_pull(&tenant(), &queue(), &agent("y"), &consumer("w"))
        .await
        .unwrap();
    assert!(claim.is_some());
    // The server that gave the tenant up no longer writes to it.
    let late = a
        .engine
        .work_publish(item_envelope(EventId::generate(), "late"))
        .await;
    assert!(is_unavailable(&late), "{late:?}");
}
