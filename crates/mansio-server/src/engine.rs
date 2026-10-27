//! The work and alert engine.
//!
//! Iggy is offset-based: it has no per-message claim, acknowledgement,
//! redelivery or dead letters. The engine builds them on top and records every
//! transition as an envelope on `facts.work` or `facts.alert`.
//!
//! # State and order
//!
//! Each tenant has one [`TenantState`] behind an async lock. Every durable
//! publish for the tenant happens with that lock held, record in the sink and
//! publish to the broker together, so the sink and the topics see the same
//! order. A transition is written before the state changes: if the write
//! fails, nothing happened.
//!
//! The state is a projection of the tenant's topics and [`crate::recover`]
//! rebuilds it on start, so none of it needs saving.
//!
//! # Two-step transitions
//!
//! Republishing an item and sending it to `dead` each publish a message and
//! then record a fact. Both cannot be one write, so the state keeps a
//! [`Phase::Owed`] between them: the message is out, the fact is still to be
//! recorded, and the item stays unclaimable until it is. A server that stops
//! in between finds the same thing on restart.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::Duration;

use mansio_core::api::{Claimed, Published};
use mansio_core::{
    AlertEvent, AlertFact, Broker, BrokerError, Caller, ConsumerName, Envelope, EventId, ItemId,
    LeaseId, Next, Principal, Segment, Sink, TenantId, Timestamp, Topic, WorkEvent, WorkFact,
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::clock::Clock;
use crate::error::ApiError;
use crate::facts::Facts;
use crate::recover;
use crate::settings::WorkSettings;
pub use crate::state::Holder;
use crate::state::{Item, Lease, Phase, TenantState, Then, backoff, held_lease};

type Held = OwnedMutexGuard<TenantState>;

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn timestamp(millis: i64) -> Result<Timestamp, ApiError> {
    Timestamp::from_unix_millis(millis).map_err(|reason| {
        warn!(millis, %reason, "the clock is out of range");
        ApiError::Internal
    })
}

/// One thing the timer has to do. The order is the order to do them in.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Task {
    /// A lease ran out.
    Expire(Segment, ItemId),
    /// An item's backoff is over, or it owes a fact.
    Advance(Segment, ItemId),
    /// An alert nobody acknowledged is due to be surfaced again.
    Resurface(EventId),
}

/// The work and alert engine for every tenant of a server.
pub struct Engine {
    broker: Arc<dyn Broker>,
    sink: Arc<dyn Sink>,
    clock: Arc<dyn Clock>,
    settings: WorkSettings,
    /// How many messages to read at a time when rebuilding state.
    page: u32,
    facts: Facts,
    tenants: StdMutex<HashMap<TenantId, Arc<Mutex<TenantState>>>>,
    /// Whether the tenants the broker holds have all been taken up.
    discovered: AtomicBool,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl Engine {
    /// An engine over `broker` that records in `sink`, reads time from
    /// `clock` and reads `page` messages at a time when rebuilding state.
    #[must_use]
    pub fn new(
        broker: Arc<dyn Broker>,
        sink: Arc<dyn Sink>,
        clock: Arc<dyn Clock>,
        settings: WorkSettings,
        page: u32,
    ) -> Self {
        Self {
            broker,
            sink,
            clock,
            settings,
            page: page.max(1),
            facts: Facts::new(),
            tenants: StdMutex::default(),
            discovered: AtomicBool::new(false),
        }
    }

    /// The tenants the engine holds state for, in order. A tenant is held from
    /// the first request that reaches the engine, so a refused request must
    /// not reach it.
    #[cfg(test)]
    pub(crate) fn tracked_tenants(&self) -> Vec<TenantId> {
        let tenants = self.tenants.lock().unwrap_or_else(PoisonError::into_inner);
        let mut names: Vec<TenantId> = tenants.keys().cloned().collect();
        names.sort();
        names
    }

    /// The settings the engine runs with.
    #[must_use]
    pub fn settings(&self) -> &WorkSettings {
        &self.settings
    }

    /// The current time, as an RFC 3339 timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`ApiError::Internal`] if the clock is out of range.
    pub fn now(&self) -> Result<Timestamp, ApiError> {
        timestamp(self.clock.now_millis())
    }

    // ---- state ------------------------------------------------------------

    /// The tenant's state, locked and up to date. The first call for a tenant
    /// rebuilds it from the broker.
    async fn lock(&self, tenant: &TenantId) -> Result<Held, ApiError> {
        let cell = {
            let mut tenants = self.tenants.lock().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(tenants.entry(tenant.clone()).or_default())
        };
        let mut held = cell.lock_owned().await;
        if !held.loaded {
            self.load(tenant, &mut held).await?;
        }
        Ok(held)
    }

    /// Rebuild a tenant's state from its topics.
    async fn load(&self, tenant: &TenantId, state: &mut TenantState) -> Result<(), ApiError> {
        let topics = match self.broker.topics(tenant).await {
            Ok(topics) => topics,
            Err(BrokerError::NotFound(_)) => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let logs = recover::read_logs(self.broker.as_ref(), tenant, &topics, self.page).await?;
        let rebuilt = recover::replay(tenant, &logs, &self.facts, self.clock.now_millis())?;
        let items: usize = rebuilt.queues.values().map(crate::state::Queue::len).sum();
        info!(%tenant, items, alerts = rebuilt.alerts.len(), "state rebuilt");
        // An alert the state already holds keeps the time it was accepted or
        // last surfaced, unless the log says later: the log only has the
        // producer's time for one that was never surfaced.
        let mut alerts = rebuilt.alerts;
        for (alert, at) in &mut alerts {
            if let Some(known) = state.alerts.get(alert) {
                *at = (*at).max(*known);
            }
        }
        state.queues = rebuilt.queues;
        state.alerts = alerts;
        state.acknowledged_alerts = rebuilt.acknowledged_alerts;
        state.loaded = true;
        Ok(())
    }

    /// Take up every tenant the broker holds: rebuild its state, so that its
    /// leases expire and its retries are published without anyone asking.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker cannot list its tenants. A tenant that
    /// cannot be rebuilt is logged and tried again by the timer.
    pub async fn recover(&self) -> Result<(), ApiError> {
        for tenant in self.broker.tenants().await? {
            if let Err(error) = self.lock(&tenant).await {
                warn!(%tenant, ?error, "the state could not be rebuilt");
            }
        }
        self.discovered.store(true, Ordering::Release);
        Ok(())
    }

    // ---- publishing -------------------------------------------------------

    /// The one way anything durable reaches the broker: record it in the
    /// sink, set the record on it, publish it. The caller holds the tenant's
    /// lock, which is what keeps the sink and the topic in the same order.
    ///
    /// Everything is published as the admin identity: a credential never
    /// writes. The server has already checked the caller's `publish` scope,
    /// and has set the principal from the credential and whatever else a caller
    /// may not set. The broker is asked to make sure the tenant and the topic
    /// exist each time; it remembers what it has found, and recreates what was
    /// deleted.
    async fn append(
        &self,
        held: &mut TenantState,
        mut envelope: Envelope,
    ) -> Result<Published, ApiError> {
        self.broker.ensure_tenant(&envelope.tenant).await?;
        self.broker
            .ensure_topic(&envelope.tenant, &envelope.topic)
            .await?;
        if envelope.topic.is_durable()
            && let Some(record) = self.sink.record(&envelope).await?
        {
            envelope.record = Some(record);
        }
        let offset = match self.broker.publish(&Caller::Admin, &envelope).await {
            Ok(offset) => offset,
            Err(error) => {
                if outcome_unknown(&error) {
                    // The connection may have dropped after the write. The
                    // state no longer matches the logs, so the next use
                    // rebuilds it from them, which finds the message if it
                    // is there.
                    held.loaded = false;
                }
                return Err(error.into());
            }
        };
        Ok(Published { offset, envelope })
    }

    /// Record a fact the server makes outside the work and alert engine, such
    /// as the issue of a credential: in the sink, then on its topic, in order
    /// with everything else for the tenant.
    ///
    /// # Errors
    ///
    /// Returns 503 if the sink or the broker is unavailable, and 502 if the
    /// broker refuses.
    pub(crate) async fn record(&self, fact: Envelope) -> Result<Published, ApiError> {
        let mut held = self.lock(&fact.tenant).await?;
        self.append(&mut held, fact).await
    }

    /// Publish an envelope to a topic: the generic messages route.
    ///
    /// Work goes through [`Engine::work_publish`], and `facts.work`,
    /// `facts.alert`, `facts.principal` and `dead` are written by the server
    /// alone. Whether the caller may publish here is for the route to decide
    /// before it gets here.
    ///
    /// # Errors
    ///
    /// Returns 400 for those topics, 409 for an alert whose id is already
    /// taken, 503 if the sink or the broker is unavailable, and 502 if the
    /// broker refuses.
    pub async fn publish(&self, envelope: Envelope) -> Result<Published, ApiError> {
        match &envelope.topic {
            Topic::Work(_) => {
                return Err(ApiError::BadRequest(
                    "work is published with work_publish, not to a work topic directly".to_owned(),
                ));
            }
            topic if topic.is_written_by_mansio() => {
                return Err(ApiError::BadRequest(format!(
                    "{topic} is written by mansio and cannot be published to"
                )));
            }
            _ => {}
        }
        if !envelope.topic.is_durable() {
            // Ephemeral: no sink, and so no order to keep.
            self.broker.ensure_tenant(&envelope.tenant).await?;
            self.broker
                .ensure_topic(&envelope.tenant, &envelope.topic)
                .await?;
            let offset = self.broker.publish(&Caller::Admin, &envelope).await?;
            return Ok(Published { offset, envelope });
        }
        let mut held = self.lock(&envelope.tenant).await?;
        let is_alert = envelope.topic == Topic::Alerts;
        if is_alert
            && (held.alerts.contains_key(&envelope.id)
                || held.acknowledged_alerts.contains(&envelope.id))
        {
            return Err(ApiError::Conflict(format!(
                "an alert with id {} already exists",
                envelope.id
            )));
        }
        let stored = self.append(&mut held, envelope).await?;
        if is_alert {
            // Timed from now, not from the `time` the producer wrote.
            held.alerts
                .insert(stored.envelope.id, self.clock.now_millis());
        }
        Ok(stored)
    }

    /// Publish a work item. The envelope is for `work.<queue>`, with its
    /// `itemid` and `attempt` already set.
    ///
    /// # Errors
    ///
    /// Returns 409 if the queue has an item with the id, open or done, and
    /// the errors of [`Engine::publish`].
    pub async fn work_publish(&self, envelope: Envelope) -> Result<Published, ApiError> {
        let (Topic::Work(queue), Some(item)) = (envelope.topic.clone(), envelope.item_id.clone())
        else {
            return Err(ApiError::Internal);
        };
        let mut held = self.lock(&envelope.tenant).await?;
        if held.queues.get(&queue).is_some_and(|q| q.knows(&item)) {
            return Err(ApiError::Conflict(format!(
                "{queue} already has an item with id {item}: an id is not used again, even when the item is done"
            )));
        }
        let stored = self.append(&mut held, envelope).await?;
        held.queue_mut(&queue)
            .insert(item, stored.envelope.clone(), Phase::Ready);
        Ok(stored)
    }

    // ---- claim, ack, fail --------------------------------------------------

    /// Claim the oldest claimable item of a queue for `principal`, under the
    /// name `consumer`. `None` if nothing can be claimed.
    ///
    /// # Errors
    ///
    /// Returns an error if the claim could not be recorded. The item is then
    /// still claimable.
    pub async fn work_pull(
        &self,
        tenant: &TenantId,
        queue: &Segment,
        principal: &Principal,
        consumer: &ConsumerName,
    ) -> Result<Option<Claimed>, ApiError> {
        let mut held = self.lock(tenant).await?;
        let now = self.clock.now_millis();
        let Some((id, message, attempt)) = held
            .queues
            .get(queue)
            .and_then(|q| q.oldest_ready().and_then(|id| Some((id, q.get(id)?))))
            .map(|(id, item)| (id.clone(), item.message.clone(), item.attempt()))
        else {
            return Ok(None);
        };

        let lease_id = LeaseId::generate();
        let lease = Lease {
            fingerprint: lease_id.fingerprint(),
            holder: principal.clone(),
            consumer: consumer.clone(),
            expires_at: now.saturating_add(millis(self.settings.lease)),
        };
        let mut data = WorkFact::new(queue.clone(), message.id, attempt);
        data.lease = Some(lease.fingerprint);
        data.holder = Some(lease.holder.clone());
        data.consumer = Some(lease.consumer.clone());
        data.expires_at = Some(timestamp(lease.expires_at)?);
        let fact = self
            .facts
            .work(tenant, WorkEvent::Claimed, principal, now, &id, &data)?;
        self.append(&mut held, fact).await?;

        held.queue_mut(queue)
            .set_phase(&id, Phase::Leased(lease.clone()));
        Ok(Some(Claimed {
            lease_id,
            consumer: lease.consumer.clone(),
            expires_at: timestamp(lease.expires_at)?,
            item: message,
        }))
    }

    /// Complete an item. Only the holder, naming the consumer and presenting
    /// the lease id, may.
    ///
    /// # Errors
    ///
    /// Returns 404 if the queue has no such open item, 403 if the lease is
    /// not the caller's, and 409 if there is no live lease. Nothing changes
    /// then.
    pub async fn work_ack(
        &self,
        tenant: &TenantId,
        queue: &Segment,
        item: &ItemId,
        holder: Holder<'_>,
    ) -> Result<Published, ApiError> {
        let mut held = self.lock(tenant).await?;
        let now = self.clock.now_millis();
        let principal = holder.principal;
        let fact = {
            let open = open_item(&held, queue, item)?;
            let lease = held_lease(open, holder, now)?;
            let data = lease_fact(queue, open, lease);
            self.facts
                .work(tenant, WorkEvent::Acked, principal, now, item, &data)?
        };
        let stored = self.append(&mut held, fact).await?;
        held.queue_mut(queue).finish(item);
        Ok(stored)
    }

    /// Give an item back as failed. Only the holder, naming the consumer and
    /// presenting the lease id, may. The item is republished after a backoff, or goes to `dead` if it
    /// has used its attempts.
    ///
    /// # Errors
    ///
    /// As [`Engine::work_ack`].
    pub async fn work_fail(
        &self,
        tenant: &TenantId,
        queue: &Segment,
        item: &ItemId,
        holder: Holder<'_>,
        reason: &str,
    ) -> Result<Published, ApiError> {
        let mut held = self.lock(tenant).await?;
        let now = self.clock.now_millis();
        let principal = holder.principal;
        let (fact, next, due) = {
            let open = open_item(&held, queue, item)?;
            let lease = held_lease(open, holder, now)?;
            let (next, retry_at) = self.decide(open.attempt(), now);
            let mut data = lease_fact(queue, open, lease);
            data.reason = Some(reason.to_owned());
            data.next = Some(next);
            data.retry_at = retry_at.map(timestamp).transpose()?;
            let fact = self
                .facts
                .work(tenant, WorkEvent::Failed, principal, now, item, &data)?;
            (fact, next, retry_at.unwrap_or(now))
        };
        let stored = self.append(&mut held, fact).await?;
        held.queue_mut(queue)
            .set_phase(item, Phase::Backoff { due, next });
        if next == Next::Dead {
            // The item is already out of the caller's hands; if the dead
            // letter cannot be written now, the timer writes it.
            if let Err(error) = self.advance(&mut held, tenant, queue, item, now).await {
                warn!(%tenant, %queue, %item, ?error, "dead lettering will be retried");
            }
        }
        Ok(stored)
    }

    /// What follows a failed attempt: a retry at some time, or the dead
    /// topic.
    fn decide(&self, attempt: u32, now: i64) -> (Next, Option<i64>) {
        if attempt >= self.settings.max_attempts {
            (Next::Dead, None)
        } else {
            let wait = backoff(
                attempt,
                self.settings.backoff_base,
                self.settings.backoff_cap,
            );
            (Next::Retry, Some(now.saturating_add(millis(wait))))
        }
    }

    // ---- alerts ------------------------------------------------------------

    /// Acknowledge an alert, once.
    ///
    /// # Errors
    ///
    /// Returns 404 if there is no such alert and 409 if it was already
    /// acknowledged. Whoever did, only the first acknowledgement counts.
    pub async fn alerts_ack(
        &self,
        tenant: &TenantId,
        alert: EventId,
        principal: &Principal,
    ) -> Result<Published, ApiError> {
        let mut held = self.lock(tenant).await?;
        let now = self.clock.now_millis();
        if held.acknowledged_alerts.contains(&alert) {
            return Err(ApiError::Conflict(format!(
                "alert {alert} was already acknowledged"
            )));
        }
        if !held.alerts.contains_key(&alert) {
            return Err(ApiError::NotFound(format!("no alert {alert}")));
        }
        let data = AlertFact {
            alert_id: alert,
            acknowledged_by: Some(principal.clone()),
            acknowledged_at: Some(timestamp(now)?),
            surfaced_at: None,
        };
        let fact = self
            .facts
            .alert(tenant, AlertEvent::Acknowledged, principal, now, &data)?;
        let stored = self.append(&mut held, fact).await?;
        held.alerts.remove(&alert);
        held.acknowledged_alerts.insert(alert);
        Ok(stored)
    }

    // ---- the timer ---------------------------------------------------------

    /// Do what the passing of time calls for in every tenant: expire leases,
    /// republish items whose backoff is over, finish transitions that were
    /// left half written, and surface alerts again.
    pub async fn tick(&self) {
        let cells: Vec<(TenantId, Arc<Mutex<TenantState>>)> = {
            let tenants = self.tenants.lock().unwrap_or_else(PoisonError::into_inner);
            tenants
                .iter()
                .map(|(tenant, cell)| (tenant.clone(), Arc::clone(cell)))
                .collect()
        };
        for (tenant, cell) in cells {
            let mut held = cell.lock_owned().await;
            if !held.loaded
                && let Err(error) = self.load(&tenant, &mut held).await
            {
                warn!(%tenant, ?error, "the state could not be rebuilt");
                continue;
            }
            let now = self.clock.now_millis();
            self.process_due(&mut held, &tenant, now).await;
        }
    }

    /// Run the timer until the task is dropped or aborted: take up the
    /// tenants the broker holds, then [`Engine::tick`] every
    /// `MANSIO_TICK_MILLIS`.
    pub async fn run(self: Arc<Self>) {
        // `interval` panics on a zero period; a tick is at least a millisecond.
        let tick = self.settings.tick.max(Duration::from_millis(1));
        let mut interval = tokio::time::interval(tick);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if !self.discovered.load(Ordering::Acquire)
                && let Err(error) = self.recover().await
            {
                warn!(?error, "the tenants could not be listed");
            }
            self.tick().await;
        }
    }

    /// Everything that is due in a tenant at `now`, in the order to do it:
    /// leases that ran out, oldest first, then items whose backoff is over or
    /// that owe a fact, then alerts to surface again.
    fn due(&self, held: &TenantState, now: i64) -> Vec<Task> {
        let mut due: Vec<(u8, i64, Task)> = Vec::new();
        for (queue, items) in &held.queues {
            for (id, item) in items.iter() {
                match &item.phase {
                    Phase::Leased(lease) if lease.expires_at <= now => {
                        due.push((0, lease.expires_at, Task::Expire(queue.clone(), id.clone())))
                    }
                    Phase::Backoff { due: at, .. } if *at <= now => {
                        due.push((1, *at, Task::Advance(queue.clone(), id.clone())));
                    }
                    Phase::Owed { .. } => {
                        due.push((1, now, Task::Advance(queue.clone(), id.clone())));
                    }
                    _ => {}
                }
            }
        }
        let interval = millis(self.settings.alert_resurface);
        for (alert, surfaced_at) in &held.alerts {
            if surfaced_at.saturating_add(interval) <= now {
                due.push((2, *surfaced_at, Task::Resurface(*alert)));
            }
        }
        due.sort();
        due.into_iter().map(|(_, _, task)| task).collect()
    }

    /// Do what is due in a tenant.
    ///
    /// A task that fails is logged and left for the next tick; it does not
    /// hold up the others. If the failure leaves the outcome of a publish
    /// unknown, the state no longer matches the logs, and the pass ends: the
    /// next tick rebuilds the state before it does anything. So a broker that
    /// cannot be reached costs one rebuild per tick, however many tasks are
    /// due.
    async fn process_due(&self, held: &mut TenantState, tenant: &TenantId, now: i64) {
        for task in self.due(held, now) {
            if let Err(error) = self.do_task(held, tenant, &task, now).await {
                warn!(%tenant, ?task, ?error, "the timer could not finish a task");
                if !held.loaded {
                    return;
                }
            }
        }
    }

    async fn do_task(
        &self,
        held: &mut TenantState,
        tenant: &TenantId,
        task: &Task,
        now: i64,
    ) -> Result<(), ApiError> {
        match task {
            Task::Expire(queue, id) => self.expire(held, tenant, queue, id, now).await,
            Task::Advance(queue, id) => self.advance(held, tenant, queue, id, now).await,
            Task::Resurface(alert) => self.resurface(held, tenant, *alert, now).await,
        }
    }

    /// An alert nobody acknowledged is surfaced again.
    async fn resurface(
        &self,
        held: &mut TenantState,
        tenant: &TenantId,
        alert: EventId,
        now: i64,
    ) -> Result<(), ApiError> {
        let data = AlertFact {
            alert_id: alert,
            acknowledged_by: None,
            acknowledged_at: None,
            surfaced_at: Some(timestamp(now)?),
        };
        let fact = self.facts.alert(
            tenant,
            AlertEvent::Resurfaced,
            self.facts.engine(),
            now,
            &data,
        )?;
        self.append(held, fact).await?;
        if let Some(surfaced_at) = held.alerts.get_mut(&alert) {
            *surfaced_at = now;
        }
        debug!(%tenant, %alert, "alert surfaced again");
        Ok(())
    }

    /// A lease ran out: record it, and start the backoff.
    async fn expire(
        &self,
        held: &mut TenantState,
        tenant: &TenantId,
        queue: &Segment,
        id: &ItemId,
        now: i64,
    ) -> Result<(), ApiError> {
        let (fact, next, due) = {
            let Some(item) = held.queues.get(queue).and_then(|q| q.get(id)) else {
                return Ok(());
            };
            let Phase::Leased(lease) = &item.phase else {
                return Ok(());
            };
            let (next, retry_at) = self.decide(item.attempt(), now);
            let mut data = lease_fact(queue, item, lease);
            data.next = Some(next);
            data.retry_at = retry_at.map(timestamp).transpose()?;
            let fact = self.facts.work(
                tenant,
                WorkEvent::Expired,
                self.facts.engine(),
                now,
                id,
                &data,
            )?;
            (fact, next, retry_at.unwrap_or(now))
        };
        self.append(held, fact).await?;
        held.queue_mut(queue)
            .set_phase(id, Phase::Backoff { due, next });
        debug!(%tenant, %queue, %id, "lease expired");
        self.advance(held, tenant, queue, id, now).await
    }

    /// Take an item one step further along a transition that is not
    /// finished: republish it or send it to `dead` once its backoff is over,
    /// and record the fact that says so.
    async fn advance(
        &self,
        held: &mut TenantState,
        tenant: &TenantId,
        queue: &Segment,
        id: &ItemId,
        now: i64,
    ) -> Result<(), ApiError> {
        let Some(item) = held.queues.get(queue).and_then(|q| q.get(id)) else {
            return Ok(());
        };
        if let Phase::Backoff { due, next } = item.phase {
            if due > now {
                return Ok(());
            }
            let current = item.message.clone();
            let (event, message, then) = match next {
                Next::Retry => (
                    WorkEvent::Retried,
                    self.facts.retry_message(&current, now)?,
                    Then::Ready,
                ),
                Next::Dead => (
                    WorkEvent::Dead,
                    self.facts.dead_message(&current, now)?,
                    Then::Gone,
                ),
            };
            // Step one: the message. Whatever happens next, it is out.
            let stored = self.append(held, message).await?;
            let fact = self
                .facts
                .owed(tenant, event, queue, &stored.envelope, Some(current.id))?;
            let q = held.queue_mut(queue);
            if then == Then::Ready {
                q.set_message(id, stored.envelope);
            }
            q.set_phase(
                id,
                Phase::Owed {
                    fact: Box::new(fact),
                    then,
                },
            );
        }

        // Step two: the fact.
        let Some(Phase::Owed { fact, then }) = held
            .queues
            .get(queue)
            .and_then(|q| q.get(id))
            .map(|item| item.phase.clone())
        else {
            return Ok(());
        };
        self.append(held, *fact).await?;
        let q = held.queue_mut(queue);
        match then {
            Then::Ready => q.set_phase(id, Phase::Ready),
            Then::Gone => q.finish(id),
        }
        Ok(())
    }
}

/// Whether a failed publish may still have stored the message. Only an error
/// about reaching the broker may: a refusal, a missing topic or an invalid
/// envelope all come before anything is stored.
fn outcome_unknown(error: &BrokerError) -> bool {
    matches!(error, BrokerError::Unavailable(_))
}

/// The open item, or 404.
fn open_item<'a>(
    state: &'a TenantState,
    queue: &Segment,
    item: &ItemId,
) -> Result<&'a Item, ApiError> {
    state
        .queues
        .get(queue)
        .and_then(|q| q.get(item))
        .ok_or_else(|| ApiError::NotFound(format!("no open item {item} in {queue}")))
}

/// The `data` of a fact about an item under a lease.
fn lease_fact(queue: &Segment, item: &Item, lease: &Lease) -> WorkFact {
    let mut data = WorkFact::new(queue.clone(), item.message.id, item.attempt());
    data.lease = Some(lease.fingerprint);
    data.holder = Some(lease.holder.clone());
    data.consumer = Some(lease.consumer.clone());
    data.expires_at = Timestamp::from_unix_millis(lease.expires_at).ok();
    data
}

#[cfg(test)]
mod tests;
