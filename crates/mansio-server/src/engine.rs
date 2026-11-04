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
//! rebuilds it on start, so none of it needs saving. A start is shortened by
//! a snapshot ([`crate::snapshot`]), which the timer writes every
//! `MANSIO_SNAPSHOT_INTERVAL_SECONDS` and which is only a cache: without a
//! usable one the state is rebuilt from offset 0.
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
    AlertEvent, AlertFact, Broker, BrokerError, Caller, ConsumerName, Envelope, EventId,
    IdempotencyKey, ItemId, LeaseId, Next, Principal, Segment, Sink, SystemLog, TenantId,
    Timestamp, Topic, WorkEvent, WorkFact,
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::clock::Clock;
use crate::error::ApiError;
use crate::facts::Facts;
use crate::idempotency::{self, Keys};
use crate::metrics::{Metrics, QueueGauge};
use crate::ownership::Ownership;
use crate::records;
use crate::recover;
use crate::settings::WorkSettings;
use crate::snapshot::{FileSnapshots, Restored, Snapshot, Snapshots};
pub use crate::state::Holder;
use crate::state::{Due, Item, Lease, Phase, Remembered, TenantState, Then, backoff, held_lease};

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

/// The reason on an `expired` fact for a lease ended because its holder's
/// credential was revoked or expired.
const REVOKED: &str = "revoked";

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

/// Where the retries of a recovery that fails stand.
#[derive(Debug, Default)]
struct RecoveryRetry {
    failures: u32,
    /// Not before this time, in milliseconds since the Unix epoch.
    next_try: i64,
    /// Not logged before this time.
    next_log: i64,
}

/// What [`Engine::try_recover`] did.
#[derive(Debug, PartialEq, Eq)]
enum RecoveryAttempt {
    Waiting,
    Recovered,
    Failed { logged: bool },
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
    /// The idempotency keys the log holds, read once, until each tenant takes
    /// its own.
    seeded_keys: Mutex<Option<HashMap<TenantId, Keys>>>,
    /// With servers sharing a broker: the idempotency log as far as this
    /// server has read it, and the keys it holds, so that a tenant taken up
    /// reads only what was added since, not the whole log.
    shared_keys: Mutex<(u64, HashMap<TenantId, Keys>)>,
    /// Where snapshots of each tenant's state are kept, if they are.
    snapshots: Option<Arc<dyn Snapshots>>,
    metrics: Arc<Metrics>,
    /// Which server owns which tenant, if servers share a broker
    /// (`MANSIO_OWNER_TTL_SECONDS`). Without it this server serves every
    /// tenant.
    ownership: Option<Arc<Ownership>>,
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
        let ownership = (!settings.owner_ttl.is_zero()).then(|| {
            Arc::new(Ownership::new(
                Arc::clone(&broker),
                Arc::clone(&clock),
                settings.owner_ttl,
                page.max(1),
            ))
        });
        Self {
            broker,
            sink,
            clock,
            settings,
            ownership,
            page: page.max(1),
            facts: Facts::new(),
            tenants: StdMutex::default(),
            discovered: AtomicBool::new(false),
            seeded_keys: Mutex::new(None),
            shared_keys: Mutex::new((0, HashMap::new())),
            snapshots: None,
            metrics: Arc::new(Metrics::new(false)),
        }
    }

    /// Count what the engine does in `metrics`.
    #[must_use]
    pub(crate) fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Each queue's open items and leases, for the tenants whose state is
    /// loaded. Waits for a tenant's lock like any request does.
    pub(crate) async fn queue_gauges(&self) -> Vec<QueueGauge> {
        let cells: Vec<(TenantId, Arc<Mutex<TenantState>>)> = {
            let tenants = self.tenants.lock().unwrap_or_else(PoisonError::into_inner);
            tenants
                .iter()
                .map(|(tenant, cell)| (tenant.clone(), Arc::clone(cell)))
                .collect()
        };
        let mut gauges = Vec::new();
        for (tenant, cell) in cells {
            let held = cell.lock().await;
            if !held.loaded {
                continue;
            }
            // A server that no longer serves the tenant keeps its old state
            // until it serves it again; its numbers are the new owner's to
            // report, not a frozen copy.
            if let Some(ownership) = &self.ownership
                && ownership.epoch_of(&tenant) != Some(held.owner_epoch)
            {
                continue;
            }
            for (queue, items) in &held.queues {
                gauges.push(QueueGauge {
                    tenant: tenant.to_string(),
                    queue: queue.to_string(),
                    open: items.len(),
                    leases: items.leased(),
                });
            }
        }
        gauges.sort_by(|a, b| (&a.tenant, &a.queue).cmp(&(&b.tenant, &b.queue)));
        gauges
    }

    /// Keep snapshots of each tenant's state in files in `dir`, one per
    /// tenant, written every `MANSIO_SNAPSHOT_INTERVAL_SECONDS`, and start
    /// from them.
    #[must_use]
    pub fn with_snapshot_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.snapshots = Some(Arc::new(FileSnapshots::new(dir)));
        self
    }

    /// Keep snapshots in `store`.
    #[cfg(test)]
    pub(crate) fn with_snapshots(mut self, store: Arc<dyn Snapshots>) -> Self {
        self.snapshots = Some(store);
        self
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

    /// Whether other servers may share the broker (ownership is on).
    pub(crate) fn shared(&self) -> bool {
        self.ownership.is_some()
    }

    /// Whether this server may act for `tenant`: always without ownership,
    /// and otherwise only while it owns the tenant (taking it if nobody does).
    ///
    /// # Errors
    ///
    /// Returns 503 if another server owns the tenant, or if the owners log
    /// cannot be read or written.
    pub(crate) async fn serves(&self, tenant: &TenantId) -> Result<(), ApiError> {
        match &self.ownership {
            Some(ownership) => ownership.acquire(tenant).await.map(|_| ()),
            None => Ok(()),
        }
    }

    /// The tenant's state, locked and up to date. The first call for a tenant
    /// rebuilds it from the broker.
    async fn lock(&self, tenant: &TenantId) -> Result<Held, ApiError> {
        // With servers sharing a broker, only the tenant's owner serves it.
        let epoch = match &self.ownership {
            Some(ownership) => ownership.acquire(tenant).await?,
            None => 0,
        };
        let cell = {
            let mut tenants = self.tenants.lock().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(tenants.entry(tenant.clone()).or_default())
        };
        let mut held = cell.lock_owned().await;
        see_owner(&mut held, epoch);
        if !held.loaded {
            self.load(tenant, &mut held).await?;
        }
        Ok(held)
    }

    /// Read the idempotency log, once, into per-tenant keys. A page at a time
    /// goes straight into the keys, which evict by window and bound as they
    /// fill, so memory stays within the bound. The broker drops records past
    /// the window, so the read is about as long as the window's worth of
    /// keyed publishes. A broker that keeps no such log has none. Any other
    /// failure is returned and nothing is stored, so the next call tries
    /// again.
    async fn seed_keys(
        &self,
        seeded: &mut Option<HashMap<TenantId, Keys>>,
    ) -> Result<(), ApiError> {
        if seeded.is_some() {
            return Ok(());
        }
        let mut tenants = HashMap::new();
        self.read_keys(&mut tenants, &mut 0).await?;
        *seeded = Some(tenants);
        Ok(())
    }

    /// Read the idempotency log from `offset` to its end into `tenants`, and
    /// move `offset` past what was read.
    async fn read_keys(
        &self,
        tenants: &mut HashMap<TenantId, Keys>,
        offset: &mut u64,
    ) -> Result<(), ApiError> {
        let now = self.clock.now_millis();
        let window = millis(self.settings.idempotency_window);
        loop {
            let batch = match self
                .broker
                .system_read(SystemLog::Idempotency, *offset, self.page)
                .await
            {
                Ok(batch) => batch,
                Err(BrokerError::Unsupported(reason)) => {
                    debug!(%reason, "the broker keeps no idempotency keys across a restart");
                    break;
                }
                Err(error) => return Err(error.into()),
            };
            let Some(last) = batch.last() else { break };
            *offset = last.offset.saturating_add(1);
            idempotency::replay_into(
                tenants,
                &batch,
                now,
                window,
                self.settings.idempotency_max_keys,
            );
        }
        Ok(())
    }

    /// Give the tenant its idempotency keys from the log, if it has not got
    /// them. The log is normally read when the server starts
    /// ([`Engine::recover`]); this reads it if that could not, and a log that
    /// cannot be read fails keyed publishes and nothing else.
    async fn ensure_keys(&self, tenant: &TenantId, held: &mut TenantState) -> Result<(), ApiError> {
        if held.keys_loaded {
            return Ok(());
        }
        if self.ownership.is_some() {
            // Another server may have published with keys since this one
            // last held the tenant, so they are read again from the log.
            let mut shared = self.shared_keys.lock().await;
            let (next, tenants) = &mut *shared;
            self.read_keys(tenants, next).await?;
            held.keys = tenants.get(tenant).cloned().unwrap_or_default();
            held.keys_loaded = true;
            return Ok(());
        }
        let mut seeded = self.seeded_keys.lock().await;
        self.seed_keys(&mut seeded).await?;
        held.keys = seeded
            .as_mut()
            .and_then(|tenants| tenants.remove(tenant))
            .unwrap_or_default();
        held.keys_loaded = true;
        Ok(())
    }

    /// Rebuild a tenant's state from its topics.
    async fn load(&self, tenant: &TenantId, state: &mut TenantState) -> Result<(), ApiError> {
        let topics = match self.broker.topics(tenant).await {
            Ok(topics) => topics,
            Err(BrokerError::NotFound(_)) => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let restored = self.restore(tenant).await;
        let (base, from, taken_at) = match restored {
            Some(restored) => (
                Some(recover::Rebuilt {
                    queues: restored.queues,
                    alerts: restored.alerts,
                    acknowledged_alerts: restored.acknowledged_alerts,
                }),
                restored.offsets,
                Some(restored.taken_at),
            ),
            None => (None, HashMap::new(), None),
        };
        let logs = recover::read_logs(&self.broker, tenant, &topics, self.page, &from).await?;
        let replayed = logs.len();
        let rebuilt = recover::replay(tenant, &logs, &self.facts, self.clock.now_millis(), base)?;
        let items: usize = rebuilt.queues.values().map(crate::state::Queue::len).sum();
        info!(
            %tenant,
            items,
            alerts = rebuilt.alerts.len(),
            replayed,
            from_snapshot = taken_at.is_some(),
            "state rebuilt"
        );
        let mut covered = from;
        covered.extend(logs.ends);
        state.covered = Some(covered);
        // A state rebuilt from the start waits one interval for its first
        // snapshot, as one rebuilt from a snapshot does for its next.
        state.snapshot_at = Some(taken_at.unwrap_or_else(|| self.clock.now_millis()));
        state.changed = taken_at.is_none() || replayed > 0;
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
        state.alerts = alerts.into();
        state.acknowledged_alerts = rebuilt.acknowledged_alerts;
        state.loaded = true;
        Ok(())
    }

    /// The snapshot the tenant's state is to be rebuilt from, if there is one
    /// that can be used. One that cannot be read, is not of this format or
    /// does not fit the topics as they are is dropped with a warning, and the
    /// state is rebuilt from offset 0.
    async fn restore(&self, tenant: &TenantId) -> Option<Restored> {
        let store = self.snapshots.as_ref()?;
        let bytes = match store.read(tenant).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return None,
            Err(error) => {
                warn!(%tenant, %error, "the snapshot could not be read; the state is rebuilt from the start of the topics");
                return None;
            }
        };
        let restored = match Snapshot::decode(tenant, &bytes) {
            Ok(restored) => restored,
            Err(why) => {
                warn!(%tenant, %why, "the snapshot could not be used; the state is rebuilt from the start of the topics");
                return None;
            }
        };
        if let Err(why) = recover::check_offsets(&self.broker, tenant, &restored.offsets).await {
            warn!(%tenant, %why, "the snapshot does not fit the topics; the state is rebuilt from the start of them");
            return None;
        }
        Some(restored)
    }

    /// The bytes of a snapshot of the tenant's state, if one is due at `now`:
    /// the interval since the last has passed, the state has changed since,
    /// and it knows which messages it covers. Taking it marks the state as
    /// saved; [`Engine::tick`] marks it changed again if the write fails.
    fn snapshot_due(&self, tenant: &TenantId, held: &mut TenantState, now: i64) -> Option<Vec<u8>> {
        let interval = millis(self.settings.snapshot_interval);
        if self.snapshots.is_none() || interval == 0 || !held.loaded || !held.changed {
            return None;
        }
        if held
            .snapshot_at
            .is_some_and(|at| now.saturating_sub(at) < interval)
        {
            return None;
        }
        let snapshot = Snapshot::of(tenant, held, now)?;
        let bytes = match snapshot.encode() {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(%tenant, %error, "a snapshot could not be made");
                return None;
            }
        };
        held.snapshot_at = Some(now);
        held.changed = false;
        Some(bytes)
    }

    /// Take up every tenant the broker holds: rebuild its state, so that its
    /// leases expire and its retries are published without anyone asking.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker cannot list its tenants. A tenant that
    /// cannot be rebuilt is logged and tried again by the timer.
    pub async fn recover(&self) -> Result<(), ApiError> {
        // The idempotency log is read first, before any tenant is locked, so
        // that no request waits on it.
        let tenants = self.broker.tenants().await?;
        // With servers sharing a broker, this one takes the tenants nobody
        // owns, and leaves the rest to their owners. A tenant's keys are read
        // when it first publishes with one, because they may have changed
        // since.
        if let Some(ownership) = &self.ownership {
            ownership.maintain(&tenants, true).await;
        }
        let mut keys_ready = self.ownership.is_none() && {
            let mut seeded = self.seeded_keys.lock().await;
            match self.seed_keys(&mut seeded).await {
                Ok(()) => true,
                Err(error) => {
                    warn!(?error, "the idempotency keys could not be read");
                    false
                }
            }
        };
        for tenant in tenants {
            if self
                .ownership
                .as_ref()
                .is_some_and(|ownership| ownership.epoch_of(&tenant).is_none())
            {
                continue;
            }
            match self.lock(&tenant).await {
                Ok(mut held) => {
                    if keys_ready && self.ensure_keys(&tenant, &mut held).await.is_err() {
                        keys_ready = false;
                    }
                }
                Err(error) => {
                    keys_ready = false;
                    warn!(%tenant, ?error, "the state could not be rebuilt");
                }
            }
        }
        if keys_ready {
            // Every tenant has taken its keys; what is left belongs to
            // tenants the broker does not list, and is not kept.
            *self.seeded_keys.lock().await = Some(HashMap::new());
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
        // The last check before a write: a server whose time to serve the
        // tenant has run out, or that has it again at another epoch than the
        // state was built at, writes nothing, however it got here.
        if let Some(ownership) = &self.ownership
            && ownership.epoch_of(&envelope.tenant) != Some(held.owner_epoch)
        {
            return Err(ApiError::Unavailable(format!(
                "tenant {} is served by another mansio-server; try again",
                envelope.tenant
            )));
        }
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
        note_published(held, &envelope.topic, offset);
        self.metrics
            .published(envelope.tenant.as_str(), &envelope.topic.to_string());
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
    /// taken, 503 if the sink or the broker is unavailable or does not say
    /// whether it stored the message, and 502 if the broker names a refusal
    /// that comes before it stores anything.
    pub async fn publish(&self, envelope: Envelope) -> Result<Published, ApiError> {
        self.publish_once(envelope)
            .await
            .map(|(published, _)| published)
    }

    /// Publish an envelope as [`Engine::publish`] does, unless an earlier
    /// publish to the same tenant and topic carried the same idempotency key
    /// inside the window (`MANSIO_IDEMPOTENCY_WINDOW_SECONDS`): then nothing is
    /// published and the answer is the first one, with `true` to say it is a
    /// repeat. Only durable topics are deduplicated; `presence` is not.
    ///
    /// # Errors
    ///
    /// As [`Engine::publish`].
    pub async fn publish_once(&self, envelope: Envelope) -> Result<(Published, bool), ApiError> {
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
            self.metrics
                .published(envelope.tenant.as_str(), &envelope.topic.to_string());
            return Ok((Published { offset, envelope }, false));
        }
        let mut held = self.lock(&envelope.tenant).await?;
        let window = millis(self.settings.idempotency_window);
        let key = envelope.idempotency_key.clone();
        if let Some(key) = &key {
            self.ensure_keys(&envelope.tenant, &mut held).await?;
            if let Some(first) = held
                .keys
                .get(&envelope.topic, key, self.clock.now_millis(), window)
                .cloned()
            {
                // The first envelope is its publisher's to read back. Another
                // principal that may only publish must not read it by
                // guessing the key, so it is told the key is taken, no more.
                if first.principal != envelope.principal {
                    return Err(ApiError::Conflict(
                        "the idempotency key was used by another principal".to_owned(),
                    ));
                }
                return Ok((self.read_first(&envelope, &first).await?, true));
            }
        }
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
        if let Some(key) = key {
            self.remember(&mut held, key, &stored).await;
        }
        Ok((stored, false))
    }

    /// Read the first publish of a key back from its topic. The server keeps
    /// no message bodies for this, only where the message landed. If that
    /// cannot be found (the broker did not say the offset, or retention has
    /// since removed the message) nothing is published and the answer is 409
    /// naming the first id.
    async fn read_first(
        &self,
        envelope: &Envelope,
        first: &idempotency::First,
    ) -> Result<Published, ApiError> {
        let gone = || {
            ApiError::Conflict(format!(
                "the idempotency key was used by the publish with id {}, which can no longer \
                 be read back; nothing was published",
                first.id
            ))
        };
        let Some(offset) = first.offset else {
            return Err(gone());
        };
        let found = match self
            .broker
            .poll(&Caller::Admin, &envelope.tenant, &envelope.topic, offset, 1)
            .await
        {
            Ok(found) => found,
            Err(BrokerError::NotFound(_)) => return Err(gone()),
            Err(error) => return Err(error.into()),
        };
        found
            .into_iter()
            .find(|stored| stored.offset == offset && stored.envelope.id == first.id)
            .map(|stored| Published {
                offset: Some(stored.offset),
                envelope: stored.envelope,
            })
            .ok_or_else(gone)
    }

    /// Remember that a publish used a key, in memory and in the log. A log
    /// that cannot be written does not undo a publish that is stored: the key
    /// holds until the next restart.
    async fn remember(&self, held: &mut TenantState, key: IdempotencyKey, stored: &Published) {
        let at = self.clock.now_millis();
        let entry =
            idempotency::Entry::seen(stored.envelope.tenant.clone(), key.clone(), at, stored);
        held.keys.insert(
            (stored.envelope.topic.clone(), key),
            at,
            idempotency::First::of(stored),
            at,
            millis(self.settings.idempotency_window),
            self.settings.idempotency_max_keys,
        );
        let written = match records::encode(&entry) {
            Ok(record) => self
                .broker
                .system_append(SystemLog::Idempotency, &record)
                .await
                .map_err(ApiError::from),
            Err(error) => Err(error),
        };
        if let Err(error) = written {
            warn!(
                ?error,
                "an idempotency key could not be recorded, so a restart forgets it"
            );
        }
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
    /// A consumer (the principal and the name together) that already holds a
    /// live lease on the queue is given that lease again, not another item.
    /// So a client whose answer was lost, and that asks again, does not leave
    /// an item leased to nobody. The lease keeps its id and its expiry and
    /// nothing is recorded. The one case that cannot be answered so is a
    /// lease the server has only read back from its facts, which do not say
    /// the id: it is given a new id on the same item with the same expiry, and
    /// that is recorded as a `claimed` fact of the same attempt. To work on
    /// several items at once, pull under several names.
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
        let existing = held
            .queues
            .get(queue)
            .and_then(|q| q.held_by(principal, consumer, now))
            .map(|(id, item, lease)| (id.clone(), item.message.clone(), lease.clone()));
        if let Some((id, message, lease)) = existing {
            return self
                .give_again(&mut held, tenant, queue, id, message, lease, now)
                .await
                .map(Some);
        }
        let Some((id, message)) = held
            .queues
            .get(queue)
            .and_then(|q| q.oldest_ready().and_then(|id| Some((id, q.get(id)?))))
            .map(|(id, item)| (id.clone(), item.message.clone()))
        else {
            return Ok(None);
        };

        let lease_id = LeaseId::generate();
        let lease = Lease {
            fingerprint: lease_id.fingerprint(),
            id: Some(Remembered(lease_id)),
            holder: principal.clone(),
            consumer: consumer.clone(),
            expires_at: now.saturating_add(millis(self.settings.lease)),
            claimed_at: now,
        };
        self.record_claim(&mut held, tenant, queue, &id, &message, &lease, now)
            .await?;
        held.queue_mut(queue)
            .set_phase(&id, Phase::Leased(lease.clone()));
        Ok(Some(Claimed {
            lease_id,
            consumer: lease.consumer.clone(),
            expires_at: timestamp(lease.expires_at)?,
            item: message,
        }))
    }

    /// Record that `lease` is held on the item, as a `claimed` fact made at
    /// `now`. The caller reads the clock once and passes it here, so that the
    /// fact of a fresh claim is timed at the moment the lease says it was
    /// claimed: a replay counts the longest lease from that fact.
    #[expect(clippy::too_many_arguments, reason = "a fact needs all of them")]
    async fn record_claim(
        &self,
        held: &mut TenantState,
        tenant: &TenantId,
        queue: &Segment,
        item: &ItemId,
        message: &Envelope,
        lease: &Lease,
        now: i64,
    ) -> Result<(), ApiError> {
        let mut data = WorkFact::new(queue.clone(), message.id, message.attempt.unwrap_or(1));
        data.lease = Some(lease.fingerprint);
        data.holder = Some(lease.holder.clone());
        data.consumer = Some(lease.consumer.clone());
        data.expires_at = Some(timestamp(lease.expires_at)?);
        let fact = self
            .facts
            .work(tenant, WorkEvent::Claimed, &lease.holder, now, item, &data)?;
        self.append(held, fact).await?;
        Ok(())
    }

    /// The lease a consumer already holds, handed to it again. `now` is the
    /// time of the pull.
    #[expect(
        clippy::too_many_arguments,
        reason = "the state, the item and the lease"
    )]
    async fn give_again(
        &self,
        held: &mut TenantState,
        tenant: &TenantId,
        queue: &Segment,
        item: ItemId,
        message: Envelope,
        lease: Lease,
        now: i64,
    ) -> Result<Claimed, ApiError> {
        let expires_at = timestamp(lease.expires_at)?;
        if let Some(Remembered(lease_id)) = lease.id {
            return Ok(Claimed {
                lease_id,
                consumer: lease.consumer,
                expires_at,
                item: message,
            });
        }
        // Read back from the facts, which keep a fingerprint of the id and not
        // the id: this is a new id for the same lease.
        let lease_id = LeaseId::generate();
        let renewed = Lease {
            fingerprint: lease_id.fingerprint(),
            id: Some(Remembered(lease_id)),
            ..lease
        };
        self.record_claim(held, tenant, queue, &item, &message, &renewed, now)
            .await?;
        held.queue_mut(queue)
            .set_phase(&item, Phase::Leased(renewed.clone()));
        Ok(Claimed {
            lease_id,
            consumer: renewed.consumer,
            expires_at,
            item: message,
        })
    }

    /// Move the end of an item's lease later, so that a consumer still working
    /// on it keeps it. Only the holder, naming the consumer and presenting the
    /// lease id, may.
    ///
    /// The lease runs to `seconds` from now, or to the lease setting from now
    /// if there are none. It is never shortened, and it never runs past
    /// `MANSIO_LEASE_MAX_SECONDS` after the claim: an extension that asks for
    /// more is capped there. `not_after`, the expiry of the holder's own
    /// credential in milliseconds since the Unix epoch, caps it too: a
    /// credential cannot hold a lease past its own end. Whatever the end
    /// comes to is recorded as an
    /// `extended` fact, even when it is the end the lease already had, so a
    /// request that is sent again is answered the same way.
    ///
    /// # Errors
    ///
    /// Returns 404 if the queue has no such open item, 403 if the lease is
    /// not the caller's, and 409 if there is no live lease, which includes a
    /// lease that has ended and that the timer has not yet expired. Nothing
    /// changes then.
    pub async fn work_extend(
        &self,
        tenant: &TenantId,
        queue: &Segment,
        item: &ItemId,
        holder: Holder<'_>,
        seconds: Option<Duration>,
        not_after: Option<i64>,
    ) -> Result<Published, ApiError> {
        let mut held = self.lock(tenant).await?;
        let now = self.clock.now_millis();
        let principal = holder.principal;
        let (fact, extended) = {
            let open = open_item(&held, queue, item)?;
            let lease = held_lease(open, holder, now)?;
            let extended = Lease {
                expires_at: extended_expiry(
                    lease,
                    now,
                    seconds.unwrap_or(self.settings.lease),
                    self.settings.lease_max,
                    not_after,
                ),
                ..lease.clone()
            };
            let data = lease_fact(queue, open, &extended);
            let fact = self
                .facts
                .work(tenant, WorkEvent::Extended, principal, now, item, &data)?;
            (fact, extended)
        };
        let stored = self.append(&mut held, fact).await?;
        held.queue_mut(queue)
            .set_phase(item, Phase::Leased(extended));
        Ok(stored)
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
            let mut held = Arc::clone(&cell).lock_owned().await;
            // A tenant another server owns is not this one's to expire, to
            // retry or to snapshot.
            if let Some(ownership) = &self.ownership {
                match ownership.epoch_of(&tenant) {
                    Some(epoch) => see_owner(&mut held, epoch),
                    None => continue,
                }
            }
            if !held.loaded
                && let Err(error) = self.load(&tenant, &mut held).await
            {
                warn!(%tenant, ?error, "the state could not be rebuilt");
                continue;
            }
            self.tick_tenant(&mut held, &tenant).await;
            let due = self.snapshot_due(&tenant, &mut held, self.clock.now_millis());
            // The write is not made with the lock held.
            drop(held);
            if let (Some(bytes), Some(store)) = (due, &self.snapshots)
                && let Err(error) = store.write(&tenant, &bytes).await
            {
                warn!(%tenant, %error, "a snapshot could not be written; it is tried again after the next interval");
                cell.lock().await.changed = true;
            }
        }
    }

    /// One tenant's share of a tick, run under its lock.
    async fn tick_tenant(&self, held: &mut TenantState, tenant: &TenantId) {
        let now = self.clock.now_millis();
        held.keys
            .prune(now, millis(self.settings.idempotency_window));
        self.process_due(held, tenant, now).await;
    }

    /// Run the timer until the task is dropped or aborted: take up the
    /// tenants the broker holds, then [`Engine::tick`] every
    /// `MANSIO_TICK_MILLIS`.
    pub async fn run(self: Arc<Self>) {
        // `interval` panics on a zero period; a tick is at least a millisecond.
        let tick = self.settings.tick.max(Duration::from_millis(1));
        let mut interval = tokio::time::interval(tick);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut retry = RecoveryRetry::default();
        loop {
            interval.tick().await;
            if !self.discovered.load(Ordering::Acquire) {
                self.try_recover(&mut retry).await;
            } else {
                self.keep_ownership().await;
            }
            self.tick().await;
        }
    }

    /// With servers sharing a broker: renew the tenants this server owns, take
    /// the ones nobody does, and take up the state of any it has just got.
    /// Done every quarter of `MANSIO_OWNER_TTL_SECONDS`.
    async fn keep_ownership(&self) {
        let Some(ownership) = &self.ownership else {
            return;
        };
        if !ownership.due() {
            return;
        }
        let tenants = match self.broker.tenants().await {
            Ok(tenants) => tenants,
            Err(error) => {
                debug!(?error, "the tenants could not be listed to renew them");
                self.tracked_tenant_ids()
            }
        };
        ownership.maintain(&tenants, false).await;
        for tenant in tenants {
            if ownership.epoch_of(&tenant).is_some()
                && let Err(error) = self.lock(&tenant).await
            {
                warn!(%tenant, ?error, "the state could not be rebuilt");
            }
        }
    }

    /// The tenants the engine holds state for.
    fn tracked_tenant_ids(&self) -> Vec<TenantId> {
        let tenants = self.tenants.lock().unwrap_or_else(PoisonError::into_inner);
        tenants.keys().cloned().collect()
    }

    /// Give up every tenant this server owns, so that another server takes
    /// them at once and not when the time runs out. Called when the server
    /// stops, after it has stopped serving.
    pub async fn release_ownership(&self) {
        if let Some(ownership) = &self.ownership {
            ownership.release_all().await;
        }
    }

    /// One attempt at [`Engine::recover`] if the retry's wait is over. A
    /// failure is logged the first time and then at most once per
    /// `MANSIO_RECOVERY_LOG_SECONDS`, and the next attempt waits twice as long
    /// as the last, from one tick up to that interval, so a broker that keeps
    /// refusing is neither asked nor reported every tick.
    async fn try_recover(&self, retry: &mut RecoveryRetry) -> RecoveryAttempt {
        let now = self.clock.now_millis();
        if now < retry.next_try {
            return RecoveryAttempt::Waiting;
        }
        let Err(error) = self.recover().await else {
            *retry = RecoveryRetry::default();
            return RecoveryAttempt::Recovered;
        };
        let interval = millis(self.settings.recovery_log);
        let first = millis(self.settings.tick.max(Duration::from_millis(1)));
        retry.failures = retry.failures.saturating_add(1);
        let wait = first
            .saturating_mul(1_i64 << retry.failures.min(32))
            .min(interval);
        retry.next_try = now.saturating_add(wait);
        let logged = now >= retry.next_log;
        if logged {
            retry.next_log = now.saturating_add(interval);
            warn!(
                ?error,
                failures = retry.failures,
                next_attempt_in_millis = wait,
                "the tenants could not be listed; the broker is asked again with a longer wait, and this is logged at most once per MANSIO_RECOVERY_LOG_SECONDS"
            );
        }
        RecoveryAttempt::Failed { logged }
    }

    /// Everything that is due in a tenant at `now`, in the order to do it:
    /// leases that ran out, oldest first, then items whose backoff is over or
    /// that owe a fact, then alerts to surface again.
    fn due(&self, held: &TenantState, now: i64) -> Vec<Task> {
        self.due_counted(held, now).0
    }

    /// [`Engine::due`] and how many index entries it looked at: the due ones
    /// and at most one more per index, so a tick with nothing due does the
    /// same work at any queue depth.
    fn due_counted(&self, held: &TenantState, now: i64) -> (Vec<Task>, usize) {
        let mut due: Vec<(u8, i64, Task)> = Vec::new();
        let mut looked = 0;
        let mut items = Vec::new();
        for (queue, items_of) in &held.queues {
            items.clear();
            looked += items_of.due(now, &mut items);
            due.extend(items.drain(..).map(|(kind, at, id)| match kind {
                Due::Lease => (0, at, Task::Expire(queue.clone(), id)),
                Due::Advance => (1, at, Task::Advance(queue.clone(), id)),
            }));
        }
        let interval = millis(self.settings.alert_resurface);
        let mut alerts = Vec::new();
        looked += held
            .alerts
            .surfaced_by(now.saturating_sub(interval), &mut alerts);
        due.extend(
            alerts
                .into_iter()
                .map(|(at, alert)| (2, at, Task::Resurface(alert))),
        );
        due.sort();
        (due.into_iter().map(|(_, _, task)| task).collect(), looked)
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
            Task::Expire(queue, id) => self.expire(held, tenant, queue, id, now, None).await,
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
        if held.alerts.contains_key(&alert) {
            held.alerts.insert(alert, now);
        }
        debug!(%tenant, %alert, "alert surfaced again");
        Ok(())
    }

    /// End every live lease `holder` has in a tenant, because its credential
    /// was revoked or expired: each is recorded as `expired` with reason
    /// `revoked`, and the item is retried after its backoff as for any other
    /// expiry. A lease that has already run out is left to the timer. Returns
    /// how many were ended.
    ///
    /// # Errors
    ///
    /// Returns an error if the tenant's state cannot be loaded. A lease that
    /// cannot be ended is logged and left for the timer, which ends it when
    /// it runs out.
    pub(crate) async fn end_leases_of(
        &self,
        tenant: &TenantId,
        holder: &Principal,
    ) -> Result<usize, ApiError> {
        let mut held = self.lock(tenant).await?;
        let now = self.clock.now_millis();
        let mut leases: Vec<(Segment, ItemId, i64)> = Vec::new();
        for (queue, items) in &held.queues {
            for (id, item) in items.iter() {
                if let Phase::Leased(lease) = &item.phase
                    && lease.holder == *holder
                    && lease.expires_at > now
                {
                    leases.push((queue.clone(), id.clone(), lease.claimed_at));
                }
            }
        }
        // The oldest claim first, as the timer ends the oldest lease first.
        leases.sort_by(|a, b| (a.2, &a.0, &a.1).cmp(&(b.2, &b.0, &b.1)));
        let mut ended = 0;
        for (queue, id, _) in leases {
            match self
                .expire(&mut held, tenant, &queue, &id, now, Some(REVOKED))
                .await
            {
                Ok(()) => ended += 1,
                Err(error) => {
                    warn!(%tenant, %queue, %id, ?error, "a revoked holder's lease could not be ended");
                    if !held.loaded {
                        break;
                    }
                }
            }
        }
        Ok(ended)
    }

    /// A lease ran out, or its holder's credential ended (`reason`): record
    /// it, and start the backoff.
    async fn expire(
        &self,
        held: &mut TenantState,
        tenant: &TenantId,
        queue: &Segment,
        id: &ItemId,
        now: i64,
        reason: Option<&str>,
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
            data.reason = reason.map(str::to_owned);
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
        self.metrics.expired(tenant.as_str(), queue.as_str());
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
        match then {
            Then::Ready => self.metrics.retried(tenant.as_str(), queue.as_str()),
            Then::Gone => self.metrics.dead(tenant.as_str(), queue.as_str()),
        }
        let q = held.queue_mut(queue);
        match then {
            Then::Ready => q.set_phase(id, Phase::Ready),
            Then::Gone => q.finish(id),
        }
        Ok(())
    }
}

/// Whether a failed publish may still have stored the message. Only an
/// `Unavailable` may: a refusal the broker named, a missing topic and an
/// invalid envelope all come before anything is stored, and a broker maps
/// every error that does not say so to `Unavailable`.
/// Note that a message was published in the state: the state has changed, and
/// if the message landed where the state expected it, it covers the message.
/// A message that landed elsewhere, or where the broker did not say, leaves
/// the state not knowing what it covers, so it is not snapshotted until it is
/// rebuilt.
fn note_published(held: &mut TenantState, topic: &Topic, offset: Option<u64>) {
    held.changed = true;
    if !recover::is_replayed(topic) {
        return;
    }
    let Some(covered) = held.covered.as_mut() else {
        return;
    };
    let expected = covered.get(topic).copied().unwrap_or(0);
    match offset {
        Some(offset) if offset == expected => {
            covered.insert(topic.clone(), offset.saturating_add(1));
        }
        _ => held.covered = None,
    }
}

/// Note the epoch at which this server owns the tenant. A state built at
/// another epoch may be missing what the other server did, so it is built
/// again, and so are the idempotency keys.
fn see_owner(held: &mut TenantState, epoch: u64) {
    if held.owner_epoch != epoch {
        held.owner_epoch = epoch;
        held.loaded = false;
        held.keys_loaded = false;
    }
}

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

/// Where an extension at `now` ends `lease`: `wanted` from now, but not
/// before the lease already ends, not after `max` from the claim and not
/// after `not_after`, the end of the holder's credential.
fn extended_expiry(
    lease: &Lease,
    now: i64,
    wanted: Duration,
    max: Duration,
    not_after: Option<i64>,
) -> i64 {
    let asked = now.saturating_add(millis(wanted));
    let ceiling = lease.claimed_at.saturating_add(millis(max));
    let ceiling = not_after.map_or(ceiling, |end| ceiling.min(end));
    asked.min(ceiling).max(lease.expires_at)
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
