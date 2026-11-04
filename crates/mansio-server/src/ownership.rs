//! Which server owns which tenant, so that several servers can share one
//! broker.
//!
//! A tenant's work state (open items, leases, backoffs) lives in the memory of
//! one server and is a projection of the tenant's topics. Two servers that
//! both served the tenant would each hold their own copy and could lease the
//! same item to two consumers. So a tenant has one owner at a time, and only
//! its owner serves it; another server answers 503 until it takes the tenant
//! over.
//!
//! # The log
//!
//! Ownership is decided in a log the broker keeps ([`SystemLog::Owners`]),
//! which has one partition and so one order that every server reads the same.
//! The log is kept forever, like the broker's other system logs: a view is a
//! fold from a tenant's first claim, so a log that lost its head would give a
//! server that starts late a different view from the servers that read it all.
//! Each server reads the whole log and folds it into a [`View`] per tenant:
//!
//! - `claim` takes a tenant at the next `epoch`. It names the `beat` of the
//!   tenant it was made against, and counts only if that is still the beat in
//!   the log when the claim is reached: an owner's renewal that landed first
//!   moved the beat on and so refuses the claim. Of two claims against the same
//!   beat the one first in the log wins. This is a compare-and-set made out of
//!   an append and a read, which the broker does not have.
//! - `renew` moves the beat of the tenants its server owns on by one. A renewal
//!   that arrives after another server's claim was accepted names an old epoch
//!   and counts for nothing.
//! - `release` gives the tenants up at once, as a server does when it stops.
//!
//! A server takes a tenant nobody owns, or whose owner's log has not changed
//! for `MANSIO_OWNER_TTL_SECONDS` by the taker's own clock, which times how
//! long it has watched and not when the record says it was written, so the
//! servers' clocks need not agree. Every duration here is read from the
//! monotonic clock ([`Clock::monotonic_millis`]), so a step of the system
//! clock, from NTP or by hand, moves neither a fence nor a wait.
//!
//! # Fencing
//!
//! A server serves a tenant only until `ttl / 2` after it began the round that
//! last proved it the owner. Another server cannot take the tenant before
//! `ttl` has passed since it last saw the owner's log change, which is after
//! that round began, so the old owner has stopped by then even if it is slow,
//! and the margin is `ttl / 2` for a skew between clocks that run at different
//! speeds and for the time a request takes. Every durable write checks this
//! just before it goes to the broker. A request that is paused for longer
//! than that margin between the check and the broker's answer is not fenced:
//! the broker has no conditional append.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::Duration;

use mansio_core::{Broker, BrokerError, EventId, SystemLog, TenantId};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::clock::Clock;
use crate::error::ApiError;

/// One line of the owners log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Record {
    /// `server` takes `tenant` at `epoch`, against the tenant's `after_beat`.
    Claim {
        server: String,
        tenant: TenantId,
        epoch: u64,
        after_beat: u64,
    },
    /// `server` still owns these tenants.
    Renew { server: String, beats: Vec<Beat> },
    /// `server` gives these tenants up.
    Release {
        server: String,
        tenants: Vec<TenantId>,
    },
}

/// One tenant's renewal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Beat {
    tenant: TenantId,
    epoch: u64,
    beat: u64,
}

/// What the log says of one tenant.
#[derive(Debug, Clone, Default)]
struct View {
    owner: Option<String>,
    epoch: u64,
    beat: u64,
    /// When this server last saw a record change this view, by its own clock.
    seen_at: i64,
}

/// The log as far as this server has read it.
#[derive(Debug, Default)]
struct Log {
    next: u64,
    views: HashMap<TenantId, View>,
}

impl Log {
    /// Fold one record in. `now` is when it was read.
    fn apply(&mut self, record: Record, now: i64) {
        match record {
            Record::Claim {
                server,
                tenant,
                epoch,
                after_beat,
            } => {
                let view = self.views.entry(tenant).or_default();
                if epoch == view.epoch.saturating_add(1) && after_beat == view.beat {
                    *view = View {
                        owner: Some(server),
                        epoch,
                        beat: 0,
                        seen_at: now,
                    };
                }
            }
            Record::Renew { server, beats } => {
                for Beat {
                    tenant,
                    epoch,
                    beat,
                } in beats
                {
                    if let Some(view) = self.views.get_mut(&tenant)
                        && view.owner.as_deref() == Some(server.as_str())
                        && view.epoch == epoch
                        && beat == view.beat.saturating_add(1)
                    {
                        view.beat = beat;
                        view.seen_at = now;
                    }
                }
            }
            Record::Release { server, tenants } => {
                for tenant in tenants {
                    if let Some(view) = self.views.get_mut(&tenant)
                        && view.owner.as_deref() == Some(server.as_str())
                    {
                        view.owner = None;
                        view.beat = view.beat.saturating_add(1);
                        view.seen_at = now;
                    }
                }
            }
        }
    }
}

/// This server's place among the servers that share its broker.
pub(crate) struct Ownership {
    broker: Arc<dyn Broker>,
    clock: Arc<dyn Clock>,
    /// This run's name for itself in the log. A restart is a new server.
    me: String,
    /// `MANSIO_OWNER_TTL_SECONDS`, in milliseconds.
    ttl: i64,
    page: u32,
    log: Mutex<Log>,
    /// The tenants this server may serve now: until when, and at what epoch.
    valid: StdMutex<HashMap<TenantId, (i64, u64)>>,
    /// Not before this time, a round is not made again.
    next_round: AtomicI64,
    /// Whether the last round failed, so that a failure is logged when it
    /// starts and not at every round.
    failing: AtomicBool,
}

impl std::fmt::Debug for Ownership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ownership")
            .field("me", &self.me)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

impl Ownership {
    /// A server that keeps tenants for `ttl` after it last renewed them, and
    /// reads the owners log `page` records at a time.
    pub(crate) fn new(
        broker: Arc<dyn Broker>,
        clock: Arc<dyn Clock>,
        ttl: Duration,
        page: u32,
    ) -> Self {
        let me = EventId::generate().to_string();
        info!(server = %me, ttl_seconds = ttl.as_secs(), "tenants are shared between servers");
        Self {
            broker,
            clock,
            me,
            ttl: millis(ttl),
            page: page.max(1),
            log: Mutex::new(Log::default()),
            valid: StdMutex::default(),
            next_round: AtomicI64::new(0),
            failing: AtomicBool::new(false),
        }
    }

    /// How often a round is made: often enough that an owner renews well
    /// inside the time it may serve on a renewal, and a taker sees it.
    fn interval(&self) -> i64 {
        (self.ttl / 4).max(1)
    }

    /// Whether a round is due.
    pub(crate) fn due(&self) -> bool {
        self.clock.monotonic_millis() >= self.next_round.load(Ordering::Acquire)
    }

    /// The epoch this server owns `tenant` at, if it may serve it now.
    pub(crate) fn epoch_of(&self, tenant: &TenantId) -> Option<u64> {
        let now = self.clock.monotonic_millis();
        self.valid
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(tenant)
            .and_then(|(until, epoch)| (now < *until).then_some(*epoch))
    }

    /// Whether another server owns the tenant and has not gone quiet.
    fn held_elsewhere(&self, log: &Log, tenant: &TenantId, now: i64) -> bool {
        log.views.get(tenant).is_some_and(|view| {
            view.owner.as_deref().is_some_and(|owner| {
                owner != self.me && now.saturating_sub(view.seen_at) < self.ttl
            })
        })
    }

    /// Whether this server may take the tenant: nobody owns it, or its owner
    /// has gone quiet for the ttl.
    fn claimable(&self, log: &Log, tenant: &TenantId, now: i64) -> bool {
        match log.views.get(tenant) {
            None => true,
            Some(view) => match view.owner.as_deref() {
                None => true,
                Some(owner) if owner == self.me => false,
                Some(_) => now.saturating_sub(view.seen_at) >= self.ttl,
            },
        }
    }

    /// The epoch at which this server may serve `tenant`, taking the tenant
    /// first if nobody owns it.
    ///
    /// # Errors
    ///
    /// Returns 503 if another server owns the tenant, or if the owners log
    /// cannot be read or written.
    pub(crate) async fn acquire(&self, tenant: &TenantId) -> Result<u64, ApiError> {
        if let Some(epoch) = self.epoch_of(tenant) {
            return Ok(epoch);
        }
        let mut log = self.log.lock().await;
        if let Some(epoch) = self.epoch_of(tenant) {
            return Ok(epoch);
        }
        let started = self.clock.monotonic_millis();
        if !self.held_elsewhere(&log, tenant, started) {
            self.round(&mut log, started, std::slice::from_ref(tenant))
                .await
                .map_err(ApiError::from)?;
        }
        self.epoch_of(tenant).ok_or_else(|| {
            ApiError::Unavailable(format!(
                "tenant {tenant} is served by another mansio-server; try again"
            ))
        })
    }

    /// The tenants this server owns, renewed, and the ones in `tenants` that
    /// nobody owns or whose owner has gone quiet, taken. Made at most every
    /// quarter of the ttl unless `force`.
    pub(crate) async fn maintain(&self, tenants: &[TenantId], force: bool) {
        let now = self.clock.monotonic_millis();
        if !force && now < self.next_round.load(Ordering::Acquire) {
            return;
        }
        let mut log = self.log.lock().await;
        let started = self.clock.monotonic_millis();
        self.next_round
            .store(started.saturating_add(self.interval()), Ordering::Release);
        match self.round(&mut log, started, tenants).await {
            Ok(()) => {
                if self.failing.swap(false, Ordering::AcqRel) {
                    info!("the owners log is reachable again");
                }
            }
            Err(error) => {
                if !self.failing.swap(true, Ordering::AcqRel) {
                    warn!(
                        ?error,
                        "tenants could not be renewed or taken; a tenant is served only until its time runs out, and this is tried again"
                    );
                }
            }
        }
    }

    /// Give up every tenant this server owns, so that another takes them
    /// without waiting out the ttl. A server that stops calls this.
    pub(crate) async fn release_all(&self) {
        let mut log = self.log.lock().await;
        let tenants: Vec<TenantId> = self
            .valid
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
            .map(|(tenant, _)| tenant)
            .collect();
        if tenants.is_empty() {
            return;
        }
        let count = tenants.len();
        let released = Record::Release {
            server: self.me.clone(),
            tenants,
        };
        match self.append(&released).await {
            Ok(()) => {
                if let Err(error) = self.sync(&mut log).await {
                    debug!(?error, "the release could not be read back");
                }
                info!(count, "tenants given up");
            }
            Err(error) => {
                warn!(
                    ?error,
                    count,
                    "tenants could not be given up; another server takes them when the ttl has passed"
                );
            }
        }
    }

    async fn append(&self, record: &Record) -> Result<(), BrokerError> {
        let bytes = serde_json::to_vec(record)
            .map_err(|error| BrokerError::Unavailable(error.to_string()))?;
        self.broker.system_append(SystemLog::Owners, &bytes).await
    }

    /// Read the rest of the log into the views.
    async fn sync(&self, log: &mut Log) -> Result<(), BrokerError> {
        loop {
            let batch = self
                .broker
                .system_read(SystemLog::Owners, log.next, self.page)
                .await?;
            let Some(last) = batch.last() else {
                return Ok(());
            };
            let next = last.offset.saturating_add(1);
            let now = self.clock.monotonic_millis();
            for stored in &batch {
                match serde_json::from_slice::<Record>(&stored.payload) {
                    Ok(record) => {
                        log.apply(record, now);
                        log.next = stored.offset.saturating_add(1);
                    }
                    Err(error) => {
                        // Ownership is a fold over one shared order: a server
                        // that skipped a record would see a different view
                        // from one that read it. Stop here, apply nothing from
                        // it on, and fail the round, so this server takes no
                        // tenant until the record can be read.
                        // The error returned below reaches `maintain`, which
                        // logs the first failure; this line repeats every round.
                        debug!(offset = stored.offset, %error, "a record of the owners log was not read; the round stops at it");
                        return Err(BrokerError::Unavailable(format!(
                            "owners log record at offset {} cannot be read: {error}",
                            stored.offset
                        )));
                    }
                }
            }
            log.next = next;
        }
    }

    /// One round: read the log, renew what this server owns, take what it
    /// may of `candidates`, read the log again, and grant the time to serve
    /// to each tenant the log shows this server owns because of what this
    /// round wrote. `started` is when the round began.
    async fn round(
        &self,
        log: &mut Log,
        started: i64,
        candidates: &[TenantId],
    ) -> Result<(), BrokerError> {
        self.sync(log).await?;

        // What each tenant must look like once this round's records are read.
        let mut expected: HashMap<TenantId, (u64, u64)> = HashMap::new();
        let beats: Vec<Beat> = log
            .views
            .iter()
            .filter(|(_, view)| view.owner.as_deref() == Some(self.me.as_str()))
            .map(|(tenant, view)| Beat {
                tenant: tenant.clone(),
                epoch: view.epoch,
                beat: view.beat.saturating_add(1),
            })
            .collect();
        for beat in &beats {
            expected.insert(beat.tenant.clone(), (beat.epoch, beat.beat));
        }
        let now = self.clock.monotonic_millis();
        let claims: Vec<Record> = candidates
            .iter()
            .filter(|tenant| self.claimable(log, tenant, now))
            .map(|tenant| {
                let (epoch, beat) = log
                    .views
                    .get(tenant)
                    .map_or((0, 0), |view| (view.epoch, view.beat));
                expected.insert(tenant.clone(), (epoch.saturating_add(1), 0));
                Record::Claim {
                    server: self.me.clone(),
                    tenant: tenant.clone(),
                    epoch: epoch.saturating_add(1),
                    after_beat: beat,
                }
            })
            .collect();
        if beats.is_empty() && claims.is_empty() {
            self.grant(log, started, &expected);
            return Ok(());
        }
        if !beats.is_empty() {
            self.append(&Record::Renew {
                server: self.me.clone(),
                beats,
            })
            .await?;
        }
        for claim in &claims {
            self.append(claim).await?;
        }
        self.sync(log).await?;
        self.grant(log, started, &expected);
        Ok(())
    }

    /// Serve the tenants whose view now shows this server at the epoch and
    /// beat the round wrote, until half the ttl after `started`, and stop
    /// serving the rest.
    fn grant(&self, log: &Log, started: i64, expected: &HashMap<TenantId, (u64, u64)>) {
        let until = started.saturating_add(self.ttl / 2);
        let mut valid = self.valid.lock().unwrap_or_else(PoisonError::into_inner);
        valid.retain(|tenant, _| {
            log.views
                .get(tenant)
                .is_some_and(|view| view.owner.as_deref() == Some(self.me.as_str()))
        });
        for (tenant, (epoch, beat)) in expected {
            let Some(view) = log.views.get(tenant) else {
                continue;
            };
            if view.owner.as_deref() == Some(self.me.as_str())
                && view.epoch == *epoch
                && view.beat == *beat
            {
                if !valid.contains_key(tenant) {
                    info!(%tenant, epoch, "this server now owns the tenant");
                }
                valid.insert(tenant.clone(), (until, *epoch));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mansio_core::MemoryBroker;

    use super::*;
    use crate::clock::manual::ManualClock;

    const TTL: Duration = Duration::from_secs(8);

    fn tenant(name: &str) -> TenantId {
        name.parse().unwrap()
    }

    fn server(broker: &Arc<MemoryBroker>, clock: &Arc<ManualClock>) -> Ownership {
        Ownership::new(broker.clone(), clock.clone(), TTL, 100)
    }

    /// Time passing for every server at once, with a round in between so that
    /// the ones that are alive renew.
    async fn pass(clock: &ManualClock, seconds: u64, alive: &[(&Ownership, &[TenantId])]) {
        for _ in 0..seconds {
            clock.advance(Duration::from_secs(1));
            for (server, tenants) in alive {
                server.maintain(tenants, false).await;
            }
        }
    }

    #[tokio::test]
    async fn the_first_server_to_ask_owns_the_tenant_and_the_second_is_refused() {
        let broker = Arc::new(MemoryBroker::new());
        let clock = Arc::new(ManualClock::new(1_000_000));
        let (a, b) = (server(&broker, &clock), server(&broker, &clock));
        let acme = tenant("acme");

        assert_eq!(a.acquire(&acme).await.unwrap(), 1);
        assert!(matches!(
            b.acquire(&acme).await,
            Err(ApiError::Unavailable(_))
        ));
        // Asking again changes nothing for either.
        assert_eq!(a.acquire(&acme).await.unwrap(), 1);
        assert!(b.acquire(&acme).await.is_err());
    }

    #[tokio::test]
    async fn two_servers_asking_at_once_never_both_own_the_tenant() {
        for _ in 0..20 {
            let broker = Arc::new(MemoryBroker::new());
            let clock = Arc::new(ManualClock::new(1_000_000));
            let (a, b) = (server(&broker, &clock), server(&broker, &clock));
            let acme = tenant("acme");
            let (first, second) = tokio::join!(a.acquire(&acme), b.acquire(&acme));
            assert!(
                first.is_ok() != second.is_ok(),
                "exactly one owns it: {first:?} {second:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_record_that_cannot_be_read_stops_the_round_and_grants_nothing() {
        let broker = Arc::new(MemoryBroker::new());
        let clock = Arc::new(ManualClock::new(1_000_000));
        let a = server(&broker, &clock);
        let acme = tenant("acme");
        broker
            .system_append(SystemLog::Owners, b"{\"op\":\"from_a_newer_server\"}")
            .await
            .unwrap();

        assert!(a.acquire(&acme).await.is_err());
        assert!(a.epoch_of(&acme).is_none());
        // Still stopped at the record on the next round, not past it.
        a.maintain(std::slice::from_ref(&acme), true).await;
        assert!(a.epoch_of(&acme).is_none());
    }

    #[tokio::test]
    async fn an_owner_keeps_its_grant_until_it_runs_out_when_a_record_cannot_be_read() {
        let broker = Arc::new(MemoryBroker::new());
        let clock = Arc::new(ManualClock::new(1_000_000));
        let a = server(&broker, &clock);
        let acme = vec![tenant("acme")];
        a.acquire(&acme[0]).await.unwrap();
        broker
            .system_append(SystemLog::Owners, b"{\"op\":\"from_a_newer_server\"}")
            .await
            .unwrap();

        // The round fails, so nothing renews, but the grant it holds stands.
        pass(&clock, 1, &[(&a, &acme)]).await;
        assert!(a.epoch_of(&acme[0]).is_some());

        // It runs out on its own, and the tenant is not served after that.
        pass(&clock, TTL.as_secs() + 1, &[(&a, &acme)]).await;
        assert!(a.epoch_of(&acme[0]).is_none());
    }

    #[tokio::test]
    async fn an_owner_that_keeps_renewing_keeps_the_tenant() {
        let broker = Arc::new(MemoryBroker::new());
        let clock = Arc::new(ManualClock::new(1_000_000));
        let (a, b) = (server(&broker, &clock), server(&broker, &clock));
        let acme = vec![tenant("acme")];
        a.acquire(&acme[0]).await.unwrap();

        pass(&clock, 60, &[(&a, &acme), (&b, &acme)]).await;

        assert!(a.epoch_of(&acme[0]).is_some());
        assert!(b.epoch_of(&acme[0]).is_none());
    }

    #[tokio::test]
    async fn a_server_that_stops_loses_the_tenant_to_the_other_within_the_ttl() {
        let broker = Arc::new(MemoryBroker::new());
        let clock = Arc::new(ManualClock::new(1_000_000));
        let (a, b) = (server(&broker, &clock), server(&broker, &clock));
        let acme = vec![tenant("acme")];
        a.acquire(&acme[0]).await.unwrap();
        pass(&clock, 5, &[(&a, &acme), (&b, &acme)]).await;

        // The first server stops; only the second goes on.
        let mut seconds = 0;
        while b.epoch_of(&acme[0]).is_none() {
            pass(&clock, 1, &[(&b, &acme)]).await;
            seconds += 1;
            assert!(seconds <= 12, "not taken over within 1.5 times the ttl");
        }
        assert!(seconds >= 6, "taken after {seconds} s, before the ttl");
        // The stopped server's own time to serve ran out before that.
        assert!(a.epoch_of(&acme[0]).is_none());
        assert_eq!(b.epoch_of(&acme[0]), Some(2));
    }

    #[tokio::test]
    async fn a_server_that_gives_its_tenants_up_hands_them_over_at_once() {
        let broker = Arc::new(MemoryBroker::new());
        let clock = Arc::new(ManualClock::new(1_000_000));
        let (a, b) = (server(&broker, &clock), server(&broker, &clock));
        let acme = vec![tenant("acme")];
        a.acquire(&acme[0]).await.unwrap();
        b.maintain(&acme, true).await;
        assert!(b.epoch_of(&acme[0]).is_none());

        a.release_all().await;
        assert!(a.epoch_of(&acme[0]).is_none());
        b.maintain(&acme, true).await;
        assert_eq!(b.epoch_of(&acme[0]), Some(2));
    }

    #[tokio::test]
    async fn a_claim_made_against_an_old_beat_is_refused() {
        let broker = Arc::new(MemoryBroker::new());
        let clock = Arc::new(ManualClock::new(1_000_000));
        let a = server(&broker, &clock);
        let acme = tenant("acme");
        a.acquire(&acme).await.unwrap();

        // A taker that saw the tenant go quiet writes its claim after the
        // owner's renewal, against the beat it saw.
        a.maintain(std::slice::from_ref(&acme), true).await;
        let stale = Record::Claim {
            server: "taker".to_owned(),
            tenant: acme.clone(),
            epoch: 2,
            after_beat: 0,
        };
        broker
            .system_append(SystemLog::Owners, &serde_json::to_vec(&stale).unwrap())
            .await
            .unwrap();
        clock.advance(Duration::from_secs(1));
        a.maintain(std::slice::from_ref(&acme), true).await;
        assert_eq!(a.epoch_of(&acme), Some(1));
    }

    #[test]
    fn a_renewal_after_another_servers_claim_counts_for_nothing() {
        let mut log = Log::default();
        let acme = tenant("acme");
        let claim = |server: &str, epoch, after_beat| Record::Claim {
            server: server.to_owned(),
            tenant: acme.clone(),
            epoch,
            after_beat,
        };
        log.apply(claim("a", 1, 0), 0);
        // Two claims against the same beat: the first stands.
        log.apply(claim("b", 2, 0), 1);
        log.apply(claim("c", 2, 0), 2);
        log.apply(
            Record::Renew {
                server: "a".to_owned(),
                beats: vec![Beat {
                    tenant: acme.clone(),
                    epoch: 1,
                    beat: 1,
                }],
            },
            3,
        );
        let view = &log.views[&acme];
        assert_eq!(view.owner.as_deref(), Some("b"));
        assert_eq!((view.epoch, view.beat, view.seen_at), (2, 0, 1));
    }

    #[tokio::test]
    async fn a_server_that_started_late_reads_the_log_and_waits_out_the_ttl() {
        let broker = Arc::new(MemoryBroker::new());
        let clock = Arc::new(ManualClock::new(1_000_000));
        let a = server(&broker, &clock);
        let acme = vec![tenant("acme")];
        a.acquire(&acme[0]).await.unwrap();

        // The first server dies at once. A server that starts now has not
        // watched it for the ttl.
        let late = server(&broker, &clock);
        late.maintain(&acme, true).await;
        assert!(late.epoch_of(&acme[0]).is_none());
        clock.advance(TTL);
        late.maintain(&acme, true).await;
        assert!(late.epoch_of(&acme[0]).is_some());
    }

    #[tokio::test]
    async fn an_owner_that_stops_renewing_stops_serving_before_a_taker_may_serve() {
        let broker = Arc::new(MemoryBroker::new());
        let clock = Arc::new(ManualClock::new(0));
        let (owner, taker) = (server(&broker, &clock), server(&broker, &clock));
        let acme = vec![tenant("acme")];
        owner.acquire(&acme[0]).await.unwrap();
        // The taker has watched the owner's last record from this moment.
        taker.maintain(&acme, true).await;

        let mut owner_stopped = None;
        let mut taker_started = None;
        for step in 1..=200_u64 {
            clock.advance(Duration::from_millis(100));
            taker.maintain(&acme, true).await;
            let mine = owner.epoch_of(&acme[0]).is_some();
            let theirs = taker.epoch_of(&acme[0]).is_some();
            assert!(!(mine && theirs), "both serve at step {step}");
            if !mine {
                owner_stopped.get_or_insert(step);
            }
            if theirs {
                taker_started.get_or_insert(step);
            }
        }
        let stopped = owner_stopped.unwrap();
        let started = taker_started.unwrap();
        assert!(stopped < started, "{stopped} before {started}");
    }
}
