//! Issuing, checking and withdrawing credentials.
//!
//! A credential belongs to a principal, and the broker holds it to the
//! principal's scopes. This module makes the broker's user and the credential
//! for a request to `principals_create`, remembers a digest of each credential
//! so that it knows whose a presented token is without any traffic to the
//! broker for a token it never issued, and checks with the broker that a token
//! it knows is still accepted. It does not ask again for
//! `MANSIO_CREDENTIAL_RECHECK_SECONDS`, so a revocation or an expiry it did not
//! see itself still takes effect within that time.
//!
//! The registry of principals is rebuilt from the server's log in the broker
//! on first use.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, PoisonError};
use std::time::Duration;

use mansio_core::api::{CreatePrincipal, Issued};
use mansio_core::{
    Broker, BrokerError, BrokerUser, Credential, Principal, PrincipalEvent, PrincipalFact,
    PrincipalId, PrincipalInfo, PrincipalKind, RevokeReason, SystemLog, TenantId, Timestamp,
};
use tokio::sync::OnceCell;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::auth::Actor;
use crate::clock::Clock;
use crate::engine::Engine;
use crate::error::ApiError;
use crate::facts::{ENGINE_PRINCIPAL, Facts};
use crate::registry::{Entry, Record, Registry};
use crate::settings::CredentialSettings;

/// The kind of principal whose credential always expires.
const AGENT: &str = "agent";

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn timestamp(millis: i64) -> Result<Timestamp, ApiError> {
    Timestamp::from_unix_millis(millis)
        .map_err(|_| ApiError::BadRequest("the expiry is too far off".to_owned()))
}

/// The name of the broker user made for a principal: `mansio-` and the id
/// without its hyphens.
fn user_name(id: PrincipalId) -> String {
    format!("{USER_PREFIX}{}", id.to_string().replace('-', ""))
}

/// The start of the name of every broker user made for a principal.
const USER_PREFIX: &str = "mansio-";

/// Whether the name has the shape [`user_name`] gives, which no other user of
/// the broker, the server's own included, has.
fn is_principal_user(name: &str) -> bool {
    name.strip_prefix(USER_PREFIX).is_some_and(|rest| {
        rest.len() == 32 && rest.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// Credentials: the principals that have one, and who a bearer token is.
pub struct Credentials {
    broker: Arc<dyn Broker>,
    engine: Arc<Engine>,
    clock: Arc<dyn Clock>,
    settings: CredentialSettings,
    /// How many records to read at a time when rebuilding the registry.
    page: u32,
    facts: Facts,
    admin: Principal,
    registry: Registry,
    loaded: OnceCell<()>,
    /// When the broker last confirmed each principal's credential, in
    /// milliseconds since the Unix epoch.
    checked: StdMutex<HashMap<PrincipalId, i64>>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl Credentials {
    /// Credentials over `broker`, recorded through `engine`. `admin` is the
    /// principal the bootstrap token stands for: no credential may take its
    /// name. `page` is how many records to read at a time on start.
    #[must_use]
    pub(crate) fn new(
        broker: Arc<dyn Broker>,
        engine: Arc<Engine>,
        clock: Arc<dyn Clock>,
        settings: CredentialSettings,
        admin: Principal,
        page: u32,
    ) -> Self {
        Self {
            broker,
            engine,
            clock,
            settings,
            page: page.max(1),
            facts: Facts::new(),
            admin,
            registry: Registry::default(),
            loaded: OnceCell::new(),
            checked: StdMutex::default(),
        }
    }

    /// The settings credentials run with.
    #[must_use]
    pub fn settings(&self) -> &CredentialSettings {
        &self.settings
    }

    fn checked(&self) -> MutexGuard<'_, HashMap<PrincipalId, i64>> {
        self.checked.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // ---- start ------------------------------------------------------------

    /// Rebuild the registry from the log, once. Every principal the log says
    /// is current is told to the broker again, because the broker keeps the
    /// scopes only in memory. A principal the broker cannot find is withdrawn,
    /// and its user is deleted first, as it may be there all the same. A
    /// broker user made for a principal that the log does not know is deleted:
    /// one whose record could not be read, or that was made just before the
    /// server stopped.
    ///
    /// # Errors
    ///
    /// Returns an error if the log cannot be read or the broker cannot be
    /// reached. The next call tries again.
    pub async fn load(&self) -> Result<(), ApiError> {
        self.loaded.get_or_try_init(|| self.rebuild()).await?;
        Ok(())
    }

    async fn rebuild(&self) -> Result<(), ApiError> {
        self.registry.clear();
        let mut offset = 0;
        loop {
            let batch = self.broker.system_read(SystemLog::Principals, offset, self.page).await?;
            let Some(last) = batch.last() else { break };
            let next = last.offset.saturating_add(1);
            for record in &batch {
                match Entry::decode(&record.payload) {
                    Ok(entry) => self.registry.apply(entry),
                    Err(reason) => warn!(
                        offset = record.offset,
                        %reason,
                        "a registry record could not be read and was left out"
                    ),
                }
            }
            if next <= offset {
                break;
            }
            offset = next;
        }
        for record in self.registry.all() {
            let told = self
                .broker
                .set_grants(record.user, &record.info.tenant, &record.info.scopes)
                .await;
            match told {
                Ok(()) => {}
                Err(BrokerError::NotFound(why)) => {
                    warn!(principal = %record.info.id, %why, "the broker has lost the user of a principal");
                    self.withdraw_lost(&record).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        self.delete_orphans().await?;
        info!(principals = self.registry.all().len(), "registry rebuilt");
        Ok(())
    }

    /// Delete the broker users made for principals that no record owns.
    async fn delete_orphans(&self) -> Result<(), ApiError> {
        let owned: HashSet<BrokerUser> = self
            .registry
            .all()
            .iter()
            .map(|record| record.user)
            .collect();
        for found in self.broker.list_users().await? {
            if !is_principal_user(&found.name) || owned.contains(&found.user) {
                continue;
            }
            warn!(
                user = %found.user,
                name = %found.name,
                "a broker user that no principal owns is deleted"
            );
            if let Err(error) = self.broker.delete_user(found.user).await {
                warn!(user = %found.user, %error, "it could not be deleted; the next start tries again");
            }
        }
        Ok(())
    }

    /// A principal the broker cannot find the user or tenant of. Its user is
    /// deleted first, because the broker not finding it says nothing about its
    /// token, which may still sign in. If that cannot be done the principal is
    /// kept, and the next start tries again.
    async fn withdraw_lost(&self, record: &Record) {
        if let Err(error) = self.broker.delete_user(record.user).await {
            warn!(
                principal = %record.info.id,
                %error,
                "the user of a principal the broker cannot find could not be deleted; the principal is kept"
            );
            return;
        }
        self.registry.remove(record.info.id);
        self.save_revoked(record.info.id).await;
        self.record_revoked(&self.engine_principal(), &record.info, RevokeReason::Failed)
            .await;
    }

    // ---- who is asking ----------------------------------------------------

    /// The actor a bearer token stands for, or 401.
    ///
    /// A token that was never issued, or was revoked, is refused here, and the
    /// broker is not asked about it. For one that was issued the broker is
    /// asked, if it has not been for `MANSIO_CREDENTIAL_RECHECK_SECONDS`, and
    /// has to say that the token is still accepted and is its user's. Whatever
    /// the broker says, a principal whose expiry has passed is refused.
    ///
    /// # Errors
    ///
    /// Returns 401 for a token that is not a current principal's, and 503 if
    /// the broker cannot be asked.
    pub(crate) async fn authenticate(&self, presented: &str) -> Result<Actor, ApiError> {
        self.load().await?;
        let credential = Credential::new(presented);
        let record = self
            .registry
            .by_digest(&credential.digest())
            .ok_or(ApiError::Unauthorized)?;
        let now = self.clock.now_millis();
        if record.expired(now) {
            return Err(ApiError::Unauthorized);
        }
        let recheck = millis(self.settings.recheck);
        let recent = self
            .checked()
            .get(&record.info.id)
            .is_some_and(|at| (0..recheck).contains(&now.saturating_sub(*at)));
        if !recent {
            if self.broker.identify(&credential).await? != record.user {
                return Err(ApiError::Unauthorized);
            }
            self.checked().insert(record.info.id, now);
        }
        Ok(Actor::scoped(
            record.info.id,
            record.info.principal(),
            record.info.tenant,
            record.info.scopes,
            credential,
        ))
    }

    // ---- issue ------------------------------------------------------------

    /// How long a credential lasts: what the request asks, or for an agent the
    /// setting, or for any other kind forever.
    fn ttl(&self, kind: &PrincipalKind, asked: Option<u64>) -> Result<Option<Duration>, ApiError> {
        match asked {
            Some(0) => Err(ApiError::BadRequest(
                "ttl_seconds must be at least 1".to_owned(),
            )),
            Some(seconds) => Ok(Some(Duration::from_secs(seconds))),
            None if kind.as_str() == AGENT => Ok(Some(self.settings.agent_ttl)),
            None => Ok(None),
        }
    }

    fn engine_principal(&self) -> Principal {
        self.facts.engine().clone()
    }

    /// Issue a credential: `principals_create`.
    ///
    /// The issue is recorded in the sink and on `facts.principal` first. Then
    /// the broker makes a user for the principal, with permissions on its
    /// tenant's stream and the topics its scopes cover, and a credential for
    /// it. Then the principal is written to the registry's log. If a step
    /// fails the ones before are undone as far as they can be, and no
    /// credential is returned.
    ///
    /// # Errors
    ///
    /// Returns 400 for a request that is not allowed, 409 if the principal
    /// already has a credential, and 502 or 503 if the broker or the sink
    /// fails.
    pub(crate) async fn create(
        &self,
        by: &Principal,
        request: CreatePrincipal,
    ) -> Result<Issued, ApiError> {
        self.load().await?;
        let CreatePrincipal {
            kind,
            name,
            tenant,
            scopes,
            ttl_seconds,
        } = request;
        if scopes.is_empty() {
            return Err(ApiError::BadRequest(
                "scopes must name at least one topic".to_owned(),
            ));
        }
        let principal = Principal::new(kind.clone(), name.clone());
        if principal == self.admin || principal.to_string() == ENGINE_PRINCIPAL {
            return Err(ApiError::BadRequest(format!(
                "{principal} is a name mansio uses itself"
            )));
        }
        let ttl = self.ttl(&kind, ttl_seconds)?;
        let now = self.clock.now_millis();
        let expires_at = ttl
            .map(|ttl| timestamp(now.saturating_add(millis(ttl))))
            .transpose()?;
        let info = PrincipalInfo {
            id: PrincipalId::generate(),
            kind,
            name,
            tenant,
            scopes,
            created_at: timestamp(now)?,
            expires_at,
        };
        self.registry.reserve(&info.tenant, &principal)?;
        let issued = self.issue(by, &info, ttl, now).await;
        if issued.is_err() {
            self.registry.release(&info.tenant, &principal);
        }
        issued
    }

    async fn issue(
        &self,
        by: &Principal,
        info: &PrincipalInfo,
        ttl: Option<Duration>,
        now: i64,
    ) -> Result<Issued, ApiError> {
        // The ledger first: a credential nobody recorded is the worse mistake.
        let fact = PrincipalFact {
            principal: info.clone(),
            reason: None,
        };
        self.engine
            .record(
                self.facts
                    .principal(PrincipalEvent::Created, by, now, &fact)?,
            )
            .await?;

        let minted = async {
            self.broker.ensure_tenant(&info.tenant).await?;
            self.broker
                .create_user(&info.tenant, &user_name(info.id), &info.scopes, ttl)
                .await
        }
        .await;
        let minted = match minted {
            Ok(minted) => minted,
            Err(error) => {
                self.record_revoked(by, info, RevokeReason::Failed).await;
                return Err(error.into());
            }
        };

        let digest = minted.credential.digest();
        let entry = Entry::Created {
            principal: info.clone(),
            user: minted.user,
            digest,
        };
        if let Err(error) = self.broker.system_append(SystemLog::Principals, &entry.encode()?).await {
            if let Err(cleanup) = self.broker.delete_user(minted.user).await {
                warn!(principal = %info.id, error = %cleanup, "a user without a record could not be deleted");
            }
            self.record_revoked(by, info, RevokeReason::Failed).await;
            return Err(error.into());
        }
        self.registry.insert(Record {
            info: info.clone(),
            user: minted.user,
            digest,
        });
        info!(
            principal = %info.id,
            kind = %info.kind,
            name = %info.name,
            tenant = %info.tenant,
            expires_at = ?info.expires_at.as_ref().map(ToString::to_string),
            "credential issued"
        );
        Ok(Issued {
            principal: info.clone(),
            credential: minted.credential,
        })
    }

    // ---- list and revoke --------------------------------------------------

    /// The principals that have a credential, oldest first: `principals_list`.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry cannot be loaded.
    pub(crate) async fn list(
        &self,
        tenant: Option<&TenantId>,
    ) -> Result<Vec<PrincipalInfo>, ApiError> {
        self.load().await?;
        Ok(self.registry.list(tenant))
    }

    /// Withdraw a credential: `principals_revoke`.
    ///
    /// The broker's user is deleted first, and with it the credential and its
    /// sessions, so the revocation holds from then on whatever else fails. It
    /// is then taken out of the registry and its log, and recorded.
    ///
    /// # Errors
    ///
    /// Returns 404 if no principal has the id, and 502 or 503 if the broker
    /// cannot delete the user, in which case the credential is still valid.
    pub(crate) async fn revoke(
        &self,
        by: &Principal,
        id: PrincipalId,
        reason: RevokeReason,
    ) -> Result<PrincipalInfo, ApiError> {
        self.load().await?;
        let found = self
            .registry
            .get(id)
            .ok_or_else(|| ApiError::NotFound(format!("no principal {id}")))?;
        self.broker.delete_user(found.user).await?;
        let record = self
            .registry
            .remove(id)
            .ok_or_else(|| ApiError::NotFound(format!("no principal {id}")))?;
        self.checked().remove(&id);
        self.save_revoked(id).await;
        self.record_revoked(by, &record.info, reason).await;
        info!(principal = %id, name = %record.info.name, ?reason, "credential revoked");
        Ok(record.info)
    }

    /// Write the revocation to the registry's log. If that fails the
    /// revocation stands: the user is gone, and the next start finds that out.
    async fn save_revoked(&self, id: PrincipalId) {
        let saved = async {
            let at = Timestamp::from_unix_millis(self.clock.now_millis())
                .map_err(|_| ApiError::Internal)?;
            let bytes = Entry::Revoked { id, at }.encode()?;
            self.broker
                .system_append(SystemLog::Principals, &bytes)
                .await
                .map_err(ApiError::from)
        }
        .await;
        if let Err(error) = saved {
            warn!(principal = %id, ?error, "the revocation could not be written to the registry's log");
        }
    }

    /// Record the revocation in the sink and on `facts.principal`. If that
    /// fails the revocation stands and the failure is logged.
    async fn record_revoked(&self, by: &Principal, info: &PrincipalInfo, reason: RevokeReason) {
        let fact = PrincipalFact {
            principal: info.clone(),
            reason: Some(reason),
        };
        let recorded = async {
            let envelope = self.facts.principal(
                PrincipalEvent::Revoked,
                by,
                self.clock.now_millis(),
                &fact,
            )?;
            self.engine.record(envelope).await
        }
        .await;
        if let Err(error) = recorded {
            warn!(principal = %info.id, ?error, "a revocation could not be recorded");
        }
    }

    // ---- the sweep --------------------------------------------------------

    /// Revoke every principal whose credential has expired, and forget the
    /// checks that are older than the recheck.
    pub async fn sweep(&self) {
        if let Err(error) = self.load().await {
            warn!(
                ?error,
                "the registry could not be loaded; the sweep will try again"
            );
            return;
        }
        let now = self.clock.now_millis();
        let by = self.engine_principal();
        for id in self.registry.expired(now) {
            if let Err(error) = self.revoke(&by, id, RevokeReason::Expired).await {
                warn!(principal = %id, ?error, "an expired credential could not be revoked");
            }
        }
        let recheck = millis(self.settings.recheck);
        self.checked().retain(|id, at| {
            (0..recheck).contains(&now.saturating_sub(*at)) && self.registry.get(*id).is_some()
        });
    }

    /// Run the sweep every `MANSIO_CREDENTIAL_SWEEP_SECONDS` until the task is
    /// dropped or aborted.
    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.settings.sweep);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            self.sweep().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as TestMutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use mansio_core::{
        BrokerUser, Caller, Envelope, Grant, MemoryBroker, Minted, NoSink, Stored, SystemRecord,
        Topic, Verb,
    };
    use serde_json::{Value, json};

    use super::*;
    use crate::clock::manual::ManualClock;
    use crate::settings::WorkSettings;

    const START: i64 = 1_792_000_000_000;

    /// A memory broker that counts what it is asked, remembers the users it
    /// made and deleted, and fails when told to.
    #[derive(Default)]
    struct Spy {
        inner: MemoryBroker,
        identifies: AtomicUsize,
        fail_create_user: AtomicBool,
        fail_delete_user: AtomicBool,
        fail_system_append: AtomicBool,
        /// `set_grants` says the user or its tenant is not found, whatever the
        /// memory broker holds.
        lose_grants: AtomicBool,
        made: TestMutex<Vec<BrokerUser>>,
        deleted: TestMutex<Vec<BrokerUser>>,
        /// Every `set_grants`, in order.
        told: TestMutex<Vec<(BrokerUser, TenantId, Vec<Grant>)>>,
    }

    fn down() -> BrokerError {
        BrokerError::Unavailable("injected".to_owned())
    }

    #[async_trait]
    impl Broker for Spy {
        async fn tenants(&self) -> Result<Vec<TenantId>, BrokerError> {
            self.inner.tenants().await
        }

        async fn topics(&self, tenant: &TenantId) -> Result<Vec<mansio_core::Topic>, BrokerError> {
            self.inner.topics(tenant).await
        }

        async fn ensure_tenant(&self, tenant: &TenantId) -> Result<(), BrokerError> {
            self.inner.ensure_tenant(tenant).await
        }

        async fn ensure_topic(
            &self,
            tenant: &TenantId,
            topic: &mansio_core::Topic,
        ) -> Result<(), BrokerError> {
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
            topic: &mansio_core::Topic,
            offset: u64,
            max: u32,
        ) -> Result<Vec<Stored>, BrokerError> {
            self.inner.poll(caller, tenant, topic, offset, max).await
        }

        async fn system_append(&self, log: SystemLog, record: &[u8]) -> Result<(), BrokerError> {
            if self.fail_system_append.load(Ordering::SeqCst) {
                return Err(down());
            }
            self.inner.system_append(log, record).await
        }

        async fn system_read(
            &self,
            log: SystemLog,
            offset: u64,
            max: u32,
        ) -> Result<Vec<SystemRecord>, BrokerError> {
            self.inner.system_read(log, offset, max).await
        }

        async fn create_user(
            &self,
            tenant: &TenantId,
            name: &str,
            grants: &[Grant],
            ttl: Option<Duration>,
        ) -> Result<Minted, BrokerError> {
            if self.fail_create_user.load(Ordering::SeqCst) {
                return Err(down());
            }
            let minted = self.inner.create_user(tenant, name, grants, ttl).await?;
            self.made.lock().unwrap().push(minted.user);
            Ok(minted)
        }

        async fn set_grants(
            &self,
            user: BrokerUser,
            tenant: &TenantId,
            grants: &[Grant],
        ) -> Result<(), BrokerError> {
            self.told
                .lock()
                .unwrap()
                .push((user, tenant.clone(), grants.to_vec()));
            if self.lose_grants.load(Ordering::SeqCst) {
                return Err(BrokerError::NotFound(format!("user {user}")));
            }
            self.inner.set_grants(user, tenant, grants).await
        }

        async fn delete_user(&self, user: BrokerUser) -> Result<(), BrokerError> {
            if self.fail_delete_user.load(Ordering::SeqCst) {
                return Err(down());
            }
            self.deleted.lock().unwrap().push(user);
            self.inner.delete_user(user).await
        }

        async fn list_users(&self) -> Result<Vec<mansio_core::BrokerUserInfo>, BrokerError> {
            self.inner.list_users().await
        }

        async fn identify(&self, credential: &Credential) -> Result<BrokerUser, BrokerError> {
            self.identifies.fetch_add(1, Ordering::SeqCst);
            self.inner.identify(credential).await
        }
    }

    struct Rig {
        broker: Arc<Spy>,
        clock: Arc<ManualClock>,
        credentials: Arc<Credentials>,
    }

    fn settings() -> CredentialSettings {
        CredentialSettings {
            agent_ttl: Duration::from_secs(60),
            recheck: Duration::from_secs(30),
            sweep: Duration::from_secs(10),
        }
    }

    impl Rig {
        fn new() -> Self {
            Self::over(Arc::default(), Arc::new(ManualClock::new(START)))
        }

        /// A server over a broker and a clock that another may have used.
        fn over(broker: Arc<Spy>, clock: Arc<ManualClock>) -> Self {
            let engine = Arc::new(Engine::new(
                broker.clone(),
                Arc::new(NoSink),
                clock.clone(),
                WorkSettings::default(),
                100,
            ));
            let credentials = Arc::new(Credentials::new(
                broker.clone(),
                engine,
                clock.clone(),
                settings(),
                admin(),
                100,
            ));
            Self {
                broker,
                clock,
                credentials,
            }
        }

        async fn restart(&self) -> Self {
            Self::over(self.broker.clone(), self.clock.clone())
        }

        async fn issue(
            &self,
            kind: &str,
            name: &str,
            ttl: Option<u64>,
        ) -> Result<Issued, ApiError> {
            self.credentials
                .create(&admin(), request(kind, name, ttl))
                .await
        }

        /// What is on `facts.principal`: `(type, reason)`.
        async fn facts(&self) -> Vec<(String, Option<String>)> {
            let topic: Topic = "facts.principal".parse().unwrap();
            match self
                .broker
                .poll(&Caller::Admin, &tenant(), &topic, 0, 100)
                .await
            {
                Ok(stored) => stored
                    .into_iter()
                    .map(|s| {
                        let reason = s
                            .envelope
                            .data
                            .as_ref()
                            .and_then(|d| d["reason"].as_str())
                            .map(str::to_owned);
                        (s.envelope.event_type.as_str().to_owned(), reason)
                    })
                    .collect(),
                Err(BrokerError::NotFound(_)) => Vec::new(),
                Err(error) => panic!("{error}"),
            }
        }
    }

    fn admin() -> Principal {
        "service:mansio".parse().unwrap()
    }

    fn tenant() -> TenantId {
        "acme".parse().unwrap()
    }

    fn request(kind: &str, name: &str, ttl: Option<u64>) -> CreatePrincipal {
        let mut body: Value = json!({
            "kind": kind,
            "name": name,
            "tenant": "acme",
            "scopes": [{"topic": "facts.build", "verbs": ["publish"]}],
        });
        if let Some(ttl) = ttl {
            body["ttl_seconds"] = json!(ttl);
        }
        serde_json::from_value(body).unwrap()
    }

    fn token(issued: &Issued) -> String {
        issued.credential.expose().to_owned()
    }

    fn is(error: &ApiError, expected: &ApiError) -> bool {
        std::mem::discriminant(error) == std::mem::discriminant(expected)
    }

    #[tokio::test]
    async fn an_issued_credential_is_its_principals_and_is_listed_without_the_secret() {
        let rig = Rig::new();
        let issued = rig.issue("agent", "builder-1", None).await.unwrap();
        let actor = rig.credentials.authenticate(&token(&issued)).await.unwrap();
        assert_eq!(actor.principal().to_string(), "agent:builder-1");
        assert_eq!(actor.id(), Some(issued.principal.id));
        assert!(!actor.is_admin());

        let listed = rig.credentials.list(Some(&tenant())).await.unwrap();
        assert_eq!(listed, vec![issued.principal.clone()]);
        assert!(
            !serde_json::to_string(&listed)
                .unwrap()
                .contains(&token(&issued))
        );
        assert!(
            rig.credentials
                .list(Some(&"other".parse().unwrap()))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            rig.facts().await,
            [("mansio.principal.created".to_owned(), None)]
        );
    }

    #[tokio::test]
    async fn a_token_that_was_never_issued_is_refused_without_asking_the_broker() {
        let rig = Rig::new();
        rig.issue("agent", "builder-1", None).await.unwrap();
        for presented in ["", "nope", &"x".repeat(5000), "Bearer"] {
            let error = rig.credentials.authenticate(presented).await.unwrap_err();
            assert!(is(&error, &ApiError::Unauthorized), "{presented:?}");
        }
        assert_eq!(rig.broker.identifies.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn the_broker_is_asked_about_a_token_again_only_after_the_recheck() {
        let rig = Rig::new();
        let issued = rig.issue("person", "paul", None).await.unwrap();
        let asked = || rig.broker.identifies.load(Ordering::SeqCst);

        rig.credentials.authenticate(&token(&issued)).await.unwrap();
        assert_eq!(asked(), 1);
        rig.clock.advance(Duration::from_secs(29));
        rig.credentials.authenticate(&token(&issued)).await.unwrap();
        assert_eq!(asked(), 1, "inside the recheck");
        rig.clock.advance(Duration::from_secs(1));
        rig.credentials.authenticate(&token(&issued)).await.unwrap();
        assert_eq!(asked(), 2, "at the recheck");
    }

    #[tokio::test]
    async fn a_user_deleted_behind_the_servers_back_is_refused_by_the_next_recheck() {
        let rig = Rig::new();
        let issued = rig.issue("person", "paul", None).await.unwrap();
        rig.credentials.authenticate(&token(&issued)).await.unwrap();
        let user = rig
            .credentials
            .registry
            .get(issued.principal.id)
            .unwrap()
            .user;
        rig.broker.inner.delete_user(user).await.unwrap();

        // Until the check is due the server goes by what it last heard.
        rig.clock.advance(Duration::from_secs(29));
        assert!(rig.credentials.authenticate(&token(&issued)).await.is_ok());
        rig.clock.advance(Duration::from_secs(1));
        let error = rig
            .credentials
            .authenticate(&token(&issued))
            .await
            .unwrap_err();
        assert!(is(&error, &ApiError::Unauthorized), "{error:?}");
    }

    #[tokio::test]
    async fn a_credential_that_belongs_to_another_user_is_refused() {
        let rig = Rig::new();
        let first = rig.issue("person", "paul", None).await.unwrap();
        let second = rig.issue("person", "ana", None).await.unwrap();
        // The broker says the first credential is a user the registry gave to
        // someone else: swap the registry's digest of the second for the first.
        let stolen = rig.credentials.registry.get(second.principal.id).unwrap();
        rig.credentials.registry.insert(Record {
            digest: first.credential.digest(),
            ..stolen
        });
        let error = rig
            .credentials
            .authenticate(&token(&first))
            .await
            .unwrap_err();
        assert!(is(&error, &ApiError::Unauthorized), "{error:?}");
    }

    #[tokio::test]
    async fn an_expired_principal_is_refused_even_if_the_broker_still_accepts_it() {
        let rig = Rig::new();
        let issued = rig.issue("agent", "builder-1", None).await.unwrap();
        assert!(issued.principal.expires_at.is_some(), "an agent expires");
        rig.credentials.authenticate(&token(&issued)).await.unwrap();

        rig.clock.advance(Duration::from_secs(60));
        let error = rig
            .credentials
            .authenticate(&token(&issued))
            .await
            .unwrap_err();
        assert!(is(&error, &ApiError::Unauthorized), "{error:?}");
        // The broker has not expired it, and the sweep revokes it.
        assert!(rig.broker.inner.identify(&issued.credential).await.is_ok());
        rig.credentials.sweep().await;
        assert!(rig.credentials.list(None).await.unwrap().is_empty());
        assert!(rig.broker.inner.identify(&issued.credential).await.is_err());
        assert_eq!(
            rig.facts().await,
            [
                ("mansio.principal.created".to_owned(), None),
                (
                    "mansio.principal.revoked".to_owned(),
                    Some("expired".to_owned())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn the_sweep_leaves_what_has_not_expired() {
        let rig = Rig::new();
        let agent = rig.issue("agent", "short", Some(5)).await.unwrap();
        let person = rig.issue("person", "paul", None).await.unwrap();
        let long = rig.issue("agent", "long", Some(500)).await.unwrap();
        rig.clock.advance(Duration::from_secs(6));
        rig.credentials.sweep().await;
        let left: std::collections::HashSet<PrincipalId> = rig
            .credentials
            .list(None)
            .await
            .unwrap()
            .iter()
            .map(|p| p.id)
            .collect();
        assert_eq!(
            left,
            std::collections::HashSet::from([person.principal.id, long.principal.id])
        );
        assert!(rig.credentials.authenticate(&token(&agent)).await.is_err());
    }

    #[tokio::test]
    async fn an_agent_always_expires_and_others_only_when_asked_and_a_bad_ttl_is_refused() {
        let rig = Rig::new();
        let at = |issued: &Issued| {
            issued
                .principal
                .expires_at
                .as_ref()
                .and_then(Timestamp::unix_millis)
        };
        let agent = rig.issue("agent", "a", None).await.unwrap();
        assert_eq!(at(&agent), Some(START + 60_000));
        let agent = rig.issue("agent", "b", Some(5)).await.unwrap();
        assert_eq!(at(&agent), Some(START + 5_000));
        assert_eq!(at(&rig.issue("person", "p", None).await.unwrap()), None);
        let asked = rig.issue("connector", "c", Some(90)).await.unwrap();
        assert_eq!(at(&asked), Some(START + 90_000));
        for bad in [Some(0), Some(u64::MAX)] {
            let error = rig.issue("agent", "bad", bad).await.unwrap_err();
            assert!(is(&error, &ApiError::BadRequest(String::new())), "{bad:?}");
        }
        // Nothing was made for the refusals, and their name is free.
        rig.issue("agent", "bad", None).await.unwrap();
    }

    #[tokio::test]
    async fn a_request_that_is_not_allowed_is_refused_before_anything_is_made() {
        let rig = Rig::new();
        let mut empty = request("agent", "x", None);
        empty.scopes.clear();
        for refused in [
            rig.credentials.create(&admin(), empty).await,
            rig.issue("service", "mansio", None).await,
            rig.issue("service", "mansio-engine", None).await,
        ] {
            let error = refused.unwrap_err();
            assert!(
                is(&error, &ApiError::BadRequest(String::new())),
                "{error:?}"
            );
        }
        rig.issue("agent", "x", None).await.unwrap();
        let error = rig.issue("agent", "x", None).await.unwrap_err();
        assert!(is(&error, &ApiError::Conflict(String::new())), "{error:?}");
        assert!(
            rig.broker.made.lock().unwrap().len() == 1,
            "one user, for the one credential"
        );
    }

    #[tokio::test]
    async fn a_credential_the_broker_could_not_make_is_never_returned_and_leaves_its_name_free() {
        let rig = Rig::new();
        rig.broker.fail_create_user.store(true, Ordering::SeqCst);
        let error = rig.issue("agent", "builder-1", None).await.unwrap_err();
        assert!(
            is(&error, &ApiError::Unavailable(String::new())),
            "{error:?}"
        );
        assert!(rig.credentials.list(None).await.unwrap().is_empty());
        // The ledger says it was started and that it came to nothing.
        assert_eq!(
            rig.facts().await,
            [
                ("mansio.principal.created".to_owned(), None),
                (
                    "mansio.principal.revoked".to_owned(),
                    Some("failed".to_owned())
                ),
            ]
        );

        rig.broker.fail_create_user.store(false, Ordering::SeqCst);
        rig.issue("agent", "builder-1", None).await.unwrap();
    }

    #[tokio::test]
    async fn a_credential_that_could_not_be_recorded_is_deleted_and_never_returned() {
        let rig = Rig::new();
        rig.broker.fail_system_append.store(true, Ordering::SeqCst);
        let error = rig.issue("agent", "builder-1", None).await.unwrap_err();
        assert!(
            is(&error, &ApiError::Unavailable(String::new())),
            "{error:?}"
        );
        assert!(rig.credentials.list(None).await.unwrap().is_empty());
        let made = rig.broker.made.lock().unwrap().clone();
        let deleted = rig.broker.deleted.lock().unwrap().clone();
        assert_eq!(made.len(), 1);
        assert_eq!(made, deleted, "the user it made is gone");
        assert_eq!(
            rig.facts().await.last().unwrap().1.as_deref(),
            Some("failed")
        );
    }

    #[tokio::test]
    async fn a_revocation_the_broker_cannot_carry_out_leaves_the_credential_in_force() {
        let rig = Rig::new();
        let issued = rig.issue("person", "paul", None).await.unwrap();
        rig.broker.fail_delete_user.store(true, Ordering::SeqCst);
        let error = rig
            .credentials
            .revoke(&admin(), issued.principal.id, RevokeReason::Requested)
            .await
            .unwrap_err();
        assert!(
            is(&error, &ApiError::Unavailable(String::new())),
            "{error:?}"
        );
        // Still its principal's, still listed, and still recorded as such.
        assert!(rig.credentials.authenticate(&token(&issued)).await.is_ok());
        assert_eq!(rig.credentials.list(None).await.unwrap().len(), 1);
        assert_eq!(rig.facts().await.len(), 1);

        rig.broker.fail_delete_user.store(false, Ordering::SeqCst);
        rig.credentials
            .revoke(&admin(), issued.principal.id, RevokeReason::Requested)
            .await
            .unwrap();
        assert!(rig.credentials.authenticate(&token(&issued)).await.is_err());
        let again = rig
            .credentials
            .revoke(&admin(), issued.principal.id, RevokeReason::Requested)
            .await
            .unwrap_err();
        assert!(is(&again, &ApiError::NotFound(String::new())), "{again:?}");
    }

    #[tokio::test]
    async fn the_registry_is_rebuilt_from_the_log_and_a_record_that_cannot_be_read_is_left_out() {
        let rig = Rig::new();
        let kept = rig.issue("person", "kept", None).await.unwrap();
        let dropped = rig.issue("person", "dropped", None).await.unwrap();
        rig.credentials
            .revoke(&admin(), dropped.principal.id, RevokeReason::Requested)
            .await
            .unwrap();
        rig.broker
            .system_append(SystemLog::Principals, b"not a record")
            .await
            .unwrap();

        let restarted = rig.restart().await;
        let actor = restarted
            .credentials
            .authenticate(&token(&kept))
            .await
            .unwrap();
        assert_eq!(actor.principal().to_string(), "person:kept");
        assert!(
            restarted
                .credentials
                .authenticate(&token(&dropped))
                .await
                .is_err()
        );
        assert_eq!(restarted.credentials.list(None).await.unwrap().len(), 1);
        // The names are as they were.
        let error = restarted.issue("person", "kept", None).await.unwrap_err();
        assert!(is(&error, &ApiError::Conflict(String::new())), "{error:?}");
        restarted.issue("person", "dropped", None).await.unwrap();
    }

    #[tokio::test]
    async fn a_principal_whose_broker_user_is_gone_is_withdrawn_when_the_server_starts() {
        let rig = Rig::new();
        let issued = rig.issue("person", "paul", None).await.unwrap();
        let user = rig
            .credentials
            .registry
            .get(issued.principal.id)
            .unwrap()
            .user;
        rig.broker.inner.delete_user(user).await.unwrap();

        let restarted = rig.restart().await;
        assert!(restarted.credentials.list(None).await.unwrap().is_empty());
        assert_eq!(
            restarted.facts().await.last().unwrap().1.as_deref(),
            Some("failed")
        );
        // And it stays withdrawn.
        let again = restarted.restart().await;
        assert!(again.credentials.list(None).await.unwrap().is_empty());
        assert_eq!(again.facts().await.len(), 2, "recorded once");
    }

    #[tokio::test]
    async fn every_principal_is_told_to_the_broker_again_on_start_with_its_tenant_and_scopes() {
        let rig = Rig::new();
        let build = rig.issue("agent", "builder-1", None).await.unwrap();
        let mut other = request("person", "paul", None);
        other.tenant = "other".parse().unwrap();
        other.scopes =
            vec![Grant::new("alerts".parse().unwrap(), [Verb::Ack, Verb::Subscribe]).unwrap()];
        let paul = rig.credentials.create(&admin(), other).await.unwrap();
        let revoked = rig.issue("person", "gone", None).await.unwrap();
        rig.credentials
            .revoke(&admin(), revoked.principal.id, RevokeReason::Requested)
            .await
            .unwrap();
        rig.broker.told.lock().unwrap().clear();

        let restarted = rig.restart().await;
        restarted.credentials.load().await.unwrap();

        let mut told = rig.broker.told.lock().unwrap().clone();
        told.sort_by_key(|(user, _, _)| user.0);
        let user_of = |issued: &Issued| {
            restarted
                .credentials
                .registry
                .get(issued.principal.id)
                .unwrap()
                .user
        };
        let mut expected = vec![
            (user_of(&build), tenant(), build.principal.scopes.clone()),
            (
                user_of(&paul),
                "other".parse().unwrap(),
                paul.principal.scopes.clone(),
            ),
        ];
        expected.sort_by_key(|(user, _, _)| user.0);
        assert_eq!(
            told, expected,
            "once each, for the principals the log says are current"
        );
    }

    #[tokio::test]
    async fn a_principal_the_broker_cannot_find_has_its_user_deleted_before_it_is_withdrawn() {
        let rig = Rig::new();
        let issued = rig.issue("person", "paul", None).await.unwrap();
        let user = rig
            .credentials
            .registry
            .get(issued.principal.id)
            .unwrap()
            .user;
        // The broker says it has no such user or tenant, and the user is
        // there all the same.
        rig.broker.lose_grants.store(true, Ordering::SeqCst);

        let restarted = rig.restart().await;
        assert!(restarted.credentials.list(None).await.unwrap().is_empty());
        assert!(
            rig.broker.deleted.lock().unwrap().contains(&user),
            "the user is deleted, not only forgotten"
        );
        assert!(
            rig.broker.inner.identify(&issued.credential).await.is_err(),
            "the token no longer signs in"
        );
    }

    #[tokio::test]
    async fn a_principal_whose_user_cannot_be_deleted_is_kept_and_tried_again_on_the_next_start() {
        let rig = Rig::new();
        let issued = rig.issue("person", "paul", None).await.unwrap();
        let user = rig
            .credentials
            .registry
            .get(issued.principal.id)
            .unwrap()
            .user;
        rig.broker.lose_grants.store(true, Ordering::SeqCst);
        rig.broker.fail_delete_user.store(true, Ordering::SeqCst);

        let restarted = rig.restart().await;
        restarted.credentials.load().await.unwrap();
        assert_eq!(
            restarted.credentials.list(None).await.unwrap().len(),
            1,
            "kept"
        );
        assert!(rig.broker.inner.identify(&issued.credential).await.is_ok());
        assert_eq!(
            restarted.facts().await.len(),
            1,
            "nothing was recorded as withdrawn"
        );

        rig.broker.fail_delete_user.store(false, Ordering::SeqCst);
        let again = rig.restart().await;
        assert!(again.credentials.list(None).await.unwrap().is_empty());
        assert!(rig.broker.deleted.lock().unwrap().contains(&user));
        assert!(rig.broker.inner.identify(&issued.credential).await.is_err());
    }

    #[tokio::test]
    async fn a_credential_with_facts_star_publish_cannot_write_what_only_mansio_writes() {
        let rig = Rig::new();
        let mut body = request("agent", "forger", Some(600));
        body.scopes =
            vec![Grant::new("facts.*".parse().unwrap(), [Verb::Publish, Verb::Subscribe]).unwrap()];
        let forger = rig.credentials.create(&admin(), body).await.unwrap();
        for name in [
            "facts.work",
            "facts.alert",
            "facts.principal",
            "dead",
            "facts.build",
        ] {
            let topic: Topic = name.parse().unwrap();
            rig.broker.ensure_topic(&tenant(), &topic).await.unwrap();
            let mut forged = Envelope::new(
                mansio_core::EventId::generate(),
                "probe".parse().unwrap(),
                "mansio.work.acked".parse().unwrap(),
                tenant(),
                topic,
                "service:mansio-engine".parse().unwrap(),
            );
            forged.subject = Some("item-1".parse().unwrap());
            let refused = rig
                .broker
                .publish(&Caller::Credential(&forger.credential), &forged)
                .await;
            assert!(
                matches!(refused, Err(BrokerError::Forbidden(_))),
                "{name}: a credential writes nothing at the broker: {refused:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_broker_user_made_for_a_principal_that_no_record_owns_is_deleted_on_start() {
        let rig = Rig::new();
        let kept = rig.issue("person", "kept", None).await.unwrap();
        let made = |name: String| {
            let broker = Arc::clone(&rig.broker);
            async move {
                broker
                    .inner
                    .create_user(&tenant(), &name, &[], None)
                    .await
                    .unwrap()
            }
        };
        // A user made just before a crash, whose record was never written, or
        // whose record cannot be read now.
        let orphan = made(user_name(PrincipalId::generate())).await;
        rig.broker
            .system_append(
                SystemLog::Principals,
                br#"{"op":"created","principal":"unreadable"}"#,
            )
            .await
            .unwrap();
        // Users that are not the principals': the server's own, and others.
        let admin = made("mansio-admin".to_owned()).await;
        let shaped_wrong = made(format!("mansio-{}", "A".repeat(32))).await;
        let someone = made("someone".to_owned()).await;

        let restarted = rig.restart().await;
        restarted.credentials.load().await.unwrap();

        let deleted = rig.broker.deleted.lock().unwrap().clone();
        assert_eq!(deleted, [orphan.user], "only the orphan");
        assert!(rig.broker.inner.identify(&orphan.credential).await.is_err());
        for left in [&admin, &shaped_wrong, &someone] {
            assert!(rig.broker.inner.identify(&left.credential).await.is_ok());
        }
        assert!(
            restarted
                .credentials
                .authenticate(&token(&kept))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_user_that_cannot_be_deleted_is_left_for_the_next_start() {
        let rig = Rig::new();
        rig.issue("person", "kept", None).await.unwrap();
        let orphan = rig
            .broker
            .inner
            .create_user(&tenant(), &user_name(PrincipalId::generate()), &[], None)
            .await
            .unwrap();
        rig.broker.fail_delete_user.store(true, Ordering::SeqCst);
        let restarted = rig.restart().await;
        restarted.credentials.load().await.unwrap();
        assert!(rig.broker.inner.identify(&orphan.credential).await.is_ok());

        rig.broker.fail_delete_user.store(false, Ordering::SeqCst);
        rig.restart().await.credentials.load().await.unwrap();
        assert!(rig.broker.inner.identify(&orphan.credential).await.is_err());
    }

    #[test]
    fn a_principals_user_name_is_mansio_and_its_id_and_nothing_else_has_that_shape() {
        let id = PrincipalId::generate();
        assert!(is_principal_user(&user_name(id)));
        for name in [
            "mansio",
            "mansio-",
            "mansio-admin",
            "iggy",
            &format!("mansio-{}", "a".repeat(31)),
            &format!("mansio-{}", "a".repeat(33)),
            &format!("mansio-{}", "g".repeat(32)),
            &format!("x{}", user_name(id)),
        ] {
            assert!(!is_principal_user(name), "{name}");
        }
    }
}
