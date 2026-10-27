//! An in-memory broker, for tests, development and `MANSIO_BROKER=memory`.
//!
//! It holds users, scopes and expiring credentials the way the Iggy backend
//! does, so that what a credential may do is enforced by the broker in every
//! test and not only against Iggy: a credential belongs to one tenant, reads
//! and writes only the topics its scopes cover, and stops working when it
//! expires or its user is deleted.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::access::{Action, Grant, allows};
use crate::broker::{Broker, BrokerUser, Caller, Minted, Stored, SystemLog, SystemRecord};
use crate::credential::{Credential, CredentialDigest};
use crate::envelope::Envelope;
use crate::error::BrokerError;
use crate::id::{ConsumerName, TenantId};
use crate::topic::Topic;

/// A topic's messages, and the offset each named consumer last committed.
#[derive(Debug, Default)]
struct Log {
    messages: Vec<Envelope>,
    consumers: HashMap<ConsumerName, u64>,
}

type Topics = HashMap<Topic, Log>;

/// A user the broker made, and what it may reach.
#[derive(Debug, Clone)]
struct User {
    tenant: TenantId,
    grants: Vec<Grant>,
}

/// A credential: whose it is, and until when.
#[derive(Debug)]
struct Token {
    user: BrokerUser,
    expires: Option<Instant>,
}

#[derive(Debug, Default)]
struct Users {
    last: u32,
    by_id: HashMap<BrokerUser, User>,
    tokens: HashMap<CredentialDigest, Token>,
}

impl Users {
    /// The user a credential signs in as, if it is known and has not expired.
    fn resolve(&self, credential: &Credential) -> Result<(BrokerUser, &User), BrokerError> {
        let invalid = || BrokerError::Unauthenticated("unknown, expired or revoked".to_owned());
        let token = self.tokens.get(&credential.digest()).ok_or_else(invalid)?;
        if token
            .expires
            .is_some_and(|expires| expires <= Instant::now())
        {
            return Err(invalid());
        }
        let user = self.by_id.get(&token.user).ok_or_else(invalid)?;
        Ok((token.user, user))
    }
}

/// What a credential's user may reach, for the checks of one request. `None`
/// is the admin identity, which may reach everything.
type Granted = Option<User>;

/// A [`Broker`] that keeps everything in memory and forgets it on drop.
#[derive(Debug, Default)]
pub struct MemoryBroker {
    tenants: Mutex<HashMap<TenantId, Topics>>,
    system: Mutex<HashMap<SystemLog, Vec<Vec<u8>>>>,
    users: Mutex<Users>,
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

    fn users(&self) -> MutexGuard<'_, Users> {
        self.users.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Who the caller is, for the checks of one request.
    fn granted(&self, caller: &Caller<'_>) -> Result<Granted, BrokerError> {
        match caller {
            Caller::Admin => Ok(None),
            Caller::Credential(credential) => {
                let users = self.users();
                let (_, user) = users.resolve(credential)?;
                Ok(Some(user.clone()))
            }
        }
    }
}

fn missing_topic(tenant: &TenantId, topic: &Topic) -> BrokerError {
    BrokerError::NotFound(format!("topic {topic} in tenant {tenant}"))
}

/// Whether the caller may do this to the topic. The broker looks the topic
/// up first, as Iggy does, so that is checked before this.
fn permit(
    granted: &Granted,
    action: Action,
    tenant: &TenantId,
    topic: &Topic,
) -> Result<(), BrokerError> {
    let Some(user) = granted else { return Ok(()) };
    if user.tenant == *tenant && allows(&user.grants, action, topic) {
        Ok(())
    } else {
        let verb = match action {
            Action::Send => "send to",
            _ => "read",
        };
        Err(BrokerError::Forbidden(format!(
            "the credential may not {verb} {topic} in {tenant}"
        )))
    }
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

    async fn publish(
        &self,
        caller: &Caller<'_>,
        envelope: &Envelope,
    ) -> Result<Option<u64>, BrokerError> {
        envelope.validate()?;
        let granted = self.granted(caller)?;
        let mut tenants = self.lock();
        let log = tenants
            .get_mut(&envelope.tenant)
            .and_then(|topics| topics.get_mut(&envelope.topic))
            .ok_or_else(|| missing_topic(&envelope.tenant, &envelope.topic))?;
        permit(&granted, Action::Send, &envelope.tenant, &envelope.topic)?;
        log.messages.push(envelope.clone());
        Ok(Some(u64::try_from(log.messages.len() - 1).unwrap_or(u64::MAX)))
    }

    async fn poll(
        &self,
        caller: &Caller<'_>,
        tenant: &TenantId,
        topic: &Topic,
        offset: u64,
        max: u32,
    ) -> Result<Vec<Stored>, BrokerError> {
        let granted = self.granted(caller)?;
        let tenants = self.lock();
        let log = tenants
            .get(tenant)
            .and_then(|topics| topics.get(topic))
            .ok_or_else(|| missing_topic(tenant, topic))?;
        permit(&granted, Action::Poll, tenant, topic)?;
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let stored = log
            .messages
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

    async fn consumer_offset(
        &self,
        caller: &Caller<'_>,
        tenant: &TenantId,
        topic: &Topic,
        consumer: &ConsumerName,
    ) -> Result<Option<u64>, BrokerError> {
        let granted = self.granted(caller)?;
        let tenants = self.lock();
        let log = tenants
            .get(tenant)
            .and_then(|topics| topics.get(topic))
            .ok_or_else(|| missing_topic(tenant, topic))?;
        permit(&granted, Action::Poll, tenant, topic)?;
        Ok(log.consumers.get(consumer).copied())
    }

    async fn commit_offset(
        &self,
        caller: &Caller<'_>,
        tenant: &TenantId,
        topic: &Topic,
        consumer: &ConsumerName,
        offset: u64,
    ) -> Result<(), BrokerError> {
        let granted = self.granted(caller)?;
        let mut tenants = self.lock();
        let log = tenants
            .get_mut(tenant)
            .and_then(|topics| topics.get_mut(topic))
            .ok_or_else(|| missing_topic(tenant, topic))?;
        permit(&granted, Action::Poll, tenant, topic)?;
        log.consumers.insert(consumer.clone(), offset);
        Ok(())
    }

    async fn system_append(&self, log: SystemLog, record: &[u8]) -> Result<(), BrokerError> {
        self.system
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(log)
            .or_default()
            .push(record.to_vec());
        Ok(())
    }

    async fn system_read(
        &self,
        log: SystemLog,
        offset: u64,
        max: u32,
    ) -> Result<Vec<SystemRecord>, BrokerError> {
        let system = self.system.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(system
            .get(&log)
            .map_or(&[][..], Vec::as_slice)
            .iter()
            .enumerate()
            .skip(usize::try_from(offset).unwrap_or(usize::MAX))
            .take(usize::try_from(max).unwrap_or(usize::MAX))
            .map(|(index, payload)| SystemRecord {
                offset: u64::try_from(index).unwrap_or(u64::MAX),
                payload: payload.clone(),
            })
            .collect())
    }

    async fn create_user(
        &self,
        tenant: &TenantId,
        _name: &str,
        grants: &[Grant],
        ttl: Option<Duration>,
    ) -> Result<Minted, BrokerError> {
        if !self.lock().contains_key(tenant) {
            return Err(BrokerError::NotFound(format!("tenant {tenant}")));
        }
        let expires = ttl
            .map(|ttl| {
                Instant::now()
                    .checked_add(ttl)
                    .ok_or_else(|| BrokerError::Rejected("the expiry is too far off".to_owned()))
            })
            .transpose()?;
        let credential = Credential::random();
        let mut users = self.users();
        users.last += 1;
        let user = BrokerUser(users.last);
        users.by_id.insert(
            user,
            User {
                tenant: tenant.clone(),
                grants: grants.to_vec(),
            },
        );
        users
            .tokens
            .insert(credential.digest(), Token { user, expires });
        Ok(Minted { user, credential })
    }

    async fn set_grants(
        &self,
        user: BrokerUser,
        tenant: &TenantId,
        grants: &[Grant],
    ) -> Result<(), BrokerError> {
        if !self.lock().contains_key(tenant) {
            return Err(BrokerError::NotFound(format!("tenant {tenant}")));
        }
        let mut users = self.users();
        let known = users
            .by_id
            .get_mut(&user)
            .ok_or_else(|| BrokerError::NotFound(format!("user {user}")))?;
        known.tenant = tenant.clone();
        known.grants = grants.to_vec();
        Ok(())
    }

    async fn delete_user(&self, user: BrokerUser) -> Result<(), BrokerError> {
        let mut users = self.users();
        users.by_id.remove(&user);
        users.tokens.retain(|_, token| token.user != user);
        Ok(())
    }

    async fn identify(&self, credential: &Credential) -> Result<BrokerUser, BrokerError> {
        self.users().resolve(credential).map(|(user, _)| user)
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
        let offset = broker.publish(&Caller::Admin, &sent).await.unwrap();
        assert_eq!(offset, Some(0));

        let tenant: TenantId = "acme".parse().unwrap();
        let topic: Topic = "facts.build".parse().unwrap();
        let got = broker
            .poll(&Caller::Admin, &tenant, &topic, 0, 10)
            .await
            .unwrap();
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
                .publish(
                    &Caller::Admin,
                    &envelope("acme", "alerts", &format!("t{n}")),
                )
                .await
                .unwrap();
            assert_eq!(offset, Some(n));
        }
        let tenant: TenantId = "acme".parse().unwrap();
        let page = broker
            .poll(&Caller::Admin, &tenant, &Topic::Alerts, 1, 2)
            .await
            .unwrap();
        let offsets: Vec<u64> = page.iter().map(|s| s.offset).collect();
        assert_eq!(offsets, [1, 2]);
        assert_eq!(page[0].envelope.event_type.as_str(), "t1");

        assert!(
            broker
                .poll(&Caller::Admin, &tenant, &Topic::Alerts, 5, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            broker
                .poll(&Caller::Admin, &tenant, &Topic::Alerts, u64::MAX, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            broker
                .poll(&Caller::Admin, &tenant, &Topic::Alerts, 0, 0)
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
            .publish(&Caller::Admin, &envelope("acme", "alerts", "t"))
            .await
            .unwrap();

        let other: TenantId = "other".parse().unwrap();
        assert!(
            broker
                .poll(&Caller::Admin, &other, &Topic::Alerts, 0, 10)
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
            .publish(&Caller::Admin, &envelope("acme", "dead", "t"))
            .await
            .unwrap_err();
        assert!(matches!(error, BrokerError::NotFound(_)));
        let error = broker
            .poll(&Caller::Admin, &tenant, &Topic::Dead, 0, 1)
            .await
            .unwrap_err();
        assert!(matches!(error, BrokerError::NotFound(_)));

        broker.ensure_tenant(&tenant).await.unwrap();
        let error = broker
            .poll(&Caller::Admin, &tenant, &Topic::Dead, 0, 1)
            .await
            .unwrap_err();
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
            .publish(&Caller::Admin, &envelope("acme", "presence", "t"))
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
                .poll(&Caller::Admin, &tenant, &Topic::Presence, 0, 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    fn grant(pattern: &str, verbs: &[crate::Verb]) -> Grant {
        Grant::new(pattern.parse().unwrap(), verbs.iter().copied()).unwrap()
    }

    /// A broker with the tenants `acme` and `other`, each holding `alerts`,
    /// `facts.build` and `work.builds`.
    async fn two_tenants() -> MemoryBroker {
        let broker = MemoryBroker::new();
        for tenant in ["acme", "other"] {
            let tenant: TenantId = tenant.parse().unwrap();
            broker.ensure_tenant(&tenant).await.unwrap();
            for topic in ["alerts", "facts.build", "work.builds"] {
                broker
                    .ensure_topic(&tenant, &topic.parse().unwrap())
                    .await
                    .unwrap();
            }
        }
        broker
    }

    async fn user(
        broker: &MemoryBroker,
        tenant: &str,
        grants: &[Grant],
        ttl: Option<Duration>,
    ) -> Minted {
        broker
            .create_user(&tenant.parse().unwrap(), "test", grants, ttl)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_credential_reads_and_writes_only_what_its_scopes_cover_in_its_tenant() {
        use crate::Verb;
        let broker = two_tenants().await;
        let minted = user(
            &broker,
            "acme",
            &[
                grant("facts.build", &[Verb::Publish]),
                grant("work.builds", &[Verb::Claim]),
            ],
            None,
        )
        .await;
        let caller = Caller::Credential(&minted.credential);
        let acme: TenantId = "acme".parse().unwrap();

        // What it was given.
        let sent = envelope("acme", "facts.build", "t");
        assert_eq!(broker.publish(&caller, &sent).await.unwrap(), Some(0));
        broker
            .poll(&caller, &acme, &"work.builds".parse().unwrap(), 0, 1)
            .await
            .unwrap();

        // Nothing else: another verb on a topic it has, a topic it lacks, and
        // another tenant's topic of the same name.
        let refused = |result: Result<_, BrokerError>| {
            assert!(
                matches!(result, Err(BrokerError::Forbidden(_))),
                "{result:?}"
            );
        };
        refused(
            broker
                .poll(&caller, &acme, &"facts.build".parse().unwrap(), 0, 1)
                .await
                .map(|_| ()),
        );
        refused(
            broker
                .publish(&caller, &envelope("acme", "work.builds", "t"))
                .await
                .map(|_| ()),
        );
        refused(
            broker
                .publish(&caller, &envelope("acme", "alerts", "t"))
                .await
                .map(|_| ()),
        );
        refused(
            broker
                .publish(&caller, &envelope("other", "facts.build", "t"))
                .await
                .map(|_| ()),
        );
        refused(
            broker
                .poll(&caller, &"other".parse().unwrap(), &Topic::Alerts, 0, 0)
                .await
                .map(|_| ()),
        );
        // Nothing was written by the refusals.
        assert!(
            broker
                .poll(
                    &Caller::Admin,
                    &"other".parse().unwrap(),
                    &"facts.build".parse().unwrap(),
                    0,
                    10
                )
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_missing_topic_is_not_found_before_it_is_forbidden() {
        let broker = two_tenants().await;
        let minted = user(&broker, "acme", &[], None).await;
        let caller = Caller::Credential(&minted.credential);
        let error = broker
            .poll(&caller, &"acme".parse().unwrap(), &Topic::Dead, 0, 1)
            .await
            .unwrap_err();
        assert!(matches!(error, BrokerError::NotFound(_)), "{error:?}");
    }

    #[tokio::test]
    async fn a_zero_count_poll_checks_the_topic_and_the_permission_and_reads_nothing() {
        use crate::Verb;
        let broker = two_tenants().await;
        let acme: TenantId = "acme".parse().unwrap();
        let reader = user(
            &broker,
            "acme",
            &[grant("alerts", &[Verb::Subscribe])],
            None,
        )
        .await;
        let writer = user(&broker, "acme", &[grant("alerts", &[Verb::Publish])], None).await;
        broker
            .publish(&Caller::Admin, &envelope("acme", "alerts", "t"))
            .await
            .unwrap();
        let read = broker
            .poll(
                &Caller::Credential(&reader.credential),
                &acme,
                &Topic::Alerts,
                0,
                0,
            )
            .await
            .unwrap();
        assert!(read.is_empty());
        let error = broker
            .poll(
                &Caller::Credential(&writer.credential),
                &acme,
                &Topic::Alerts,
                0,
                0,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, BrokerError::Forbidden(_)));
    }

    #[tokio::test]
    async fn a_pattern_reaches_topics_created_after_the_user() {
        use crate::Verb;
        let broker = two_tenants().await;
        let minted = user(
            &broker,
            "acme",
            &[grant("facts.*", &[Verb::Publish, Verb::Subscribe])],
            None,
        )
        .await;
        let caller = Caller::Credential(&minted.credential);
        let later = "facts.later";
        broker
            .ensure_topic(&"acme".parse().unwrap(), &later.parse().unwrap())
            .await
            .unwrap();
        broker
            .publish(&caller, &envelope("acme", later, "t"))
            .await
            .unwrap();
        let read = broker
            .poll(
                &caller,
                &"acme".parse().unwrap(),
                &later.parse().unwrap(),
                0,
                10,
            )
            .await
            .unwrap();
        assert_eq!(read.len(), 1);
    }

    #[tokio::test]
    async fn an_unknown_credential_is_not_authenticated() {
        let broker = two_tenants().await;
        let stranger = Credential::new("not-a-credential");
        assert!(matches!(
            broker.identify(&stranger).await,
            Err(BrokerError::Unauthenticated(_))
        ));
        assert!(matches!(
            broker
                .poll(
                    &Caller::Credential(&stranger),
                    &"acme".parse().unwrap(),
                    &Topic::Alerts,
                    0,
                    1
                )
                .await,
            Err(BrokerError::Unauthenticated(_))
        ));
        assert!(matches!(
            broker
                .publish(
                    &Caller::Credential(&stranger),
                    &envelope("acme", "alerts", "t")
                )
                .await,
            Err(BrokerError::Unauthenticated(_))
        ));
    }

    #[tokio::test]
    async fn a_credential_stops_working_when_it_expires() {
        use crate::Verb;
        let broker = two_tenants().await;
        let minted = user(
            &broker,
            "acme",
            &[grant("alerts", &[Verb::Subscribe])],
            Some(Duration::from_millis(150)),
        )
        .await;
        let caller = Caller::Credential(&minted.credential);
        let acme: TenantId = "acme".parse().unwrap();
        assert_eq!(
            broker.identify(&minted.credential).await.unwrap(),
            minted.user
        );
        broker
            .poll(&caller, &acme, &Topic::Alerts, 0, 1)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(matches!(
            broker.identify(&minted.credential).await,
            Err(BrokerError::Unauthenticated(_))
        ));
        assert!(matches!(
            broker.poll(&caller, &acme, &Topic::Alerts, 0, 1).await,
            Err(BrokerError::Unauthenticated(_))
        ));
    }

    #[tokio::test]
    async fn deleting_a_user_ends_its_credential_and_is_not_an_error_twice() {
        use crate::Verb;
        let broker = two_tenants().await;
        let minted = user(
            &broker,
            "acme",
            &[grant("alerts", &[Verb::Subscribe])],
            None,
        )
        .await;
        let other = user(
            &broker,
            "acme",
            &[grant("alerts", &[Verb::Subscribe])],
            None,
        )
        .await;
        broker.delete_user(minted.user).await.unwrap();
        broker.delete_user(minted.user).await.unwrap();
        assert!(matches!(
            broker.identify(&minted.credential).await,
            Err(BrokerError::Unauthenticated(_))
        ));
        assert_eq!(
            broker.identify(&other.credential).await.unwrap(),
            other.user
        );
    }

    #[tokio::test]
    async fn users_need_a_tenant_and_have_distinct_numbers_and_credentials() {
        let broker = MemoryBroker::new();
        let missing: TenantId = "nope".parse().unwrap();
        assert!(matches!(
            broker.create_user(&missing, "x", &[], None).await,
            Err(BrokerError::NotFound(_))
        ));
        broker.ensure_tenant(&missing).await.unwrap();
        let a = broker.create_user(&missing, "a", &[], None).await.unwrap();
        let b = broker.create_user(&missing, "b", &[], None).await.unwrap();
        assert_ne!(a.user, b.user);
        assert_ne!(a.credential.expose(), b.credential.expose());
        assert!(a.credential.expose().len() >= 64);
    }

    #[tokio::test]
    async fn set_grants_replaces_what_a_user_may_do() {
        use crate::Verb;
        let broker = two_tenants().await;
        let acme: TenantId = "acme".parse().unwrap();
        let minted = user(&broker, "acme", &[], None).await;
        let caller = Caller::Credential(&minted.credential);
        assert!(matches!(
            broker.poll(&caller, &acme, &Topic::Alerts, 0, 1).await,
            Err(BrokerError::Forbidden(_))
        ));
        broker
            .set_grants(minted.user, &acme, &[grant("alerts", &[Verb::Subscribe])])
            .await
            .unwrap();
        broker
            .poll(&caller, &acme, &Topic::Alerts, 0, 1)
            .await
            .unwrap();
        assert!(matches!(
            broker.set_grants(BrokerUser(999), &acme, &[]).await,
            Err(BrokerError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn the_system_logs_keep_order_and_pages_and_are_apart() {
        let broker = MemoryBroker::new();
        let principals = SystemLog::Principals;
        assert!(broker.system_read(principals, 0, 10).await.unwrap().is_empty());
        for n in 0..4u8 {
            broker.system_append(principals, &[n]).await.unwrap();
        }
        let page = broker.system_read(principals, 1, 2).await.unwrap();
        let seen: Vec<(u64, Vec<u8>)> = page.into_iter().map(|r| (r.offset, r.payload)).collect();
        assert_eq!(seen, [(1, vec![1]), (2, vec![2])]);
        assert!(broker.system_read(principals, 4, 10).await.unwrap().is_empty());

        // Another log has nothing of the first one's.
        let webhooks = SystemLog::Webhooks;
        assert!(broker.system_read(webhooks, 0, 10).await.unwrap().is_empty());
        broker.system_append(webhooks, b"w").await.unwrap();
        let read = broker.system_read(webhooks, 0, 10).await.unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].payload, b"w");
        assert_eq!(broker.system_read(principals, 0, 10).await.unwrap().len(), 4);
    }

    fn consumer(name: &str) -> ConsumerName {
        name.parse().unwrap()
    }

    #[tokio::test]
    async fn a_consumer_keeps_the_offset_it_committed_per_topic_and_name() {
        let broker = two_tenants().await;
        let acme: TenantId = "acme".parse().unwrap();
        let (alerts, builds): (Topic, Topic) = (Topic::Alerts, "facts.build".parse().unwrap());
        let get = |topic: &Topic, name: &str| {
            let (topic, name) = (topic.clone(), consumer(name));
            let acme = acme.clone();
            let broker = &broker;
            async move {
                broker
                    .consumer_offset(&Caller::Admin, &acme, &topic, &name)
                    .await
                    .unwrap()
            }
        };

        assert_eq!(get(&alerts, "a").await, None);
        broker
            .commit_offset(&Caller::Admin, &acme, &alerts, &consumer("a"), 4)
            .await
            .unwrap();
        assert_eq!(get(&alerts, "a").await, Some(4));
        // Zero is an offset like any other, and is not "none".
        broker
            .commit_offset(&Caller::Admin, &acme, &alerts, &consumer("b"), 0)
            .await
            .unwrap();
        assert_eq!(get(&alerts, "b").await, Some(0));
        // Another topic and another name are other consumers.
        assert_eq!(get(&builds, "a").await, None);
        // A commit replaces what was there, even with a smaller offset.
        broker
            .commit_offset(&Caller::Admin, &acme, &alerts, &consumer("a"), 2)
            .await
            .unwrap();
        assert_eq!(get(&alerts, "a").await, Some(2));
    }

    #[tokio::test]
    async fn a_consumer_needs_a_topic_and_the_right_to_read_it() {
        use crate::Verb;
        let broker = two_tenants().await;
        let acme: TenantId = "acme".parse().unwrap();
        let name = consumer("watcher");
        let missing = broker
            .consumer_offset(&Caller::Admin, &acme, &Topic::Dead, &name)
            .await
            .unwrap_err();
        assert!(matches!(missing, BrokerError::NotFound(_)), "{missing:?}");
        let missing = broker
            .commit_offset(&Caller::Admin, &acme, &Topic::Dead, &name, 1)
            .await
            .unwrap_err();
        assert!(matches!(missing, BrokerError::NotFound(_)), "{missing:?}");

        let reader = user(&broker, "acme", &[grant("alerts", &[Verb::Subscribe])], None).await;
        let writer = user(&broker, "acme", &[grant("alerts", &[Verb::Publish])], None).await;
        let (read, write) = (
            Caller::Credential(&reader.credential),
            Caller::Credential(&writer.credential),
        );
        broker
            .commit_offset(&read, &acme, &Topic::Alerts, &name, 3)
            .await
            .unwrap();
        assert_eq!(
            broker
                .consumer_offset(&read, &acme, &Topic::Alerts, &name)
                .await
                .unwrap(),
            Some(3)
        );
        // A credential that may not read the topic may not move a consumer on
        // it or see where one is, and another tenant's credential neither.
        assert!(matches!(
            broker.commit_offset(&write, &acme, &Topic::Alerts, &name, 9).await,
            Err(BrokerError::Forbidden(_))
        ));
        assert!(matches!(
            broker.consumer_offset(&write, &acme, &Topic::Alerts, &name).await,
            Err(BrokerError::Forbidden(_))
        ));
        let other: TenantId = "other".parse().unwrap();
        assert!(matches!(
            broker.consumer_offset(&read, &other, &Topic::Alerts, &name).await,
            Err(BrokerError::Forbidden(_))
        ));
        assert_eq!(
            broker
                .consumer_offset(&Caller::Admin, &acme, &Topic::Alerts, &name)
                .await
                .unwrap(),
            Some(3),
            "the refused commit changed nothing"
        );
    }
}
