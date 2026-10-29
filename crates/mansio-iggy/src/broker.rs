//! `IggyBroker`: the `Broker` trait over the Iggy Rust SDK, TCP transport.
//!
//! The broker holds one client for the admin identity, which creates streams,
//! topics and users, and one per credential in use, which Iggy holds to the
//! permissions of the user the credential belongs to.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use iggy::prelude::{
    AutoLogin, Client, Consumer, Credentials, Durability as IggyDurability, Identifier,
    IggyByteSize, IggyClient, IggyClientBuilder, IggyDuration, IggyError, IggyExpiry, IggyMessage,
    MaxTopicSize, MessageClient, Partitioning, PersonalAccessTokenClient, PollingStrategy,
    StreamClient, TopicClient, TopicCreateOptions, UserClient, UserStatus,
};
use mansio_core::{
    Broker, BrokerError, BrokerUser, BrokerUserInfo, Caller, Credential, Envelope, EventId, Grant,
    Minted, Stored, SystemRecord, TenantId, Topic,
};
use tokio::sync::{Mutex as AsyncMutex, OnceCell};
use tracing::{info, warn};

use crate::access::{Users, no_permissions, permissions};
use crate::config::{Durability, DurabilityPolicy, IggyConfig, Retention};
use crate::sessions::{Session, Sessions};

/// The Iggy stream that holds the server's own records. Its name is not a
/// valid tenant id, so no tenant can be given it and no principal's
/// permissions ever name it.
pub const SYSTEM_STREAM: &str = "mansio.system";
/// The topic of [`SYSTEM_STREAM`] that holds the registry of principals.
pub const SYSTEM_TOPIC: &str = "principals";
/// What the personal access token of every user mansio makes is called.
const TOKEN_NAME: &str = "mansio";

/// The one partition of every topic.
const PARTITION: u32 = 0;

/// The streams and topics this process has found or created, so that a
/// request after the first skips the round trips that check for them.
#[derive(Debug, Default)]
struct Ensured {
    /// A tenant is a known stream; its topics are the known topics in it.
    known: Mutex<HashMap<TenantId, HashSet<Topic>>>,
    /// Topics this process made whose users have not been told yet, because
    /// telling them failed. The next request tries again, and finds the topic
    /// already there.
    unopened: Mutex<HashSet<(TenantId, Topic)>>,
}

impl Ensured {
    fn lock(&self) -> MutexGuard<'_, HashMap<TenantId, HashSet<Topic>>> {
        self.known.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn has_stream(&self, tenant: &TenantId) -> bool {
        self.lock().contains_key(tenant)
    }

    fn has_topic(&self, tenant: &TenantId, topic: &Topic) -> bool {
        self.lock()
            .get(tenant)
            .is_some_and(|topics| topics.contains(topic))
    }

    fn unopened(&self) -> MutexGuard<'_, HashSet<(TenantId, Topic)>> {
        self.unopened.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A topic was made and its users are yet to be told.
    fn mark_unopened(&self, tenant: &TenantId, topic: &Topic) {
        self.unopened().insert((tenant.clone(), topic.clone()));
    }

    fn is_unopened(&self, tenant: &TenantId, topic: &Topic) -> bool {
        self.unopened().contains(&(tenant.clone(), topic.clone()))
    }

    fn mark_opened(&self, tenant: &TenantId, topic: &Topic) {
        self.unopened().remove(&(tenant.clone(), topic.clone()));
    }

    fn add_stream(&self, tenant: &TenantId) {
        self.lock().entry(tenant.clone()).or_default();
    }

    fn add_topic(&self, tenant: &TenantId, topic: &Topic) {
        self.lock()
            .entry(tenant.clone())
            .or_default()
            .insert(topic.clone());
    }

    /// Check the stream and all its topics again next time, because Iggy
    /// says one of them is gone.
    fn forget(&self, tenant: &TenantId) {
        self.lock().remove(tenant);
    }

    /// Pass the result of a request for this tenant through, and forget the
    /// tenant if Iggy says its stream or one of its topics is gone.
    fn forget_if_missing<T>(
        &self,
        tenant: &TenantId,
        result: Result<T, BrokerError>,
    ) -> Result<T, BrokerError> {
        if matches!(result, Err(BrokerError::NotFound(_))) {
            self.forget(tenant);
        }
        result
    }
}

/// A [`Broker`] backed by an Iggy server.
#[derive(Debug)]
pub struct IggyBroker {
    /// The admin identity.
    client: Arc<IggyClient>,
    address: String,
    connect_timeout: Duration,
    retention: Retention,
    durability: DurabilityPolicy,
    segment_size_bytes: u64,
    locate_window: u32,
    ensured: Ensured,
    /// The clients signed in with a principal's credential.
    sessions: Sessions,
    /// The scopes of every user made, to apply again when a topic is made.
    users: Users,
    /// Held while permissions are worked out and applied, so that a user made
    /// while a topic is being made is not left without it.
    permissions: AsyncMutex<()>,
    /// Set once the stream of the server's own records exists.
    system_ready: OnceCell<()>,
}

/// A client for the address, not yet connected and signed in as nobody.
fn new_client(address: &str) -> Result<IggyClient, BrokerError> {
    IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(address.to_owned())
        .build()
        .map_err(map_iggy)
}

/// What applying a user's permissions came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Applied {
    /// The permissions its scopes work out to are set.
    Done,
    /// Its tenant's stream is not there, and it has no permissions.
    NoStream,
    /// There is no such user.
    NoUser,
}

/// End a client that is no longer used. Whether Iggy hears about it does not
/// matter.
async fn end(client: &IggyClient) {
    let _ = client.shutdown().await;
}

impl IggyBroker {
    /// Connect to Iggy and sign in as the configured user. The connection
    /// reconnects and signs in again by itself if it drops.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Unavailable`] if Iggy does not answer within
    /// the connect timeout or the sign-in fails.
    pub async fn connect(config: IggyConfig) -> Result<Self, BrokerError> {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(config.address.clone())
            .with_auto_sign_in(AutoLogin::Enabled(Credentials::UsernamePassword(
                config.username.clone(),
                config.password.clone(),
            )))
            .build()
            .map_err(map_iggy)?;
        tokio::time::timeout(config.connect_timeout, client.connect())
            .await
            .map_err(|_| {
                BrokerError::Unavailable(format!(
                    "no answer from Iggy at {} within {:?}",
                    config.address, config.connect_timeout
                ))
            })?
            .map_err(map_iggy)?;
        info!(address = %config.address, user = %config.username, "connected to Iggy");
        Ok(Self {
            client: Arc::new(client),
            address: config.address,
            connect_timeout: config.connect_timeout,
            retention: config.retention,
            durability: config.durability,
            segment_size_bytes: config.segment_size_bytes,
            locate_window: config.locate_window,
            ensured: Ensured::default(),
            sessions: Sessions::default(),
            users: Users::default(),
            permissions: AsyncMutex::new(()),
            system_ready: OnceCell::new(),
        })
    }

    /// Close the connections.
    ///
    /// # Errors
    ///
    /// Returns an error if Iggy cannot be told.
    pub async fn shutdown(&self) -> Result<(), BrokerError> {
        for session in self.sessions.drain() {
            end(&session.client).await;
        }
        self.client.shutdown().await.map_err(map_iggy)
    }

    // ---- sessions ---------------------------------------------------------

    /// A client signed in with the credential. Iggy refusing the sign-in with
    /// a reason that is about the token (see [`login_failure`]) means the
    /// credential is not valid: unknown, expired, or its user is gone.
    async fn sign_in(&self, credential: &Credential) -> Result<Session, BrokerError> {
        let timed_out = || {
            BrokerError::Unavailable(format!(
                "no answer from Iggy at {} within {:?}",
                self.address, self.connect_timeout
            ))
        };
        let client = new_client(&self.address)?;
        match tokio::time::timeout(self.connect_timeout, client.connect()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                end(&client).await;
                return Err(map_iggy(error));
            }
            Err(_) => {
                end(&client).await;
                return Err(timed_out());
            }
        }
        let identity = tokio::time::timeout(
            self.connect_timeout,
            client.login_with_personal_access_token(credential.expose()),
        )
        .await;
        match identity {
            Ok(Ok(identity)) => Ok(Session {
                client: Arc::new(client),
                user: BrokerUser(identity.user_id),
            }),
            Ok(Err(error)) => {
                end(&client).await;
                Err(login_failure(error))
            }
            Err(_) => {
                end(&client).await;
                Err(timed_out())
            }
        }
    }

    /// The credential's session: the one kept, or a new one that is kept.
    async fn session(&self, credential: &Credential) -> Result<Session, BrokerError> {
        let key = credential.digest();
        if let Some(session) = self.sessions.get(&key) {
            return Ok(session);
        }
        let (kept, spare) = self.sessions.insert(key, self.sign_in(credential).await?);
        if let Some(spare) = spare {
            end(&spare.client).await;
        }
        Ok(kept)
    }

    /// The client a call is made on.
    async fn client_for(&self, caller: &Caller<'_>) -> Result<Arc<IggyClient>, BrokerError> {
        match caller {
            Caller::Admin => Ok(Arc::clone(&self.client)),
            Caller::Credential(credential) => Ok(self.session(credential).await?.client),
        }
    }

    /// Turn what Iggy answered a caller into a result. A credential Iggy no
    /// longer accepts loses its session, so that the next request signs in
    /// again and is told so.
    async fn settle<T>(
        &self,
        caller: &Caller<'_>,
        result: Result<T, IggyError>,
    ) -> Result<T, BrokerError> {
        match (caller, result) {
            (_, Ok(value)) => Ok(value),
            (Caller::Admin, Err(error)) => Err(map_iggy(error)),
            (Caller::Credential(credential), Err(error)) => {
                let mapped = map_credential(error);
                if matches!(mapped, BrokerError::Unauthenticated(_))
                    && let Some(session) = self.sessions.remove(&credential.digest())
                {
                    end(&session.client).await;
                }
                Err(mapped)
            }
        }
    }

    // ---- messages ---------------------------------------------------------

    /// Find where a message landed by looking through the most recent ones.
    /// Iggy can accept a send without reporting an offset, and the message is
    /// then near the end. `None` if it is not among them.
    async fn locate(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        id: EventId,
    ) -> Result<Option<u64>, IggyError> {
        if self.locate_window == 0 {
            return Ok(None);
        }
        let polled = self
            .client
            .poll_messages(
                stream,
                topic,
                Some(PARTITION),
                &Consumer::default(),
                &PollingStrategy::last(),
                self.locate_window,
                false,
            )
            .await?;
        Ok(find_offset(&polled.messages, id))
    }

    /// Send the envelope. The message is stored once this returns `Ok`, with
    /// or without an offset.
    async fn send(
        &self,
        caller: &Caller<'_>,
        envelope: &Envelope,
    ) -> Result<Option<u64>, BrokerError> {
        let client = self.client_for(caller).await?;
        let stream = stream_id(&envelope.tenant)?;
        let topic = topic_id(&envelope.topic)?;
        let mut message = IggyMessage::builder()
            .id(envelope.id.as_u128())
            .payload(Bytes::from(envelope.to_json_vec()?))
            .build()
            .map_err(map_iggy)?;
        let sent = client
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(PARTITION),
                std::slice::from_mut(&mut message),
            )
            .await;
        let sent = self.settle(caller, sent).await?;
        if let Some(confirmation) = sent.confirmations.first() {
            return Ok(Some(confirmation.base_offset));
        }
        // Iggy accepted the message but did not say where. Failing now would
        // have the client send it again and store it twice, so a message that
        // cannot be found is answered without an offset. The search reads as
        // the admin identity: the sender may be allowed to send and not to
        // read.
        let located = match self.locate(&stream, &topic, envelope.id).await {
            Ok(located) => located,
            Err(error) => {
                warn!(id = %envelope.id, %error, "searching for the offset of an accepted message failed");
                None
            }
        };
        if located.is_none() {
            warn!(
                tenant = %envelope.tenant,
                topic = %envelope.topic,
                id = %envelope.id,
                window = self.locate_window,
                "Iggy accepted a message without an offset and it was not found among the recent ones; answering without one"
            );
        }
        Ok(located)
    }

    /// Read from the topic. With `max` 0 nothing is returned, and Iggy is
    /// still asked for the newest message, so that it decides whether the
    /// topic exists and the caller may read it.
    async fn fetch(
        &self,
        caller: &Caller<'_>,
        tenant: &TenantId,
        topic: &Topic,
        offset: u64,
        max: u32,
    ) -> Result<Vec<Stored>, BrokerError> {
        let client = self.client_for(caller).await?;
        let (strategy, count) = if max == 0 {
            (PollingStrategy::last(), 1)
        } else {
            (PollingStrategy::offset(offset), max)
        };
        let polled = client
            .poll_messages(
                &stream_id(tenant)?,
                &topic_id(topic)?,
                Some(PARTITION),
                &Consumer::default(),
                &strategy,
                count,
                false,
            )
            .await;
        let polled = self.settle(caller, polled).await?;
        if max == 0 {
            return Ok(Vec::new());
        }
        polled
            .messages
            .iter()
            .map(|message| decode(tenant, topic, message))
            .collect()
    }

    /// Make sure the tenant's stream exists, and remember that it does.
    async fn find_or_create_stream(&self, tenant: &TenantId) -> Result<(), BrokerError> {
        if self.ensured.has_stream(tenant) {
            return Ok(());
        }
        if self.make_stream(tenant.as_str()).await? {
            // Iggy may give a new stream the number of one that was deleted,
            // and permissions name streams by number: whoever held the old
            // number must not hold the new stream.
            self.refresh_users().await?;
        }
        self.ensured.add_stream(tenant);
        Ok(())
    }

    /// Make sure the topic exists in the tenant's stream, and remember that
    /// it does.
    async fn find_or_create_topic(
        &self,
        tenant: &TenantId,
        topic: &Topic,
    ) -> Result<(), BrokerError> {
        if self.ensured.has_topic(tenant, topic) {
            return Ok(());
        }
        let options = topic_options(
            &self.retention,
            &self.durability,
            self.segment_size_bytes,
            topic,
        );
        let made = self
            .make_topic(tenant.as_str(), &topic.to_string(), &options)
            .await?;
        // Users get a topic their scopes cover when it is made. One that was
        // found is already open to them: `load` applied every principal over
        // every topic there was, and a topic made since was opened by the
        // request that made it, unless that failed and left it marked.
        if made {
            self.ensured.mark_unopened(tenant, topic);
        }
        if self.ensured.is_unopened(tenant, topic) {
            self.open_to_users(tenant).await?;
            self.ensured.mark_opened(tenant, topic);
        }
        self.ensured.add_topic(tenant, topic);
        Ok(())
    }

    // ---- streams and topics -----------------------------------------------

    /// Create the stream unless it exists. Whether this call made it.
    async fn make_stream(&self, name: &str) -> Result<bool, BrokerError> {
        let id = Identifier::named(name).map_err(map_iggy)?;
        match self.client.get_stream(&id).await {
            Ok(Some(_)) => return Ok(false),
            Ok(None) | Err(IggyError::StreamIdNotFound(_) | IggyError::StreamNameNotFound(_)) => {}
            Err(error) => return Err(map_iggy(error)),
        }
        match self.client.create_stream(name).await {
            Ok(_) => {
                info!(stream = name, "created stream");
                Ok(true)
            }
            // Another request created it first.
            Err(IggyError::StreamNameAlreadyExists(_)) => Ok(false),
            Err(error) => Err(map_iggy(error)),
        }
    }

    /// Create the topic unless it exists. Whether this call made it.
    async fn make_topic(
        &self,
        stream: &str,
        name: &str,
        options: &TopicCreateOptions,
    ) -> Result<bool, BrokerError> {
        let stream_id = Identifier::named(stream).map_err(map_iggy)?;
        let id = Identifier::named(name).map_err(map_iggy)?;
        match self.client.get_topic(&stream_id, &id).await {
            Ok(Some(_)) => return Ok(false),
            Ok(None) | Err(IggyError::TopicIdNotFound(..) | IggyError::TopicNameNotFound(..)) => {}
            Err(error) => return Err(map_iggy(error)),
        }
        match self.client.create_topic(&stream_id, name, options).await {
            Ok(_) => {
                info!(stream, topic = name, "created topic");
                Ok(true)
            }
            // Another request created it first.
            Err(IggyError::TopicNameAlreadyExists(..)) => Ok(false),
            Err(error) => Err(map_iggy(error)),
        }
    }

    // ---- the server's own records -----------------------------------------

    /// Make the stream and topic for the server's own records, once.
    async fn ensure_system(&self) -> Result<(), BrokerError> {
        self.system_ready
            .get_or_try_init(|| async {
                self.make_stream(SYSTEM_STREAM).await?;
                self.make_topic(SYSTEM_STREAM, SYSTEM_TOPIC, &system_topic_options())
                    .await?;
                Ok::<(), BrokerError>(())
            })
            .await
            .map(|_| ())
    }

    // ---- users ------------------------------------------------------------

    /// The stream's number and its topics' numbers and names, or `None` if
    /// there is no stream.
    async fn stream_topics(
        &self,
        tenant: &TenantId,
    ) -> Result<Option<(u32, Vec<(u32, Topic)>)>, BrokerError> {
        let stream = stream_id(tenant)?;
        let details = match self.client.get_stream(&stream).await {
            Ok(Some(details)) => details,
            Ok(None) | Err(IggyError::StreamIdNotFound(_) | IggyError::StreamNameNotFound(_)) => {
                return Ok(None);
            }
            Err(error) => return Err(map_iggy(error)),
        };
        let topics = self.client.get_topics(&stream).await.map_err(map_iggy)?;
        let named = topics
            .iter()
            .filter_map(|topic| Some((topic.id, topic.name.parse().ok()?)))
            .collect();
        Ok(Some((details.id, named)))
    }

    /// Give the user the permissions its scopes work out to on the topics the
    /// tenant has now, or none if the tenant's stream is gone. The caller
    /// holds the permissions lock.
    async fn apply(
        &self,
        user: BrokerUser,
        tenant: &TenantId,
        grants: &[Grant],
    ) -> Result<Applied, BrokerError> {
        let (granted, applied) = match self.stream_topics(tenant).await? {
            Some((stream, topics)) => (permissions(stream, &topics, grants), Applied::Done),
            None => (no_permissions(), Applied::NoStream),
        };
        match self
            .client
            .update_permissions(&user_id(user)?, Some(granted))
            .await
        {
            Ok(()) => Ok(applied),
            Err(IggyError::ResourceNotFound(_)) => Ok(Applied::NoUser),
            Err(error) => Err(map_iggy(error)),
        }
    }

    /// A topic of the tenant was found or made: work out again what every
    /// user of the tenant may read, which includes the topic if their scopes
    /// cover it.
    async fn open_to_users(&self, tenant: &TenantId) -> Result<(), BrokerError> {
        let _held = self.permissions.lock().await;
        for (user, grants) in self.users.of_tenant(tenant) {
            if self.apply(user, tenant, &grants).await? == Applied::NoUser {
                warn!(%tenant, %user, "a user the broker was told about is gone");
                self.users.remove(user);
            }
        }
        Ok(())
    }

    /// A stream was made: work out again what every user of every tenant may
    /// read. A user whose stream is not there gets no permissions.
    async fn refresh_users(&self) -> Result<(), BrokerError> {
        let _held = self.permissions.lock().await;
        for (user, tenant, grants) in self.users.all() {
            if self.apply(user, &tenant, &grants).await? == Applied::NoUser {
                warn!(%tenant, %user, "a user the broker was told about is gone");
                self.users.remove(user);
            }
        }
        Ok(())
    }

    /// Sign in as the new user, make its token, and sign out.
    async fn mint_token(
        &self,
        name: &str,
        password: &Credential,
        ttl: Option<Duration>,
    ) -> Result<Credential, BrokerError> {
        let client = new_client(&self.address)?;
        let expiry = ttl.map_or(IggyExpiry::NeverExpire, |ttl| {
            IggyExpiry::ExpireDuration(IggyDuration::from(ttl))
        });
        let minted = tokio::time::timeout(self.connect_timeout, async {
            client.connect().await?;
            client.login_user(name, password.expose()).await?;
            client
                .create_personal_access_token(TOKEN_NAME, expiry)
                .await
        })
        .await;
        end(&client).await;
        match minted {
            Ok(Ok(raw)) => Ok(Credential::new(raw.token)),
            Ok(Err(error)) => Err(map_iggy(error)),
            Err(_) => Err(BrokerError::Unavailable(format!(
                "no answer from Iggy at {} within {:?}",
                self.address, self.connect_timeout
            ))),
        }
    }
}

/// Where the message with this id is, if it is among `messages`.
fn find_offset(messages: &[IggyMessage], id: EventId) -> Option<u64> {
    let wanted = id.as_u128();
    messages
        .iter()
        .rev()
        .find(|message| message.header.id == wanted)
        .map(|message| message.header.offset)
}

fn stream_id(tenant: &TenantId) -> Result<Identifier, BrokerError> {
    Identifier::named(tenant.as_str()).map_err(map_iggy)
}

fn topic_id(topic: &Topic) -> Result<Identifier, BrokerError> {
    Identifier::named(&topic.to_string()).map_err(map_iggy)
}

/// A user by its number, which no other user can share.
fn user_id(user: BrokerUser) -> Result<Identifier, BrokerError> {
    Identifier::numeric(user.0).map_err(map_iggy)
}

/// How the topic of the server's own records is created: kept forever, no size
/// cap, and written to stable storage before it is confirmed, because losing a
/// record loses a principal.
fn system_topic_options() -> TopicCreateOptions {
    TopicCreateOptions {
        partitions_count: Some(1),
        message_expiry: Some(IggyExpiry::NeverExpire),
        max_topic_size: Some(MaxTopicSize::Unlimited),
        durability: IggyDurability::Persisted,
        consumer_offset_durability: IggyDurability::Persisted,
        ..TopicCreateOptions::default()
    }
}

fn expiry(retention: &Retention, topic: &Topic) -> IggyExpiry {
    match retention.for_kind(topic.kind()) {
        Some(keep) => IggyExpiry::ExpireDuration(IggyDuration::from(keep)),
        None => IggyExpiry::NeverExpire,
    }
}

fn iggy_durability(durability: Durability) -> IggyDurability {
    match durability {
        Durability::Replicated => IggyDurability::Replicated,
        Durability::Persisted => IggyDurability::Persisted,
    }
}

/// How a topic is created: one partition, the retention and durability of its
/// kind, the configured segment size (none leaves it to Iggy), and no size cap
/// when it keeps messages forever, so that a default cap on the server never
/// trims it.
fn topic_options(
    retention: &Retention,
    durability: &DurabilityPolicy,
    segment_size_bytes: u64,
    topic: &Topic,
) -> TopicCreateOptions {
    let durability = iggy_durability(durability.for_kind(topic.kind()));
    TopicCreateOptions {
        partitions_count: Some(1),
        message_expiry: Some(expiry(retention, topic)),
        segment_size: (segment_size_bytes > 0).then(|| IggyByteSize::new(segment_size_bytes)),
        max_topic_size: retention
            .for_kind(topic.kind())
            .is_none()
            .then_some(MaxTopicSize::Unlimited),
        durability,
        consumer_offset_durability: durability,
        ..TopicCreateOptions::default()
    }
}

/// Turn what Iggy stored back into an envelope, checking it is the one asked
/// for.
fn decode(tenant: &TenantId, topic: &Topic, message: &IggyMessage) -> Result<Stored, BrokerError> {
    let offset = message.header.offset;
    let corrupt = |why: String| {
        BrokerError::Corrupt(format!("offset {offset} of {topic} in {tenant}: {why}"))
    };
    let envelope =
        Envelope::from_json_slice(&message.payload).map_err(|e| corrupt(e.to_string()))?;
    if envelope.tenant != *tenant || envelope.topic != *topic {
        return Err(corrupt(format!(
            "it says it belongs to {} in {}",
            envelope.topic, envelope.tenant
        )));
    }
    Ok(Stored { offset, envelope })
}

/// Sort what the Iggy SDK reports into what the engine acts on.
///
/// `Rejected` says nothing was stored, so it is an allowlist: the statuses
/// that Iggy, or the SDK before it sends, gives for a request it has refused
/// outright. Every other error is `Unavailable`, the answer that has a client
/// send again with the same id. The TCP client writes and flushes a request
/// before it reads the reply, and a reply that cannot be read fails after the
/// write: a frame of the wrong size or shape is `InvalidCommand`, an eviction
/// whose reason is unreadable is `Unauthenticated`, a status code it does not
/// know is `Error`. None of those says whether the write was applied.
fn map_iggy(error: IggyError) -> BrokerError {
    let text = error.to_string();
    match error {
        IggyError::StreamIdNotFound(_)
        | IggyError::StreamNameNotFound(_)
        | IggyError::TopicIdNotFound(..)
        | IggyError::TopicNameNotFound(..)
        | IggyError::ResourceNotFound(_) => BrokerError::NotFound(text),
        IggyError::Unauthorized
        | IggyError::InvalidIdentifier
        | IggyError::InvalidStreamName
        | IggyError::InvalidStreamId
        | IggyError::InvalidTopicName
        | IggyError::InvalidTopicId
        | IggyError::InvalidMessagesCount
        | IggyError::TooBigMessagePayload => BrokerError::Rejected(text),
        _ => BrokerError::Unavailable(text),
    }
}

/// Why Iggy would not sign in with a token. Only the refusals that say the
/// token is not valid (unknown, malformed, expired, or its user gone or
/// inactive) are `Unauthenticated`, which ends a kept session and answers a
/// caller 401 for good. Anything else, an answer about reaching Iggy or one
/// that is not known to be about the token, is `Unavailable`: a valid
/// credential must not be refused for good because of a fault that is not its
/// own. Nothing is written by a sign-in, so unlike `map_iggy` there is no
/// outcome to be unsure of.
fn login_failure(error: IggyError) -> BrokerError {
    let text = error.to_string();
    match error {
        IggyError::InvalidPersonalAccessToken
        | IggyError::PersonalAccessTokenExpired(..)
        | IggyError::InvalidCredentials
        | IggyError::Unauthenticated
        | IggyError::UserInactive
        | IggyError::ResourceNotFound(_)
        | IggyError::InvalidFormat => BrokerError::Unauthenticated(text),
        _ => BrokerError::Unavailable(text),
    }
}

/// What Iggy said to a request made for a credential: not signed in, not
/// allowed, or something else.
fn map_credential(error: IggyError) -> BrokerError {
    let text = error.to_string();
    match error {
        IggyError::Unauthorized => BrokerError::Forbidden(text),
        IggyError::Unauthenticated
        | IggyError::InvalidCredentials
        | IggyError::InvalidPersonalAccessToken
        | IggyError::PersonalAccessTokenExpired(..) => BrokerError::Unauthenticated(text),
        other => map_iggy(other),
    }
}

#[async_trait]
impl Broker for IggyBroker {
    async fn tenants(&self) -> Result<Vec<TenantId>, BrokerError> {
        let streams = self.client.get_streams().await.map_err(map_iggy)?;
        Ok(streams
            .iter()
            .filter_map(|stream| stream.name.parse().ok())
            .collect())
    }

    async fn topics(&self, tenant: &TenantId) -> Result<Vec<Topic>, BrokerError> {
        let topics = self
            .client
            .get_topics(&stream_id(tenant)?)
            .await
            .map_err(map_iggy)?;
        Ok(topics
            .iter()
            .filter_map(|topic| topic.name.parse().ok())
            .collect())
    }

    async fn ensure_tenant(&self, tenant: &TenantId) -> Result<(), BrokerError> {
        let result = self.find_or_create_stream(tenant).await;
        self.ensured.forget_if_missing(tenant, result)
    }

    async fn ensure_topic(&self, tenant: &TenantId, topic: &Topic) -> Result<(), BrokerError> {
        let result = self.find_or_create_topic(tenant, topic).await;
        self.ensured.forget_if_missing(tenant, result)
    }

    async fn publish(
        &self,
        caller: &Caller<'_>,
        envelope: &Envelope,
    ) -> Result<Option<u64>, BrokerError> {
        let result = self.send(caller, envelope).await;
        self.ensured.forget_if_missing(&envelope.tenant, result)
    }

    async fn poll(
        &self,
        caller: &Caller<'_>,
        tenant: &TenantId,
        topic: &Topic,
        offset: u64,
        max: u32,
    ) -> Result<Vec<Stored>, BrokerError> {
        let result = self.fetch(caller, tenant, topic, offset, max).await;
        // A credential reading a topic that is not there says nothing about
        // what the server has found: it would otherwise make the server check
        // the whole tenant again on anyone's say-so.
        match caller {
            Caller::Admin => self.ensured.forget_if_missing(tenant, result),
            Caller::Credential(_) => result,
        }
    }

    async fn system_append(&self, record: &[u8]) -> Result<(), BrokerError> {
        self.ensure_system().await?;
        let mut message = IggyMessage::builder()
            .payload(Bytes::copy_from_slice(record))
            .build()
            .map_err(map_iggy)?;
        self.client
            .send_messages(
                &Identifier::named(SYSTEM_STREAM).map_err(map_iggy)?,
                &Identifier::named(SYSTEM_TOPIC).map_err(map_iggy)?,
                &Partitioning::partition_id(PARTITION),
                std::slice::from_mut(&mut message),
            )
            .await
            .map_err(map_iggy)?;
        Ok(())
    }

    async fn system_read(&self, offset: u64, max: u32) -> Result<Vec<SystemRecord>, BrokerError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        self.ensure_system().await?;
        let polled = self
            .client
            .poll_messages(
                &Identifier::named(SYSTEM_STREAM).map_err(map_iggy)?,
                &Identifier::named(SYSTEM_TOPIC).map_err(map_iggy)?,
                Some(PARTITION),
                &Consumer::default(),
                &PollingStrategy::offset(offset),
                max,
                false,
            )
            .await
            .map_err(map_iggy)?;
        Ok(polled
            .messages
            .iter()
            .map(|message| SystemRecord {
                offset: message.header.offset,
                payload: message.payload.to_vec(),
            })
            .collect())
    }

    async fn create_user(
        &self,
        tenant: &TenantId,
        name: &str,
        grants: &[Grant],
        ttl: Option<Duration>,
    ) -> Result<Minted, BrokerError> {
        // Nobody is told this password: it is used once, to sign in as the
        // user and make its token, and then dropped.
        let password = Credential::random();
        let user = {
            // Held until the user is remembered, so that a topic made in the
            // meantime is opened to it; not held for the sign-in and the token.
            let _held = self.permissions.lock().await;
            let (stream, topics) = self
                .stream_topics(tenant)
                .await?
                .ok_or_else(|| BrokerError::NotFound(format!("tenant {tenant}")))?;
            let made = self
                .client
                .create_user(
                    name,
                    password.expose(),
                    UserStatus::Active,
                    Some(permissions(stream, &topics, grants)),
                )
                .await
                .map_err(map_iggy)?;
            let user = BrokerUser(made.id);
            self.users.set(user, tenant, grants);
            user
        };
        match self.mint_token(name, &password, ttl).await {
            Ok(credential) => Ok(Minted { user, credential }),
            Err(error) => {
                self.users.remove(user);
                if let Err(cleanup) = self.client.delete_user(&user_id(user)?).await {
                    warn!(%user, error = %cleanup, "a user without a token could not be deleted");
                }
                Err(error)
            }
        }
    }

    async fn set_grants(
        &self,
        user: BrokerUser,
        tenant: &TenantId,
        grants: &[Grant],
    ) -> Result<(), BrokerError> {
        let _held = self.permissions.lock().await;
        match self.apply(user, tenant, grants).await? {
            Applied::Done => {
                self.users.set(user, tenant, grants);
                Ok(())
            }
            Applied::NoStream => Err(BrokerError::NotFound(format!("tenant {tenant}"))),
            Applied::NoUser => Err(BrokerError::NotFound(format!("user {user}"))),
        }
    }

    async fn list_users(&self) -> Result<Vec<BrokerUserInfo>, BrokerError> {
        let users = self.client.get_users().await.map_err(map_iggy)?;
        Ok(users
            .into_iter()
            .map(|found| BrokerUserInfo {
                user: BrokerUser(found.id),
                name: found.username,
            })
            .collect())
    }

    async fn delete_user(&self, user: BrokerUser) -> Result<(), BrokerError> {
        match self.client.delete_user(&user_id(user)?).await {
            Ok(()) | Err(IggyError::ResourceNotFound(_)) => {}
            Err(error) => return Err(map_iggy(error)),
        }
        self.users.remove(user);
        for session in self.sessions.remove_user(user) {
            end(&session.client).await;
        }
        Ok(())
    }

    async fn identify(&self, credential: &Credential) -> Result<BrokerUser, BrokerError> {
        let Some(open) = self.sessions.get(&credential.digest()) else {
            return Ok(self.session(credential).await?.user);
        };
        // The session is in use and must not be signed in again under the
        // requests on it, so Iggy is asked on a client of its own.
        match self.sign_in(credential).await {
            Ok(checked) => {
                end(&checked.client).await;
                Ok(checked.user)
            }
            Err(error @ BrokerError::Unauthenticated(_)) => {
                if let Some(gone) = self.sessions.remove(&credential.digest()) {
                    end(&gone.client).await;
                }
                Err(error)
            }
            Err(error) => {
                warn!(user = %open.user, %error, "a credential could not be checked");
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use iggy::prelude::IggyMessageHeader;
    use mansio_core::{Principal, TopicKind};

    use super::*;

    fn envelope(tenant: &str, topic: &str) -> Envelope {
        Envelope::new(
            EventId::generate(),
            "test".parse().unwrap(),
            "t".parse().unwrap(),
            tenant.parse().unwrap(),
            topic.parse().unwrap(),
            "service:test".parse::<Principal>().unwrap(),
        )
    }

    fn message(offset: u64, id: u128, payload: Vec<u8>) -> IggyMessage {
        let header = IggyMessageHeader {
            id,
            offset,
            ..IggyMessageHeader::default()
        };
        IggyMessage {
            header,
            payload: Bytes::from(payload),
            user_headers: None,
        }
    }

    #[test]
    fn decode_returns_the_envelope_with_its_offset() {
        let sent = envelope("acme", "facts.build");
        let wire = message(7, sent.id.as_u128(), sent.to_json_vec().unwrap());
        let stored = decode(&sent.tenant, &sent.topic, &wire).unwrap();
        assert_eq!(
            stored,
            Stored {
                offset: 7,
                envelope: sent
            }
        );
    }

    #[test]
    fn decode_refuses_what_does_not_belong() {
        let sent = envelope("acme", "facts.build");
        let wire = message(0, sent.id.as_u128(), sent.to_json_vec().unwrap());

        let other_tenant: TenantId = "other".parse().unwrap();
        assert!(matches!(
            decode(&other_tenant, &sent.topic, &wire),
            Err(BrokerError::Corrupt(_))
        ));
        assert!(matches!(
            decode(&sent.tenant, &Topic::Alerts, &wire),
            Err(BrokerError::Corrupt(_))
        ));

        let not_json = message(0, 1, b"hello".to_vec());
        assert!(matches!(
            decode(&sent.tenant, &sent.topic, &not_json),
            Err(BrokerError::Corrupt(_))
        ));
    }

    #[test]
    fn iggy_errors_are_sorted_into_broker_errors() {
        let stream = Identifier::named("acme").unwrap();
        assert!(matches!(
            map_iggy(IggyError::StreamIdNotFound(stream)),
            BrokerError::NotFound(_)
        ));
        assert!(matches!(
            map_iggy(IggyError::TopicNameNotFound("t".into(), "s".into())),
            BrokerError::NotFound(_)
        ));
        assert!(matches!(
            map_iggy(IggyError::Disconnected),
            BrokerError::Unavailable(_)
        ));
        assert!(matches!(
            map_iggy(IggyError::CannotEstablishConnection),
            BrokerError::Unavailable(_)
        ));
    }

    #[test]
    fn only_a_named_refusal_that_precedes_the_write_is_rejected() {
        for refusal in [
            IggyError::Unauthorized,
            IggyError::InvalidIdentifier,
            IggyError::InvalidStreamName,
            IggyError::InvalidStreamId,
            IggyError::InvalidTopicName,
            IggyError::InvalidTopicId,
            IggyError::InvalidMessagesCount,
            IggyError::TooBigMessagePayload,
        ] {
            let text = refusal.to_string();
            assert!(
                matches!(map_iggy(refusal), BrokerError::Rejected(why) if why == text),
                "{text}"
            );
        }
    }

    #[test]
    fn an_error_that_can_follow_the_write_is_never_a_refusal() {
        // What the TCP client raises once it has written a request and cannot
        // make sense of the reply (a frame it cannot read, a status it does
        // not know, an eviction whose reason it maps to a sign-in error): the
        // outcome is not known, and a 502 would have the client believe
        // nothing was stored.
        for after_the_write in [
            IggyError::InvalidCommand,
            IggyError::Error,
            IggyError::Unauthenticated,
            IggyError::EmptyResponse,
            IggyError::StaleClient,
            IggyError::TransientNotCommitted,
            IggyError::InvalidCredentials,
            IggyError::CannotAppendMessage,
        ] {
            let text = after_the_write.to_string();
            assert!(
                matches!(map_iggy(after_the_write), BrokerError::Unavailable(why) if why == text),
                "{text}"
            );
        }
    }

    #[test]
    fn what_iggy_says_to_a_credential_is_sorted_into_refusals() {
        // Signed in, and not allowed.
        assert!(matches!(
            map_credential(IggyError::Unauthorized),
            BrokerError::Forbidden(_)
        ));
        // Not signed in, or a token Iggy does not accept.
        for error in [
            IggyError::Unauthenticated,
            IggyError::InvalidCredentials,
            IggyError::InvalidPersonalAccessToken,
            IggyError::PersonalAccessTokenExpired("mansio".to_owned(), 7),
        ] {
            assert!(
                matches!(map_credential(error), BrokerError::Unauthenticated(_)),
                "refused as unauthenticated"
            );
        }
        // The rest is as for the admin.
        assert!(matches!(
            map_credential(IggyError::Disconnected),
            BrokerError::Unavailable(_)
        ));
        assert!(matches!(
            map_credential(IggyError::TopicNameNotFound("t".into(), "s".into())),
            BrokerError::NotFound(_)
        ));
        // For the admin, the same Unauthorized is the server's own fault.
        assert!(matches!(
            map_iggy(IggyError::Unauthorized),
            BrokerError::Rejected(_)
        ));
    }

    #[test]
    fn only_a_refusal_that_says_the_token_is_bad_is_an_invalid_token() {
        for refused in [
            IggyError::InvalidPersonalAccessToken,
            IggyError::PersonalAccessTokenExpired("mansio".to_owned(), 7),
            IggyError::Unauthenticated,
            IggyError::InvalidCredentials,
            IggyError::UserInactive,
            IggyError::ResourceNotFound("user".to_owned()),
            IggyError::InvalidFormat,
        ] {
            let text = refused.to_string();
            assert!(
                matches!(login_failure(refused), BrokerError::Unauthenticated(why) if why == text),
                "{text}"
            );
        }
    }

    #[test]
    fn any_other_sign_in_failure_leaves_the_token_standing() {
        for other in [
            IggyError::Disconnected,
            IggyError::CannotEstablishConnection,
            IggyError::TcpError,
            IggyError::TaskTimeout,
            IggyError::Error,
            IggyError::InvalidCommand,
            IggyError::RequestAlreadyApplied,
            IggyError::ClientNotFound(3),
            IggyError::Unauthorized,
        ] {
            let text = other.to_string();
            assert!(
                matches!(login_failure(other), BrokerError::Unavailable(why) if why == text),
                "{text}"
            );
        }
    }

    #[test]
    fn the_topic_of_the_servers_own_records_is_kept_forever_and_written_to_disk() {
        let options = system_topic_options();
        assert_eq!(options.partitions_count, Some(1));
        assert_eq!(options.message_expiry, Some(IggyExpiry::NeverExpire));
        assert_eq!(options.max_topic_size, Some(MaxTopicSize::Unlimited));
        assert_eq!(options.durability, IggyDurability::Persisted);
        assert_eq!(
            options.consumer_offset_durability,
            IggyDurability::Persisted
        );
    }

    #[test]
    fn the_stream_of_the_servers_own_records_cannot_be_a_tenant() {
        assert!(SYSTEM_STREAM.parse::<TenantId>().is_err());
        assert!(SYSTEM_TOPIC.parse::<Topic>().is_err());
        assert!(user_id(BrokerUser(7)).is_ok());
    }

    #[test]
    fn expiry_follows_the_retention_of_the_kind() {
        let retention = Retention {
            presence: Some(Duration::from_secs(60)),
            ..Retention::default()
        };
        let presence = expiry(&retention, &Topic::Presence);
        assert_eq!(
            presence,
            IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_secs(60)))
        );
        assert_eq!(
            expiry(&retention, &"work.builds".parse().unwrap()),
            IggyExpiry::NeverExpire
        );
        assert_eq!(retention.for_kind(TopicKind::Dead), None);
    }

    #[test]
    fn names_become_named_identifiers_even_when_they_look_numeric() {
        let id = stream_id(&"123".parse().unwrap()).unwrap();
        assert_eq!(id.get_string_value().unwrap(), "123");
        let id = topic_id(&"facts.build".parse().unwrap()).unwrap();
        assert_eq!(id.get_string_value().unwrap(), "facts.build");
    }

    #[test]
    fn a_message_is_found_by_its_id_not_by_reading_its_payload() {
        let wanted = EventId::generate();
        let other = EventId::generate();
        let messages = [
            message(40, other.as_u128(), b"not an envelope".to_vec()),
            message(41, wanted.as_u128(), b"not an envelope either".to_vec()),
            message(42, other.as_u128(), b"x".to_vec()),
        ];
        assert_eq!(find_offset(&messages, wanted), Some(41));
        assert_eq!(find_offset(&messages, EventId::generate()), None);
        assert_eq!(find_offset(&[], wanted), None);
    }

    #[test]
    fn the_latest_of_two_messages_with_one_id_is_the_one_found() {
        let id = EventId::generate();
        let messages = [
            message(3, id.as_u128(), Vec::new()),
            message(9, id.as_u128(), Vec::new()),
        ];
        assert_eq!(find_offset(&messages, id), Some(9));
    }

    #[test]
    fn durable_topics_are_persisted_and_presence_is_replicated_by_default() {
        let retention = Retention::default();
        let policy = DurabilityPolicy::default();
        for name in ["facts.build", "work.builds", "alerts", "dead"] {
            let options = topic_options(&retention, &policy, 0, &name.parse().unwrap());
            assert_eq!(options.durability, IggyDurability::Persisted, "{name}");
            assert_eq!(
                options.consumer_offset_durability,
                IggyDurability::Persisted,
                "{name}"
            );
        }
        let options = topic_options(&retention, &policy, 0, &Topic::Presence);
        assert_eq!(options.durability, IggyDurability::Replicated);
        assert_eq!(
            options.consumer_offset_durability,
            IggyDurability::Replicated
        );
    }

    #[test]
    fn the_durability_setting_reaches_the_topic() {
        let policy = DurabilityPolicy {
            durable: Durability::Replicated,
            presence: Durability::Persisted,
        };
        let facts = topic_options(
            &Retention::default(),
            &policy,
            0,
            &"facts.x".parse().unwrap(),
        );
        assert_eq!(facts.durability, IggyDurability::Replicated);
        let presence = topic_options(&Retention::default(), &policy, 0, &Topic::Presence);
        assert_eq!(presence.durability, IggyDurability::Persisted);
    }

    #[test]
    fn a_topic_has_one_partition_and_no_size_cap_unless_it_expires() {
        let retention = Retention {
            presence: Some(Duration::from_secs(60)),
            ..Retention::default()
        };
        let policy = DurabilityPolicy::default();

        let kept = topic_options(&retention, &policy, 0, &"work.builds".parse().unwrap());
        assert_eq!(kept.partitions_count, Some(1));
        assert_eq!(kept.message_expiry, Some(IggyExpiry::NeverExpire));
        assert_eq!(kept.max_topic_size, Some(MaxTopicSize::Unlimited));

        let expiring = topic_options(&retention, &policy, 0, &Topic::Presence);
        assert_eq!(expiring.partitions_count, Some(1));
        assert_eq!(
            expiring.message_expiry,
            Some(IggyExpiry::ExpireDuration(IggyDuration::from(
                Duration::from_secs(60)
            )))
        );
        assert_eq!(expiring.max_topic_size, None);
    }

    #[test]
    fn the_segment_size_reaches_the_topic_and_zero_leaves_it_to_iggy() {
        let (retention, policy) = (Retention::default(), DurabilityPolicy::default());
        let topic: Topic = "alerts".parse().unwrap();
        assert_eq!(
            topic_options(&retention, &policy, 0, &topic).segment_size,
            None
        );
        assert_eq!(
            topic_options(&retention, &policy, 1024 * 1024, &topic).segment_size,
            Some(IggyByteSize::new(1024 * 1024))
        );
    }

    #[test]
    fn what_was_ensured_is_remembered_until_it_is_forgotten() {
        let ensured = Ensured::default();
        let acme: TenantId = "acme".parse().unwrap();
        let other: TenantId = "other".parse().unwrap();
        let alerts = Topic::Alerts;
        assert!(!ensured.has_stream(&acme) && !ensured.has_topic(&acme, &alerts));

        ensured.add_stream(&acme);
        assert!(ensured.has_stream(&acme));
        assert!(!ensured.has_topic(&acme, &alerts));

        ensured.add_topic(&acme, &alerts);
        ensured.add_topic(&other, &alerts);
        assert!(ensured.has_topic(&acme, &alerts));
        assert!(!ensured.has_topic(&acme, &Topic::Dead));
        assert!(ensured.has_stream(&other), "a topic implies its stream");

        ensured.forget(&acme);
        assert!(!ensured.has_stream(&acme) && !ensured.has_topic(&acme, &alerts));
        assert!(ensured.has_topic(&other, &alerts), "other tenants are kept");
    }

    #[test]
    fn a_topic_made_stays_unopened_until_its_users_have_been_told() {
        let ensured = Ensured::default();
        let acme: TenantId = "acme".parse().unwrap();
        let alerts = Topic::Alerts;
        assert!(
            !ensured.is_unopened(&acme, &alerts),
            "a topic found is open"
        );

        ensured.mark_unopened(&acme, &alerts);
        assert!(ensured.is_unopened(&acme, &alerts));
        assert!(!ensured.is_unopened(&acme, &Topic::Dead));
        // Telling them failed, the topic was not remembered as ensured, and
        // the next request still has it to do.
        assert!(!ensured.has_topic(&acme, &alerts));
        assert!(ensured.is_unopened(&acme, &alerts));

        ensured.mark_opened(&acme, &alerts);
        assert!(!ensured.is_unopened(&acme, &alerts));
    }

    #[test]
    fn a_tenant_is_forgotten_when_its_stream_is_reported_missing() {
        let ensured = Ensured::default();
        let acme: TenantId = "acme".parse().unwrap();
        let other: TenantId = "other".parse().unwrap();
        let alerts = Topic::Alerts;
        ensured.add_topic(&acme, &alerts);
        ensured.add_topic(&other, &alerts);

        let passed = ensured.forget_if_missing::<()>(&acme, Ok(()));
        assert!(passed.is_ok());
        assert!(
            ensured.has_topic(&acme, &alerts),
            "a success keeps the cache"
        );

        let outage = ensured
            .forget_if_missing::<()>(&acme, Err(BrokerError::Unavailable("no answer".into())));
        assert!(matches!(outage, Err(BrokerError::Unavailable(_))));
        assert!(
            ensured.has_topic(&acme, &alerts),
            "an outage keeps the cache"
        );

        let missing = ensured
            .forget_if_missing::<()>(&acme, Err(BrokerError::NotFound("stream acme".into())));
        assert!(matches!(missing, Err(BrokerError::NotFound(_))));
        assert!(!ensured.has_stream(&acme) && !ensured.has_topic(&acme, &alerts));
        assert!(ensured.has_topic(&other, &alerts), "other tenants are kept");
    }
}
