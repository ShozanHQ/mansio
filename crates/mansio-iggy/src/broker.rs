//! `IggyBroker`: the `Broker` trait over the Iggy Rust SDK, TCP transport.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use iggy::prelude::{
    AutoLogin, Client, Consumer, Credentials, Durability as IggyDurability, Identifier, IggyClient,
    IggyClientBuilder, IggyDuration, IggyError, IggyExpiry, IggyMessage, MaxTopicSize,
    MessageClient, Partitioning, PollingStrategy, StreamClient, TopicClient, TopicCreateOptions,
};
use mansio_core::{Broker, BrokerError, Envelope, EventId, Stored, TenantId, Topic};
use tracing::{info, warn};

use crate::config::{Durability, DurabilityPolicy, IggyConfig, Retention};

/// The one partition of every topic.
const PARTITION: u32 = 0;

/// The streams and topics this process has found or created, so that a
/// request after the first skips the round trips that check for them.
#[derive(Debug, Default)]
struct Ensured {
    /// A tenant is a known stream; its topics are the known topics in it.
    known: Mutex<HashMap<TenantId, HashSet<Topic>>>,
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
}

/// A [`Broker`] backed by an Iggy server.
#[derive(Debug)]
pub struct IggyBroker {
    client: IggyClient,
    retention: Retention,
    durability: DurabilityPolicy,
    locate_window: u32,
    ensured: Ensured,
}

impl IggyBroker {
    /// Connect to Iggy and sign in as the configured user. The connection
    /// reconnects and signs in again by itself if it drops.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Unavailable`] if Iggy does not answer within
    /// the connect timeout, and [`BrokerError::Rejected`] if it refuses the
    /// credentials.
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
        info!(address = %config.address, "connected to Iggy");
        Ok(Self {
            client,
            retention: config.retention,
            durability: config.durability,
            locate_window: config.locate_window,
            ensured: Ensured::default(),
        })
    }

    /// Close the connection.
    ///
    /// # Errors
    ///
    /// Returns an error if Iggy cannot be told.
    pub async fn shutdown(&self) -> Result<(), BrokerError> {
        self.client.shutdown().await.map_err(map_iggy)
    }

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
    async fn send(&self, envelope: &Envelope) -> Result<Option<u64>, BrokerError> {
        let stream = stream_id(&envelope.tenant)?;
        let topic = topic_id(&envelope.topic)?;
        let mut message = IggyMessage::builder()
            .id(envelope.id.as_u128())
            .payload(Bytes::from(envelope.to_json_vec()?))
            .build()
            .map_err(map_iggy)?;
        let sent = self
            .client
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(PARTITION),
                std::slice::from_mut(&mut message),
            )
            .await
            .map_err(map_iggy)?;
        if let Some(confirmation) = sent.confirmations.first() {
            return Ok(Some(confirmation.base_offset));
        }
        // Iggy accepted the message but did not say where. Failing now would
        // have the client send it again and store it twice, so a message that
        // cannot be found is answered without an offset.
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

    async fn fetch(
        &self,
        tenant: &TenantId,
        topic: &Topic,
        offset: u64,
        max: u32,
    ) -> Result<Vec<Stored>, BrokerError> {
        let polled = self
            .client
            .poll_messages(
                &stream_id(tenant)?,
                &topic_id(topic)?,
                Some(PARTITION),
                &Consumer::default(),
                &PollingStrategy::offset(offset),
                max,
                false,
            )
            .await
            .map_err(map_iggy)?;
        polled
            .messages
            .iter()
            .map(|message| decode(tenant, topic, message))
            .collect()
    }

    /// If Iggy says the stream or topic is gone, check for them again on the
    /// next request.
    fn forget_if_missing<T>(
        &self,
        tenant: &TenantId,
        result: Result<T, BrokerError>,
    ) -> Result<T, BrokerError> {
        if matches!(result, Err(BrokerError::NotFound(_))) {
            self.ensured.forget(tenant);
        }
        result
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
/// kind, and no size cap when it keeps messages forever, so that a default
/// cap on the server never trims it.
fn topic_options(
    retention: &Retention,
    durability: &DurabilityPolicy,
    topic: &Topic,
) -> TopicCreateOptions {
    let durability = iggy_durability(durability.for_kind(topic.kind()));
    TopicCreateOptions {
        partitions_count: Some(1),
        message_expiry: Some(expiry(retention, topic)),
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

fn map_iggy(error: IggyError) -> BrokerError {
    let text = error.to_string();
    match error {
        IggyError::StreamIdNotFound(_)
        | IggyError::StreamNameNotFound(_)
        | IggyError::TopicIdNotFound(..)
        | IggyError::TopicNameNotFound(..)
        | IggyError::ResourceNotFound(_) => BrokerError::NotFound(text),
        IggyError::Disconnected
        | IggyError::CannotEstablishConnection
        | IggyError::ClientShutdown
        | IggyError::StaleClient
        | IggyError::TcpError
        | IggyError::TransientNotCommitted
        | IggyError::TransientNotAccepted
        | IggyError::TaskTimeout => BrokerError::Unavailable(text),
        _ => BrokerError::Rejected(text),
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
        if self.ensured.has_stream(tenant) {
            return Ok(());
        }
        let stream = stream_id(tenant)?;
        match self.client.get_stream(&stream).await {
            Ok(Some(_)) => {
                self.ensured.add_stream(tenant);
                return Ok(());
            }
            Ok(None) | Err(IggyError::StreamIdNotFound(_) | IggyError::StreamNameNotFound(_)) => {}
            Err(error) => return Err(map_iggy(error)),
        }
        match self.client.create_stream(tenant.as_str()).await {
            Ok(_) => {
                info!(%tenant, "created stream");
                self.ensured.add_stream(tenant);
                Ok(())
            }
            // Another request created it first.
            Err(IggyError::StreamNameAlreadyExists(_)) => {
                self.ensured.add_stream(tenant);
                Ok(())
            }
            Err(error) => Err(map_iggy(error)),
        }
    }

    async fn ensure_topic(&self, tenant: &TenantId, topic: &Topic) -> Result<(), BrokerError> {
        if self.ensured.has_topic(tenant, topic) {
            return Ok(());
        }
        let stream = stream_id(tenant)?;
        let id = topic_id(topic)?;
        match self.client.get_topic(&stream, &id).await {
            Ok(Some(_)) => {
                self.ensured.add_topic(tenant, topic);
                return Ok(());
            }
            Ok(None) | Err(IggyError::TopicIdNotFound(..) | IggyError::TopicNameNotFound(..)) => {}
            Err(error) => return Err(map_iggy(error)),
        }
        let options = topic_options(&self.retention, &self.durability, topic);
        match self
            .client
            .create_topic(&stream, &topic.to_string(), &options)
            .await
        {
            Ok(_) => {
                info!(%tenant, %topic, "created topic");
                self.ensured.add_topic(tenant, topic);
                Ok(())
            }
            // Another request created it first.
            Err(IggyError::TopicNameAlreadyExists(..)) => {
                self.ensured.add_topic(tenant, topic);
                Ok(())
            }
            Err(error) => Err(map_iggy(error)),
        }
    }

    async fn publish(&self, envelope: &Envelope) -> Result<Option<u64>, BrokerError> {
        let result = self.send(envelope).await;
        self.forget_if_missing(&envelope.tenant, result)
    }

    async fn poll(
        &self,
        tenant: &TenantId,
        topic: &Topic,
        offset: u64,
        max: u32,
    ) -> Result<Vec<Stored>, BrokerError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let result = self.fetch(tenant, topic, offset, max).await;
        self.forget_if_missing(tenant, result)
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
        assert!(matches!(
            map_iggy(IggyError::Unauthorized),
            BrokerError::Rejected(_)
        ));
        assert!(matches!(
            map_iggy(IggyError::InvalidCredentials),
            BrokerError::Rejected(_)
        ));
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
            let options = topic_options(&retention, &policy, &name.parse().unwrap());
            assert_eq!(options.durability, IggyDurability::Persisted, "{name}");
            assert_eq!(
                options.consumer_offset_durability,
                IggyDurability::Persisted,
                "{name}"
            );
        }
        let options = topic_options(&retention, &policy, &Topic::Presence);
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
        let facts = topic_options(&Retention::default(), &policy, &"facts.x".parse().unwrap());
        assert_eq!(facts.durability, IggyDurability::Replicated);
        let presence = topic_options(&Retention::default(), &policy, &Topic::Presence);
        assert_eq!(presence.durability, IggyDurability::Persisted);
    }

    #[test]
    fn a_topic_has_one_partition_and_no_size_cap_unless_it_expires() {
        let retention = Retention {
            presence: Some(Duration::from_secs(60)),
            ..Retention::default()
        };
        let policy = DurabilityPolicy::default();

        let kept = topic_options(&retention, &policy, &"work.builds".parse().unwrap());
        assert_eq!(kept.partitions_count, Some(1));
        assert_eq!(kept.message_expiry, Some(IggyExpiry::NeverExpire));
        assert_eq!(kept.max_topic_size, Some(MaxTopicSize::Unlimited));

        let expiring = topic_options(&retention, &policy, &Topic::Presence);
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
}
