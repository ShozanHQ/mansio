//! `IggyBroker`: the `Broker` trait over the Iggy Rust SDK, TCP transport.

use async_trait::async_trait;
use bytes::Bytes;
use iggy::prelude::{
    AutoLogin, Client, Consumer, Credentials, Identifier, IggyClient, IggyClientBuilder,
    IggyDuration, IggyError, IggyExpiry, IggyMessage, MessageClient, Partitioning, PollingStrategy,
    StreamClient, TopicClient, TopicCreateOptions,
};
use mansio_core::{Broker, BrokerError, Envelope, EventId, Stored, TenantId, Topic};
use tracing::{debug, info};

use crate::config::{IggyConfig, Retention};

/// The one partition of every topic.
const PARTITION: u32 = 0;

/// A [`Broker`] backed by an Iggy server.
#[derive(Debug)]
pub struct IggyBroker {
    client: IggyClient,
    retention: Retention,
    locate_window: u32,
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
            locate_window: config.locate_window,
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
    /// Iggy reports no offset for a publish it treats as a duplicate, and
    /// the original is then near the end.
    async fn locate(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        id: EventId,
    ) -> Result<u64, BrokerError> {
        if self.locate_window > 0 {
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
                .await
                .map_err(map_iggy)?;
            let found = polled
                .messages
                .iter()
                .rev()
                .find(|message| {
                    Envelope::from_json_slice(&message.payload).is_ok_and(|e| e.id == id)
                })
                .map(|message| message.header.offset);
            if let Some(offset) = found {
                return Ok(offset);
            }
        }
        Err(BrokerError::Rejected(format!(
            "Iggy accepted message {id} without reporting its offset, and it is not among the last {} messages",
            self.locate_window
        )))
    }
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
    async fn ensure_tenant(&self, tenant: &TenantId) -> Result<(), BrokerError> {
        let stream = stream_id(tenant)?;
        match self.client.get_stream(&stream).await {
            Ok(Some(_)) => return Ok(()),
            Ok(None) | Err(IggyError::StreamIdNotFound(_) | IggyError::StreamNameNotFound(_)) => {}
            Err(error) => return Err(map_iggy(error)),
        }
        match self.client.create_stream(tenant.as_str()).await {
            Ok(_) => {
                debug!(%tenant, "created stream");
                Ok(())
            }
            // Another request created it first.
            Err(IggyError::StreamNameAlreadyExists(_)) => Ok(()),
            Err(error) => Err(map_iggy(error)),
        }
    }

    async fn ensure_topic(&self, tenant: &TenantId, topic: &Topic) -> Result<(), BrokerError> {
        let stream = stream_id(tenant)?;
        let id = topic_id(topic)?;
        match self.client.get_topic(&stream, &id).await {
            Ok(Some(_)) => return Ok(()),
            Ok(None) | Err(IggyError::TopicIdNotFound(..) | IggyError::TopicNameNotFound(..)) => {}
            Err(error) => return Err(map_iggy(error)),
        }
        let options = TopicCreateOptions {
            partitions_count: Some(1),
            message_expiry: Some(expiry(&self.retention, topic)),
            ..TopicCreateOptions::default()
        };
        match self
            .client
            .create_topic(&stream, &topic.to_string(), &options)
            .await
        {
            Ok(_) => {
                debug!(%tenant, %topic, "created topic");
                Ok(())
            }
            // Another request created it first.
            Err(IggyError::TopicNameAlreadyExists(..)) => Ok(()),
            Err(error) => Err(map_iggy(error)),
        }
    }

    async fn publish(&self, envelope: &Envelope) -> Result<u64, BrokerError> {
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
                &Partitioning::balanced(),
                std::slice::from_mut(&mut message),
            )
            .await
            .map_err(map_iggy)?;
        match sent.confirmations.first() {
            Some(confirmation) => Ok(confirmation.base_offset),
            None => self.locate(&stream, &topic, envelope.id).await,
        }
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
}
