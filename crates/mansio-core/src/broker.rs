//! The `Broker` trait: what mansio needs from a message broker.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::envelope::Envelope;
use crate::error::BrokerError;
use crate::id::TenantId;
use crate::topic::Topic;

/// An envelope as the broker holds it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stored {
    /// Its position in the topic, from 0. A topic has one partition, so this
    /// is also the publish order.
    pub offset: u64,
    /// The envelope.
    pub envelope: Envelope,
}

/// A message broker with tenants, topics and offsets.
///
/// A tenant is an isolated stream; a topic lives in a tenant and has a single
/// ordered partition. Implementations are the Iggy backend and
/// [`MemoryBroker`](crate::MemoryBroker).
#[async_trait]
pub trait Broker: Send + Sync {
    /// Make sure the tenant exists. Does nothing if it already does.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker cannot be reached or refuses.
    async fn ensure_tenant(&self, tenant: &TenantId) -> Result<(), BrokerError>;

    /// Make sure the topic exists in the tenant. Does nothing if it already
    /// does.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::NotFound`] if the tenant does not exist, or an
    /// error if the broker cannot be reached or refuses.
    async fn ensure_topic(&self, tenant: &TenantId, topic: &Topic) -> Result<(), BrokerError>;

    /// Append the envelope to the topic named in it, and return its offset.
    ///
    /// The broker does not deduplicate: publishing the same id twice stores
    /// it twice.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::NotFound`] if the tenant or topic does not
    /// exist, or an error if the broker cannot be reached or refuses.
    async fn publish(&self, envelope: &Envelope) -> Result<u64, BrokerError>;

    /// Read up to `max` envelopes from `offset` on, in publish order. An
    /// offset past the end returns nothing.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::NotFound`] if the tenant or topic does not
    /// exist, [`BrokerError::Corrupt`] if a stored message is not a valid
    /// envelope, or an error if the broker cannot be reached or refuses.
    async fn poll(
        &self,
        tenant: &TenantId,
        topic: &Topic,
        offset: u64,
        max: u32,
    ) -> Result<Vec<Stored>, BrokerError>;
}
