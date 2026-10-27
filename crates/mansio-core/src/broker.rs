//! The `Broker` trait: what mansio needs from a message broker.

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::access::Grant;
use crate::credential::Credential;
use crate::envelope::Envelope;
use crate::error::BrokerError;
use crate::id::{ConsumerName, TenantId};
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

/// Who a read or a write is for.
///
/// The broker enforces permissions on a credential: it may read what its
/// scopes cover and nothing else, and write nothing, however the request is
/// made. Creating streams, topics and users is the admin identity's alone and
/// has no caller argument.
#[derive(Debug, Clone, Copy)]
pub enum Caller<'a> {
    /// The server's own identity, which may read and write every topic.
    Admin,
    /// A principal's credential. The broker acts as the user it belongs to.
    Credential(&'a Credential),
}

/// The user a broker made for a principal: the broker's own number for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BrokerUser(pub u32);

impl fmt::Display for BrokerUser {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A user made for a principal, and the credential that signs in as it.
#[derive(Debug, Clone)]
pub struct Minted {
    /// The broker's user.
    pub user: BrokerUser,
    /// The credential. The broker does not keep it in a form that can be
    /// shown again.
    pub credential: Credential,
}

/// One of the logs the server keeps in the broker where no credential can
/// reach it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemLog {
    /// The registry of principals and their credentials' digests.
    Principals,
    /// The registry of webhooks, which holds their signing secrets.
    Webhooks,
}

impl SystemLog {
    /// Every log.
    pub const ALL: [SystemLog; 2] = [SystemLog::Principals, SystemLog::Webhooks];

    /// The name of the log: the topic it lives in.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            SystemLog::Principals => "principals",
            SystemLog::Webhooks => "webhooks",
        }
    }
}

/// A user of the broker, by number and name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerUserInfo {
    /// The broker's number for it.
    pub user: BrokerUser,
    /// Its name.
    pub name: String,
}

/// A record the server keeps in the broker where no credential can reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemRecord {
    /// Its position, from 0.
    pub offset: u64,
    /// What was appended.
    pub payload: Vec<u8>,
}

fn unsupported(what: &str) -> BrokerError {
    BrokerError::Rejected(format!("this broker does not support {what}"))
}

/// A message broker with tenants, topics and offsets.
///
/// A tenant is an isolated stream; a topic lives in a tenant and has a single
/// ordered partition. Implementations are the Iggy backend and
/// [`MemoryBroker`](crate::MemoryBroker).
///
/// Listing, creating and the system records are the admin identity's. Reading
/// and writing messages are done as a [`Caller`], and a credential is held to
/// its scopes by the broker. Identities have their own methods, with
/// defaults that refuse, for a broker that has none.
#[async_trait]
pub trait Broker: Send + Sync {
    /// Every tenant the broker holds. A server uses this on start to find
    /// the work and alerts it has to take up again. Names that are not valid
    /// tenant ids are left out.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker cannot be reached or refuses.
    async fn tenants(&self) -> Result<Vec<TenantId>, BrokerError>;

    /// Every topic the tenant holds, in no particular order. Names that are
    /// not valid topics are left out.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::NotFound`] if the tenant does not exist, or an
    /// error if the broker cannot be reached or refuses.
    async fn topics(&self, tenant: &TenantId) -> Result<Vec<Topic>, BrokerError>;

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

    /// Append the envelope to the topic named in it, as `caller`, and return
    /// its offset. A credential writes nothing: it is held to reading, and the
    /// server writes for it as the admin, with the principal stamped from the
    /// credential.
    ///
    /// `None` means the broker accepted and stored the message but could not
    /// say where it landed. The caller must not publish it again.
    ///
    /// The broker does not deduplicate: publishing the same id twice stores
    /// it twice.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::NotFound`] if the tenant or topic does not
    /// exist, [`BrokerError::Forbidden`] for a credential, which may not send
    /// anywhere, [`BrokerError::Unauthenticated`] if it is not valid,
    /// [`BrokerError::Rejected`] if the broker named a refusal that comes before
    /// it stores anything (the message was not stored), or
    /// [`BrokerError::Unavailable`] for any other failure, after which the
    /// message may have been stored.
    async fn publish(
        &self,
        caller: &Caller<'_>,
        envelope: &Envelope,
    ) -> Result<Option<u64>, BrokerError>;

    /// Read up to `max` envelopes from `offset` on, in publish order, as
    /// `caller`. An offset past the end returns nothing. An offset below the
    /// first message the broker still holds, because retention deleted the head
    /// of the topic, reads from that first message: a reader that starts at 0
    /// sees what is left. A `max` of 0 reads nothing, and still checks that the
    /// topic exists and that the caller may read it.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::NotFound`] if the tenant or topic does not
    /// exist, [`BrokerError::Forbidden`] if the credential may not read there,
    /// [`BrokerError::Unauthenticated`] if it is not valid,
    /// [`BrokerError::Corrupt`] if a stored message is not a valid envelope,
    /// or an error if the broker cannot be reached or refuses.
    async fn poll(
        &self,
        caller: &Caller<'_>,
        tenant: &TenantId,
        topic: &Topic,
        offset: u64,
        max: u32,
    ) -> Result<Vec<Stored>, BrokerError>;

    /// Append a record to one of the server's own logs, which no credential
    /// can read or write: the registries of principals and of webhooks live
    /// there.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker cannot be reached, refuses, or has no
    /// such log.
    async fn system_append(&self, _log: SystemLog, _record: &[u8]) -> Result<(), BrokerError> {
        Err(unsupported("system records"))
    }

    /// Up to `max` records of one of the server's own logs from `offset` on,
    /// in order.
    ///
    /// # Errors
    ///
    /// As [`Broker::system_append`].
    async fn system_read(
        &self,
        _log: SystemLog,
        _offset: u64,
        _max: u32,
    ) -> Result<Vec<SystemRecord>, BrokerError> {
        Err(unsupported("system records"))
    }

    /// The offset of the last message the named consumer committed on the
    /// topic, or `None` if it has committed none. The broker keeps it, so it
    /// survives a restart of the server. A consumer belongs to a tenant and a
    /// topic, and its name is the whole of its identity.
    ///
    /// This is the admin identity's, like listing: the server first reads the
    /// topic as the caller, so that the broker holds the caller to its right to
    /// read it, and only then asks for the consumer.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::NotFound`] if the tenant or topic does not
    /// exist, or an error if the broker cannot be reached, refuses, or keeps
    /// no consumer offsets.
    async fn consumer_offset(
        &self,
        _tenant: &TenantId,
        _topic: &Topic,
        _consumer: &ConsumerName,
    ) -> Result<Option<u64>, BrokerError> {
        Err(unsupported("consumer offsets"))
    }

    /// Store `offset` as the last message the named consumer has handled on
    /// the topic, replacing what it was. Any offset may be stored, so a
    /// consumer can be set back to read again. Like every write it is the
    /// admin identity's.
    ///
    /// # Errors
    ///
    /// As [`Broker::consumer_offset`]; a failure other than a refusal that
    /// comes first may have stored the offset.
    async fn commit_offset(
        &self,
        _tenant: &TenantId,
        _topic: &Topic,
        _consumer: &ConsumerName,
        _offset: u64,
    ) -> Result<(), BrokerError> {
        Err(unsupported("consumer offsets"))
    }

    /// Make a user called `name` that may reach only `tenant`, and only what
    /// `grants` allow there, and a credential for it that stops working after
    /// `ttl` if one is given. The tenant must exist. The user's permissions
    /// name the topics that exist; the broker adds a topic to them when one
    /// that matches is created later.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::NotFound`] if the tenant does not exist, or an
    /// error if the broker cannot be reached or refuses.
    async fn create_user(
        &self,
        _tenant: &TenantId,
        _name: &str,
        _grants: &[Grant],
        _ttl: Option<Duration>,
    ) -> Result<Minted, BrokerError> {
        Err(unsupported("users"))
    }

    /// Tell the broker what an existing user may do, and apply it. The
    /// server does this for every principal it knows when it starts, because
    /// the broker keeps the scopes only in memory.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::NotFound`] if the user or the tenant does not
    /// exist, or an error if the broker cannot be reached or refuses.
    async fn set_grants(
        &self,
        _user: BrokerUser,
        _tenant: &TenantId,
        _grants: &[Grant],
    ) -> Result<(), BrokerError> {
        Err(unsupported("users"))
    }

    /// Every user the broker has, the server's own and the ones made for
    /// principals alike. A broker without users has none.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker cannot be reached or refuses.
    async fn list_users(&self) -> Result<Vec<BrokerUserInfo>, BrokerError> {
        Ok(Vec::new())
    }

    /// Delete the user and every credential of it, and end its sessions. A
    /// user that is already gone is not an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker cannot be reached or refuses.
    async fn delete_user(&self, _user: BrokerUser) -> Result<(), BrokerError> {
        Err(unsupported("users"))
    }

    /// The user a credential signs in as, checked with the broker now.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Unauthenticated`] if the credential is unknown,
    /// expired or revoked, or an error if the broker cannot be reached.
    async fn identify(&self, _credential: &Credential) -> Result<BrokerUser, BrokerError> {
        Err(BrokerError::Unauthenticated(
            "this broker has no credentials".to_owned(),
        ))
    }
}
