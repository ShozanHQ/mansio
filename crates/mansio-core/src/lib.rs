//! The core of mansio: the envelope, tenants, principals and topics as
//! types, and the `Broker` and `Sink` traits. No network I/O lives here.
//!
//! The contract is described in `ARCHITECTURE.md` at the repository root.

mod access;
pub mod api;
mod broker;
mod credential;
mod envelope;
mod error;
mod id;
mod memory;
mod principal;
mod sink;
mod topic;
pub mod webhook;
mod work;

pub use access::{Action, Grant, TopicPattern, Verb, allows};
pub use broker::{
    Broker, BrokerUser, BrokerUserInfo, Caller, Minted, Stored, SystemLog, SystemRecord,
};
pub use credential::{Credential, CredentialDigest};
pub use envelope::{
    ApprovalState, Budget, DataContentType, Envelope, Hash, Record, Scope, Timestamp,
};
pub use error::{BrokerError, EnvelopeError, ParseError, SinkError};
pub use id::{
    CausationId, ConsumerName, CorrelationId, EventId, EventType, IdempotencyKey, ItemId,
    LeaseFingerprint, LeaseId, Principal, PrincipalId, PrincipalKind, PrincipalName, RecordId,
    Segment, Source, Subject, TenantId, WebhookId,
};
pub use memory::MemoryBroker;
pub use principal::{PrincipalEvent, PrincipalFact, PrincipalInfo, RevokeReason};
pub use sink::{NoSink, Sink};
pub use topic::{Topic, TopicKind};
pub use webhook::{
    Webhook, WebhookError, WebhookEvent, WebhookFact, WebhookInfo, WebhookSecret, WebhookStatus,
};
pub use work::{AlertEvent, AlertFact, Next, WorkEvent, WorkFact};
