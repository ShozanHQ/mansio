//! The core of mansio: the envelope, tenants, principals and topics as
//! types, and the `Broker` and `Sink` traits. No network I/O lives here.
//!
//! The contract is described in `ARCHITECTURE.md` at the repository root.

pub mod api;
mod broker;
mod envelope;
mod error;
mod id;
mod memory;
mod sink;
mod topic;
mod work;

pub use broker::{Broker, Stored};
pub use envelope::{
    ApprovalState, Budget, DataContentType, Envelope, Hash, Record, Scope, Timestamp,
};
pub use error::{BrokerError, EnvelopeError, ParseError, SinkError};
pub use id::{
    CausationId, ConsumerName, CorrelationId, EventId, EventType, IdempotencyKey, ItemId, LeaseId,
    Principal, PrincipalKind, PrincipalName, RecordId, Segment, Source, Subject, TenantId,
};
pub use memory::MemoryBroker;
pub use sink::{NoSink, Sink};
pub use topic::{Topic, TopicKind};
pub use work::{AlertEvent, AlertFact, Next, WorkEvent, WorkFact};
