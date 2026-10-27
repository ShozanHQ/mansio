//! The mansio [`Broker`](mansio_core::Broker) over Apache Iggy.
//!
//! A tenant is an Iggy stream named by the tenant id. Each topic is an Iggy
//! topic with one partition. The Iggy message id is the envelope id and the
//! payload is the envelope as CloudEvents JSON.
//!
//! A principal is an Iggy user whose permissions name the topics its scopes
//! cover, and its credential is a personal access token of that user. The
//! broker acts for a credential on a client signed in with it, so Iggy refuses
//! what the scopes do not allow.

mod access;
mod broker;
mod config;
mod provision;
mod sessions;

pub use broker::{
    IggyBroker, SYSTEM_CURSORS_TOPIC, SYSTEM_STREAM, SYSTEM_TOPIC, SYSTEM_WEBHOOKS_TOPIC,
};
pub use config::{
    DEFAULT_ADDRESS, DEFAULT_CONNECT_TIMEOUT, DEFAULT_LOCATE_WINDOW, DEFAULT_PRESENCE_RETENTION,
    DEFAULT_USERNAME, Durability, DurabilityPolicy, IggyConfig, MAX_SEGMENT_SIZE_BYTES,
    MIN_SEGMENT_SIZE_BYTES, Retention, check_segment_size,
};
pub use provision::{ProvisionConfig, Provisioned, provision_admin};
