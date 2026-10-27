//! The mansio [`Broker`](mansio_core::Broker) over Apache Iggy.
//!
//! A tenant is an Iggy stream named by the tenant id. Each topic is an Iggy
//! topic with one partition. The Iggy message id is the envelope id and the
//! payload is the envelope as CloudEvents JSON.

mod broker;
mod config;

pub use broker::IggyBroker;
pub use config::{
    DEFAULT_ADDRESS, DEFAULT_CONNECT_TIMEOUT, DEFAULT_LOCATE_WINDOW, DEFAULT_PRESENCE_RETENTION,
    DEFAULT_USERNAME, Durability, DurabilityPolicy, IggyConfig, MAX_SEGMENT_SIZE_BYTES,
    MIN_SEGMENT_SIZE_BYTES, Retention, check_segment_size,
};
