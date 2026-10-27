//! Typed errors for the core crate.

use thiserror::Error;

/// A value that does not meet the rules for its type.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParseError {
    /// The value was empty.
    #[error("{what} must not be empty")]
    Empty {
        /// Which kind of value.
        what: &'static str,
    },
    /// The value was longer than the type allows.
    #[error("{what} is longer than {max} bytes")]
    TooLong {
        /// Which kind of value.
        what: &'static str,
        /// The limit in bytes.
        max: usize,
    },
    /// The value broke a format rule.
    #[error("{what} {value:?} is not valid: {rule}")]
    Invalid {
        /// Which kind of value.
        what: &'static str,
        /// The offending input.
        value: String,
        /// The rule it broke.
        rule: &'static str,
    },
}

/// An envelope that could not be read, or that contradicts itself.
#[derive(Debug, Error)]
pub enum EnvelopeError {
    /// The JSON is not a valid envelope: bad syntax, a missing or unknown
    /// attribute, or an attribute that fails its rule.
    #[error("not a valid envelope: {0}")]
    Malformed(#[from] serde_json::Error),
    /// The attributes are individually valid but do not fit together.
    #[error("invalid envelope: {0}")]
    Inconsistent(&'static str),
}

/// A broker could not do what it was asked.
#[derive(Debug, Error)]
pub enum BrokerError {
    /// The tenant or topic does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// The broker could not be reached, did not answer in time, or answered in
    /// a way that does not say whether it applied the request. A write that
    /// fails like this may still have been stored.
    #[error("broker unavailable: {0}")]
    Unavailable(String),
    /// The broker named a refusal that it makes before it stores or applies
    /// anything, so a write that fails like this was not stored. An answer that
    /// is not one of those is [`BrokerError::Unavailable`].
    #[error("broker refused the request: {0}")]
    Rejected(String),
    /// A stored message is not a valid envelope for where it was found.
    #[error("stored message is corrupt: {0}")]
    Corrupt(String),
    /// The envelope to publish is invalid.
    #[error(transparent)]
    Envelope(#[from] EnvelopeError),
}

/// A sink could not record an envelope.
#[derive(Debug, Error)]
pub enum SinkError {
    /// The sink failed; nothing was recorded.
    #[error("sink failed: {0}")]
    Failed(String),
}
