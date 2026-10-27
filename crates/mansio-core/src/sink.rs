//! The `Sink` trait: where a host records what mansio is about to publish.

use async_trait::async_trait;

use crate::envelope::{Envelope, Record};
use crate::error::SinkError;

/// Called, in order, before every durable publish. A host with a
/// hash-chained log appends the envelope there and returns the [`Record`] it
/// made, which is set on the envelope before it is published so that
/// subscribers can check it against the host's log.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Record the envelope.
    ///
    /// # Errors
    ///
    /// Returns an error if the envelope could not be recorded. It is then
    /// not published.
    async fn record(&self, envelope: &Envelope) -> Result<Option<Record>, SinkError>;
}

/// A sink that records nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSink;

#[async_trait]
impl Sink for NoSink {
    async fn record(&self, _envelope: &Envelope) -> Result<Option<Record>, SinkError> {
        Ok(None)
    }
}
