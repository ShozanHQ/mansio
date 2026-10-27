//! The JSON bodies of the HTTP API, shared by the server and its clients.

use serde::{Deserialize, Serialize};

use crate::broker::Stored;
use crate::envelope::Envelope;

/// The answer to a publish: the envelope as stored, and where it landed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Published {
    /// The offset in the topic.
    pub offset: u64,
    /// The envelope with the server-set attributes filled in.
    pub envelope: Envelope,
}

/// The answer to a poll.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Polled {
    /// Envelopes from the requested offset on, in publish order.
    pub messages: Vec<Stored>,
}

/// The body of every refusal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// What went wrong.
    pub error: ErrorDetail,
}

/// A machine-readable code and a message for a person.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorDetail {
    /// A stable code such as `bad_request` or `unauthorized`.
    pub code: String,
    /// What to fix.
    pub message: String,
}
