//! The JSON bodies of the HTTP API, shared by the server and its clients.

use serde::{Deserialize, Serialize};

use crate::broker::Stored;
use crate::envelope::Envelope;

/// The answer to a publish: the envelope as stored, and where it landed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Published {
    /// The offset in the topic. Absent when the broker accepted the message
    /// but could not say where it landed: it is stored, so do not publish it
    /// again; poll and match on the envelope's `id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{EventId, Principal};

    fn envelope() -> Envelope {
        Envelope::new(
            EventId::generate(),
            "test".parse().unwrap(),
            "t".parse().unwrap(),
            "acme".parse().unwrap(),
            "alerts".parse().unwrap(),
            "service:test".parse::<Principal>().unwrap(),
        )
    }

    #[test]
    fn an_offset_the_broker_did_not_give_is_left_out() {
        let with = Published {
            offset: Some(3),
            envelope: envelope(),
        };
        let json = serde_json::to_value(&with).unwrap();
        assert_eq!(json["offset"], 3);
        assert_eq!(serde_json::from_value::<Published>(json).unwrap(), with);

        let without = Published {
            offset: None,
            envelope: envelope(),
        };
        let json = serde_json::to_value(&without).unwrap();
        assert!(json.get("offset").is_none(), "{json}");
        assert!(json.get("envelope").is_some());
        assert_eq!(serde_json::from_value::<Published>(json).unwrap(), without);
    }
}
