//! A principal the admin API issued a credential to, and the facts mansio
//! records about it on `facts.principal`.
//!
//! None of it holds a secret: the credential is returned once, when it is
//! issued, and mansio keeps no copy.

use serde::{Deserialize, Serialize};

use crate::access::Grant;
use crate::envelope::Timestamp;
use crate::id::{Principal, PrincipalId, PrincipalKind, PrincipalName, TenantId};

/// A principal as the API describes it and the registry records it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrincipalInfo {
    /// Names the principal in the API and in the broker.
    pub id: PrincipalId,
    /// The kind: `person`, `agent`, `connector`, `service` or another token.
    pub kind: PrincipalKind,
    /// The name, unique within its kind and tenant.
    pub name: PrincipalName,
    /// The tenant: the one stream the credential reaches.
    pub tenant: TenantId,
    /// What the credential may do.
    pub scopes: Vec<Grant>,
    /// When the credential was issued.
    pub created_at: Timestamp,
    /// When the credential stops working. Agents always have one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
}

impl PrincipalInfo {
    /// The `kind:name` that stands for the principal on every envelope it
    /// publishes and in every lease it holds.
    #[must_use]
    pub fn principal(&self) -> Principal {
        Principal::new(self.kind.clone(), self.name.clone())
    }
}

/// A change to a principal, recorded as an envelope on `facts.principal` with
/// the principal id as its `subject`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrincipalEvent {
    /// A credential was issued.
    Created,
    /// A credential was withdrawn.
    Revoked,
}

impl PrincipalEvent {
    /// Both changes.
    pub const ALL: [PrincipalEvent; 2] = [PrincipalEvent::Created, PrincipalEvent::Revoked];

    /// The CloudEvents `type` of the fact: `mansio.principal.created` and
    /// `mansio.principal.revoked`.
    #[must_use]
    pub fn event_type(self) -> &'static str {
        match self {
            PrincipalEvent::Created => "mansio.principal.created",
            PrincipalEvent::Revoked => "mansio.principal.revoked",
        }
    }

    /// The change a CloudEvents `type` names, if it is one.
    #[must_use]
    pub fn from_event_type(event_type: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|event| event.event_type() == event_type)
    }
}

/// Why a credential was withdrawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RevokeReason {
    /// The admin API was asked to.
    Requested,
    /// Its expiry passed and the sweep removed it.
    Expired,
    /// It could not be issued, or the broker no longer knows its user.
    Failed,
}

/// The `data` of a `facts.principal` envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrincipalFact {
    /// The principal the change is about.
    pub principal: PrincipalInfo,
    /// `revoked`: why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<RevokeReason>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info() -> PrincipalInfo {
        PrincipalInfo {
            id: PrincipalId::generate(),
            kind: "agent".parse().unwrap(),
            name: "builder-1".parse().unwrap(),
            tenant: "acme".parse().unwrap(),
            scopes: vec![Grant::new("work.builds".parse().unwrap(), [crate::Verb::Claim]).unwrap()],
            created_at: "2026-10-26T12:00:00Z".parse().unwrap(),
            expires_at: None,
        }
    }

    #[test]
    fn the_principal_is_kind_colon_name() {
        assert_eq!(info().principal().to_string(), "agent:builder-1");
    }

    #[test]
    fn every_event_type_maps_back() {
        for event in PrincipalEvent::ALL {
            assert!(event.event_type().starts_with("mansio.principal."));
            assert_eq!(
                PrincipalEvent::from_event_type(event.event_type()),
                Some(event)
            );
        }
        assert_eq!(PrincipalEvent::from_event_type("mansio.work.claimed"), None);
    }

    #[test]
    fn a_fact_keeps_only_what_is_set_and_round_trips() {
        let created = PrincipalFact {
            principal: info(),
            reason: None,
        };
        let json = serde_json::to_value(&created).unwrap();
        assert!(json.get("reason").is_none());
        assert!(json["principal"].get("expires_at").is_none());
        assert_eq!(
            serde_json::from_value::<PrincipalFact>(json).unwrap(),
            created
        );

        let revoked = PrincipalFact {
            principal: info(),
            reason: Some(RevokeReason::Expired),
        };
        let json = serde_json::to_value(&revoked).unwrap();
        assert_eq!(json["reason"], "expired");
        assert_eq!(
            serde_json::from_value::<PrincipalFact>(json).unwrap(),
            revoked
        );
    }
}
