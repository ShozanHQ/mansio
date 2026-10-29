//! The principals that have a credential.
//!
//! The registry is a projection of a log the server keeps in the broker where
//! no credential can reach it ([`mansio_core::Broker::system_append`]): a `created` record
//! for each credential issued and a `revoked` record for each withdrawn. On
//! start the log is read from the beginning and the registry rebuilt. Nothing
//! secret is in it: not the credential, only a digest of it that finds the
//! principal a presented credential belongs to without asking the broker, and
//! not the password of the broker user, which nobody knows.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

use mansio_core::{
    BrokerUser, CredentialDigest, Grant, Principal, PrincipalId, PrincipalInfo, PrincipalKind,
    PrincipalName, TenantId, Timestamp, TopicPattern, Verb,
};
use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ApiError;

/// The version of the record format written now. A record says which it is in
/// `v`; one without is version 1, which was written before the field was.
///
/// A later version that changes what a record means adds a number here and
/// keeps reading the earlier ones: a record that was valid when it was written
/// is a principal for as long as the log is kept.
const VERSION: u64 = 1;

/// One record of the log.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum Entry {
    /// A credential was issued for the principal, as the broker's user.
    Created {
        principal: PrincipalInfo,
        user: BrokerUser,
        digest: CredentialDigest,
    },
    /// The principal's credential was withdrawn.
    Revoked { id: PrincipalId, at: Timestamp },
}

/// A record as it is read: the shape of the version it was written in, with
/// fields it does not know ignored, and a scope not checked again.
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum StoredEntry {
    Created {
        principal: StoredPrincipal,
        user: BrokerUser,
        digest: CredentialDigest,
    },
    Revoked {
        id: PrincipalId,
        at: Timestamp,
    },
}

#[derive(Deserialize)]
struct StoredPrincipal {
    id: PrincipalId,
    kind: PrincipalKind,
    name: PrincipalName,
    tenant: TenantId,
    scopes: Vec<StoredGrant>,
    created_at: Timestamp,
    #[serde(default)]
    expires_at: Option<Timestamp>,
}

#[derive(Deserialize)]
struct StoredGrant {
    topic: TopicPattern,
    verbs: Vec<Verb>,
}

impl From<StoredEntry> for Entry {
    fn from(stored: StoredEntry) -> Self {
        match stored {
            StoredEntry::Created {
                principal,
                user,
                digest,
            } => Entry::Created {
                principal: PrincipalInfo {
                    id: principal.id,
                    kind: principal.kind,
                    name: principal.name,
                    tenant: principal.tenant,
                    scopes: principal
                        .scopes
                        .into_iter()
                        .map(|grant| Grant::from_stored(grant.topic, grant.verbs))
                        .collect(),
                    created_at: principal.created_at,
                    expires_at: principal.expires_at,
                },
                user,
                digest,
            },
            StoredEntry::Revoked { id, at } => Entry::Revoked { id, at },
        }
    }
}

impl Entry {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, ApiError> {
        let mut value = serde_json::to_value(self).map_err(|reason| {
            tracing::error!(%reason, "a registry record did not serialize");
            ApiError::Internal
        })?;
        let Some(fields) = value.as_object_mut() else {
            tracing::error!("a registry record did not serialize to an object");
            return Err(ApiError::Internal);
        };
        fields.insert("v".to_owned(), VERSION.into());
        serde_json::to_vec(&value).map_err(|reason| {
            tracing::error!(%reason, "a registry record did not serialize");
            ApiError::Internal
        })
    }

    /// Read a record of any version up to this one. A scope in it is taken as
    /// it was stored, whatever the rules for a new one are now.
    ///
    /// # Errors
    ///
    /// Returns an error for a record that is not one, or was written by a
    /// version newer than this one.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let value: Value = serde_json::from_slice(bytes)?;
        let version = match value.get("v") {
            None => 1,
            Some(v) => v
                .as_u64()
                .ok_or_else(|| serde_json::Error::custom("the version `v` is not a number"))?,
        };
        if version == 0 || version > VERSION {
            return Err(serde_json::Error::custom(format!(
                "version {version} of the record format; this server reads up to {VERSION}"
            )));
        }
        Ok(serde_json::from_value::<StoredEntry>(value)?.into())
    }
}

/// A principal and the broker's user behind its credential.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Record {
    pub(crate) info: PrincipalInfo,
    pub(crate) user: BrokerUser,
    /// Of the credential, to find this principal by.
    pub(crate) digest: CredentialDigest,
}

impl Record {
    /// Whether the credential has stopped working at `now`, in milliseconds
    /// since the Unix epoch.
    pub(crate) fn expired(&self, now: i64) -> bool {
        self.info
            .expires_at
            .as_ref()
            .and_then(Timestamp::unix_millis)
            .is_some_and(|expires| expires <= now)
    }
}

/// A principal is named by its kind and name within its tenant.
type Name = (TenantId, Principal);

#[derive(Debug, Default)]
struct Inner {
    by_id: HashMap<PrincipalId, Record>,
    by_digest: HashMap<CredentialDigest, PrincipalId>,
    /// Names in use, including those of principals being issued, so that two
    /// requests for one name do not both succeed.
    names: HashSet<Name>,
}

/// The principals that have a credential now.
#[derive(Debug, Default)]
pub(crate) struct Registry {
    inner: Mutex<Inner>,
}

fn name_of(info: &PrincipalInfo) -> Name {
    (info.tenant.clone(), info.principal())
}

impl Registry {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Forget everything, before the log is read again.
    pub(crate) fn clear(&self) {
        *self.lock() = Inner::default();
    }

    /// Take a record of the log into account.
    pub(crate) fn apply(&self, entry: Entry) {
        match entry {
            Entry::Created {
                principal,
                user,
                digest,
            } => self.insert(Record {
                info: principal,
                user,
                digest,
            }),
            Entry::Revoked { id, .. } => {
                self.remove(id);
            }
        }
    }

    /// Claim the name for a principal about to be issued: a conflict if it is
    /// taken.
    pub(crate) fn reserve(&self, tenant: &TenantId, principal: &Principal) -> Result<(), ApiError> {
        if self
            .lock()
            .names
            .insert((tenant.clone(), principal.clone()))
        {
            Ok(())
        } else {
            Err(ApiError::Conflict(format!(
                "{principal} already has a credential in {tenant}"
            )))
        }
    }

    /// Give up a name that was reserved and never used.
    pub(crate) fn release(&self, tenant: &TenantId, principal: &Principal) {
        self.lock()
            .names
            .remove(&(tenant.clone(), principal.clone()));
    }

    /// Add a principal. Its name is reserved from now on.
    pub(crate) fn insert(&self, record: Record) {
        let mut inner = self.lock();
        if let Some(old) = inner.by_id.remove(&record.info.id) {
            inner.by_digest.remove(&old.digest);
        }
        inner.names.insert(name_of(&record.info));
        inner.by_digest.insert(record.digest, record.info.id);
        inner.by_id.insert(record.info.id, record);
    }

    /// Take a principal out, and free its name.
    pub(crate) fn remove(&self, id: PrincipalId) -> Option<Record> {
        let mut inner = self.lock();
        let record = inner.by_id.remove(&id)?;
        inner.by_digest.remove(&record.digest);
        inner.names.remove(&name_of(&record.info));
        Some(record)
    }

    pub(crate) fn get(&self, id: PrincipalId) -> Option<Record> {
        self.lock().by_id.get(&id).cloned()
    }

    /// The principal a credential was issued to, found by its digest.
    pub(crate) fn by_digest(&self, digest: &CredentialDigest) -> Option<Record> {
        let inner = self.lock();
        inner.by_id.get(inner.by_digest.get(digest)?).cloned()
    }

    /// Every principal, oldest first.
    pub(crate) fn all(&self) -> Vec<Record> {
        let mut records: Vec<Record> = self.lock().by_id.values().cloned().collect();
        records.sort_by(|a, b| {
            let issued = |record: &Record| record.info.created_at.unix_millis();
            issued(a)
                .cmp(&issued(b))
                .then_with(|| a.info.id.cmp(&b.info.id))
        });
        records
    }

    /// The principals of the tenant, or of every tenant, oldest first.
    pub(crate) fn list(&self, tenant: Option<&TenantId>) -> Vec<PrincipalInfo> {
        self.all()
            .into_iter()
            .filter(|record| tenant.is_none_or(|tenant| record.info.tenant == *tenant))
            .map(|record| record.info)
            .collect()
    }

    /// The principals whose credential has expired at `now`.
    pub(crate) fn expired(&self, now: i64) -> Vec<PrincipalId> {
        self.all()
            .into_iter()
            .filter(|record| record.expired(now))
            .map(|record| record.info.id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use mansio_core::{Credential, Grant, Verb};
    use serde_json::json;

    use super::*;

    fn digest(token: &str) -> CredentialDigest {
        Credential::new(token).digest()
    }

    fn info(name: &str, tenant: &str, created: &str, expires: Option<&str>) -> PrincipalInfo {
        PrincipalInfo {
            id: PrincipalId::generate(),
            kind: "agent".parse().unwrap(),
            name: name.parse().unwrap(),
            tenant: tenant.parse().unwrap(),
            scopes: vec![Grant::new("alerts".parse().unwrap(), [Verb::Subscribe]).unwrap()],
            created_at: created.parse().unwrap(),
            expires_at: expires.map(|at| at.parse().unwrap()),
        }
    }

    fn created(info: &PrincipalInfo, user: u32) -> Entry {
        Entry::Created {
            principal: info.clone(),
            user: BrokerUser(user),
            digest: digest(&format!("token-{user}")),
        }
    }

    fn record(info: &PrincipalInfo, user: u32) -> Record {
        Record {
            info: info.clone(),
            user: BrokerUser(user),
            digest: digest(&format!("token-{user}")),
        }
    }

    #[test]
    fn a_record_of_the_log_round_trips_and_names_no_secret() {
        let principal = info("a", "acme", "2026-10-26T12:00:00Z", None);
        for entry in [
            created(&principal, 7),
            Entry::Revoked {
                id: principal.id,
                at: "2026-10-26T13:00:00Z".parse().unwrap(),
            },
        ] {
            let bytes = entry.encode().unwrap();
            assert_eq!(Entry::decode(&bytes).unwrap(), entry);
        }
        let text = String::from_utf8(created(&principal, 7).encode().unwrap()).unwrap();
        assert!(text.contains(r#""op":"created""#), "{text}");
        assert!(text.contains(&digest("token-7").to_string()), "{text}");
        assert!(!text.contains("token-7"), "{text}");
        assert!(Entry::decode(b"not json").is_err());
        assert!(Entry::decode(br#"{"op":"other"}"#).is_err());
    }

    #[test]
    fn a_record_says_which_version_of_the_format_it_is() {
        let principal = info("a", "acme", "2026-10-26T12:00:00Z", None);
        let text = String::from_utf8(created(&principal, 7).encode().unwrap()).unwrap();
        let written: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(written["v"], 1, "{text}");
        let revoked = Entry::Revoked {
            id: principal.id,
            at: "2026-10-26T13:00:00Z".parse().unwrap(),
        };
        let written: serde_json::Value =
            serde_json::from_slice(&revoked.encode().unwrap()).unwrap();
        assert_eq!(written["v"], 1);
    }

    /// A `created` record as an earlier version wrote it, with the fields and
    /// the scopes the test gives.
    fn old_record(version: Option<serde_json::Value>, scopes: serde_json::Value) -> Vec<u8> {
        let mut record = json!({
            "op": "created",
            "principal": {
                "id": PrincipalId::generate().to_string(),
                "kind": "agent",
                "name": "builder",
                "tenant": "acme",
                "scopes": scopes,
                "created_at": "2026-10-26T12:00:00Z",
                "a_field_a_later_version_added": true,
            },
            "user": 4,
            "digest": digest("token-4").to_string(),
            "another_field": [1, 2],
        });
        if let Some(version) = version {
            record["v"] = version;
        }
        serde_json::to_vec(&record).unwrap()
    }

    #[test]
    fn a_record_of_an_earlier_version_is_read_whatever_has_been_added_to_the_format() {
        let scopes = json!([{"topic": "alerts", "verbs": ["subscribe"], "note": "ignored"}]);
        // No `v` is version 1; fields it does not know are ignored.
        for version in [None, Some(json!(1))] {
            let entry = Entry::decode(&old_record(version, scopes.clone())).unwrap();
            let Entry::Created {
                principal, user, ..
            } = entry
            else {
                panic!("a created record");
            };
            assert_eq!(user, BrokerUser(4));
            assert_eq!(principal.name.to_string(), "builder");
            assert_eq!(principal.scopes.len(), 1);
        }
    }

    #[test]
    fn a_scope_that_todays_rules_would_refuse_is_still_the_principals_scope() {
        // Neither would be made now: only mansio publishes to `facts.work`,
        // and `claim` applies to work topics. Both were valid once.
        let scopes = json!([
            {"topic": "facts.work", "verbs": ["publish", "subscribe"]},
            {"topic": "alerts", "verbs": ["claim"]},
        ]);
        let Entry::Created { principal, .. } = Entry::decode(&old_record(None, scopes)).unwrap()
        else {
            panic!("a created record");
        };
        assert_eq!(principal.scopes.len(), 2);
        let facts_work: mansio_core::Topic = "facts.work".parse().unwrap();
        assert!(!mansio_core::allows(
            &principal.scopes,
            mansio_core::Action::Send,
            &facts_work
        ));
        assert!(mansio_core::allows(
            &principal.scopes,
            mansio_core::Action::Poll,
            &facts_work
        ));
    }

    #[test]
    fn a_record_of_a_later_version_or_without_a_meaning_is_not_read() {
        let scopes = json!([{"topic": "alerts", "verbs": ["subscribe"]}]);
        for version in [json!(2), json!(0), json!("1"), json!(null)] {
            let error = Entry::decode(&old_record(Some(version.clone()), scopes.clone()))
                .unwrap_err()
                .to_string();
            assert!(error.contains("version"), "{version}: {error}");
        }
        // And what is not a record of any version.
        assert!(Entry::decode(br#"{"op":"created","principal":"x"}"#).is_err());
        assert!(Entry::decode(b"").is_err());
    }

    #[test]
    fn replaying_the_log_leaves_the_principals_not_revoked() {
        let registry = Registry::default();
        let a = info("a", "acme", "2026-10-26T12:00:00Z", None);
        let b = info("b", "acme", "2026-10-26T12:01:00Z", None);
        let c = info("c", "other", "2026-10-26T12:02:00Z", None);
        for entry in [
            created(&a, 1),
            created(&b, 2),
            Entry::Revoked {
                id: a.id,
                at: "2026-10-26T12:30:00Z".parse().unwrap(),
            },
            created(&c, 3),
        ] {
            registry.apply(entry);
        }
        assert!(registry.get(a.id).is_none());
        assert_eq!(registry.get(b.id).unwrap().user, BrokerUser(2));
        assert_eq!(registry.by_digest(&digest("token-3")).unwrap().info, c);
        assert!(registry.by_digest(&digest("token-1")).is_none(), "revoked");
        assert!(registry.by_digest(&digest("token-9")).is_none(), "unknown");
        let names: Vec<String> = registry
            .list(None)
            .iter()
            .map(|p| p.name.to_string())
            .collect();
        assert_eq!(names, ["b", "c"]);
        let acme = registry.list(Some(&"acme".parse().unwrap()));
        assert_eq!(acme, vec![b]);
    }

    #[test]
    fn a_name_is_taken_by_a_principal_in_its_tenant_until_it_is_revoked() {
        let registry = Registry::default();
        let a = info("a", "acme", "2026-10-26T12:00:00Z", None);
        registry.insert(record(&a, 1));
        let tenant: TenantId = "acme".parse().unwrap();
        assert!(matches!(
            registry.reserve(&tenant, &a.principal()),
            Err(ApiError::Conflict(_))
        ));
        // The same name in another tenant is another principal.
        registry
            .reserve(&"other".parse().unwrap(), &a.principal())
            .unwrap();
        registry.remove(a.id).unwrap();
        registry.reserve(&tenant, &a.principal()).unwrap();
        assert!(matches!(
            registry.reserve(&tenant, &a.principal()),
            Err(ApiError::Conflict(_))
        ));
        registry.release(&tenant, &a.principal());
        registry.reserve(&tenant, &a.principal()).unwrap();
    }

    #[test]
    fn expiry_is_judged_against_the_time_given() {
        let registry = Registry::default();
        let forever = info("forever", "acme", "2026-10-26T12:00:00Z", None);
        let short = info(
            "short",
            "acme",
            "2026-10-26T12:00:00Z",
            Some("2026-10-26T12:00:10Z"),
        );
        registry.insert(record(&forever, 1));
        registry.insert(record(&short, 2));
        let at = |text: &str| text.parse::<Timestamp>().unwrap().unix_millis().unwrap();
        assert!(registry.expired(at("2026-10-26T12:00:09Z")).is_empty());
        assert_eq!(registry.expired(at("2026-10-26T12:00:10Z")), [short.id]);
        assert_eq!(registry.expired(at("2030-01-01T00:00:00Z")), [short.id]);
        assert!(!registry.get(forever.id).unwrap().expired(i64::MAX));
    }

    #[test]
    fn clearing_forgets_everything_including_names() {
        let registry = Registry::default();
        let a = info("a", "acme", "2026-10-26T12:00:00Z", None);
        registry.apply(created(&a, 1));
        registry.clear();
        assert!(registry.all().is_empty());
        registry.reserve(&a.tenant, &a.principal()).unwrap();
    }
}
