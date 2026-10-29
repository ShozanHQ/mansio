//! Iggy permissions for a principal's scopes.
//!
//! Iggy names streams and topics by number in a permission, so scopes are
//! expanded to the topics a tenant has now. The broker remembers each user's
//! scopes and expands them again whenever a topic or a stream is created,
//! because Iggy may give a new one the number of a deleted one. A user is given
//! the right to poll and never the right to send: a credential that could send
//! could write any principal, item id or fact, so every write goes through the
//! server, which writes as the admin.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard, PoisonError};

use iggy::prelude::{GlobalPermissions, Permissions, StreamPermissions, TopicPermissions};
use mansio_core::{Action, BrokerUser, Grant, TenantId, Topic, allows};

/// What a user may do on the topics a stream has: poll the ones its scopes
/// cover, and nothing else.
///
/// No flag is set on the stream or globally, and no topic is given a send
/// permission: the permission of a user who is not the admin never covers a
/// stream as a whole, so a topic made later is closed until the broker opens it
/// for a user whose scopes cover it.
pub(crate) fn permissions(stream: u32, topics: &[(u32, Topic)], grants: &[Grant]) -> Permissions {
    let polled = topics
        .iter()
        .filter(|(_, topic)| allows(grants, Action::Poll, topic))
        .map(|(id, _)| {
            (
                to_usize(*id),
                TopicPermissions {
                    poll_messages: true,
                    ..TopicPermissions::default()
                },
            )
        })
        .collect();
    Permissions {
        global: GlobalPermissions::default(),
        streams: Some(BTreeMap::from([(
            to_usize(stream),
            StreamPermissions {
                topics: Some(polled),
                ..StreamPermissions::default()
            },
        )])),
    }
}

/// No permissions at all: what a user has whose stream is gone.
pub(crate) fn no_permissions() -> Permissions {
    Permissions::default()
}

fn to_usize(id: u32) -> usize {
    usize::try_from(id).unwrap_or(usize::MAX)
}

/// The permissions of the user the server itself acts as: it manages users and
/// streams, and reads and writes every topic, because the facts it publishes
/// and the state it rebuilds are in them. It does not manage the server. The
/// reads that managing implies are spelled out, so that Iggy needs to infer
/// nothing.
pub(crate) fn admin_permissions() -> Permissions {
    Permissions {
        global: GlobalPermissions {
            manage_users: true,
            read_users: true,
            manage_streams: true,
            read_streams: true,
            manage_topics: true,
            read_topics: true,
            poll_messages: true,
            send_messages: true,
            ..GlobalPermissions::default()
        },
        streams: None,
    }
}

/// The tenant and scopes of every user the broker made or was told about.
#[derive(Debug, Default)]
pub(crate) struct Users {
    known: Mutex<HashMap<BrokerUser, (TenantId, Vec<Grant>)>>,
}

impl Users {
    fn lock(&self) -> MutexGuard<'_, HashMap<BrokerUser, (TenantId, Vec<Grant>)>> {
        self.known.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn set(&self, user: BrokerUser, tenant: &TenantId, grants: &[Grant]) {
        self.lock().insert(user, (tenant.clone(), grants.to_vec()));
    }

    pub(crate) fn remove(&self, user: BrokerUser) {
        self.lock().remove(&user);
    }

    /// Every user and its tenant and scopes.
    pub(crate) fn all(&self) -> Vec<(BrokerUser, TenantId, Vec<Grant>)> {
        self.lock()
            .iter()
            .map(|(user, (tenant, grants))| (*user, tenant.clone(), grants.clone()))
            .collect()
    }

    /// The users of the tenant and their scopes.
    pub(crate) fn of_tenant(&self, tenant: &TenantId) -> Vec<(BrokerUser, Vec<Grant>)> {
        self.lock()
            .iter()
            .filter(|(_, (own, _))| own == tenant)
            .map(|(user, (_, grants))| (*user, grants.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use mansio_core::Verb;

    use super::*;

    fn grant(pattern: &str, verbs: &[Verb]) -> Grant {
        Grant::new(pattern.parse().unwrap(), verbs.iter().copied()).unwrap()
    }

    /// What `permissions` names for these scopes over the test topics.
    fn permissions_all(grants: &[Grant]) -> std::collections::BTreeMap<usize, TopicPermissions> {
        permissions(1, &topics(), grants)
            .streams
            .unwrap()
            .remove(&1)
            .unwrap()
            .topics
            .unwrap()
    }

    fn topics() -> Vec<(u32, Topic)> {
        [
            "alerts",
            "facts.build",
            "facts.signal",
            "work.builds",
            "dead",
        ]
        .iter()
        .enumerate()
        .map(|(n, name)| (u32::try_from(n).unwrap() + 10, name.parse().unwrap()))
        .collect()
    }

    #[test]
    fn only_topics_the_scopes_let_it_read_are_named_and_none_can_be_sent_to() {
        let grants = [
            grant("facts.build", &[Verb::Publish]),
            grant("facts.signal", &[Verb::Publish, Verb::Subscribe]),
            grant("work.builds", &[Verb::Claim]),
            grant("alerts", &[Verb::Ack]),
        ];
        let permissions = permissions(7, &topics(), &grants);

        assert_eq!(permissions.global, GlobalPermissions::default());
        let streams = permissions.streams.unwrap();
        assert_eq!(streams.keys().copied().collect::<Vec<_>>(), [7]);
        let stream = &streams[&7];
        assert!(!stream.manage_stream && !stream.read_stream && !stream.manage_topics);
        assert!(!stream.read_topics && !stream.poll_messages && !stream.send_messages);

        // facts.build (11) is only published through the server, and an ack on
        // alerts (10) is enforced by mansio: neither is the broker's to allow.
        // facts.signal (12) and work.builds (13) are read.
        let named = stream.topics.as_ref().unwrap();
        assert_eq!(named.keys().copied().collect::<Vec<_>>(), [12, 13]);
        for topic in named.values() {
            assert!(topic.poll_messages);
            assert!(!topic.send_messages && !topic.read_topic && !topic.manage_topic);
        }
    }

    #[test]
    fn a_publish_scope_never_gives_the_user_the_right_to_send() {
        let grants = [
            grant("facts.*", &[Verb::Publish]),
            grant("work.*", &[Verb::Publish]),
        ];
        let permissions = permissions(1, &topics(), &grants);
        let named = permissions.streams.unwrap()[&1].topics.clone().unwrap();
        assert!(named.is_empty(), "nothing to read, and nothing to send to");
        let all = [grant("facts.*", &[Verb::Publish, Verb::Subscribe])];
        let permissions = permissions_all(&all);
        assert!(permissions.values().all(|t| !t.send_messages));
    }

    #[test]
    fn a_wildcard_names_every_topic_of_its_family_that_exists() {
        let grants = [grant("facts.*", &[Verb::Subscribe])];
        let permissions = permissions(1, &topics(), &grants);
        let named = permissions.streams.unwrap()[&1].topics.clone().unwrap();
        assert_eq!(named.keys().copied().collect::<Vec<_>>(), [11, 12]);
        assert!(named.values().all(|t| t.poll_messages && !t.send_messages));
    }

    #[test]
    fn no_scopes_give_a_stream_entry_with_no_topics() {
        let permissions = permissions(1, &topics(), &[]);
        let streams = permissions.streams.unwrap();
        assert!(streams[&1].topics.as_ref().unwrap().is_empty());
        assert_eq!(permissions.global, GlobalPermissions::default());
    }

    #[test]
    fn a_user_whose_stream_is_gone_has_no_permissions() {
        let none = no_permissions();
        assert_eq!(none.global, GlobalPermissions::default());
        assert!(none.streams.is_none());
    }

    #[test]
    fn the_admin_manages_users_and_streams_and_not_the_server() {
        let admin = admin_permissions();
        let global = &admin.global;
        assert!(global.manage_users && global.manage_streams && global.manage_topics);
        assert!(global.read_users && global.read_streams && global.read_topics);
        assert!(global.poll_messages && global.send_messages);
        assert!(!global.manage_servers && !global.read_servers);
        assert!(admin.streams.is_none());
    }

    #[test]
    fn users_are_remembered_by_tenant_until_removed() {
        let users = Users::default();
        let acme: TenantId = "acme".parse().unwrap();
        let other: TenantId = "other".parse().unwrap();
        let scopes = [grant("alerts", &[Verb::Ack])];
        users.set(BrokerUser(1), &acme, &scopes);
        users.set(BrokerUser(2), &other, &[]);
        users.set(BrokerUser(3), &acme, &[]);

        let mut acme_users: Vec<u32> = users.of_tenant(&acme).iter().map(|(u, _)| u.0).collect();
        acme_users.sort_unstable();
        assert_eq!(acme_users, [1, 3]);
        assert_eq!(users.all().len(), 3);
        users.remove(BrokerUser(1));
        assert_eq!(users.of_tenant(&acme).len(), 1);
        assert_eq!(users.of_tenant(&"nobody".parse().unwrap()).len(), 0);
    }
}
