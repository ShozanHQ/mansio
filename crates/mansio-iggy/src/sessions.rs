//! The clients signed in with a principal's credential.
//!
//! A request made for a credential goes over a client of its own, signed in
//! as the user the credential belongs to, so Iggy decides what it may do. The
//! clients are kept between requests, one per credential, and ended when the
//! user is deleted or Iggy stops accepting the credential. A table of them
//! holds digests of credentials, not the credentials.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use iggy::prelude::IggyClient;
use mansio_core::{BrokerUser, CredentialDigest};

/// A client signed in as a credential's user.
#[derive(Debug, Clone)]
pub(crate) struct Session {
    pub(crate) client: Arc<IggyClient>,
    pub(crate) user: BrokerUser,
}

#[derive(Debug, Default)]
pub(crate) struct Sessions {
    open: Mutex<HashMap<CredentialDigest, Session>>,
}

impl Sessions {
    fn lock(&self) -> MutexGuard<'_, HashMap<CredentialDigest, Session>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn get(&self, key: &CredentialDigest) -> Option<Session> {
        self.lock().get(key).cloned()
    }

    /// Keep the session, unless another request kept one for the same
    /// credential first. Returns the session that is kept, and the one that
    /// was passed in if it is not, so that its client can be ended.
    pub(crate) fn insert(
        &self,
        key: CredentialDigest,
        session: Session,
    ) -> (Session, Option<Session>) {
        let mut open = self.lock();
        match open.get(&key) {
            Some(existing) => (existing.clone(), Some(session)),
            None => {
                open.insert(key, session.clone());
                (session, None)
            }
        }
    }

    pub(crate) fn remove(&self, key: &CredentialDigest) -> Option<Session> {
        self.lock().remove(key)
    }

    /// Take every session of a user out of the table.
    pub(crate) fn remove_user(&self, user: BrokerUser) -> Vec<Session> {
        let mut open = self.lock();
        let keys: Vec<CredentialDigest> = open
            .iter()
            .filter(|(_, session)| session.user == user)
            .map(|(key, _)| *key)
            .collect();
        keys.iter().filter_map(|key| open.remove(key)).collect()
    }

    /// Take every session out of the table.
    pub(crate) fn drain(&self) -> Vec<Session> {
        self.lock().drain().map(|(_, session)| session).collect()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use iggy::prelude::IggyClientBuilder;
    use mansio_core::Credential;

    use super::*;

    /// A client that is built and not connected.
    fn session(user: u32) -> Session {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address("127.0.0.1:1".to_owned())
            .build()
            .unwrap();
        Session {
            client: Arc::new(client),
            user: BrokerUser(user),
        }
    }

    fn key(token: &str) -> CredentialDigest {
        Credential::new(token).digest()
    }

    #[test]
    fn a_credential_has_one_session_and_the_first_one_kept_wins() {
        let sessions = Sessions::default();
        assert!(sessions.get(&key("a")).is_none());

        let first = session(1);
        let (kept, spare) = sessions.insert(key("a"), first.clone());
        assert!(spare.is_none());
        assert!(Arc::ptr_eq(&kept.client, &first.client));

        let second = session(1);
        let (kept, spare) = sessions.insert(key("a"), second.clone());
        assert!(Arc::ptr_eq(&kept.client, &first.client));
        assert!(Arc::ptr_eq(&spare.unwrap().client, &second.client));
        assert_eq!(sessions.len(), 1);
    }

    #[test]
    fn removing_a_user_takes_all_its_sessions_and_only_those() {
        let sessions = Sessions::default();
        sessions.insert(key("a"), session(1));
        sessions.insert(key("b"), session(1));
        sessions.insert(key("c"), session(2));

        assert_eq!(sessions.remove_user(BrokerUser(1)).len(), 2);
        assert!(sessions.get(&key("a")).is_none());
        assert!(sessions.get(&key("b")).is_none());
        assert!(sessions.get(&key("c")).is_some());
        assert!(sessions.remove(&key("c")).is_some());
        assert!(sessions.remove(&key("c")).is_none());
    }

    #[test]
    fn draining_empties_the_table() {
        let sessions = Sessions::default();
        sessions.insert(key("a"), session(1));
        sessions.insert(key("c"), session(2));
        assert_eq!(sessions.drain().len(), 2);
        assert_eq!(sessions.len(), 0);
    }
}
