//! Where every named consumer and every webhook has got to.
//!
//! A position is the offset of the last message a consumer handled. mansio
//! keeps all of them itself, in a log of the server's own stream in the broker
//! that no credential can reach ([`mansio_core::SystemLog::Cursors`]), and not
//! in the broker's own consumer offsets: Iggy lets any user that may read a
//! topic store an offset for any consumer of it, which would let a read-only
//! credential move another consumer, or a webhook, on.
//!
//! A named consumer belongs to the principal that uses it, in a tenant and on a
//! topic, so two principals that use one name have two positions, and neither
//! can read or move the other's. A webhook's position is its own and is not
//! reachable through the consumers API at all.
//!
//! The positions are a projection of the log, read when first needed. Every
//! commit is a record, written before the position changes, and the log is
//! never trimmed, so it grows with the number of commits.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, PoisonError};

use mansio_core::{Broker, ConsumerName, Principal, SystemLog, TenantId, Topic, WebhookId};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, OnceCell};
use tracing::{info, warn};

use crate::error::ApiError;

/// Whose position, and on what.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Cursor {
    /// A consumer that a principal named, on a topic of a tenant.
    Named {
        tenant: TenantId,
        topic: Topic,
        principal: Principal,
        consumer: ConsumerName,
    },
    /// A webhook, on the topic it delivers.
    Webhook(WebhookId),
}

/// One record of the log: a position set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Entry {
    Named {
        tenant: TenantId,
        topic: Topic,
        principal: Principal,
        consumer: ConsumerName,
        offset: u64,
    },
    Webhook {
        webhook: WebhookId,
        offset: u64,
    },
}

impl Entry {
    fn new(cursor: &Cursor, offset: u64) -> Self {
        match cursor.clone() {
            Cursor::Named {
                tenant,
                topic,
                principal,
                consumer,
            } => Entry::Named {
                tenant,
                topic,
                principal,
                consumer,
                offset,
            },
            Cursor::Webhook(webhook) => Entry::Webhook { webhook, offset },
        }
    }

    fn cursor(self) -> (Cursor, u64) {
        match self {
            Entry::Named {
                tenant,
                topic,
                principal,
                consumer,
                offset,
            } => (
                Cursor::Named {
                    tenant,
                    topic,
                    principal,
                    consumer,
                },
                offset,
            ),
            Entry::Webhook { webhook, offset } => (Cursor::Webhook(webhook), offset),
        }
    }
}

/// The positions of every consumer and webhook.
pub struct Cursors {
    broker: Arc<dyn Broker>,
    /// How many records to read at a time when loading.
    page: u32,
    positions: StdMutex<HashMap<Cursor, u64>>,
    loaded: OnceCell<()>,
    /// Held from the write of a record to the change of the position it sets,
    /// so that the positions end as the log does when commits race.
    writing: Mutex<()>,
}

impl std::fmt::Debug for Cursors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cursors").finish_non_exhaustive()
    }
}

impl Cursors {
    /// Positions kept in `broker`'s log, read `page` records at a time.
    pub(crate) fn new(broker: Arc<dyn Broker>, page: u32) -> Self {
        Self {
            broker,
            page: page.max(1),
            positions: StdMutex::default(),
            loaded: OnceCell::new(),
            writing: Mutex::new(()),
        }
    }

    fn positions(&self) -> MutexGuard<'_, HashMap<Cursor, u64>> {
        self.positions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Read the log, once. The next call tries again if this one failed.
    ///
    /// # Errors
    ///
    /// Returns an error if the log cannot be read.
    pub async fn load(&self) -> Result<(), ApiError> {
        self.loaded.get_or_try_init(|| self.rebuild()).await?;
        Ok(())
    }

    async fn rebuild(&self) -> Result<(), ApiError> {
        let mut positions = HashMap::new();
        let mut offset = 0;
        loop {
            let batch = self
                .broker
                .system_read(SystemLog::Cursors, offset, self.page)
                .await?;
            let Some(last) = batch.last() else { break };
            let next = last.offset.saturating_add(1);
            for record in &batch {
                match serde_json::from_slice::<Entry>(&record.payload) {
                    Ok(entry) => {
                        let (cursor, at) = entry.cursor();
                        positions.insert(cursor, at);
                    }
                    Err(reason) => warn!(
                        offset = record.offset,
                        %reason,
                        "a position record could not be read and was left out"
                    ),
                }
            }
            if next <= offset {
                break;
            }
            offset = next;
        }
        info!(positions = positions.len(), "positions loaded");
        *self.positions() = positions;
        Ok(())
    }

    /// The offset of the last message the cursor handled, or `None` if it has
    /// handled none.
    ///
    /// # Errors
    ///
    /// Returns an error if the log cannot be read.
    pub(crate) async fn get(&self, cursor: &Cursor) -> Result<Option<u64>, ApiError> {
        self.load().await?;
        Ok(self.positions().get(cursor).copied())
    }

    /// Set where the cursor has got to. The record is written first: a write
    /// that fails changes nothing.
    ///
    /// # Errors
    ///
    /// Returns 502 or 503 if the log cannot be written.
    pub(crate) async fn set(&self, cursor: Cursor, offset: u64) -> Result<(), ApiError> {
        self.load().await?;
        let _writing = self.writing.lock().await;
        if self.positions().get(&cursor) == Some(&offset) {
            return Ok(());
        }
        let record = serde_json::to_vec(&Entry::new(&cursor, offset)).map_err(|reason| {
            tracing::error!(%reason, "a position record did not serialize");
            ApiError::Internal
        })?;
        self.broker
            .system_append(SystemLog::Cursors, &record)
            .await?;
        self.positions().insert(cursor, offset);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mansio_core::MemoryBroker;

    use super::*;

    fn named(principal: &str, consumer: &str) -> Cursor {
        Cursor::Named {
            tenant: "acme".parse().unwrap(),
            topic: "facts.build".parse().unwrap(),
            principal: principal.parse().unwrap(),
            consumer: consumer.parse().unwrap(),
        }
    }

    #[tokio::test]
    async fn a_position_is_kept_per_principal_consumer_and_webhook() {
        let broker = Arc::new(MemoryBroker::new());
        let cursors = Cursors::new(broker.clone(), 2);
        let (one, two) = (named("agent:one", "dash"), named("agent:two", "dash"));
        let webhook = Cursor::Webhook(WebhookId::generate());
        assert_eq!(cursors.get(&one).await.unwrap(), None);

        cursors.set(one.clone(), 4).await.unwrap();
        cursors.set(webhook.clone(), 9).await.unwrap();
        // Zero is a position, not "none", and another principal's consumer of
        // the same name is another consumer.
        cursors.set(named("agent:one", "other"), 0).await.unwrap();
        assert_eq!(cursors.get(&one).await.unwrap(), Some(4));
        assert_eq!(cursors.get(&two).await.unwrap(), None);
        assert_eq!(
            cursors.get(&named("agent:one", "other")).await.unwrap(),
            Some(0)
        );
        assert_eq!(cursors.get(&webhook).await.unwrap(), Some(9));
        // A commit replaces what was there, even with less.
        cursors.set(one.clone(), 1).await.unwrap();
        assert_eq!(cursors.get(&one).await.unwrap(), Some(1));
    }

    #[tokio::test]
    async fn the_positions_are_read_back_from_the_log_by_a_new_server() {
        let broker = Arc::new(MemoryBroker::new());
        let cursors = Cursors::new(broker.clone(), 2);
        let one = named("agent:one", "dash");
        let webhook = Cursor::Webhook(WebhookId::generate());
        for offset in 0..5 {
            cursors.set(one.clone(), offset).await.unwrap();
        }
        cursors.set(webhook.clone(), 3).await.unwrap();
        // A record that cannot be read is left out and the rest is read.
        broker
            .system_append(SystemLog::Cursors, b"not a record")
            .await
            .unwrap();

        let restarted = Cursors::new(broker.clone(), 2);
        assert_eq!(restarted.get(&one).await.unwrap(), Some(4));
        assert_eq!(restarted.get(&webhook).await.unwrap(), Some(3));
        assert_eq!(
            restarted.get(&named("agent:two", "dash")).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn the_same_position_twice_is_written_once() {
        let broker = Arc::new(MemoryBroker::new());
        let cursors = Cursors::new(broker.clone(), 10);
        let one = named("agent:one", "dash");
        cursors.set(one.clone(), 2).await.unwrap();
        cursors.set(one, 2).await.unwrap();
        let log = broker.system_read(SystemLog::Cursors, 0, 10).await.unwrap();
        assert_eq!(log.len(), 1);
    }

    #[tokio::test]
    async fn the_log_holds_no_credential_and_no_secret() {
        let broker = Arc::new(MemoryBroker::new());
        let cursors = Cursors::new(broker.clone(), 10);
        cursors.set(named("agent:one", "dash"), 2).await.unwrap();
        let log = broker.system_read(SystemLog::Cursors, 0, 10).await.unwrap();
        let text = String::from_utf8(log[0].payload.clone()).unwrap();
        assert!(
            text.contains(r#""op":"named""#) && text.contains("agent:one"),
            "{text}"
        );
    }
}
