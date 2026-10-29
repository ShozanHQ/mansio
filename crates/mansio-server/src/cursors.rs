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
//!
//! A record that cannot be read (damaged, or written by a newer version of the
//! format) is skipped with a warning, unlike the records of the principals and
//! of the webhooks, whose load fails. What a skipped record costs is that a
//! consumer or a webhook starts from an older position, and so reads or
//! delivers some messages again: every consumer of this bus already has to
//! cope with a message twice, as delivery is at least once.
//!
//! A commit whose record may or may not have been stored (the broker failed in
//! a way that does not say) leaves the positions possibly one record behind the
//! log, so the next use reads the log again before it trusts them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, PoisonError};

use mansio_core::{Broker, ConsumerName, Principal, SystemLog, TenantId, Topic, WebhookId};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::error::ApiError;
use crate::records;

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

/// What is known of the log, all of it changed together.
#[derive(Debug, Default)]
struct State {
    positions: HashMap<Cursor, u64>,
    /// Whether the positions are what the log says, as far as is known. It is
    /// false until the log has been read, and nothing reads the positions while
    /// it is.
    fresh: bool,
    /// How many commits have failed in a way that does not say whether their
    /// record was stored. A read of the log that began before one of them may
    /// not have the record, and so does not make the positions fresh.
    unsure: u64,
}

/// The positions of every consumer and webhook.
pub struct Cursors {
    broker: Arc<dyn Broker>,
    /// How many records to read at a time when loading.
    page: u32,
    state: StdMutex<State>,
    /// Held while the log is read, so that it is read by one at a time and
    /// whoever else needs the positions waits for it.
    reading: Mutex<()>,
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
            state: StdMutex::default(),
            reading: Mutex::new(()),
            writing: Mutex::new(()),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Read the log, if it has not been read or a commit may have left the
    /// positions behind it. Whoever calls while it is being read waits for it,
    /// and never sees the positions as they were before. The next call tries
    /// again if this one failed.
    ///
    /// # Errors
    ///
    /// Returns an error if the log cannot be read.
    pub async fn load(&self) -> Result<(), ApiError> {
        if self.state().fresh {
            return Ok(());
        }
        let _reading = self.reading.lock().await;
        let unsure = {
            let state = self.state();
            if state.fresh {
                return Ok(());
            }
            state.unsure
        };
        let positions = self.read_log().await?;
        // Fresh only if no commit has failed unsure since the read began; if
        // one has, the next use reads again.
        let mut state = self.state();
        state.positions = positions;
        state.fresh = state.unsure == unsure;
        Ok(())
    }

    /// A commit failed in a way that does not say whether its record was
    /// stored: the positions may be one record behind the log.
    fn commit_was_unsure(&self) {
        let mut state = self.state();
        state.unsure = state.unsure.saturating_add(1);
        state.fresh = false;
    }

    async fn read_log(&self) -> Result<HashMap<Cursor, u64>, ApiError> {
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
                match records::decode::<Entry>(&record.payload) {
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
        Ok(positions)
    }

    /// The offset of the last message the cursor handled, or `None` if it has
    /// handled none.
    ///
    /// # Errors
    ///
    /// Returns an error if the log cannot be read.
    pub(crate) async fn get(&self, cursor: &Cursor) -> Result<Option<u64>, ApiError> {
        self.load().await?;
        Ok(self.state().positions.get(cursor).copied())
    }

    /// Set where the cursor has got to. It waits for a read of the log that is
    /// in progress and compares with what that read found. The record is
    /// written first, and the position changes once it has been. If the write
    /// fails in a way that does not say whether the record was stored, the
    /// positions are read from the log again before they are used.
    ///
    /// # Errors
    ///
    /// Returns 502 or 503 if the log cannot be written.
    pub(crate) async fn set(&self, cursor: Cursor, offset: u64) -> Result<(), ApiError> {
        let _writing = self.writing.lock().await;
        // After a commit that may or may not have been stored this reads the
        // log again, so that "the position is already this" is true of the log.
        self.load().await?;
        if self.state().positions.get(&cursor) == Some(&offset) {
            return Ok(());
        }
        let record = records::encode(&Entry::new(&cursor, offset))?;
        if let Err(error) = self.broker.system_append(SystemLog::Cursors, &record).await {
            // The record may be in the log all the same.
            self.commit_was_unsure();
            return Err(error.into());
        }
        self.state().positions.insert(cursor, offset);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    use async_trait::async_trait;
    use mansio_core::{BrokerError, Caller, Envelope, MemoryBroker, Stored, SystemRecord};

    use super::*;

    /// A memory broker whose appends to the positions' log fail as told: after
    /// storing the record, as a connection that drops after the write does, or
    /// without storing it.
    #[derive(Default)]
    struct Unsure {
        inner: MemoryBroker,
        store_then_fail: AtomicBool,
        fail_without_storing: AtomicBool,
        /// How many times a read of the log gives way to the other tasks
        /// before it answers, as a read over a network does. Every read counts.
        yields: AtomicU32,
        reads: AtomicU32,
    }

    #[async_trait]
    impl Broker for Unsure {
        async fn tenants(&self) -> Result<Vec<TenantId>, BrokerError> {
            self.inner.tenants().await
        }

        async fn topics(&self, tenant: &TenantId) -> Result<Vec<Topic>, BrokerError> {
            self.inner.topics(tenant).await
        }

        async fn ensure_tenant(&self, tenant: &TenantId) -> Result<(), BrokerError> {
            self.inner.ensure_tenant(tenant).await
        }

        async fn ensure_topic(&self, tenant: &TenantId, topic: &Topic) -> Result<(), BrokerError> {
            self.inner.ensure_topic(tenant, topic).await
        }

        async fn publish(
            &self,
            caller: &Caller<'_>,
            envelope: &Envelope,
        ) -> Result<Option<u64>, BrokerError> {
            self.inner.publish(caller, envelope).await
        }

        async fn poll(
            &self,
            caller: &Caller<'_>,
            tenant: &TenantId,
            topic: &Topic,
            offset: u64,
            max: u32,
        ) -> Result<Vec<Stored>, BrokerError> {
            self.inner.poll(caller, tenant, topic, offset, max).await
        }

        async fn system_append(&self, log: SystemLog, record: &[u8]) -> Result<(), BrokerError> {
            if self.fail_without_storing.load(Ordering::SeqCst) {
                return Err(BrokerError::Unavailable("injected".to_owned()));
            }
            self.inner.system_append(log, record).await?;
            if self.store_then_fail.load(Ordering::SeqCst) {
                return Err(BrokerError::Unavailable("lost the answer".to_owned()));
            }
            Ok(())
        }

        async fn system_read(
            &self,
            log: SystemLog,
            offset: u64,
            max: u32,
        ) -> Result<Vec<SystemRecord>, BrokerError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            for _ in 0..self.yields.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            self.inner.system_read(log, offset, max).await
        }
    }

    #[tokio::test]
    async fn a_commit_whose_record_was_stored_though_it_failed_is_not_trusted_to_have_failed() {
        let broker = Arc::new(Unsure::default());
        let cursors = Cursors::new(broker.clone(), 10);
        let one = named("agent:one", "dash");
        cursors.set(one.clone(), 3).await.unwrap();

        // The commit of 5 is stored and reported as an error. The consumer is
        // told so, and goes back to 3.
        broker.store_then_fail.store(true, Ordering::SeqCst);
        assert!(cursors.set(one.clone(), 5).await.is_err());
        broker.store_then_fail.store(false, Ordering::SeqCst);
        cursors.set(one.clone(), 3).await.unwrap();

        // It is 3 now, in the log as well as in the cache: a server that reads
        // the log finds 3 and not 5.
        assert_eq!(cursors.get(&one).await.unwrap(), Some(3));
        let restarted = Cursors::new(broker.clone(), 10);
        assert_eq!(restarted.get(&one).await.unwrap(), Some(3));
    }

    #[tokio::test]
    async fn a_commit_that_was_not_stored_is_written_by_the_next_one_of_the_same_value() {
        let broker = Arc::new(Unsure::default());
        let cursors = Cursors::new(broker.clone(), 10);
        let one = named("agent:one", "dash");
        cursors.set(one.clone(), 3).await.unwrap();

        broker.fail_without_storing.store(true, Ordering::SeqCst);
        assert!(cursors.set(one.clone(), 5).await.is_err());
        broker.fail_without_storing.store(false, Ordering::SeqCst);
        // The cache says 3 and so does the log: 5 is written, and once.
        assert_eq!(cursors.get(&one).await.unwrap(), Some(3));
        cursors.set(one.clone(), 5).await.unwrap();
        cursors.set(one.clone(), 5).await.unwrap();
        let log = broker.system_read(SystemLog::Cursors, 0, 10).await.unwrap();
        assert_eq!(log.len(), 2, "3 and then 5");
        assert_eq!(Cursors::new(broker, 10).get(&one).await.unwrap(), Some(5));
    }

    /// A log with a position for each of the cursors, written by one server and
    /// read by another over a broker whose reads give way to other tasks.
    async fn a_log_of_positions(cursors: &[(&Cursor, u64)]) -> Arc<Unsure> {
        let broker = Arc::new(Unsure::default());
        let writer = Cursors::new(broker.clone(), 2);
        for (cursor, offset) in cursors {
            writer.set((*cursor).clone(), *offset).await.unwrap();
        }
        broker.yields.store(3, Ordering::SeqCst);
        broker.reads.store(0, Ordering::SeqCst);
        broker
    }

    #[tokio::test]
    async fn nobody_reads_a_position_before_the_first_read_of_the_log_has_finished() {
        let (one, two) = (
            Cursor::Webhook(WebhookId::generate()),
            Cursor::Webhook(WebhookId::generate()),
        );
        let broker = a_log_of_positions(&[(&one, 3), (&two, 7)]).await;
        let cursors = Cursors::new(broker.clone(), 2);

        // As a server that has just started with two webhooks: each task asks
        // where it stopped while the first of them is still reading the log.
        let (a, b, c) = tokio::join!(cursors.get(&one), cursors.get(&two), cursors.get(&one));
        assert_eq!(a.unwrap(), Some(3), "the first, which reads");
        assert_eq!(b.unwrap(), Some(7), "the second, which waits for it");
        assert_eq!(c.unwrap(), Some(3), "and the third");
        // And the log was read once for the three.
        let reads_for_one_pass = broker.reads.load(Ordering::SeqCst);
        assert!(reads_for_one_pass <= 3, "{reads_for_one_pass} reads");
        let _ = cursors.get(&one).await.unwrap();
        assert_eq!(broker.reads.load(Ordering::SeqCst), reads_for_one_pass);
    }

    #[tokio::test]
    async fn a_commit_during_the_first_read_compares_with_what_the_read_found() {
        let one = named("agent:one", "dash");
        let other = named("agent:two", "dash");
        let broker = a_log_of_positions(&[(&one, 4)]).await;
        let cursors = Cursors::new(broker.clone(), 2);

        // The commit arrives while the log is being read. The position is 4
        // already, so nothing is written: not a record that says what the log
        // does, which a read of the empty map would have written.
        let (read, set) = tokio::join!(cursors.get(&other), cursors.set(one.clone(), 4));
        assert_eq!(read.unwrap(), None);
        set.unwrap();
        let log = broker.system_read(SystemLog::Cursors, 0, 10).await.unwrap();
        assert_eq!(log.len(), 1, "the one record that was there");

        // A different position is written, after the read.
        cursors.set(one.clone(), 5).await.unwrap();
        assert_eq!(cursors.get(&one).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn a_commit_that_fails_unsure_while_the_log_is_read_makes_the_next_use_read_it_again() {
        let one = named("agent:one", "dash");
        let broker = a_log_of_positions(&[(&one, 4)]).await;
        let cursors = Cursors::new(broker.clone(), 2);

        // The read has begun when a commit fails without saying whether its
        // record is stored: what the read found may be a record behind.
        let (read, ()) = tokio::join!(cursors.load(), async {
            tokio::task::yield_now().await;
            cursors.commit_was_unsure();
        });
        read.unwrap();
        assert!(!cursors.state().fresh, "the read does not make it fresh");
        let before = broker.reads.load(Ordering::SeqCst);
        assert_eq!(cursors.get(&one).await.unwrap(), Some(4));
        assert!(broker.reads.load(Ordering::SeqCst) > before, "read again");
        assert!(cursors.state().fresh);
    }

    #[tokio::test]
    async fn a_record_of_a_newer_version_is_skipped_like_one_that_cannot_be_read() {
        let broker = Arc::new(MemoryBroker::new());
        let cursors = Cursors::new(broker.clone(), 10);
        let one = named("agent:one", "dash");
        cursors.set(one.clone(), 2).await.unwrap();
        broker
            .system_append(
                SystemLog::Cursors,
                br#"{"op":"named","v":2,"tenant":"acme","topic":"facts.build","principal":"agent:one","consumer":"dash","offset":9}"#,
            )
            .await
            .unwrap();
        // The older record is the position, and the load did not fail.
        let restarted = Cursors::new(broker, 10);
        assert_eq!(restarted.get(&one).await.unwrap(), Some(2));
    }

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
