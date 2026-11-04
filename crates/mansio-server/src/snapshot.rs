//! Snapshots of a tenant's engine state.
//!
//! Rebuilding the state reads every topic from its first message, so a start
//! takes as long as the tenant has history. A snapshot is the state at one
//! moment, with the offset of each topic it covers: a start reads the snapshot
//! and then only the messages after those offsets.
//!
//! A snapshot covers what [`crate::state`] holds and the logs do not say at a
//! glance: the open items of each queue with their phase and leases, the items
//! owed a fact, the ids of finished items, the alerts nobody acknowledged and
//! the ids of those that were. It does not cover the idempotency keys, which
//! have their own log, nor any lease id, which only its holder has.
//!
//! A snapshot is a cache of the logs and never the record. One that cannot be
//! read, is of a format this server does not know, or does not fit the topics
//! as they are now is dropped with a warning, and the state is rebuilt from
//! offset 0.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use async_trait::async_trait;
use mansio_core::{EventId, Segment, TenantId, Topic};
use serde::{Deserialize, Serialize};

use crate::state::{QueueRecord, TenantState};

/// The version of the format written now. A snapshot of another version is not
/// read.
pub(crate) const FORMAT: u32 = 1;

/// Where snapshots are kept: the newest one of each tenant.
#[async_trait]
pub(crate) trait Snapshots: Send + Sync {
    /// The bytes of the tenant's snapshot, or `None` if it has none.
    async fn read(&self, tenant: &TenantId) -> io::Result<Option<Vec<u8>>>;

    /// Replace the tenant's snapshot. A reader sees the old one or the new
    /// one, never part of either.
    async fn write(&self, tenant: &TenantId, bytes: &[u8]) -> io::Result<()>;
}

/// Snapshots in files, one per tenant, in a directory.
#[derive(Debug)]
pub(crate) struct FileSnapshots {
    dir: PathBuf,
}

impl FileSnapshots {
    pub(crate) fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn path(&self, tenant: &TenantId) -> PathBuf {
        self.dir.join(format!("{tenant}.json"))
    }
}

#[async_trait]
impl Snapshots for FileSnapshots {
    async fn read(&self, tenant: &TenantId) -> io::Result<Option<Vec<u8>>> {
        match tokio::fs::read(self.path(tenant)).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn write(&self, tenant: &TenantId, bytes: &[u8]) -> io::Result<()> {
        use tokio::io::AsyncWriteExt;

        tokio::fs::create_dir_all(&self.dir).await?;
        let path = self.path(tenant);
        let temporary = path.with_extension("json.tmp");
        let mut file = tokio::fs::File::create(&temporary).await?;
        file.write_all(bytes).await?;
        file.sync_data().await?;
        drop(file);
        tokio::fs::rename(&temporary, &path).await
    }
}

/// A tenant's state at one moment.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Snapshot {
    format: u32,
    tenant: TenantId,
    /// When it was taken, in milliseconds since the Unix epoch.
    pub(crate) taken_at: i64,
    /// For each topic it covers, the offset of the first message it does not.
    offsets: Vec<(String, u64)>,
    queues: Vec<(Segment, QueueRecord)>,
    /// Alerts nobody acknowledged, with when each was published or last
    /// surfaced.
    alerts: Vec<(EventId, i64)>,
    acknowledged_alerts: Vec<EventId>,
}

/// What a snapshot holds, taken apart.
#[derive(Debug)]
pub(crate) struct Restored {
    pub(crate) taken_at: i64,
    /// The offset each topic is to be read from.
    pub(crate) offsets: HashMap<Topic, u64>,
    pub(crate) queues: HashMap<Segment, crate::state::Queue>,
    pub(crate) alerts: HashMap<EventId, i64>,
    pub(crate) acknowledged_alerts: std::collections::HashSet<EventId>,
}

impl Snapshot {
    /// The snapshot of `state` at `now`, or `None` if the state does not know
    /// which messages it covers.
    pub(crate) fn of(tenant: &TenantId, state: &TenantState, now: i64) -> Option<Self> {
        let covered = state.covered.as_ref()?;
        let mut offsets: Vec<(String, u64)> = covered
            .iter()
            .map(|(topic, next)| (topic.to_string(), *next))
            .collect();
        offsets.sort();
        let mut queues: Vec<(Segment, QueueRecord)> = state
            .queues
            .iter()
            .map(|(name, queue)| (name.clone(), queue.to_record()))
            .collect();
        queues.sort_by(|a, b| a.0.cmp(&b.0));
        let mut alerts: Vec<(EventId, i64)> = state
            .alerts
            .iter()
            .map(|(alert, at)| (*alert, *at))
            .collect();
        alerts.sort();
        let mut acknowledged_alerts: Vec<EventId> =
            state.acknowledged_alerts.iter().copied().collect();
        acknowledged_alerts.sort();
        Some(Self {
            format: FORMAT,
            tenant: tenant.clone(),
            taken_at: now,
            offsets,
            queues,
            alerts,
            acknowledged_alerts,
        })
    }

    /// The bytes to store.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// The snapshot in `bytes`, if it is one of this format and of `tenant`.
    ///
    /// # Errors
    ///
    /// Returns what is wrong with it, to be logged.
    pub(crate) fn decode(tenant: &TenantId, bytes: &[u8]) -> Result<Restored, String> {
        let snapshot: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if snapshot.format != FORMAT {
            return Err(format!(
                "format {} of the snapshot; this server reads {FORMAT}",
                snapshot.format
            ));
        }
        if snapshot.tenant != *tenant {
            return Err(format!("it is the snapshot of tenant {}", snapshot.tenant));
        }
        let mut offsets = HashMap::new();
        for (topic, next) in snapshot.offsets {
            let topic = topic
                .parse::<Topic>()
                .map_err(|error| format!("the topic {topic:?} in it is not one: {error}"))?;
            offsets.insert(topic, next);
        }
        Ok(Restored {
            taken_at: snapshot.taken_at,
            offsets,
            queues: snapshot
                .queues
                .into_iter()
                .map(|(name, record)| (name, crate::state::Queue::from_record(record)))
                .collect(),
            alerts: snapshot.alerts.into_iter().collect(),
            acknowledged_alerts: snapshot.acknowledged_alerts.into_iter().collect(),
        })
    }
}
