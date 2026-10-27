//! Two-pass tests: what a restarted server finds in a real Iggy.
//!
//! The in-process tests in `restart.rs` stop and start a server over a
//! `MemoryBroker`. These do it against Iggy, where the server's rebuild reads
//! real messages a page at a time: a first pass leaves work in the topics,
//! the CI job restarts mansio-server (`docker compose restart`), and a second
//! pass checks what the new server made of it.
//!
//! The passes are `#[ignore]`d, so a plain `cargo test` leaves them alone, and
//! the job selects them by name with `-- --ignored <name>`. Run that way they
//! fail, rather than skip, without a live server (`MANSIO_E2E_URL`) or without
//! the directory the passes keep their state in (`MANSIO_E2E_STATE_DIR`), and
//! a second pass fails without its first.
//!
//! - `restart_first_pass` and `restart_second_pass`: a queue longer than
//!   `MANSIO_POLL_MAX_COUNT`, one item claimed before the restart. After it
//!   the lease still holds, the item is retried exactly once, and every other
//!   item can be claimed once.
//! - `retention_first_pass` and `retention_second_pass`: an `alerts` topic on a
//!   short retention loses its head to Iggy's cleaner, and an alert is
//!   published after that. After the restart the server's rebuild, which
//!   reads the topic from offset 0, finds the alerts that are left.
//!
//! The first group also runs in one process, with the restart in between,
//! against the in-process server (`a_long_queue_survives_a_restart_...`), so
//! that the checks are exercised on every `cargo test`.

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail, ensure};
use mansio_core::{Envelope, LeaseId};
use mansio_server::Limits;
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::harness::{LocalSettings, SLACK, Target, item_of, job, millis, type_names};

const QUEUE: &str = "builds";
/// The consumer name before and after the restart.
const BEFORE: &str = "before-restart";
const AFTER: &str = "after-restart";
/// More items than a page, by this many.
const BEYOND_A_PAGE: u32 = 3;
/// The largest page the restart passes accept: more would be a lot of
/// publishes for a queue that only has to be longer than a page.
const LARGEST_PAGE: u32 = 100;
/// How much each of the alerts that fill a segment carries. Two of them
/// exceed the 1 MiB segment the retention step asks for, and one fits in the
/// 1 MiB a body may be.
const FILLER_BYTES: usize = 600_000;
/// How long Iggy may take to delete the head of the `alerts` topic: the
/// retention, plus up to one pass of its cleaner, plus a slow runner.
const RETENTION_WAIT: Duration = Duration::from_secs(300);
/// How often to look whether it has.
const RETENTION_LOOK: Duration = Duration::from_secs(2);

// ---- state kept between the passes -------------------------------------------

fn state_path(pass: &str) -> anyhow::Result<PathBuf> {
    let dir = std::env::var_os("MANSIO_E2E_STATE_DIR")
        .filter(|dir| !dir.is_empty())
        .context("MANSIO_E2E_STATE_DIR must name a directory for the state of the passes")?;
    Ok(PathBuf::from(dir).join(format!("{pass}.json")))
}

fn save(pass: &str, state: &Value) -> anyhow::Result<()> {
    let path = state_path(pass)?;
    std::fs::write(&path, serde_json::to_vec_pretty(state)?)
        .with_context(|| format!("writing {}", path.display()))
}

fn load(pass: &str) -> anyhow::Result<Value> {
    let path = state_path(pass)?;
    let text = std::fs::read(&path)
        .with_context(|| format!("reading {}: has the first pass run?", path.display()))?;
    Ok(serde_json::from_slice(&text)?)
}

fn text<'a>(state: &'a Value, key: &str) -> anyhow::Result<&'a str> {
    state[key]
        .as_str()
        .with_context(|| format!("{key} in the state of the first pass"))
}

fn texts(state: &Value, key: &str) -> anyhow::Result<Vec<String>> {
    state[key]
        .as_array()
        .with_context(|| format!("{key} in the state of the first pass"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .with_context(|| format!("an item of {key}"))
        })
        .collect()
}

fn now_millis() -> anyhow::Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

// ---- the restart passes --------------------------------------------------------

/// Publish a queue longer than a page of the server's, and claim the oldest
/// item. Returns what the second pass needs.
async fn restart_before(target: &Target) -> anyhow::Result<Value> {
    let page = target.settings.poll_max_count;
    ensure!(
        page <= LARGEST_PAGE,
        "MANSIO_POLL_MAX_COUNT is {page}: set it to a small number (the restart steps use 5), so that a queue longer than a page is a few publishes"
    );
    let mut items = Vec::new();
    for n in 0..page + BEYOND_A_PAGE {
        items.push(item_of_published(
            &target.work_publish(QUEUE, &job(n)).await?,
        )?);
    }
    let claim = target.claim(QUEUE, BEFORE, SLACK).await?;
    let held = item_of(&claim)?;
    ensure!(
        held == items[0],
        "the oldest item should be claimed first, not {held}"
    );
    Ok(json!({
        "tenant": target.tenant,
        "items": items,
        "held": held,
        "lease_id": claim.lease_id.to_string(),
        "expires_at": millis(&claim.expires_at)?,
    }))
}

fn item_of_published(published: &mansio_core::api::Published) -> anyhow::Result<String> {
    Ok(published
        .envelope
        .item_id
        .as_ref()
        .context("a published item has an item id")?
        .to_string())
}

/// The server has been restarted: the lease taken before it still holds, the
/// item is retried exactly once after it, and every other item is claimable
/// once.
async fn restart_after(target: &Target, state: &Value) -> anyhow::Result<()> {
    let items = texts(state, "items")?;
    let held = text(state, "held")?.to_owned();
    let expires_at = state["expires_at"]
        .as_i64()
        .context("expires_at in the state of the first pass")?;

    // Every other item is claimable once, oldest first, as before the restart:
    // the server read the queue back from the topic, past its first page. Each
    // is acknowledged as it is claimed, so that none of them comes back.
    let mut claimed = Vec::new();
    while let Some(claim) = target.pull(QUEUE, AFTER).await? {
        claimed.push(item_of(&claim)?);
        target.ack_claim(QUEUE, &claim).await?;
        ensure!(
            claimed.len() <= items.len(),
            "more claims than items: {claimed:?}"
        );
    }
    assert_eq!(
        claimed,
        items[1..],
        "every item but the one under the lease is claimed once, in order"
    );

    // The lease taken before the restart held through all of it. The item is
    // invisible, and the server knows whose lease it is: it is not the lease
    // of a stranger (403), and it has not been forgotten (409).
    assert!(target.pull(QUEUE, AFTER).await?.is_none());
    let stranger = LeaseId::generate().to_string();
    let (status, body) = target.ack(QUEUE, &held, &stranger).await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let late = now_millis()?;
    ensure!(
        late < expires_at,
        "the lease taken before the restart ended {} ms before these checks were done: raise MANSIO_LEASE_SECONDS in the step",
        late - expires_at
    );

    // After the lease and the backoff the item is claimed again, as attempt 2.
    let remaining = Duration::from_millis(u64::try_from(expires_at - late)?);
    let retried = target
        .claim(QUEUE, AFTER, remaining + target.backoff(1) + SLACK)
        .await?;
    ensure!(
        now_millis()? >= expires_at,
        "the item was claimed again before its lease ended"
    );
    assert_eq!(item_of(&retried)?, held);
    assert_eq!(retried.item.attempt, Some(2));
    assert_ne!(retried.lease_id.to_string(), text(state, "lease_id")?);
    target.ack_claim(QUEUE, &retried).await?;

    // Exactly once: nothing is left to claim, however long the server looks.
    assert!(target.pull(QUEUE, AFTER).await?.is_none());
    tokio::time::sleep(target.settle()).await;
    assert!(target.pull(QUEUE, AFTER).await?.is_none());

    // The topics say the same. Each item was claimed and acknowledged; the
    // one that was under the lease expired, was retried once and was claimed
    // and acknowledged again.
    let facts = target.read("facts.work").await?;
    for item in &items {
        let about: Vec<Envelope> = facts
            .iter()
            .filter(|fact| {
                fact.subject.as_ref().map(ToString::to_string).as_deref() == Some(item.as_str())
            })
            .cloned()
            .collect();
        let expected: &[&str] = if *item == held {
            &["claimed", "expired", "retried", "claimed", "acked"]
        } else {
            &["claimed", "acked"]
        };
        assert_eq!(type_names(&about, "mansio.work."), expected, "item {item}");
    }
    assert_eq!(
        facts.len(),
        5 + 2 * (items.len() - 1),
        "no other transition"
    );
    let queue = target.read("work.builds").await?;
    assert_eq!(queue.len(), items.len() + 1, "one retry, not two");
    let retries: Vec<&Envelope> = queue.iter().filter(|m| m.attempt == Some(2)).collect();
    assert_eq!(retries.len(), 1);
    assert_eq!(
        retries[0].item_id.as_ref().map(ToString::to_string),
        Some(held)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "run by the restart steps of the e2e job: -- --ignored restart_first_pass"]
async fn restart_first_pass() -> anyhow::Result<()> {
    let target = Target::live(None).await?;
    save("restart", &restart_before(&target).await?)
}

#[tokio::test]
#[ignore = "run by the restart steps of the e2e job: -- --ignored restart_second_pass"]
async fn restart_second_pass() -> anyhow::Result<()> {
    let state = load("restart")?;
    let target = Target::live(Some(text(&state, "tenant")?)).await?;
    restart_after(&target, &state).await
}

/// The same two passes in one process, with the restart in between, on the
/// in-process server: a queue longer than a page, a lease that holds through
/// the restart, a retry exactly once.
#[tokio::test]
async fn a_long_queue_survives_a_restart_across_pages_and_the_lease_still_holds()
-> anyhow::Result<()> {
    let settings = LocalSettings {
        limits: Limits {
            poll_default_count: 3,
            poll_max_count: 3,
            ..Limits::default()
        },
        work: mansio_server::WorkSettings {
            lease: Duration::from_secs(5),
            ..LocalSettings::default().work
        },
    };
    let mut target = Target::start_with(settings).await?;
    if !target.is_local() {
        eprintln!("MANSIO_E2E_URL is set: a live server cannot be restarted from here");
        return Ok(());
    }
    assert_eq!(target.settings.poll_max_count, 3);
    let state = restart_before(&target).await?;
    target.stop_server().await?;
    target.start_server().await?;
    restart_after(&target, &state).await
}

// ---- the retention passes ------------------------------------------------------

/// An alert body of about `FILLER_BYTES`.
fn big_alert(n: u32) -> Value {
    json!({
        "source": "e2e",
        "type": "disk.full",
        "data": {"n": n, "filler": "x".repeat(FILLER_BYTES)}
    })
}

fn small_alert(what: &str) -> Value {
    json!({"source": "e2e", "type": "disk.full", "subject": what})
}

async fn publish_alert(target: &Target, body: &Value) -> anyhow::Result<String> {
    Ok(target.alert_publish(body).await?.envelope.id.to_string())
}

/// Fill a segment of `alerts` so that Iggy rolls it, publish an alert, wait
/// until Iggy's cleaner has deleted the rolled segment, and publish another.
/// The topic was created with a segment of 1 MiB and a retention of seconds
/// (`MANSIO_IGGY_SEGMENT_SIZE_BYTES` and `MANSIO_RETENTION_ALERTS_SECONDS` of
/// the server, `IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL` of Iggy), and Iggy
/// only deletes a segment that has filled and rolled.
async fn retention_before(target: &Target) -> anyhow::Result<Value> {
    let mut old = Vec::new();
    for n in 0..2 {
        old.push(publish_alert(target, &big_alert(n)).await?);
    }
    // Acknowledged, they do not surface again while the rest takes its time.
    for id in &old {
        let (status, body) = target.alert_ack(id).await?;
        ensure!(
            status == StatusCode::OK,
            "acknowledging {id}: {status} {body}"
        );
    }
    let kept = publish_alert(target, &small_alert("kept")).await?;

    // Iggy answers an offset below the first message it still has with that
    // message, so a poll from 0 shows when the head has gone.
    let deadline = std::time::Instant::now() + RETENTION_WAIT;
    loop {
        let page = target.poll("alerts", "offset=0&count=1").await?;
        match page.messages.first() {
            Some(first) if first.offset > 0 => break,
            Some(_) => {}
            None => bail!(
                "alerts holds no message at all: Iggy answered an offset below the head of the topic with nothing"
            ),
        }
        if std::time::Instant::now() >= deadline {
            bail!(
                "Iggy still holds the first alert after {RETENTION_WAIT:?}: check MANSIO_RETENTION_ALERTS_SECONDS, MANSIO_IGGY_SEGMENT_SIZE_BYTES and IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL"
            );
        }
        tokio::time::sleep(RETENTION_LOOK).await;
    }

    let later = publish_alert(target, &small_alert("later")).await?;

    // What is left, read from the start: the alerts after the head, and none
    // of the two before it.
    let left: Vec<String> = target
        .read("alerts")
        .await?
        .iter()
        .map(|alert| alert.id.to_string())
        .collect();
    ensure!(
        left.contains(&kept) && left.contains(&later),
        "a read from offset 0 after the head was deleted lost the alerts that are left: {left:?}"
    );
    ensure!(
        !old.iter().any(|id| left.contains(id)),
        "the head was deleted, yet a read from offset 0 still shows it: {left:?}"
    );
    Ok(json!({
        "tenant": target.tenant,
        "old": old,
        "kept": kept,
        "later": later,
    }))
}

/// The server has been restarted, and rebuilt its state from the topic that
/// lost its head: the alerts that are left are known to it, each is
/// acknowledged once.
async fn retention_after(target: &Target, state: &Value) -> anyhow::Result<()> {
    for key in ["kept", "later"] {
        let id = text(state, key)?;
        let (status, body) = target.alert_ack(id).await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "the rebuilt server does not know the alert {key} ({id}): {body}"
        );
        let (status, _) = target.alert_ack(id).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{key} twice");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "run by the retention step of the e2e job: -- --ignored retention_first_pass"]
async fn retention_first_pass() -> anyhow::Result<()> {
    let target = Target::live(None).await?;
    save("retention", &retention_before(&target).await?)
}

#[tokio::test]
#[ignore = "run by the retention step of the e2e job: -- --ignored retention_second_pass"]
async fn retention_second_pass() -> anyhow::Result<()> {
    let state = load("retention")?;
    let target = Target::live(Some(text(&state, "tenant")?)).await?;
    retention_after(&target, &state).await
}
