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
//!   `MANSIO_POLL_MAX_COUNT`, one item claimed before the restart and its lease
//!   extended to twice the lease setting. After it the lease still holds, past
//!   the end it first had, until the end it was extended to; the item is then
//!   retried exactly once, and every other item can be claimed once. Two
//!   credentials are issued before it and one revoked; after it the registry
//!   of principals the new server rebuilt from its log in Iggy knows the one
//!   that was kept (it publishes and polls as its principal), refuses the
//!   revoked one with 401, and lists just the kept principal. A server whose
//!   registry did not load would answer every credential with 503, and the
//!   second pass fails.
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
use mansio_core::api::Polled;
use mansio_core::{Envelope, LeaseId};
use mansio_server::Limits;
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::harness::{LocalSettings, SLACK, Target, extended_to, item_of, job, millis, type_names};

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

/// Keep the state of a pass for the next. It holds the credentials the first
/// pass issued, so it is readable by its owner alone, and the directory is the
/// job's, outside the checkout.
fn save(pass: &str, state: &Value) -> anyhow::Result<()> {
    use std::io::Write;
    let path = state_path(pass)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options
        .open(&path)
        .and_then(|mut file| file.write_all(&serde_json::to_vec_pretty(state)?))
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

/// Publish a queue longer than a page of the server's, claim the oldest item
/// and extend its lease to twice the lease setting from now. Returns what the
/// second pass needs.
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
    ensure!(
        target.lease_max() >= target.lease() * 2,
        "MANSIO_LEASE_MAX_SECONDS is below twice MANSIO_LEASE_SECONDS: the lease cannot be extended"
    );
    let extended = target
        .extend_claim(QUEUE, &claim, Some(target.lease().as_secs() * 2))
        .await?;
    let extended_end = extended_to(&extended)?;
    ensure!(
        extended_end > millis(&claim.expires_at)?,
        "the extension should end the lease later than the claim did"
    );
    Ok(json!({
        "tenant": target.tenant,
        "items": items,
        "held": held,
        "lease_id": claim.lease_id.to_string(),
        "expires_at": millis(&claim.expires_at)?,
        "extended_to": extended_end,
        "credentials": credentials_before(target).await?,
    }))
}

/// The topic the two credentials of the restart passes publish to and poll.
const CREDENTIAL_TOPIC: &str = "facts.restart";
const KEPT: &str = "restart-kept";
const REVOKED: &str = "restart-revoked";

/// Issue two credentials, each of which publishes once, and revoke one. The
/// answer is what the second pass needs to use them, and they are the secrets
/// of a stack that is thrown away: it goes to the state directory, never to
/// the repository.
async fn credentials_before(target: &Target) -> anyhow::Result<Value> {
    let scopes = json!([{ "topic": CREDENTIAL_TOPIC, "verbs": ["publish", "subscribe"] }]);
    let kept = target.issue("service", KEPT, &scopes, None).await?;
    let revoked = target.issue("service", REVOKED, &scopes, None).await?;
    for credential in [&kept, &revoked] {
        let (status, body) = credential.publish(CREDENTIAL_TOPIC).await?;
        ensure!(
            status == StatusCode::CREATED,
            "a new credential's publish answered {status}: {body}"
        );
    }
    let (status, body) = target.revoke(&revoked.id).await?;
    ensure!(
        status == StatusCode::OK,
        "principals_revoke answered {status}: {body}"
    );
    let (status, body) = revoked.publish(CREDENTIAL_TOPIC).await?;
    ensure!(
        status == StatusCode::UNAUTHORIZED,
        "the revoked credential's publish answered {status}: {body}"
    );
    Ok(json!({
        "kept": { "id": kept.id, "token": kept.token },
        "revoked": { "id": revoked.id, "token": revoked.token },
    }))
}

/// The server has been restarted, and rebuilt the registry of principals from
/// its log: the credential that was kept is its principal's, the revoked one
/// is refused, and the list holds the one.
async fn credentials_after(target: &Target, state: &Value) -> anyhow::Result<()> {
    let credential = |which: &str| -> anyhow::Result<crate::harness::Issued> {
        Ok(target.adopt(text(&state[which], "id")?, text(&state[which], "token")?))
    };
    let (kept, revoked) = (credential("kept")?, credential("revoked")?);

    // Not a 503, which is what a registry that did not load answers.
    let (status, body) = kept.publish(CREDENTIAL_TOPIC).await?;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "the kept credential publishes: {body}"
    );
    let (status, body) = kept.poll(CREDENTIAL_TOPIC).await?;
    assert_eq!(status, StatusCode::OK, "the kept credential polls: {body}");
    let polled: Polled = serde_json::from_value(body)?;
    let wrote: Vec<String> = polled
        .messages
        .iter()
        .map(|stored| stored.envelope.principal.to_string())
        .collect();
    assert_eq!(
        wrote,
        [
            format!("service:{KEPT}"),
            format!("service:{REVOKED}"),
            format!("service:{KEPT}")
        ],
        "each message carries the principal of the credential that sent it, before and after the restart"
    );

    let (status, body) = revoked.publish(CREDENTIAL_TOPIC).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    let (status, body) = revoked.poll(CREDENTIAL_TOPIC).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    let listed = target.principals().await?;
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0]["id"], kept.id.as_str());
    assert_eq!(listed[0]["name"], KEPT);
    // The revocation is in the registry too, not only in Iggy.
    let (status, _) = target.revoke(&revoked.id).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

fn item_of_published(published: &mansio_core::api::Published) -> anyhow::Result<String> {
    Ok(published
        .envelope
        .item_id
        .as_ref()
        .context("a published item has an item id")?
        .to_string())
}

/// The server has been restarted: the lease taken before it still holds, past
/// the end it first had and until the end it was extended to, the item is
/// retried exactly once after that, and every other item is claimable once.
async fn restart_after(target: &Target, state: &Value) -> anyhow::Result<()> {
    let items = texts(state, "items")?;
    let held = text(state, "held")?.to_owned();
    let first_end = state["expires_at"]
        .as_i64()
        .context("expires_at in the state of the first pass")?;
    let expires_at = state["extended_to"]
        .as_i64()
        .context("extended_to in the state of the first pass")?;

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
    let (status, body) = target.ack(QUEUE, &held, BEFORE, &stranger).await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let late = now_millis()?;
    ensure!(
        late < expires_at,
        "the lease taken before the restart and extended ended {} ms before these checks were done: raise MANSIO_LEASE_SECONDS in the step",
        late - expires_at
    );

    // The lease was extended, and the new server read that back: past the end
    // it first had, the item is still invisible, and the holder is told the
    // end it was extended to. It is given a new id, as a lease read back from
    // the facts has to be, and the old one stops working.
    if late < first_end {
        tokio::time::sleep(Duration::from_millis(u64::try_from(first_end - late)?)).await;
    }
    tokio::time::sleep(target.settle()).await;
    ensure!(
        now_millis()? > first_end && now_millis()? < expires_at,
        "the checks of a lease past its first end and before its extended end need a lease of several seconds (MANSIO_LEASE_SECONDS in the step)"
    );
    assert!(target.pull(QUEUE, AFTER).await?.is_none());
    let held_again = target
        .pull(QUEUE, BEFORE)
        .await?
        .context("the lease the holder still has")?;
    assert_eq!(item_of(&held_again)?, held);
    assert_eq!(millis(&held_again.expires_at)?, expires_at);
    assert_ne!(held_again.lease_id.to_string(), text(state, "lease_id")?);
    let (status, body) = target
        .ack(QUEUE, &held, BEFORE, text(state, "lease_id")?)
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "the old id: {body}");
    let late = now_millis()?;
    ensure!(
        late < expires_at,
        "the extended lease ended during the checks"
    );

    // After the extended end and the backoff the item is claimed again, as
    // attempt 2.
    let remaining = Duration::from_millis(u64::try_from(expires_at - late)?);
    let retried = target
        .claim(QUEUE, AFTER, remaining + target.backoff(1) + SLACK)
        .await?;
    ensure!(
        now_millis()? >= expires_at,
        "the item was claimed again before its extended lease ended"
    );
    assert_eq!(item_of(&retried)?, held);
    assert_eq!(retried.item.attempt, Some(2));
    assert_ne!(retried.lease_id.to_string(), text(state, "lease_id")?);
    assert_ne!(retried.lease_id, held_again.lease_id);
    target.ack_claim(QUEUE, &retried).await?;

    // Exactly once: nothing is left to claim, however long the server looks.
    assert!(target.pull(QUEUE, AFTER).await?.is_none());
    tokio::time::sleep(target.settle()).await;
    assert!(target.pull(QUEUE, AFTER).await?.is_none());

    // The topics say the same. Each item was claimed and acknowledged; the
    // one that was under the lease was extended, given a new lease id after the
    // restart, expired, was retried once and was claimed and acknowledged
    // again.
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
            &[
                "claimed", "extended", "claimed", "expired", "retried", "claimed", "acked",
            ]
        } else {
            &["claimed", "acked"]
        };
        assert_eq!(type_names(&about, "mansio.work."), expected, "item {item}");
    }
    assert_eq!(
        facts.len(),
        7 + 2 * (items.len() - 1),
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

    credentials_after(target, &state["credentials"]).await
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
        ..LocalSettings::default()
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
