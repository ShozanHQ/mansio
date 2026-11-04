//! A restarted server picks up where the last one stopped. These run only
//! against the in-process server, which they can stop and start.

use std::time::Instant;

use anyhow::Context;
use reqwest::StatusCode;

use crate::harness::{SLACK, Target, item_of, job};

const QUEUE: &str = "builds";

/// A server stopped mid-lease, and a new one started on the same broker: the
/// lease still holds, and the item is claimed again exactly once after it.
#[tokio::test]
async fn a_restarted_server_keeps_the_lease_and_claims_the_item_again_exactly_once()
-> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        eprintln!("MANSIO_E2E_URL is set: a live server cannot be restarted from here");
        return Ok(());
    }
    target.work_publish(QUEUE, &job(1)).await?;
    let started = Instant::now();
    let first = target.claim(QUEUE, "worker", SLACK).await?;

    target.stop_server().await?;
    target.start_server().await?;

    // The new server knows the lease: nobody else can claim the item.
    assert!(target.pull(QUEUE, "other").await?.is_none());

    // After the lease and the backoff it is claimed, as attempt 2.
    let wait = target.lease() + target.backoff(1) + SLACK;
    let second = target.claim(QUEUE, "other", wait).await?;
    assert!(started.elapsed() >= target.lease());
    assert_eq!(second.item.attempt, Some(2));
    assert_eq!(second.item.item_id, first.item.item_id);

    // Exactly once: nobody else gets it, and the log has one of each.
    assert!(target.pull(QUEUE, "worker").await?.is_none());
    tokio::time::sleep(target.settle()).await;
    assert!(target.pull(QUEUE, "worker").await?.is_none());
    assert_eq!(
        target.work_transitions().await?,
        ["claimed", "expired", "retried", "claimed"]
    );
    assert_eq!(target.read("work.builds").await?.len(), 2);

    // The lease the old server gave out is not the item's now.
    let (status, _) = target
        .ack(
            QUEUE,
            &item_of(&first)?,
            first.consumer.as_str(),
            &first.lease_id.to_string(),
        )
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    target.ack_claim(QUEUE, &second).await?;
    Ok(())
}

/// The facts keep a fingerprint of a lease id and not the id, so a restarted
/// server cannot say it again. A consumer that asks for the lease it holds is
/// given a new id for the same item, with the same expiry, and the old id stops
/// working.
#[tokio::test]
async fn a_consumer_that_asks_again_after_a_restart_is_given_a_new_id_for_its_lease()
-> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        return Ok(());
    }
    target.work_publish(QUEUE, &job(1)).await?;
    let first = target.claim(QUEUE, "worker", SLACK).await?;

    target.stop_server().await?;
    target.start_server().await?;

    let again = target
        .pull(QUEUE, "worker")
        .await?
        .context("the lease it holds")?;
    assert_eq!(again.item, first.item);
    assert_eq!(again.expires_at, first.expires_at);
    assert_ne!(again.lease_id, first.lease_id);
    // Nobody else is given the item, and the holder is told the same lease.
    assert!(target.pull(QUEUE, "other").await?.is_none());
    assert_eq!(target.pull(QUEUE, "worker").await?, Some(again.clone()));

    // The old id is refused, the new one is the lease, and the fingerprint of
    // it is what a second restart finds.
    let (status, _) = target
        .ack(
            QUEUE,
            &item_of(&first)?,
            first.consumer.as_str(),
            &first.lease_id.to_string(),
        )
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    target.stop_server().await?;
    target.start_server().await?;
    target.ack_claim(QUEUE, &again).await?;
    assert_eq!(
        target.work_transitions().await?,
        ["claimed", "claimed", "acked"]
    );
    Ok(())
}

/// A server that was down while the lease ran out finds the expiry waiting,
/// and publishes the retry once.
#[tokio::test]
async fn a_server_that_was_down_when_the_lease_ended_expires_it_on_start() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        return Ok(());
    }
    target.work_publish(QUEUE, &job(1)).await?;
    let first = target.claim(QUEUE, "worker", SLACK).await?;

    target.stop_server().await?;
    tokio::time::sleep(target.lease()).await;
    target.start_server().await?;

    // Nobody has asked anything of the new server, and it already moves.
    let second = target
        .claim(QUEUE, "other", target.backoff(1) + SLACK)
        .await?;
    assert_eq!(second.item.attempt, Some(2));
    assert_eq!(second.item.item_id, first.item.item_id);
    assert!(target.pull(QUEUE, "worker").await?.is_none());
    assert_eq!(
        target.work_transitions().await?,
        ["claimed", "expired", "retried", "claimed"]
    );
    Ok(())
}

/// A retry that was due, or pending, when the server stopped is published
/// once by the next.
#[tokio::test]
async fn a_failure_whose_retry_was_not_published_is_retried_once_after_a_restart()
-> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        return Ok(());
    }
    target.work_publish(QUEUE, &job(1)).await?;
    let first = target.claim(QUEUE, "worker", SLACK).await?;
    target
        .fail_claim(QUEUE, &first, "stopped before the retry")
        .await?;

    target.stop_server().await?;
    target.start_server().await?;
    assert!(target.pull(QUEUE, "worker").await?.is_none());

    let second = target
        .claim(QUEUE, "worker", target.backoff(1) + SLACK)
        .await?;
    assert_eq!(second.item.attempt, Some(2));
    assert!(target.pull(QUEUE, "other").await?.is_none());
    tokio::time::sleep(target.settle()).await;
    assert_eq!(
        target
            .pull(QUEUE, "worker")
            .await?
            .map(|claim| claim.lease_id),
        Some(second.lease_id),
        "the holder is given its lease again, and nobody is given a second item"
    );
    assert_eq!(
        target.work_transitions().await?,
        ["claimed", "failed", "retried", "claimed"]
    );
    assert_eq!(target.read("work.builds").await?.len(), 2);
    Ok(())
}

/// What was done stays done: an acked item is not delivered again, and an
/// acknowledged alert is not surfaced again.
#[tokio::test]
async fn acked_items_and_acknowledged_alerts_stay_done_after_a_restart() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        return Ok(());
    }
    target.work_publish(QUEUE, &job(1)).await?;
    let claim = target.claim(QUEUE, "worker", SLACK).await?;
    target.ack_claim(QUEUE, &claim).await?;
    let alert = target
        .alert_publish(&serde_json::json!({"source": "e2e", "type": "disk.full"}))
        .await?;
    let (status, _) = target.alert_ack(&alert.envelope.id.to_string()).await?;
    assert_eq!(status, StatusCode::OK);

    target.stop_server().await?;
    target.start_server().await?;

    let (status, _) = target.alert_ack(&alert.envelope.id.to_string()).await?;
    assert_eq!(status, StatusCode::CONFLICT);
    tokio::time::sleep(
        target.lease()
            + target.backoff(1)
            + std::time::Duration::from_secs(target.settings.alert_resurface_seconds),
    )
    .await;
    assert!(target.pull(QUEUE, "worker").await?.is_none());
    assert_eq!(target.work_transitions().await?, ["claimed", "acked"]);
    assert_eq!(target.alert_transitions().await?, ["acknowledged"]);
    Ok(())
}

/// Publishes `count` jobs and returns their item ids, in order.
async fn publish_jobs(target: &Target, count: u32) -> anyhow::Result<Vec<String>> {
    let mut ids = Vec::new();
    for n in 0..count {
        let published = target.work_publish(QUEUE, &job(n)).await?;
        let id = published
            .envelope
            .item_id
            .context("a published item has an id")?;
        ids.push(id.to_string());
    }
    Ok(ids)
}

/// Everything the engine replays here: the messages of the queue and the
/// facts about them.
async fn replayable(target: &Target) -> anyhow::Result<u64> {
    let work = target.read("work.builds").await?.len();
    let facts = target.read("facts.work").await?.len();
    Ok(u64::try_from(work + facts)?)
}

/// Pull and acknowledge until nothing is claimable, and return the items in
/// the order they came.
async fn pull_all(target: &Target, who: &str) -> anyhow::Result<Vec<String>> {
    let mut items = Vec::new();
    for n in 0.. {
        match target.pull(QUEUE, &format!("{who}-{n}")).await? {
            Some(claim) => {
                items.push(item_of(&claim)?);
                target.ack_claim(QUEUE, &claim).await?;
            }
            None => break,
        }
    }
    Ok(items)
}

/// A server that starts from a snapshot reads only the messages after it, and
/// the state it reaches is the one a read of every message gives.
#[tokio::test]
async fn a_restart_from_a_snapshot_replays_fewer_messages_and_reaches_the_same_state()
-> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        eprintln!("MANSIO_E2E_URL is set: a live server cannot be restarted from here");
        return Ok(());
    }
    let ids = publish_jobs(&target, 12).await?;
    for (n, id) in ids.iter().take(6).enumerate() {
        let claim = target.claim(QUEUE, &format!("done-{n}"), SLACK).await?;
        assert_eq!(&item_of(&claim)?, id);
        target.ack_claim(QUEUE, &claim).await?;
    }
    // A snapshot that holds all of that, then one more claim that it does not.
    let written_after = std::time::SystemTime::now();
    target.snapshot_after(written_after).await?;
    let held = target.claim(QUEUE, "held", SLACK).await?;
    assert_eq!(item_of(&held)?, ids[6]);
    // Held for long enough that it is still held when the last server starts.
    target.extend_claim(QUEUE, &held, Some(30)).await?;
    let total = replayable(&target).await?;

    // The server starts from the snapshot.
    target.stop_server().await?;
    let before = target.messages_read();
    target.start_server().await?;
    let remaining = pull_all(&target, "after-snapshot").await?;
    let from_snapshot = target.messages_read() - before;
    assert!(
        from_snapshot < total,
        "a start from a snapshot read {from_snapshot} messages; a full replay reads {total}"
    );
    // The lease it kept is not claimable; the five that were not claimed are,
    // in the order they were published.
    assert_eq!(remaining, ids[7..]);

    // The same state from a read of everything: eleven are done, and the
    // seventh is leased, so nothing is claimable.
    let total = replayable(&target).await?;
    target.stop_server().await?;
    target.forget_snapshots()?;
    let before = target.messages_read();
    target.start_server().await?;
    assert!(pull_all(&target, "after-full-replay").await?.is_empty());
    let full = target.messages_read() - before;
    assert!(
        full >= total,
        "a full replay reads all {total}: read {full}"
    );
    assert!(from_snapshot < full);
    let acked = target
        .work_transitions()
        .await?
        .iter()
        .filter(|transition| *transition == "acked")
        .count();
    assert_eq!(acked, 11);
    Ok(())
}

/// A snapshot that cannot be read does not stop the server: it starts from
/// offset 0, with a warning.
#[tokio::test]
async fn a_snapshot_that_cannot_be_read_is_replaced_by_a_full_replay() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        return Ok(());
    }
    let ids = publish_jobs(&target, 8).await?;
    let claim = target.claim(QUEUE, "done", SLACK).await?;
    target.ack_claim(QUEUE, &claim).await?;
    let total = replayable(&target).await?;

    target.stop_server().await?;
    target.put_snapshot(b"{ this is not a snapshot")?;
    let before = target.messages_read();
    target.start_server().await?;
    let remaining = pull_all(&target, "after").await?;
    let read = target.messages_read() - before;
    assert!(
        read >= total,
        "a full replay reads all {total}: read {read}"
    );
    assert_eq!(remaining, ids[1..]);
    Ok(())
}
