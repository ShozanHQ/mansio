//! A restarted server picks up where the last one stopped. These run only
//! against the in-process server, which they can stop and start.

use std::time::Instant;

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
    assert!(target.pull(QUEUE, "worker").await?.is_none());

    // After the lease and the backoff it is claimed, as attempt 2.
    let wait = target.lease() + target.backoff(1) + SLACK;
    let second = target.claim(QUEUE, "other", wait).await?;
    assert!(started.elapsed() >= target.lease());
    assert_eq!(second.item.attempt, Some(2));
    assert_eq!(second.item.item_id, first.item.item_id);

    // Exactly once: nobody gets it again, and the log has one of each.
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
    assert!(target.pull(QUEUE, "worker").await?.is_none());
    tokio::time::sleep(target.settle()).await;
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
