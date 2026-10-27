//! The order of the sink and of `facts`.

use std::sync::Arc;

use anyhow::{Context, ensure};
use mansio_core::{Envelope, EventId};
use serde_json::json;
use tokio::task::JoinSet;

use crate::harness::{SLACK, Target, job};

const QUEUE: &str = "builds";
const OTHER: &str = "reviews";

/// `(topic, type)` of each envelope.
fn shape(envelopes: &[Envelope]) -> Vec<(String, String)> {
    envelopes
        .iter()
        .map(|e| (e.topic.to_string(), e.event_type.as_str().to_owned()))
        .collect()
}

fn pair(topic: &str, event_type: &str) -> (String, String) {
    (topic.to_owned(), event_type.to_owned())
}

/// The envelopes of one topic, in the order the sink recorded them.
fn sink_topic(records: &[Envelope], topic: &str) -> Vec<Envelope> {
    records
        .iter()
        .filter(|e| e.topic.to_string() == topic)
        .cloned()
        .collect()
}

/// The envelopes the sink recorded for any of `topics`, in order.
fn sink_family(records: &[Envelope], topics: &[&str]) -> Vec<Envelope> {
    records
        .iter()
        .filter(|e| topics.contains(&e.topic.to_string().as_str()))
        .cloned()
        .collect()
}

/// Done criterion 5: every transition appears on `facts` and in the sink, in
/// order: claimed, acked, failed, expired, retried, dead, and an alert's
/// resurfaced and acknowledged.
#[tokio::test]
async fn every_transition_appears_on_facts_and_in_the_sink_in_order() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let wait = target.lease() + target.backoff(2) + SLACK;
    let max = target.settings.max_attempts;
    ensure!(max >= 3, "the test needs three attempts");

    // An alert, which goes unacknowledged while the rest happens.
    let alert = target
        .alert_publish(&json!({"source": "e2e", "type": "disk.full"}))
        .await?;

    // One item, through a failure, an expiry, and an ack.
    target.work_publish(QUEUE, &job(1)).await?;
    let first = target.claim(QUEUE, "worker", SLACK).await?;
    target.fail_claim(QUEUE, &first, "first try").await?;
    // Each claim is by a consumer of its own: one that holds a live lease is
    // given that lease again, not the next attempt.
    let second = target.claim(QUEUE, "worker-b", wait).await?;
    let third = target.claim(QUEUE, "worker-c", wait).await?; // after the lease
    assert_eq!(
        (first.item.attempt, second.item.attempt, third.item.attempt),
        (Some(1), Some(2), Some(3))
    );
    target.ack_claim(QUEUE, &third).await?;

    // Another, in another queue, that fails every attempt and goes to dead.
    target.work_publish(OTHER, &job(2)).await?;
    for attempt in 1..=max {
        let claim = target
            .claim(OTHER, "worker", target.backoff(attempt) + SLACK)
            .await?;
        assert_eq!(claim.item.attempt, Some(attempt));
        target.fail_claim(OTHER, &claim, "no good").await?;
    }

    // The alert has been surfaced again by now; then it is acknowledged.
    target
        .until("the alert to be surfaced again", SLACK, || async {
            Ok(target
                .alert_transitions()
                .await?
                .contains(&"resurfaced".to_owned())
                .then_some(()))
        })
        .await?;
    let (_, body) = target.alert_ack(&alert.envelope.id.to_string()).await?;
    assert_eq!(body["envelope"]["type"], "mansio.alert.acknowledged");

    let work_facts = target.read("facts.work").await?;
    let mut expected = vec![
        "claimed", "failed", "retried", "claimed", "expired", "retried", "claimed", "acked",
    ];
    expected.extend(["claimed", "failed", "retried"].repeat(max as usize - 1));
    expected.extend(["claimed", "failed", "dead"]);
    assert_eq!(target.work_transitions().await?, expected);
    let alert_facts = target.read("facts.alert").await?;
    let alert_types = target.alert_transitions().await?;
    assert_eq!(alert_types.last().map(String::as_str), Some("acknowledged"));
    assert!(alert_types.len() >= 2, "{alert_types:?}");
    assert!(
        alert_types[..alert_types.len() - 1]
            .iter()
            .all(|t| t == "resurfaced"),
        "{alert_types:?}"
    );
    // Time does not run backwards along a topic.
    let times: Vec<_> = work_facts
        .iter()
        .map(|e| crate::harness::millis(e.time.as_ref().context("time")?))
        .collect::<anyhow::Result<_>>()?;
    assert!(times.windows(2).all(|w| w[0] <= w[1]), "{times:?}");

    let Some(records) = target.sink_records()? else {
        eprintln!("MANSIO_E2E_SINK_PATH is not set: the order of the sink is not checked");
        return Ok(());
    };
    // Each topic's messages are in the sink as they are on the topic.
    for topic in [
        "facts.work",
        "facts.alert",
        "work.builds",
        "work.reviews",
        "alerts",
        "dead",
    ] {
        assert_eq!(
            sink_topic(&records, topic),
            target.read(topic).await?,
            "{topic}"
        );
    }

    // And across topics, for the work: a message is recorded before the fact
    // that says it was published, and nothing is out of place.
    let work = sink_family(
        &records,
        &["work.builds", "work.reviews", "dead", "facts.work"],
    );
    let mut expected = vec![
        pair("work.builds", "job.requested"),
        pair("facts.work", "mansio.work.claimed"),
        pair("facts.work", "mansio.work.failed"),
        pair("work.builds", "job.requested"),
        pair("facts.work", "mansio.work.retried"),
        pair("facts.work", "mansio.work.claimed"),
        pair("facts.work", "mansio.work.expired"),
        pair("work.builds", "job.requested"),
        pair("facts.work", "mansio.work.retried"),
        pair("facts.work", "mansio.work.claimed"),
        pair("facts.work", "mansio.work.acked"),
        pair("work.reviews", "job.requested"),
    ];
    for _ in 1..max {
        expected.extend([
            pair("facts.work", "mansio.work.claimed"),
            pair("facts.work", "mansio.work.failed"),
            pair("work.reviews", "job.requested"),
            pair("facts.work", "mansio.work.retried"),
        ]);
    }
    expected.extend([
        pair("facts.work", "mansio.work.claimed"),
        pair("facts.work", "mansio.work.failed"),
        pair("dead", "job.requested"),
        pair("facts.work", "mansio.work.dead"),
    ]);
    assert_eq!(shape(&work), expected);

    // And for the alert: published, surfaced again, acknowledged.
    let alerts = shape(&sink_family(&records, &["alerts", "facts.alert"]));
    assert_eq!(alerts.first(), Some(&pair("alerts", "disk.full")));
    assert_eq!(
        alerts.last(),
        Some(&pair("facts.alert", "mansio.alert.acknowledged"))
    );
    assert!(
        alerts[1..alerts.len() - 1]
            .iter()
            .all(|p| *p == pair("facts.alert", "mansio.alert.resurfaced")),
        "{alerts:?}"
    );
    assert_eq!(sink_topic(&records, "facts.alert"), alert_facts);
    Ok(())
}

/// Concurrent durable publishes reach the sink in the order they reach the
/// topic.
#[tokio::test]
async fn concurrent_publishes_reach_the_sink_in_the_order_of_the_topic() -> anyhow::Result<()> {
    let target = Arc::new(Target::start().await?);
    let mut publishes = JoinSet::new();
    for n in 0..40 {
        let target = Arc::clone(&target);
        publishes.spawn(async move {
            target
                .publish(
                    "facts.build",
                    &json!({"source": "e2e", "type": "e2e.concurrent", "data": {"n": n}}),
                )
                .await
        });
    }
    while let Some(published) = publishes.join_next().await {
        published??;
    }

    let topic = target.read("facts.build").await?;
    assert_eq!(topic.len(), 40);
    let ids: std::collections::HashSet<EventId> = topic.iter().map(|e| e.id).collect();
    assert_eq!(ids.len(), 40);
    if let Some(records) = target.sink_records()? {
        assert_eq!(sink_topic(&records, "facts.build"), topic);
    }
    Ok(())
}
