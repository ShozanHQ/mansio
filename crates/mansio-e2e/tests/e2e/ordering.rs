//! The order of the sink and of `facts`.

use std::sync::Arc;

use anyhow::{Context, ensure};
use mansio_core::{Envelope, EventId};
use serde_json::json;
use tokio::task::JoinSet;

use crate::harness::{SLACK, Target, job};

const QUEUE: &str = "builds";

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

/// Done criterion 5: every transition appears on `facts` and in the sink, in
/// order.
#[tokio::test]
async fn every_transition_appears_on_facts_and_in_the_sink_in_order() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let wait = target.lease() + target.backoff(2) + SLACK;
    ensure!(
        target.settings.max_attempts >= 3,
        "the test needs three attempts"
    );

    // One item, through a failure, an expiry, and an ack.
    target.work_publish(QUEUE, &job(1)).await?;
    let first = target.claim(QUEUE, "worker", SLACK).await?;
    target.fail_claim(QUEUE, &first, "first try").await?;
    let second = target.claim(QUEUE, "worker", wait).await?;
    let third = target.claim(QUEUE, "worker", wait).await?; // after the lease
    assert_eq!(
        (first.item.attempt, second.item.attempt, third.item.attempt),
        (Some(1), Some(2), Some(3))
    );
    target.ack_claim(QUEUE, &third).await?;
    // And an alert, acknowledged.
    let alert = target
        .alert_publish(&json!({"source": "e2e", "type": "disk.full"}))
        .await?;
    let (_, body) = target.alert_ack(&alert.envelope.id.to_string()).await?;
    assert_eq!(body["envelope"]["type"], "mansio.alert.acknowledged");

    let work_facts = target.read("facts.work").await?;
    assert_eq!(
        target.work_transitions().await?,
        [
            "claimed", "failed", "retried", "claimed", "expired", "retried", "claimed", "acked"
        ]
    );
    assert_eq!(target.alert_transitions().await?, ["acknowledged"]);
    let alert_facts = target.read("facts.alert").await?;
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
    // Each topic's facts are in the sink as they are on the topic.
    assert_eq!(sink_topic(&records, "facts.work"), work_facts);
    assert_eq!(sink_topic(&records, "facts.alert"), alert_facts);
    assert_eq!(
        sink_topic(&records, "work.builds"),
        target.read("work.builds").await?
    );
    assert_eq!(sink_topic(&records, "alerts"), target.read("alerts").await?);
    // And across topics: a message is recorded before the fact that says it
    // was published, and nothing is out of place.
    assert_eq!(
        shape(&records),
        [
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
            pair("alerts", "disk.full"),
            pair("facts.alert", "mansio.alert.acknowledged"),
        ]
    );
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
