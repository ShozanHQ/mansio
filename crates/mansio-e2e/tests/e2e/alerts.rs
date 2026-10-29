//! Alerts: publish, acknowledge once, surface again until acknowledged.

use std::time::Instant;

use anyhow::Context;
use mansio_core::EventId;
use reqwest::StatusCode;
use serde_json::json;

use crate::harness::{PRINCIPAL, SLACK, Target, millis};

fn alert() -> serde_json::Value {
    json!({
        "source": "e2e",
        "type": "disk.full",
        "subject": "host-7",
        "datacontenttype": "application/json",
        "data": {"free_gb": 0}
    })
}

/// Done criterion 4: an alert is acknowledged once and the ack names the
/// principal.
#[tokio::test]
async fn an_alert_is_acknowledged_once_and_the_ack_names_the_principal() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let published = target.alert_publish(&alert()).await?;
    let id = published.envelope.id.to_string();
    assert_eq!(published.envelope.topic.to_string(), "alerts");
    assert_eq!(published.envelope.principal.to_string(), PRINCIPAL);
    assert_eq!(
        target.read("alerts").await?,
        vec![published.envelope.clone()]
    );

    let (status, body) = target.alert_ack(&id).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let acked: mansio_core::api::Published = serde_json::from_value(body)?;
    assert_eq!(
        acked.envelope.event_type.as_str(),
        "mansio.alert.acknowledged"
    );
    assert_eq!(acked.envelope.topic.to_string(), "facts.alert");
    assert_eq!(acked.envelope.principal.to_string(), PRINCIPAL);
    assert_eq!(
        acked.envelope.subject.as_ref().map(|s| s.to_string()),
        Some(id.clone())
    );
    let data = acked.envelope.data.as_ref().context("data")?;
    assert_eq!(data["alert_id"], json!(id));
    assert_eq!(data["acknowledged_by"], PRINCIPAL);
    assert!(data["acknowledged_at"].is_string());

    // Once only.
    let (status, body) = target.alert_ack(&id).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "conflict");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or("")
            .contains("already acknowledged")
    );
    // An alert id is used once: a resend cannot reopen it.
    let mut again = alert();
    again["id"] = json!(id);
    let (status, response) = target
        .authed(
            reqwest::Method::POST,
            &target.tenant_uri("alerts"),
            Some(&again),
        )
        .await?;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    let (status, _) = target.alert_ack(&id).await?;
    assert_eq!(status, StatusCode::CONFLICT);
    // Not an alert, and not an id.
    let (status, _) = target.alert_ack(&EventId::generate().to_string()).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = target.alert_ack("nope").await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let facts = target.read("facts.alert").await?;
    assert_eq!(facts.len(), 1, "one acknowledgement, not three");
    assert_eq!(facts[0], acked.envelope);
    Ok(())
}

/// An alert published through the generic route is an alert too.
#[tokio::test]
async fn an_alert_published_to_the_alerts_topic_can_be_acknowledged() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let published = target.publish("alerts", &alert()).await?;
    let (status, body) = target.alert_ack(&published.envelope.id.to_string()).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    Ok(())
}

/// An alert unacknowledged after the interval gets one `resurfaced` fact per
/// interval until it is acknowledged.
#[tokio::test]
async fn an_unacknowledged_alert_is_surfaced_again_each_interval_until_acknowledged()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let interval = std::time::Duration::from_secs(target.settings.alert_resurface_seconds);
    let id = target.alert_publish(&alert()).await?.envelope.id;

    // Nothing is surfaced inside the first interval.
    let published_at = Instant::now();
    assert!(target.alert_transitions().await?.is_empty());

    let two = target
        .until("two resurfaced facts", interval * 2 + SLACK, || async {
            let facts = target.read("facts.alert").await?;
            Ok((facts.len() >= 2).then_some(facts))
        })
        .await?;
    assert!(
        published_at.elapsed() >= interval,
        "the first came after {:?}, inside the interval of {interval:?}",
        published_at.elapsed()
    );
    for fact in &two[..2] {
        assert_eq!(fact.event_type.as_str(), "mansio.alert.resurfaced");
        assert_eq!(
            fact.subject.as_ref().map(|s| s.to_string()),
            Some(id.to_string())
        );
        assert_eq!(
            fact.data.as_ref().context("data")?["alert_id"],
            json!(id.to_string())
        );
    }
    // One per interval: the second is a whole interval after the first.
    let gap = millis(two[1].time.as_ref().context("time")?)?
        - millis(two[0].time.as_ref().context("time")?)?;
    assert!(
        gap >= i64::try_from(interval.as_millis())?,
        "resurfaced {gap} ms apart, inside the interval of {interval:?}"
    );

    // Acknowledged, it is surfaced no more.
    let (status, _) = target.alert_ack(&id.to_string()).await?;
    assert_eq!(status, StatusCode::OK);
    let before = target.alert_transitions().await?;
    assert_eq!(before.last().map(String::as_str), Some("acknowledged"));
    tokio::time::sleep(interval + target.settle()).await;
    assert_eq!(target.alert_transitions().await?, before);
    Ok(())
}
