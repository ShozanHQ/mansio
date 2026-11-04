//! `GET /metrics`: request counts and latency, publishes by tenant and topic,
//! and the state of the queues, scraped the way Prometheus does.

use anyhow::{Context, ensure};
use reqwest::StatusCode;
use serde_json::json;

use crate::harness::{SLACK, Target};

const QUEUE: &str = "metrics-q";

fn job(n: u32) -> serde_json::Value {
    json!({
        "source": "e2e",
        "type": "job",
        "datacontenttype": "application/json",
        "data": {"n": n}
    })
}

/// One scrape, with the admin token.
async fn scrape(target: &Target) -> anyhow::Result<String> {
    let response = target
        .http
        .get(format!("{}/metrics", target.base))
        .bearer_auth(&target.token)
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::OK,
        "GET /metrics answered {}",
        response.status()
    );
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    ensure!(
        content_type.starts_with("text/plain; version=0.0.4"),
        "content type {content_type}"
    );
    Ok(response.text().await?)
}

/// The value of the series `series` (a name and its labels as written) in a
/// scrape.
fn sample(text: &str, series: &str) -> Option<f64> {
    text.lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
        .and_then(|value| value.parse().ok())
}

fn at_least(text: &str, series: &str, least: f64) -> anyhow::Result<()> {
    let value = sample(text, series).with_context(|| format!("{series} is not in\n{text}"))?;
    ensure!(value >= least, "{series} is {value}, not {least} or more");
    Ok(())
}

/// Done criterion 2: the endpoint serves request counts and latency by route
/// and status, and publishes by tenant and topic.
#[tokio::test]
async fn metrics_count_requests_by_route_and_status_and_publishes_by_tenant_and_topic()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    target.publish("facts.metrics", &job(1)).await?;
    let (status, _) = target
        .call(
            reqwest::Method::GET,
            &target.messages_uri("facts.metrics"),
            None,
            None,
        )
        .await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Only the admin reads them.
    let (status, _) = target
        .call(
            reqwest::Method::GET,
            &format!("{}/metrics", target.base),
            None,
            None,
        )
        .await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let text = scrape(&target).await?;
    let route = "/v1/tenants/{tenant}/topics/{topic}/messages";
    at_least(
        &text,
        &format!("mansio_http_requests_total{{method=\"POST\",route=\"{route}\",status=\"201\"}}"),
        1.0,
    )?;
    at_least(
        &text,
        &format!("mansio_http_requests_total{{method=\"GET\",route=\"{route}\",status=\"401\"}}"),
        1.0,
    )?;
    at_least(
        &text,
        &format!(
            "mansio_http_request_duration_seconds_count{{method=\"POST\",route=\"{route}\",status=\"201\"}}"
        ),
        1.0,
    )?;
    at_least(
        &text,
        &format!(
            "mansio_http_request_duration_seconds_bucket{{method=\"POST\",route=\"{route}\",status=\"201\",le=\"+Inf\"}}"
        ),
        1.0,
    )?;
    at_least(
        &text,
        &format!(
            "mansio_published_messages_total{{tenant=\"{}\",topic=\"facts.metrics\"}}",
            target.tenant
        ),
        1.0,
    )?;
    // The ids in a path are not labels.
    ensure!(
        !text.contains(&format!("route=\"/v1/tenants/{}", target.tenant)),
        "a route label holds a path:\n{text}"
    );
    Ok(())
}

/// Done criterion 3: open items, active leases, expiries, retries and dead
/// letters per queue.
#[tokio::test]
async fn metrics_report_open_items_leases_expiries_retries_and_dead_letters_per_queue()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let max = target.settings.max_attempts;
    ensure!(max >= 2, "the test needs at least two attempts");
    let labels = format!("{{tenant=\"{}\",queue=\"{QUEUE}\"}}", target.tenant);

    // One item whose first lease runs out and whose later attempts fail until
    // it goes to dead.
    target.work_publish(QUEUE, &job(1)).await?;
    let _lost = target.claim(QUEUE, "w1", SLACK).await?;
    let mut claim = target
        .claim(QUEUE, "w2", target.lease() + target.backoff(1) + SLACK)
        .await?;
    for attempt in 2..max {
        target.fail_claim(QUEUE, &claim, "broke").await?;
        claim = target
            .claim(QUEUE, "w2", target.backoff(attempt) + SLACK)
            .await?;
    }
    target.fail_claim(QUEUE, &claim, "the last straw").await?;
    target
        .until("the dead letter to be counted", target.lease(), || async {
            let text = scrape(&target).await?;
            Ok(
                sample(&text, &format!("mansio_queue_dead_letters_total{labels}"))
                    .filter(|dead| *dead >= 1.0),
            )
        })
        .await?;

    // A second item, claimed and held.
    target.work_publish(QUEUE, &job(2)).await?;
    let _held = target.claim(QUEUE, "w3", SLACK).await?;
    let text = scrape(&target).await?;
    assert_eq!(
        sample(&text, &format!("mansio_queue_open_items{labels}")),
        Some(1.0),
        "{text}"
    );
    assert_eq!(
        sample(&text, &format!("mansio_queue_active_leases{labels}")),
        Some(1.0),
        "{text}"
    );
    at_least(
        &text,
        &format!("mansio_queue_lease_expiries_total{labels}"),
        1.0,
    )?;
    at_least(
        &text,
        &format!("mansio_queue_retries_total{labels}"),
        f64::from(max - 1),
    )?;
    at_least(
        &text,
        &format!("mansio_queue_dead_letters_total{labels}"),
        1.0,
    )?;
    Ok(())
}
