//! A publish with an idempotency key the topic has already seen inside the
//! window is answered with the first, and publishes nothing.
//!
//! The window and the bound are the server's (`MANSIO_IDEMPOTENCY_WINDOW_SECONDS`
//! and `MANSIO_IDEMPOTENCY_MAX_KEYS`), read from `GET /v1/settings`. The tests
//! that wait out the window or fill the bound need them small, as the
//! in-process server and the e2e job set them, and say so when they are not.
//!
//! `idempotency_first_pass` and `idempotency_second_pass` are the restart
//! across a real Iggy: the job restarts mansio-server between them with a
//! window long enough to outlast the restart. The same check runs in one
//! process against the in-process server in
//! `a_restarted_server_answers_a_repeated_key_with_the_first`.

use std::time::Duration;

use anyhow::{Context, bail, ensure};
use mansio_core::api::Published;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::harness::Target;

/// The most keys a test that fills the bound will publish.
const LARGEST_BOUND: u64 = 200;
/// The longest window a test that waits it out will wait.
const LONGEST_WINDOW: u64 = 30;

fn body(key: &str, n: u32) -> Value {
    json!({
        "source": "e2e",
        "type": "build.finished",
        "subject": format!("build/{n}"),
        "idempotencykey": key,
        "data": {"n": n}
    })
}

/// Publish and say what the server answered.
async fn send(
    target: &Target,
    topic: &str,
    body: &Value,
) -> anyhow::Result<(StatusCode, Published)> {
    let (status, response) = target
        .authed(Method::POST, &target.messages_uri(topic), Some(body))
        .await?;
    ensure!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "publish answered {status}: {response}"
    );
    Ok((status, serde_json::from_value(response)?))
}

#[tokio::test]
async fn a_repeated_key_is_answered_with_the_first_and_publishes_nothing() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let (status, first) = send(&target, "facts.build", &body("k1", 1)).await?;
    assert_eq!(status, StatusCode::CREATED);

    // The same key with another body, as a retry after a timeout is not
    // always byte for byte the same request: the first still answers.
    for n in 2..4 {
        let (status, again) = send(&target, "facts.build", &body("k1", n)).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(again, first);
    }
    let stored = target.read("facts.build").await?;
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0], first.envelope);

    // The key belongs to the topic: another topic, and another key, publish.
    let (status, other) = send(&target, "facts.deploy", &body("k1", 1)).await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_ne!(other.envelope.id, first.envelope.id);
    let (status, next) = send(&target, "facts.build", &body("k2", 1)).await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(next.offset, first.offset.map(|offset| offset + 1));

    // A publish without a key is never a repeat.
    let plain = json!({"source": "e2e", "type": "build.finished"});
    for _ in 0..2 {
        let (status, _) = send(&target, "facts.build", &plain).await?;
        assert_eq!(status, StatusCode::CREATED);
    }
    assert_eq!(target.read("facts.build").await?.len(), 4);
    Ok(())
}

#[tokio::test]
async fn a_key_belongs_to_its_tenant() -> anyhow::Result<()> {
    let one = Target::start().await?;
    let (status, first) = send(&one, "facts.build", &body("k1", 1)).await?;
    assert_eq!(status, StatusCode::CREATED);
    // Another tenant of the same server, the same key.
    let uri = format!(
        "{}/v1/tenants/{}-other/topics/facts.build/messages",
        one.base, one.tenant
    );
    let (status, response) = one.authed(Method::POST, &uri, Some(&body("k1", 1))).await?;
    assert_eq!(status, StatusCode::CREATED, "{response}");
    let theirs: Published = serde_json::from_value(response)?;
    assert_ne!(theirs.envelope.id, first.envelope.id);
    Ok(())
}

#[tokio::test]
async fn an_alert_with_a_repeated_key_is_answered_with_the_first() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let alert = json!({"source": "e2e", "type": "disk.full", "idempotencykey": "a1"});
    let first = target.alert_publish(&alert).await?;
    let uri = target.tenant_uri("alerts");
    let (status, again) = target.authed(Method::POST, &uri, Some(&alert)).await?;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(serde_json::from_value::<Published>(again)?, first);
    assert_eq!(target.read("alerts").await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_key_is_forgotten_when_its_window_ends() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let window = target.settings.idempotency_window_seconds;
    if window > LONGEST_WINDOW {
        anyhow::bail!(
            "the window is {window} s, too long to wait out: run with MANSIO_IDEMPOTENCY_WINDOW_SECONDS of {LONGEST_WINDOW} or less"
        );
    }
    let (_, first) = send(&target, "facts.build", &body("k1", 1)).await?;
    tokio::time::sleep(Duration::from_secs(window) + Duration::from_millis(500)).await;
    let (status, second) = send(&target, "facts.build", &body("k1", 2)).await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_ne!(second.envelope.id, first.envelope.id);
    // The new publish starts a new window.
    let (status, again) = send(&target, "facts.build", &body("k1", 3)).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again, second);
    assert_eq!(target.read("facts.build").await?.len(), 2);
    Ok(())
}

/// The oldest key goes when the bound is passed; the newest are kept.
#[tokio::test]
async fn the_oldest_key_is_forgotten_when_the_bound_is_passed() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let bound = target.settings.idempotency_max_keys;
    if bound > LARGEST_BOUND {
        anyhow::bail!(
            "the bound is {bound}, too many keys to fill: run with MANSIO_IDEMPOTENCY_MAX_KEYS of {LARGEST_BOUND} or less"
        );
    }
    let bound = u32::try_from(bound)?;
    for n in 0..=bound {
        let (status, _) = send(&target, "facts.build", &body(&format!("k{n}"), n)).await?;
        assert_eq!(status, StatusCode::CREATED);
    }
    // k0 was the first of bound + 1 keys, so it is gone, and publishes again
    // (which forgets k1); the newest is still remembered.
    let (status, _) = send(&target, "facts.build", &body(&format!("k{bound}"), 0)).await?;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&target, "facts.build", &body("k0", 0)).await?;
    assert_eq!(status, StatusCode::CREATED);
    Ok(())
}

#[tokio::test]
async fn a_restarted_server_answers_a_repeated_key_with_the_first() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        eprintln!("MANSIO_E2E_URL is set: a live server cannot be restarted from here");
        return Ok(());
    }
    let (_, first) = send(&target, "facts.build", &body("k1", 1)).await?;

    target.stop_server().await?;
    target.start_server().await?;

    let (status, again) = send(&target, "facts.build", &body("k1", 2)).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again, first);
    assert_eq!(target.read("facts.build").await?.len(), 1);

    // A second restart finds the same.
    target.stop_server().await?;
    target.start_server().await?;
    let (status, again) = send(&target, "facts.build", &body("k1", 3)).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again, first);
    Ok(())
}

// ---- the restart passes against Iggy -------------------------------------------

/// Publish with a key, as the first pass does. Returns what the second needs.
async fn restart_before(target: &Target) -> anyhow::Result<Value> {
    let (status, first) = send(target, "facts.build", &body("restart-key", 1)).await?;
    ensure!(
        status == StatusCode::CREATED,
        "the first publish answered {status}"
    );
    Ok(json!({"tenant": target.tenant, "first": first}))
}

/// The server has been restarted: the key is still the first publish's.
async fn restart_after(target: &Target, state: &Value) -> anyhow::Result<()> {
    let first: Published =
        serde_json::from_value(state["first"].clone()).context("first in the state")?;
    let window = target.settings.idempotency_window_seconds;
    if window < 120 {
        bail!(
            "MANSIO_IDEMPOTENCY_WINDOW_SECONDS is {window}: set it to at least 120, so that the key outlasts the restart"
        );
    }
    let (status, again) = send(target, "facts.build", &body("restart-key", 2)).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "the restarted server forgot the key"
    );
    assert_eq!(again, first);
    assert_eq!(target.read("facts.build").await?.len(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "run by the restart steps of the e2e job: -- --ignored idempotency_first_pass"]
async fn idempotency_first_pass() -> anyhow::Result<()> {
    let target = Target::live(None).await?;
    crate::passes::save("idempotency", &restart_before(&target).await?)
}

#[tokio::test]
#[ignore = "run by the restart steps of the e2e job: -- --ignored idempotency_second_pass"]
async fn idempotency_second_pass() -> anyhow::Result<()> {
    let state = crate::passes::load("idempotency")?;
    let tenant = state["tenant"].as_str().context("tenant in the state")?;
    let target = Target::live(Some(tenant)).await?;
    restart_after(&target, &state).await
}
