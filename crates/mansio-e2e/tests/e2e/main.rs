//! End-to-end tests: publish and poll through the HTTP API, and through the
//! CLI binary; work items with claim, lease, ack, fail, retry and a dead
//! topic; alerts; the order of the sink; and credentials, which the broker
//! holds to their scopes.
//!
//! With `MANSIO_E2E_URL` unset, each test starts a server in this process on
//! a `MemoryBroker`. With it set, the tests use that server (CI runs them
//! against `docker compose` and Iggy), and `MANSIO_TOKEN` must be the token
//! it accepts. See `e2e/harness.rs`.

mod alerts;
mod consumers;
mod credentials;
mod events;
mod harness;
mod ordering;
mod passes;
mod receiver;
mod restart;
mod surfaces;
mod webhooks;
mod work;

use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::OnceLock;

use anyhow::{Context, anyhow, bail, ensure};
use harness::Target;
use mansio_core::api::Published;
use mansio_core::{Envelope, EventId};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

/// Where the server said a message landed. These tests publish one message at
/// a time and expect to be told.
fn offset_of(published: &Published) -> anyhow::Result<u64> {
    published
        .offset
        .context("the server stored the message but reported no offset")
}

/// An envelope body with every attribute a client may set.
fn full_body(id: &str) -> Value {
    json!({
        "specversion": "1.0",
        "id": id,
        "source": "https://example.com/builder",
        "type": "build.finished",
        "subject": "build/42",
        "time": "2026-10-26T12:00:00.123456789Z",
        "datacontenttype": "application/json",
        "data": {"ok": true, "n": [1, 2, {"deep": null}], "s": "é"},
        "correlationid": "corr-1",
        "causationid": "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b0",
        "idempotencykey": "key-1",
        "scope": "work.builds:claim",
        "budget": "30.50 usd",
        "approvalstate": "requested",
        "recordid": "rec-9",
        "recordprevhash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "recordhash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "itemid": "item-7",
        "attempt": 2
    })
}

#[tokio::test]
async fn a_published_envelope_is_polled_back_with_every_field_intact() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let id = EventId::generate().to_string();
    let sent = full_body(&id);

    let published = target.publish("facts.builds", &sent).await?;

    // What the server was told, plus what it sets from the path and the token.
    let mut expected = sent;
    expected["tenant"] = json!(target.tenant);
    expected["topic"] = json!("facts.builds");
    expected["principalkind"] = json!("service");
    expected["principalname"] = json!("mansio");
    assert_eq!(serde_json::to_value(&published.envelope)?, expected);

    let offset = offset_of(&published)?;
    let polled = target
        .poll("facts.builds", &format!("offset={offset}"))
        .await?;
    ensure!(
        polled.messages.len() == 1,
        "expected one message, got {}",
        polled.messages.len()
    );
    let message = &polled.messages[0];
    assert_eq!(message.offset, offset);
    assert_eq!(message.envelope.id.to_string(), id);
    assert_eq!(message.envelope, published.envelope);
    assert_eq!(serde_json::to_value(&message.envelope)?, expected);
    Ok(())
}

#[tokio::test]
async fn omitted_id_and_time_are_filled_in_and_kept() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let published = target
        .publish(
            "facts.build",
            &json!({"source": "e2e", "type": "e2e.minimal"}),
        )
        .await?;
    assert!(published.envelope.time.is_some());

    let polled = target.poll("facts.build", "offset=0").await?;
    let envelopes: Vec<&Envelope> = polled.messages.iter().map(|m| &m.envelope).collect();
    assert_eq!(envelopes, vec![&published.envelope]);
    Ok(())
}

#[tokio::test]
async fn polling_continues_from_an_offset_in_publish_order() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let mut offsets = Vec::new();
    for n in 0..4 {
        let body = json!({"source": "e2e", "type": format!("e2e.n{n}")});
        offsets.push(offset_of(&target.publish("alerts", &body).await?)?);
    }
    ensure!(
        offsets.windows(2).all(|w| w[1] == w[0] + 1),
        "offsets {offsets:?} are not consecutive"
    );

    let page = target
        .poll("alerts", &format!("offset={}&count=2", offsets[1]))
        .await?;
    let types: Vec<&str> = page
        .messages
        .iter()
        .map(|m| m.envelope.event_type.as_str())
        .collect();
    assert_eq!(types, ["e2e.n1", "e2e.n2"]);

    let past_end = target
        .poll("alerts", &format!("offset={}", offsets[3] + 1))
        .await?;
    assert!(past_end.messages.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_body_carrying_a_server_set_attribute_is_refused() -> anyhow::Result<()> {
    let target = Target::start().await?;
    for name in ["principalkind", "principalname", "tenant", "topic"] {
        let mut body = json!({"source": "e2e", "type": "e2e.forged"});
        body[name] = json!("agent");
        let (status, response) = target
            .call(
                Method::POST,
                &target.messages_uri("facts.build"),
                Some(&target.token),
                Some(&body),
            )
            .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name}: {response}");
        assert_eq!(response["error"]["code"], "bad_request");
    }
    // Nothing was stored, and so there is no topic.
    assert!(target.read("facts.build").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_bad_topic_name_is_refused() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let body = json!({"source": "e2e", "type": "e2e.bad"});
    for topic in ["facts", "facts.Build", "queue.builds", "work.", "alerts.x"] {
        let (status, response) = target
            .call(
                Method::POST,
                &target.messages_uri(topic),
                Some(&target.token),
                Some(&body),
            )
            .await?;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "publish to {topic}: {response}"
        );
        let (status, _) = target
            .call(
                Method::GET,
                &target.messages_uri(topic),
                Some(&target.token),
                None,
            )
            .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "poll of {topic}");
    }
    Ok(())
}

#[tokio::test]
async fn a_request_without_the_token_is_refused() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let body = json!({"source": "e2e", "type": "e2e.anonymous"});
    for token in [None, Some("not-the-token")] {
        let (status, response) = target
            .call(
                Method::POST,
                &target.messages_uri("alerts"),
                token,
                Some(&body),
            )
            .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "publish with {token:?}");
        assert_eq!(response["error"]["code"], "unauthorized");
        let (status, _) = target
            .call(Method::GET, &target.messages_uri("alerts"), token, None)
            .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "poll with {token:?}");
    }
    // The health check is the one open route.
    let (status, _) = target
        .call(Method::GET, &format!("{}/healthz", target.base), None, None)
        .await?;
    assert_eq!(status, StatusCode::OK);
    Ok(())
}

/// The `mansio` binary, built once per test run so it is never stale.
fn cli_binary() -> anyhow::Result<PathBuf> {
    static BINARY: OnceLock<Result<PathBuf, String>> = OnceLock::new();
    BINARY
        .get_or_init(|| build_cli().map_err(|e| format!("{e:#}")))
        .clone()
        .map_err(|e| anyhow!(e))
}

fn build_cli() -> anyhow::Result<PathBuf> {
    let mut dir = std::env::current_exe()?;
    dir.pop();
    if dir.ends_with("deps") {
        dir.pop();
    }
    let profile = match dir.file_name().and_then(|name| name.to_str()) {
        Some("debug") | None => "dev".to_owned(),
        Some(other) => other.to_owned(),
    };
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let build = Command::new(cargo)
        .args([
            "build",
            "--locked",
            "--quiet",
            "--package",
            "mansio",
            "--profile",
            &profile,
        ])
        .output()
        .context("running cargo build for the mansio binary")?;
    if !build.status.success() {
        bail!(
            "building mansio failed: {}",
            String::from_utf8_lossy(&build.stderr)
        );
    }
    let binary = dir.join(format!("mansio{}", std::env::consts::EXE_SUFFIX));
    ensure!(binary.exists(), "{} was not built", binary.display());
    Ok(binary)
}

async fn run_cli(target: &Target, args: Vec<String>) -> anyhow::Result<Output> {
    let binary = cli_binary()?;
    let (url, token) = (target.base.clone(), target.token.clone());
    tokio::task::spawn_blocking(move || {
        Command::new(binary)
            .args(args)
            .env("MANSIO_URL", url)
            .env("MANSIO_TOKEN", token)
            .output()
    })
    .await?
    .context("running the mansio binary")
}

fn only_line(output: &Output) -> anyhow::Result<Value> {
    ensure!(
        output.status.success(),
        "mansio failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout.clone())?;
    let mut lines = stdout.lines();
    let line = lines
        .next()
        .ok_or_else(|| anyhow!("mansio printed nothing"))?;
    ensure!(
        lines.next().is_none(),
        "mansio printed more than one line: {stdout}"
    );
    Ok(serde_json::from_str(line)?)
}

#[tokio::test]
async fn the_cli_publishes_then_polls() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let args = |words: &[&str]| words.iter().map(|w| (*w).to_owned()).collect::<Vec<_>>();

    let published = run_cli(
        &target,
        args(&[
            "publish",
            "--tenant",
            &target.tenant,
            "--topic",
            "facts.build",
            "--type",
            "e2e.cli",
            "--source",
            "e2e",
            "--subject",
            "s1",
            "--data",
            r#"{"n":1}"#,
            "--correlation-id",
            "c1",
            "--causation-id",
            "k1",
            "--idempotency-key",
            "i1",
        ]),
    )
    .await?;
    let published = only_line(&published)?;
    assert_eq!(published["type"], "e2e.cli");
    assert_eq!(published["tenant"], json!(target.tenant));
    assert_eq!(published["principalkind"], "service");
    assert_eq!(published["data"], json!({"n": 1}));
    assert_eq!(published["correlationid"], "c1");
    assert_eq!(published["causationid"], "k1");
    assert_eq!(published["idempotencykey"], "i1");
    assert_eq!(published["subject"], "s1");

    let polled = run_cli(
        &target,
        args(&[
            "poll",
            "--tenant",
            &target.tenant,
            "--topic",
            "facts.build",
            "--offset",
            "0",
            "--count",
            "10",
        ]),
    )
    .await?;
    assert_eq!(only_line(&polled)?, published);
    Ok(())
}

#[tokio::test]
async fn the_cli_exits_non_zero_with_a_message_on_refusal() -> anyhow::Result<()> {
    let target = Target::start().await?;

    // The server refuses a count of zero.
    let output = run_cli(
        &target,
        [
            "poll",
            "--tenant",
            &target.tenant,
            "--topic",
            "alerts",
            "--count",
            "0",
        ]
        .map(str::to_owned)
        .to_vec(),
    )
    .await?;
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("refused") && stderr.contains("count must be"),
        "{stderr}"
    );
    assert!(output.stdout.is_empty());

    // The CLI refuses a bad topic before sending anything.
    let output = run_cli(
        &target,
        ["poll", "--tenant", &target.tenant, "--topic", "nope"]
            .map(str::to_owned)
            .to_vec(),
    )
    .await?;
    assert!(!output.status.success());
    Ok(())
}
