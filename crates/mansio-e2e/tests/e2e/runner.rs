//! `mansio work run -- CMD`: one item claimed, the command run for it while
//! the lease is held, and the item acked or failed by how the command ends.
//!
//! The commands are `sh -c` scripts that write into a directory of their own,
//! their pid first, and wait at a gate file where they must be slow. Every
//! wait is read from the target, so that the tests hold against a live server
//! with its own settings.

use std::collections::BTreeMap;
use std::process::Child;
use std::sync::mpsc::Receiver;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use mansio_core::Timestamp;
use serde_json::{Value, json};

use crate::harness::{DataValue, SLACK, Target, job, millis};
use crate::surfaces::lines_of;
use crate::watcher::{GATED, Scratch, exited, gone, pid_of, run, signal, spawn};

const QUEUE: &str = "jobs";

/// A command that writes its pid to `$1/pid`, its standard input to
/// `$1/item` and its environment to `$1/env`, and waits at the gate
/// `$1/gate`.
const RECORDED: &str = r#"echo $$ > "$1/pid"; cat > "$1/item"; env > "$1/env"; while [ ! -e "$1/gate" ]; do sleep 0.05; done"#;

/// The words of `work run` for `consumer`, with `extra` flags before the
/// `--` and the script after it, given the scratch directory as `$1`.
fn work_run(t: &str, consumer: &str, extra: &[&str], script: &str, dir: &str) -> Vec<String> {
    let mut words = vec![
        "work",
        "run",
        "--tenant",
        t,
        "--queue",
        QUEUE,
        "--consumer",
        consumer,
    ];
    words.extend(extra);
    words.extend(["--", "sh", "-c", script, "sh", dir]);
    words.into_iter().map(str::to_owned).collect()
}

fn words(owned: &[String]) -> Vec<&str> {
    owned.iter().map(String::as_str).collect()
}

/// The test's clock, in milliseconds since the epoch.
fn now() -> anyhow::Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

fn as_millis(duration: Duration) -> anyhow::Result<i64> {
    Ok(i64::try_from(duration.as_millis())?)
}

/// The milliseconds of an RFC 3339 time in a JSON value.
fn millis_in(value: &Value) -> anyhow::Result<i64> {
    let text = value.as_str().context("a time as text")?;
    millis(&text.parse::<Timestamp>()?)
}

/// The facts of the tenant's work, as each one's transition and data.
async fn facts(target: &Target) -> anyhow::Result<Vec<(String, Value)>> {
    Ok(target
        .read("facts.work")
        .await?
        .iter()
        .map(|fact| {
            (
                fact.event_type.as_str().replace("mansio.work.", ""),
                fact.data.as_ref().map_or(Value::Null, |data| data.value()),
            )
        })
        .collect())
}

/// The reason of the first `failed` fact.
async fn failed_reason(target: &Target) -> anyhow::Result<Value> {
    let facts = facts(target).await?;
    let (_, data) = facts
        .iter()
        .find(|(transition, _)| transition == "failed")
        .with_context(|| format!("a failed fact in {facts:?}"))?;
    Ok(data["reason"].clone())
}

/// At least `n` lines the runner printed, once it has printed them.
async fn printed(
    target: &Target,
    lines: &Receiver<String>,
    n: usize,
) -> anyhow::Result<Vec<Value>> {
    let mut seen = Vec::new();
    let found = target
        .until(&format!("{n} lines from the runner"), SLACK, || {
            while let Ok(line) = lines.try_recv() {
                seen.push(line);
            }
            let done = (seen.len() >= n).then(|| seen.clone());
            async move { Ok(done) }
        })
        .await?;
    found
        .iter()
        .map(|line| serde_json::from_str(line).with_context(|| format!("not JSON: {line}")))
        .collect()
}

/// The environment a command wrote with `env`.
fn env_of(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

/// Wait for the runner to exit within `timeout`, and answer its status code.
async fn code_of(
    target: &Target,
    child: &mut Child,
    timeout: Duration,
) -> anyhow::Result<Option<i32>> {
    Ok(exited(target, child, timeout).await?.code())
}

#[tokio::test]
async fn the_runner_holds_the_lease_while_its_command_runs_and_acks_on_success()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "run-holds")?;
    target.work_publish(QUEUE, &job(1)).await?;
    let t = target.tenant.clone();
    let args = work_run(&t, "w1", &[], RECORDED, dir.arg()?);
    let mut child = spawn(&target, &target.token, &words(&args), &[])?;
    let out = lines_of(&mut child)?;
    pid_of(&target, &dir.join("pid")).await?;

    let at = &target;
    let first_end = target
        .until("a claimed fact", SLACK, || async move {
            let facts = facts(at).await?;
            facts
                .iter()
                .find(|(transition, _)| transition == "claimed")
                .map(|(_, data)| millis_in(&data["expires_at"]))
                .transpose()
        })
        .await?;
    let settle = as_millis(target.settle())?;
    target
        .until(
            "an extension, and the first end to pass",
            target.lease() + SLACK,
            || async move {
                let extended = facts(at).await?.iter().any(|(t, _)| t == "extended");
                Ok((extended && now()? >= first_end + settle).then_some(()))
            },
        )
        .await?;
    assert!(
        target.pull(QUEUE, "other").await?.is_none(),
        "another consumer was given the item"
    );
    let transitions = target.work_transitions().await?;
    assert!(
        !transitions.iter().any(|t| t == "expired"),
        "{transitions:?}"
    );

    std::fs::write(dir.join("gate"), "")?;
    assert_eq!(code_of(&target, &mut child, SLACK).await?, Some(0));
    let printed = printed(&target, &out, 1).await?;
    assert_eq!(printed.len(), 1, "{printed:?}");
    assert_eq!(printed[0]["type"], "mansio.work.acked", "{printed:?}");

    let transitions = target.work_transitions().await?;
    assert!(transitions.len() >= 3, "{transitions:?}");
    assert_eq!(transitions.first().map(String::as_str), Some("claimed"));
    assert_eq!(transitions.last().map(String::as_str), Some("acked"));
    assert!(
        transitions[1..transitions.len() - 1]
            .iter()
            .all(|t| t == "extended"),
        "{transitions:?}"
    );

    let stdin = std::fs::read_to_string(dir.join("item"))?;
    assert!(
        stdin.ends_with('\n') && stdin.lines().count() == 1,
        "one line: {stdin:?}"
    );
    let item: Value = serde_json::from_str(&stdin)?;
    assert_eq!(item["data"], json!({"n": 1}), "{item}");
    assert_eq!(item["attempt"], 1, "{item}");
    assert!(
        item.get("lease_id").is_none(),
        "the claim reached the command"
    );
    let env = env_of(&std::fs::read_to_string(dir.join("env"))?);
    let id = item["itemid"].as_str().context("an item id")?;
    for (name, value) in [
        ("MANSIO_TENANT", t.as_str()),
        ("MANSIO_QUEUE", QUEUE),
        ("MANSIO_ITEM_ID", id),
        ("MANSIO_ATTEMPT", "1"),
    ] {
        assert_eq!(env.get(name).map(String::as_str), Some(value), "{name}");
    }
    assert!(
        !env.contains_key("MANSIO_TOKEN"),
        "the token reached the command"
    );
    Ok(())
}

#[tokio::test]
async fn a_command_that_fails_fails_the_item() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "run-fails")?;
    target.work_publish(QUEUE, &job(1)).await?;
    let t = target.tenant.clone();
    let args = work_run(&t, "w1", &[], "exit 7", dir.arg()?);
    let output = run(&target, &target.token, &words(&args), &[]).await?;
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    let printed: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(printed["type"], "mansio.work.failed", "{printed}");
    let reason = failed_reason(&target).await?;
    assert!(
        reason.as_str().is_some_and(|reason| reason.contains('7')),
        "{reason}"
    );
    assert_eq!(reason, "the command exited with status 7");
    Ok(())
}

#[tokio::test]
async fn no_item_runs_nothing() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "run-none")?;
    let t = target.tenant.clone();
    let args = work_run(&t, "w1", &[], r#"touch "$1/ran""#, dir.arg()?);
    let output = run(&target, &target.token, &words(&args), &[]).await?;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    assert!(!dir.join("ran").exists(), "the command ran");
    Ok(())
}

#[tokio::test]
async fn a_killed_runner_leaves_the_item_to_be_claimed_again() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "run-killed")?;
    target.work_publish(QUEUE, &job(1)).await?;
    let t = target.tenant.clone();
    let first = work_run(&t, "a", &[], RECORDED, dir.arg()?);
    let mut child = spawn(&target, &target.token, &words(&first), &[])?;
    let pid = pid_of(&target, &dir.join("pid")).await?;
    child.kill()?;
    child.wait()?;
    signal(&pid, "-9")?;
    gone(&target, &pid).await?;

    std::fs::remove_file(dir.join("pid"))?;
    std::fs::write(dir.join("gate"), "")?;
    let wait = target.lease() + target.backoff(1) + target.settle() + SLACK;
    let seconds = wait.as_secs().to_string();
    let second = work_run(&t, "b", &["--wait", &seconds], RECORDED, dir.arg()?);
    let mut child = spawn(&target, &target.token, &words(&second), &[])?;
    let out = lines_of(&mut child)?;
    assert_eq!(code_of(&target, &mut child, wait + SLACK).await?, Some(0));
    let printed = printed(&target, &out, 1).await?;
    assert_eq!(printed[0]["type"], "mansio.work.acked", "{printed:?}");
    let env = env_of(&std::fs::read_to_string(dir.join("env"))?);
    assert_eq!(env.get("MANSIO_ATTEMPT").map(String::as_str), Some("2"));
    let item: Value = serde_json::from_str(&std::fs::read_to_string(dir.join("item"))?)?;
    assert_eq!(item["attempt"], 2, "{item}");

    let transitions: Vec<String> = target
        .work_transitions()
        .await?
        .into_iter()
        .filter(|t| t != "extended")
        .collect();
    assert_eq!(
        transitions,
        ["claimed", "expired", "retried", "claimed", "acked"]
    );
    Ok(())
}

#[tokio::test]
async fn a_lease_that_can_grow_no_further_ends_the_command() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "run-longest")?;
    target.work_publish(QUEUE, &job(1)).await?;
    let issued = target
        .issue(
            "agent",
            "runner-1",
            &json!([{"topic": format!("work.{QUEUE}"), "verbs": ["claim"]}]),
            Some(target.lease().as_secs() * 2),
        )
        .await?;
    let t = target.tenant.clone();
    let args = work_run(&t, "w1", &[], GATED, dir.arg()?);
    let mut child = spawn(&target, &issued.token, &words(&args), &[])?;
    let credential_end = millis_in(&issued.principal["expires_at"])?;
    let pid = pid_of(&target, &dir.join("pid")).await?;

    let code = code_of(&target, &mut child, target.lease() * 2 + SLACK).await?;
    let ended = now()?;
    assert_eq!(code, Some(4));
    assert!(
        ended < credential_end,
        "it ended at {ended}, after the credential at {credential_end}"
    );
    gone(&target, &pid).await?;
    assert_eq!(
        failed_reason(&target).await?,
        "the command ran past the longest lease"
    );
    Ok(())
}

#[tokio::test]
async fn a_signal_ends_the_run_and_fails_the_item() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "run-signal")?;
    target.work_publish(QUEUE, &job(1)).await?;
    let t = target.tenant.clone();
    let args = work_run(&t, "w1", &[], GATED, dir.arg()?);
    let mut child = spawn(
        &target,
        &target.token,
        &words(&args),
        &[("MANSIO_RUN_STOP_GRACE_SECONDS", "1")],
    )?;
    let pid = pid_of(&target, &dir.join("pid")).await?;

    signal(&child.id().to_string(), "-TERM")?;
    let code = code_of(&target, &mut child, Duration::from_secs(1) + SLACK).await?;
    assert_eq!(code, Some(3));
    gone(&target, &pid).await?;
    assert_eq!(failed_reason(&target).await?, "stopped");
    Ok(())
}
