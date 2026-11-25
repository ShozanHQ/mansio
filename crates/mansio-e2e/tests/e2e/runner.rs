//! `mansio work run -- CMD`: one work item claimed, a command run for it with
//! the lease held while it runs, and the item acked or failed by how the
//! command ended.
//!
//! The commands are `sh -c` scripts that write into a directory of their own,
//! as in watcher.rs. One that must be slow writes its pid first and waits at a
//! gate file the test creates.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::process::{Child, ExitStatus, Stdio};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use mansio_core::{Envelope, Timestamp};
use serde_json::{Value, json};

use crate::harness::{DataValue, SLACK, Target, job, millis};
use crate::surfaces::{command_at, lines_of};
use crate::watcher::{GATED, Scratch, exited, gone, pid_of, run, signal};

const QUEUE: &str = "jobs";

/// A runner started in the background: what it prints, and the file its
/// standard error goes to. The file is not a pipe, which a command it left
/// behind would hold open.
struct Runner {
    child: Child,
    out: Receiver<String>,
    stderr: PathBuf,
}

impl Runner {
    /// What the runner wrote on its standard error so far.
    fn stderr(&self) -> String {
        std::fs::read_to_string(&self.stderr).unwrap_or_default()
    }

    /// Wait for the runner to exit, without holding up the server in this
    /// process.
    async fn ended(&mut self, target: &Target, timeout: Duration) -> anyhow::Result<ExitStatus> {
        let status = exited(target, &mut self.child, timeout).await;
        status.with_context(|| format!("the runner's stderr: {}", self.stderr()))
    }

    /// Everything the runner printed, once its standard output has closed.
    async fn printed(&self, target: &Target) -> anyhow::Result<Vec<Value>> {
        let mut seen = Vec::new();
        let lines = target
            .until("the runner's output to end", SLACK, || {
                let done = loop {
                    match self.out.try_recv() {
                        Ok(line) => seen.push(line),
                        Err(TryRecvError::Empty) => break None,
                        Err(TryRecvError::Disconnected) => break Some(seen.clone()),
                    }
                };
                async move { Ok(done) }
            })
            .await?;
        lines
            .iter()
            .map(|line| serde_json::from_str(line).with_context(|| format!("not JSON: {line}")))
            .collect()
    }
}

/// Start `mansio` in the background, its standard output piped and its
/// standard error written to `<name>.stderr` in the scratch directory.
fn start(
    target: &Target,
    token: &str,
    dir: &Scratch,
    name: &str,
    args: &[&str],
    env: &[(&str, &str)],
) -> anyhow::Result<Runner> {
    let stderr = dir.join(&format!("{name}.stderr"));
    let mut child = command_at(&target.base, Some(token), args)?
        .envs(env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(File::create(&stderr)?))
        .spawn()?;
    let out = lines_of(&mut child)?;
    Ok(Runner { child, out, stderr })
}

/// The words of `work run` for the queue under `consumer`, the command last.
fn work_run<'a>(tenant: &'a str, consumer: &'a str, command: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec![
        "work",
        "run",
        "--tenant",
        tenant,
        "--queue",
        QUEUE,
        "--consumer",
        consumer,
        "--",
    ];
    args.extend(command);
    args
}

/// Milliseconds since the epoch, by this machine's clock.
fn now_millis() -> anyhow::Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

/// A member of a fact's `data`.
fn data_of(envelope: &Envelope, name: &str) -> Value {
    envelope
        .data
        .as_ref()
        .map_or(Value::Null, |data| data.value()[name].clone())
}

/// The facts of `facts.work` of one type, as `failed`.
async fn facts_of(target: &Target, kind: &str) -> anyhow::Result<Vec<Envelope>> {
    let wanted = format!("mansio.work.{kind}");
    Ok(target
        .read("facts.work")
        .await?
        .into_iter()
        .filter(|envelope| envelope.event_type.as_str() == wanted)
        .collect())
}

/// The reason of the one `failed` fact.
async fn failed_reason(target: &Target) -> anyhow::Result<String> {
    let failed = facts_of(target, "failed").await?;
    anyhow::ensure!(failed.len() == 1, "{} failed facts", failed.len());
    data_of(&failed[0], "reason")
        .as_str()
        .map(str::to_owned)
        .context("a failed fact has a reason")
}

/// The environment a command wrote with `env`.
fn env_of(path: &std::path::Path) -> anyhow::Result<BTreeMap<String, String>> {
    Ok(std::fs::read_to_string(path)?
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect())
}

#[tokio::test]
async fn the_runner_holds_the_lease_while_its_command_runs_and_acks_on_success()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "run-held")?;
    target.work_publish(QUEUE, &job(1)).await?;
    let t = target.tenant.clone();
    let script = format!(r#"cat > "$1/stdin"; env > "$1/env"; {GATED}"#);
    let mut runner = start(
        &target,
        &target.token,
        &dir,
        "runner",
        &work_run(&t, "w1", &["sh", "-c", &script, "sh", dir.arg()?]),
        &[],
    )?;
    let pid = pid_of(&target, &dir.join("pid")).await?;

    // Where the claim's lease first ended.
    let at = &target;
    let first_end = target
        .until("a claimed fact", SLACK, || async move {
            let claimed = facts_of(at, "claimed").await?;
            let Some(first) = claimed.first() else {
                return Ok(None);
            };
            let end: Timestamp = serde_json::from_value(data_of(first, "expires_at"))?;
            Ok(Some(millis(&end)?))
        })
        .await?;
    target
        .until("an extended fact", target.lease() + SLACK, || async move {
            Ok((!facts_of(at, "extended").await?.is_empty()).then_some(()))
        })
        .await?;
    let settle = i64::try_from(target.settle().as_millis())?;
    target
        .until(
            "the claim's first end to pass",
            target.lease() + SLACK,
            || {
                let passed = now_millis().map(|now| (now > first_end + settle).then_some(()));
                async move { passed }
            },
        )
        .await?;
    assert!(
        target.pull(QUEUE, "other").await?.is_none(),
        "another consumer was given the item"
    );
    assert!(
        facts_of(&target, "expired").await?.is_empty(),
        "the lease expired while the command ran"
    );

    std::fs::write(dir.join("gate"), "")?;
    let status = runner.ended(&target, SLACK).await?;
    assert_eq!(status.code(), Some(0), "exit {status}: {}", runner.stderr());
    let printed = runner.printed(&target).await?;
    assert_eq!(printed.len(), 1, "{printed:?}");
    assert_eq!(printed[0]["type"], "mansio.work.acked", "{printed:?}");

    let transitions = target.work_transitions().await?;
    let (first, rest) = transitions.split_first().context("facts")?;
    let (last, between) = rest.split_last().context("more facts")?;
    assert_eq!(first, "claimed", "{transitions:?}");
    assert_eq!(last, "acked", "{transitions:?}");
    assert!(
        !between.is_empty() && between.iter().all(|kind| kind == "extended"),
        "{transitions:?}"
    );

    let item_id = printed[0]["itemid"].as_str().context("an item id")?;
    let stdin = std::fs::read_to_string(dir.join("stdin"))?;
    assert!(
        stdin.ends_with('\n') && stdin.lines().count() == 1,
        "one line: {stdin:?}"
    );
    let item: Value = serde_json::from_str(&stdin)?;
    assert_eq!(item["itemid"], item_id, "{item}");
    assert_eq!(item["type"], "job.requested", "{item}");
    assert_eq!(item["data"], json!({"n": 1}), "{item}");
    assert!(
        item.get("lease_id").is_none(),
        "the claim reached the command"
    );

    let env = env_of(&dir.join("env"))?;
    for (name, value) in [
        ("MANSIO_TENANT", t.as_str()),
        ("MANSIO_QUEUE", QUEUE),
        ("MANSIO_ITEM_ID", item_id),
        ("MANSIO_ATTEMPT", "1"),
    ] {
        assert_eq!(env.get(name).map(String::as_str), Some(value), "{name}");
    }
    assert!(
        !env.contains_key("MANSIO_TOKEN"),
        "the token reached the command"
    );
    gone(&target, &pid).await?;
    Ok(())
}

#[tokio::test]
async fn a_command_that_fails_fails_the_item() -> anyhow::Result<()> {
    let target = Target::start().await?;
    target.work_publish(QUEUE, &job(1)).await?;
    let t = target.tenant.clone();
    let output = run(
        &target,
        &target.token,
        &work_run(&t, "w1", &["sh", "-c", "exit 7"]),
        &[],
    )
    .await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(3),
        "exit {}: {stderr}",
        output.status
    );
    let printed: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(printed["type"], "mansio.work.failed", "{printed}");
    let reason = failed_reason(&target).await?;
    assert_eq!(reason, "the command exited with status 7");
    Ok(())
}

#[tokio::test]
async fn no_item_runs_nothing() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "run-none")?;
    let t = target.tenant.clone();
    let output = run(
        &target,
        &target.token,
        &work_run(
            &t,
            "w1",
            &["sh", "-c", r#"touch "$1/ran""#, "sh", dir.arg()?],
        ),
        &[],
    )
    .await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(0),
        "exit {}: {stderr}",
        output.status
    );
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
    let command = ["sh", "-c", GATED, "sh", dir.arg()?];
    let mut first = start(
        &target,
        &target.token,
        &dir,
        "a",
        &work_run(&t, "a", &command),
        &[],
    )?;
    let pid = pid_of(&target, &dir.join("pid")).await?;
    first.child.kill()?;
    first.child.wait()?;
    signal(&pid, "-9")?;
    gone(&target, &pid).await?;

    std::fs::write(dir.join("gate"), "")?;
    let wait = target.lease() + target.backoff(1) + target.settle() + SLACK;
    let seconds = wait.as_secs().to_string();
    let mut args = vec!["work", "run", "--wait", &seconds];
    args.extend(&work_run(&t, "b", &command)[2..]);
    let mut second = start(&target, &target.token, &dir, "b", &args, &[])?;
    let status = second.ended(&target, wait + SLACK).await?;
    assert_eq!(status.code(), Some(0), "exit {status}: {}", second.stderr());
    let printed = second.printed(&target).await?;
    assert_eq!(printed.len(), 1, "{printed:?}");
    assert_eq!(printed[0]["type"], "mansio.work.acked", "{printed:?}");
    assert_eq!(printed[0]["attempt"], 2, "{printed:?}");

    let transitions: Vec<String> = target
        .work_transitions()
        .await?
        .into_iter()
        .filter(|kind| kind != "extended")
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
    let mut runner = start(
        &target,
        &issued.token,
        &dir,
        "runner",
        &work_run(&t, "w1", &["sh", "-c", GATED, "sh", dir.arg()?]),
        &[],
    )?;
    let credential_ends: Timestamp =
        serde_json::from_value(issued.principal["expires_at"].clone())?;
    let credential_ends = millis(&credential_ends)?;
    let pid = pid_of(&target, &dir.join("pid")).await?;

    let status = runner.ended(&target, target.lease() * 2 + SLACK).await?;
    let ended_at = now_millis()?;
    assert_eq!(status.code(), Some(4), "exit {status}: {}", runner.stderr());
    assert!(
        ended_at < credential_ends,
        "the runner ended at {ended_at}, after its credential at {credential_ends}: {}",
        runner.stderr()
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
    let mut runner = start(
        &target,
        &target.token,
        &dir,
        "runner",
        &work_run(&t, "w1", &["sh", "-c", GATED, "sh", dir.arg()?]),
        &[("MANSIO_RUN_STOP_GRACE_SECONDS", "1")],
    )?;
    let pid = pid_of(&target, &dir.join("pid")).await?;

    // The runner only: the command does not get it.
    signal(&runner.child.id().to_string(), "-TERM")?;
    let status = runner
        .ended(&target, Duration::from_secs(1) + SLACK)
        .await?;
    assert_eq!(status.code(), Some(3), "exit {status}: {}", runner.stderr());
    gone(&target, &pid).await?;
    let printed = runner.printed(&target).await?;
    assert_eq!(printed.len(), 1, "{printed:?}");
    assert_eq!(printed[0]["type"], "mansio.work.failed", "{printed:?}");
    assert_eq!(failed_reason(&target).await?, "stopped");
    Ok(())
}
