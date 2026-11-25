//! `mansio watch -- CMD`: a command run for each envelope, the place moved
//! only when it exits 0.
//!
//! The commands are `sh -c` scripts that write into a directory of their own.
//! One that must be slow waits for a file the test creates (a gate), and
//! writes its pid first, so that the test can see it run and kill it.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::mpsc::Receiver;

use anyhow::{Context, ensure};
use serde_json::{Value, json};

use crate::harness::{SLACK, Target, job};
use crate::surfaces::{command_at, lines_of};

const TOPIC: &str = "facts.build";

/// A command that waits at the gate `$1/gate`, after it has counted its run
/// in `$1/runs` and written its pid to `$1/pid`.
pub(crate) const GATED: &str =
    r#"echo run >> "$1/runs"; echo $$ > "$1/pid"; while [ ! -e "$1/gate" ]; do sleep 0.05; done"#;

/// A directory of the test's own, removed when it is dropped.
pub(crate) struct Scratch(PathBuf);

impl Scratch {
    pub(crate) fn new(target: &Target, name: &str) -> anyhow::Result<Self> {
        let tenant: String = target
            .tenant
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .collect();
        let dir = std::env::temp_dir().join(format!(
            "mansio-watcher-{}-{tenant}-{name}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        Ok(Self(dir))
    }

    /// The directory as a word for the command line.
    pub(crate) fn arg(&self) -> anyhow::Result<&str> {
        self.0
            .to_str()
            .context("the scratch directory is not UTF-8")
    }

    pub(crate) fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The lines of a file, none if it is not there yet.
fn lines(path: &Path) -> anyhow::Result<Vec<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(text.lines().map(str::to_owned).collect()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

/// Wait until the file holds at least `n` lines, and answer them.
async fn file_lines(
    target: &Target,
    path: &Path,
    n: usize,
    timeout: std::time::Duration,
) -> anyhow::Result<Vec<String>> {
    target
        .until(&format!("{n} lines in {}", path.display()), timeout, || {
            let found = lines(path).map(|found| (found.len() >= n).then_some(found));
            async move { found }
        })
        .await
}

/// The first `n` lines a child prints on its standard output.
async fn printed_lines(
    target: &Target,
    lines: &Receiver<String>,
    n: usize,
) -> anyhow::Result<Vec<String>> {
    let mut seen = Vec::new();
    target
        .until(&format!("{n} lines from the watcher"), SLACK, || {
            while let Ok(line) = lines.try_recv() {
                seen.push(line);
            }
            let done = (seen.len() >= n).then(|| seen.clone());
            async move { Ok(done) }
        })
        .await
}

/// Start the binary with its standard output piped and its standard error
/// not read: the command's output goes there, and an orphaned command may
/// hold it open.
fn spawn(
    target: &Target,
    token: &str,
    args: &[&str],
    env: &[(&str, &str)],
) -> anyhow::Result<Child> {
    Ok(command_at(&target.base, Some(token), args)?
        .envs(env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?)
}

/// Run the binary to its end.
pub(crate) async fn run(
    target: &Target,
    token: &str,
    args: &[&str],
    env: &[(&str, &str)],
) -> anyhow::Result<Output> {
    let mut command = command_at(&target.base, Some(token), args)?;
    command.envs(env.iter().copied());
    tokio::task::spawn_blocking(move || command.output())
        .await?
        .context("running the mansio binary")
}

/// Wait for a child to exit, without holding up the server in this process.
pub(crate) async fn exited(
    target: &Target,
    child: &mut Child,
    timeout: std::time::Duration,
) -> anyhow::Result<ExitStatus> {
    target
        .until("the watcher to exit", timeout, || {
            let status = child.try_wait().map_err(anyhow::Error::from);
            async move { status }
        })
        .await
}

/// Send a signal to a process.
pub(crate) fn signal(pid: &str, signal: &str) -> anyhow::Result<()> {
    let sent = Command::new("kill").args([signal, pid]).status()?;
    ensure!(sent.success(), "kill {signal} {pid} failed");
    Ok(())
}

/// Wait until a process is gone.
pub(crate) async fn gone(target: &Target, pid: &str) -> anyhow::Result<()> {
    target
        .until(&format!("process {pid} to be gone"), SLACK, || {
            let alive = Command::new("kill")
                .args(["-0", pid])
                .stderr(Stdio::null())
                .status()
                .map(|status| (!status.success()).then_some(()))
                .map_err(anyhow::Error::from);
            async move { alive }
        })
        .await
}

/// The pid a command wrote, once it has.
pub(crate) async fn pid_of(target: &Target, path: &Path) -> anyhow::Result<String> {
    let found = file_lines(target, path, 1, SLACK).await?;
    Ok(found.concat().trim().to_owned())
}

/// Where a named consumer reads next, asked under `token`.
async fn next_of(target: &Target, token: &str, topic: &str, consumer: &str) -> anyhow::Result<u64> {
    let output = run(
        target,
        token,
        &[
            "consumers",
            "get",
            "--tenant",
            &target.tenant,
            "--topic",
            topic,
            "--consumer",
            consumer,
        ],
        &[],
    )
    .await?;
    ensure!(
        output.status.success(),
        "consumers get failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let state: Value = serde_json::from_slice(&output.stdout)?;
    state["next"].as_u64().context("the answer carries next")
}

/// Publish an envelope to the topic, as JSON.
async fn publish(target: &Target, n: u32) -> anyhow::Result<Value> {
    let body = json!({"source": "e2e", "type": format!("e2e.n{n}"), "data": {"n": n}});
    Ok(serde_json::to_value(
        target.publish(TOPIC, &body).await?.envelope,
    )?)
}

fn parse(lines: &[String]) -> anyhow::Result<Vec<Value>> {
    lines
        .iter()
        .map(|line| serde_json::from_str(line).with_context(|| format!("not JSON: {line}")))
        .collect()
}

fn id_of(envelope: &Value) -> anyhow::Result<String> {
    Ok(envelope["id"].as_str().context("an id")?.to_owned())
}

#[tokio::test]
async fn the_watcher_runs_its_command_once_for_each_envelope_and_resumes_after_a_restart()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "once")?;
    let ids = dir.join("ids");
    let file = ids.to_str().context("UTF-8")?;
    let mut sent = Vec::new();
    for n in 0..3 {
        sent.push(publish(&target, n).await?);
    }
    let t = target.tenant.clone();
    let script = r#"echo "$MANSIO_EVENT_ID" >> "$1""#;
    let mut child = spawn(
        &target,
        &target.token,
        &[
            "watch",
            "--tenant",
            &t,
            "--topic",
            TOPIC,
            "--consumer",
            "runner",
            "--",
            "sh",
            "-c",
            script,
            "sh",
            file,
        ],
        &[],
    )?;
    let out = lines_of(&mut child)?;
    let printed = printed_lines(&target, &out, 3).await?;
    file_lines(&target, &ids, 3, SLACK).await?;
    signal(&child.id().to_string(), "-TERM")?;
    let status = exited(&target, &mut child, SLACK).await?;
    assert!(status.success(), "exit {status}");
    assert_eq!(next_of(&target, &target.token, TOPIC, "runner").await?, 3);
    assert_eq!(parse(&printed)?, sent, "the three envelopes, in order");

    for n in 3..5 {
        sent.push(publish(&target, n).await?);
    }
    let output = run(
        &target,
        &target.token,
        &[
            "watch",
            "--tenant",
            &t,
            "--topic",
            TOPIC,
            "--consumer",
            "runner",
            "--limit",
            "2",
            "--",
            "sh",
            "-c",
            script,
            "sh",
            file,
        ],
        &[],
    )
    .await?;
    assert!(output.status.success(), "exit {}", output.status);
    let expected: Vec<String> = sent.iter().map(id_of).collect::<anyhow::Result<_>>()?;
    assert_eq!(lines(&ids)?, expected, "five distinct ids in publish order");
    Ok(())
}

#[tokio::test]
async fn the_command_gets_the_envelope_and_not_the_token() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "env")?;
    let sent = publish(&target, 7).await?;
    let t = target.tenant.clone();
    let output = run(
        &target,
        &target.token,
        &[
            "watch",
            "--tenant",
            &t,
            "--topic",
            TOPIC,
            "--limit",
            "1",
            "--",
            "sh",
            "-c",
            r#"cat > "$1/stdin"; env > "$1/env""#,
            "sh",
            dir.arg()?,
        ],
        &[],
    )
    .await?;
    assert!(output.status.success(), "exit {}", output.status);
    let printed = String::from_utf8(output.stdout)?;
    let stdin = std::fs::read_to_string(dir.join("stdin"))?;
    assert_eq!(stdin, printed, "the envelope, as one line");
    assert_eq!(serde_json::from_str::<Value>(&stdin)?, sent);

    let env: BTreeMap<String, String> = lines(&dir.join("env"))?
        .iter()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect();
    let id = id_of(&sent)?;
    for (name, value) in [
        ("MANSIO_TENANT", t.as_str()),
        ("MANSIO_TOPIC", TOPIC),
        ("MANSIO_OFFSET", "0"),
        ("MANSIO_EVENT_ID", id.as_str()),
        ("MANSIO_EVENT_TYPE", "e2e.n7"),
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
async fn a_command_that_fails_is_run_again_and_the_place_does_not_move() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "retry")?;
    let sent = vec![publish(&target, 0).await?, publish(&target, 1).await?];
    let t = target.tenant.clone();
    // Counts its runs for each offset, and fails the first two for offset 0.
    let script = r#"runs="$1/runs.$MANSIO_OFFSET"; n=$(cat "$runs" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > "$runs"; [ "$MANSIO_OFFSET" != 0 ] || [ "$n" -ge 3 ]"#;
    let mut child = command_at(
        &target.base,
        Some(&target.token),
        &[
            "watch",
            "--tenant",
            &t,
            "--topic",
            TOPIC,
            "--consumer",
            "again",
            "--limit",
            "2",
            "--",
            "sh",
            "-c",
            script,
            "sh",
            dir.arg()?,
        ],
    )?
    .env("MANSIO_WATCH_RETRY_BASE_MILLIS", "20")
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;
    let out = lines_of(&mut child)?;
    let first = printed_lines(&target, &out, 1).await?;
    assert_eq!(
        lines(&dir.join("runs.0"))?,
        ["3"],
        "the first envelope was printed before its third run"
    );
    let status = exited(&target, &mut child, SLACK).await?;
    assert!(status.success(), "exit {status}");
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .context("piped stderr")?
        .read_to_string(&mut stderr)?;
    let rest = match 2_usize.saturating_sub(first.len()) {
        0 => Vec::new(),
        left => printed_lines(&target, &out, left).await?,
    };
    let printed: Vec<String> = first.into_iter().chain(rest).collect();
    assert_eq!(parse(&printed)?, sent);
    assert_eq!(
        lines(&dir.join("runs.0"))?,
        ["3"],
        "three runs for the first"
    );
    assert_eq!(lines(&dir.join("runs.1"))?, ["1"], "one for the next");
    assert_eq!(
        stderr
            .matches("the command exited with status 1 for offset 0; trying again in")
            .count(),
        2,
        "{stderr}"
    );
    assert_eq!(next_of(&target, &target.token, TOPIC, "again").await?, 2);
    Ok(())
}

#[tokio::test]
async fn a_watcher_that_gives_up_exits_3_and_resumes_at_the_same_envelope() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "giveup")?;
    let sent = publish(&target, 0).await?;
    publish(&target, 1).await?;
    let t = target.tenant.clone();
    let watch = |script: &str, dir: &str| -> Vec<String> {
        [
            "watch",
            "--tenant",
            &t,
            "--topic",
            TOPIC,
            "--consumer",
            "giver",
            "--limit",
            "1",
            "--",
            "sh",
            "-c",
            script,
            "sh",
            dir,
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    };
    let dir_arg = dir.arg()?;
    let failing = watch(r#"echo run >> "$1/runs"; exit 1"#, dir_arg);
    let failing: Vec<&str> = failing.iter().map(String::as_str).collect();
    let output = run(
        &target,
        &target.token,
        &failing,
        &[
            ("MANSIO_WATCH_ATTEMPTS", "2"),
            ("MANSIO_WATCH_RETRY_BASE_MILLIS", "20"),
        ],
    )
    .await?;
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    assert_eq!(lines(&dir.join("runs"))?.len(), 2, "two runs");
    assert_eq!(next_of(&target, &target.token, TOPIC, "giver").await?, 0);

    // A command that cannot be started is an error at once.
    let missing = dir.join("no-such-program");
    let output = run(
        &target,
        &target.token,
        &[
            "watch",
            "--tenant",
            &t,
            "--topic",
            TOPIC,
            "--consumer",
            "giver",
            "--",
            missing.to_str().context("UTF-8")?,
        ],
        &[],
    )
    .await?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("could not be started"),
        "{output:?}"
    );
    assert_eq!(next_of(&target, &target.token, TOPIC, "giver").await?, 0);

    let succeeding = watch(r#"cat > "$1/seen""#, dir_arg);
    let succeeding: Vec<&str> = succeeding.iter().map(String::as_str).collect();
    let output = run(&target, &target.token, &succeeding, &[]).await?;
    assert!(output.status.success(), "{output:?}");
    let seen: Value = serde_json::from_str(&std::fs::read_to_string(dir.join("seen"))?)?;
    assert_eq!(seen, sent, "the same envelope is handled first");
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout)?,
        sent,
        "and printed"
    );
    assert_eq!(next_of(&target, &target.token, TOPIC, "giver").await?, 1);
    Ok(())
}

#[tokio::test]
async fn a_watcher_killed_while_its_command_runs_runs_it_again() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "killed")?;
    publish(&target, 0).await?;
    publish(&target, 1).await?;
    let t = target.tenant.clone();
    // The first envelope passes at once; the second waits at the gate.
    let script = format!(r#"if [ "$MANSIO_OFFSET" = 0 ]; then exit 0; fi; {GATED}"#);
    let args = [
        "watch",
        "--tenant",
        &t,
        "--topic",
        TOPIC,
        "--consumer",
        "killed",
        "--limit",
        "2",
        "--",
        "sh",
        "-c",
        &script,
        "sh",
        dir.arg()?,
    ];
    let mut child = spawn(&target, &target.token, &args, &[])?;
    let out = lines_of(&mut child)?;
    printed_lines(&target, &out, 1).await?;
    let pid = pid_of(&target, &dir.join("pid")).await?;

    child.kill()?;
    child.wait()?;
    signal(&pid, "-9")?;
    gone(&target, &pid).await?;
    assert_eq!(next_of(&target, &target.token, TOPIC, "killed").await?, 1);

    std::fs::remove_file(dir.join("pid"))?;
    let mut child = spawn(&target, &target.token, &args, &[])?;
    let out = lines_of(&mut child)?;
    let again = pid_of(&target, &dir.join("pid")).await?;
    assert_ne!(again, pid, "a new run");
    assert_eq!(lines(&dir.join("runs"))?.len(), 2, "run again");
    assert_eq!(
        next_of(&target, &target.token, TOPIC, "killed").await?,
        1,
        "not moved while the command runs"
    );
    std::fs::write(dir.join("gate"), "")?;
    let printed = printed_lines(&target, &out, 1).await?;
    assert_eq!(parse(&printed)?[0]["type"], "e2e.n1");
    let at = &target;
    target
        .until("the place to move", SLACK, || async move {
            Ok((next_of(at, &at.token, TOPIC, "killed").await? == 2).then_some(()))
        })
        .await?;
    signal(&child.id().to_string(), "-TERM")?;
    exited(&target, &mut child, SLACK).await?;
    gone(&target, &again).await?;
    Ok(())
}

#[tokio::test]
async fn a_second_signal_kills_the_command() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "second")?;
    publish(&target, 0).await?;
    let t = target.tenant.clone();
    let mut child = spawn(
        &target,
        &target.token,
        &[
            "watch",
            "--tenant",
            &t,
            "--topic",
            TOPIC,
            "--consumer",
            "second",
            "--",
            "sh",
            "-c",
            GATED,
            "sh",
            dir.arg()?,
        ],
        &[],
    )?;
    let pid = pid_of(&target, &dir.join("pid")).await?;
    let watcher = child.id().to_string();

    // One signal: the watcher lets the command finish, which it does not.
    signal(&watcher, "-TERM")?;
    tokio::time::sleep(target.settle()).await;
    assert!(
        child.try_wait()?.is_none(),
        "it ended on the first signal with its command still running"
    );

    // The second kills the command and ends the watcher.
    signal(&watcher, "-TERM")?;
    let status = exited(&target, &mut child, SLACK).await?;
    assert_eq!(status.code(), Some(130), "exit {status}");
    gone(&target, &pid).await?;
    assert_eq!(next_of(&target, &target.token, TOPIC, "second").await?, 0);
    Ok(())
}

#[tokio::test]
async fn a_watcher_of_a_work_queue_runs_its_command_for_each_attempt() -> anyhow::Result<()> {
    const QUEUE: &str = "watched";
    let target = Target::start().await?;
    let dir = Scratch::new(&target, "work")?;
    let topic = format!("work.{QUEUE}");
    let watcher = target
        .issue(
            "person",
            "watcher",
            &json!([{"topic": topic, "verbs": ["subscribe"]}]),
            None,
        )
        .await?;
    target.work_publish(QUEUE, &job(1)).await?;
    let t = target.tenant.clone();
    let seen = dir.join("seen");
    let mut child = spawn(
        &target,
        &watcher.token,
        &[
            "watch",
            "--tenant",
            &t,
            "--topic",
            &topic,
            "--consumer",
            "starter",
            "--",
            "sh",
            "-c",
            r#"cat >> "$1""#,
            "sh",
            seen.to_str().context("UTF-8")?,
        ],
        &[],
    )?;
    let first = parse(&file_lines(&target, &seen, 1, SLACK).await?)?;

    let claim = target.claim(QUEUE, "worker", SLACK).await?;
    target.fail_claim(QUEUE, &claim, "try again").await?;
    let both = parse(&file_lines(&target, &seen, 2, target.backoff(1) + SLACK).await?)?;
    assert_eq!(both[0], first[0]);
    assert_eq!(first[0]["attempt"], 1);
    assert_eq!(both[1]["attempt"], 2, "the second attempt");
    assert_eq!(both[1]["itemid"], first[0]["itemid"], "of the same item");

    signal(&child.id().to_string(), "-TERM")?;
    let status = exited(&target, &mut child, SLACK).await?;
    assert!(status.success(), "exit {status}");
    assert_eq!(
        next_of(&target, &watcher.token, &topic, "starter").await?,
        2
    );
    Ok(())
}
