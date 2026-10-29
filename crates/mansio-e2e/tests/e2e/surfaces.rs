//! The command line and the MCP server, as an agent or a person runs them:
//! the `mansio` binary against a server.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::mpsc::{self, Receiver as Lines};
use std::time::Duration;

use anyhow::{Context, anyhow, ensure};
use serde_json::{Value, json};

use crate::harness::{SLACK, Target, job};
use crate::receiver::{Receiver, Reply};

/// A command line for the binary against this target, with the token given
/// (or none: the environment has no `MANSIO_TOKEN` then).
fn command(target: &Target, token: Option<&str>, args: &[&str]) -> anyhow::Result<Command> {
    command_at(&target.base, token, args)
}

/// The same against any address.
fn command_at(base: &str, token: Option<&str>, args: &[&str]) -> anyhow::Result<Command> {
    let mut command = Command::new(crate::cli_binary()?);
    command
        .args(args)
        .env_remove("MANSIO_TOKEN")
        .env("MANSIO_URL", base)
        // The binary looks again every 50 ms, and tries a failed request again
        // after 20 ms.
        .env("MANSIO_POLL_INTERVAL_MILLIS", "50")
        .env("MANSIO_RETRY_BACKOFF_MILLIS", "20");
    if let Some(token) = token {
        command.env("MANSIO_TOKEN", token);
    }
    Ok(command)
}

/// Run the binary to its end.
async fn run(target: &Target, token: Option<&str>, args: &[&str]) -> anyhow::Result<Output> {
    run_at(&target.base, token, args).await
}

/// The same against any address.
async fn run_at(base: &str, token: Option<&str>, args: &[&str]) -> anyhow::Result<Output> {
    let mut command = command_at(base, token, args)?;
    tokio::task::spawn_blocking(move || command.output())
        .await?
        .context("running the mansio binary")
}

/// What the binary printed, a JSON value to a line, if it succeeded.
fn printed(output: &Output) -> anyhow::Result<Vec<Value>> {
    ensure!(
        output.status.success(),
        "mansio failed ({}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout.clone())?
        .lines()
        .map(|line| serde_json::from_str(line).with_context(|| format!("not JSON: {line}")))
        .collect()
}

/// A run that must have printed one line.
async fn one(target: &Target, token: &str, args: &[&str]) -> anyhow::Result<Value> {
    let mut lines = printed(&run(target, Some(token), args).await?)?;
    ensure!(lines.len() == 1, "{args:?} printed {} lines", lines.len());
    Ok(lines.remove(0))
}

/// A run that must have been refused: no success, nothing on stdout, and a
/// reason that says `why`.
async fn refused(
    target: &Target,
    token: Option<&str>,
    args: &[&str],
    why: &str,
) -> anyhow::Result<()> {
    refused_at(&target.base, token, args, why).await
}

/// The same against any address.
async fn refused_at(
    base: &str,
    token: Option<&str>,
    args: &[&str],
    why: &str,
) -> anyhow::Result<()> {
    let output = run_at(base, token, args).await?;
    ensure!(!output.status.success(), "{args:?} was not refused");
    ensure!(
        output.stdout.is_empty(),
        "{args:?} printed {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(
        stderr.contains(why),
        "{args:?}: {stderr:?} does not say {why:?}"
    );
    Ok(())
}

/// Done criterion 2: the commands refuse without a credential, and call
/// nothing.
#[tokio::test]
async fn every_command_refuses_without_a_token_and_calls_nothing() -> anyhow::Result<()> {
    let target = Target::start().await?;
    // A server of our own that would see any call.
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let elsewhere = format!("http://{}", listener.local_addr()?);

    let t = target.tenant.clone();
    let id = "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3";
    let commands: Vec<Vec<&str>> = vec![
        vec![
            "publish",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--type",
            "t",
            "--source",
            "s",
        ],
        vec!["poll", "--tenant", &t, "--topic", "facts.build"],
        vec!["watch", "--tenant", &t, "--topic", "facts.build"],
        vec![
            "work", "publish", "--tenant", &t, "--queue", "builds", "--type", "t", "--source", "s",
        ],
        vec![
            "work",
            "pull",
            "--tenant",
            &t,
            "--queue",
            "builds",
            "--consumer",
            "w",
        ],
        vec![
            "work",
            "ack",
            "--tenant",
            &t,
            "--queue",
            "builds",
            "--item",
            "i",
            "--consumer",
            "w",
            "--lease-id",
            id,
        ],
        vec![
            "work",
            "fail",
            "--tenant",
            &t,
            "--queue",
            "builds",
            "--item",
            "i",
            "--consumer",
            "w",
            "--lease-id",
            id,
            "--reason",
            "r",
        ],
        vec![
            "alerts", "publish", "--tenant", &t, "--type", "t", "--source", "s",
        ],
        vec!["alerts", "ack", "--tenant", &t, "--alert", id],
        vec![
            "principals",
            "create",
            "--kind",
            "agent",
            "--name",
            "a",
            "--tenant",
            &t,
            "--scopes",
            r#"[{"topic":"alerts","verbs":["ack"]}]"#,
        ],
        vec!["principals", "list"],
        vec!["principals", "revoke", "--id", id],
        vec![
            "webhooks",
            "create",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--endpoint",
            "http://127.0.0.1:1/",
        ],
        vec!["webhooks", "list"],
        vec!["webhooks", "delete", "--id", id],
        vec![
            "consumers",
            "get",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--consumer",
            "c",
        ],
        vec![
            "consumers",
            "commit",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--consumer",
            "c",
            "--offset",
            "1",
        ],
        vec!["mcp"],
    ];
    for args in &commands {
        refused_at(&elsewhere, None, args, "MANSIO_TOKEN").await?;
        // An empty token is none.
        let mut with_empty = args.clone();
        with_empty.extend(["--token", ""]);
        refused_at(&elsewhere, None, &with_empty, "MANSIO_TOKEN").await?;
    }
    assert!(
        matches!(listener.accept(), Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock),
        "something connected to the server"
    );

    // Help needs no token.
    for args in [
        &["--help"][..],
        &["work", "pull", "--help"][..],
        &["webhooks", "--help"][..],
    ] {
        let output = run_at(&elsewhere, None, args).await?;
        assert!(output.status.success(), "{args:?}");
        assert!(String::from_utf8_lossy(&output.stdout).contains("Usage"));
    }

    // A token the server does not know is refused by the server.
    refused(&target, Some("not-a-token"), &commands[4], "401").await?;
    refused(&target, Some("not-a-token"), &commands[8], "unauthorized").await?;
    Ok(())
}

/// Done criterion 1: `watch` streams from a cursor, and resumes after a
/// restart with nothing missed and nothing twice.
#[tokio::test]
async fn watch_resumes_with_the_same_consumer_with_nothing_missed_and_nothing_twice()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let t = target.tenant.clone();
    let mut sent = Vec::new();
    for n in 0..5 {
        let body = json!({"source": "e2e", "type": format!("e2e.n{n}")});
        sent.push(target.publish("facts.build", &body).await?.envelope);
    }
    let watch = |limit: &str| {
        vec![
            "watch",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--consumer",
            "dash",
            "--limit",
            limit,
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>()
    };
    let go = |args: Vec<String>| {
        let target = &target;
        async move {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            printed(&run(target, Some(&target.token), &args).await?)
        }
    };

    // Stopped after two.
    let first = go(watch("2")).await?;
    assert_eq!(first.len(), 2);
    let state = one(
        &target,
        &target.token,
        &[
            "consumers",
            "get",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--consumer",
            "dash",
        ],
    )
    .await?;
    assert_eq!(state, json!({"consumer": "dash", "offset": 1, "next": 2}));

    // Three more, and started again: the remaining six, in order.
    for n in 5..8 {
        let body = json!({"source": "e2e", "type": format!("e2e.n{n}")});
        sent.push(target.publish("facts.build", &body).await?.envelope);
    }
    let rest = go(watch("6")).await?;
    assert_eq!(rest.len(), 6);
    let seen: Vec<Value> = first.into_iter().chain(rest).collect();
    let expected: Vec<Value> = sent
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<_, _>>()?;
    assert_eq!(seen, expected, "every envelope, once, in order");

    // A consumer that is caught up waits for what comes.
    let child = command(
        &target,
        Some(&target.token),
        &[
            "watch",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--consumer",
            "dash",
            "--limit",
            "1",
        ],
    )?
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let late = target
        .publish("facts.build", &json!({"source": "e2e", "type": "e2e.late"}))
        .await?
        .envelope;
    let output = tokio::task::spawn_blocking(move || child.wait_with_output()).await??;
    assert_eq!(printed(&output)?, vec![serde_json::to_value(&late)?]);

    // Without a consumer it reads from the offset given and keeps no place.
    let plain = printed(
        &run(
            &target,
            Some(&target.token),
            &[
                "watch",
                "--tenant",
                &t,
                "--topic",
                "facts.build",
                "--from",
                "6",
                "--limit",
                "2",
            ],
        )
        .await?,
    )?;
    assert_eq!(plain, expected[6..8].to_vec());
    // A consumer and a start together are refused.
    refused(
        &target,
        Some(&target.token),
        &[
            "watch",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--consumer",
            "x",
            "--from",
            "1",
        ],
        "cannot be used together",
    )
    .await?;
    // A topic nobody has published to is not found.
    refused(
        &target,
        Some(&target.token),
        &["watch", "--tenant", &t, "--topic", "facts.nobody"],
        "404",
    )
    .await?;
    Ok(())
}

/// The lines a child prints, as they come.
fn lines_of(child: &mut Child) -> anyhow::Result<Lines<String>> {
    let stdout = child.stdout.take().context("piped stdout")?;
    let (sender, lines) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { return };
            if sender.send(line).is_err() {
                return;
            }
        }
    });
    Ok(lines)
}

/// A server that answers `watch` with one message and never answers the commit
/// of it, so that `watch` is stuck finishing its line.
async fn server_that_never_answers_a_commit() -> anyhow::Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut chunk = [0_u8; 1024];
                while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                    match socket.read(&mut chunk).await {
                        Ok(n) if n > 0 => head.extend_from_slice(&chunk[..n]),
                        _ => return,
                    }
                }
                let head = String::from_utf8_lossy(&head).into_owned();
                let line = head.lines().next().unwrap_or_default().to_owned();
                let body = if line.starts_with("GET /v1/tenants/t/topics/facts.build/consumers/") {
                    r#"{"consumer":"c","next":0}"#
                } else if line.starts_with("GET /v1/tenants/t/topics/facts.build/messages") {
                    r#"{"messages":[{"offset":0,"envelope":{"id":"first"}}]}"#
                } else {
                    // The commit: read, and say nothing.
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    return;
                };
                let reply = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(reply.as_bytes()).await;
            });
        }
    });
    Ok(base)
}

/// A second signal while `watch` finishes its line ends it at once, without the
/// commit it was waiting for, and not with a success.
#[cfg(unix)]
#[tokio::test]
async fn a_second_signal_ends_a_watch_that_is_stuck_finishing_its_line() -> anyhow::Result<()> {
    let base = server_that_never_answers_a_commit().await?;
    let mut child = command_at(
        &base,
        Some("a-token"),
        &[
            "watch",
            "--tenant",
            "t",
            "--topic",
            "facts.build",
            "--consumer",
            "c",
        ],
    )?
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;
    let lines = lines_of(&mut child)?;
    let pid = child.id();
    let status = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let kill = |signal: &str| -> anyhow::Result<()> {
            let sent = Command::new("kill")
                .args([signal, &pid.to_string()])
                .status()?;
            ensure!(sent.success(), "kill failed");
            Ok(())
        };
        // It printed the line, and is waiting for the commit to be answered.
        let first = lines.recv_timeout(SLACK).context("the line")?;
        ensure!(first.contains("first"), "{first}");
        std::thread::sleep(Duration::from_millis(300));

        // One signal asks it to finish, which it cannot: it does not exit.
        kill("-TERM")?;
        std::thread::sleep(Duration::from_millis(700));
        ensure!(
            child.try_wait()?.is_none(),
            "it ended on the first signal, with the commit not done"
        );

        // The second ends it, at once.
        kill("-TERM")?;
        let started = std::time::Instant::now();
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok((status, started.elapsed()));
            }
            ensure!(
                started.elapsed() < Duration::from_secs(5),
                "it did not end on the second signal"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    })
    .await??;
    assert!(!status.0.success(), "a second signal is not a success");
    assert_eq!(status.0.code(), Some(130));
    assert!(status.1 < Duration::from_secs(5));
    Ok(())
}

/// On SIGTERM, `watch` finishes the line it is on, commits it and exits 0; a
/// restart with the same consumer prints the rest.
#[cfg(unix)]
#[tokio::test]
async fn watch_finishes_its_line_commits_and_exits_zero_on_sigterm() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let t = target.tenant.clone();
    let mut sent = Vec::new();
    for n in 0..5 {
        let body = json!({"source": "e2e", "type": format!("e2e.n{n}")});
        sent.push(serde_json::to_value(
            target.publish("facts.build", &body).await?.envelope,
        )?);
    }
    let args = [
        "watch",
        "--tenant",
        &t,
        "--topic",
        "facts.build",
        "--consumer",
        "term",
    ];
    let mut child = command(&target, Some(&target.token), &args)?
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let lines = lines_of(&mut child)?;
    let pid = child.id();
    let (printed_before, status) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let mut seen = Vec::new();
        while seen.len() < 2 {
            seen.push(lines.recv_timeout(SLACK).context("a line from watch")?);
        }
        let signalled = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()?;
        ensure!(signalled.success(), "kill failed");
        // What it printed before it noticed, and nothing after it exited.
        while let Ok(line) = lines.recv_timeout(SLACK) {
            seen.push(line);
        }
        Ok((seen, child.wait()?))
    })
    .await??;
    assert!(status.success(), "exit {status}");
    let count = printed_before.len();
    assert!((2..=5).contains(&count), "{count} lines");

    // The broker has exactly what was printed.
    let state = one(
        &target,
        &target.token,
        &[
            "consumers",
            "get",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--consumer",
            "term",
        ],
    )
    .await?;
    assert_eq!(state["next"], count);

    // The rest, once.
    let rest = if count < 5 {
        let remaining = (5 - count).to_string();
        printed(
            &run(
                &target,
                Some(&target.token),
                &[
                    "watch",
                    "--tenant",
                    &t,
                    "--topic",
                    "facts.build",
                    "--consumer",
                    "term",
                    "--limit",
                    &remaining,
                ],
            )
            .await?,
        )?
    } else {
        Vec::new()
    };
    let seen: Vec<Value> = printed_before
        .iter()
        .map(|line| serde_json::from_str(line))
        .collect::<Result<Vec<Value>, _>>()?
        .into_iter()
        .chain(rest)
        .collect();
    assert_eq!(seen, sent, "nothing missed and nothing twice");
    Ok(())
}

/// Done criterion 2: `work pull | ack | fail` and `alerts ack` work with a
/// credential, and the rest of the operations with the admin's.
#[tokio::test]
async fn the_commands_do_what_their_operations_do() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let t = target.tenant.clone();
    let admin = target.token.clone();
    // The first delivery is refused, so that the listing has an error to show.
    let receiver = Receiver::start(&target, [Reply::Status(500)]).await?;

    // principals create | list
    let scopes = json!([
        {"topic": "work.builds", "verbs": ["publish", "claim"]},
        {"topic": "alerts", "verbs": ["publish", "ack"]},
        {"topic": "facts.build", "verbs": ["publish", "subscribe"]},
    ])
    .to_string();
    let issued = one(
        &target,
        &admin,
        &[
            "principals",
            "create",
            "--kind",
            "agent",
            "--name",
            "builder-1",
            "--tenant",
            &t,
            "--scopes",
            &scopes,
            "--ttl-seconds",
            "600",
        ],
    )
    .await?;
    let agent = issued["credential"]
        .as_str()
        .context("a credential")?
        .to_owned();
    let agent_id = issued["principal"]["id"]
        .as_str()
        .context("an id")?
        .to_owned();
    assert_eq!(issued["principal"]["name"], "builder-1");
    let listed = printed(
        &run(
            &target,
            Some(&admin),
            &["principals", "list", "--tenant", &t],
        )
        .await?,
    )?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["id"], json!(agent_id));
    assert!(
        !listed[0].to_string().contains(&agent),
        "no credential in a listing"
    );

    // publish | poll with the credential.
    let published = one(
        &target,
        &agent,
        &[
            "publish",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--type",
            "build.done",
            "--source",
            "cli",
            "--data",
            r#"{"ok":true}"#,
            "--subject",
            "s1",
        ],
    )
    .await?;
    assert_eq!(published["principalname"], "builder-1");
    let polled = printed(
        &run(
            &target,
            Some(&agent),
            &[
                "poll",
                "--tenant",
                &t,
                "--topic",
                "facts.build",
                "--count",
                "5",
            ],
        )
        .await?,
    )?;
    assert_eq!(polled, vec![published]);

    // work publish | pull | fail | ack
    let item = one(
        &target,
        &agent,
        &[
            "work",
            "publish",
            "--tenant",
            &t,
            "--queue",
            "builds",
            "--type",
            "job.requested",
            "--source",
            "cli",
            "--data",
            &job(1)["data"].to_string(),
        ],
    )
    .await?;
    let item_id = item["itemid"].as_str().context("an item id")?.to_owned();
    assert_eq!(item["attempt"], 1);
    let pull = [
        "work",
        "pull",
        "--tenant",
        &t,
        "--queue",
        "builds",
        "--consumer",
        "w1",
    ];
    let claim = one(&target, &agent, &pull).await?;
    let lease = claim["lease_id"].as_str().context("a lease")?.to_owned();
    assert_eq!(claim["item"]["itemid"], json!(item_id));
    // A pull again by the same consumer is the same lease.
    assert_eq!(one(&target, &agent, &pull).await?, claim);

    let failed = one(
        &target,
        &agent,
        &[
            "work",
            "fail",
            "--tenant",
            &t,
            "--queue",
            "builds",
            "--item",
            &item_id,
            "--consumer",
            "w1",
            "--lease-id",
            &lease,
            "--reason",
            "it broke",
        ],
    )
    .await?;
    assert_eq!(failed["type"], "mansio.work.failed");
    assert_eq!(failed["data"]["reason"], "it broke");
    // --wait looks until the retry is claimable.
    let wait = (target.backoff(1) + SLACK).as_secs().to_string();
    let again = one(
        &target,
        &agent,
        &[
            "work",
            "pull",
            "--tenant",
            &t,
            "--queue",
            "builds",
            "--consumer",
            "w1",
            "--wait",
            &wait,
        ],
    )
    .await?;
    assert_eq!(again["item"]["attempt"], 2);
    let acked = one(
        &target,
        &agent,
        &[
            "work",
            "ack",
            "--tenant",
            &t,
            "--queue",
            "builds",
            "--item",
            &item_id,
            "--consumer",
            again["consumer"].as_str().context("a consumer")?,
            "--lease-id",
            again["lease_id"].as_str().context("a lease")?,
        ],
    )
    .await?;
    assert_eq!(acked["type"], "mansio.work.acked");
    // Nothing left: nothing printed, and it is not a failure.
    assert!(printed(&run(&target, Some(&agent), &pull).await?)?.is_empty());

    // alerts publish | ack
    let alert = one(
        &target,
        &agent,
        &[
            "alerts",
            "publish",
            "--tenant",
            &t,
            "--type",
            "disk.full",
            "--source",
            "cli",
        ],
    )
    .await?;
    let alert_id = alert["id"].as_str().context("an id")?.to_owned();
    let ack = ["alerts", "ack", "--tenant", &t, "--alert", &alert_id];
    assert_eq!(
        one(&target, &agent, &ack).await?["type"],
        "mansio.alert.acknowledged"
    );
    refused(&target, Some(&agent), &ack, "409").await?;

    // consumers commit | get
    let committed = one(
        &target,
        &agent,
        &[
            "consumers",
            "commit",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--consumer",
            "c",
            "--offset",
            "0",
        ],
    )
    .await?;
    assert_eq!(committed, json!({"consumer": "c", "offset": 0, "next": 1}));
    // Only a message that exists can be committed.
    refused(
        &target,
        Some(&agent),
        &[
            "consumers",
            "commit",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--consumer",
            "c",
            "--offset",
            "5",
        ],
        "409",
    )
    .await?;
    let state = one(
        &target,
        &agent,
        &[
            "consumers",
            "get",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--consumer",
            "c",
        ],
    )
    .await?;
    assert_eq!(state, committed);

    // What a credential may not do is refused.
    refused(
        &target,
        Some(&agent),
        &[
            "work",
            "pull",
            "--tenant",
            &t,
            "--queue",
            "reviews",
            "--consumer",
            "w",
        ],
        "403",
    )
    .await?;
    refused(&target, Some(&agent), &["principals", "list"], "403").await?;
    refused(
        &target,
        Some(&agent),
        &[
            "webhooks",
            "create",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--endpoint",
            &receiver.url,
        ],
        "403",
    )
    .await?;

    // webhooks create | list | delete
    let made = one(
        &target,
        &admin,
        &[
            "webhooks",
            "create",
            "--tenant",
            &t,
            "--topic",
            "facts.build",
            "--endpoint",
            &receiver.url,
        ],
    )
    .await?;
    let webhook_id = made["webhook"]["id"].as_str().context("an id")?.to_owned();
    assert!(
        made["secret"]
            .as_str()
            .is_some_and(|s| s.starts_with("whsec_"))
    );
    let hooks = printed(&run(&target, Some(&admin), &["webhooks", "list", "--tenant", &t]).await?)?;
    assert_eq!(hooks.len(), 1);
    assert_eq!(hooks[0]["id"], json!(webhook_id));
    assert!(hooks[0].get("secret").is_none());
    // It delivers the one message of facts.build, after one refusal, and the
    // listing says so: the offset it delivered, and the error before it.
    let wait = target.webhook_give_up() + target.webhook_backoff(1) + SLACK;
    let delivered = target
        .until("the delivery to be listed", wait, || async {
            let hooks =
                printed(&run(&target, Some(&admin), &["webhooks", "list", "--tenant", &t]).await?)?;
            Ok(hooks
                .into_iter()
                .find(|hook| hook["last_delivered_offset"] == 0))
        })
        .await?;
    assert_eq!(delivered["last_error"]["offset"], 0);
    assert_eq!(delivered["last_error"]["message"], "HTTP 500");
    let deleted = one(
        &target,
        &admin,
        &["webhooks", "delete", "--id", &webhook_id],
    )
    .await?;
    assert_eq!(deleted["id"], json!(webhook_id));

    // principals revoke: the credential stops working.
    let revoked = one(
        &target,
        &admin,
        &["principals", "revoke", "--id", &agent_id],
    )
    .await?;
    assert_eq!(revoked["id"], json!(agent_id));
    refused(&target, Some(&agent), &pull, "401").await?;
    Ok(())
}

/// `work pull --wait` keeps looking, and returns as soon as there is an item.
#[tokio::test]
async fn work_pull_waits_for_an_item_to_arrive() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let t = target.tenant.clone();
    let child = command(
        &target,
        Some(&target.token),
        &[
            "work",
            "pull",
            "--tenant",
            &t,
            "--queue",
            "builds",
            "--consumer",
            "w",
            "--wait",
            "20",
        ],
    )?
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let item = target.work_publish("builds", &job(1)).await?.envelope;
    let output = tokio::task::spawn_blocking(move || child.wait_with_output()).await??;
    let lines = printed(&output)?;
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["item"]["id"], json!(item.id.to_string()));

    // And with nothing arriving it returns, quietly, when the wait ends.
    let started = std::time::Instant::now();
    let output = run(
        &target,
        Some(&target.token),
        &[
            "work",
            "pull",
            "--tenant",
            &t,
            "--queue",
            "empty",
            "--consumer",
            "w",
            "--wait",
            "1",
        ],
    )
    .await?;
    assert!(printed(&output)?.is_empty());
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert!(started.elapsed() < Duration::from_secs(20));
    Ok(())
}

/// An MCP server that is running, spoken to over its stdin and stdout.
struct Mcp {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Lines<String>,
}

impl Mcp {
    fn start(binary: &std::path::Path, base: &str, token: &str) -> anyhow::Result<Self> {
        let mut child = Command::new(binary)
            .arg("mcp")
            .env("MANSIO_URL", base)
            .env("MANSIO_TOKEN", token)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let lines = lines_of(&mut child)?;
        let stdin = child.stdin.take();
        Ok(Self {
            child,
            stdin,
            lines,
        })
    }

    fn send(&mut self, message: &Value) -> anyhow::Result<()> {
        let stdin = self.stdin.as_mut().context("stdin is closed")?;
        writeln!(stdin, "{message}")?;
        stdin.flush()?;
        Ok(())
    }

    fn receive(&mut self) -> anyhow::Result<Value> {
        let line = self
            .lines
            .recv_timeout(SLACK)
            .map_err(|_| anyhow!("no message from the MCP server within {SLACK:?}"))?;
        Ok(serde_json::from_str(&line)?)
    }

    /// A request and its answer.
    fn request(&mut self, id: u64, method: &str, params: Value) -> anyhow::Result<Value> {
        self.send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))?;
        let answer = self.receive()?;
        ensure!(
            answer["id"] == json!(id),
            "answer to another request: {answer}"
        );
        Ok(answer)
    }

    /// A tool call, which must have been answered with a result.
    fn call(&mut self, id: u64, tool: &str, arguments: Value) -> anyhow::Result<(bool, String)> {
        let answer = self.request(
            id,
            "tools/call",
            json!({"name": tool, "arguments": arguments}),
        )?;
        let result = answer.get("result").with_context(|| format!("{answer}"))?;
        let text = result["content"][0]["text"]
            .as_str()
            .context("a text result")?
            .to_owned();
        Ok((result["isError"].as_bool().unwrap_or(false), text))
    }

    /// Close stdin, as a harness that is done with the server does, and wait
    /// for it to exit.
    fn finish(mut self) -> anyhow::Result<std::process::ExitStatus> {
        drop(self.stdin.take());
        Ok(self.child.wait()?)
    }
}

/// Done criterion 3: the MCP server speaks over stdio: initialize, list the
/// tools, call `work_pull` and `work_ack`.
#[tokio::test]
async fn the_mcp_server_lists_the_tools_and_pulls_and_acks_work_over_stdio() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let t = target.tenant.clone();
    let agent = target
        .issue(
            "agent",
            "mcp-agent",
            &json!([{"topic": "work.builds", "verbs": ["claim"]}]),
            None,
        )
        .await?;
    let item = target.work_publish("builds", &job(1)).await?.envelope;
    let (base, token) = (target.base.clone(), agent.token.clone());
    let binary = crate::cli_binary()?;

    let talked = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let mut mcp = Mcp::start(&binary, &base, &token)?;
        let init = mcp.request(
            1,
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "e2e", "version": "1"}
            }),
        )?;
        mcp.send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))?;
        let listed = mcp.request(2, "tools/list", json!({}))?;
        let args = json!({"tenant": t, "queue": "builds", "consumer": "claude-1"});
        let pulled = mcp.call(3, "work_pull", args.clone())?;
        // The answer to a pull may be lost: asking again gives the same lease.
        let again = mcp.call(4, "work_pull", args.clone())?;
        let claim: Value = serde_json::from_str(&pulled.1)?;
        let ack_args = json!({
            "tenant": t, "queue": "builds",
            "item": claim["item"]["itemid"], "consumer": claim["consumer"],
            "lease_id": claim["lease_id"]
        });
        let acked = mcp.call(5, "work_ack", ack_args.clone())?;
        let empty = mcp.call(6, "work_pull", args)?;
        let twice = mcp.call(7, "work_ack", ack_args)?;
        let bad = mcp.call(8, "work_pull", json!({"tenant": t}))?;
        let status = mcp.finish()?;
        Ok((
            init, listed, pulled, again, acked, empty, twice, bad, status,
        ))
    })
    .await??;
    let (init, listed, pulled, again, acked, empty, twice, bad, status) = talked;

    assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(init["result"]["serverInfo"]["name"], "mansio");
    let mut names: Vec<&str> = listed["result"]["tools"]
        .as_array()
        .context("tools")?
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    names.sort_unstable();
    let mut expected = vec![
        "alerts_ack",
        "alerts_publish",
        "consumers_commit",
        "consumers_get",
        "poll",
        "principals_create",
        "principals_list",
        "principals_revoke",
        "publish",
        "webhooks_create",
        "webhooks_delete",
        "webhooks_list",
        "work_ack",
        "work_fail",
        "work_publish",
        "work_pull",
    ];
    expected.sort_unstable();
    assert_eq!(
        names, expected,
        "every operation but watch, by the CLI's names"
    );

    assert!(!pulled.0, "{}", pulled.1);
    let claim: Value = serde_json::from_str(&pulled.1)?;
    assert_eq!(claim["item"]["id"], json!(item.id.to_string()));
    assert_eq!(again, pulled, "the same lease again");
    assert!(!acked.0, "{}", acked.1);
    let acked: Value = serde_json::from_str(&acked.1)?;
    assert_eq!(acked["type"], "mansio.work.acked");
    assert_eq!(empty, (false, "no output".to_owned()));
    // A mistake is an answer the model can read, not a broken server.
    assert!(twice.0, "a second ack is refused: {}", twice.1);
    assert!(twice.1.contains("404"), "{}", twice.1);
    assert!(bad.0 && bad.1.contains("queue is required"), "{}", bad.1);
    assert!(status.success(), "exit {status}");

    assert_eq!(target.work_transitions().await?, ["claimed", "acked"]);
    Ok(())
}

/// The MCP server without a token does not start.
#[tokio::test]
async fn the_mcp_server_does_not_start_without_a_token() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let output = run(&target, None, &["mcp"]).await?;
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("MANSIO_TOKEN"));
    Ok(())
}
