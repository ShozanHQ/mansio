//! `mansio watch`: follow a topic and print each envelope as a line, and with
//! a command after `--`, run it for each envelope first.
//!
//! With `--consumer` the server keeps the position. The offset of a line is
//! committed after the line has been written and flushed, so a process that
//! is stopped at any moment, and started again with the same consumer, prints
//! every envelope once: none is missed, and none is printed twice unless it
//! was stopped between the flush and the commit. On SIGINT or SIGTERM the
//! line being handled is finished and committed, and the exit is 0. A second
//! signal while it finishes ends it at once, without the commit, with the
//! status 130.
//!
//! With a command, an envelope is printed and committed only once the command
//! has exited 0 for it, so the command runs at least once for each envelope,
//! and again for one when the watch is stopped between the command's exit and
//! the commit. A command that fails is run again for the same envelope after
//! a wait that doubles up to a cap, and when the attempts allowed are used
//! the watch exits 3 without committing. On the first signal no further
//! envelope is taken: a command that is running is let finish, and is not run
//! again if it fails. A second signal kills the command, reaps it, and exits
//! 130.

use std::io::{self, Write};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::watch;

use crate::child;
use crate::client::{Client, Json};
use crate::ops::{WatchArgs, consumer_commit, consumer_state, read_messages};

/// The status of a watch that a second signal ended.
const KILLED: i32 = 130;
/// The status of a watch whose command failed as many times as it may.
const GAVE_UP: u8 = 3;

/// No signal has come.
const RUNNING: u8 = 0;
/// The first signal has come: finish the envelope and stop.
const ASKED: u8 = 1;
/// The second signal came while a command was running: kill it and exit.
const KILL: u8 = 2;

/// Tells the watch to stop when SIGINT or SIGTERM comes.
struct Stop {
    stage: watch::Receiver<u8>,
    /// Set while a command runs, so that the listener leaves a second signal
    /// to the loop, which holds the command and kills it first.
    command_running: Arc<AtomicBool>,
}

impl Stop {
    /// Start listening for the signals. The first asks the watch to finish the
    /// line it is on and stop; a second, while it does, ends the process at
    /// once, for a commit that hangs or a server that does not answer. When a
    /// command is running the second is recorded instead, and the loop kills
    /// the command and exits.
    fn listen() -> Self {
        let (sender, stage) = watch::channel(RUNNING);
        let command_running = Arc::new(AtomicBool::new(false));
        let running = Arc::clone(&command_running);
        tokio::spawn(async move {
            let mut signals = Signals::new();
            signals.next().await;
            let _ = sender.send(ASKED);
            signals.next().await;
            if running.load(Ordering::SeqCst) {
                let _ = sender.send(KILL);
                // The loop exits; keep the channel open until it does.
                std::future::pending::<()>().await;
            }
            std::process::exit(KILLED);
        });
        Self {
            stage,
            command_running,
        }
    }

    fn is_set(&self) -> bool {
        *self.stage.borrow() >= ASKED
    }

    fn must_kill(&self) -> bool {
        *self.stage.borrow() >= KILL
    }

    /// Completes when the stage has reached `at_least`.
    async fn reached(&self, at_least: u8) {
        let mut receiver = self.stage.clone();
        while *receiver.borrow_and_update() < at_least {
            if receiver.changed().await.is_err() {
                // The listener is gone: nothing will stop the watch.
                std::future::pending::<()>().await;
            }
        }
    }

    /// Completes when the signal has come.
    async fn wait(&self) {
        self.reached(ASKED).await;
    }
}

/// SIGINT and SIGTERM, where there is a SIGTERM, listened to for as long as
/// this lives: a signal that comes between one wait and the next is not lost.
#[cfg(unix)]
struct Signals {
    interrupt: Option<tokio::signal::unix::Signal>,
    terminate: Option<tokio::signal::unix::Signal>,
}

#[cfg(unix)]
impl Signals {
    fn new() -> Self {
        use tokio::signal::unix::{SignalKind, signal};
        Self {
            interrupt: signal(SignalKind::interrupt()).ok(),
            terminate: signal(SignalKind::terminate()).ok(),
        }
    }

    /// Completes at the next SIGINT or SIGTERM. If neither can be listened to,
    /// never.
    async fn next(&mut self) {
        async fn one(signal: &mut Option<tokio::signal::unix::Signal>) {
            match signal {
                Some(signal) => {
                    signal.recv().await;
                }
                None => std::future::pending::<()>().await,
            }
        }
        tokio::select! {
            () = one(&mut self.interrupt) => {},
            () = one(&mut self.terminate) => {},
        }
    }
}

#[cfg(not(unix))]
struct Signals;

#[cfg(not(unix))]
impl Signals {
    fn new() -> Self {
        Self
    }

    async fn next(&mut self) {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// How the runs of the command for one envelope ended.
enum Outcome {
    /// It exited 0: print and commit.
    Done,
    /// A signal asked the watch to stop before it did.
    Stopped,
    /// It failed as many times as it may.
    GaveUp,
}

/// The parts of an envelope the command is told in its environment.
#[derive(Deserialize)]
struct Head {
    #[serde(default)]
    id: Option<String>,
    #[serde(rename = "type", default)]
    event_type: Option<String>,
}

/// Run the command for one envelope until it exits 0, the attempts are used,
/// or a signal stops the watch.
async fn run_command(
    client: &Client,
    args: &WatchArgs,
    stop: &Stop,
    offset: u64,
    envelope: &Json,
) -> anyhow::Result<Outcome> {
    let head: Head = serde_json::from_str(envelope.get())?;
    let offset_text = offset.to_string();
    let topic = args.topic.to_string();
    let env = [
        ("MANSIO_TENANT", args.tenant.as_str()),
        ("MANSIO_TOPIC", topic.as_str()),
        ("MANSIO_OFFSET", offset_text.as_str()),
        ("MANSIO_EVENT_ID", head.id.as_deref().unwrap_or_default()),
        (
            "MANSIO_EVENT_TYPE",
            head.event_type.as_deref().unwrap_or_default(),
        ),
    ];
    let mut failures = 0_u64;
    loop {
        let mut running = child::start(&args.command, &env, format!("{envelope}\n").into_bytes())?;
        // No await between the start and this, nor between the end of the
        // wait and the clear: the listener sees the flag as it is.
        stop.command_running.store(true, Ordering::SeqCst);
        let status = tokio::select! {
            biased;
            () = stop.reached(KILL) => None,
            status = running.wait() => Some(status),
        };
        stop.command_running.store(false, Ordering::SeqCst);
        let status = match status {
            Some(status) if !stop.must_kill() => status?,
            // A second signal: the command is killed and reaped first, and is
            // never left running. One that has exited already is reaped.
            _ => {
                let _ = running.kill().await;
                std::process::exit(KILLED);
            }
        };
        if status.success() {
            return Ok(Outcome::Done);
        }
        failures += 1;
        if stop.is_set() {
            // A terminal signals the command too; it is not run again.
            return Ok(Outcome::Stopped);
        }
        let attempts = client.watch_attempts();
        if attempts != 0 && failures >= attempts {
            eprintln!("mansio: the command failed {failures} times for offset {offset}; giving up");
            return Ok(Outcome::GaveUp);
        }
        let wait = retry_wait(client, failures);
        let millis = wait.as_millis();
        match status.code() {
            Some(code) => eprintln!(
                "mansio: the command exited with status {code} for offset {offset}; trying again in {millis}ms"
            ),
            None => eprintln!(
                "mansio: the command was ended by a signal for offset {offset}; trying again in {millis}ms"
            ),
        }
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            () = stop.wait() => return Ok(Outcome::Stopped),
        }
    }
}

/// The wait after the `failures`th failure: the base, doubled for each
/// failure after the first, up to the cap.
fn retry_wait(client: &Client, failures: u64) -> Duration {
    let doublings = u32::try_from(failures.saturating_sub(1).min(31)).unwrap_or(31);
    client
        .watch_retry_base()
        .saturating_mul(1_u32 << doublings)
        .min(client.watch_retry_cap())
}

/// Follow the topic until a signal, the limit, or an error.
///
/// # Errors
///
/// Fails with the server's reason if it refuses, and with the network's if it
/// cannot be reached even after the retries, and with the system's if the
/// command cannot be started. A reader that has gone away (a closed pipe) is
/// not a failure. A command that failed as many times as it may is the exit
/// status 3.
pub async fn watch(client: &Client, args: WatchArgs) -> anyhow::Result<ExitCode> {
    if args.limit == Some(0) {
        return Ok(ExitCode::SUCCESS);
    }
    let stop = Stop::listen();
    let mut next = match &args.consumer {
        Some(consumer) => {
            let state = consumer_state(client, &args.tenant, &args.topic, consumer).await?;
            serde_json::from_str::<serde_json::Value>(state.get())?["next"]
                .as_u64()
                .unwrap_or(0)
        }
        None => args.from,
    };
    let mut printed = 0_u64;
    let stdout = io::stdout();
    while !stop.is_set() {
        let read = tokio::select! {
            read = read_messages(client, &args.tenant, &args.topic, next, None) => read?,
            () = stop.wait() => return Ok(ExitCode::SUCCESS),
        };
        if read.is_empty() {
            tokio::select! {
                () = tokio::time::sleep(client.poll_interval()) => {}
                () = stop.wait() => return Ok(ExitCode::SUCCESS),
            }
            continue;
        }
        for (offset, envelope) in read {
            if !args.command.is_empty() {
                match run_command(client, &args, &stop, offset, &envelope).await? {
                    Outcome::Done => {}
                    Outcome::Stopped => return Ok(ExitCode::SUCCESS),
                    Outcome::GaveUp => return Ok(ExitCode::from(GAVE_UP)),
                }
            }
            let mut out = stdout.lock();
            let written = writeln!(out, "{envelope}").and_then(|()| out.flush());
            drop(out);
            match written {
                Ok(()) => {}
                // Whoever was reading has gone; the line is not committed.
                Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                    return Ok(ExitCode::SUCCESS);
                }
                Err(error) => return Err(error.into()),
            }
            if let Some(consumer) = &args.consumer {
                consumer_commit(client, &args.tenant, &args.topic, consumer, offset).await?;
            }
            next = offset.saturating_add(1);
            printed += 1;
            if args.limit.is_some_and(|limit| printed >= limit) || stop.is_set() {
                return Ok(ExitCode::SUCCESS);
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}
