//! `mansio watch`: follow a topic and print each envelope as a line.
//!
//! With `--consumer` the server keeps the position. The offset of a line is
//! committed after the line has been written and flushed, so a process that
//! is stopped at any moment, and started again with the same consumer, prints
//! every envelope once: none is missed, and none is printed twice unless it
//! was stopped between the flush and the commit. On SIGINT or SIGTERM the
//! line being handled is finished and committed, and the exit is 0. A second
//! signal while it finishes ends it at once, without the commit, with the
//! status 130.

use std::io::{self, Write};

use tokio::sync::watch;

use crate::client::Client;
use crate::ops::{WatchArgs, consumer_commit, consumer_state, read_messages};

/// The status of a watch that a second signal ended.
const KILLED: i32 = 130;

/// Tells the watch to stop when SIGINT or SIGTERM comes.
struct Stop(watch::Receiver<bool>);

impl Stop {
    /// Start listening for the signals. The first asks the watch to finish the
    /// line it is on and stop; a second, while it does, ends the process at
    /// once, for a commit that hangs or a server that does not answer.
    fn listen() -> Self {
        let (sender, receiver) = watch::channel(false);
        tokio::spawn(async move {
            let mut signals = Signals::new();
            signals.next().await;
            let _ = sender.send(true);
            signals.next().await;
            std::process::exit(KILLED);
        });
        Self(receiver)
    }

    fn is_set(&self) -> bool {
        *self.0.borrow()
    }

    /// Completes when the signal has come.
    async fn wait(&self) {
        let mut receiver = self.0.clone();
        while !*receiver.borrow_and_update() {
            if receiver.changed().await.is_err() {
                // The listener is gone: nothing will stop the watch.
                std::future::pending::<()>().await;
            }
        }
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

/// Follow the topic until a signal, the limit, or an error.
///
/// # Errors
///
/// Fails with the server's reason if it refuses, and with the network's if it
/// cannot be reached even after the retries. A reader that has gone away
/// (a closed pipe) is not a failure.
pub async fn watch(client: &Client, args: WatchArgs) -> anyhow::Result<()> {
    if args.limit == Some(0) {
        return Ok(());
    }
    let stop = Stop::listen();
    let mut next = match &args.consumer {
        Some(consumer) => {
            let state = consumer_state(client, &args.tenant, &args.topic, consumer).await?;
            state["next"].as_u64().unwrap_or(0)
        }
        None => args.from,
    };
    let mut printed = 0_u64;
    let stdout = io::stdout();
    while !stop.is_set() {
        let read = tokio::select! {
            read = read_messages(client, &args.tenant, &args.topic, next, None) => read?,
            () = stop.wait() => return Ok(()),
        };
        if read.is_empty() {
            tokio::select! {
                () = tokio::time::sleep(client.poll_interval()) => {}
                () = stop.wait() => return Ok(()),
            }
            continue;
        }
        for (offset, envelope) in read {
            let mut out = stdout.lock();
            let written = writeln!(out, "{envelope}").and_then(|()| out.flush());
            drop(out);
            match written {
                Ok(()) => {}
                // Whoever was reading has gone; the line is not committed.
                Err(error) if error.kind() == io::ErrorKind::BrokenPipe => return Ok(()),
                Err(error) => return Err(error.into()),
            }
            if let Some(consumer) = &args.consumer {
                consumer_commit(client, &args.tenant, &args.topic, consumer, offset).await?;
            }
            next = offset.saturating_add(1);
            printed += 1;
            if args.limit.is_some_and(|limit| printed >= limit) || stop.is_set() {
                return Ok(());
            }
        }
    }
    Ok(())
}
