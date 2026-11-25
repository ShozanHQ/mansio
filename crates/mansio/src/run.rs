//! `mansio work run`: claim one work item, run a command for it while the
//! lease is held, and ack or fail the item by how the command ends.
//!
//! The claim is `work pull`'s, `--wait` included. The command gets the item
//! as one line on its standard input and its id and attempt in its
//! environment, never the lease id: only the runner extends, acks or fails.
//! The lease is extended each time a share of what is left of it has passed
//! (`MANSIO_RUN_EXTEND_AT_PERCENT`), with every extension raced against the
//! lease's end. An extension whose end does not move means the lease is at its
//! longest: the command is let run until `MANSIO_RUN_STOP_GRACE_SECONDS`
//! before that end, then killed, and the item failed, with the exit status 4.
//! A refusal that says the lease is gone (401, 403, 404 or 409), or an end
//! that passes with no extension answered, kills the command and sends
//! nothing: the exit status is 4.
//!
//! A command that exits 0 acks the item and the exit is 0; one that fails, or
//! cannot be started, fails it and the exit is 3. The envelope of the ack or
//! the fail is printed. An ack or a fail that is refused or lost is not sent
//! again: the exit is 1, the lease runs out and the item is tried again, so
//! the command runs at least once for an item.
//!
//! A signal while the runner waits for an item ends the wait with the status
//! 130 and nothing run. The first signal while the command runs gives it the
//! grace to end by itself, with the lease held; after the grace it is killed,
//! the item failed as `stopped`, and the exit is 3. A second signal kills it
//! at once, fails the item as `stopped`, and the exit is 130. No signal is
//! passed on to the command, which a terminal or a service manager signals
//! itself.

use std::future::Future;
use std::pin::Pin;
use std::process::{ExitCode, ExitStatus};
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mansio_core::Timestamp;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{Map, Value};

use crate::child;
use crate::client::{Client, Json, Refused};
use crate::ops::{self, Args, Op, member};
use crate::watch::{KILL, Stop};

/// The command failed, could not be started, or was stopped.
const FAILED: u8 = 3;
/// The lease was lost, or could grow no further.
const LEASE_ENDED: u8 = 4;
/// A signal ended the wait for an item, or a second one killed the command.
const SIGNALLED: u8 = 130;

/// The reason a command that outlived the longest lease is failed with.
const PAST_THE_LONGEST: &str = "the command ran past the longest lease";
/// The reason a command a signal stopped is failed with.
const STOPPED: &str = "stopped";

/// The parts of the item the command is told in its environment.
#[derive(Deserialize)]
struct Head {
    itemid: String,
    attempt: u64,
}

/// What became of the command.
enum Ended {
    /// It ended by itself.
    Exited(ExitStatus),
    /// It was killed and the item must be failed with this reason, and the
    /// run exit with this status.
    Killed(&'static str, u8),
    /// The lease is gone: it was killed and nothing is sent.
    LeaseLost,
}

/// An extension in flight.
type Extending<'a> = Pin<Box<dyn Future<Output = anyhow::Result<Vec<Json>>> + 'a>>;

/// The runner's clock, in milliseconds since the epoch.
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| i64::try_from(since.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// How long until `at`, on the runner's clock; nothing once it has passed.
fn until(at: i64) -> Duration {
    Duration::from_millis(u64::try_from(at.saturating_sub(now())).unwrap_or(0))
}

/// An RFC 3339 time as JSON text, in milliseconds; none if it is not one.
fn millis_of(text: &RawValue) -> Option<i64> {
    serde_json::from_str::<String>(text.get())
        .ok()?
        .parse::<Timestamp>()
        .ok()?
        .unix_millis()
}

/// The end of the lease an extension answered: `data.expires_at` of its
/// envelope.
fn extended_end(answer: &[Json]) -> Option<i64> {
    let envelope = answer.first()?;
    let data = member(envelope, "data").ok()?;
    millis_of(&member(&data, "expires_at").ok()?)
}

/// Whether a failed extension says that the lease is gone.
fn lease_is_gone(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<Refused>()
        .is_some_and(|refused| matches!(refused.status.as_u16(), 401 | 403 | 404 | 409))
}

/// Completes with the extension's answer, or never when none is in flight.
async fn answer_of(extending: &mut Option<Extending<'_>>) -> anyhow::Result<Vec<Json>> {
    match extending {
        Some(call) => call.await,
        None => std::future::pending().await,
    }
}

#[expect(clippy::print_stdout, reason = "the CLI prints its results to stdout")]
fn print(lines: &[Json]) {
    for line in lines {
        println!("{line}");
    }
}

/// Claim one item, run the command for it, and ack or fail it.
///
/// # Errors
///
/// Fails, before anything is sent, if an argument is not valid. Fails with
/// the server's reason or the network's if the claim, the ack or the fail is
/// refused or not answered; the ack or the fail is not sent again. The other
/// endings are exit statuses: 3, 4 and 130.
pub async fn work_run(client: &Client, values: &Map<String, Value>) -> anyhow::Result<ExitCode> {
    let args = Args::check(Op::WorkRun, values)?;
    let command = args.command();
    let stop = Stop::listen();
    let claimed = tokio::select! {
        claimed = ops::work_pull(client, &args) => claimed?,
        () = stop.wait() => return Ok(ExitCode::from(SIGNALLED)),
    };
    let Some(claim) = claimed.into_iter().next() else {
        return Ok(ExitCode::SUCCESS);
    };
    let mut last = now();
    let not_understood = || anyhow::anyhow!("the server's claim was not understood");
    let item = member(&claim, "item")?;
    let head: Head = serde_json::from_str(item.get()).map_err(|_| not_understood())?;
    let lease_id: String =
        serde_json::from_str(member(&claim, "lease_id")?.get()).map_err(|_| not_understood())?;
    let consumer: String =
        serde_json::from_str(member(&claim, "consumer")?.get()).map_err(|_| not_understood())?;
    let mut end = millis_of(&member(&claim, "expires_at")?).ok_or_else(not_understood)?;

    let text = |name: &str| values.get(name).and_then(Value::as_str).unwrap_or_default();
    let mut lease = Map::new();
    for (name, value) in [
        ("tenant", text("tenant")),
        ("queue", text("queue")),
        ("item", head.itemid.as_str()),
        ("consumer", consumer.as_str()),
        ("lease_id", lease_id.as_str()),
    ] {
        lease.insert(name.to_owned(), Value::String(value.to_owned()));
    }
    let fail = |reason: &str| {
        let mut values = lease.clone();
        values.insert("reason".to_owned(), Value::String(reason.to_owned()));
        values
    };

    let attempt = head.attempt.to_string();
    let env = [
        ("MANSIO_TENANT", text("tenant")),
        ("MANSIO_QUEUE", text("queue")),
        ("MANSIO_ITEM_ID", head.itemid.as_str()),
        ("MANSIO_ATTEMPT", attempt.as_str()),
    ];
    let input = format!("{}\n", item.get()).into_bytes();
    let mut running = match child::start(&command, &env, input) {
        Ok(running) => running,
        Err(error) => {
            eprintln!("mansio: {error:#}");
            let reason = "the command could not be started";
            print(&ops::run(client, Op::WorkFail, &fail(reason)).await?);
            return Ok(ExitCode::from(FAILED));
        }
    };
    // No await between the start and this: the listener sees the flag as it
    // is, and leaves a second signal to the loop below.
    stop.command_running.store(true, Ordering::SeqCst);

    let percent = i64::try_from(client.run_extend_at_percent()).unwrap_or(50);
    let grace = i64::try_from(client.run_stop_grace().as_millis()).unwrap_or(i64::MAX);
    let mut longest = false;
    let mut stop_by: Option<i64> = None;
    let mut extending: Option<Extending<'_>> = None;
    let ended = loop {
        let extend_at = last.saturating_add(end.saturating_sub(last).saturating_mul(percent) / 100);
        tokio::select! {
            biased;
            () = stop.reached(KILL) => break Ended::Killed(STOPPED, SIGNALLED),
            () = tokio::time::sleep(until(end.saturating_sub(grace))), if longest => {
                break Ended::Killed(PAST_THE_LONGEST, LEASE_ENDED);
            }
            () = tokio::time::sleep(until(end)) => break Ended::LeaseLost,
            status = running.wait() => break Ended::Exited(status?),
            () = stop.wait(), if stop_by.is_none() => {
                stop_by = Some(now().saturating_add(grace));
            }
            () = tokio::time::sleep(until(stop_by.unwrap_or(i64::MAX))), if stop_by.is_some() => {
                break Ended::Killed(STOPPED, FAILED);
            }
            answer = answer_of(&mut extending) => {
                extending = None;
                last = now();
                match answer {
                    Ok(answer) => match extended_end(&answer) {
                        Some(new_end) if new_end == end => longest = true,
                        Some(new_end) => end = new_end,
                        None => eprintln!("mansio: the server's answer to the extension was not understood; trying again"),
                    },
                    Err(error) if lease_is_gone(&error) => break Ended::LeaseLost,
                    Err(error) => {
                        let what = if error.downcast_ref::<reqwest::Error>().is_some() {
                            "could not be sent"
                        } else {
                            "failed"
                        };
                        eprintln!("mansio: the extension {what}: {error:#}; trying again");
                    }
                }
            }
            () = tokio::time::sleep(until(extend_at)), if extending.is_none() && !longest => {
                extending = Some(Box::pin(ops::run(client, Op::WorkExtend, &lease)));
            }
        }
    };
    drop(extending);
    if !matches!(ended, Ended::Exited(_)) {
        // Killed and reaped, never left running.
        let _ = running.kill().await;
    }
    // The command has ended and been reaped: from here a second signal ends
    // the process at once, an ack or a fail in flight or not.
    stop.command_running.store(false, Ordering::SeqCst);
    match ended {
        Ended::LeaseLost => {
            eprintln!("mansio: the lease on item {} was lost", head.itemid);
            Ok(ExitCode::from(LEASE_ENDED))
        }
        Ended::Killed(reason, code) => {
            print(&ops::run(client, Op::WorkFail, &fail(reason)).await?);
            Ok(ExitCode::from(code))
        }
        Ended::Exited(_) if now() >= end => {
            eprintln!(
                "mansio: the lease on item {} ended before the command did",
                head.itemid
            );
            Ok(ExitCode::from(LEASE_ENDED))
        }
        Ended::Exited(status) if status.success() => {
            print(&ops::run(client, Op::WorkAck, &lease).await?);
            Ok(ExitCode::SUCCESS)
        }
        Ended::Exited(status) => {
            let reason = match status.code() {
                Some(code) => format!("the command exited with status {code}"),
                None => "the command was ended by a signal".to_owned(),
            };
            print(&ops::run(client, Op::WorkFail, &fail(&reason)).await?);
            Ok(ExitCode::from(FAILED))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(text: &str) -> Json {
        RawValue::from_string(text.to_owned()).unwrap()
    }

    #[test]
    fn an_extension_answers_its_end_and_anything_else_is_not_understood() {
        let answer = [json(
            r#"{"type":"mansio.work.extended","data":{"expires_at":"1970-01-01T00:00:01.5Z"}}"#,
        )];
        assert_eq!(extended_end(&answer), Some(1_500));
        for text in [
            r#"{"type":"mansio.work.extended"}"#,
            r#"{"data":{"expires_at":"soon"}}"#,
            r#"{"data":{"expires_at":7}}"#,
        ] {
            assert_eq!(extended_end(&[json(text)]), None, "{text}");
        }
        assert_eq!(extended_end(&[]), None);
    }

    #[test]
    fn only_a_refusal_that_says_the_lease_is_gone_loses_it() {
        let refused = |status: u16| {
            anyhow::Error::new(Refused {
                status: reqwest::StatusCode::from_u16(status).unwrap(),
                text: String::new(),
            })
        };
        for status in [401, 403, 404, 409] {
            assert!(lease_is_gone(&refused(status)), "{status}");
            assert!(
                lease_is_gone(&refused(status).context("extending")),
                "{status}"
            );
        }
        for status in [400, 429, 500, 503] {
            assert!(!lease_is_gone(&refused(status)), "{status}");
        }
        assert!(!lease_is_gone(&anyhow::anyhow!("network")));
    }
}
