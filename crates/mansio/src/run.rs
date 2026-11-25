//! `mansio work run`: claim one work item, run a command for it with the lease
//! held, and ack or fail the item by how the command ended.
//!
//! The claim is `work pull`'s. The command gets the item as one JSON line on
//! its standard input, and never the lease id: only the runner extends, acks
//! or fails. The lease is extended when `MANSIO_RUN_EXTEND_AT_PERCENT` of the
//! time from the last answer to the lease's end has passed. An end that does
//! not move is the longest lease: the command is let run until
//! `MANSIO_RUN_STOP_GRACE_SECONDS` before it, then killed, and the item
//! failed. An extension the server refuses as gone (401, 403, 404, 409), or
//! a lease whose end passes, is a lost lease: the command is killed and
//! nothing is sent. An extension in flight is dropped only at the lease's
//! end: when the command ends, or is killed, first, its answer is waited for
//! before the ack or the fail is sent.
//!
//! The first SIGINT or SIGTERM lets the command end by itself for the grace,
//! with the lease held, and then kills it; a second kills it at once. A
//! signal is never passed on to the command.

use std::future::Future;
use std::pin::Pin;
use std::process::{ExitCode, ExitStatus};
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use mansio_core::Timestamp;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::child;
use crate::client::{Client, Json, Refused};
use crate::ops::{self, Args, Op, member};
use crate::watch::{KILL, Stop};

/// The status of a run whose command failed, could not be started, or was
/// stopped.
const FAILED: u8 = 3;
/// The status of a run whose lease was lost or could grow no further.
const LOST: u8 = 4;
/// The status of a run a signal ended while it waited for an item, or that a
/// second signal killed.
const SIGNALLED: u8 = 130;

/// The parts of the item the command is told in its environment.
#[derive(Deserialize)]
struct Head {
    itemid: String,
    attempt: u64,
}

/// The part of an `extended` fact the runner reads: where the lease ends.
#[derive(Deserialize)]
struct Extended {
    data: ExtendedData,
}

#[derive(Deserialize)]
struct ExtendedData {
    expires_at: String,
}

/// The lease the runner holds, as `work extend`, `work ack` and `work fail`
/// name it.
struct Lease {
    tenant: String,
    queue: String,
    item: String,
    consumer: String,
    lease_id: String,
}

impl Lease {
    /// The arguments of a call on the lease, with a reason for a fail.
    fn values(&self, reason: Option<&str>) -> Map<String, Value> {
        let mut values = Map::new();
        for (name, value) in [
            ("tenant", &self.tenant),
            ("queue", &self.queue),
            ("item", &self.item),
            ("consumer", &self.consumer),
            ("lease_id", &self.lease_id),
        ] {
            values.insert(name.to_owned(), Value::String(value.clone()));
        }
        if let Some(reason) = reason {
            values.insert("reason".to_owned(), Value::String(reason.to_owned()));
        }
        values
    }
}

/// How the time the command ran ended.
enum Ending {
    /// The command ended by itself.
    Exited(ExitStatus),
    /// The grace after the first signal ran out, and the command is killed.
    Stopped,
    /// A second signal came, and the command is killed.
    Killed,
    /// The lease can grow no further, and the command is killed.
    Longest,
    /// The lease is gone, and the command is killed.
    Lost,
}

/// An extension on its way: the answer of `work extend`.
type Extension<'a> = Pin<Box<dyn Future<Output = anyhow::Result<Vec<Json>>> + 'a>>;

/// What holding the lease ended with: how, the extension still in flight if
/// one is, and where the lease ends.
struct Held<'a> {
    ending: Ending,
    in_flight: Option<Extension<'a>>,
    end: i64,
}

/// Milliseconds since the Unix epoch, by this machine's clock.
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Completes when this machine's clock reaches `at`, in milliseconds.
fn until(at: i64) -> tokio::time::Sleep {
    let left = u64::try_from(at.saturating_sub(now_millis())).unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(left))
}

/// An end of a lease as RFC 3339 text, in milliseconds since the epoch.
fn end_of(text: &str) -> anyhow::Result<i64> {
    text.parse::<Timestamp>()
        .ok()
        .and_then(|end| end.unix_millis())
        .with_context(|| format!("the server's lease end {text:?} was not understood"))
}

/// A member of the claim that is text.
fn text_of(claim: &Json, name: &str) -> anyhow::Result<String> {
    serde_json::from_str(member(claim, name)?.get())
        .with_context(|| format!("the claim's {name} is not text"))
}

/// An argument that `Args::check` has found to be text.
fn argument(values: &Map<String, Value>, name: &str) -> anyhow::Result<String> {
    values
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("{name} is required"))
}

/// Whether a failed extension says that the lease is gone.
fn lease_is_gone(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<Refused>()
        .is_some_and(|refused| matches!(refused.status.as_u16(), 401 | 403 | 404 | 409))
}

/// Where an extension's answer says the lease now ends.
fn new_end(printed: &[Json]) -> anyhow::Result<i64> {
    let fact = printed
        .first()
        .context("the server answered the extension with nothing")?;
    let extended: Extended =
        serde_json::from_str(fact.get()).context("the extension's answer was not understood")?;
    end_of(&extended.data.expires_at)
}

/// Send an extension with the server's own lease length.
fn extend(client: &Client, values: Map<String, Value>) -> Extension<'_> {
    Box::pin(async move { ops::run(client, Op::WorkExtend, &values).await })
}

/// The answer of the extension in flight; never, if there is none.
async fn answer_of(in_flight: &mut Option<Extension<'_>>) -> anyhow::Result<Vec<Json>> {
    match in_flight {
        Some(extension) => extension.await,
        None => std::future::pending().await,
    }
}

/// Hold the lease while the command runs, until it ends, a signal stops it,
/// or the lease is gone or can grow no further. `received` is when the claim
/// was answered and `end` where its lease ends.
///
/// # Errors
///
/// Fails if the system cannot say how the command ended.
async fn hold<'a>(
    client: &'a Client,
    lease: &Lease,
    stop: &Stop,
    running: &mut child::Running,
    received: i64,
    mut end: i64,
) -> anyhow::Result<Held<'a>> {
    let percent = i64::try_from(client.run_extend_at_percent()).unwrap_or(50);
    let grace = client.run_stop_grace();
    let grace_millis = i64::try_from(grace.as_millis()).unwrap_or(i64::MAX);
    // The last answer, or the last extension that failed: the next extension
    // is due at the percent of what was left from then.
    let mut from = received;
    let mut in_flight: Option<Extension<'a>> = None;
    let mut longest = false;
    let mut stop_by: Option<tokio::time::Instant> = None;
    loop {
        let due = from.saturating_add(end.saturating_sub(from).saturating_mul(percent) / 100);
        tokio::select! {
            biased;
            () = stop.reached(KILL) => {
                return Ok(Held { ending: Ending::Killed, in_flight, end });
            }
            status = running.wait() => {
                return Ok(Held { ending: Ending::Exited(status?), in_flight, end });
            }
            () = tokio::time::sleep_until(stop_by.unwrap_or_else(tokio::time::Instant::now)),
                if stop_by.is_some() =>
            {
                return Ok(Held { ending: Ending::Stopped, in_flight, end });
            }
            () = until(end.saturating_sub(grace_millis)), if longest => {
                eprintln!("mansio: the lease can grow no further; stopping the command");
                return Ok(Held { ending: Ending::Longest, in_flight, end });
            }
            // The lease's end: an extension in flight is dropped, and the
            // lease is gone.
            () = until(end) => {
                eprintln!("mansio: the lease ended before an extension was answered");
                return Ok(Held { ending: Ending::Lost, in_flight: None, end });
            }
            answer = answer_of(&mut in_flight) => {
                in_flight = None;
                let now = now_millis();
                match answer {
                    Err(error) if lease_is_gone(&error) => {
                        eprintln!("mansio: the lease is gone: {error:#}");
                        return Ok(Held { ending: Ending::Lost, in_flight: None, end });
                    }
                    Ok(printed) => match new_end(&printed) {
                        Ok(extended) if extended == end => longest = true,
                        Ok(extended) => {
                            end = extended;
                            from = now;
                        }
                        Err(error) => {
                            eprintln!("mansio: the extension failed, trying again: {error:#}");
                            from = now;
                        }
                    },
                    Err(error) => {
                        if error.downcast_ref::<reqwest::Error>().is_some() {
                            eprintln!("mansio: the extension could not be sent, trying again: {error:#}");
                        } else {
                            eprintln!("mansio: the extension failed, trying again: {error:#}");
                        }
                        from = now;
                    }
                }
            }
            () = until(due), if in_flight.is_none() && !longest => {
                in_flight = Some(extend(client, lease.values(None)));
            }
            () = stop.wait(), if stop_by.is_none() => {
                stop_by = Some(tokio::time::Instant::now() + grace);
            }
        }
    }
}

/// Fail the item with `reason`, print the envelope the fail answers, and end
/// with `code`.
///
/// # Errors
///
/// Fails if the fail is refused or cannot be sent; it is not sent again.
async fn fail(client: &Client, lease: &Lease, reason: &str, code: u8) -> anyhow::Result<ExitCode> {
    let printed = ops::run(client, Op::WorkFail, &lease.values(Some(reason))).await?;
    crate::print(&printed);
    Ok(ExitCode::from(code))
}

/// Claim one item, run the command for it with the lease held, and ack or
/// fail it by how the command ended.
///
/// # Errors
///
/// Fails, before anything is sent, if an argument is not valid. Fails with the
/// server's reason if the claim, the ack or the fail is refused, and with the
/// network's if one cannot be sent; an ack or a fail is not sent again.
pub async fn work_run(client: &Client, values: &Map<String, Value>) -> anyhow::Result<ExitCode> {
    let args = Args::check(Op::WorkRun, values)?;
    let command = args.command();
    let stop = Stop::listen();
    // A claim the server made for a pull dropped here is never seen: its
    // lease runs out, and the item is tried again.
    let claimed = tokio::select! {
        claimed = ops::work_pull(client, &args) => claimed?,
        () = stop.wait() => return Ok(ExitCode::from(SIGNALLED)),
    };
    let Some(claim) = claimed.into_iter().next() else {
        return Ok(ExitCode::SUCCESS);
    };
    let received = now_millis();
    let item = member(&claim, "item")?;
    let head: Head =
        serde_json::from_str(item.get()).context("the claimed item was not understood")?;
    let lease = Lease {
        tenant: argument(values, "tenant")?,
        queue: argument(values, "queue")?,
        item: head.itemid,
        consumer: text_of(&claim, "consumer")?,
        lease_id: text_of(&claim, "lease_id")?,
    };
    let end = end_of(&text_of(&claim, "expires_at")?)?;

    let attempt = head.attempt.to_string();
    let env = [
        ("MANSIO_TENANT", lease.tenant.as_str()),
        ("MANSIO_QUEUE", lease.queue.as_str()),
        ("MANSIO_ITEM_ID", lease.item.as_str()),
        ("MANSIO_ATTEMPT", attempt.as_str()),
    ];
    let input = format!("{}\n", item.get()).into_bytes();
    let mut running = match child::start(&command, &env, input) {
        Ok(running) => running,
        Err(error) => {
            eprintln!("mansio: {error:#}");
            return fail(client, &lease, "the command could not be started", FAILED).await;
        }
    };
    // No await between the start and this: the listener sees the flag as it
    // is, and leaves a second signal to the loop, which kills the command.
    stop.command_running.store(true, Ordering::SeqCst);
    let held = hold(client, &lease, &stop, &mut running, received, end).await;
    let held = match held {
        Ok(held) => held,
        Err(error) => {
            let _ = running.kill().await;
            stop.command_running.store(false, Ordering::SeqCst);
            return Err(error);
        }
    };
    if !matches!(held.ending, Ending::Exited(_)) {
        // Killed and reaped: the command is never left running.
        let _ = running.kill().await;
    }
    // From here a second signal ends the process at once with 130.
    stop.command_running.store(false, Ordering::SeqCst);

    let mut ending = held.ending;
    // An extension in flight is not dropped to send the ack or the fail: its
    // answer is waited for first, until the lease's end at the latest.
    if let Some(extension) = held.in_flight {
        tokio::select! {
            answer = extension => {
                if answer.as_ref().is_err_and(lease_is_gone) {
                    ending = Ending::Lost;
                }
            }
            () = until(held.end) => ending = Ending::Lost,
        }
    }
    // A second signal that came while the command was being killed.
    if stop.must_kill() && !matches!(ending, Ending::Lost) {
        ending = Ending::Killed;
    }

    match ending {
        Ending::Exited(status) if status.success() => {
            let printed = ops::run(client, Op::WorkAck, &lease.values(None)).await?;
            crate::print(&printed);
            Ok(ExitCode::SUCCESS)
        }
        Ending::Exited(status) => {
            let reason = match status.code() {
                Some(code) => format!("the command exited with status {code}"),
                None => "the command was ended by a signal".to_owned(),
            };
            fail(client, &lease, &reason, FAILED).await
        }
        Ending::Stopped => fail(client, &lease, "stopped", FAILED).await,
        Ending::Killed => fail(client, &lease, "stopped", SIGNALLED).await,
        Ending::Longest => {
            fail(
                client,
                &lease,
                "the command ran past the longest lease",
                LOST,
            )
            .await
        }
        Ending::Lost => {
            eprintln!(
                "mansio: the lease on item {} was lost; neither an ack nor a fail was sent",
                lease.item
            );
            Ok(ExitCode::from(LOST))
        }
    }
}
