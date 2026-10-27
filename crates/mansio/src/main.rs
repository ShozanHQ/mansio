//! The mansio command line: publish to a topic and poll it, against the
//! HTTP API.

mod client;

use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use client::Client;
use mansio_core::{
    CausationId, CorrelationId, EventType, IdempotencyKey, Source, Subject, TenantId, Topic,
};
use serde_json::{Map, Value};

#[derive(Debug, Parser)]
#[command(name = "mansio", version, about = "Publish to and poll the mansio bus")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Publish an envelope to a topic and print it as stored.
    Publish(PublishArgs),
    /// Print the envelopes in a topic, one JSON object per line.
    Poll(PollArgs),
}

#[derive(Debug, Args)]
struct PublishArgs {
    /// The tenant to publish to.
    #[arg(long)]
    tenant: TenantId,
    /// The topic: `facts.<kind>`, `work.<queue>`, `alerts`, `presence` or `dead`.
    #[arg(long)]
    topic: Topic,
    /// What happened (CloudEvents type).
    #[arg(long = "type")]
    event_type: EventType,
    /// Where it happened (CloudEvents source).
    #[arg(long)]
    source: Source,
    /// The subject within the source.
    #[arg(long)]
    subject: Option<Subject>,
    /// The payload, as JSON.
    #[arg(long, value_parser = parse_json)]
    data: Option<Value>,
    /// The chain of work this belongs to.
    #[arg(long)]
    correlation_id: Option<CorrelationId>,
    /// The event that caused this one.
    #[arg(long)]
    causation_id: Option<CausationId>,
    /// A key consumers can deduplicate on.
    #[arg(long)]
    idempotency_key: Option<IdempotencyKey>,
}

#[derive(Debug, Args)]
struct PollArgs {
    /// The tenant to read from.
    #[arg(long)]
    tenant: TenantId,
    /// The topic to read.
    #[arg(long)]
    topic: Topic,
    /// Where to start; the first message is offset 0.
    #[arg(long, default_value_t = 0)]
    offset: u64,
    /// How many to read at most. The server decides if not given.
    #[arg(long)]
    count: Option<u32>,
}

fn parse_json(text: &str) -> Result<Value, String> {
    serde_json::from_str(text).map_err(|e| format!("not valid JSON: {e}"))
}

/// The envelope attributes to send. The server fills in the rest.
fn publish_body(args: &PublishArgs) -> Map<String, Value> {
    let mut body = Map::new();
    let mut put = |name: &str, value: Option<String>| {
        if let Some(value) = value {
            body.insert(name.to_owned(), Value::String(value));
        }
    };
    put("source", Some(args.source.to_string()));
    put("type", Some(args.event_type.to_string()));
    put("subject", args.subject.as_ref().map(ToString::to_string));
    put(
        "correlationid",
        args.correlation_id.as_ref().map(ToString::to_string),
    );
    put(
        "causationid",
        args.causation_id.as_ref().map(ToString::to_string),
    );
    put(
        "idempotencykey",
        args.idempotency_key.as_ref().map(ToString::to_string),
    );
    if let Some(data) = &args.data {
        put("datacontenttype", Some("application/json".to_owned()));
        body.insert("data".to_owned(), data.clone());
    }
    body
}

#[expect(clippy::print_stdout, reason = "the CLI prints envelopes to stdout")]
async fn run(cli: Cli) -> anyhow::Result<()> {
    let client = Client::from_lookup(|name| std::env::var(name).ok())?;
    match cli.command {
        Command::Publish(args) => {
            let published = client
                .publish(&args.tenant, &args.topic, &publish_body(&args))
                .await?;
            println!("{}", serde_json::to_string(&published.envelope)?);
        }
        Command::Poll(args) => {
            let polled = client
                .poll(&args.tenant, &args.topic, args.offset, args.count)
                .await?;
            for stored in polled.messages {
                println!("{}", serde_json::to_string(&stored.envelope)?);
            }
        }
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mansio: {error:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("mansio").chain(args.iter().copied()))
    }

    #[test]
    fn publish_sends_only_what_was_given() {
        let cli = parse(&[
            "publish",
            "--tenant",
            "acme",
            "--topic",
            "facts.build",
            "--type",
            "build.done",
            "--source",
            "ci",
            "--data",
            r#"{"ok":true}"#,
            "--correlation-id",
            "c1",
        ])
        .unwrap();
        let Command::Publish(args) = cli.command else {
            panic!("not publish");
        };
        assert_eq!(
            Value::Object(publish_body(&args)),
            json!({
                "source": "ci",
                "type": "build.done",
                "correlationid": "c1",
                "datacontenttype": "application/json",
                "data": {"ok": true}
            })
        );
    }

    #[test]
    fn bad_arguments_are_refused_before_any_request() {
        for args in [
            &[
                "publish", "--tenant", "Acme", "--topic", "alerts", "--type", "t", "--source", "s",
            ][..],
            &[
                "publish", "--tenant", "acme", "--topic", "nope", "--type", "t", "--source", "s",
            ][..],
            &[
                "publish", "--tenant", "acme", "--topic", "alerts", "--type", "t", "--source", "s",
                "--data", "{",
            ][..],
            &[
                "publish", "--tenant", "acme", "--topic", "alerts", "--source", "s",
            ][..],
            &["poll", "--tenant", "acme"][..],
            &[
                "poll", "--tenant", "acme", "--topic", "alerts", "--offset", "-1",
            ][..],
        ] {
            assert!(parse(args).is_err(), "{args:?}");
        }
    }

    #[test]
    fn poll_defaults_to_the_start() {
        let cli = parse(&["poll", "--tenant", "acme", "--topic", "alerts"]).unwrap();
        let Command::Poll(args) = cli.command else {
            panic!("not poll");
        };
        assert_eq!((args.offset, args.count), (0, None));
    }
}
