//! The command line, built from the operations: `mansio work pull`,
//! `mansio webhooks list` and the rest, each flag a parameter.

use clap::{Arg, ArgMatches, Command, value_parser};
use serde_json::{Map, Value};

use crate::client::Connection;
use crate::ops::{Kind, Op};

/// The command that serves the operations as MCP tools on stdio.
pub const MCP: &str = "mcp";

/// What the command line asks for.
#[derive(Debug)]
pub enum Chosen {
    /// An operation and its arguments.
    Op(Op, Map<String, Value>),
    /// The MCP server.
    Mcp,
}

/// The groups of commands, in the order they are listed.
const GROUPS: [(&str, &str); 5] = [
    (
        "work",
        "Work items: publish, claim, complete and fail them.",
    ),
    ("alerts", "Alerts: publish and acknowledge them."),
    (
        "principals",
        "Credentials: issue, list and withdraw them. Admin only.",
    ),
    (
        "webhooks",
        "Webhooks: deliver a topic, signed, to a URL. Admin only.",
    ),
    (
        "consumers",
        "Named consumers: where each one stands on a topic.",
    ),
];

fn parse_json(text: &str) -> Result<Value, String> {
    serde_json::from_str(text).map_err(|e| format!("not valid JSON: {e}"))
}

fn leaf(op: Op, name: &'static str) -> Command {
    let mut command = Command::new(name).about(op.about());
    for param in op.params() {
        let mut arg = Arg::new(param.name)
            .long(param.flag())
            .value_name(param.name.to_uppercase())
            .help(param.help)
            .required(param.required);
        arg = match param.kind {
            Kind::Text => arg,
            Kind::Int => arg.value_parser(value_parser!(u64)),
            Kind::Json | Kind::Scopes => arg.value_parser(parse_json),
        };
        command = command.arg(arg);
    }
    command
}

/// The whole command line.
pub fn command() -> Command {
    let mut root = Command::new("mansio")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Publish to, read from and work the mansio bus")
        .long_about(
            "Publish to, read from and work the mansio bus.\n\n\
             Every command talks to the server at MANSIO_URL (or --url) with the bearer \
             token in MANSIO_TOKEN (or --token), and prints JSON, one object per line.",
        )
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(
            Arg::new("url")
                .long("url")
                .global(true)
                .value_name("URL")
                .help("The server. Default: $MANSIO_URL, else http://127.0.0.1:7070."),
        )
        .arg(
            Arg::new("token")
                .long("token")
                .global(true)
                .value_name("TOKEN")
                .help("The bearer token. Default: $MANSIO_TOKEN. Without one nothing is sent."),
        );
    let mut groups: Vec<Command> = GROUPS
        .iter()
        .map(|(name, about)| {
            Command::new(*name)
                .about(*about)
                .subcommand_required(true)
                .arg_required_else_help(true)
        })
        .collect();
    for op in Op::ALL {
        match *op.path() {
            [name] => root = root.subcommand(leaf(op, name)),
            [group, name] => {
                if let Some(at) = GROUPS.iter().position(|(known, _)| *known == group) {
                    let taken = std::mem::replace(&mut groups[at], Command::new(group));
                    groups[at] = taken.subcommand(leaf(op, name));
                }
            }
            _ => {}
        }
    }
    for group in groups {
        root = root.subcommand(group);
    }
    root.subcommand(Command::new(MCP).about(
        "Serve the operations as MCP tools on stdin and stdout, for an agent's harness to run.",
    ))
}

/// The connection flags.
pub fn connection(matches: &ArgMatches) -> Connection {
    Connection {
        url: matches.get_one::<String>("url").cloned(),
        token: matches.get_one::<String>("token").cloned(),
    }
}

/// Which operation was asked for, and with what.
///
/// # Errors
///
/// Fails if the command line names a command this build does not know, which
/// the parser has already refused.
pub fn chosen(matches: &ArgMatches) -> anyhow::Result<Chosen> {
    let mut path: Vec<&str> = Vec::new();
    let mut current = matches;
    while let Some((name, inner)) = current.subcommand() {
        path.push(name);
        current = inner;
    }
    if path == [MCP] {
        return Ok(Chosen::Mcp);
    }
    let op = Op::from_path(&path)
        .ok_or_else(|| anyhow::anyhow!("{} is not a command", path.join(" ")))?;
    let mut args = Map::new();
    for param in op.params() {
        let value = match param.kind {
            Kind::Text => current
                .get_one::<String>(param.name)
                .map(|text| Value::String(text.clone())),
            Kind::Int => current.get_one::<u64>(param.name).map(|n| Value::from(*n)),
            Kind::Json | Kind::Scopes => current.get_one::<Value>(param.name).cloned(),
        };
        if let Some(value) = value {
            args.insert(param.name.to_owned(), value);
        }
    }
    Ok(Chosen::Op(op, args))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse(args: &[&str]) -> Result<ArgMatches, clap::Error> {
        command().try_get_matches_from(std::iter::once("mansio").chain(args.iter().copied()))
    }

    #[test]
    fn the_command_line_is_consistent() {
        command().debug_assert();
    }

    #[test]
    fn publish_collects_what_was_given() {
        let matches = parse(&[
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
        let Chosen::Op(op, args) = chosen(&matches).unwrap() else {
            panic!("not an operation");
        };
        assert_eq!(op, Op::Publish);
        assert_eq!(
            Value::Object(args),
            json!({
                "tenant": "acme",
                "topic": "facts.build",
                "type": "build.done",
                "source": "ci",
                "data": {"ok": true},
                "correlation_id": "c1",
            })
        );
    }

    #[test]
    fn nested_commands_and_numbers_are_found() {
        let matches = parse(&[
            "work",
            "pull",
            "--tenant",
            "acme",
            "--queue",
            "builds",
            "--consumer",
            "w1",
            "--wait",
            "5",
        ])
        .unwrap();
        let Chosen::Op(op, args) = chosen(&matches).unwrap() else {
            panic!("not an operation");
        };
        assert_eq!(op, Op::WorkPull);
        assert_eq!(args["wait"], json!(5));
        assert_eq!(args["consumer"], json!("w1"));
    }

    #[test]
    fn the_connection_flags_work_before_and_after_the_command() {
        for args in [
            &[
                "--url",
                "http://h:1",
                "--token",
                "t",
                "poll",
                "--tenant",
                "a",
                "--topic",
                "alerts",
            ][..],
            &[
                "poll",
                "--tenant",
                "a",
                "--topic",
                "alerts",
                "--url",
                "http://h:1",
                "--token",
                "t",
            ][..],
        ] {
            let matches = parse(args).unwrap();
            let connection = connection(&matches);
            assert_eq!(connection.url.as_deref(), Some("http://h:1"));
            assert_eq!(connection.token.as_deref(), Some("t"));
        }
    }

    #[test]
    fn mcp_is_a_command() {
        assert!(matches!(
            chosen(&parse(&["mcp"]).unwrap()).unwrap(),
            Chosen::Mcp
        ));
    }

    #[test]
    fn bad_arguments_are_refused_by_the_parser() {
        for args in [
            &[
                "publish", "--tenant", "acme", "--topic", "alerts", "--source", "s",
            ][..],
            &["poll", "--tenant", "acme"][..],
            &[
                "poll", "--tenant", "acme", "--topic", "alerts", "--offset", "-1",
            ][..],
            &[
                "publish", "--tenant", "acme", "--topic", "alerts", "--type", "t", "--source", "s",
                "--data", "{",
            ][..],
            &["work"][..],
            &["work", "nope"][..],
            &["nope"][..],
            &[][..],
        ] {
            assert!(parse(args).is_err(), "{args:?}");
        }
    }

    #[test]
    fn help_is_not_an_error_and_names_every_command() {
        let error = parse(&["--help"]).unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::DisplayHelp);
        let help = error.to_string();
        for word in [
            "publish",
            "poll",
            "watch",
            "work",
            "alerts",
            "principals",
            "webhooks",
            "consumers",
            "mcp",
        ] {
            assert!(help.contains(word), "{word} is not in the help");
        }
    }
}
