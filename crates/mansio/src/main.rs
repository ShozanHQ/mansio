//! The mansio command line and MCP server.
//!
//! Every operation of the bus is a command (`mansio work pull`) and a tool
//! (`work_pull`) with the same name and the same parameters, and both are
//! clients of the HTTP API. `mansio mcp` serves the tools on stdio.

mod cli;
mod client;
mod mcp;
mod ops;
mod watch;

use std::process::ExitCode;

use anyhow::Context;
use cli::Chosen;
use client::Client;

#[expect(clippy::print_stdout, reason = "the CLI prints its results to stdout")]
fn print(lines: &[serde_json::Value]) {
    for line in lines {
        println!("{line}");
    }
}

async fn run() -> anyhow::Result<()> {
    let matches = cli::command().get_matches();
    let chosen = cli::chosen(&matches)?;
    // Nothing is sent without a token, and a command that has none says so
    // before it does anything.
    let client = Client::from_lookup(cli::connection(&matches), |name| std::env::var(name).ok())?;
    match chosen {
        Chosen::Mcp => mcp::serve(client).await.context("serving MCP"),
        Chosen::Op(ops::Op::Watch, args) => {
            let watched = ops::Args::check(ops::Op::Watch, &args)?.watch()?;
            watch::watch(&client, watched).await
        }
        Chosen::Op(op, args) => {
            print(&ops::run(&client, op, &args).await?);
            Ok(())
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mansio: {error:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod parity {
    //! The command line and the MCP server offer the same operations.
    //!
    //! The two are built from one description of the operations, and this
    //! reads them back from what each actually offers, so that a change to
    //! either that drops an operation or a parameter fails here. The
    //! exemptions are named: `watch` streams, which a tool call cannot, and
    //! `mcp` is what serves the tools.

    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::Value;

    use super::*;

    /// Commands that are not tools.
    const COMMAND_ONLY: [&str; 2] = ["watch", "mcp"];

    /// What a command offers: its flags, and which of them are required.
    #[derive(Debug, PartialEq, Eq)]
    struct Offer {
        parameters: BTreeSet<String>,
        required: BTreeSet<String>,
    }

    /// The commands of the CLI by operation name, found by walking the tree.
    fn commands(
        command: &clap::Command,
        path: &mut Vec<String>,
        found: &mut BTreeMap<String, Offer>,
    ) {
        let mut any = false;
        for sub in command.get_subcommands() {
            any = true;
            path.push(sub.get_name().to_owned());
            commands(sub, path, found);
            path.pop();
        }
        if any || path.is_empty() {
            return;
        }
        let own = |arg: &&clap::Arg| {
            !arg.is_global_set()
                && arg.get_long().is_some()
                && arg.get_id() != "help"
                && arg.get_id() != "version"
        };
        let flags: Vec<&clap::Arg> = command.get_arguments().filter(own).collect();
        found.insert(
            path.join("_"),
            Offer {
                parameters: flags
                    .iter()
                    .filter_map(|arg| arg.get_long())
                    .map(|long| long.replace('-', "_"))
                    .collect(),
                required: flags
                    .iter()
                    .filter(|arg| arg.is_required_set())
                    .filter_map(|arg| arg.get_long())
                    .map(|long| long.replace('-', "_"))
                    .collect(),
            },
        );
    }

    fn tool_offers() -> BTreeMap<String, Offer> {
        let names = |value: &Value| -> BTreeSet<String> {
            value
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default()
        };
        mcp::tools()
            .iter()
            .map(|tool| {
                let schema = &tool["inputSchema"];
                (
                    tool["name"].as_str().unwrap_or_default().to_owned(),
                    Offer {
                        parameters: schema["properties"]
                            .as_object()
                            .map(|properties| properties.keys().cloned().collect())
                            .unwrap_or_default(),
                        required: names(&schema["required"]),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn the_command_line_and_the_mcp_tools_offer_the_same_operations() {
        let mut cli = BTreeMap::new();
        commands(&cli::command(), &mut Vec::new(), &mut cli);
        let tools = tool_offers();

        let only_commands: BTreeSet<&str> = cli
            .keys()
            .map(String::as_str)
            .filter(|name| !tools.contains_key(*name))
            .collect();
        assert_eq!(
            only_commands,
            BTreeSet::from(COMMAND_ONLY),
            "the commands that are not tools"
        );
        let only_tools: Vec<&String> = tools
            .keys()
            .filter(|name| !cli.contains_key(*name))
            .collect();
        assert!(
            only_tools.is_empty(),
            "tools with no command: {only_tools:?}"
        );
        for (name, tool) in &tools {
            assert_eq!(
                Some(tool),
                cli.get(name),
                "{name}: the flags and the properties differ"
            );
        }
        assert!(tools.len() >= 15, "{} tools", tools.len());
    }

    #[test]
    fn the_tools_have_the_names_the_ticket_lists() {
        let tools = tool_offers();
        for name in [
            "publish",
            "poll",
            "work_publish",
            "work_pull",
            "work_ack",
            "work_fail",
            "alerts_publish",
            "alerts_ack",
            "principals_create",
            "principals_list",
            "principals_revoke",
            "webhooks_create",
            "webhooks_list",
            "webhooks_delete",
            "consumers_get",
            "consumers_commit",
        ] {
            assert!(tools.contains_key(name), "{name}");
        }
    }
}
