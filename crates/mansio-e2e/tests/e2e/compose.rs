//! The two-server Compose profile: the lines that make the second server
//! refuse to start without ownership, and the command that sets it.
//!
//! There is no YAML reader in the workspace, so these read the files as text
//! and check lines.

use std::fs;
use std::io;
use std::path::Path;

const COMMAND: &str = "MANSIO_OWNER_TTL_SECONDS=30 docker compose --profile ha up --build --wait";

fn read(file: &str) -> io::Result<String> {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(file),
    )
}

/// The text from the line that starts `start` up to the line that starts `end`.
fn block<'a>(text: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let rest = &text[text.find(start)?..];
    let to = rest.get(start.len()..)?.find(end)? + start.len();
    rest.get(..to)
}

#[test]
fn the_second_server_expects_peers() {
    let compose = read("compose.yaml").unwrap();
    let second = block(&compose, "\n  mansio-server-b:", "\nvolumes:").unwrap();
    assert!(second.contains("MANSIO_EXPECT_PEERS: \"true\""));
    assert!(!second.contains("MANSIO_OWNER_TTL_SECONDS:"));
}

#[test]
fn the_first_server_passes_both_settings_through() {
    let compose = read("compose.yaml").unwrap();
    let first = block(
        &compose,
        "\n  mansio-server: &mansio-server",
        "\n  mansio-server-b:",
    )
    .unwrap();
    assert!(first.contains("MANSIO_EXPECT_PEERS: ${MANSIO_EXPECT_PEERS:-}"));
    assert!(first.contains("MANSIO_OWNER_TTL_SECONDS: ${MANSIO_OWNER_TTL_SECONDS:-}"));
}

#[test]
fn the_documented_command_sets_the_owner_ttl() {
    assert!(read("compose.yaml").unwrap().contains(COMMAND));
    assert!(read("README.md").unwrap().contains(COMMAND));
}
