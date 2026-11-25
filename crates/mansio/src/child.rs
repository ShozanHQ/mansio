//! A command the CLI runs for a message: started with the message as one
//! line on its standard input, its output on the CLI's standard error, then
//! waited for or killed.
//!
//! The command never holds the CLI's credential: `MANSIO_TOKEN` is removed
//! from its environment. Both of its output streams go to standard error, so
//! that what the CLI prints on standard output stays its own.

use std::process::{ExitStatus, Stdio};

use anyhow::{Context, bail};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};

/// A command that has been started.
#[derive(Debug)]
pub struct Running {
    child: Child,
}

/// Start `argv`, its program first, with `env` added to its environment and
/// `input` written to its standard input, which is then closed.
///
/// # Errors
///
/// Fails with the system's reason if the program cannot be started: not
/// found, or not executable.
pub fn start(argv: &[String], env: &[(&str, &str)], input: Vec<u8>) -> anyhow::Result<Running> {
    let Some((program, rest)) = argv.split_first() else {
        bail!("there is no command to run");
    };
    let mut child = Command::new(program)
        .args(rest)
        .env_remove("MANSIO_TOKEN")
        .envs(env.iter().copied())
        .stdin(Stdio::piped())
        .stdout(std::io::stderr())
        .stderr(std::io::stderr())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("the command {program} could not be started"))?;
    // Written from a task of its own, so that a command that does not read
    // its input is not waited on to read it. A command that has closed its
    // input (or exited) has not failed for that.
    if let Some(mut stdin) = child.stdin.take() {
        tokio::spawn(async move {
            let _ = stdin.write_all(&input).await;
            let _ = stdin.shutdown().await;
        });
    }
    Ok(Running { child })
}

impl Running {
    /// Wait for the command to end.
    ///
    /// # Errors
    ///
    /// Fails if the system cannot say how it ended.
    pub async fn wait(&mut self) -> anyhow::Result<ExitStatus> {
        self.child.wait().await.context("waiting for the command")
    }

    /// Kill the command and reap it, so that it is not left running.
    ///
    /// # Errors
    ///
    /// Fails if the system refuses the kill.
    pub async fn kill(&mut self) -> anyhow::Result<()> {
        self.child.kill().await.context("killing the command")
    }
}
