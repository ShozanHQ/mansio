//! The standalone mansio server.
//!
//! `mansio-server` serves. `mansio-server provision-admin` makes the Iggy
//! user the server acts as, using the root credentials from the environment
//! once, so that the server itself is never given them.

use std::io::IsTerminal;
use std::sync::Arc;

use anyhow::Context;
use mansio_core::{Broker, MemoryBroker, NoSink, Sink};
use mansio_iggy::{IggyBroker, Provisioned, provision_admin};
use mansio_server::{Auth, Backend, FileSink, Server, Settings};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match std::env::args().nth(1).as_deref() {
        None => serve().await,
        Some("provision-admin") => provision().await,
        Some(other) => anyhow::bail!("unknown command {other:?}; the only one is provision-admin"),
    }
}

/// Make the Iggy user the server acts as, or set its permissions again.
async fn provision() -> anyhow::Result<()> {
    let config = Settings::provision_from_env().context("reading settings")?;
    let user = config.admin_username.clone();
    let done = provision_admin(config)
        .await
        .context("making the Iggy admin user")?;
    match done {
        Provisioned::Created => info!(%user, "made the Iggy admin user"),
        Provisioned::Updated => info!(%user, "the Iggy admin user exists; its permissions are set"),
    }
    Ok(())
}

async fn serve() -> anyhow::Result<()> {
    let settings = Settings::from_env().context("reading settings")?;

    let broker_name = settings.broker.name();
    let broker: Arc<dyn Broker> = match settings.broker {
        Backend::Iggy(iggy) => Arc::new(
            IggyBroker::connect(iggy)
                .await
                .context("connecting to Iggy")?,
        ),
        Backend::Memory => Arc::new(MemoryBroker::new()),
    };
    let sink: Arc<dyn Sink> = match &settings.sink_path {
        Some(path) => Arc::new(
            FileSink::open(path)
                .await
                .with_context(|| format!("opening the sink file {}", path.display()))?,
        ),
        None => Arc::new(NoSink),
    };

    let listener = TcpListener::bind(settings.listen)
        .await
        .with_context(|| format!("listening on {}", settings.listen))?;
    info!(
        listen = %settings.listen,
        broker = broker_name,
        sink = ?settings.sink_path,
        principal = %settings.token_principal,
        "mansio-server started"
    );

    Server::new(broker, Auth::new(settings.token, settings.token_principal))
        .with_sink(sink)
        .with_limits(settings.limits)
        .with_work_settings(settings.work)
        .context("the work settings")?
        .with_credential_settings(settings.credentials)
        .serve(listener, shutdown_signal())
        .await
        .context("serving")?;
    info!("mansio-server stopped");
    Ok(())
}

/// Completes on Ctrl-C, or on SIGTERM where there is one.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
