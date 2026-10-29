//! The mansio HTTP server.
//!
//! A host application embeds it by building a [`Server`] over its own
//! [`Broker`](mansio_core::Broker) and [`Sink`](mansio_core::Sink) and
//! calling [`Server::serve`]. The `mansio-server` binary does that with the
//! settings in [`Settings`] and a JSON Lines [`FileSink`].
//!
//! Besides publishing and polling, the server runs the [`Engine`]: work items
//! with claim, lease, acknowledgement, retry with backoff and a dead topic,
//! and alerts with acknowledgement. Every transition is recorded in the sink
//! and published to `facts.work` or `facts.alert`.

mod app;
mod auth;
mod clock;
mod engine;
mod error;
mod facts;
mod recover;
mod settings;
mod sink;
mod state;
mod work_api;

pub use app::{AppState, Server};
pub use auth::{Auth, Authenticated};
pub use clock::{Clock, SystemClock};
pub use engine::Engine;
pub use error::ApiError;
pub use settings::{
    Backend, DEFAULT_LISTEN, DEFAULT_SINK_PATH, DEFAULT_TOKEN_PRINCIPAL, Limits, Settings,
    SettingsError, WorkSettings,
};
pub use sink::FileSink;
