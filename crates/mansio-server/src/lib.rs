//! The mansio HTTP server.
//!
//! A host application embeds it by building a [`Server`] over its own
//! [`Broker`](mansio_core::Broker) and [`Sink`](mansio_core::Sink) and
//! calling [`Server::serve`]. The `mansio-server` binary does that with the
//! settings in [`Settings`] and a JSON Lines [`FileSink`].

mod app;
mod auth;
mod error;
mod settings;
mod sink;

pub use app::{AppState, Server};
pub use auth::{Auth, Authenticated};
pub use error::ApiError;
pub use settings::{
    Backend, DEFAULT_LISTEN, DEFAULT_SINK_PATH, DEFAULT_TOKEN_PRINCIPAL, Limits, Settings,
    SettingsError,
};
pub use sink::FileSink;
