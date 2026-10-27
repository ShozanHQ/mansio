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
//!
//! [`Webhooks`] deliver the envelopes of a topic, signed, to the addresses
//! the admin API registered, and `GET /v1/events` serves a topic as
//! server-sent events.
//!
//! [`Credentials`] issue and withdraw the credentials of principals, each a
//! user of the broker with permissions on its tenant's stream and the topics its
//! scopes cover, and say who a bearer token is. The admin API for them is in
//! the routes of [`Server`].

mod app;
mod auth;
mod clock;
mod consumers_api;
mod credentials;
mod engine;
mod error;
mod events_api;
mod facts;
mod principals_api;
mod recover;
mod registry;
mod settings;
mod sink;
mod state;
mod tail;
mod webhooks;
mod webhooks_api;
mod work_api;

pub use app::{AppState, Parts, Server};
pub use auth::{Actor, Auth, Authenticated};
pub use clock::{Clock, SystemClock};
pub use credentials::Credentials;
pub use engine::{Engine, Holder};
pub use error::ApiError;
pub use settings::{
    Backend, CredentialSettings, DEFAULT_LISTEN, DEFAULT_SINK_PATH, DEFAULT_TOKEN_PRINCIPAL,
    Limits, Settings, SettingsError, StreamSettings, WebhookSettings, WorkSettings,
};
pub use sink::FileSink;
pub use webhooks::Webhooks;
