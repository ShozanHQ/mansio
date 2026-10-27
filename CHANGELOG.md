# Changelog

All notable changes to mansio are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Added

- The Cargo workspace: `mansio-core`, `mansio-iggy`, `mansio-server`, `mansio`
  (the CLI) and `mansio-e2e`, on a pinned Rust 1.99.0 toolchain with
  `cargo-deny` and a lint profile that denies `unwrap`, `expect` and `panic`
  outside tests.
- The envelope (CloudEvents 1.0 JSON plus tenant, principal, topic,
  correlation, causation, idempotency key, scope, budget, approval state, an
  optional ledger record, and work item id and attempt) and the topic types
  `facts.<kind>`, `work.<queue>`, `alerts`, `presence` and `dead`, parsed once
  at the edge.
- The `Broker` and `Sink` traits, `MemoryBroker`, and `IggyBroker` over
  Apache Iggy (one stream per tenant, one partition per topic).
- `mansio-server`: `GET /healthz`, publish and poll under
  `/v1/tenants/{tenant}/topics/{topic}/messages`, a bearer token mapped to a
  principal, and a JSON Lines file sink. It runs in-process for a host
  application.
- `mansio publish` and `mansio poll`.
- `compose.yaml` and a `Dockerfile` that start Iggy 0.9.0 and mansio-server.
- CI: format, clippy, test, `cargo-deny`, licence and naming checks, and an
  end-to-end run through Iggy.
- `MANSIO_IGGY_DURABILITY` and `MANSIO_IGGY_PRESENCE_DURABILITY`, choosing
  `persisted` or `replicated` for the durable topics and for `presence`.
- The publish answer's `offset` is optional: left out when Iggy does not say
  where the message landed and it is not among the recent ones.
- Work items over the HTTP API (`work_publish`, `work_pull`, `work_ack`,
  `work_fail`): a claim is a lease, a fail or an expired lease republishes the
  item one attempt on after a doubling backoff, and past the attempt cap it goes
  to `dead` and nowhere else. Alerts (`alerts_publish`, `alerts_ack`) are
  acknowledged once and surfaced again each interval until they are. Every
  transition is an envelope on `facts.work` or `facts.alert`, recorded in the
  sink first. A restarted server rebuilds its state from the topics.
- `GET /v1/settings`, and the settings `MANSIO_LEASE_SECONDS`,
  `MANSIO_MAX_ATTEMPTS`, `MANSIO_BACKOFF_BASE_SECONDS`,
  `MANSIO_BACKOFF_CAP_SECONDS`, `MANSIO_ALERT_RESURFACE_SECONDS` and
  `MANSIO_TICK_MILLIS`.
- `Broker::tenants` and `Broker::topics`, so a server can find what to take up
  again on start.
- Credentials. The admin API (`principals_create`, `principals_list`,
  `principals_revoke`: `POST /v1/principals`, `GET /v1/principals?tenant=`,
  `DELETE /v1/principals/{id}`) issues a credential to a principal with scopes
  (topic patterns and the verbs `publish`, `subscribe`, `claim`, `ack`,
  `approve`). The credential is a personal access token of an Iggy user with
  permissions on exactly its tenant's stream and the topics its scopes cover,
  so the broker refuses the rest. It expires (an agent's always does), a
  revocation deletes the user, and a sweep revokes the expired. Each issue and
  revoke is recorded on `facts.principal` and in the sink first, and the
  principals are kept in a log in the broker that no credential can read.
- `MemoryBroker` holds users, scopes and expiring credentials as Iggy does, so
  credentials are tested without Iggy.
- `mansio-server provision-admin`, which makes the Iggy user the server acts
  as. `compose.yaml` runs it as `iggy-admin`.
- The settings `MANSIO_AGENT_CREDENTIAL_TTL_SECONDS`,
  `MANSIO_CREDENTIAL_RECHECK_SECONDS` and `MANSIO_CREDENTIAL_SWEEP_SECONDS`,
  and `MANSIO_E2E_IGGY_ADDRESS` for the tests that ask Iggy itself.

### Changed

- Every durable publish holds a per-tenant lock across the sink record and the
  broker publish, so the sink sees a tenant's messages in the order its topics
  hold them.
- The generic messages route refuses `work.<queue>`, `facts.work` and
  `facts.alert`.
- An item id and an alert id are used once: resending one the queue or tenant
  has had is a 409.
- After a publish fails, which may still have stored the message, the engine
  rebuilds the tenant's state from its topics.
- `Server::with_work_settings` checks the settings and returns an error.
- `compose.yaml` passes the work settings through, and keeps the sink file in
  `MANSIO_SINK_DIR` when it is set.- Every request is authenticated by its bearer credential and acts through the
  broker as that credential; the bootstrap `MANSIO_TOKEN` is the admin's.
  `Broker::publish` and `Broker::poll` take a `Caller`, and `Server::build`
  returns the routes, the engine and the credentials as `Parts`.
- A read no longer makes a stream or a topic: a topic nobody has published to is
  a 404. The generic messages route also refuses `facts.principal`.
- mansio-server no longer acts as Iggy's root user. `MANSIO_IGGY_USERNAME`
  defaults to `mansio`, the user `iggy-admin` makes; the root password is given
  to Iggy and `iggy-admin` only. `compose.yaml` publishes Iggy's TCP port on
  loopback.
