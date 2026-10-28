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
