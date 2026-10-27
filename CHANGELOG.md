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
- `MANSIO_IGGY_SEGMENT_SIZE_BYTES`, the segment size of the topics mansio
  creates. Iggy applies retention to whole segments that have filled and
  rolled.
- Two-pass end-to-end tests that restart mansio-server between the passes, run
  by the CI `e2e` job against Iggy: a queue longer than a page with a claimed
  item, and an `alerts` topic that retention has trimmed.
- Credentials. The admin API (`principals_create`, `principals_list`,
  `principals_revoke`: `POST /v1/principals`, `GET /v1/principals?tenant=`,
  `DELETE /v1/principals/{id}`) issues a credential to a principal with scopes
  (topic patterns and the verbs `publish`, `subscribe`, `claim`, `ack`,
  `approve`). The credential is a personal access token of an Iggy user that
  may read exactly the topics of its tenant that its scopes cover and send to
  nothing: every write goes through the server, which checks the `publish`
  scope, stamps the principal from the credential and writes as its own admin
  identity. A scope may not grant `publish` on a topic only mansio writes
  (`facts.work`, `facts.alert`, `facts.principal`, `dead`). The credential
  expires (an agent's always does), a revocation deletes the user, and a sweep
  revokes the expired. Each issue and revoke is recorded on `facts.principal`
  and in the sink first, and the principals are kept in a log in the broker
  that no credential can read. On start the server deletes every broker user it
  made for a principal that no record owns.
- `MemoryBroker` holds users, scopes and expiring credentials as Iggy does, so
  credentials are tested without Iggy.
- `mansio-server provision-admin`, which makes the Iggy user the server acts
  as. `compose.yaml` runs it as `iggy-admin`.
- The settings `MANSIO_AGENT_CREDENTIAL_TTL_SECONDS`,
  `MANSIO_CREDENTIAL_RECHECK_SECONDS` and `MANSIO_CREDENTIAL_SWEEP_SECONDS`,
  and `MANSIO_E2E_IGGY_ADDRESS` for the tests that ask Iggy itself.
- The CLI has a command for every operation, named as the operation with `_`
  as a space (`publish`, `poll`, `watch`, `work publish | pull | ack | fail`,
  `alerts publish | ack`, `principals create | list | revoke`,
  `webhooks create | list | delete`, `consumers get | commit`), `--url` and
  `--token`, JSON out one object to a line, and no request without a token.
  `work pull --wait` looks until an item can be claimed. `mansio watch`
  streams a topic and, with `--consumer`, resumes where it stopped with
  nothing missed and nothing twice.
- `mansio mcp`, an MCP server on stdio that offers every command but `watch`
  as a tool with the same name and the same parameters, and docs for Claude
  Code and Codex. The CLI and MCP retry a request that failed on the network.
- Named durable consumers: `consumers_get` and `consumers_commit`, an offset
  per tenant, topic, principal and name that mansio keeps in a log of its own
  stream in the broker, never in Iggy's consumer offsets. A commit past the
  last message of the topic is a 409.
- `GET /v1/events`, a topic as server-sent events: `id` is the offset, `event`
  the envelope's type, `data` the envelope. It resumes after `Last-Event-ID`,
  and a shutdown ends every stream and waits at most
  `MANSIO_SHUTDOWN_GRACE_SECONDS` for the rest.
- Webhooks (`webhooks_create`, `webhooks_list`, `webhooks_delete`, admin only):
  each envelope of a topic is POSTed in order as CloudEvents JSON with a
  `mansio-signature` HMAC-SHA256 header, from a durable consumer, retried with
  a backoff, and recorded as `mansio.webhook.failed` on `facts.webhook` when
  given up on. Secrets are kept on the server's own `webhooks` log.
- The settings `MANSIO_STREAM_POLL_MILLIS`, `MANSIO_SSE_KEEPALIVE_SECONDS`,
  `MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS`, `MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS`,
  `MANSIO_WEBHOOK_MAX_ATTEMPTS`, `MANSIO_WEBHOOK_TIMEOUT_SECONDS` and
  `MANSIO_SHUTDOWN_GRACE_SECONDS` for the server, and `MANSIO_RETRIES`,
  `MANSIO_RETRY_BACKOFF_MILLIS` and `MANSIO_POLL_INTERVAL_MILLIS` for the CLI.

### Changed

- A `work_pull` by a consumer that holds a live lease on the queue returns that
  lease again instead of claiming another item, so a claim whose answer was
  lost can be asked for again. A server restarted since the claim cannot say
  the lease id again, and gives a new one for the same item and expiry.
- A broker error is a 502 only when it is a refusal the broker named before it
  stores anything (not found, unauthorized, an invalid identifier, a message
  count or payload size it refuses). Every other Iggy error is a 503, because
  the Iggy client writes a request before it reads the reply, and a reply it
  cannot read says nothing about whether the write was applied. A 503 is
  resent with the same id, and the engine rebuilds the tenant's state after
  one.
- Every durable publish holds a per-tenant lock across the sink record and the
  broker publish, so the sink sees a tenant's messages in the order its topics
  hold them.
- The generic messages route refuses `work.<queue>`, `facts.work`,
  `facts.alert` and `dead`.
- An item id and an alert id are used once: resending one the queue or tenant
  has had is a 409.
- After a publish fails, which may still have stored the message, the engine
  rebuilds the tenant's state from its topics.
- `Server::with_work_settings` checks the settings and returns an error.
- `compose.yaml` passes the work settings through, and keeps the sink file in
  `MANSIO_SINK_DIR` when it is set. The health check of mansio-server has a
  start period of `MANSIO_HEALTH_START_PERIOD` (60s), because a start replays
  every tenant's topics before `/healthz` answers.
- Every request is authenticated by its bearer credential; the bootstrap
  `MANSIO_TOKEN` is the admin's. Reads go through the broker as that
  credential. `Broker::publish` and `Broker::poll` take a `Caller`, and a
  credential's publish is refused by the broker. `Server::build` returns the
  routes, the engine and the credentials as `Parts`.
- The `facts.work` facts record a fingerprint of a lease (`lease`), never its
  id, which is the holder's proof and was readable by every subscriber of
  `facts.*`. The holder is the principal, the consumer name and the lease id:
  `work_ack` and `work_fail` name the `consumer`, and `work_pull` answers with
  it.
- A read no longer makes a stream or a topic: a topic nobody has published to is
  a 404. The generic messages route also refuses `facts.principal` and `dead`.
- mansio-server no longer acts as Iggy's root user. `MANSIO_IGGY_USERNAME`
  defaults to `mansio`, the user `iggy-admin` makes; the root password is given
  to Iggy and `iggy-admin` only. `compose.yaml` publishes Iggy's TCP port on
  loopback.
