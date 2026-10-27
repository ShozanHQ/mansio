# mansio architecture

mansio is a message bus for agents, people and programs that are not connected all the time. A producer publishes facts and work to a topic; a consumer that joins later picks up what is waiting, claims a work item exactly once, or acknowledges an alert. Apache Iggy is the broker. mansio adds what Iggy does not have: per-item claim with a lease, acknowledgement, retry with backoff, a dead topic, alert acknowledgement, and credentials the broker enforces.

## Crates

| Crate | Kind | Holds |
|---|---|---|
| `mansio-core` | lib | The envelope, tenant and principal ids, topics, work and alert types, the `Broker` and `Sink` traits, `MemoryBroker`, and the JSON bodies the HTTP API and its clients share. No network I/O. |
| `mansio-iggy` | lib | `IggyBroker`: the `Broker` trait over the Iggy Rust SDK (TCP), plus the admin calls for users, permissions and personal access tokens. |
| `mansio-server` | lib and bin | The HTTP API, the work and alert engine, credentials, server-sent events, webhooks. A host application embeds it as a library with its own `Sink`; the binary runs it standalone with the file sink. |
| `mansio` | bin | The CLI and the MCP server (`mansio mcp`, stdio). Both are clients of the HTTP API and use the same operation names. |
| `mansio-e2e` | test | End-to-end tests. With `MANSIO_E2E_URL` unset they start an in-process server on `MemoryBroker`; with it set they run against a live server and Iggy (CI, through compose). |

## The envelope

CloudEvents 1.0 in its JSON format. Required: `specversion` (`1.0`), `id` (a lowercase hyphenated UUID, never nil; also the Iggy message id), `source`, `type`. Optional CloudEvents attributes: `subject`, `time` (RFC 3339, kept as written), `datacontenttype` (only `application/json`), `data` (JSON). On publish the server fills in `specversion`, `id` and `time` when the producer left them out. Iggy does not deduplicate on the message id, so a resend is stored again: consumers deduplicate on `id` and `idempotencykey`.

Extension attributes, all scalar as CloudEvents requires:

| Attribute | Type | Meaning |
|---|---|---|
| `tenant` | string | The tenant; the Iggy stream. Required. |
| `topic` | string | The topic it was published to. Required. |
| `principalkind`, `principalname` | string | Who published it, taken from the credential, never from the request body. Required on anything the server accepts. A kind matches `[a-z][a-z0-9_-]{0,31}`, a name `[A-Za-z0-9][A-Za-z0-9._@:+-]{0,127}`; written together as `kind:name`. |
| `correlationid`, `causationid` | string | The chain of work this belongs to and the event that caused it. Text of up to 256 bytes, no control characters. |
| `idempotencykey` | string | A producer's key, carried so consumers can deduplicate on it as well as on `id`. |
| `scope` | string | The scope the action ran under, for example `work.builds:claim`. Up to 128 characters from `[a-z0-9._*:-]`. |
| `budget` | string | `<amount> <unit>`, for example `30 usd` or `200000 tokens`. The amount is a non-negative decimal of at most 24 characters, the unit a lowercase token. |
| `approvalstate` | string | `none`, `requested`, `approved` or `rejected`. |
| `recordid`, `recordprevhash`, `recordhash` | string | Set when the event came from a hash-chained log: its id there, the previous event's hash and its own (64 lowercase hex). All three or none. |
| `itemid`, `attempt` | string, integer | Work items: the stable id across retries, and the attempt number from 1. An attempt needs an item id. |

Unknown attributes are refused at the boundary. Ids, tenants, topics and principals are newtypes parsed once at the edge.

## Topics

A tenant is an Iggy stream. Its topics:

| Topic | Holds | Retention |
|---|---|---|
| `facts.<kind>` | Durable events: from a host's ledger, and every work and alert transition mansio makes (`facts.work`, `facts.alert`). | setting |
| `work.<queue>` | Claimable items: builds, reviews, approvals, connector reads. | setting |
| `alerts` | Escalations a person acknowledges. | setting |
| `dead` | Work items past the attempt cap, and nowhere else. | setting |
| `presence` | Heartbeats and progress. Ephemeral, short expiry, never sent to the sink. | setting, short |

Segments match `[a-z0-9][a-z0-9_-]{0,62}`; so does a tenant id. Each topic has one partition, so order is publish order, and an offset counts from 0. The Iggy stream is named by the tenant id and the Iggy topic by the topic name. Topics are created on first use, by a publish or a poll, by the server's admin identity, with a message retention set per kind of topic (`MANSIO_RETENTION_<KIND>_SECONDS`, 0 keeps forever).

## Work items

Iggy is offset-based. mansio-server runs one dispatcher per `work.<queue>`, reading in order under a named consumer whose offset Iggy stores.

- **Claim** hands the oldest unclaimed item to the caller with a lease: holder, item id, expiry (now plus `MANSIO_LEASE_SECONDS`). Another caller cannot see it while the lease holds.
- **Ack** by the holder completes it.
- **Fail** by the holder, or an expired lease, republishes the item to the same queue as a new message with the same `itemid` and `attempt` plus one, after a backoff of `MANSIO_BACKOFF_BASE_SECONDS * 2^(attempt-1)` capped at `MANSIO_BACKOFF_CAP_SECONDS`. Past `MANSIO_MAX_ATTEMPTS` it goes to `dead` and nowhere else.
- Every transition (`claimed`, `acked`, `failed`, `expired`, `retried`, `dead`) is an envelope on `facts.work`, recorded in the sink first and then published. The lease table is a projection of `facts.work`: a restarted server rebuilds it by replay, so a restart neither drops nor duplicates an item.

## Alerts

Published to `alerts`. `ack` by a principal records who and when as `facts.alert` `acknowledged`; a second ack is refused. An alert unacknowledged after `MANSIO_ALERT_RESURFACE_SECONDS` is surfaced again as `facts.alert` `resurfaced`.

## Sink

`Sink::record(&Envelope) -> Result<Option<Record>>` is called before every durable publish mansio makes, in order. A host returns the record it appended (id and hashes), which is set on the envelope before it is published, so subscribers can verify it against the host's log. The standalone binary uses a JSON Lines file sink (`MANSIO_SINK_PATH`) and returns no record. A record a producer put on the envelope is kept when the sink returns none and replaced when it returns one.

## Credentials

A principal is a kind (`person`, `agent`, `connector`, `service`, or another lowercase token), a name and a tenant, with scopes: topic patterns (`work.builds`, `facts.*`) and verbs (`publish`, `subscribe`, `claim`, `ack`, `approve`). The admin API mints an Iggy user with exactly the matching stream and topic permissions and a personal access token, which is the credential; an expiry on it is enforced by Iggy. The server acts for a caller through an Iggy client logged in with the caller's token, so Iggy refuses what the scopes do not allow. Revocation deletes the Iggy user. The server's own admin identity comes from the environment; configuration names variables, never values.

Until per-principal credentials exist, one bearer token (`MANSIO_TOKEN`), compared in constant time, stands for one principal (`MANSIO_TOKEN_PRINCIPAL`, default `service:mansio`) in every tenant. The server sets `tenant`, `topic`, `principalkind` and `principalname` from the path and that principal, and refuses a body that carries any of them.

## Surfaces

One HTTP API under `/v1`:

- `GET /healthz`, open: `{"status":"ok"}`.
- `POST /v1/tenants/{tenant}/topics/{topic}/messages`: the body is an envelope without the four attributes above; the answer is 201 with `{"offset", "envelope"}`, the envelope as stored.
- `GET /v1/tenants/{tenant}/topics/{topic}/messages?offset=N&count=M`: 200 with `{"messages": [{"offset", "envelope"}]}` from that offset, in publish order.

Every refusal is `{"error": {"code", "message"}}` with 400 (malformed), 401 (no valid token), 404, 413 (body too large), 502 (the broker refused) or 503 (the broker or the sink is unavailable). Everything but `/healthz` needs `Authorization: Bearer <token>`.

The CLI, the MCP server, server-sent events (`GET /v1/events`, resuming from `Last-Event-ID`) and signed webhooks (CloudEvents JSON, HMAC-SHA256 signature header, retry with backoff) all use it, with the same operation names.

## Settings

Every limit, interval and switch is a named setting read from the environment with a default, listed in the README. No constant stands in for a setting.

## Rust profile

One lint and toolchain profile for the workspace: `unsafe_code` denied; clippy `all` with warnings as errors in CI; `unwrap_used`, `expect_used`, `panic`, `todo`, `dbg_macro` denied, and `print_stdout` (the CLI binary expects it, with a reason); tests may unwrap. Exceptions are `#[expect(lint, reason = "...")]` on the item. Pinned `rust-toolchain.toml`, `rust-version` to match, `resolver = "3"`, `overflow-checks = true` in release, `--locked` everywhere, `cargo deny check` with a licence allowlist and crates.io as the only source. Typed errors (`thiserror` in libraries, `anyhow` only at a binary's edge), `#[serde(deny_unknown_fields)]` on request types, secrets in `secrecy` types compared in constant time.
