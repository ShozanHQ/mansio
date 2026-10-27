# mansio architecture

mansio is a message bus for agents, people and programs that are not connected all the time. A producer publishes facts and work to a topic; a consumer that joins later picks up what is waiting, claims a work item exactly once, or acknowledges an alert. Apache Iggy is the broker. mansio adds what Iggy does not have: per-item claim with a lease, acknowledgement, retry with backoff, a dead topic, alert acknowledgement, and credentials the broker enforces.

## Crates

| Crate | Kind | Holds |
|---|---|---|
| `mansio-core` | lib | The envelope, tenant and principal ids, topics, scopes (topic patterns and verbs), the credential type, work and alert types, the `Broker` and `Sink` traits, `MemoryBroker`, and the JSON bodies the HTTP API and its clients share. No network I/O. |
| `mansio-iggy` | lib | `IggyBroker`: the `Broker` trait over the Iggy Rust SDK (TCP), with Iggy users, permissions and personal access tokens for credentials and a client per credential in use; and the making of the Iggy user the server acts as. |
| `mansio-server` | lib and bin | The HTTP API, the work and alert engine, credentials, server-sent events, webhooks. A host application embeds it as a library with its own `Sink`; the binary runs it standalone with the file sink, and `mansio-server provision-admin` makes the Iggy user it acts as. |
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
| `facts.<kind>` | Durable events: from a host's ledger, and every work and alert transition and every credential issued or revoked that mansio records (`facts.work`, `facts.alert`, `facts.principal`). | setting |
| `work.<queue>` | Claimable items: builds, reviews, approvals, connector reads. | setting |
| `alerts` | Escalations a person acknowledges. | setting |
| `dead` | Work items past the attempt cap, and nowhere else. | setting |
| `presence` | Heartbeats and progress. Ephemeral, short expiry, never sent to the sink. | setting, short |

Segments match `[a-z0-9][a-z0-9_-]{0,62}`; so does a tenant id. Each topic has one partition, so order is publish order, and an offset counts from 0. The Iggy stream is named by the tenant id and the Iggy topic by the topic name. Topics are created on first use, by a publish, by the server's admin identity; a read never creates a stream or a topic, and a read of one that does not exist is a 404. A topic is made with a message retention set per kind of topic (`MANSIO_RETENTION_<KIND>_SECONDS`, 0 keeps forever, and a topic that keeps forever has no size cap) and a durability: `persisted` for everything that goes through the sink (`MANSIO_IGGY_DURABILITY`), `replicated` for `presence` (`MANSIO_IGGY_PRESENCE_DURABILITY`). `persisted` makes Iggy confirm a write only once it is on stable storage, so a 201 survives a crash of the host. The Iggy backend remembers the streams and topics it has found or created, and checks again for a tenant when Iggy says one is missing.

## Work items

Iggy is offset-based: it has no per-message claim, acknowledgement, redelivery or dead letters. The engine in mansio-server builds them and records every transition. An item is named by the `id` of its first message, which `work_publish` sets as `itemid` with `attempt` 1; a caller cannot set either, and the generic messages route refuses `work.<queue>`, `facts.work`, `facts.alert` and `facts.principal`. An item id is used once: `work_publish` answers 409 for an id the queue has already had, open or done, so a resend of finished work does not run it again. The engine keeps the id of every finished item.

- **Claim** (`work_pull`) hands the oldest claimable item to the caller with a lease: a random lease id, the holder (the principal behind the credential and the consumer name from the request), the item and an expiry (now plus `MANSIO_LEASE_SECONDS`). Another caller cannot see it while the lease holds, and the claim is atomic.
- **Ack** (`work_ack`) by the holder, presenting the lease id, completes it. Another lease id or principal is refused with 403, an item with no live lease (never claimed, expired, waiting for its retry) with 409, an item that is not open with 404. Nothing changes then.
- **Fail** (`work_fail`) by the holder, or an expired lease, republishes the item to the same queue as a new message with the same `itemid`, `attempt` plus one and the message before as `causationid`, after a backoff of `MANSIO_BACKOFF_BASE_SECONDS * 2^(attempt-1)` capped at `MANSIO_BACKOFF_CAP_SECONDS`. When `attempt` plus one would pass `MANSIO_MAX_ATTEMPTS`, the item is published to `dead` at once instead, and nowhere else. Republished and dead messages are published by `service:mansio-engine`.
- Every transition (`claimed`, `acked`, `failed`, `expired`, `retried`, `dead`) is an envelope on `facts.work` with the item id as `subject` and the item id and attempt as attributes; `data` names the queue, message id, attempt, and as they apply the lease id, holder, consumer, expiry, reason and what happens next (`next`, `retry_at`). It is recorded in the sink first and then published.

A timer (`MANSIO_TICK_MILLIS`) drives expiries, republishes and the surfacing of alerts. It reads the clock through a trait, so tests move time by hand.

### State

The engine keeps, per tenant, the open items of each queue, the ids of finished ones (16 bytes each), their leases and the alerts, and the ids of acknowledged alerts. None of it is authoritative: it is a projection of `work.<queue>`, `facts.work`, `dead`, `alerts` and `facts.alert`. On start the server lists the tenants and topics the broker holds (`Broker::tenants`, `Broker::topics`), reads those topics from offset 0, and rebuilds the state; a tenant it first meets later is rebuilt on its first request. An item's newest message says what it is, and the transitions recorded for that attempt say where it stands: a lease still holds until its recorded expiry, a failure waits until its recorded `retry_at`. So a restart neither drops nor duplicates an item, and a lease that ended while the server was down is expired on start. Order is by position in the queue, not by offset, because Iggy does not always report one. A message that cannot be read is skipped with a warning naming tenant, topic and offset, and does not stop the tenant: the page it is in is read again one message at a time, and the next page whole.

A transition is written before the state changes, so a write that fails changes nothing. But a publish that fails because the broker could not be reached does not say the message was not stored (the connection may drop after the write), so after one the tenant's state is thrown away and rebuilt from the logs on its next use. A refusal is a definite answer and does not rebuild anything. The timer ends its pass for a tenant after the first such failure, so a broker that cannot be reached costs one rebuild per tick, however much is due. An alert keeps the time it was accepted or last surfaced through a rebuild. That finds a stored item, acknowledgement or claim whatever the caller was told.

Republishing an item and sending it to `dead` each publish a message and then record a fact, which cannot be one write. Between the two the item is not claimable and owes its fact; the timer finishes it, and so does a restart, which finds the message published and the fact missing and records the fact once. A message that was never published is published once. The fact that is owed is made from its message alone: its id is the message's id with a fixed pattern mixed in, and its time is the message's time. A restarted server makes the very fact the stopped one would have, so if the sink had already recorded it, the second record has the same `id` and a consumer that deduplicates on `id` sees one.

The timer works through what is due in a tenant one task at a time. A task that fails is logged and left for the next tick, and the others go on, so one item the broker keeps refusing does not hold up the rest.

The ids of finished items and acknowledged alerts are all that is kept of them, and they only grow while the server runs. A restart rebuilds them from the topics, so what bounds them is `MANSIO_RETENTION_WORK_SECONDS` and `MANSIO_RETENTION_ALERTS_SECONDS`; with the default of keeping forever they are unbounded, and an id older than the retention can be used again.

A publish that fails with an error about reaching the broker may have been stored, so a client of `work_publish` or `alerts_publish` that gets a 503 resends with the same `id`; a 409 then means the first attempt was stored. A refusal (a 502, the broker said no) is definite: nothing was stored and the state is left as it is.

The engine is one process's view of a broker: its lock and its state are in memory, so claims are atomic within one mansio-server. Run one mansio-server per broker; two on the same broker could give the same item to two consumers.

Retention must keep `facts.work` at least as long as `work.<queue>`: an item whose facts expired while its message did not is claimable again.

## Alerts

Published to `alerts` (`alerts_publish`, or the generic route to `alerts`). The alert is the message and its id is the message's `id`; an id is used once, and a resend of one the tenant has is refused with 409, so a resend cannot reopen an acknowledged alert. `alerts_ack` by a principal records who and when as `facts.alert` `acknowledged`; a second ack is refused with 409, and the first in the log is the one that counts. An alert unacknowledged `MANSIO_ALERT_RESURFACE_SECONDS` after it was published is surfaced again as `facts.alert` `resurfaced`, once per interval until it is acknowledged. The interval runs from the server's clock when it accepts the alert, not from the `time` the producer wrote; after a restart it runs from the last `resurfaced` fact, or else from the alert's own `time`, or from the restart if that is in the future. Once acknowledged, an alert is kept in memory as its id only.

## Sink

`Sink::record(&Envelope) -> Result<Option<Record>>` is called before every durable publish mansio makes, in order. A host returns the record it appended (id and hashes), which is set on the envelope before it is published, so subscribers can verify it against the host's log. The standalone binary uses a JSON Lines file sink (`MANSIO_SINK_PATH`) and returns no record. A record a producer put on the envelope is kept when the sink returns none and replaced when it returns one.

Record and publish happen under one async lock per tenant, so the sink sees a tenant's messages in the order its topics hold them, whichever route they took. A failed publish after a record leaves the record in the sink with nothing on the topic.

## Credentials

A principal is a kind (`person`, `agent`, `connector`, `service`, or another lowercase token), a name and a tenant, with scopes: a topic pattern (a topic, `facts.*` or `work.*`) and verbs (`publish`, `subscribe`, `claim`, `ack`, `approve`). A kind and a name name one principal in a tenant. The admin API (`principals_create`, `principals_list`, `principals_revoke`) issues the credential: a user of the broker with permissions on its tenant's stream and the topics its scopes cover, and a personal access token of that user, which is the credential. Only the bootstrap token, now the admin principal's credential (`MANSIO_TOKEN`), compared in constant time and standing for `MANSIO_TOKEN_PRINCIPAL`, may call it.

**Verbs.** `publish` is the broker's send permission; `subscribe` and `claim` are its poll permission; `claim` and `approve` (on `work.approvals` only) also let the holder `work_pull` from the queue and `work_ack` or `work_fail` its own leases; `ack` (on `alerts` only) lets it `alerts_ack`. The broker has no word for claim, approve or ack, so mansio enforces them. A verb on a pattern it cannot apply to is refused when the scope is made.

**In the broker.** An Iggy permission names a topic by number. `IggyBroker` expands a pattern to the topics the tenant has, remembers each user's scopes, and applies them again to every user whose pattern matches when a topic is made, and `MemoryBroker` evaluates the same scopes. A permission is never given on a stream as a whole, or globally. The server's own Iggy user, made by `provision-admin` with root credentials that the server is never given, manages users and streams and reads and writes every topic. It creates streams and topics, on a publish; a read creates nothing.

**Who is asking.** A bearer is the admin token, or a credential: a digest of every credential issued is in the registry, so a token that was never issued, or was revoked, is refused without any traffic to the broker. For one that is known, the expiry is checked against the server's clock, and the broker is asked whether it still accepts the token and whose it is, at most every `MANSIO_CREDENTIAL_RECHECK_SECONDS`, so a revocation or an expiry the server did not itself see applies within that time. Identity is the credential's and nothing in a request. The server acts for a credential through a client signed in with it (`Caller::Credential`, one client per credential in use), so the broker refuses what the scopes do not allow, and a refusal is a 403. A request is also checked against the scopes before anything is recorded or created, so a refusal leaves nothing behind; `work_pull` additionally reads the queue as the caller before a lease is handed out.

**Issue.** Recorded first: the `created` fact goes to the sink and `facts.principal`. Then the broker makes the user, with a random password nobody is told and that is dropped once the token is made, and the token with the expiry (an `agent` always has one: `MANSIO_AGENT_CREDENTIAL_TTL_SECONDS`, or the request's). Then the registry log is written. If a step after the fact fails, the user is deleted, a `revoked` fact with the reason `failed` closes it, and no credential is returned. The credential is returned once; no log, fact or record holds it.

**Registry.** The principals are a projection of a log in the broker that no credential can reach (the Iggy stream `mansio.system`, topic `principals`, whose name is not a valid tenant): `created` with the principal, its broker user and the digest of its credential, and `revoked`. It is read when first needed, and every principal in it is told to the broker again, because the broker keeps scopes in memory; one whose user the broker has lost is withdrawn.

**Revoke and sweep.** The broker's user is deleted first, which ends its token and its sessions, and the revocation holds from then on whatever else fails. Then the principal leaves the registry and its log, and `revoked` is recorded (reasons `requested`, `expired`, `failed`). A sweep every `MANSIO_CREDENTIAL_SWEEP_SECONDS` revokes the expired, as `service:mansio-engine`.

The bootstrap token's principal (`service:mansio` by default) and `service:mansio-engine` cannot be issued. The server sets `tenant`, `topic`, `principalkind` and `principalname` from the path and the credential's principal, and refuses a body that carries any of them.

## Surfaces

One HTTP API under `/v1`:

- `GET /healthz`, open: `{"status":"ok"}`.
- `POST /v1/tenants/{tenant}/topics/{topic}/messages`: the body is an envelope without the four attributes above; the answer is 201 with `{"offset", "envelope"}`, the envelope as stored. `offset` is absent when Iggy stored the message without saying where and it is not among the last `MANSIO_IGGY_LOCATE_WINDOW` messages: the message is stored, so the client must not send it again.
- `GET /v1/tenants/{tenant}/topics/{topic}/messages?offset=N&count=M`: 200 with `{"messages": [{"offset", "envelope"}]}` from that offset, in publish order.
- `GET /v1/settings`: the effective non-secret settings (the work settings and the API limits).
- `work_publish`: `POST /v1/tenants/{tenant}/work/{queue}/items`, an envelope without the server-set attributes and without `itemid` and `attempt`; 201 as for a publish.
- `work_pull`: `POST /v1/tenants/{tenant}/work/{queue}/pull` with `{"consumer"}`; 200 with `{"lease_id", "expires_at", "item"}`, or 204 when nothing is claimable.
- `work_ack`: `POST /v1/tenants/{tenant}/work/{queue}/items/{itemid}/ack` with `{"lease_id"}`, and `work_fail`: `.../fail` with `{"lease_id", "reason"}`; 200 with the transition's envelope.
- `alerts_publish`: `POST /v1/tenants/{tenant}/alerts`, as a publish to `alerts`. `alerts_ack`: `POST /v1/tenants/{tenant}/alerts/{id}/ack`; 200 with the `acknowledged` envelope.
- `principals_create`: `POST /v1/principals` with `{"kind", "name", "tenant", "scopes", "ttl_seconds"?}`; 201 with `{"principal", "credential"}`. `principals_list`: `GET /v1/principals?tenant=`; 200 with `{"principals"}`. `principals_revoke`: `DELETE /v1/principals/{id}`; 200 with the principal. The admin credential only.

Every refusal is `{"error": {"code", "message"}}` with 400 (malformed), 401 (no valid token), 403 (a valid credential that may not do this, or a lease that is not the caller's), 404 (including a read of a topic nobody has published to), 409 (the state does not allow it: no live lease, an alert already acknowledged, an item or alert id already used, a principal that already has a credential), 413 (body too large), 502 (the broker refused) or 503 (the broker or the sink is unavailable). A 502 or 503 is logged at `warn` with the tenant, topic and cause. Everything but `/healthz` needs `Authorization: Bearer <token>`.

The CLI, the MCP server, server-sent events (`GET /v1/events`, resuming from `Last-Event-ID`) and signed webhooks (CloudEvents JSON, HMAC-SHA256 signature header, retry with backoff) all use it, with the same operation names.

## Settings

Every limit, interval and switch is a named setting read from the environment with a default, listed in the README. No constant stands in for a setting.

## Rust profile

One lint and toolchain profile for the workspace: `unsafe_code` denied; clippy `all` with warnings as errors in CI; `unwrap_used`, `expect_used`, `panic`, `todo`, `dbg_macro` denied, and `print_stdout` (the CLI binary expects it, with a reason); tests may unwrap. Exceptions are `#[expect(lint, reason = "...")]` on the item. Pinned `rust-toolchain.toml`, `rust-version` to match, `resolver = "3"`, `overflow-checks = true` in release, `--locked` everywhere, `cargo deny check` with a licence allowlist and crates.io as the only source. Typed errors (`thiserror` in libraries, `anyhow` only at a binary's edge), `#[serde(deny_unknown_fields)]` on request types, secrets in `secrecy` types compared in constant time.
