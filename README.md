# mansio

[![ci](https://github.com/ShozanHQ/mansio/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ShozanHQ/mansio/actions/workflows/ci.yml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/ShozanHQ/mansio/badge)](https://securityscorecards.dev/viewer/?uri=github.com/ShozanHQ/mansio)

A message bus for agents, people and programs that are not connected all the
time. A producer publishes facts and work to a topic; a consumer that joins
later picks up what is waiting. [Apache Iggy](https://iggy.apache.org) is the
broker, and mansio is the contract around it. Open source under Apache-2.0.

mansio is built with [Shozan](https://shozan.ai).

This is an early build. Publishing and polling work, and so do work items
(claim with a lease, acknowledge, fail, retry with backoff, a dead topic),
alerts (acknowledge, surface again) and credentials (a principal gets a
credential that the broker holds to its scopes) over the HTTP API. Humans,
agents and programs consume the same items through the surfaces that fit them:
a [command line](#the-cli) and an [MCP server](#mcp) for agents, [server-sent
events](#server-sent-events) for a dashboard, and [signed
webhooks](#webhooks) for software that will not hold a connection.

## The contract

### Envelope

Every message is a [CloudEvents](https://cloudevents.io) 1.0 event in its JSON
format, with extension attributes. Unknown attributes are refused.

```json
{
  "specversion": "1.0",
  "id": "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3",
  "source": "https://example.com/builder",
  "type": "build.finished",
  "time": "2026-10-26T12:00:00Z",
  "data": {"ok": true},
  "tenant": "acme",
  "topic": "work.builds",
  "principalkind": "agent",
  "principalname": "builder-1",
  "correlationid": "run-42",
  "budget": "30 usd",
  "itemid": "item-7",
  "attempt": 1
}
```

`specversion`, `id`, `source`, `type`, `tenant`, `topic`, `principalkind` and
`principalname` are required. A producer sets `source`, `type` and whatever
else it has; the server sets `tenant`, `topic`, `principalkind` and
`principalname` from the URL and the credential, and refuses a request body
that carries any of them. If `id` or `time` is missing the server fills it in.
The full list of attributes and their rules is in
[ARCHITECTURE.md](ARCHITECTURE.md#the-envelope).

### Topics

A tenant is an Iggy stream. Its topics are:

| Topic | Holds |
|---|---|
| `facts.<kind>` | Durable events. |
| `work.<queue>` | Claimable items. |
| `alerts` | Escalations a person acknowledges. |
| `dead` | Work items past the attempt cap. |
| `presence` | Heartbeats and progress. Ephemeral, never sent to the sink. |

Names inside a topic and tenant ids match `[a-z0-9][a-z0-9_-]{0,62}`. Each
topic has one partition, so order is publish order, and an offset counts from
0. A topic is created by the first publish to it; reading one that nobody has
published to answers 404.

## Run it

### With Docker Compose

```sh
docker compose up --build --wait
```

This starts Iggy 0.9.0, makes the Iggy user mansio-server acts as (the
`iggy-admin` service runs once and stops), and starts mansio-server, and waits
until it is healthy. mansio-server listens on `127.0.0.1:7070` and Iggy's TCP
port is published on `127.0.0.1:8090`. The token and the passwords in
`compose.yaml` are development defaults (`mansio-dev-token` for the admin
token, `iggy` for Iggy's root user, `mansio-dev-admin` for the Iggy user
mansio-server acts as). Set `MANSIO_TOKEN`, `IGGY_ROOT_PASSWORD` and
`MANSIO_IGGY_PASSWORD` in the environment, or in a `.env` file, to change
them. Only Iggy and `iggy-admin` are given the root password.

```sh
export MANSIO_TOKEN=mansio-dev-token
cargo run -p mansio -- publish --tenant acme --topic facts.build \
  --type build.finished --source ci --data '{"ok":true}'
cargo run -p mansio -- poll --tenant acme --topic facts.build
```

`docker compose down --volumes` stops it and removes the data.

### Without Docker, in memory

The memory broker needs nothing else, and forgets everything when the server
stops. It holds users, scopes and expiring credentials the way Iggy does, so
credentials work on it as they do on Iggy.

```sh
MANSIO_BROKER=memory MANSIO_TOKEN=dev MANSIO_SINK_PATH= cargo run -p mansio-server
```

An empty `MANSIO_SINK_PATH` turns the file sink off; otherwise every durable
publish is also appended to `mansio-sink.jsonl`.

### What the file sink guarantees

- Each record is one complete line of CloudEvents JSON, written in a single
  call. If a write fails, the file is cut back to the end of the last whole
  line, so the next record never joins half of one.
- The record is synced to disk (`sync_data`) before the publish goes on, so a
  record the server acknowledged survives a crash of the process or the
  machine.
- On start, if the file ends in a line with no newline (a write cut short by a
  crash), the server refuses to start. The message names the file and the byte
  offset where the torn line begins; truncate the file to that offset, or
  remove the line, and start again. The server never repairs the file itself.
- `MANSIO_SINK_SYNC=false` turns the sync off, for development. Lines are still
  written whole, but a crash or power loss can lose the most recent records.
  Leave it on in production.

## Install

Each release, `vX.Y.Z`, is on the
[releases page](https://github.com/ShozanHQ/mansio/releases).

The `mansio` command line, as an archive for macOS (Apple silicon and Intel),
Linux (x86-64 and arm64) and Windows (x86-64), each with its `.sha256`:

```sh
gh release download v0.1.0 --repo ShozanHQ/mansio --pattern 'mansio-aarch64-apple-darwin.tar.xz*'
```

Or from source: `cargo install mansio --locked`.

The server, as a multi-arch (linux/amd64 and linux/arm64) image:

```sh
docker pull ghcr.io/shozanhq/mansio-server:0.1.0
```

The libraries, `mansio-core`, `mansio-iggy` and `mansio-server`, are on
[crates.io](https://crates.io/crates/mansio-core).

## Verify a release

Everything a release publishes was built by this repository's release workflow,
and you can check that before you run it. You need the
[GitHub CLI](https://cli.github.com) for the first two checks and
[cosign](https://docs.sigstore.dev/cosign/installation/) for the others.

An archive (the attestation covers the archive, which holds the binary) has a build provenance attestation:

```sh
gh attestation verify mansio-aarch64-apple-darwin.tar.xz --repo ShozanHQ/mansio
```

So does the image, by tag or by digest:

```sh
gh attestation verify oci://ghcr.io/shozanhq/mansio-server:0.1.0 --repo ShozanHQ/mansio
```

The image is also signed with cosign, keyless, and carries its CycloneDX SBOM as
an attestation. Both name the release workflow on `main` as their signer:

```sh
signer=https://github.com/ShozanHQ/mansio/.github/workflows/release.yml@refs/heads/main
issuer=https://token.actions.githubusercontent.com
cosign verify ghcr.io/shozanhq/mansio-server:0.1.0 \
  --certificate-identity "$signer" --certificate-oidc-issuer "$issuer"
cosign verify-attestation ghcr.io/shozanhq/mansio-server:0.1.0 --type cyclonedx \
  --certificate-identity "$signer" --certificate-oidc-issuer "$issuer"
```

The SBOM lists the Rust dependencies built into the server. It is also on the
release as `mansio-server-X.Y.Z.cdx.json`.

The tag is an annotated tag signed with an SSH key that only the `release`
environment holds. Its public half is `.github/release-signers`, committed with the release setup.
In a clone, this prints `Good "git" signature`:

```sh
git fetch --tags
git -c gpg.ssh.allowedSignersFile=.github/release-signers verify-tag v0.1.0
```

## The guarantee

Work is never dropped and never done twice, for consumers that are not always
connected. [`tests/offline/run.sh`](tests/offline/run.sh) is that claim, run on
every pull request by the `offline` job of CI, against Iggy through compose. It
mints an agent credential for a fresh tenant with `mansio principals create`
and runs three scenarios of N items (`MANSIO_OFFLINE_ITEMS`, 50) with a
consumer, [`tests/offline/consumer.sh`](tests/offline/consumer.sh): a plain
shell loop of `mansio work pull --wait` and `mansio work ack` that appends the
item's id to a file only when the ack succeeds. Any harness can copy its shape;
read the next paragraph before copying its order.

1. A consumer is started and killed with `kill -9`. N items are published. A
   new process of the consumer starts. The file ends with N lines, N distinct
   ids, none twice, every published id in it, and `facts.work` holds one
   claim and one ack, both of attempt 1, for each item.
2. N items are published. A consumer that kills itself right after its first
   claim dies holding that item's lease. A consumer under another name takes
   the rest, and the held item once its lease has ended and its backoff has
   passed. The file ends as in 1, and for the held item `facts.work` holds
   exactly one `expired` and one `retried`, then the claim and the ack of
   attempt 2.
3. As in 2, but the consumer that died starts again under the same name while
   its lease is live. A consumer that holds a live lease is given it again, so
   it gets the held item back and acknowledges it once. The file ends as in 1,
   and `facts.work` holds one claim and one ack of attempt 1 for every item,
   the held one included: no expiry, no second attempt.

The consumer appends after the ack so that the file counts acknowledgements and
can never hold an item twice, which is what the test needs. The price is a lost
effect: if the consumer dies between the ack and the append, or the answer to
the ack is lost (the CLI does not send an ack again), the server has the item as
done and the file does not hold it. A real worker does its effect first,
idempotent on the item's `itemid` (the attempt is in the item), and then acks,
so that a death between the two is a redelivery that the idempotent effect
absorbs.

The consumer that takes over in scenario 2 has a name of its own because the
dead one's name would be given its lease back, and there would be no expiry to
wait for.

After the last item of a scenario is done the test goes on watching for as long
as a redelivery can take, so that a duplicate that arrives late is still caught
(`MANSIO_OFFLINE_SETTLE_SECONDS`).

To run it you need a built `mansio`, `jq`, `curl`, `pkill` and `pgrep`, and a
server with a lease of seconds, not the default five minutes, because scenario
2 waits for it, and long enough to restart a consumer inside, because scenario
3 does. The test reads the server's lease, backoff and tick from
`GET /v1/settings` and refuses to start if its timeout, or a settle time you
set, is shorter than they need. It also checks the consumers' logs: no ack
may have failed, and the pulls that failed and were tried again are counted and
printed, so that a broker that hiccups is visible in a passing run. On Linux,
with Docker:

```sh
MANSIO_LEASE_SECONDS=8 MANSIO_MAX_ATTEMPTS=3 MANSIO_BACKOFF_BASE_SECONDS=1 \
  MANSIO_BACKOFF_CAP_SECONDS=4 MANSIO_TICK_MILLIS=200 docker compose up --build --wait
cargo build -p mansio
MANSIO_TOKEN=mansio-dev-token MANSIO_BIN=target/debug/mansio tests/offline/run.sh
docker compose down --volumes
```

Without Docker, on the memory broker (Iggy's server runs on Linux only), start
the server in another terminal with the same settings and point the test at it:

```sh
MANSIO_BROKER=memory MANSIO_TOKEN=dev MANSIO_SINK_PATH= MANSIO_LEASE_SECONDS=8 \
  MANSIO_MAX_ATTEMPTS=3 MANSIO_BACKOFF_BASE_SECONDS=1 MANSIO_BACKOFF_CAP_SECONDS=4 \
  MANSIO_TICK_MILLIS=200 cargo run -p mansio-server
cargo build -p mansio
MANSIO_TOKEN=dev MANSIO_BIN=target/debug/mansio tests/offline/run.sh
```

It prints what it checks and ends with `PASS`, or with `FAIL` and the last
lines of each consumer's log, and exits non-zero. Its settings are in
[The offline test](#the-offline-test).

## The CLI

`mansio` is a client of the HTTP API at `MANSIO_URL` (or `--url`) with the
bearer token in `MANSIO_TOKEN` (or `--token`; a flag wins over the variable).
Every command prints JSON, one object per line, and exits non-zero with a
message on stderr if the server refuses. Without a token every command but
`--help` exits non-zero at once, says so, and sends nothing.

There is one command for each HTTP operation, named as the operation with `_`
as a space, and each flag is a parameter of the operation, with `-` for `_`.
`mansio --help` and `mansio work pull --help` list them.

| Command | Prints |
|---|---|
| `publish`, `work publish`, `alerts publish` | The envelope as stored. |
| `poll` | The envelopes from `--offset` on, so the next poll starts at the offset plus the number of lines. |
| `watch` | The envelopes of a topic as they arrive, until stopped. |
| `work pull` | The lease: `{"lease_id", "consumer", "expires_at", "item"}`. Nothing, and exit 0, if there is no item. |
| `work extend`, `work ack`, `work fail`, `alerts ack` | The envelope of the transition. For `work extend` it is the `extended` fact, and `data.expires_at` is where the lease now ends. |
| `principals create` | `{"principal", "credential"}`: the credential is shown this once. |
| `principals list`, `webhooks list` | One principal, or webhook, a line. |
| `principals revoke`, `webhooks delete` | The principal, or webhook. |
| `webhooks create` | `{"webhook", "secret"}`: the signing secret is shown this once. |
| `webhooks rotate` | `{"webhook", "secret", "previous_secret_valid_until"}`: the new signing secret is shown this once. |
| `consumers get`, `consumers commit` | `{"consumer", "offset", "next"}`. |
| `mcp` | Nothing: it serves [MCP](#mcp) on stdin and stdout. |

```sh
export MANSIO_URL=http://127.0.0.1:7070 MANSIO_TOKEN=...

mansio publish --tenant acme --topic facts.build --type build.finished \
  --source ci --data '{"ok":true}' [--subject S] [--correlation-id ID] \
  [--causation-id ID] [--idempotency-key KEY] [--id UUID]

# A worker: claim, then ack or fail with what the claim printed. Work that
# takes longer than the lease extends it first, again and again, so that
# nobody else is given the item.
mansio work pull --tenant acme --queue builds --consumer worker-1 --wait 30
mansio work extend --tenant acme --queue builds --item ITEMID \
  --consumer worker-1 --lease-id LEASEID [--seconds 600]
mansio work ack  --tenant acme --queue builds --item ITEMID \
  --consumer worker-1 --lease-id LEASEID
mansio work fail --tenant acme --queue builds --item ITEMID \
  --consumer worker-1 --lease-id LEASEID --reason "out of memory"

mansio alerts ack --tenant acme --alert ALERTID

# Credentials and webhooks, with the admin token.
mansio principals create --kind agent --name builder-1 --tenant acme \
  --scopes '[{"topic":"work.builds","verbs":["claim"]}]' --ttl-seconds 3600
mansio webhooks create --tenant acme --topic facts.build \
  --endpoint https://example.com/hook --start next
mansio webhooks list --tenant acme
mansio webhooks rotate --id WEBHOOKID
```

- `work pull --wait SECONDS` looks again every `MANSIO_POLL_INTERVAL_MILLIS`
  until an item can be claimed or the wait ends.
- `watch --tenant T --topic X --consumer NAME` prints one envelope a line from
  where the named consumer stopped (mansio keeps its position, see
  [Consumers](#consumers)), and commits each offset after the line has been
  written and flushed. Stopped and started again with the same consumer, it
  prints every envelope once: none is missed, and none is printed twice unless
  the process was killed between the flush and the commit. On SIGINT or
  SIGTERM it finishes the line it is on, commits it and exits 0; a second
  signal while it does ends it at once, without the commit, with status 130.
  `--from OFFSET` starts at an offset and keeps no place (the default is 0),
  and `--limit N` stops after N envelopes. A topic nobody has published to is
  a 404.
- The CLI and MCP retry on network errors, `MANSIO_RETRIES` times, waiting
  `MANSIO_RETRY_BACKOFF_MILLIS` and then twice as long each time. A request
  whose connection could not be made is always sent again. One whose answer
  may have been lost is sent again only if sending it twice does what
  sending it once does: a read, `work pull` (a consumer that holds a live
  lease is given it again, so a claim whose answer is lost does not leave an
  item leased to nobody), `work extend` (a repeat leaves the lease running to
  the later of the two ends) and `consumers commit`. A `publish`, an `ack` and the
  like are not, so give a `publish`, `work publish` or `alerts publish` an
  `--id` and send it again yourself if it fails: a 409 then means the first
  attempt was stored.

## MCP

`mansio mcp` is an [MCP](https://modelcontextprotocol.io) server on stdio, for
the harness of an agent (Claude Code, Codex and the like) to run. It reads
`MANSIO_URL` and `MANSIO_TOKEN` as the CLI does, and does not start without a
token. It offers every command but `watch` (which streams, and a tool call
cannot) as a tool with the same name as the command, `work_pull` for
`work pull`, and the parameters of the command as its input, `lease_id` for
`--lease-id`. A tool does what the command does, by the same code, and returns
what the command prints; an operation that fails is a result with `isError`,
so that the model can read the reason and try again. A test lists the commands
and the tools and fails on any difference beyond `watch` and `mcp`. It speaks
the protocol versions 2024-11-05, 2025-03-26, 2025-06-18 and 2025-11-25,
answers the one the client asks for, and accepts a batch of messages, which
2025-03-26 requires and later versions do not send. A line that is not UTF-8
or not JSON is a JSON-RPC parse error and does not end the server.

| Tool | |
|---|---|
| `publish`, `poll` | Publish to a topic; read a topic. |
| `work_publish`, `work_pull`, `work_extend`, `work_ack`, `work_fail` | Work items. `work_pull` takes `wait` seconds, and `work_extend` takes `seconds`. |
| `alerts_publish`, `alerts_ack` | Alerts. |
| `consumers_get`, `consumers_commit` | Where a named consumer stands. |
| `principals_create`, `principals_list`, `principals_revoke` | Credentials, with the admin token. |
| `webhooks_create`, `webhooks_list`, `webhooks_delete`, `webhooks_rotate` | Webhooks, with the admin token. `webhooks_create` takes `start`, `first` or `next`. |

Configuration for [Claude Code](docs/mcp-claude-code.md) and
[Codex](docs/mcp-codex.md), each with a first `work_pull` and `work_ack`, is in
`docs/`. An agent should hold a credential of its own, with the scopes it needs
and a short life (`principals create`), and not the admin token.

## The HTTP API

Everything except `/healthz` needs `Authorization: Bearer <token>`. The token
is the admin token (`MANSIO_TOKEN`) or a credential issued to a principal, see
[Credentials](#credentials). The examples use the admin token.

`GET /healthz` answers 200 `{"status":"ok"}`. While the registry of principals
cannot be loaded (the broker could not be read, or a record of it cannot be)
it answers 503 `{"status":"degraded","credentials":"unavailable","message":...}`:
the server is up, every credential gets 503, and only the admin token works.
The server log says why, at `error`. `compose.yaml`'s health check fails on it,
so the stack is not healthy until the registry loads. The registry of
[webhooks](#webhooks) is the same: while it cannot be loaded `/healthz` says
`"webhooks":"unavailable"` too, the webhooks admin API answers 503, and no
webhook is delivered.

```sh
curl -H "Authorization: Bearer $MANSIO_TOKEN" -H 'Content-Type: application/json' \
  -d '{"source":"ci","type":"build.finished","data":{"ok":true}}' \
  http://127.0.0.1:7070/v1/tenants/acme/topics/facts.build/messages
# 201 {"offset":0,"envelope":{...}}

curl -H "Authorization: Bearer $MANSIO_TOKEN" \
  'http://127.0.0.1:7070/v1/tenants/acme/topics/facts.build/messages?offset=0&count=10'
# 200 {"messages":[{"offset":0,"envelope":{...}}]}
```

`offset` is left out when Iggy stored the message but did not say where it
landed and mansio could not find it among the recent messages (see
`MANSIO_IGGY_LOCATE_WINDOW`). The answer is still 201: the message is stored,
so do not publish it again. Poll and match on the envelope's `id` to find it.

A publish that carries an `idempotencykey` is safe to send again. If the tenant
has already accepted a publish to the same topic with the same key less than
`MANSIO_IDEMPOTENCY_WINDOW_SECONDS` ago, the answer is 200 with the first
publish's envelope and offset, and nothing is published. The first is a 201. The
server remembers at most `MANSIO_IDEMPOTENCY_MAX_KEYS` keys per tenant, forgets
the one remembered longest ago first, drops keys past their window on every
timer tick, and keeps them across a restart in a log that Iggy trims to the
window (the server sets the log's expiry to the window each time it starts).
A key holds only where the first publish landed, never its body; a repeat reads
the envelope back from the topic, and if retention has removed it the answer is
409 naming the first id. The log is read when the server
starts, not inside a request. The same key from another principal is answered
409 with no envelope. A publish
without a key, a key on `presence` and `work_publish` are not deduplicated, and a
publish that answered 503 may be stored without its key being remembered, so
consumers still deduplicate on `id`. ARCHITECTURE.md has the whole rule.

A refusal is `{"error":{"code":"...","message":"..."}}` with status 400, 401
(no valid credential), 403 (a valid credential that may not do this, or a lease
that is not yours), 404, 405 (the route does not take that method), 409, 413,
500 (a fault in the server; the message is generic and the detail is in the
log), 502 or 503. A 502 is a refusal the broker
named before it stored anything: nothing was stored. That includes a payload
above Iggy's cap (see `MANSIO_MAX_BODY_BYTES`). A 503 is every other
failure of the broker or the sink, including an answer from Iggy that does not
say whether it applied the request, so a write that gets a 503 may have been
stored: send it again with the same `id`. A 502 or 503 is also logged at `warn`
with the tenant, the topic and what the broker or the sink said.

Length limits are in bytes, and a value over its limit is a 400. Free-text
attributes are not empty and hold no control characters: `source` and `subject`
at most 1024; `type`, `correlationid`, `causationid`, `idempotencykey`, `itemid`
and `recordid` at most 256. A tenant id and each part of a topic name are at
most 63 (`[a-z0-9][a-z0-9_-]{0,62}`), a principal kind at most 32 and a
principal or consumer name at most 128. `scope` is at most 128 characters from `[a-z0-9._*:-]`. In `budget` (`<amount> <unit>`) the amount is at most 24 characters and the unit at most 32.

`GET /v1/settings` answers with the settings the server runs with, none of
them secret: the work, credential, stream and webhook settings below and the
API limits.

### Work items

Iggy is offset-based and has no per-message claim, acknowledgement, redelivery
or dead letters. mansio builds them, and records every transition as an
envelope on `facts.work` and in the sink. Work does not go through the generic
messages route, which refuses `work.<queue>` and the topics mansio writes
itself: `facts.work`, `facts.alert`, `facts.principal`, `facts.webhook` and
`dead`. The
operations are `work_publish`, `work_pull`, `work_extend`, `work_ack` and
`work_fail`.

```sh
# work_publish: the item is named by the id of its message, and starts at attempt 1.
curl -H "Authorization: Bearer $MANSIO_TOKEN" -H 'Content-Type: application/json' \
  -d '{"source":"ci","type":"build.requested","data":{"sha":"abc"}}' \
  http://127.0.0.1:7070/v1/tenants/acme/work/builds/items
# 201 {"offset":0,"envelope":{...,"itemid":"<id>","attempt":1}}

# work_pull: claim the oldest claimable item as consumer "worker-1".
curl -H "Authorization: Bearer $MANSIO_TOKEN" -H 'Content-Type: application/json' \
  -d '{"consumer":"worker-1"}' \
  http://127.0.0.1:7070/v1/tenants/acme/work/builds/pull
# 200 {"lease_id":"<uuid>","consumer":"worker-1","expires_at":"2026-10-26T12:05:00.000Z","item":{...}}
# 204 when nothing can be claimed

# work_extend, by the holder, while it is still working: the lease now ends
# `seconds` from now, or MANSIO_LEASE_SECONDS from now if there are none.
curl ... -d '{"lease_id":"<uuid>","consumer":"worker-1","seconds":600}' \
  http://127.0.0.1:7070/v1/tenants/acme/work/builds/items/<itemid>/extend
# 200 {"offset":7,"envelope":{"type":"mansio.work.extended",...,"data":{...,"expires_at":"2026-10-26T12:15:00.000Z"}}}

# work_ack, or work_fail with a reason, by the holder, with the consumer name
# and the lease id.
curl ... -d '{"lease_id":"<uuid>","consumer":"worker-1"}' \
  http://127.0.0.1:7070/v1/tenants/acme/work/builds/items/<itemid>/ack
curl ... -d '{"lease_id":"<uuid>","consumer":"worker-1","reason":"out of memory"}' \
  http://127.0.0.1:7070/v1/tenants/acme/work/builds/items/<itemid>/fail
```

- A claim is a lease of `MANSIO_LEASE_SECONDS`. Nobody else is given the item
  while it holds, and two concurrent pulls never get the same one. Work that
  takes longer extends it (`work_extend`, below).
- A pull by a consumer (the principal and the consumer name together) that
  already holds a live lease on the queue is answered with that lease again,
  with the same id and expiry, and nothing is recorded: it does not claim
  another item. So a client whose answer was lost, and that asks again, does
  not leave an item leased to nobody. To work on several items at once, pull
  under several consumer names. The facts keep only a fingerprint of a lease
  id, so a server that was restarted since the claim cannot say the id again:
  it gives the consumer a new id for the same item and expiry, records that as
  a second `claimed` fact of the attempt, and the old id stops working.
- Only the holder, which is the principal behind the token, the consumer name
  it pulled under and the lease id (an unguessable UUID), can ack or fail.
  Another lease id, consumer name or principal is a 403, an item with no live lease (never claimed, expired, waiting for its
  retry) is a 409, an item that is not open (done, dead, unknown) is a 404.
  Nothing changes then. An ack after the lease ended is a 409.
- `work_extend` by the holder moves the end of the lease to `seconds` from now
  (`MANSIO_LEASE_SECONDS` from now if there are none), so that a build or a
  review that takes far longer than a lease keeps its item: extend every half
  lease, and nothing else claims it, for as many lease lengths as the longest
  lease allows. A consumer that works in blocking calls, as an agent does,
  cannot extend during one: before a step that may outlast the lease it
  extends with `seconds` at least as long as the step is expected to take, and
  extends again between steps. The longest a lease may run is
  `MANSIO_LEASE_MAX_SECONDS` (a day by default), counted from the claim however
  often it is extended. An extension that would
  pass it is capped there and not refused, and the answer says where the
  lease ends (`data.expires_at` of the `extended` fact), so a consumer that
  sees the end stop moving knows it has the last of it. An extension never
  shortens a lease, so a small `seconds` leaves it where it is. The lease id
  stays the same. It is refused as an ack is: another lease id, consumer name
  or principal is a 403, an item with no live lease is a 409 (which includes a
  lease that has ended, even before the timer has expired the item), an item
  that is not open is a 404, and `seconds` of 0 is a 400. Nothing changes
  then. Every accepted extension is recorded as a `mansio.work.extended` fact
  with the lease's fingerprint and its new end, and a repeat of one whose
  answer was lost is safe: the lease runs to the later of the two ends. A
  restarted server takes the latest end recorded for the attempt, and counts
  the longest lease from the first claim.
- A credential's own end caps its leases. A credential that expires, which an
  agent's always does, cannot extend a lease past that expiry: the extension is
  capped there, as at the longest lease, and the answer says where the lease
  ends. Revoking a principal (`principals_revoke`), or the sweep removing an
  agent credential whose expiry has passed, ends every live lease the
  principal holds at once. Each ending is recorded as a `mansio.work.expired`
  fact with the reason `revoked`, and the item is retried after its backoff,
  or goes to `dead` at the attempt cap, as for any lease that runs out. So a
  crashed or revoked holder that asked for a long extension does not keep its
  item until `MANSIO_LEASE_MAX_SECONDS`.
- A fail, or a lease that ends unanswered, republishes the item to its queue
  as a new message with the same `itemid`, `attempt` plus one and
  `causationid` the message before. The wait is
  `MANSIO_BACKOFF_BASE_SECONDS * 2^(attempt-1)`, at most
  `MANSIO_BACKOFF_CAP_SECONDS`.
- When the next attempt would pass `MANSIO_MAX_ATTEMPTS`, the item goes to
  `dead` at once instead, and to nowhere else.
- An item id is used once. `work_publish` answers 409 for an id the queue has
  already had, open or done, so resending finished work does not run it
  again. The server keeps the id of every finished item, 16 bytes each, and
  of every acknowledged alert. They are rebuilt from the topics when the
  server starts, so what bounds them is `MANSIO_RETENTION_WORK_SECONDS` and
  `MANSIO_RETENTION_ALERTS_SECONDS`: with the default, which keeps everything,
  they only grow, and an id older than the retention can be used again.
- A client that gets a 503 from `work_publish` or `alerts_publish` cannot tell
  whether the message was stored. It sends it again with the same `id`: a 409
  then means the first attempt was stored, and there is nothing to resend.
  Without an `id` the resend is a new item. A 502 is a refusal the broker
  named before it stored anything: nothing was stored.
- A restarted server rebuilds all of this from `work.<queue>`, `facts.work`
  and `dead`, so it drops no item and republishes none twice. A publish that
  fails may still have been stored, so the server rebuilds the tenant from the
  topics after one; a stored item or acknowledgement is not lost or repeated.
  A message in a topic that cannot be read is skipped, with a warning. Keep
  `MANSIO_RETENTION_FACTS_SECONDS` no shorter than `MANSIO_RETENTION_WORK_SECONDS`
  (the server logs a warning at start when it is):
  an item whose facts were deleted while its message was not is claimable
  again. To start faster, the server writes a snapshot of each tenant's state
  every `MANSIO_SNAPSHOT_INTERVAL_SECONDS` and a start replays only what came
  after it. Iggy deletes expired messages a whole segment at a time, and only
  after the segment has filled and rolled (see `MANSIO_IGGY_SEGMENT_SIZE_BYTES`);
  Iggy 0.9 answers a read from offset 0 of a topic whose head it has deleted
  with the first message left, so the rebuild sees everything that remains.

The transitions are `mansio.work.claimed`, `.extended`, `.acked`, `.failed`,
`.expired`, `.retried` and `.dead` on `facts.work`, with the item id as `subject`, and
`data` naming the queue, the message id, the attempt, and as they apply the
lease, holder, consumer, expiry, reason and what happens next. The lease is a
fingerprint (32 hex digits of a digest of the lease id) and never the id, which
is the holder's proof: whoever can read `facts.work` can tell which lease a fact
is about and cannot act under it. Transitions
nobody asked for (an expiry, a republish, a dead letter) are made by
`service:mansio-engine`.

### Alerts

```sh
# alerts_publish
curl -H "Authorization: Bearer $MANSIO_TOKEN" -H 'Content-Type: application/json' \
  -d '{"source":"monitor","type":"disk.full","data":{"host":"db-1"}}' \
  http://127.0.0.1:7070/v1/tenants/acme/alerts
# 201 {"offset":0,"envelope":{...}}   the alert id is the envelope's id

# alerts_ack: records who and when, once.
curl -X POST -H "Authorization: Bearer $MANSIO_TOKEN" \
  http://127.0.0.1:7070/v1/tenants/acme/alerts/<id>/ack
# 200 the mansio.alert.acknowledged envelope; a second ack is a 409
```

An alert nobody has acknowledged `MANSIO_ALERT_RESURFACE_SECONDS` after it was
published gets a `mansio.alert.resurfaced` envelope on `facts.alert`, and
another after each further interval, until it is acknowledged. The interval is
counted from the server's clock when it accepts the alert, not from the `time`
the producer wrote. An alert id is used once: publishing an id the tenant
already has is a 409, so a resend cannot reopen an acknowledged alert. As for
work, a client that gets a 503 sends the alert again with the same `id`, and
a 409 means the first attempt was stored.

### Consumers

A named durable consumer is a name whose position on a topic mansio keeps, so
it survives a restart of the server and of the client. The position is the
offset of the last message the consumer handled, and it reads next from one
after it. The operations are `consumers_get` and `consumers_commit`, and they
need the right to read the topic.

A consumer belongs to the principal that uses it: the name is a name in that
principal's own namespace, per tenant and topic. Two principals that both use
`dash` have two consumers, and neither can read or move the other's. The
positions are kept in a log of mansio's own stream in the broker that no
credential can reach, and not in Iggy's consumer offsets, which any credential
that may read a topic could set for any consumer of it. Every commit is a
record of that log, and the log is never trimmed. A webhook's position is kept
the same way and is not reachable through this API at all. A record of that log
that cannot be read is skipped with a warning, not made to fail the load as the
principals' and the webhooks' are: the cost is that a consumer or a webhook
starts from an older position and sees some messages again, and delivery here is
at least once anyway.

```sh
# consumers_get: 200 {"consumer":"dash","offset":4,"next":5}
# "offset" is the last one committed and is left out if there is none; "next"
# is where the consumer reads next, 0 if there is none.
curl -H "Authorization: Bearer $MANSIO_TOKEN" \
  http://127.0.0.1:7070/v1/tenants/acme/topics/facts.build/consumers/dash

# consumers_commit: the offset of the last message handled.
curl -H "Authorization: Bearer $MANSIO_TOKEN" -H 'Content-Type: application/json' \
  -d '{"offset":4}' \
  http://127.0.0.1:7070/v1/tenants/acme/topics/facts.build/consumers/dash/commit
```

The message at the offset must exist: an offset past the last message of the
topic is a 409, as is any offset of an empty topic, so a mistake cannot make a
consumer skip what is published next. An earlier offset sets the consumer back
to read again, and one that retention has since deleted is accepted. A topic
nobody has published to has no consumers (404).

### Server-sent events

`GET /v1/events?tenant=T&topic=X` serves the topic as `text/event-stream`, for a
dashboard. Each event's `id` is the offset of the message, its `event` the
envelope's `type`, and its `data` the envelope as CloudEvents JSON.

```
id: 7
event: build.finished
data: {"specversion":"1.0","id":"...","type":"build.finished",...}
```

- The stream starts at the first message, or at `?from=N`, and goes on with
  what is published later.
- A client that reconnects sends `Last-Event-ID`, which a browser's
  `EventSource` does by itself, and is sent the messages after it: nothing is
  missed and nothing is sent twice. `Last-Event-ID` wins over `?from=`.
- A quiet stream sends a comment line every `MANSIO_SSE_KEEPALIVE_SECONDS`, so
  that a proxy does not take the connection for dead.
- It is authorised as `poll` is, with the caller's credential, and the answer
  is the status of a refusal (401, 403, 404) before any event. A credential
  that is revoked while the stream is open ends it at the next read; one that
  expires ends it when the sweep deletes its user, which is up to
  `MANSIO_CREDENTIAL_SWEEP_SECONDS` later.
  The browser's `EventSource` cannot send an `Authorization` header: use a
  client that can, such as `fetch` with a stream reader.
- The server looks for new messages every `MANSIO_STREAM_POLL_MILLIS`.
- Open streams are limited. A credential that holds
  `MANSIO_SSE_MAX_PER_CREDENTIAL` streams gets 429 for the next one, and past
  `MANSIO_SSE_MAX` streams in all the server answers 503 to everyone. Both
  are checked after authorisation and before the first read, and a closed
  stream gives its place back. Both are in `GET /v1/settings`.
- When the server is told to stop it ends every event stream first, so a
  connected client does not hold the shutdown up; the client sees the stream
  end and reconnects with `Last-Event-ID` to the next server.

### Webhooks

Software that will not hold a connection is sent each envelope of a topic as an
HTTP POST. The admin credential makes them: `webhooks_create` (a tenant, a
topic, an `http` or `https` URL and optionally a `start`), `webhooks_list`,
`webhooks_rotate` and `webhooks_delete`.

```sh
curl -H "Authorization: Bearer $MANSIO_TOKEN" -H 'Content-Type: application/json' \
  -d '{"tenant":"acme","topic":"facts.build","url":"https://example.com/hook","start":"next"}' \
  http://127.0.0.1:7070/v1/webhooks
# 201 {"webhook":{"id":"<uuid>",...},"secret":"whsec_..."}   the secret, once

curl -H "Authorization: Bearer $MANSIO_TOKEN" 'http://127.0.0.1:7070/v1/webhooks?tenant=acme'
# 200 {"webhooks":[{"id":"<uuid>","tenant":"acme","topic":"facts.build",
#      "url":"https://example.com/hook","created_at":"...",
#      "last_delivered_offset":7,"last_error":{"at":"...","offset":7,"message":"HTTP 500"}}]}
#      no secret in it

curl -X POST -H "Authorization: Bearer $MANSIO_TOKEN" http://127.0.0.1:7070/v1/webhooks/<id>/rotate
# 200 {"webhook":{...},"secret":"whsec_...","previous_secret_valid_until":"..."}
#      the new secret, once

curl -X DELETE -H "Authorization: Bearer $MANSIO_TOKEN" http://127.0.0.1:7070/v1/webhooks/<id>
```

A delivery is a POST of one envelope in the CloudEvents JSON format (structured
mode), `Content-Type: application/cloudevents+json`, with the header

```
mansio-signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">
```

The key is the signing secret as text (`whsec_...`), and the body is the bytes
of the request. [docs/webhooks.md](docs/webhooks.md) verifies it in a few
lines. The timestamp is the moment of that attempt.

- Each webhook follows its topic from a position of its own that mansio keeps
  (see [Consumers](#consumers)), so it delivers in offset order, one envelope
  at a time, and a restarted server resumes where the last one stopped. A new
  webhook starts at the first message the topic holds, or, with `"start":
  "next"`, at the first one published after it is made (`"first"` is the
  default). `next` reads the topic through once to find its end. A topic
  nobody has published to yet is waited for.
- A delivery goes only to an address outside the server's own network. The
  address a host name resolves to is checked when each delivery is made, and a
  loopback (`127.0.0.0/8`, `::1`), link-local (`169.254.0.0/16`, `fe80::/10`)
  or private (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `fc00::/7`)
  address, or a host name with one among its addresses, is refused: the
  attempt fails with "the destination is a loopback, link-local or private
  address" (never the address itself), is retried like any other failure and
  is given up on the same way. `MANSIO_WEBHOOK_ALLOW_PRIVATE_DESTINATIONS=true`
  allows them, for a receiver inside the network. While they are
  refused, deliveries ignore a proxy named in the server's environment, which
  would resolve the host itself.
- `webhooks_rotate` makes a new signing secret and returns it this once.
  Until `MANSIO_WEBHOOK_ROTATION_GRACE_SECONDS` have passed
  (`previous_secret_valid_until` says when) every delivery is signed with the
  new secret and the old, `mansio-signature: t=...,v1=<new>,v1=<old>`, and a
  receiver that holds either verifies it; after that, with the new one only.
  Rotating again inside the grace drops the first secret. The old secret is
  not kept past the grace on the wire, but stays in the server's log.
- Creating, rotating and deleting a webhook are recorded as
  `mansio.webhook.created`, `mansio.webhook.rotated` and
  `mansio.webhook.deleted` on `facts.webhook`, in the sink first, made by the
  admin who did it. They carry the webhook, the topic and the origin of the
  address (scheme, host and port, not a user name, path or query), the start
  of a created one and `previous_secret_valid_until` of a rotated one, and no
  secret.
- A 2xx is delivery. Any other answer, no answer within
  `MANSIO_WEBHOOK_TIMEOUT_SECONDS` and a connection that cannot be made are
  tried again after `MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS * 2^(attempt-1)`, at
  most `MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS`. A redirect is not followed.
  After `MANSIO_WEBHOOK_MAX_ATTEMPTS` attempts the delivery is recorded as
  `mansio.webhook.failed` on `facts.webhook` (the webhook, the offset, the
  message id, the attempts and the last error, never the address or the
  secret) and the webhook moves on to the next envelope.
- Delivery is at least once. A server that stops between the receiver's answer
  and the commit of the offset sends that envelope again, so a receiver
  deduplicates on the envelope's `id`.
- The listing shows each webhook's `last_delivered_offset` (after a restart,
  until the next delivery, the offset it last moved past) and its `last_error`,
  which stays until another delivery fails. It holds no secret.
- The secrets are kept on the server's own log in the broker, with the
  principals', where no credential can read. A topic cannot be delivered to
  its own failures: a webhook for `facts.webhook` is refused.
- The registry of webhooks fails loud, as the principals' does. A record of
  its log that cannot be read (damaged, or written by a newer version of the
  format: each carries its version in `v`, 1 when absent) keeps the registry
  unloaded, because skipping a deletion would revive a webhook that delivers
  to an address the admin removed, and skipping a creation would lose the only
  copy of its secret. The log says why, at `error`, when the failures start and
  again if what goes wrong changes, not on every try. Until it loads,
  `webhooks_create`, `webhooks_list`, `webhooks_rotate` and `webhooks_delete`
  answer 503,
  `/healthz` is `degraded`, and no delivery task starts; it is read again after
  a wait that grows from `MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS` to
  `MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS`, by whichever request, health check or
  start comes first.
- A webhook that cannot read its position, its topic, or record a delivery it
  gave up on (the broker is down) waits as long, with the same growing wait,
  and logs it once when it starts and once when it ends, not on each try.
- The fact for a delivery given up on has an `id` made from the webhook's and
  the message's, so one recorded again after a restart is recognisably the
  same fact.
- The address is at most `MANSIO_WEBHOOK_URL_MAX_BYTES`.

### More than one server per broker

The lock that makes a claim atomic, and the state behind it, are in one
mansio-server's memory. So by default, run one mansio-server per broker: two on
the same broker could give the same item to two consumers.

To run several for availability, set `MANSIO_OWNER_TTL_SECONDS` to the same
value (3 or more) on every one of them. Each tenant then has one owner at a
time, and only its owner serves it: the others answer `503` `unavailable` for
that tenant, and a client sends the request again, as it does for any 503, or a
load balancer in front of the servers sends it to the next. A tenant goes to
whichever server asks for it first; the others stand by.

When the owner stops, another server takes its tenants within
`MANSIO_OWNER_TTL_SECONDS` and a quarter of it (a kill or a crash), or at once
(a stop in order, `SIGTERM`). Nothing is lost: the new owner rebuilds the
tenant from the broker as a restart does, so a lease the old owner gave is held
until it ends and the item is then retried, and every item that was waiting is
claimable. Try it with `docker compose --profile ha up --build --wait`, which
adds a second server on port 7071 (set the variable for both).

Credentials follow the broker: a server reads new records of the principals log
at every sweep (`MANSIO_CREDENTIAL_SWEEP_SECONDS`) and, at most once per
`MANSIO_CREDENTIAL_FOLLOW_MILLIS`, when it is shown a token it does not know, so a credential issued through one
server works on the other, including after the first dies.

Still per server: webhooks and consumer positions are read when a server
starts, so a webhook or a named consumer made through one server is known to
another only after that restarts, a position committed on one is not seen by
the other, and each server delivers the webhooks it holds (a server that holds
webhooks logs a warning at start when this setting is on). The bootstrap
`MANSIO_TOKEN` works on all of them. Iggy's own clustering is separate: mansio
uses one Iggy.

### Credentials

Identity secures the bus, and the broker enforces it: a consumer that was never
granted a topic cannot read it however it asks. The admin credential (the
bootstrap `MANSIO_TOKEN`) issues a credential to a principal, and a host
application's token issuance calls this, so one issuance covers the host's API
and the bus. The operations are `principals_create`, `principals_list` and
`principals_revoke`, and only the admin credential may call them.

```sh
# principals_create: a kind, a name, one tenant, and scopes.
curl -H "Authorization: Bearer $MANSIO_TOKEN" -H 'Content-Type: application/json' \
  -d '{"kind":"agent","name":"builder-1","tenant":"acme","scopes":[
        {"topic":"facts.build","verbs":["publish"]},
        {"topic":"work.builds","verbs":["claim"]}]}' \
  http://127.0.0.1:7070/v1/principals
# 201 {"principal":{"id":"<uuid>","kind":"agent","name":"builder-1","tenant":"acme",
#      "scopes":[...],"created_at":"...","expires_at":"..."},"credential":"<token>"}

# principals_list: the principals that have a credential, of one tenant or all.
curl -H "Authorization: Bearer $MANSIO_TOKEN" 'http://127.0.0.1:7070/v1/principals?tenant=acme'
# 200 {"principals":[{...}]}   no credential in it

# principals_revoke: 200 with the principal; its credential stops working.
curl -X DELETE -H "Authorization: Bearer $MANSIO_TOKEN" http://127.0.0.1:7070/v1/principals/<id>
```

The credential is in the answer to `principals_create` and nowhere else: mansio
keeps no copy of it, and it is in no log, fact or record. Present it as
`Authorization: Bearer <token>`. Who a request is from comes from the
credential and from nothing in the request: the `principalkind` and
`principalname` on what it publishes are the principal's. A kind and a name
name one principal in a tenant (409 otherwise), and `service:mansio`, the admin
principal, and `service:mansio-engine` cannot be issued.

A scope is a topic pattern and the verbs granted there. A pattern is a topic
(`work.builds`, `facts.build`, `alerts`), or `facts.*` or `work.*` for every
topic of that family, including ones made later. There is no pattern for the
whole stream.

| Verb | Lets the principal | On |
|---|---|---|
| `publish` | publish through the server: `messages`, `work_publish`, `alerts_publish` | any topic except `facts.work`, `facts.alert`, `facts.principal` and `dead`, which only mansio writes |
| `subscribe` | read messages | any topic |
| `claim` | read messages, `work_pull` from the queue, and `work_extend`, `work_ack` and `work_fail` its own leases | `work.<queue>` |
| `ack` | `alerts_ack` | `alerts` |
| `approve` | do what `claim` does | `work.approvals` only |

A scope that grants a verb where it cannot apply, such as `claim` on
`facts.build`, or `publish` on a topic only mansio writes, is refused with 400.
`publish` on `facts.*` or `work.*` leaves those topics out. A credential reaches
its tenant and no other.

- An `agent` credential always expires: after `ttl_seconds` if the request
  gives it, else after `MANSIO_AGENT_CREDENTIAL_TTL_SECONDS`. Any other kind
  expires only if the request gives `ttl_seconds`. An expired credential is
  refused with 401 at once, and a sweep every `MANSIO_CREDENTIAL_SWEEP_SECONDS`
  revokes it.
- A revocation deletes the broker's user and with it the credential, so the
  broker refuses it as well as mansio. A server that did not itself see a
  revocation or an expiry asks the broker about a token at least every
  `MANSIO_CREDENTIAL_RECHECK_SECONDS`, so it takes effect within that time. A
  token that was never issued or was revoked is refused without asking the
  broker.
- Each issue and revoke is an envelope on `facts.principal`,
  `mansio.principal.created` and `mansio.principal.revoked` (with the reason:
  `requested`, `expired` or `failed`), recorded in the sink first. The
  generic messages route refuses the topic, and a credential with `facts.*`
  can read it. The principals themselves are kept in a log in the broker that
  no credential can read, and read again on start. Each record in it carries
  the version of its format, and an earlier version is read as it was written,
  its scopes included, whatever the rules for a new scope are by then. If a
  record cannot be read the registry does not load: nothing is deleted or
  revoked for it, every credential is refused with 503, and `/healthz` is
  degraded, until the record is dealt with and the server restarted.
- A `principals_create` that answers 5xx may have made the principal anyway
  (it fails after the broker has made the user, or the record fails to be
  written, and mansio undoes what it can). Its credential was not returned. List
  the tenant's principals, revoke the one of that kind and name, and issue it
  again: until then the name is taken and a retry is a 409.

#### What enforces what

**A credential reads; the server writes.** Each principal is a user of the
broker that may read the topics of its tenant's stream that its scopes cover,
named one by one, and never the stream as a whole. It may send to nothing, so a
credential that talks to the broker directly can write nothing: it cannot name
a principal, an item id or an attempt, or write a fact. A write goes to
mansio-server, which checks the credential's `publish` scope, sets the
principal from the credential and everything else a caller may not set, and
publishes as its own admin identity. mansio reads for a credential on a
connection signed in as that user, so a read the scopes do not allow is refused
by the broker and answered 403. A topic made later is opened to the users whose
scopes cover it, and a stream made later re-checks every user, because Iggy may
give a new stream the number of a deleted one. The test suite asks the broker
itself, without mansio-server, and Iggy refuses a credential that sends
anywhere, or reads another tenant's stream.

mansio enforces what the broker has no word for: that a write is a `publish`
and never to a topic only mansio writes, that only the holder of a lease acks
or fails it, that `claim` and `approve` are what let a principal take from a
queue, that `ack` is what lets it acknowledge an alert, and that only the admin
credential calls the admin API. It checks a request against the scopes before
it records or creates anything, so a refused request leaves nothing behind and
a tenant name a request merely carries is not held. The server acts as an Iggy
user of its own that manages users and streams and reads and writes topics
(`mansio-server provision-admin` makes it; see [Settings](#settings)), never as
Iggy's root user.

**How long a revoked or expired credential reads.** Iggy checks a token when it
signs in and not afterwards. mansio-server refuses a request with an expired or
revoked credential at once. A session a holder opened with Iggy directly before
that keeps what it is allowed to read until the user is deleted: at a
revocation, immediately; at an expiry, by the next sweep, so within
`MANSIO_CREDENTIAL_SWEEP_SECONDS`, and only while mansio-server runs. A user
that is signed in can make tokens for itself, including ones that never
expire; they stop working with the user. When the server starts, and has read the whole
registry, it deletes the Iggy users it made for principals it holds no record
of. While a record cannot be read it deletes none.

### Request log and metrics

The server writes one log line per request, at `info`, when the answer is
ready: `method`, `route` (the route pattern, such as
`/v1/tenants/{tenant}/topics/{topic}/messages`, or `unmatched`, never the
path), `tenant` (from the path or the `tenant` query, `-` if there is none),
`status`, `duration_ms`, and `principal_kind` and `principal_name` of the
credential (`-` when the request had none that was accepted). A header is
never logged, so a token never is. For an event stream the duration is the
time to the start of the answer. The log is always on.

`GET /metrics` serves Prometheus text, for the admin token only (it names
every tenant, topic and queue). Point a scrape job at it with
`authorization: {credentials: <MANSIO_TOKEN>}`.

| Metric | Type | Labels |
| --- | --- | --- |
| `mansio_http_requests_total` | counter | `method`, `route`, `status` |
| `mansio_http_request_duration_seconds` | histogram | `method`, `route`, `status` |
| `mansio_published_messages_total` | counter | `tenant`, `topic` |
| `mansio_queue_open_items` | gauge | `tenant`, `queue` |
| `mansio_queue_active_leases` | gauge | `tenant`, `queue` |
| `mansio_queue_lease_expiries_total` | counter | `tenant`, `queue` |
| `mansio_queue_retries_total` | counter | `tenant`, `queue` |
| `mansio_queue_dead_letters_total` | counter | `tenant`, `queue` |

Counters start at zero when the server starts. Open items are those not yet
acknowledged or dead lettered, and a gauge is read when the endpoint is
scraped, for tenants whose state is loaded. Published messages include the
facts the server writes itself, on `facts.*` and `dead`. `MANSIO_METRICS=false`
turns the endpoint off (it is then a 404) and stops the counting; the request
log stays.

With `MANSIO_OWNER_TTL_SECONDS` set, the queue gauges come only from the
server that serves each tenant; a server that has lost a tenant reports none
for it, so a sum across servers does not count a tenant twice.

### Order

Every durable publish mansio makes (messages, work, transitions) is recorded
in the sink and then published, with a lock per tenant held across both, so
the sink sees each tenant's messages in the order the topics hold them.

## Settings

Every limit, interval and switch is a variable. Nothing else is configured.

### mansio-server

| Variable | Default | Meaning |
|---|---|---|
| `MANSIO_LISTEN` | `127.0.0.1:7070` (`0.0.0.0:7070` in the image) | Address the HTTP API listens on. |
| `MANSIO_BROKER` | `iggy` | `iggy` or `memory`. |
| `MANSIO_TOKEN` | none, required | The admin's bearer token, which may do everything in every tenant, and call the admin API. |
| `MANSIO_TOKEN_PRINCIPAL` | `service:mansio` | The `kind:name` the admin token stands for. |
| `MANSIO_AGENT_CREDENTIAL_TTL_SECONDS` | `3600` | How long an agent credential lasts when the request does not say. |
| `MANSIO_CREDENTIAL_RECHECK_SECONDS` | `30` | The longest a credential is trusted without asking the broker again, so the longest a revocation the server did not see itself takes to apply. Also how long the server waits after a registry that could not be loaded before it tries again. |
| `MANSIO_CREDENTIAL_SWEEP_SECONDS` | `60` | How often expired credentials are revoked. With servers sharing a broker, only a tenant's owner revokes its principals; a revoke asked of another server answers 503. |
| `MANSIO_CREDENTIAL_FOLLOW_MILLIS` | `1000` | The soonest the principals log is read again for a token the registry does not know, so that tokens never issued cannot make the server read it at every request. Also how long a broker user no record owns is left before it is deleted, when servers share a broker. |
| `MANSIO_SINK_PATH` | `mansio-sink.jsonl` (`/data/sink.jsonl` in the image) | JSON Lines file that every durable publish is appended to before it is published. Empty turns the sink off. |
| `MANSIO_SINK_SYNC` | `true` | `true` or `false`. Whether the sink syncs each record to disk before the publish goes on. `false` is for development: a crash can lose recent records. |
| `MANSIO_METRICS` | `true` | `true` or `false`. Whether `GET /metrics` is served and the counters behind it are kept. `false` makes `/metrics` a 404. The request log is written either way; see [Request log and metrics](#request-log-and-metrics). |
| `MANSIO_MAX_BODY_BYTES` | `1048576` | Largest request body; a larger one is a 413. Iggy has a cap of its own on a message payload, 64 000 000 bytes (the stored envelope, not the body's `data` alone), and a body over it is refused by Iggy as a 502, which a client does not retry. A value above about 64 000 000 lets a body reach that refusal; a value below it makes this setting the limit. Raise it only when messages are that large: Iggy holds a whole message in memory. |
| `MANSIO_POLL_DEFAULT_COUNT` | `100` | Envelopes a poll returns when `count` is not given. |
| `MANSIO_POLL_MAX_COUNT` | `1000` | The most a poll may ask for. Also how many messages the server reads at a time when it rebuilds its state on start. |
| `MANSIO_LEASE_SECONDS` | `300` | How long a claim holds, and how long an extension without `seconds` runs from now. Past it the item expires and, after its backoff, is claimed again. At most `315360000` (ten years). |
| `MANSIO_LEASE_MAX_SECONDS` | `86400` | The longest a lease may run, counted from the claim however often it is extended. An extension that would pass it is capped there. At least `MANSIO_LEASE_SECONDS`, at most `315360000` (ten years). |
| `MANSIO_MAX_ATTEMPTS` | `5` | Attempts an item gets. A failure or expiry of the last one sends it to `dead`. |
| `MANSIO_BACKOFF_BASE_SECONDS` | `5` | The wait before an item is republished after its first failure. It doubles with each attempt. |
| `MANSIO_BACKOFF_CAP_SECONDS` | `300` | The longest that wait gets. At least the base. |
| `MANSIO_RECOVERY_LOG_SECONDS` | `60` | While the broker will not list its tenants at startup (for example an Iggy user that may not list streams), the server asks again after a wait that doubles from one tick up to this many seconds, and logs the failure at most once per this interval. At least 1. |
| `MANSIO_SNAPSHOT_INTERVAL_SECONDS` | `300` | How often the state of a tenant that has changed is written to a snapshot, which the next start reads instead of replaying every topic from offset 0 (see "Snapshots" in ARCHITECTURE.md). 0 writes none. |
| `MANSIO_SNAPSHOT_DIR` | `mansio-snapshots` | The directory of the snapshots, one file per tenant; `/data/snapshots` in the container image. Empty keeps none. A snapshot that cannot be read is ignored with a warning and the start replays from offset 0. |
| `MANSIO_OWNER_TTL_SECONDS` | `0` | Set on every mansio-server that shares a broker, to the same value (3 or more; 30 or more for a cluster that stays up for months, because every owner appends a record each quarter of it, about 11,500 a day per tenant at 30, and a starting server reads them all), to run more than one: a tenant is served by one of them, and another takes it over this long (and a quarter more) after its server stops renewing it. `0` serves every tenant and must be the only server on its broker (see [More than one server per broker](#more-than-one-server-per-broker)). |
| `MANSIO_ALERT_RESURFACE_SECONDS` | `3600` | How long an alert may go unacknowledged before it is surfaced again, and again after each further interval. |
| `MANSIO_TICK_MILLIS` | `500` | How often the timer looks for leases to expire, items to republish and alerts to surface again. |
| `MANSIO_IDEMPOTENCY_WINDOW_SECONDS` | `86400` | How long a publish's `idempotencykey` is remembered. A second publish to the same tenant and topic with the same key inside the window is answered 200 with the first envelope and offset and publishes nothing. At least `1`, at most `315360000` (ten years). |
| `MANSIO_IDEMPOTENCY_MAX_KEYS` | `10000` | The most keys a tenant remembers, in memory and when they are read back after a restart. Past it the key remembered longest ago is forgotten first, even inside its window. A key holds a few hundred bytes (no message body), so this bounds memory. At least `1`. |
| `MANSIO_STREAM_POLL_MILLIS` | `500` | How often an event stream and a webhook look for new messages once they have caught up. |
| `MANSIO_SSE_KEEPALIVE_SECONDS` | `15` | How often a quiet event stream sends a comment. |
| `MANSIO_SSE_MAX_PER_CREDENTIAL` | `10` | The most event streams one credential may hold open at once (the bootstrap token counts as one credential). The next `GET /v1/events` gets 429 `too_many_requests` until one closes. At least `1`. |
| `MANSIO_SSE_MAX` | `1000` | The most event streams open on the server in all. The next `GET /v1/events`, from anyone, gets 503 `unavailable` until one closes. Each open stream looks at the broker every `MANSIO_STREAM_POLL_MILLIS`, so this bounds that load. At least `1`. |
| `MANSIO_SHUTDOWN_GRACE_SECONDS` | `10` | On SIGINT or SIGTERM the server ends every event stream, and waits this long for the requests still being served before it stops anyway. |
| `MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS` | `5` | The wait before a delivery is tried again after its first failed attempt. It doubles with each attempt. |
| `MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS` | `300` | The longest that wait gets. At least the base. |
| `MANSIO_WEBHOOK_MAX_ATTEMPTS` | `8` | Attempts at one delivery before it is recorded as failed and the webhook moves on. |
| `MANSIO_WEBHOOK_TIMEOUT_SECONDS` | `10` | How long one attempt may take. |
| `MANSIO_WEBHOOK_URL_MAX_BYTES` | `2048` | The longest address a webhook may have. |
| `MANSIO_WEBHOOK_ALLOW_PRIVATE_DESTINATIONS` | `false` | Whether a delivery may go to a loopback, link-local or private address. Off, it is refused, judged on the address the host name resolves to when the delivery is made. |
| `MANSIO_WEBHOOK_ROTATION_GRACE_SECONDS` | `86400` | How long after `webhooks_rotate` deliveries are signed with the old secret as well as the new. `0` stops signing with the old one at once. At most `3153600000` (100 years); the server refuses a larger value at start. |
| `MANSIO_IGGY_ADDRESS` | `127.0.0.1:8090` | Iggy's TCP address, `host:port`. |
| `MANSIO_IGGY_USERNAME` | `mansio` | The Iggy user mansio-server acts as. It manages users and streams and reads and writes topics; it is not Iggy's root user. |
| `MANSIO_IGGY_PASSWORD` | none, required with `iggy` | Its password. If Iggy refuses it at startup, the server exits with a message that names this variable and `MANSIO_IGGY_USERNAME`. |
| `MANSIO_IGGY_CONNECT_TIMEOUT_SECONDS` | `60` | How long startup waits for Iggy. |
| `MANSIO_IGGY_SIGNIN_TIMEOUT_SECONDS` | `10` | How long signing a credential in to Iggy may take (connecting and logging in), so that a hung Iggy holds a credentialed request for this long and not for `MANSIO_IGGY_CONNECT_TIMEOUT_SECONDS`. At least `1`. |
| `MANSIO_IGGY_SESSION_IDLE_SECONDS` | `300` | How long a credential may go unused before its Iggy clients are closed. The next request signs in again. At least `1`. |
| `MANSIO_IGGY_SESSION_CLIENTS` | `2` | The most Iggy clients kept for one credential. A client answers one request at a time, so with more than one an open event stream and a pull by the same credential do not wait for each other. Past this number, requests share the least busy client. From `1` to `64`. |
| `MANSIO_IGGY_LOCATE_WINDOW` | `100` | How many recent messages to search, by message id, when Iggy accepts a publish without saying where it landed. 0 turns the search off. A message that is not found is still answered with 201, without an offset. |
| `MANSIO_IGGY_SEGMENT_SIZE_BYTES` | `0` | The segment size Iggy gives the topics mansio creates, except `presence` (next row). 0 leaves it to Iggy, which uses 1 GiB. Otherwise a multiple of 512 from 1048576 (1 MiB) to 1073741824. Iggy applies retention to whole segments that have filled and rolled, never to the one it is writing, so this is how coarsely the `MANSIO_RETENTION_*` settings trim a topic. Applied when a topic is created. |
| `MANSIO_IGGY_PRESENCE_SEGMENT_SIZE_BYTES` | `1048576` | The same for `presence`, which is ephemeral: 1 MiB by default, so a heartbeat is deleted soon after `MANSIO_RETENTION_PRESENCE_SECONDS` instead of waiting for a 1 GiB segment to fill. 0 leaves it to Iggy. Applied when a topic is created. |
| `MANSIO_IGGY_DURABILITY` | `persisted` | When Iggy confirms a write to `facts.*`, `work.*`, `alerts` and `dead`: `persisted` or `replicated`. See [Durability](#durability). |
| `MANSIO_IGGY_PRESENCE_DURABILITY` | `replicated` | The same for `presence`, which is ephemeral. |
| `MANSIO_RETENTION_FACTS_SECONDS` | `0` | How long Iggy keeps `facts.*` messages. 0 keeps them forever. |
| `MANSIO_RETENTION_WORK_SECONDS` | `0` | The same for `work.*`. |
| `MANSIO_RETENTION_ALERTS_SECONDS` | `0` | The same for `alerts`. |
| `MANSIO_RETENTION_DEAD_SECONDS` | `0` | The same for `dead`. |
| `MANSIO_RETENTION_PRESENCE_SECONDS` | `3600` | The same for `presence`. |
| `RUST_LOG` | `info` | Log filter. |

Retention is a minimum, not a deadline, for every kind of topic. Iggy deletes
only segments that have filled and rolled, never the one it is writing, so a
message lives until its retention has passed and its segment has rolled. A
quiet topic with large segments can keep a message far longer than its
retention; the segment size (`MANSIO_IGGY_SEGMENT_SIZE_BYTES`, and
`MANSIO_IGGY_PRESENCE_SEGMENT_SIZE_BYTES` for `presence`) bounds how long.

Retention, durability and segment size are applied when a topic is created; a
topic that already exists keeps the ones it was created with.

#### The Iggy user

`mansio-server provision-admin` signs in to Iggy as the root user once, makes
the user `MANSIO_IGGY_USERNAME` with `MANSIO_IGGY_PASSWORD` (or, if it exists,
sets its permissions again and leaves its password), and exits. It reads
`MANSIO_IGGY_ADDRESS`, `MANSIO_IGGY_USERNAME`, `MANSIO_IGGY_PASSWORD` and
`MANSIO_IGGY_CONNECT_TIMEOUT_SECONDS` as the server does, and the root user from
`IGGY_ROOT_USERNAME` (default `iggy`) and `IGGY_ROOT_PASSWORD` (required). The
server itself is never given those two. `compose.yaml` runs it as `iggy-admin`.

#### Durability

With `persisted`, Iggy answers a publish only after the message is on stable
storage, so a 201 survives a crash of the host. With `replicated`, Iggy
answers after its quorum commit without that barrier; on one node that is a
write to the operating system, and a crash of the host can lose the messages
acknowledged since the last flush, after the sink has already recorded them.
`persisted` costs write latency. Under `persisted` Iggy 0.9's partition setting
`wal_group_commit_delay_micros` (environment `IGGY_PARTITION_WAL_GROUP_COMMIT_DELAY_MICROS`,
0 in the configuration Iggy prints on start) is the knob that trades latency
for group commits. See [Iggy's durability documentation](https://iggy.apache.org/docs).
mansio sets both Iggy's `durability` and `consumer_offset_durability` on the
topics it creates, and leaves Iggy's own settings at its defaults.

### mansio (CLI)

| Variable | Default | Meaning |
|---|---|---|
| `MANSIO_URL` | `http://127.0.0.1:7070` | The server to talk to. `--url` wins. |
| `MANSIO_TOKEN` | none, required | The bearer token. `--token` wins. Without one nothing is sent. |
| `MANSIO_TIMEOUT_SECONDS` | `30` | Time allowed for one request. |
| `MANSIO_RETRIES` | `3` | How many times a request that failed on the network is tried again. |
| `MANSIO_RETRY_BACKOFF_MILLIS` | `250` | The wait before the first retry. It doubles with each. |
| `MANSIO_POLL_INTERVAL_MILLIS` | `500` | How often `watch` and `work pull --wait` look again when there is nothing yet. |

### Tests and compose

| Variable | Default | Meaning |
|---|---|---|
| `MANSIO_E2E_URL` | unset | A running server for `mansio-e2e` to use, with `MANSIO_TOKEN` set to its token. Unset starts a server in the test process on the memory broker, with a lease of 2 s that may run to 60 s, 3 attempts, a backoff from 1 s to 4 s, alerts surfaced every 2 s, an agent credential that lasts 3 s, is checked every second and swept every second, streams that look every 50 ms and send a comment every second, a grace of 2 s for a shutdown, and webhooks that retry after 1 s to 4 s, give up after 3 attempts, wait 2 s for an answer, may go to private addresses and sign with a rotated secret's predecessor for 4 s, and an idempotency window of 4 s over at most 20 keys. Against a running server the tests read its settings from `GET /v1/settings`. |
| `MANSIO_E2E_STATE_DIR` | unset | A directory that the first and second passes of the restart, idempotency and retention tests (see [Develop](#develop)) keep their state in. Required by those tests, which fail without it. |
| `MANSIO_E2E_SINK_PATH` | unset | The sink file of the running server, as the test process can read it. With it the tests also check the order of the sink; without it they check the topics only. |
| `MANSIO_E2E_IGGY_ADDRESS` | unset | Iggy's TCP address as the test process reaches it (`127.0.0.1:8090` for `compose.yaml`). With `MANSIO_E2E_URL` set it is required: the credential tests ask Iggy itself, without mansio-server, what a credential may do, and fail if they cannot. Without `MANSIO_E2E_URL` they ask the memory broker. |
| `MANSIO_E2E_WEBHOOK_HOST` | unset | The name by which mansio-server reaches the webhook receiver that the tests start (`host.docker.internal` for `compose.yaml`, which maps it to the host). With `MANSIO_E2E_URL` set it is required, and the receiver then listens on every interface; the webhook tests fail without it. In process the receiver listens on loopback. |
| `MANSIO_SINK_DIR` | the `sink` volume | A directory on the host for `compose.yaml` to keep the sink file in, so that a test can read it. |
| `MANSIO_LEASE_SECONDS`, the other work settings and the credential settings above, `MANSIO_POLL_DEFAULT_COUNT`, `MANSIO_POLL_MAX_COUNT`, `MANSIO_IGGY_SEGMENT_SIZE_BYTES`, `MANSIO_RETENTION_ALERTS_SECONDS`, and the stream and webhook settings above | the defaults | Passed through by `compose.yaml` to mansio-server, so a test run can use short ones. Against a running server the tests read them from `GET /v1/settings`. |
| `MANSIO_HEALTH_START_PERIOD` | `60s` | How long `compose.yaml` lets mansio-server fail its health check after it starts before it counts the failures. A start replays the topics of every tenant before the server answers, and `/healthz` is dark until it has, so a long replay needs a long period. |
| `IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL` | `1 m` | How often Iggy's segment cleaner looks for expired segments, read by `compose.yaml` for the `iggy` service. The retention test sets it to seconds. |
| `IGGY_ROOT_USERNAME`, `IGGY_ROOT_PASSWORD` | `iggy`, `iggy` | Iggy's root user, read by `compose.yaml` for Iggy and `iggy-admin` only. Development defaults. |
| `MANSIO_IGGY_USERNAME`, `MANSIO_IGGY_PASSWORD` | `mansio`, `mansio-dev-admin` | The Iggy user mansio-server acts as, read by `compose.yaml` for `iggy-admin` and mansio-server. Development defaults. |
| `IGGY_PORT` | `8090` | The port on `127.0.0.1` that `compose.yaml` publishes Iggy's TCP port on. |

### The offline test

Read by `tests/offline/run.sh` and `tests/offline/consumer.sh` (see [The
guarantee](#the-guarantee)). The test also reads `MANSIO_URL` and
`MANSIO_POLL_INTERVAL_MILLIS` (see the CLI settings) and takes the admin token
from `MANSIO_TOKEN`; its consumers are given the credential it mints as their
`MANSIO_TOKEN`.

| Variable | Default | Meaning |
|---|---|---|
| `MANSIO_BIN` | `mansio` | The `mansio` command that the test and its consumers run. |
| `MANSIO_OFFLINE_ITEMS` | `50` | Items published in each scenario. |
| `MANSIO_OFFLINE_TIMEOUT_SECONDS` | `120` | How long a scenario waits for every item to be done. It must be longer than the server's lease plus its backoff cap, which the test checks before it starts. |
| `MANSIO_OFFLINE_SETTLE_SECONDS` | the server's lease plus its backoff cap plus twice its tick and the CLI's poll interval, read from `GET /v1/settings` | How long the test keeps watching after the last item is done, before it checks for a duplicate that arrives late. The longest a redelivery can take is that default, so the test refuses to start with a shorter one. |
| `MANSIO_OFFLINE_KILL_WAIT_SECONDS` | `5` | How long a consumer that was killed may take to disappear from the process table before the test fails. |
| `MANSIO_OFFLINE_CREDENTIAL_TTL_SECONDS` | `3600` | How long the agent credentials the test mints last. |
| `MANSIO_OFFLINE_PULL_WAIT_SECONDS` | `5` | How long one `work pull` of the consumer looks for an item. |
| `MANSIO_OFFLINE_RETRY_SECONDS` | `1` | The consumer's wait after a pull that failed. |
| `MANSIO_OFFLINE_STOP_AFTER_CLAIM` | `0` | `1` makes the consumer `kill -9` itself right after its first claim, holding the lease. The test sets it for the consumer that dies in scenarios 2 and 3. |

## Develop

The toolchain is pinned in `rust-toolchain.toml`; every cargo command in CI
uses `--locked`.

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo deny check
```

`unwrap`, `expect`, `panic` and `todo` are denied outside tests. The end-to-end
tests in `crates/mansio-e2e` run against an in-process server by default. To
run them against Iggy:

```sh
MANSIO_LEASE_SECONDS=2 MANSIO_MAX_ATTEMPTS=3 MANSIO_BACKOFF_BASE_SECONDS=1 \
  MANSIO_BACKOFF_CAP_SECONDS=4 MANSIO_ALERT_RESURFACE_SECONDS=2 \
  MANSIO_TICK_MILLIS=200 MANSIO_AGENT_CREDENTIAL_TTL_SECONDS=5 \
  MANSIO_CREDENTIAL_RECHECK_SECONDS=2 MANSIO_CREDENTIAL_SWEEP_SECONDS=2 \
  MANSIO_STREAM_POLL_MILLIS=100 MANSIO_SSE_KEEPALIVE_SECONDS=1 \
  MANSIO_WEBHOOK_BACKOFF_BASE_SECONDS=1 MANSIO_WEBHOOK_BACKOFF_CAP_SECONDS=4 \
  MANSIO_WEBHOOK_MAX_ATTEMPTS=3 MANSIO_WEBHOOK_TIMEOUT_SECONDS=2 \
  MANSIO_WEBHOOK_ALLOW_PRIVATE_DESTINATIONS=true \
  MANSIO_WEBHOOK_ROTATION_GRACE_SECONDS=4 \
  MANSIO_IDEMPOTENCY_WINDOW_SECONDS=10 MANSIO_IDEMPOTENCY_MAX_KEYS=20 \
  docker compose up --build --wait
MANSIO_E2E_URL=http://127.0.0.1:7070 MANSIO_TOKEN=mansio-dev-token \
  MANSIO_E2E_IGGY_ADDRESS=127.0.0.1:8090 \
  MANSIO_E2E_WEBHOOK_HOST=host.docker.internal cargo test -p mansio-e2e --locked
docker compose down --volumes
```

The default work and credential settings make a test of a lease, a backoff or an
expiry take minutes, so set short ones as above. The tests that stop and start
the server run only against the in-process one.

The tests that need Iggy to hold real messages across a restart come in pairs
of passes, `restart_first_pass` and `restart_second_pass`, `retention_first_pass`
and `retention_second_pass` in `crates/mansio-e2e/tests/e2e/passes.rs`, and
`idempotency_first_pass` and `idempotency_second_pass` in
`crates/mansio-e2e/tests/e2e/idempotency.rs`. They are
`#[ignore]`d, so a plain `cargo test` leaves them alone, and they fail, not
skip, without `MANSIO_E2E_URL` or `MANSIO_E2E_STATE_DIR`. The `e2e` job in
`.github/workflows/ci.yml`, through the action in `.github/actions/e2e`, runs
them in order: it recreates mansio-server with
pages of 5, a lease of 30 s, a segment of 1 MiB, alerts and presence kept for
5 s, a presence segment of about 1 MiB
(`MANSIO_IGGY_PRESENCE_SEGMENT_SIZE_BYTES`) and an idempotency window of 600 s,
runs the three first passes and `presence_retention`, restarts mansio-server
(`docker compose restart`), and runs the three second passes. The idempotency
pair publishes with a key, and after the restart checks that the same key is
answered 200 with the first envelope and offset and that the topic holds one
message. The restart pair publishes a queue longer than
`MANSIO_POLL_MAX_COUNT`, claims one item and extends its lease to twice the
lease setting, and after the restart checks that the lease still holds past
the end it first had and until the end it was extended to, that the item is
retried exactly once after that and that every other item can be claimed
once. The retention pair fills and rolls a
segment of `alerts`, waits for Iggy to delete it, publishes another alert, and
after the restart checks that the server's rebuild, which reads from offset 0,
finds the alerts that are left. `presence_retention` is a single pass with no
restart: it publishes a `presence` heartbeat, fills and rolls the segment, and
checks that the first heartbeat is gone once the 5 s retention has passed while
the one in the open segment is still there. The same checks as the restart pair run in one
process, against the in-process server, on every `cargo test`.

The `offline` job runs `tests/offline/run.sh` against a compose stack of its own;
see [The guarantee](#the-guarantee).

`crates/mansio-models` checks the rules of the work engine on a model of it,
over every state the model can reach: one item, two consumers, claims, leases
that run out, retries up to the attempt cap, `dead`, a restart and a publish
that answers 503 having stored the message. A rule that breaks fails
`cargo test` with the shortest path to it, numbered, in plain words. The chart
of the model is
[`crates/mansio-models/charts/work-item.md`](crates/mansio-models/charts/work-item.md);
a test fails when it is stale, and `UPDATE_CHARTS=1 cargo test -p
mansio-models` redraws it.

Every image in the `Dockerfile` and `compose.yaml` is pinned by digest, with its
tag kept beside it to read, and so is every action in the workflows by commit.
Dependabot moves each pin and its comment or tag together. A scheduled workflow
(`.github/workflows/scheduled.yml`) runs `cargo deny check` daily, and every
Monday the same end-to-end suite against `apache/iggy:edge` in place of the
pinned release. It reports and never blocks.

### Releases

`.github/workflows/release.yml` runs on every push to `main`.
[release-plz](https://release-plz.dev) keeps a release pull request open with the
version bumps and the `CHANGELOG.md` (configured in `release-plz.toml`). Merging
it starts the jobs in the `release` environment, which the owner approves once:
they publish the crates to crates.io by trusted publishing (the run's OIDC
identity, no stored token), create the signed `vX.Y.Z` tag, build the `mansio`
archives with [cargo-dist](https://github.com/axodotdev/cargo-dist)
(`dist-workspace.toml`), push the image, attest, sign and verify each of them,
and create the GitHub release. crates.io needs each crate's trusted publisher
set to this repository, the workflow `release.yml` and the environment
`release`, and 0.1.0 of each crate published by hand once, since a crate that
does not exist yet cannot have one. The tag needs a `TAG_SIGNING_KEY` secret
(an SSH private key) in the `release` environment and its public half committed
as `.github/release-signers`. Until 0.1.0 is on crates.io the `semver`
check of `ci.yml` is skipped; after it, it compares the library crates with the
last release on every pull request.

## Contributing and security

[CONTRIBUTING.md](CONTRIBUTING.md) says how to build, test and propose a change.
To report a vulnerability, see [SECURITY.md](SECURITY.md); do not open an issue.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
