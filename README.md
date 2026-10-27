# mansio

A message bus for agents, people and programs that are not connected all the
time. A producer publishes facts and work to a topic; a consumer that joins
later picks up what is waiting. [Apache Iggy](https://iggy.apache.org) is the
broker, and mansio is the contract around it. Open source under Apache-2.0.

This is an early build. Publishing and polling work, and so do work items
(claim with a lease, acknowledge, fail, retry with backoff, a dead topic) and
alerts (acknowledge, surface again) over the HTTP API. The CLI and MCP
commands for them, per-principal credentials, server-sent events and webhooks
are designed in [ARCHITECTURE.md](ARCHITECTURE.md) and not built yet.

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
0. Topics are created the first time they are used.

## Run it

### With Docker Compose

```sh
docker compose up --build --wait
```

This starts Iggy 0.9.0 and mansio-server, and waits until both are healthy.
mansio-server listens on `127.0.0.1:7070`; Iggy is reachable only from the
other container. The token and the Iggy admin password in `compose.yaml` are
development defaults (`mansio-dev-token`, `iggy`). Set `MANSIO_TOKEN` and
`IGGY_ROOT_PASSWORD` in the environment, or in a `.env` file, to change them.

```sh
export MANSIO_TOKEN=mansio-dev-token
cargo run -p mansio -- publish --tenant acme --topic facts.build \
  --type build.finished --source ci --data '{"ok":true}'
cargo run -p mansio -- poll --tenant acme --topic facts.build
```

`docker compose down --volumes` stops it and removes the data.

### Without Docker, in memory

The memory broker needs nothing else, and forgets everything when the server
stops.

```sh
MANSIO_BROKER=memory MANSIO_TOKEN=dev MANSIO_SINK_PATH= cargo run -p mansio-server
```

An empty `MANSIO_SINK_PATH` turns the file sink off; otherwise every durable
publish is also appended to `mansio-sink.jsonl`.

## The CLI

`mansio` talks to the HTTP API at `MANSIO_URL` with the bearer token in
`MANSIO_TOKEN`. It prints one JSON envelope per line, and exits non-zero with
a message on stderr if the server refuses.

```sh
mansio publish --tenant T --topic facts.build --type TYPE --source SOURCE \
  [--subject S] [--data JSON] [--correlation-id ID] [--causation-id ID] \
  [--idempotency-key KEY]
mansio poll --tenant T --topic TOPIC [--offset N] [--count M]
```

`publish` prints the envelope as stored. `poll` prints the envelopes from
`--offset` on, so the next poll starts at the offset plus the number of lines.

## The HTTP API

Everything except `/healthz` needs `Authorization: Bearer <token>`.

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

A refusal is `{"error":{"code":"...","message":"..."}}` with status 400, 401,
403, 404, 409, 413, 502 or 503. A 502 or 503 is also logged at `warn` with the
tenant, the topic and what the broker or the sink said.

`GET /v1/settings` answers with the settings the server runs with, none of
them secret: the work settings below and the API limits.

### Work items

Iggy is offset-based and has no per-message claim, acknowledgement, redelivery
or dead letters. mansio builds them, and records every transition as an
envelope on `facts.work` and in the sink. Work does not go through the generic
messages route, which refuses `work.<queue>`, `facts.work` and `facts.alert`.
The operations are `work_publish`, `work_pull`, `work_ack` and `work_fail`.

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
# 200 {"lease_id":"<uuid>","expires_at":"2026-10-26T12:05:00.000Z","item":{...}}
# 204 when nothing can be claimed

# work_ack, or work_fail with a reason, by the holder, with the lease id.
curl ... -d '{"lease_id":"<uuid>"}' \
  http://127.0.0.1:7070/v1/tenants/acme/work/builds/items/<itemid>/ack
curl ... -d '{"lease_id":"<uuid>","reason":"out of memory"}' \
  http://127.0.0.1:7070/v1/tenants/acme/work/builds/items/<itemid>/fail
```

- A claim is a lease of `MANSIO_LEASE_SECONDS`. Nobody else is given the item
  while it holds, and two concurrent pulls never get the same one.
- Only the holder, which is the principal behind the token plus the lease id
  (an unguessable UUID), can ack or fail. Another lease id or principal is a
  403, an item with no live lease (never claimed, expired, waiting for its
  retry) is a 409, an item that is not open (done, dead, unknown) is a 404.
  Nothing changes then. An ack after the lease ended is a 409.
- A fail, or a lease that ends unanswered, republishes the item to its queue
  as a new message with the same `itemid`, `attempt` plus one and
  `causationid` the message before. The wait is
  `MANSIO_BACKOFF_BASE_SECONDS * 2^(attempt-1)`, at most
  `MANSIO_BACKOFF_CAP_SECONDS`.
- When the next attempt would pass `MANSIO_MAX_ATTEMPTS`, the item goes to
  `dead` at once instead, and to nowhere else.
- An item id is used once. `work_publish` answers 409 for an id the queue has
  already had, open or done, so resending finished work does not run it
  again. The server keeps the id of every finished item.
- A restarted server rebuilds all of this from `work.<queue>`, `facts.work`
  and `dead`, so it drops no item and republishes none twice. A publish that
  fails may still have been stored, so the server rebuilds the tenant from the
  topics after one; a stored item or acknowledgement is not lost or repeated.
  A message in a topic that cannot be read is skipped, with a warning. Keep
  `MANSIO_RETENTION_FACTS_SECONDS` no shorter than `MANSIO_RETENTION_WORK_SECONDS`:
  an item whose facts were deleted while its message was not is claimable
  again.

The transitions are `mansio.work.claimed`, `.acked`, `.failed`, `.expired`,
`.retried` and `.dead` on `facts.work`, with the item id as `subject`, and
`data` naming the queue, the message id, the attempt, and as they apply the
lease id, holder, consumer, expiry, reason and what happens next. Transitions
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
already has is a 409, so a resend cannot reopen an acknowledged alert.

### One server per broker

The lock that makes a claim atomic, and the state behind it, are in one
mansio-server's memory. Run one mansio-server per broker; two on the same
broker could give the same item to two consumers.

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
| `MANSIO_TOKEN` | none, required | The bearer token clients present. |
| `MANSIO_TOKEN_PRINCIPAL` | `service:mansio` | The `kind:name` the token stands for. |
| `MANSIO_SINK_PATH` | `mansio-sink.jsonl` (`/data/sink.jsonl` in the image) | JSON Lines file that every durable publish is appended to before it is published. Empty turns the sink off. |
| `MANSIO_MAX_BODY_BYTES` | `1048576` | Largest request body. |
| `MANSIO_POLL_DEFAULT_COUNT` | `100` | Envelopes a poll returns when `count` is not given. |
| `MANSIO_POLL_MAX_COUNT` | `1000` | The most a poll may ask for. Also how many messages the server reads at a time when it rebuilds its state on start. |
| `MANSIO_LEASE_SECONDS` | `300` | How long a claim holds. Past it the item expires and, after its backoff, is claimed again. |
| `MANSIO_MAX_ATTEMPTS` | `5` | Attempts an item gets. A failure or expiry of the last one sends it to `dead`. |
| `MANSIO_BACKOFF_BASE_SECONDS` | `5` | The wait before an item is republished after its first failure. It doubles with each attempt. |
| `MANSIO_BACKOFF_CAP_SECONDS` | `300` | The longest that wait gets. At least the base. |
| `MANSIO_ALERT_RESURFACE_SECONDS` | `3600` | How long an alert may go unacknowledged before it is surfaced again, and again after each further interval. |
| `MANSIO_TICK_MILLIS` | `500` | How often the timer looks for leases to expire, items to republish and alerts to surface again. |
| `MANSIO_IGGY_ADDRESS` | `127.0.0.1:8090` | Iggy's TCP address, `host:port`. |
| `MANSIO_IGGY_USERNAME` | `iggy` | The Iggy admin user mansio acts as. |
| `MANSIO_IGGY_PASSWORD` | none, required with `iggy` | Its password. |
| `MANSIO_IGGY_CONNECT_TIMEOUT_SECONDS` | `60` | How long startup waits for Iggy. |
| `MANSIO_IGGY_LOCATE_WINDOW` | `100` | How many recent messages to search, by message id, when Iggy accepts a publish without saying where it landed. 0 turns the search off. A message that is not found is still answered with 201, without an offset. |
| `MANSIO_IGGY_DURABILITY` | `persisted` | When Iggy confirms a write to `facts.*`, `work.*`, `alerts` and `dead`: `persisted` or `replicated`. See [Durability](#durability). |
| `MANSIO_IGGY_PRESENCE_DURABILITY` | `replicated` | The same for `presence`, which is ephemeral. |
| `MANSIO_RETENTION_FACTS_SECONDS` | `0` | How long Iggy keeps `facts.*` messages. 0 keeps them forever. |
| `MANSIO_RETENTION_WORK_SECONDS` | `0` | The same for `work.*`. |
| `MANSIO_RETENTION_ALERTS_SECONDS` | `0` | The same for `alerts`. |
| `MANSIO_RETENTION_DEAD_SECONDS` | `0` | The same for `dead`. |
| `MANSIO_RETENTION_PRESENCE_SECONDS` | `3600` | The same for `presence`. |
| `RUST_LOG` | `info` | Log filter. |

Retention and durability are applied when a topic is created; a topic that
already exists keeps the ones it was created with.

#### Durability

With `persisted`, Iggy answers a publish only after the message is on stable
storage, so a 201 survives a crash of the host. With `replicated`, Iggy
answers after its quorum commit without that barrier; on one node that is a
write to the operating system, and a crash of the host can lose the messages
acknowledged since the last flush, after the sink has already recorded them.
`persisted` costs write latency. Iggy's flush thresholds,
`messages_required_to_save` and `size_of_messages_required_to_save`, set how
much its journal holds before a flush, and they do not weaken `persisted`.
See [Iggy's durability documentation](https://iggy.apache.org/docs). mansio
sets both Iggy's `durability` and `consumer_offset_durability` on the topics
it creates, and leaves the flush thresholds at Iggy's defaults.

### mansio (CLI)

| Variable | Default | Meaning |
|---|---|---|
| `MANSIO_URL` | `http://127.0.0.1:7070` | The server to talk to. |
| `MANSIO_TOKEN` | none, required | The bearer token. |
| `MANSIO_TIMEOUT_SECONDS` | `30` | Time allowed for one request. |

### Tests and compose

| Variable | Default | Meaning |
|---|---|---|
| `MANSIO_E2E_URL` | unset | A running server for `mansio-e2e` to use, with `MANSIO_TOKEN` set to its token. Unset starts a server in the test process on the memory broker, with a lease of 2 s, 3 attempts, a backoff from 1 s to 4 s and alerts surfaced every 2 s. Against a running server the tests read its work settings from `GET /v1/settings`. |
| `MANSIO_E2E_SINK_PATH` | unset | The sink file of the running server, as the test process can read it. With it the tests also check the order of the sink; without it they check the topics only. |
| `MANSIO_SINK_DIR` | the `sink` volume | A directory on the host for `compose.yaml` to keep the sink file in, so that a test can read it. |
| `MANSIO_LEASE_SECONDS` and the other work settings above | the defaults | Passed through by `compose.yaml` to mansio-server, so a test run can use short ones. |
| `IGGY_ROOT_USERNAME`, `IGGY_ROOT_PASSWORD` | `iggy`, `iggy` | The Iggy admin user, read by `compose.yaml` for both services. Development defaults. |

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
  MANSIO_TICK_MILLIS=200 docker compose up --build --wait
MANSIO_E2E_URL=http://127.0.0.1:7070 MANSIO_TOKEN=mansio-dev-token \
  cargo test -p mansio-e2e --locked
docker compose down --volumes
```

The default work settings make a test of a lease or a backoff take minutes, so
set short ones as above. The tests that stop and start the server run only
against the in-process one.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
