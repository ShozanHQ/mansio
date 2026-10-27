# mansio

A message bus for agents, people and programs that are not connected all the
time. A producer publishes facts and work to a topic; a consumer that joins
later picks up what is waiting. [Apache Iggy](https://iggy.apache.org) is the
broker, and mansio is the contract around it. Open source under Apache-2.0.

This is an early build. Publishing and polling work. Claiming work with a
lease, acknowledgement, retry, alerts, per-principal credentials, MCP,
server-sent events and webhooks are designed in
[ARCHITECTURE.md](ARCHITECTURE.md) and not built yet.

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

A refusal is `{"error":{"code":"...","message":"..."}}` with status 400, 401,
404, 413, 502 or 503.

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
| `MANSIO_POLL_MAX_COUNT` | `1000` | The most a poll may ask for. |
| `MANSIO_IGGY_ADDRESS` | `127.0.0.1:8090` | Iggy's TCP address, `host:port`. |
| `MANSIO_IGGY_USERNAME` | `iggy` | The Iggy admin user mansio acts as. |
| `MANSIO_IGGY_PASSWORD` | none, required with `iggy` | Its password. |
| `MANSIO_IGGY_CONNECT_TIMEOUT_SECONDS` | `60` | How long startup waits for Iggy. |
| `MANSIO_IGGY_LOCATE_WINDOW` | `100` | How many recent messages to search when Iggy accepts a publish without saying where it landed. 0 turns the search off. |
| `MANSIO_RETENTION_FACTS_SECONDS` | `0` | How long Iggy keeps `facts.*` messages. 0 keeps them forever. |
| `MANSIO_RETENTION_WORK_SECONDS` | `0` | The same for `work.*`. |
| `MANSIO_RETENTION_ALERTS_SECONDS` | `0` | The same for `alerts`. |
| `MANSIO_RETENTION_DEAD_SECONDS` | `0` | The same for `dead`. |
| `MANSIO_RETENTION_PRESENCE_SECONDS` | `3600` | The same for `presence`. |
| `RUST_LOG` | `info` | Log filter. |

Retention is applied when a topic is created.

### mansio (CLI)

| Variable | Default | Meaning |
|---|---|---|
| `MANSIO_URL` | `http://127.0.0.1:7070` | The server to talk to. |
| `MANSIO_TOKEN` | none, required | The bearer token. |
| `MANSIO_TIMEOUT_SECONDS` | `30` | Time allowed for one request. |

### Tests and compose

| Variable | Default | Meaning |
|---|---|---|
| `MANSIO_E2E_URL` | unset | A running server for `mansio-e2e` to use, with `MANSIO_TOKEN` set to its token. Unset starts a server in the test process on the memory broker. |
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
docker compose up --build --wait
MANSIO_E2E_URL=http://127.0.0.1:7070 MANSIO_TOKEN=mansio-dev-token \
  cargo test -p mansio-e2e --locked
docker compose down --volumes
```

## Licence

Apache-2.0. See [LICENSE](LICENSE).
