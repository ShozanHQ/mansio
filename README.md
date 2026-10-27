# mansio

A message bus for agents, people and programs that are not connected all the
time. A producer publishes facts and work to a topic; a consumer that joins
later picks up what is waiting. [Apache Iggy](https://iggy.apache.org) is the
broker, and mansio is the contract around it. Open source under Apache-2.0.

This is an early build. Publishing and polling work, and so do work items
(claim with a lease, acknowledge, fail, retry with backoff, a dead topic),
alerts (acknowledge, surface again) and credentials (a principal gets a
credential that the broker holds to its scopes) over the HTTP API. The CLI and
MCP commands for them, server-sent events and webhooks are designed in
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

Everything except `/healthz` needs `Authorization: Bearer <token>`. The token
is the admin token (`MANSIO_TOKEN`) or a credential issued to a principal, see
[Credentials](#credentials). The examples use the admin token.

`GET /healthz` answers 200 `{"status":"ok"}`. While the registry of principals
cannot be loaded (the broker could not be read, or a record of it cannot be)
it answers 503 `{"status":"degraded","credentials":"unavailable","message":...}`:
the server is up, every credential gets 503, and only the admin token works.
The server log says why, at `error`. `compose.yaml`'s health check fails on it,
so the stack is not healthy until the registry loads.

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

A refusal is `{"error":{"code":"...","message":"..."}}` with status 400, 401
(no valid credential), 403 (a valid credential that may not do this, or a lease
that is not yours), 404, 409, 413, 502 or 503. A 502 is a refusal the broker
named before it stored anything: nothing was stored. A 503 is every other
failure of the broker or the sink, including an answer from Iggy that does not
say whether it applied the request, so a write that gets a 503 may have been
stored: send it again with the same `id`. A 502 or 503 is also logged at `warn`
with the tenant, the topic and what the broker or the sink said.

`GET /v1/settings` answers with the settings the server runs with, none of
them secret: the work and credential settings below and the API limits.

### Work items

Iggy is offset-based and has no per-message claim, acknowledgement, redelivery
or dead letters. mansio builds them, and records every transition as an
envelope on `facts.work` and in the sink. Work does not go through the generic
messages route, which refuses `work.<queue>` and the topics mansio writes
itself: `facts.work`, `facts.alert`, `facts.principal` and `dead`. The
operations are `work_publish`, `work_pull`, `work_ack` and `work_fail`.

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

# work_ack, or work_fail with a reason, by the holder, with the consumer name
# and the lease id.
curl ... -d '{"lease_id":"<uuid>","consumer":"worker-1"}' \
  http://127.0.0.1:7070/v1/tenants/acme/work/builds/items/<itemid>/ack
curl ... -d '{"lease_id":"<uuid>","consumer":"worker-1","reason":"out of memory"}' \
  http://127.0.0.1:7070/v1/tenants/acme/work/builds/items/<itemid>/fail
```

- A claim is a lease of `MANSIO_LEASE_SECONDS`. Nobody else is given the item
  while it holds, and two concurrent pulls never get the same one.
- Only the holder, which is the principal behind the token, the consumer name
  it pulled under and the lease id (an unguessable UUID), can ack or fail.
  Another lease id, consumer name or principal is a 403, an item with no live lease (never claimed, expired, waiting for its
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
  `MANSIO_RETENTION_FACTS_SECONDS` no shorter than `MANSIO_RETENTION_WORK_SECONDS`:
  an item whose facts were deleted while its message was not is claimable
  again. Iggy deletes expired messages a whole segment at a time, and only
  after the segment has filled and rolled (see `MANSIO_IGGY_SEGMENT_SIZE_BYTES`);
  Iggy 0.9 answers a read from offset 0 of a topic whose head it has deleted
  with the first message left, so the rebuild sees everything that remains.

The transitions are `mansio.work.claimed`, `.acked`, `.failed`, `.expired`,
`.retried` and `.dead` on `facts.work`, with the item id as `subject`, and
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

### One server per broker

The lock that makes a claim atomic, and the state behind it, are in one
mansio-server's memory. Run one mansio-server per broker; two on the same
broker could give the same item to two consumers.

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
| `claim` | read messages, `work_pull` from the queue, and `work_ack` and `work_fail` its own leases | `work.<queue>` |
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
| `MANSIO_CREDENTIAL_SWEEP_SECONDS` | `60` | How often expired credentials are revoked. |
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
| `MANSIO_IGGY_USERNAME` | `mansio` | The Iggy user mansio-server acts as. It manages users and streams and reads and writes topics; it is not Iggy's root user. |
| `MANSIO_IGGY_PASSWORD` | none, required with `iggy` | Its password. |
| `MANSIO_IGGY_CONNECT_TIMEOUT_SECONDS` | `60` | How long startup waits for Iggy. |
| `MANSIO_IGGY_LOCATE_WINDOW` | `100` | How many recent messages to search, by message id, when Iggy accepts a publish without saying where it landed. 0 turns the search off. A message that is not found is still answered with 201, without an offset. |
| `MANSIO_IGGY_SEGMENT_SIZE_BYTES` | `0` | The segment size Iggy gives the topics mansio creates. 0 leaves it to Iggy, which uses 1 GiB. Otherwise a multiple of 512 from 1048576 (1 MiB) to 1073741824. Iggy applies retention to whole segments that have filled and rolled, never to the one it is writing, so this is how coarsely the `MANSIO_RETENTION_*` settings trim a topic. Applied when a topic is created. |
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
| `MANSIO_E2E_URL` | unset | A running server for `mansio-e2e` to use, with `MANSIO_TOKEN` set to its token. Unset starts a server in the test process on the memory broker, with a lease of 2 s, 3 attempts, a backoff from 1 s to 4 s, alerts surfaced every 2 s, and an agent credential that lasts 3 s, is checked every second and swept every second. Against a running server the tests read its settings from `GET /v1/settings`. |
| `MANSIO_E2E_STATE_DIR` | unset | A directory that the first and second passes of the restart and retention tests (see [Develop](#develop)) keep their state in. Required by those tests, which fail without it. |
| `MANSIO_E2E_SINK_PATH` | unset | The sink file of the running server, as the test process can read it. With it the tests also check the order of the sink; without it they check the topics only. |
| `MANSIO_E2E_IGGY_ADDRESS` | unset | Iggy's TCP address as the test process reaches it (`127.0.0.1:8090` for `compose.yaml`). With `MANSIO_E2E_URL` set it is required: the credential tests ask Iggy itself, without mansio-server, what a credential may do, and fail if they cannot. Without `MANSIO_E2E_URL` they ask the memory broker. |
| `MANSIO_SINK_DIR` | the `sink` volume | A directory on the host for `compose.yaml` to keep the sink file in, so that a test can read it. |
| `MANSIO_LEASE_SECONDS`, the other work settings and the credential settings above, `MANSIO_POLL_DEFAULT_COUNT`, `MANSIO_POLL_MAX_COUNT`, `MANSIO_IGGY_SEGMENT_SIZE_BYTES` and `MANSIO_RETENTION_ALERTS_SECONDS` | the defaults | Passed through by `compose.yaml` to mansio-server, so a test run can use short ones. Against a running server the tests read them from `GET /v1/settings`. |
| `MANSIO_HEALTH_START_PERIOD` | `60s` | How long `compose.yaml` lets mansio-server fail its health check after it starts before it counts the failures. A start replays the topics of every tenant before the server answers, and `/healthz` is dark until it has, so a long replay needs a long period. |
| `IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL` | `1 m` | How often Iggy's segment cleaner looks for expired segments, read by `compose.yaml` for the `iggy` service. The retention test sets it to seconds. |
| `IGGY_ROOT_USERNAME`, `IGGY_ROOT_PASSWORD` | `iggy`, `iggy` | Iggy's root user, read by `compose.yaml` for Iggy and `iggy-admin` only. Development defaults. |
| `MANSIO_IGGY_USERNAME`, `MANSIO_IGGY_PASSWORD` | `mansio`, `mansio-dev-admin` | The Iggy user mansio-server acts as, read by `compose.yaml` for `iggy-admin` and mansio-server. Development defaults. |
| `IGGY_PORT` | `8090` | The port on `127.0.0.1` that `compose.yaml` publishes Iggy's TCP port on. |

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
  docker compose up --build --wait
MANSIO_E2E_URL=http://127.0.0.1:7070 MANSIO_TOKEN=mansio-dev-token \
  MANSIO_E2E_IGGY_ADDRESS=127.0.0.1:8090 cargo test -p mansio-e2e --locked
docker compose down --volumes
```

The default work and credential settings make a test of a lease, a backoff or an
expiry take minutes, so set short ones as above. The tests that stop and start
the server run only against the in-process one.

The tests that need Iggy to hold real messages across a restart come in pairs
of passes, `restart_first_pass` and `restart_second_pass`, `retention_first_pass`
and `retention_second_pass` in `crates/mansio-e2e/tests/e2e/passes.rs`. They are
`#[ignore]`d, so a plain `cargo test` leaves them alone, and they fail, not
skip, without `MANSIO_E2E_URL` or `MANSIO_E2E_STATE_DIR`. The `e2e` job in
`.github/workflows/ci.yml` runs them in order: it recreates mansio-server with
pages of 5, a lease of 30 s, a segment of 1 MiB and alerts kept for 5 s, runs
the two first passes, restarts mansio-server (`docker compose restart`), and
runs the two second passes. The restart pair publishes a queue longer than
`MANSIO_POLL_MAX_COUNT`, claims one item, and after the restart checks that
the lease still holds, that the item is retried exactly once after it and that
every other item can be claimed once. The retention pair fills and rolls a
segment of `alerts`, waits for Iggy to delete it, publishes another alert, and
after the restart checks that the server's rebuild, which reads from offset 0,
finds the alerts that are left. The same checks as the restart pair run in one
process, against the in-process server, on every `cargo test`.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
