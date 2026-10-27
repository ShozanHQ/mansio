# mansio in Codex

`mansio mcp` is an MCP server on stdio. Codex runs it from the
`[mcp_servers.mansio]` table of its configuration, `~/.codex/config.toml`.

## Configure

Give the agent a credential of its own and not the admin token. With the admin
token:

```sh
export MANSIO_URL=http://127.0.0.1:7070
export MANSIO_TOKEN=...   # the admin token
mansio principals create --kind agent --name codex-1 --tenant acme \
  --scopes '[{"topic":"work.builds","verbs":["claim","publish"]},{"topic":"facts.build","verbs":["publish","subscribe"]}]' \
  --ttl-seconds 28800
```

It prints the credential once. Export it as `MANSIO_TOKEN` in the shell Codex
is started from, together with `MANSIO_URL`, and add this to
`~/.codex/config.toml`:

```toml
[mcp_servers.mansio]
command = "mansio"
args = ["mcp"]
# Passed on from the environment Codex runs in. The token is never written here.
env_vars = ["MANSIO_URL", "MANSIO_TOKEN"]
# A work_pull with a wait of up to this long is fine.
tool_timeout_sec = 120
```

`mansio` has to be on the `PATH` (`cargo install --path crates/mansio`), or
`command` has to be its full path. If your version of Codex has no `env_vars`,
set the server's variables under `[mcp_servers.mansio.env]` instead, for the
address, and start Codex from a shell that has `MANSIO_TOKEN`:

```toml
[mcp_servers.mansio.env]
MANSIO_URL = "http://127.0.0.1:7070"
```

The tools are the commands of `mansio`, with the same names and parameters:
`publish`, `poll`, `work_publish`, `work_pull`, `work_ack`, `work_fail`,
`alerts_publish`, `alerts_ack`, `consumers_get`, `consumers_commit`,
`principals_create`, `principals_list`, `principals_revoke`, `webhooks_create`,
`webhooks_list` and `webhooks_delete`. The last six need the admin token and
are refused to a credential like the one above.

## A first `work_pull` and `work_ack`

Publish a job for it, from a shell:

```sh
mansio work publish --tenant acme --queue builds --type build.requested \
  --source ci --data '{"sha":"abc123"}'
```

Then ask Codex: "Pull a job from the builds queue of acme as consumer codex-1,
do it, and acknowledge it." It calls `work_pull`:

```json
{"tenant": "acme", "queue": "builds", "consumer": "codex-1"}
```

and gets one line of JSON back: the lease and the item.

```json
{"lease_id":"5b0c0b51-4a53-4a3c-9d3a-0d6e0e2f7a11","consumer":"codex-1","expires_at":"2026-10-26T12:05:00.000Z","item":{"specversion":"1.0","id":"0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3","itemid":"0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3","attempt":1,"type":"build.requested","data":{"sha":"abc123"},"...":"..."}}
```

The item is leased to `codex-1` until `expires_at`. When the work is done it
calls `work_ack` with the item's `itemid`, the `consumer` and the `lease_id` it
was given:

```json
{"tenant": "acme", "queue": "builds", "item": "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3", "consumer": "codex-1", "lease_id": "5b0c0b51-4a53-4a3c-9d3a-0d6e0e2f7a11"}
```

If it cannot do the work it calls `work_fail` with the same and a `reason`, and
the item is offered again after a backoff. A `work_pull` that finds nothing
returns `no output`.

If the answer to a `work_pull` is lost, asking again under the same consumer
name returns the same lease, not another item. To work on several items at
once, use several consumer names.
