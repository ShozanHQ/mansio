# mansio in Claude Code

`mansio mcp` is an MCP server on stdio. Claude Code runs it from a `.mcp.json`
in the project, so everyone who works there gets the same tools.

## Configure

Give the agent a credential of its own and not the admin token. With the admin
token:

```sh
export MANSIO_URL=http://127.0.0.1:7070
export MANSIO_TOKEN=...   # the admin token
mansio principals create --kind agent --name claude-1 --tenant acme \
  --scopes '[{"topic":"work.builds","verbs":["claim","publish"]},{"topic":"facts.build","verbs":["publish","subscribe"]}]' \
  --ttl-seconds 28800
```

It prints the credential once. Put it in the environment Claude Code starts
from, as `MANSIO_TOKEN`, together with `MANSIO_URL`:

```sh
export MANSIO_URL=http://127.0.0.1:7070
export MANSIO_TOKEN=...   # the credential that was printed
```

Then `.mcp.json`, at the root of the project:

```json
{
  "mcpServers": {
    "mansio": {
      "command": "mansio",
      "args": ["mcp"],
      "env": {
        "MANSIO_URL": "${MANSIO_URL:-http://127.0.0.1:7070}",
        "MANSIO_TOKEN": "${MANSIO_TOKEN}"
      }
    }
  }
}
```

`mansio` has to be on the `PATH` (`cargo install --path crates/mansio`), or
`command` has to be its full path. The token is read from the environment each
time Claude Code starts the server, and is never written into the file. Run
`/mcp` in Claude Code to see that `mansio` is connected; without a token the
server does not start, and says so.

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

Then ask Claude: "Pull a job from the builds queue of acme as consumer
claude-1, do it, and acknowledge it." It calls `work_pull`:

```json
{"tenant": "acme", "queue": "builds", "consumer": "claude-1"}
```

and gets one line of JSON back: the lease and the item.

```json
{"lease_id":"5b0c0b51-4a53-4a3c-9d3a-0d6e0e2f7a11","consumer":"claude-1","expires_at":"2026-10-26T12:05:00.000Z","item":{"specversion":"1.0","id":"0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3","itemid":"0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3","attempt":1,"type":"build.requested","data":{"sha":"abc123"},"...":"..."}}
```

The item is leased to `claude-1` until `expires_at`. When the work is done it
calls `work_ack` with the item's `itemid`, the `consumer` and the `lease_id` it
was given:

```json
{"tenant": "acme", "queue": "builds", "item": "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3", "consumer": "claude-1", "lease_id": "5b0c0b51-4a53-4a3c-9d3a-0d6e0e2f7a11"}
```

If it cannot do the work it calls `work_fail` with the same and a `reason`, and
the item is offered again after a backoff. A `work_pull` that finds nothing
returns `no output`. `wait` makes it look for that many seconds first:

```json
{"tenant": "acme", "queue": "builds", "consumer": "claude-1", "wait": 20}
```

If the answer to a `work_pull` is lost, asking again under the same consumer
name returns the same lease, not another item. To work on several items at
once, use several consumer names.
