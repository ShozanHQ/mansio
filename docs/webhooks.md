# Verifying a webhook

mansio sends each envelope of a topic to the URL of a webhook as a POST of one
CloudEvent in its JSON format (structured mode):

```
POST /hook HTTP/1.1
Content-Type: application/cloudevents+json
mansio-signature: t=1792000000,v1=bc2a11368784fc25863ca8e376d93ec761cb9e42b17f71aeed9943a84ac3a07f

{"specversion":"1.0","id":"0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3","type":"build.finished",...}
```

The `v1` value is the hex HMAC-SHA256 of `<t>.<body>`, where `t` is the number
in the header and `<body>` is the exact bytes of the request, under the signing
secret as text (`whsec_...`, shown once by `mansio webhooks create`). Check it
before you trust the body, and check that `t` is recent, so that a captured
request cannot be sent again later.

After `mansio webhooks rotate` the header carries two `v1` values, one for the
new secret and one for the old, until the grace period
(`MANSIO_WEBHOOK_ROTATION_GRACE_SECONDS`) is over:

```
mansio-signature: t=1792000000,v1=<new secret's HMAC>,v1=<old secret's HMAC>
```

A receiver that holds either secret verifies it if **any** `v1` matches, so
change the secret it holds at any time in the grace without missing a
delivery. The samples below do that: do not read the header into a map keyed
by name, which keeps only the last `v1`.

Answer with any 2xx once you have the envelope. Anything else, or no answer
within `MANSIO_WEBHOOK_TIMEOUT_SECONDS`, is tried again with a backoff. A
delivery is at least once: deduplicate on the envelope's `id`.

## Python

```python
import hashlib, hmac, time

def verify(secret: str, header: str, body: bytes, tolerance: int = 300) -> bool:
    parts = [p.split("=", 1) for p in header.split(",")]
    t = next(v for k, v in parts if k == "t")
    expected = hmac.new(secret.encode(), t.encode() + b"." + body, hashlib.sha256).hexdigest()
    same = any(hmac.compare_digest(expected, v) for k, v in parts if k == "v1")
    return same and abs(time.time() - int(t)) <= tolerance
```

## Node

```js
const crypto = require("node:crypto");

function verify(secret, header, body, tolerance = 300) {
  const parts = header.split(",").map((p) => p.split("="));
  const t = (parts.find(([k]) => k === "t") ?? [])[1];
  const expected = crypto.createHmac("sha256", secret).update(`${t}.`).update(body).digest("hex");
  const same = parts.some(([k, v]) => k === "v1" && v.length === expected.length &&
    crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(v)));
  return same && Math.abs(Date.now() / 1000 - Number(t)) <= tolerance;
}
```

`body` is the raw request body (a `Buffer`), not a parsed and printed again
JSON value.

## Rust

`mansio-core` has it:

```rust
use mansio_core::WebhookSecret;
use mansio_core::webhook::verify;

let secret = WebhookSecret::new("whsec_...");
let ok = verify(&secret, header, body, now_unix_seconds, 300);
```

`verify` accepts a header with several `v1` values, as one is in the grace
period after a rotation.

## Shell, to try it

```sh
t=${HEADER#t=}; t=${t%%,*}
printf '%s.%s' "$t" "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex
```
