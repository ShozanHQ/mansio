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

Answer with any 2xx once you have the envelope. Anything else, or no answer
within `MANSIO_WEBHOOK_TIMEOUT_SECONDS`, is tried again with a backoff. A
delivery is at least once: deduplicate on the envelope's `id`.

## Python

```python
import hashlib, hmac, time

def verify(secret: str, header: str, body: bytes, tolerance: int = 300) -> bool:
    parts = dict(p.split("=", 1) for p in header.split(","))
    t = parts["t"]
    expected = hmac.new(secret.encode(), t.encode() + b"." + body, hashlib.sha256).hexdigest()
    return abs(time.time() - int(t)) <= tolerance and hmac.compare_digest(expected, parts["v1"])
```

## Node

```js
const crypto = require("node:crypto");

function verify(secret, header, body, tolerance = 300) {
  const { t, v1 } = Object.fromEntries(header.split(",").map((p) => p.split("=")));
  const expected = crypto.createHmac("sha256", secret).update(`${t}.`).update(body).digest("hex");
  const same = expected.length === v1.length &&
    crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(v1));
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

## Shell, to try it

```sh
t=${HEADER#t=}; t=${t%%,*}
printf '%s.%s' "$t" "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex
```
