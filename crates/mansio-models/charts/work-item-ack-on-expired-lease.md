# One work item, two consumers, up to 2 attempts, with an ack accepted on an expired lease (a seeded bug)

Checked by `cargo test -p mansio-models`, which fails when this file is stale; `UPDATE_CHARTS=1 cargo test -p mansio-models` redraws it.

## Rules

- always: an item has at most one live lease: **holds** (ARCHITECTURE.md, Work items, Claim)
- always: an ack or extend with a lease that is not live is refused: **broken** (ARCHITECTURE.md, Work items, Extend and Ack)
- always: an item is acked at most once: **holds** (README, The guarantee)
- always: an item past the attempt cap is in dead and nowhere else: **holds** (ARCHITECTURE.md, Work items, Fail)
- eventually: every item ends acked or dead: **holds** (README, The guarantee)
- sometimes: an item is acked after a retry: **holds** (ARCHITECTURE.md, Work items, Fail, and State)
- sometimes: an item reaches dead: **holds** (ARCHITECTURE.md, Work items, Fail)

"an ack or extend with a lease that is not live is refused" is broken. The shortest path that breaks it:

```text
  start: Not published yet
  1. the producer publishes it (work_publish) -> Waiting to be claimed
  2. a consumer claims it with a lease (work_pull) -> Leased to a consumer
  3. the lease runs out (the clock) -> Its lease has run out, and the timer has not yet expired it
  4. its holder acks it (work_ack) -> Acked, done
```

## Chart

1934 states checked. An arrow lists every step that takes an item from one state to the other. Red: every item in that state breaks a rule. Red and dashed: some items in it do. Blue: the shortest path to the first broken rule, its steps numbered.

```mermaid
stateDiagram-v2
    s0 : Not published yet
    s1 : Waiting to be claimed
    s2 : The publish answered 503 and stored nothing, the producer sends it again
    s3 : Leased to a consumer
    s4 : Acked, done
    s5 : Waiting out its backoff before a retry
    s6 : Its lease has run out, and the timer has not yet expired it
    s7 : On dead, done
    [*] --> s0
    s0 --> s1 : 1. the producer publishes it (work_publish) or the publish answers 503 though the message was stored (work_publish)
    s0 --> s2 : the publish answers 503 and nothing was stored (work_publish)
    s1 --> s3 : 2. a consumer claims it with a lease (work_pull) or a consumer claims it and the answer is lost to a 503 (work_pull)
    s1 --> s1 : the server restarts and rebuilds its state from the topics (recover, replay) or the producer sends it again and is answered 409, it was stored (work_publish)
    s2 --> s1 : the producer sends it again (work_publish)
    s3 --> s3 : its holder extends the lease (work_extend) or the server restarts and rebuilds its state from the topics (recover, replay) or its holder pulls again after a rebuild and is given a new lease id for the same lease (work_pull) or the producer sends it again and is answered 409, it was stored (work_publish) or its holder pulls again after a rebuild, and the answer with the new lease id is lost to a 503 (work_pull)
    s3 --> s4 : its holder acks it (work_ack) or its holder acks it and the answer is lost to a 503 (work_ack)
    s3 --> s5 : its holder fails it [attempts left] (work_fail)
    s3 --> s6 : 3. the lease runs out (the clock)
    s4 --> s4 : the server restarts and rebuilds its state from the topics (recover, replay) or the producer sends it again and is answered 409, it was stored (work_publish)
    s5 --> s1 : the timer republishes it as a new message, the attempt plus one (tick, advance)
    s5 --> s5 : the server restarts and rebuilds its state from the topics (recover, replay) or the producer sends it again and is answered 409, it was stored (work_publish)
    s6 --> s4 : 4. its holder acks it (work_ack) or its holder acks it and the answer is lost to a 503 (work_ack)
    s6 --> s5 : the timer expires the lease [attempts left] (tick, expire)
    s6 --> s6 : the server restarts and rebuilds its state from the topics (recover, replay) or the producer sends it again and is answered 409, it was stored (work_publish)
    s3 --> s7 : its holder fails it and it goes to dead [last attempt] (work_fail)
    s7 --> s7 : the server restarts and rebuilds its state from the topics (recover, replay) or the producer sends it again and is answered 409, it was stored (work_publish)
    s6 --> s7 : the timer expires the lease and it goes to dead [last attempt] (tick, expire)
    classDef breaksSome fill:#fff4f5,stroke:#e72e3b,stroke-width:2px,stroke-dasharray:6,color:#cd2834
    classDef bugPath stroke:#1680f3,stroke-width:3px
    class s0 bugPath
    class s1 bugPath
    class s3 bugPath
    class s4 breaksSome
    class s6 bugPath
```
