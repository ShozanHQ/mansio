# Contributing to mansio

mansio is a message bus for agents, people and programs that are not connected
all the time, on Apache Iggy, under Apache-2.0. [README.md](README.md) says what
it does and how to use it; [ARCHITECTURE.md](ARCHITECTURE.md) says how it is
built. Read the part that your change touches before you start.

By contributing you agree that your contribution is licensed under Apache-2.0,
as the [LICENSE](LICENSE) says of contributions.

To report a vulnerability, do not open an issue: see [SECURITY.md](SECURITY.md).

## Build and test

The toolchain is Rust 1.99, pinned in `rust-toolchain.toml`; rustup installs it
the first time you run cargo in the checkout. Every cargo command uses
`--locked`.

```sh
cargo build --workspace --locked
cargo test --workspace --locked
```

`cargo test --workspace` needs nothing else: the end-to-end tests in
`crates/mansio-e2e` start an in-process server on the in-memory broker
(`MemoryBroker`), so they run on any machine.

Iggy's server is Linux-only, so what runs against Iggy runs in CI or on a Linux
machine with Docker: the end-to-end suite through compose, and the offline test
in `tests/offline`. The README's "Develop" section has the commands. If your
change touches `crates/mansio-iggy`, compose or the Dockerfile and you cannot run
them, say so in the pull request and let CI be the test.

## The gates

A pull request merges when all of these pass:

| Gate | What it runs |
|---|---|
| fmt | `cargo fmt --all --check` |
| clippy | `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` |
| tests | `cargo test --workspace --locked` |
| cargo-deny | `cargo deny check`: licences, advisories, bans, sources (`deny.toml`) |
| hygiene | every crate is Apache-2.0, and nothing in the tree, the commit messages or the pull request text refers to how mansio is built rather than to mansio |
| e2e | the end-to-end suite against Iggy, through compose |
| offline | consumers killed while idle and while holding a lease, every item done once |

Run the first four before you push. A weakened or deleted test is a failed
review; add tests, do not trade them.

## The code

- Follow the Rust profile in [ARCHITECTURE.md](ARCHITECTURE.md#rust-profile):
  `unsafe` is denied; `unwrap`, `expect`, `panic`, `todo`, `dbg!` and printing
  to stdout are denied outside tests; an exception is an
  `#[expect(lint, reason = "...")]` on the item; errors are typed (`thiserror` in
  libraries, `anyhow` only at a binary's edge); request types deny unknown
  fields; secrets are `secrecy` types compared in constant time.
- Every limit, interval, count and switch is a named setting, read from the
  environment with a default and documented in the README's settings table. A
  constant does not stand in for a setting.
- An operation ships on every surface it has: the HTTP API, the CLI and the MCP
  server use the same names. A test fails if the CLI and MCP drift apart.
- Test the way a user does: through the HTTP API, in `crates/mansio-e2e`. Add a
  unit test only where the logic is dense enough to earn one.
- Change [ARCHITECTURE.md](ARCHITECTURE.md) in the same pull request when the
  code departs from it.

## Writing

No em-dashes, in prose, code comments, commit messages or pull requests. Use a
comma, a colon or a new sentence. Cut rather than elaborate.

## Commits and pull requests

- Sign your commits (`git commit -S`), so that GitHub shows them as verified.
- One change per pull request, kept to what the issue asks. Something else you
  find along the way becomes its own issue.
- Fill in the pull request template: what, why, the done criteria, the settings
  added, and how you tested it.
- Changes to the rules (`.github/`, `deny.toml`, the toolchain and lint files,
  `Cargo.toml`, the Dockerfile and compose) and to the code that decides who may
  do what (credentials, access, sessions, provisioning and the registry) are
  owned in `.github/CODEOWNERS` and wait for the owner's review.
