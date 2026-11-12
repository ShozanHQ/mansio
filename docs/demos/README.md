# Demos

A demo is a walkthrough of what shipped in a week: `docs/demos/<date>.md`. It
has a "What shipped" table of pull requests, the open issues, and numbered
steps. CI runs the fenced `bash` blocks of the newest file, in order, against a
fresh memory-broker server, with `.github/scripts/demo-check.sh`. The start
step is fenced `sh` and what a command prints `text`, so neither runs.

## Link the code

Demos are written for [diffmap](https://github.com/tanishqkancharla/diffmap),
which links prose to code.

- Every row of "What shipped" links the code it describes with a reference: a
  path from the repository root in double square brackets, such as
  `[[crates/mansio-server/src/app.rs]]`, with an optional `#symbol` (only for a
  symbol you found in that file) or `#L12-L30` line range. Pin a line range
  with `<sha>:` before the path, since lines move. The commit must be one that
  changed the file, such as the last one (`git log -1 --format=%h -- <path>`);
  the viewer shows the file as of that commit and refuses any other commit.
- A design change, such as a new flow across services or a changed data path,
  gets a `services` or `mermaid diff` fence in its step. A bump or a fix does
  not.
- A reference names a tracked file. `demo-check.sh` checks that before it
  runs anything, and fails naming the file. Commits are not checked.

## View a demo

For a commit or a pull request, swap `github.com` for `diffmap.dev` and add the
file's path:

- `https://diffmap.dev/ShozanHQ/mansio/commit/<sha>/docs/demos/<date>.md`
- `https://diffmap.dev/ShozanHQ/mansio/pull/<n>/docs/demos/<date>.md`

Or from a checkout, at the repository root, with the pinned viewer:

```sh
npx @tanishqkancharla/diffmap@0.10.0 serve docs/demos/<date>.md
```

The syntax is in the same command with `guide` in place of `serve` and the
path.

## At a release

Before anything is published, the release run's summary links the demos that
cover the release and names the pull requests in none
(`.github/scripts/release-walkthroughs.sh`). It reads the rows under
`## What shipped`: one pull request a row, starting `| #N |`.
