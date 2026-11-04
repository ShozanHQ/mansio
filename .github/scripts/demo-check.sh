#!/usr/bin/env bash
# Runs a demo: every fenced `bash` block of a markdown file under docs/demos/,
# in order, in one shell, against a fresh mansio-server on the memory broker.
# Fails, naming the block and the first command that exits non-zero.
#
#   demo-check.sh docs/demos/2026-11-06.md
#
# Before it builds or starts anything it checks every `[[...]]` reference in
# the file outside its `bash`, `sh` and `text` fences (so a `[[ ]]` test in a
# step is not read as one): the part before the first `#` (and without a
# leading `<sha>:` or `<sha>..<sha>:`) must be a tracked file, by
# `git ls-files --error-unmatch` from the repository root. A failure prints the
# path and exits 1. Commits are not checked: CI's checkout is shallow.
#
# Blocks fenced as anything but `bash` (the step that starts the server is
# `sh`, what a command prints is `text`) are not run. The shell the blocks run
# in has what a reader has after the start step:
#
#   MANSIO_URL, MANSIO_TOKEN  the server and its admin token
#   MANSIO_DEMO_DIR           an empty directory; the server's sink is
#                             $MANSIO_DEMO_DIR/sink.jsonl
#   PATH                      with the `mansio` command first
#
# MANSIO_SERVER_BIN and MANSIO_BIN say where the two binaries are; if they are
# not there, cargo builds them first. MANSIO_DEMO_PORT picks the port; by
# default a free one is found.
set -euo pipefail

if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then
  echo "usage: $0 <demo.md>" >&2
  exit 2
fi
demo=$1

root=$(cd "$(dirname "$0")/../.." && pwd)

# Every reference names a tracked file. The `|| true` keeps a demo without
# references passing under pipefail.
missing=0
refs=$( { awk '
    /^[[:space:]]*```(bash|sh|text)[[:space:]]*$/ && !skip { skip = 1; next }
    /^[[:space:]]*```/ && skip { skip = 0; next }
    !skip' "$demo" | grep -o '\[\[[^]]*\]\]' || true; } \
  | sed -e 's/^\[\[//' -e 's/\]\]$//' -e 's/#.*$//' \
        -e 's/^[0-9a-fA-F]*\.\.[0-9a-fA-F]*://' -e 's/^[0-9a-fA-F]*://' \
  | sort -u)
while IFS= read -r ref; do
  [ -n "$ref" ] || continue
  if ! (cd "$root" && git ls-files --error-unmatch -- "$ref") >/dev/null 2>&1; then
    echo "demo-check: reference [[$ref]] in $demo is not a tracked file" >&2
    missing=1
  fi
done <<EOF_REFS
$refs
EOF_REFS
[ "$missing" -eq 0 ] || exit 1

server_bin=${MANSIO_SERVER_BIN:-$root/target/debug/mansio-server}
cli_bin=${MANSIO_BIN:-$root/target/debug/mansio}
if [ ! -x "$server_bin" ] || [ ! -x "$cli_bin" ]; then
  (cd "$root" && cargo build -p mansio -p mansio-server --locked)
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/mansio-demo.XXXXXX")
server_pid=
cleanup() {
  if [ -n "$server_pid" ]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

# The blocks, with a marker before each so that a failure names its block.
awk '
  /^```bash[[:space:]]*$/ && !inside { inside = 1; n++; printf "__demo_block=%d\n", n; next }
  /^```/ && inside { inside = 0; next }
  inside { print }
  END { if (n == 0) exit 3 }
' "$demo" >"$work/blocks.sh" || {
  echo "demo-check: $demo has no bash block" >&2
  exit 1
}

port=${MANSIO_DEMO_PORT:-}
if [ -z "$port" ]; then
  for _ in $(seq 1 50); do
    candidate=$((20000 + RANDOM % 20000))
    if ! (exec 3<>"/dev/tcp/127.0.0.1/$candidate") 2>/dev/null; then
      port=$candidate
      break
    fi
  done
fi
[ -n "$port" ] || { echo "demo-check: no free port found" >&2; exit 1; }

mkdir -p "$work/demo"
(
  cd "$work"
  exec env MANSIO_LISTEN="127.0.0.1:$port" MANSIO_BROKER=memory MANSIO_TOKEN=demo-token \
    MANSIO_SINK_PATH="$work/demo/sink.jsonl" MANSIO_SNAPSHOT_DIR= \
    "$server_bin"
) >"$work/server.log" 2>&1 &
server_pid=$!

ready=
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if [ -z "$ready" ]; then
  echo "demo-check: the server did not become healthy; its log:" >&2
  cat "$work/server.log" >&2
  exit 1
fi

export MANSIO_URL="http://127.0.0.1:$port" MANSIO_TOKEN=demo-token
export MANSIO_DEMO_DIR="$work/demo"
export PATH
PATH="$(dirname "$cli_bin"):$PATH"

# One shell, so a variable set in one block is there in the next. The ERR trap
# names the block and the command; set -e then ends the run.
runner="$work/run.sh"
{
  echo '__demo_block=0'
  echo 'set -eEuo pipefail'
  printf '__demo_file=%s\n' "$(printf %q "$demo")"
  # Only the main shell reports, so a failure inside $(...) prints once.
  # shellcheck disable=SC2016
  echo 'trap '"'"'[ "$BASH_SUBSHELL" -eq 0 ] && echo "demo-check: FAIL in bash block $__demo_block of $__demo_file: $BASH_COMMAND" >&2'"'"' ERR'
  cat "$work/blocks.sh"
} >"$runner"

if bash "$runner"; then
  echo "demo-check: $demo passed"
else
  status=$?
  echo "demo-check: server log, last lines:" >&2
  tail -n 20 "$work/server.log" >&2
  exit "$status"
fi
