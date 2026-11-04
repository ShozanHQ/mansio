#!/usr/bin/env bash
# Checks that demo-check.sh fails on a failing block and names the block and
# the first failing command, and that it fails on a reference to a file that
# is not tracked before it builds or starts anything.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=$("$here/demo-check.sh" "$here/fixtures/failing-demo.md" 2>&1)
status=$?
printf '%s\n' "$out"
[ "$status" -ne 0 ] || { echo "demo-check-test: expected a non-zero exit" >&2; exit 1; }
grep -qF "bash block 2 of" <<<"$out" || { echo "demo-check-test: block 2 not named" >&2; exit 1; }
grep -qF "false-command-for-the-check" <<<"$out" || { echo "demo-check-test: command not named" >&2; exit 1; }

# A reference to a file that is not tracked fails before anything is built:
# with no binaries and no cargo on PATH, a check that ran later would fail
# differently.
out=$(MANSIO_SERVER_BIN=/nonexistent/mansio-server MANSIO_BIN=/nonexistent/mansio PATH=/usr/bin:/bin \
  "$here/demo-check.sh" "$here/fixtures/bad-reference-demo.md" 2>&1)
status=$?
printf '%s\n' "$out"
[ "$status" -ne 0 ] || { echo "demo-check-test: expected a non-zero exit for a bad reference" >&2; exit 1; }
grep -qF "reference [[docs/nope.md]]" <<<"$out" || { echo "demo-check-test: bad reference not named" >&2; exit 1; }
! grep -qE 'cargo|Compiling|demo-check: the server' <<<"$out" || { echo "demo-check-test: the reference check ran after a build or a server start" >&2; exit 1; }
echo "demo-check-test: ok"
