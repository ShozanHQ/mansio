#!/usr/bin/env bash
# Checks that demo-check.sh fails on a failing block and names the block and
# the first failing command.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=$("$here/demo-check.sh" "$here/fixtures/failing-demo.md" 2>&1)
status=$?
printf '%s\n' "$out"
[ "$status" -ne 0 ] || { echo "demo-check-test: expected a non-zero exit" >&2; exit 1; }
grep -qF "bash block 2 of" <<<"$out" || { echo "demo-check-test: block 2 not named" >&2; exit 1; }
grep -qF "false-command-for-the-check" <<<"$out" || { echo "demo-check-test: command not named" >&2; exit 1; }
echo "demo-check-test: ok"
