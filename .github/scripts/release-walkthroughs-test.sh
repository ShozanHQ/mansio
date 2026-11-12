#!/usr/bin/env bash
# Checks release-walkthroughs.sh on a scratch repository: pull requests before
# and after a tag, one after it in a demo and the rest in none.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
fail() { echo "release-walkthroughs-test: $*" >&2; exit 1; }
# Without a scratch directory, git would run in this checkout.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/walkthroughs.XXXXXX") || fail "no scratch directory"
trap 'rm -rf "$tmp"' EXIT
g() { git -C "$tmp" -c user.name=test -c user.email=test@example.com -c commit.gpgsign=false -c tag.gpgsign=false "$@"; }
list() { (cd "$tmp" && GITHUB_REPOSITORY=example/repo bash "$here/release-walkthroughs.sh"); }

g init -q -b main
mkdir -p "$tmp/docs/demos"
# #30 is shipped and #3 is not: a number is not matched by its prefix. #4 is
# in a table, but not the one under "What shipped".
printf '%s\n' '## What shipped' '' '| Pull request | What |' '|---|---|' \
  '| #1 | Before the tag. |' '| #30 | After the tag. |' '' \
  '## Still open' '' '| #4 | Not shipped. |' >"$tmp/docs/demos/2026-01-02.md"
# An older demo, with nothing merged since the tag.
printf '%s\n' '## What shipped' '' '| #1 | Before the tag. |' >"$tmp/docs/demos/2026-01-01.md"
g add .
g commit -q -m 'First (#1)'
# An older release and, below, a tag that is no release: neither is the start.
g tag v0.0.1
g commit -q --allow-empty -m 'Second (#2)'
g tag v0.1.0
g commit -q --allow-empty -m 'Third (#3)'
g tag nightly
g commit -q --allow-empty -m 'Fourth (#4)'
g commit -q --allow-empty -m 'Thirtieth (#30)'
# Titled like a release, and not the release commit.
g commit -q --allow-empty -m 'chore: release the hounds (#7)'
# A merge: its title counts, its branch's commit does not.
g checkout -q -b side
g commit -q --allow-empty -m 'On the branch (#9)'
g checkout -q main
g merge -q --no-ff side -m 'Merged (#8)'
g commit -q --allow-empty -m 'chore: release v0.2.0 (#5)'
at="https://diffmap.dev/example/repo/commit/$(g rev-parse HEAD)/docs/demos"
demo="$at/2026-01-02.md"

# Since the tag: the demo with the one it ships, the others named, nothing
# from before the tag, and not the release commit.
out=$(list) || fail "the script failed"
printf '%s\n' "$out"
grep -qxF -- "- [2026-01-02]($demo): #30" <<<"$out" || fail "the demo is not listed with #30 alone"
grep -qxF "In no walkthrough: #3 #4 #7 #8" <<<"$out" || fail "#3, #4, #7 and #8 are not named as in no walkthrough"
! grep -qE '#(1|2|5|9)( |$)' <<<"$out" || fail "listed: a pull request from before the tag, the release commit or a branch commit"
! grep -qF "2026-01-01" <<<"$out" || fail "a demo with nothing since the tag is listed"

# A rerun after the release is tagged lists the same.
g tag v0.2.0
again=$(list) || fail "the script failed on the tagged release commit"
[ "$again" = "$out" ] || fail "the list changed once the release commit was tagged"

# Before the first release: everything since the first commit.
g tag -d v0.0.1 v0.1.0 v0.2.0 >/dev/null
out=$(list) || fail "the script failed with no tag"
printf '%s\n' "$out"
grep -qxF -- "- [2026-01-01]($at/2026-01-01.md): #1" <<<"$out" || fail "with no tag, the older demo is not listed with #1"
grep -qxF -- "- [2026-01-02]($demo): #1 #30" <<<"$out" || fail "with no tag, the demo is not listed with #1 and #30"
grep -qxF "In no walkthrough: #2 #3 #4 #7 #8" <<<"$out" || fail "with no tag, #2, #3, #4, #7 and #8 are not named"

# A release with nothing in it says so.
g tag v0.1.9 HEAD^
out=$(list) || fail "the script failed on an empty release"
grep -qxF "No pull request merged." <<<"$out" || fail "an empty release is not named as empty"
echo "release-walkthroughs-test: ok"
