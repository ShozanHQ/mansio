#!/usr/bin/env bash
# The walkthroughs that cover a release, as Markdown for the run summary: each
# demo in docs/demos/ that lists, under "What shipped", a pull request merged
# since the last v* tag, as a diffmap link at HEAD, then the pull requests in
# no demo. Run on the release commit.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
to=$(git rev-parse HEAD)
repo=${GITHUB_REPOSITORY:-ShozanHQ/mansio}
# The release before this commit, so a rerun after the tag lists the same.
from=$(git describe --tags --abbrev=0 --match 'v[0-9]*' HEAD^ 2>/dev/null || true)
# A merge's title ends in its pull request's number. The release commit itself
# is not a change to show; any other pull request titled like it is.
prs=$(git log --first-parent --format=%s ${from:+"$from.."}HEAD \
  | sed '1{/^chore: release/d;}' | sed -n 's/.*(#\([0-9][0-9]*\))$/\1/p' | sort -n)

echo "## Walkthroughs since ${from:-the first commit}"
echo
shopt -s nullglob
covered=" "
for demo in docs/demos/[0-9]*.md; do
  rows=$(awk '/^## /{shipped = /^## What shipped/} shipped' "$demo")
  here=""
  for n in $prs; do
    if grep -qE "^\| *#$n *\|" <<<"$rows"; then
      here="$here #$n"
      covered="$covered$n "
    fi
  done
  [ -z "$here" ] || echo "- [$(basename "$demo" .md)](https://diffmap.dev/$repo/commit/$to/$demo):$here"
done
missing=""
for n in $prs; do
  case "$covered" in *" $n "*) ;; *) missing="$missing #$n" ;; esac
done
[ -z "$missing" ] || printf '\nIn no walkthrough:%s\n' "$missing"
[ -n "$prs" ] || echo "No pull request merged."
