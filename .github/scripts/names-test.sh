#!/usr/bin/env bash
# The test of names.sh: that it still refuses an internal ticket id and the
# issue tracker's host, in any letter case, and lets the product name through.
# Run from the root of the checkout:
#
#   names-test.sh
#
# It feeds names.sh text through stdin and names.sh tree a scratch git
# repository, prints one line per case and exits 1 if any case fails.
#
# Every id and host is built from pieces, so that this file does not match the
# check it tests.
set -euo pipefail

names=$PWD/.github/scripts/names.sh
a=sho
c=lin
d=ear
e=app
id="${a}-123"
host="${c}${d}.${e}"
upper() { printf '%s' "$1" | tr '[:lower:]' '[:upper:]'; }

failed=0

# report <exit code wanted> <case> <exit code got>
report() {
  if [ "$3" -eq "$1" ]; then
    echo "ok    $2"
  else
    echo "FAIL  $2: exit $3, wanted $1"
    failed=1
  fi
}

# text <exit code wanted> <case>, with the text on stdin
text() {
  local got=0
  bash "$names" text case >/dev/null 2>&1 || got=$?
  report "$1" "$2" "$got"
}

# tree <exit code wanted> <case> <the one file's content>
tree() {
  local repo got=0
  repo=$(mktemp -d "${TMPDIR:-/tmp}/names-test.XXXXXX")
  printf '%s\n' "$3" >"$repo/file.txt"
  git -C "$repo" init -q
  git -C "$repo" add file.txt
  (cd "$repo" && bash "$names" tree >/dev/null 2>&1) || got=$?
  rm -rf "$repo"
  report "$1" "$2" "$got"
}

# history: a scratch repository in which one commit adds a file with a ticket
# id and the next removes it. The final tree is clean; the first commit is not.
history() {
  local repo got=0 first out
  repo=$(mktemp -d "${TMPDIR:-/tmp}/names-test.XXXXXX")
  git -C "$repo" init -q
  git -C "$repo" config user.email test@example.com
  git -C "$repo" config user.name test
  git -C "$repo" config commit.gpgsign false
  printf 'clean\n' >"$repo/clean.txt"
  printf 'Fixes %s.\n' "$id" >"$repo/stray.txt"
  git -C "$repo" add .
  git -C "$repo" commit -q -m one
  first=$(git -C "$repo" rev-parse HEAD)
  git -C "$repo" rm -q stray.txt
  git -C "$repo" commit -q -m two
  (cd "$repo" && bash "$names" tree >/dev/null 2>&1) || got=$?
  report 0 "history: the final tree passes" "$got"
  got=0
  out=$(cd "$repo" && bash "$names" tree "$first" 2>&1) || got=$?
  report 1 "history: the commit that adds the file fails" "$got"
  case $out in
    *"$first"*stray.txt*) echo "ok    history: the output names the commit and the path" ;;
    *) echo "FAIL  history: the output names the commit and the path"; failed=1 ;;
  esac
  got=0
  (cd "$repo" && bash "$names" tree HEAD >/dev/null 2>&1) || got=$?
  report 0 "history: the commit that removes it passes" "$got"
  rm -rf "$repo"
}

text 1 "text: a ticket id fails" <<<"Fixes ${id} in the lease code"
text 1 "text: a ticket id in capitals fails" <<<"Fixes $(upper "$id") in the lease code"
text 1 "text: a ticket id on a later line fails" <<<$'A clean line.\n'"Closes ${id}."
text 1 "text: the tracker's host fails" <<<"See https://${host}/team/issue"
text 1 "text: the tracker's host in capitals fails" <<<"See https://$(upper "$host")/team/issue"
text 0 "text: the product name passes" <<<"mansio is built with Shozan."
text 0 "text: Built with Shozan. passes" <<<"Built with Shozan."
text 0 "text: a clean line passes" <<<"Retry with backoff, then a dead topic."
text 0 "text: empty input passes" </dev/null

tree 1 "tree: a file with a ticket id fails" "Fixes ${id}."
tree 1 "tree: a file with the tracker's host fails" "See https://${host}/team/issue"
tree 0 "tree: a file with the product name passes" "Built with Shozan."
tree 0 "tree: a clean file passes" "Retry with backoff, then a dead topic."

history

exit "$failed"
