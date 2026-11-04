#!/usr/bin/env bash
# The hygiene check of names. mansio may say that it is built with Shozan, but
# it carries nothing that is a dead end to anyone outside: no internal ticket
# id, and no link to the issue tracker, which its host gives away. Run as
#
#   names.sh tree [<commit>] on the tracked files of the checkout, or of the
#                            tree of <commit>, binary ones included, and on
#                            their paths; a commit's lines start with its id
#                            and the path
#   names.sh text <label>    on text from stdin, such as a commit message or a
#                            pull request title or body; <label> says which
#
# It prints each line that carries one of them and exits 1, or exits 0. A line
# is cut to 300 printable characters for the log, so that a binary file does
# not fill it.
#
# Every pattern is built from pieces, so that this file does not match them.
set -euo pipefail
# Bytes, not characters, so that a binary file matches the same way anywhere.
export LC_ALL=C

a=sho
c=lin
d=ear
e=app
export ID="${a}-[0-9]"
export HOST="(?<![\\w-])${c}${d}\\.${e}(?![\\w-])"

# Reads lines on stdin and prints those that carry a ticket id or the
# tracker's host, after the label $1 if it is not empty.
strays() {
  LABEL=$1 perl -ne '
    $line = $_;
    chomp $line;
    if (/$ENV{ID}|$ENV{HOST}/i) {
      $shown = substr($line, 0, 300);
      $shown =~ s/[^\x20-\x7e]/?/g;
      print(($ENV{LABEL} ne "" ? "$ENV{LABEL}: " : "") . "$shown\n");
    }
  '
}

case "${1:-}" in
  tree)
    commit=${2:-}
    if [ -n "$commit" ]; then
      label="commit $commit"
      git rev-parse --verify --quiet "${commit}^{tree}" >/dev/null || {
        echo "names.sh: no such commit: $commit" >&2
        exit 2
      }
      pathlabel="path in $commit"
    else
      label="the tree"
      pathlabel="path"
    fi
    # -a reads a binary file as text: without it git grep only says that a
    # binary file matches, and nothing here would test it. The first pass
    # over-matches; strays() is the test.
    contents=$({ git grep -a -i -n -E "${ID}|${c}${d}\\.${e}" ${commit:+"$commit"} || [ $? -eq 1 ]; } | strays "")
    if [ -n "$commit" ]; then
      paths=$(git ls-tree -r --name-only "$commit" | strays "$pathlabel")
    else
      paths=$(git ls-files | strays "$pathlabel")
    fi
    stray=$(printf '%s\n%s\n' "$paths" "$contents" | sed '/^$/d')
    ;;
  text)
    label=${2:?usage: names.sh text <label>}
    stray=$(cat | strays "$label")
    ;;
  *)
    echo "usage: names.sh tree [<commit>] | names.sh text <label>" >&2
    exit 2
    ;;
esac

if [ -n "$stray" ]; then
  printf '%s\n' "$stray"
  echo "::error::${label} carries an internal ticket id or an issue tracker link, which no one outside can follow"
  exit 1
fi
