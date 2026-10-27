#!/usr/bin/env bash
# The hygiene check of names. mansio carries its own words and nothing from the
# system that builds it: not that system's name, not its internal ids, not the
# host of its internal tools. Run as
#
#   names.sh tree            on the tracked files of the checkout, binary ones
#                            included, and on their paths
#   names.sh text <label>    on text from stdin, such as a commit message or a
#                            pull request title or body; <label> says which
#
# It prints each line that gives one of them away and exits 1, or exits 0. A
# line is cut to 300 printable characters for the log, so that a binary file
# does not fill it.
#
# Every pattern is built from pieces, so that this file does not match them.
# The one place the name may appear is this repository's own address,
# github.com/<owner>/mansio, where the owner is named for that system. The
# address is struck out of a line, in any case and only whole (not as the start
# of a longer name or of a longer host), before the line is tested.
set -euo pipefail
# Bytes, not characters, so that a binary file matches the same way anywhere.
export LC_ALL=C

a=sho
b=zan
c=lin
d=ear
e=app
export PRODUCT="${a}${b}"
export ID="${a}-[0-9]"
export HOST="(?<![\\w-])${c}${d}\\.${e}(?![\\w-])"

# Reads lines on stdin and prints those that still name one of them once the
# address is struck out, after the label $1 if it is not empty.
strays() {
  LABEL=$1 perl -ne '
    $line = $_;
    chomp $line;
    s{(?<![\w.-])github\.com/\Q$ENV{PRODUCT}\EHQ/mansio(?![\w-])}{}gi;
    if (/\Q$ENV{PRODUCT}\E|$ENV{ID}|$ENV{HOST}/i) {
      $shown = substr($line, 0, 300);
      $shown =~ s/[^\x20-\x7e]/?/g;
      print(($ENV{LABEL} ne "" ? "$ENV{LABEL}: " : "") . "$shown\n");
    }
  '
}

case "${1:-}" in
  tree)
    label="the tree"
    # -a reads a binary file as text: without it git grep only says that a
    # binary file matches, and nothing here would test it. The first pass
    # over-matches; strays() is the test.
    contents=$({ git grep -a -i -n -E "${PRODUCT}|${ID}|${c}${d}\\.${e}" || [ $? -eq 1 ]; } | strays "")
    paths=$(git ls-files | strays "path")
    stray=$(printf '%s\n%s\n' "$paths" "$contents" | sed '/^$/d')
    ;;
  text)
    label=${2:?usage: names.sh text <label>}
    stray=$(cat | strays "$label")
    ;;
  *)
    echo "usage: names.sh tree | names.sh text <label>" >&2
    exit 2
    ;;
esac

if [ -n "$stray" ]; then
  printf '%s\n' "$stray"
  echo "::error::${label} refers to something internal to how mansio is built; mansio's own words only, bar this repository's address"
  exit 1
fi
