#!/usr/bin/env bash
# The hygiene check of names. mansio carries its own words and nothing from the
# system that builds it: not that system's name, not its internal ids, not the
# host of its internal tools. Run as
#
#   names.sh tree            on the tracked files of the checkout
#   names.sh text <label>    on text from stdin, such as a commit message or a
#                            pull request title or body; <label> says which
#
# It prints each line that gives one of them away and exits 1, or exits 0.
#
# Every pattern is built from pieces, so that this file does not match them.
# The one place the name may appear is this repository's own address,
# github.com/<owner>/mansio, where the owner is named for that system. The
# address is struck out of a line, in any case and only whole (not as the start
# of a longer name or of a longer host), before the line is tested.
set -euo pipefail

a=sho
b=zan
c=lin
d=ear
e=app
export PRODUCT="${a}${b}"
export ID="${a}-[0-9]"
export HOST="(?<![\\w-])${c}${d}\\.${e}(?![\\w-])"

strays() {
  perl -ne '
    $line = $_;
    chomp $line;
    s{(?<![\w.-])github\.com/\Q$ENV{PRODUCT}\EHQ/mansio(?![\w-])}{}gi;
    if (/\Q$ENV{PRODUCT}\E|$ENV{ID}|$ENV{HOST}/i) {
      print(($ENV{LABEL} ne "" ? "$ENV{LABEL}: " : "") . "$line\n");
    }
  '
}

case "${1:-}" in
  tree)
    label=""
    # A first pass that over-matches; strays() is the test.
    text=$(git grep -i -n -E "${PRODUCT}|${ID}|${c}${d}\\.${e}" || [ $? -eq 1 ])
    ;;
  text)
    label=${2:?usage: names.sh text <label>}
    text=$(cat)
    ;;
  *)
    echo "usage: names.sh tree | names.sh text <label>" >&2
    exit 2
    ;;
esac

stray=$(printf '%s\n' "$text" | LABEL="$label" strays)
if [ -n "$stray" ]; then
  printf '%s\n' "$stray"
  echo "::error::${label:-the tree} refers to something internal to how mansio is built; mansio's own words only, bar this repository's address"
  exit 1
fi
