#!/bin/sh
# The offline-consumer test: work is never dropped and never done twice, for
# consumers that are not always connected.
#
#   MANSIO_TOKEN=<admin token> tests/offline/run.sh
#
# It assumes a running mansio-server (compose, with Iggy, in CI) and a built
# `mansio`, and talks to the server only through the CLI. Each scenario uses a
# tenant of its own and an agent credential minted for it, and a consumer
# (consumer.sh) that appends each item it acknowledges to a file.
#
#   1. A consumer is started and killed with kill -9. N items are published.
#      A new process of the consumer starts. The file ends with N lines, N
#      distinct ids, none twice, every published id in it, and facts.work
#      holds a claim and an ack of attempt 1 for each item and nothing else.
#   2. N items are published. A consumer that kills itself right after its
#      first claim dies holding that item's lease. A consumer under another
#      name takes the rest, and the held item once its lease has ended. The
#      file ends as in 1, and facts.work holds, for the held item, exactly
#      one expired and one retried, with the claim and the ack of attempt 2.
#   3. As in 2, but the consumer that died is started again under the same
#      name while its lease is live. A consumer that holds a live lease is
#      given it again, so it gets the held item back, acknowledges it once,
#      and facts.work holds no expiry and no second attempt for it.
#
# The consumer of scenario 2 that takes over has a name of its own because a
# restart under the dead consumer's name would be given its lease back, and
# there would be no expiry to see: that is scenario 3.
#
# After the last item of a scenario is done the test watches for a late
# duplicate for as long as a redelivery can take: the lease, the longest
# backoff and the server's tick and the CLI's poll interval twice over, all
# read from the server's settings.
#
# The lease and the backoff of the server decide how long scenarios 2 and 3
# take, so run the server with short ones (README, "The guarantee").
#
# Settings, all optional except the token:
#   MANSIO_TOKEN                            the server's admin token, required
#   MANSIO_URL                              the server, default http://127.0.0.1:7070
#   MANSIO_BIN                              the mansio command, default mansio
#   MANSIO_OFFLINE_ITEMS                    items in each scenario, default 50
#   MANSIO_OFFLINE_TIMEOUT_SECONDS          how long a scenario waits for every
#                                           item, default 120
#   MANSIO_OFFLINE_SETTLE_SECONDS           how long it then watches for a late
#                                           duplicate, default the lease plus
#                                           the backoff cap plus a margin; it
#                                           refuses a shorter one
#   MANSIO_OFFLINE_KILL_WAIT_SECONDS        how long a killed consumer may take
#                                           to disappear, default 5
#   MANSIO_POLL_INTERVAL_MILLIS             the CLI's, default 500, part of the
#                                           margin
#   MANSIO_OFFLINE_CREDENTIAL_TTL_SECONDS   how long the credentials last,
#                                           default 3600
# and the settings of consumer.sh.

set -eu

here=$(cd "$(dirname "$0")" && pwd)

: "${MANSIO_TOKEN:?MANSIO_TOKEN is the admin token of the server under test}"
MANSIO_URL=${MANSIO_URL:-http://127.0.0.1:7070}
MANSIO_BIN=${MANSIO_BIN:-mansio}
export MANSIO_URL MANSIO_BIN
items=${MANSIO_OFFLINE_ITEMS:-50}
timeout=${MANSIO_OFFLINE_TIMEOUT_SECONDS:-120}
settle=${MANSIO_OFFLINE_SETTLE_SECONDS:-}
kill_wait=${MANSIO_OFFLINE_KILL_WAIT_SECONDS:-5}
poll_millis=${MANSIO_POLL_INTERVAL_MILLIS:-500}
credential_ttl=${MANSIO_OFFLINE_CREDENTIAL_TTL_SECONDS:-3600}

queue=offline
run=$(date +%s)-$$
work=$(mktemp -d "${TMPDIR:-/tmp}/mansio-offline.XXXXXX")

say() {
  printf '%s\n' "$*"
}

show_logs() {
  for log in "$work"/*.log; do
    if [ -f "$log" ]; then
      printf -- '--- %s, the last 15 lines\n' "$(basename "$log")" >&2
      tail -n 15 "$log" >&2
    fi
  done
}

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  show_logs
  exit 1
}

cleanup() {
  # Every consumer of this run is named offline-<run>-<something>.
  pkill -9 -f -- " offline-$run-" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mansio() {
  "$MANSIO_BIN" "$@"
}

number() {
  case $2 in
    '' | *[!0-9]*) fail "$1 must be a whole number, not '$2'" ;;
  esac
}

# ---- before anything is started ---------------------------------------------

for tool in jq curl pkill pgrep; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool is needed and was not found"
done
command -v "$MANSIO_BIN" >/dev/null 2>&1 || fail "$MANSIO_BIN was not found: build it (cargo build -p mansio) and set MANSIO_BIN"
number MANSIO_OFFLINE_ITEMS "$items"
number MANSIO_OFFLINE_TIMEOUT_SECONDS "$timeout"
if [ -n "$settle" ]; then
  number MANSIO_OFFLINE_SETTLE_SECONDS "$settle"
fi
number MANSIO_OFFLINE_KILL_WAIT_SECONDS "$kill_wait"
number MANSIO_POLL_INTERVAL_MILLIS "$poll_millis"
number MANSIO_OFFLINE_CREDENTIAL_TTL_SECONDS "$credential_ttl"
[ "$items" -ge 1 ] || fail "MANSIO_OFFLINE_ITEMS must be at least 1"

# The token goes to curl on its standard input, not on its command line.
settings=$(printf 'header = "Authorization: Bearer %s"\n' "$MANSIO_TOKEN" |
  curl --fail --silent --show-error --max-time 10 --config - "$MANSIO_URL/v1/settings") ||
  fail "GET $MANSIO_URL/v1/settings failed: is the server up, and is MANSIO_TOKEN its admin token?"
lease=$(printf '%s\n' "$settings" | jq -er .lease_seconds) || fail "the server's settings have no lease_seconds"
backoff_cap=$(printf '%s\n' "$settings" | jq -er .backoff_cap_seconds) || fail "the server's settings have no backoff_cap_seconds"
if [ "$timeout" -le $((lease + backoff_cap)) ]; then
  fail "the server leases for $lease s and waits up to $backoff_cap s to retry, and the timeout is $timeout s: run the server with a shorter lease (MANSIO_LEASE_SECONDS) or set MANSIO_OFFLINE_TIMEOUT_SECONDS higher"
fi
tick_millis=$(printf '%s\n' "$settings" | jq -er .tick_millis) || fail "the server's settings have no tick_millis"
# An item that is redelivered late arrives within the lease, the longest
# backoff, a tick of the server's timer and a look of the consumer, and the
# margin allows each of the last two twice.
margin=$(( (2 * (tick_millis + poll_millis) + 999) / 1000 ))
settle_least=$((lease + backoff_cap + margin))
if [ -z "$settle" ]; then
  settle=$settle_least
elif [ "$settle" -lt "$settle_least" ]; then
  fail "MANSIO_OFFLINE_SETTLE_SECONDS is $settle s, and a redelivery can take $settle_least s ($lease s lease, $backoff_cap s backoff cap, $margin s for the tick and the poll interval): a duplicate would arrive after the check"
fi
say "server $MANSIO_URL: lease $lease s, backoff cap $backoff_cap s, $items items per scenario, timeout $timeout s, settle $settle s"

# ---- steps ------------------------------------------------------------------

# mint TENANT: an agent credential in $credential that may claim from the queue.
mint() {
  reply=$(mansio principals create --kind agent --name consumer --tenant "$1" \
    --scopes "[{\"topic\":\"work.$queue\",\"verbs\":[\"claim\"]}]" \
    --ttl-seconds "$credential_ttl") || fail "principals create failed for $1"
  credential=$(printf '%s\n' "$reply" | jq -er .credential) || fail "principals create answered without a credential"
}

# publish_items TENANT FILE: publish the items, and write their ids to FILE.
publish_items() {
  : >"$2"
  n=1
  while [ "$n" -le "$items" ]; do
    reply=$(mansio work publish --tenant "$1" --queue "$queue" \
      --type offline.item --source mansio-offline-test --data "{\"n\":$n}") ||
      fail "work publish $n of $items failed"
    printf '%s\n' "$reply" | jq -er .itemid >>"$2" || fail "work publish answered without an itemid"
    n=$((n + 1))
  done
  say "published $items items"
}

# start_consumer TENANT NAME OUTFILE STOP: start consumer.sh as NAME with the
# credential, in the background; its pid is in $consumer_pid and its log in
# $work/NAME.log.
start_consumer() {
  MANSIO_TOKEN=$credential MANSIO_OFFLINE_STOP_AFTER_CLAIM=$4 \
    sh "$here/consumer.sh" "$1" "$queue" "$2" "$3" >>"$work/$2.log" 2>&1 &
  consumer_pid=$!
}

# kill_dead NAME PID: stop the consumer dead, and the mansio it is running. A
# pull that is waiting for an item is a process of its own, and left alone it
# would go on claiming for a consumer that is gone.
kill_dead() {
  kill -STOP "$2" 2>/dev/null || true
  pkill -9 -f -- " $1 " 2>/dev/null || true
  kill -9 "$2" 2>/dev/null || true
  wait "$2" 2>/dev/null || true
  gone "$1"
}

# gone NAME: wait until no process has NAME on its command line. A process
# that was killed can be seen for a moment while it dies, so look again, and
# kill what is still there, for MANSIO_OFFLINE_KILL_WAIT_SECONDS at most.
gone() {
  waited=0
  while pgrep -f -- " $1 " >/dev/null 2>&1; do
    [ "$waited" -lt "$kill_wait" ] || fail "consumer $1 is still running after kill -9"
    pkill -9 -f -- " $1 " 2>/dev/null || true
    sleep 1
    waited=$((waited + 1))
  done
}

# wait_for_log NAME TEXT: wait until the consumer's log has a line TEXT.
wait_for_log() {
  waited=0
  until grep -q -x -- "$2" "$work/$1.log" 2>/dev/null; do
    [ "$waited" -lt "$timeout" ] || fail "consumer $1 never logged '$2'"
    sleep 1
    waited=$((waited + 1))
  done
}

# wait_for_exit PID: wait until the process has gone.
wait_for_exit() {
  waited=0
  while kill -0 "$1" 2>/dev/null; do
    [ "$waited" -lt "$timeout" ] || fail "process $1 did not stop"
    sleep 1
    waited=$((waited + 1))
  done
  wait "$1" 2>/dev/null || true
}

lines() {
  wc -l <"$1" | tr -d ' '
}

# wait_for_lines FILE WANT PID: wait until FILE has WANT lines, failing if the
# consumer stops or the timeout passes first.
wait_for_lines() {
  waited=0
  while [ "$(lines "$1")" -lt "$2" ]; do
    kill -0 "$3" 2>/dev/null || fail "the consumer stopped with $(lines "$1") of $2 items done"
    [ "$waited" -lt "$timeout" ] || fail "$(lines "$1") of $2 items done after $timeout s"
    sleep 1
    waited=$((waited + 1))
  done
}

# check_file FILE PUBLISHED: N lines, N distinct ids, none twice, and the ids
# that were published.
check_file() {
  got=$(lines "$1")
  [ "$got" -eq "$items" ] || fail "the file has $got lines, not $items"
  distinct=$(sort -u "$1" | wc -l | tr -d ' ')
  [ "$distinct" -eq "$items" ] || fail "the file has $distinct distinct ids, not $items"
  twice=$(sort "$1" | uniq -d)
  [ -z "$twice" ] || fail "done twice: $twice"
  sort "$2" >"$work/published.sorted"
  sort "$1" >"$work/done.sorted"
  lost=$(comm -23 "$work/published.sorted" "$work/done.sorted")
  [ -z "$lost" ] || fail "published and never done: $lost"
  stray=$(comm -13 "$work/published.sorted" "$work/done.sorted")
  [ -z "$stray" ] || fail "done and never published: $stray"
  say "the file has $got lines, $distinct distinct ids, none twice, all published"
}

# read_facts TENANT FILE: every envelope of facts.work, a page at a time.
read_facts() {
  : >"$2"
  offset=0
  while :; do
    page=$(mansio poll --tenant "$1" --topic facts.work --offset "$offset") ||
      fail "poll of facts.work failed for $1"
    if [ -z "$page" ]; then
      break
    fi
    printf '%s\n' "$page" >>"$2"
    offset=$((offset + $(printf '%s\n' "$page" | wc -l)))
  done
}

# sequences FILE: one line for each item, "ITEM event:attempt event:attempt ...",
# the transitions of the item in the order facts.work holds them.
sequences() {
  jq -rs 'group_by(.itemid)
    | map("\(.[0].itemid) " + (map("\(.type | ltrimstr("mansio.work.")):\(.attempt)") | join(" ")))
    | .[]' "$1"
}

# check_untouched FILE HELD: there are N items in facts.work, and all but HELD
# (which may be empty) were claimed and acknowledged once, as attempt 1, and
# nothing else happened to them.
check_untouched() {
  count=$(sequences "$1" | wc -l | tr -d ' ')
  [ "$count" -eq "$items" ] || fail "facts.work has $count items, not $items"
  if [ -n "$2" ]; then
    others=$(sequences "$1" | grep -v -x -- "$2 .*" | grep -v -x -- '[^ ]* claimed:1 acked:1' || true)
  else
    others=$(sequences "$1" | grep -v -x -- '[^ ]* claimed:1 acked:1' || true)
  fi
  [ -z "$others" ] || fail "items that were not claimed and acknowledged once: $others"
}

# ---- scenario 1 -------------------------------------------------------------

say "== scenario 1: a consumer is killed while idle, work arrives, a new process takes it"
tenant=offline-one-$run
name=offline-$run-a
done_file=$work/one.done
: >"$done_file"
mint "$tenant"
start_consumer "$tenant" "$name" "$done_file" 0
wait_for_log "$name" pulling
kill_dead "$name" "$consumer_pid"
say "the consumer was killed with kill -9"
publish_items "$tenant" "$work/one.published"
if [ -s "$done_file" ]; then
  fail "something was done while the consumer was dead"
fi
start_consumer "$tenant" "$name" "$done_file" 0
pid=$consumer_pid
say "a new process of the consumer started"
wait_for_lines "$done_file" "$items" "$pid"
sleep "$settle"
check_file "$done_file" "$work/one.published"
read_facts "$tenant" "$work/one.facts"
check_untouched "$work/one.facts" ""
say "facts.work: each item claimed and acknowledged once, as attempt 1"
kill_dead "$name" "$pid"
say "PASS scenario 1"

# ---- scenario 2 -------------------------------------------------------------

say "== scenario 2: a consumer dies holding a lease, and the item is redelivered once after it"
tenant=offline-two-$run
dying=offline-$run-b
taking=offline-$run-c
done_file=$work/two.done
: >"$done_file"
mint "$tenant"
publish_items "$tenant" "$work/two.published"
start_consumer "$tenant" "$dying" "$done_file" 1
pid=$consumer_pid
wait_for_exit "$pid"
held=$(sed -n 's/^claimed \([^ ]*\) attempt 1$/\1/p' "$work/$dying.log")
case $held in
  '' | *[!A-Za-z0-9._@:+-]*) fail "consumer $dying did not log exactly one claim, but: '$held'" ;;
esac
gone "$dying"
if [ -s "$done_file" ]; then
  fail "the consumer that died acknowledged something"
fi
say "consumer $dying died holding item $held"
start_consumer "$tenant" "$taking" "$done_file" 0
pid=$consumer_pid
say "consumer $taking started; waiting past the lease of $lease s"
wait_for_lines "$done_file" "$items" "$pid"
sleep "$settle"
check_file "$done_file" "$work/two.published"
read_facts "$tenant" "$work/two.facts"
check_untouched "$work/two.facts" "$held"
sequences "$work/two.facts" >"$work/two.sequences"
want="$held claimed:1 expired:1 retried:2 claimed:2 acked:2"
got=$(grep -x -- "$held .*" "$work/two.sequences" || true)
[ "$got" = "$want" ] || fail "the held item went '$got', not '$want'"
say "facts.work: $got"
say "the others were claimed and acknowledged once, as attempt 1"
kill_dead "$taking" "$pid"
say "PASS scenario 2"

# ---- scenario 3 -------------------------------------------------------------

say "== scenario 3: a consumer dies holding a lease and starts again under its name while the lease is live"
tenant=offline-three-$run
name=offline-$run-d
done_file=$work/three.done
: >"$done_file"
mint "$tenant"
publish_items "$tenant" "$work/three.published"
start_consumer "$tenant" "$name" "$done_file" 1
pid=$consumer_pid
wait_for_exit "$pid"
held=$(sed -n 's/^claimed \([^ ]*\) attempt 1$/\1/p' "$work/$name.log")
case $held in
  '' | *[!A-Za-z0-9._@:+-]*) fail "consumer $name did not log exactly one claim, but: '$held'" ;;
esac
gone "$name"
if [ -s "$done_file" ]; then
  fail "the consumer that died acknowledged something"
fi
say "consumer $name died holding item $held"
start_consumer "$tenant" "$name" "$done_file" 0
pid=$consumer_pid
say "a new process of $name started, inside the lease of $lease s"
wait_for_lines "$done_file" "$items" "$pid"
sleep "$settle"
# The restarted consumer was given the item it held, not another.
again=$(grep -c -x -- "claimed $held attempt 1" "$work/$name.log" || true)
[ "$again" -eq 2 ] || fail "item $held was claimed $again times under $name, not by the dead process and again by the new one"
check_file "$done_file" "$work/three.published"
read_facts "$tenant" "$work/three.facts"
check_untouched "$work/three.facts" ""
say "facts.work: each item, $held among them, claimed and acknowledged once, as attempt 1, with no expiry"
kill_dead "$name" "$pid"
say "PASS scenario 3"

say "PASS: $items items in each scenario, none dropped, none done twice"
