#!/bin/sh
# A consumer of mansio work for a program that is not always running. Copy it
# into any harness; it needs `mansio` (see MANSIO_BIN) and `jq`.
#
#   MANSIO_TOKEN=<credential> sh consumer.sh TENANT QUEUE CONSUMER OUTFILE
#
# It loops `mansio work pull --wait`. For each item it claims it runs
# `mansio work ack`, and only if that succeeds appends the item id to OUTFILE.
#
# The append comes after the ack on purpose. The file counts acknowledgements,
# and an item is acknowledged once, so the file can never hold an item twice,
# which is what the offline test needs. The price is a lost effect: if the
# consumer dies between the ack and the append, or the answer to the ack is
# lost (the CLI does not send an ack again), the server has the item as done
# and the file does not hold it. An item the consumer held when it died before
# the ack is not acknowledged and not appended, and is redelivered to someone
# once its lease ends.
#
# A real worker does it the other way round: its effect first, idempotent on
# the item's `itemid` (the attempt is in the item, as `attempt`), and then the
# ack. A death between the two is a redelivery, and the idempotent effect
# absorbs it.
#
# The credential is read from MANSIO_TOKEN and never written anywhere. A
# progress line goes to stderr for each step; the test reads them.
#
# Settings, all optional:
#   MANSIO_URL                         the server (the CLI's default is 127.0.0.1:7070)
#   MANSIO_BIN                         the mansio command, default mansio
#   MANSIO_OFFLINE_PULL_WAIT_SECONDS   how long one pull looks for an item, default 5
#   MANSIO_OFFLINE_RETRY_SECONDS       the wait after a pull the server refused or
#                                      could not be reached for, default 1
#   MANSIO_OFFLINE_STOP_AFTER_CLAIM    1 makes the consumer kill -9 itself right
#                                      after its first claim, holding the lease
#                                      and acknowledging nothing, default 0

set -eu

if [ "$#" -ne 4 ]; then
  echo "usage: MANSIO_TOKEN=<credential> consumer.sh TENANT QUEUE CONSUMER OUTFILE" >&2
  exit 2
fi
tenant=$1
queue=$2
consumer=$3
out=$4

: "${MANSIO_TOKEN:?MANSIO_TOKEN is the credential this consumer presents}"
bin=${MANSIO_BIN:-mansio}
wait_seconds=${MANSIO_OFFLINE_PULL_WAIT_SECONDS:-5}
retry_seconds=${MANSIO_OFFLINE_RETRY_SECONDS:-1}
stop_after_claim=${MANSIO_OFFLINE_STOP_AFTER_CLAIM:-0}

log() {
  printf '%s\n' "$*" >&2
}

log "started $consumer"
while :; do
  log "pulling"
  if ! claim=$("$bin" work pull --tenant "$tenant" --queue "$queue" \
    --consumer "$consumer" --wait "$wait_seconds"); then
    log "pull failed, trying again in $retry_seconds s"
    sleep "$retry_seconds"
    continue
  fi
  # Nothing could be claimed within the wait.
  if [ -z "$claim" ]; then
    continue
  fi

  lease=$(printf '%s\n' "$claim" | jq -er .lease_id)
  item=$(printf '%s\n' "$claim" | jq -er .item.itemid)
  attempt=$(printf '%s\n' "$claim" | jq -er .item.attempt)
  expires_at=$(printf '%s\n' "$claim" | jq -er .expires_at)
  log "claimed $item attempt $attempt expires_at $expires_at"

  if [ "$stop_after_claim" = 1 ]; then
    kill -9 "$$"
    exit 1
  fi

  if "$bin" work ack --tenant "$tenant" --queue "$queue" --item "$item" \
    --consumer "$consumer" --lease-id "$lease" >/dev/null; then
    printf '%s\n' "$item" >>"$out"
    log "done $item"
  else
    log "ack of $item failed: if the server recorded it, $item is done and not in the file"
  fi
done
