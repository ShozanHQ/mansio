#!/bin/sh
# A consumer of mansio work for a program that is not always running. Copy it
# into any harness; it needs `mansio` (see MANSIO_BIN) and `jq`.
#
#   MANSIO_TOKEN=<credential> sh consumer.sh TENANT QUEUE CONSUMER OUTFILE
#
# It loops `mansio work pull --wait`. For each item it claims it runs
# `mansio work ack`, and only if that succeeds appends the item id to OUTFILE,
# so the file holds one line for each item this consumer finished. An item is
# acknowledged once, so it is appended at most once. An item the consumer held
# when it died is not acknowledged and not appended, and is redelivered to
# someone once its lease ends.
#
# The credential is read from MANSIO_TOKEN and never written anywhere. A
# progress line goes to stderr for each step.
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
  log "claimed $item attempt $attempt"

  if [ "$stop_after_claim" = 1 ]; then
    kill -9 "$$"
    exit 1
  fi

  if "$bin" work ack --tenant "$tenant" --queue "$queue" --item "$item" \
    --consumer "$consumer" --lease-id "$lease" >/dev/null; then
    printf '%s\n' "$item" >>"$out"
    log "done $item"
  else
    log "ack of $item refused, so it is not done here"
  fi
done
