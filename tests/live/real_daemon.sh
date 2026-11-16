#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/hermes-tui-live.XXXXXX")
pid=
cleanup() { if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

start_fixture() {
  name=$1
  rm -rf "$tmp/$name-ledger"
  rm -f "$tmp/$name.sock"
  cargo run --quiet --manifest-path "$root/Cargo.toml" -p hermes-tui-cli \
    --example real_daemon_fixture -- "$tmp/$name-ledger" "$tmp/$name.sock" \
    >"$tmp/$name.out" 2>"$tmp/$name.err" &
  pid=$!
  i=0
  while [ ! -S "$tmp/$name.sock" ]; do
    i=$((i+1))
    if [ "$i" -ge 100 ]; then cat "$tmp/$name.err" >&2; exit 1; fi
    sleep 0.05
  done
}

finish_fixture() { wait "$pid"; pid=; }

start_consumer_fixture() {
  name=$1
  rm -rf "$tmp/$name-ledger"
  rm -f "$tmp/$name.sock"
  cargo run --quiet --manifest-path "$root/Cargo.toml" -p hermes-tui-cli \
    --example consumer_daemon_fixture -- "$tmp/$name-ledger" "$tmp/$name.sock" \
    >"$tmp/$name.out" 2>"$tmp/$name.err" &
  pid=$!
  i=0
  while [ ! -S "$tmp/$name.sock" ]; do
    i=$((i+1))
    if [ "$i" -ge 100 ]; then cat "$tmp/$name.err" >&2; exit 1; fi
    sleep 0.05
  done
}

start_fixture observe
output=$(cargo run --quiet --manifest-path "$root/Cargo.toml" -p hermes-tui-cli -- \
  headless "$tmp/observe.sock" --stream 05050505050505050505050505050505 \
  --consumer 06060606060606060606060606060606)
finish_fixture
printf '%s\n' "$output" | grep '"complete": true' >/dev/null
printf '%s\n' "$output" | grep '"capability": "ledger.observe.v1"' >/dev/null
printf '%s\n' "$output" | grep '"streams": 1' >/dev/null
printf '%s\n' "$output" | grep '"status": "current"' >/dev/null
printf '%s\n' "$output" | grep '"retained_events": 1' >/dev/null
printf '%s\n' "$output" | grep '"configured_cursor": {' >/dev/null
printf '%s\n' "$output" | grep '"next_sequence": 1' >/dev/null
printf '%s\n' "$output" | grep '"backlog": 0' >/dev/null

start_fixture refused
if refused=$(cargo run --quiet --manifest-path "$root/Cargo.toml" -p hermes-tui-cli -- \
  disclose-events "$tmp/refused.sock" 05050505050505050505050505050505 0 1 \
  'disclose stream' 2>"$tmp/refused-client.err"); then
  echo 'FAIL: inexact Hermes disclosure confirmation was accepted' >&2
  exit 1
fi
finish_fixture
printf '%s' "$refused" | python3 -c '
import json, sys
value = json.load(sys.stdin)
assert not value["complete"] and value["status"] == "confirmation_refused"
assert value["required_confirmation"] == "DISCLOSE HERMES STREAM 05050505050505050505050505050505 FROM 0 LIMIT 1"
'

start_fixture disclose
disclosed=$(cargo run --quiet --manifest-path "$root/Cargo.toml" -p hermes-tui-cli -- \
  disclose-events "$tmp/disclose.sock" 05050505050505050505050505050505 0 1 \
  'DISCLOSE HERMES STREAM 05050505050505050505050505050505 FROM 0 LIMIT 1')
finish_fixture
printf '%s' "$disclosed" | python3 -c '
import json, sys
value = json.load(sys.stdin)
assert value["complete"] and value["capability"] == "ledger.events.disclose.v1"
assert value["start"] == 0 and value["limit"] == 1 and value["gap"] is None
assert value["retained_from"] == 0 and value["published_edge"] == 0
assert len(value["events"]) == 1
event = value["events"][0]
assert event["sequence"] == 0 and event["payload_hex"] == "0a"
'

start_consumer_fixture acknowledge-refused
if refused=$(cargo run --quiet --manifest-path "$root/Cargo.toml" -p hermes-tui-cli -- \
  acknowledge "$tmp/acknowledge-refused.sock" \
  06060606060606060606060606060606 05050505050505050505050505050505 1 1 \
  'acknowledge' 2>"$tmp/acknowledge-refused-client.err"); then
  echo 'FAIL: inexact Hermes acknowledgement confirmation was accepted' >&2
  exit 1
fi
finish_fixture
printf '%s' "$refused" | python3 -c '
import json, sys
value = json.load(sys.stdin)
assert not value["complete"] and value["status"] == "confirmation_refused"
assert value["required_confirmation"] == "ACKNOWLEDGE HERMES CONSUMER 06060606060606060606060606060606 STREAM 05050505050505050505050505050505 FROM 1 THROUGH 1"
'

start_consumer_fixture acknowledge
acknowledged=$(cargo run --quiet --manifest-path "$root/Cargo.toml" -p hermes-tui-cli -- \
  acknowledge "$tmp/acknowledge.sock" \
  06060606060606060606060606060606 05050505050505050505050505050505 1 1 \
  'ACKNOWLEDGE HERMES CONSUMER 06060606060606060606060606060606 STREAM 05050505050505050505050505050505 FROM 1 THROUGH 1')
finish_fixture
printf '%s' "$acknowledged" | python3 -c '
import json, sys
value = json.load(sys.stdin)
assert value["complete"] and value["capability"] == "ledger.acknowledge.v1"
assert value["status"] == "acknowledged"
assert value["previous_next_sequence"] == 1 and value["next_sequence"] == 2
'

echo 'PASS: real Hermes daemon -> observation, bounded disclosure and same-connection exact-confirmed acknowledgement'
