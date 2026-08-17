#!/bin/sh
# Reproduce (or disprove) the 2026-08-17 attempt-#3 stop-the-world freeze
# locally: run the mirror with stdout piped through a trickle reader — an
# emulator of journald falling behind on a stalled host disk — and count
# `event loop gap` warnings in the data-dir rolling logs.
#
# Freeze mechanism under test (MULTI-MIRROR-UPSTREAM-RESETS.md): the fmt layer
# wrote every log line synchronously from the emitting task. stdout under
# systemd is a pipe to journald; when the host storage stalls the pipe fills
# and every Tokio task that logs blocks in write() — CPU drops to ~0, all
# sessions freeze, everyone releases together when the pipe drains. The
# rolling-file writer has the same shape (writeback throttling). The fix
# (startup.rs) moves both onto dedicated `tracing_appender::non_blocking`
# writer threads.
#
# Usage:
#   local-logging-stall-test.sh <binary> [region] [minutes] [drain-rate-kb]
#
#   binary   path to spacetimedb-standalone (pre- or post-fix for A/B)
#   region   BitCraft region number (default 12; any 274-table region floods
#            the pipe by itself: the pre-fix binary logs 2 INFO lines/table)
#   minutes  run length (default 4)
#   drain    pipe drain rate in KB/s (default 4 — a journald that has fallen
#            far behind, not a hard stop; use 0 for a fully stalled pipe)
#
# Verdict: freeze = `event loop gap` WARN lines in <data-dir>/logs/*.log
# (the file sink keeps writing on a healthy local disk, so gap warnings land
# there even while stdout is wedged). Post-fix expectation: zero gap
# warnings, mirror still reaches live and keeps serving /v1/mirrors.
#
# No Docker, no CPU cap needed — the failure mode is a blocked write, not CPU
# starvation. Native macOS or Linux.

set -eu

BINARY="${1:?usage: $0 <binary> [region] [minutes] [drain-kb-s]}"
REGION="${2:-12}"
MINUTES="${3:-4}"
DRAIN_KB_S="${4:-4}"

HERE="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "$HERE/../.." && pwd)"
TOKEN_FILE="$WORKSPACE_ROOT/.developer-token"
[ -f "$TOKEN_FILE" ] || TOKEN_FILE="$WORKSPACE_ROOT/spacetimedb-relay/.upstream-token"
[ -f "$TOKEN_FILE" ] || { echo "no upstream token file found" >&2; exit 1; }

JWT_KEY_DIR="${JWT_KEY_DIR:-$HOME/.config/spacetime}"
UPSTREAM="${UPSTREAM:-wss://bitcraft-early-access.spacetimedb.com}"
LISTEN="127.0.0.1:3190"
STATUS="127.0.0.1:3191"

DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/logging-stall-test.XXXXXX")"
STDOUT_CAPTURE="$DATA_DIR/stdout-through-trickle.txt"

echo "binary:    $BINARY"
echo "region:    bitcraft-live-$REGION  (upstream $UPSTREAM)"
echo "data dir:  $DATA_DIR"
echo "pipe:      stdout+stderr -> trickle reader at ${DRAIN_KB_S} KB/s"

# Trickle reader: drain (and discard) the pipe at a fixed rate so it stays
# full whenever the mirror logs faster than that — the journald-stall shape.
# This is a rate-limited /dev/null; the capture file stays empty by design.
# Rate 0 = never read after the first chunk (fully wedged pipe). NB: the
# script must come from `-c`, not a heredoc — a heredoc would steal this
# process's own stdin (the pipe under test) and kill the reader at birth.
trickle() {
    python3 -u -c '
import sys, time
rate_kb = int(sys.argv[1])
buf = sys.stdin.buffer
while True:
    d = buf.read(4096)
    if not d:
        break
    if rate_kb == 0:
        time.sleep(60)  # wedged: effectively never drain
    else:
        time.sleep(max(4096 / (rate_kb * 1024), 0.01))
' "$DRAIN_KB_S"
}

"$BINARY" start \
    --data-dir "$DATA_DIR" \
    --listen-addr "$LISTEN" \
    --mirror-status-listen-addr "$STATUS" \
    --jwt-key-dir "$JWT_KEY_DIR" \
    --public-mirror-v1 \
    --non-interactive \
    --mirror-token-file "$TOKEN_FILE" \
    --mirror-subscribe-concurrency 1 \
    --mirror "$UPSTREAM/bitcraft-live-$REGION" \
    2>&1 | trickle >"$STDOUT_CAPTURE" &
MIRROR_PID=$!
TRICKLE_PID=$(jobs -p | tail -1)

cleanup() {
    kill "$MIRROR_PID" "$TRICKLE_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Compact one-line summary of /v1/mirrors.
mirror_summary() {
    curl -fsS --max-time 3 "http://$STATUS/v1/mirrors" 2>/dev/null \
        | python3 -c 'import json,sys
try:
    ms = json.load(sys.stdin).get("mirrors", [])
    print(" ".join("%s:%s tables %s/%s" % (m["database"], m["connectivity"], m.get("tables_live", "?"), m.get("tables_total", "?")) for m in ms) or "empty")
except Exception:
    print("status-unreachable")' || echo status-unreachable
}

echo "running for ${MINUTES}m — sampling /v1/mirrors every 20s..."
I=0
SAW_MIRROR=0
while [ "$I" -lt $((MINUTES * 3)) ]; do
    sleep 20
    SUMMARY="$(mirror_summary)"
    case "$SUMMARY" in
        status-unreachable) ;;
        *) SAW_MIRROR=1 ;;
    esac
    echo "  [$(( (I + 1) * 20 ))s] $SUMMARY"
    I=$((I + 1))
done

echo
echo "== verdict =="
if [ "$SAW_MIRROR" -ne 1 ]; then
    echo "HARNESS FAILURE: /v1/mirrors never answered — the mirror did not run;"
    echo "no conclusion about logging stalls can be drawn from this attempt."
    exit 2
fi
GAPS=$(grep -c "event loop gap" "$DATA_DIR"/logs/*.log 2>/dev/null || true)
echo "event-loop gap warnings in rolling logs: ${GAPS:-0}"
if [ "${GAPS:-0}" -gt 0 ]; then
    echo "FREEZE REPRODUCED: the runtime stalled behind a slow log sink."
    grep -h "event loop gap" "$DATA_DIR"/logs/*.log | sort -u | head -5
    exit 1
else
    echo "no runtime stalls: logging decoupled from the sink (or sink never filled)."
fi
