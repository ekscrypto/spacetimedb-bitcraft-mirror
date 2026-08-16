#!/bin/sh
# Local full-fleet test for the one-process bitcraft-mirror topology: a single
# spacetimedb-standalone with --public-mirror-v1 --bitcraft-cache mirroring the
# entire production database set, monitored end to end.
#
# Usage:
#   scripts/local-full-fleet-test.sh start    launch the mirror in the background
#   scripts/local-full-fleet-test.sh status   one-shot snapshot of mirrors/cache/RSS
#   scripts/local-full-fleet-test.sh watch    sample until every database has been
#                                             live plus SOAK_MINUTES of steady state
#   scripts/local-full-fleet-test.sh stop     stop the mirror, keep collected data
#
# Knobs (environment, all optional):
#   REGIONS          region ids / "global" / full db names (default: production set)
#   UPSTREAM_BASE    default wss://bitcraft-early-access.spacetimedb.com
#   LISTEN           mirror client listen (default 127.0.0.1:3100)
#   STATUS_ADDR      /v1/mirrors sidecar (default 127.0.0.1:3130 = main port + 30)
#   CACHE_BIND       embedded cache bind (default 127.0.0.1:8089)
#   CONCURRENCY      --mirror-subscribe-concurrency (default 1; each extra slot
#                    adds ~8 GB transient seed memory — see MULTI-MIRROR-STARVATION.md)
#   SOAK_MINUTES     steady-state soak after all-live (default 60)
#   TOKEN_FILE       upstream bearer token file
#   JWT_KEY_DIR      dir with id_ecdsa / id_ecdsa.pub (default ~/.config/spacetime)
#   CACHE_MEM_CEILING_BYTES  --cache-mem-ceiling-bytes (default: 48 GiB — the 8 GiB
#                    binary default flips /cache-health ready=false once whole-process
#                    RSS passes it, which the full fleet always does)
#   RSS_GUARD_MB     monitor kills the mirror above this RSS (default 49152 = 48 GiB;
#                    local stand-in for the production MemoryHigh unit + ram guard)
#   RUN_DIR          artifacts dir (default /tmp/bitcraft-full-fleet)
set -eu

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORKSPACE_ROOT=$(cd "$REPO_ROOT/.." && pwd)

UPSTREAM_BASE="${UPSTREAM_BASE:-wss://bitcraft-early-access.spacetimedb.com}"
LISTEN="${LISTEN:-127.0.0.1:3100}"
STATUS_ADDR="${STATUS_ADDR:-127.0.0.1:3130}"
CACHE_BIND="${CACHE_BIND:-127.0.0.1:8089}"
CONCURRENCY="${CONCURRENCY:-1}"
SOAK_MINUTES="${SOAK_MINUTES:-60}"
RSS_GUARD_MB="${RSS_GUARD_MB:-49152}"
CACHE_MEM_CEILING_BYTES="${CACHE_MEM_CEILING_BYTES:-51539607552}"
RUN_DIR="${RUN_DIR:-/tmp/bitcraft-full-fleet}"
JWT_KEY_DIR="${JWT_KEY_DIR:-$HOME/.config/spacetime}"
REGIONS="${REGIONS:-global 3 7 8 9 11 12 13 14 15 17 18 19 23}"
TOKEN_FILE="${TOKEN_FILE:-$WORKSPACE_ROOT/.developer-token}"
[ -f "$TOKEN_FILE" ] || TOKEN_FILE="$WORKSPACE_ROOT/spacetimedb-relay/.upstream-token"

BIN="$REPO_ROOT/target/release/spacetimedb-standalone"
PID_FILE="$RUN_DIR/mirror.pid"
LOG="$RUN_DIR/mirror.log"

database_for() {
    case "$1" in
        global) echo "bitcraft-live-global" ;;
        bitcraft-live-*) echo "$1" ;;
        *) echo "bitcraft-live-$1" ;;
    esac
}

mirror_pid() {
    [ -f "$PID_FILE" ] && cat "$PID_FILE" 2>/dev/null || true
}

mirror_alive() {
    pid=$(mirror_pid)
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

cmd_start() {
    [ -x "$BIN" ] || {
        echo "missing $BIN -- build it first:" >&2
        echo "  cd $REPO_ROOT && cargo build --release -p spacetimedb-standalone" >&2
        exit 1
    }
    [ -f "$TOKEN_FILE" ] || {
        echo "no token file (tried \$TOKEN_FILE, $WORKSPACE_ROOT/.developer-token," >&2
        echo "$WORKSPACE_ROOT/spacetimedb-relay/.upstream-token)" >&2
        exit 1
    }
    [ -f "$JWT_KEY_DIR/id_ecdsa" ] && [ -f "$JWT_KEY_DIR/id_ecdsa.pub" ] || {
        echo "missing id_ecdsa keypair in $JWT_KEY_DIR (set JWT_KEY_DIR)" >&2
        exit 1
    }
    if mirror_alive; then
        echo "mirror already running (pid $(mirror_pid)); run '$0 stop' first" >&2
        exit 1
    fi
    mkdir -p "$RUN_DIR/data"

    set -- "$BIN" start \
        --data-dir "$RUN_DIR/data" \
        --listen-addr "$LISTEN" \
        --mirror-status-listen-addr "$STATUS_ADDR" \
        --jwt-key-dir "$JWT_KEY_DIR" \
        --public-mirror-v1 \
        --non-interactive \
        --mirror-token-file "$TOKEN_FILE" \
        --mirror-subscribe-concurrency "$CONCURRENCY" \
        --bitcraft-cache \
        --cache-bind "$CACHE_BIND" \
        --cache-mem-ceiling-bytes "$CACHE_MEM_CEILING_BYTES"
    for region in $REGIONS; do
        set -- "$@" --mirror "$UPSTREAM_BASE/$(database_for "$region")"
    done

    echo "log: $LOG"
    nohup env RUST_LOG="${RUST_LOG:-spacetimedb=info,public_mirror=info,relay_cache=info}" \
        "$@" >>"$LOG" 2>&1 &
    echo $! >"$PID_FILE"
    sleep 2
    mirror_alive || {
        echo "mirror exited immediately -- tail of $LOG:" >&2
        tail -20 "$LOG" >&2
        exit 1
    }
    echo "mirror pid $(mirror_pid), mirroring: $REGIONS"
    echo "next: $0 watch   (or $0 status for a one-shot snapshot)"
}

cmd_status() {
    if mirror_alive; then
        ps -o pid=,rss=,cputime=,etime= -p "$(mirror_pid)"
    else
        echo "mirror not running"
    fi
    echo "--- mirrors (http://$STATUS_ADDR/v1/mirrors)"
    curl -fsS "http://$STATUS_ADDR/v1/mirrors" 2>/dev/null | python3 -m json.tool \
        || echo "(sidecar not answering)"
    echo "--- cache (http://$CACHE_BIND/cache-health)"
    curl -fsS "http://$CACHE_BIND/cache-health" 2>/dev/null | python3 -m json.tool \
        || echo "(cache not answering)"
}

cmd_watch() {
    mirror_alive || { echo "mirror not running; '$0 start' first" >&2; exit 1; }
    export REGIONS STATUS_ADDR CACHE_BIND SOAK_MINUTES RSS_GUARD_MB RUN_DIR
    exec python3 "$REPO_ROOT/scripts/local-full-fleet-watch.py"
}

cmd_stop() {
    pid=$(mirror_pid)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        echo "sent SIGTERM to $pid"
        i=0
        while kill -0 "$pid" 2>/dev/null && [ "$i" -lt 100 ]; do
            sleep 0.2
            i=$((i + 1))
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
            echo "forced SIGKILL"
        fi
    else
        echo "mirror not running"
    fi
    rm -f "$PID_FILE"
    echo "logs and monitor data kept in $RUN_DIR"
}

case "${1:-}" in
    start) cmd_start ;;
    status) cmd_status ;;
    watch) cmd_watch ;;
    stop) cmd_stop ;;
    *)
        echo "usage: $0 start|status|watch|stop" >&2
        exit 2
        ;;
esac
