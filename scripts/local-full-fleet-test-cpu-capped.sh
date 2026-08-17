#!/bin/sh
# CPU-capped local repro for the one-process multi-mirror upstream resets
# (MULTI-MIRROR-UPSTREAM-RESETS.md): the same full-fleet run as
# local-full-fleet-test.sh, but inside a Linux Docker container capped with
# --cpus=N so aggregate CPU starvation can be reproduced on a fast dev
# machine. Tests the worker-starvation mechanism: on a saturated runtime the
# session tasks stop being polled in time and the 10s client-ping / ~30s
# upstream-pong / 30s probe deadlines start to slip, producing the exact
# death modes seen in production.
#
# Scope note: a CFS quota emulates the host's aggregate CPU shortage, not its
# 5x-slower individual cores. That is deliberate — the mechanism under test
# is starvation-induced poll gaps (see the `event loop gap` warnings emitted
# by the instrumented build), which depend on total capacity, not per-core
# speed alone.
#
# Usage:
#   scripts/local-full-fleet-test-cpu-capped.sh build          build (Linux, cached)
#   scripts/local-full-fleet-test-cpu-capped.sh start [CPUS]   run capped (default 4)
#   scripts/local-full-fleet-test-cpu-capped.sh status         mirrors + container stats
#   scripts/local-full-fleet-test-cpu-capped.sh death-modes    count the three death
#                                                              modes + starvation signals
#   scripts/local-full-fleet-test-cpu-capped.sh watch          sample until soak elapses
#   scripts/local-full-fleet-test-cpu-capped.sh stop           remove the container
#
# Knobs (environment, all optional — defaults match local-full-fleet-test.sh):
#   REGIONS, UPSTREAM_BASE, CONCURRENCY, SOAK_MINUTES, TOKEN_FILE, JWT_KEY_DIR,
#   CACHE_MEM_CEILING_BYTES   as in local-full-fleet-test.sh
#   CPUS                      CPU quota for `start` (default 1). Calibrated
#                             2026-08-17 with `openssl speed -evp sha256`
#                             (8192-byte blocks): the whole production host
#                             (Xeon E3-1270 v6, 4C/8T) aggregates to 1414k/s
#                             vs 2328k/s for ONE M2 Max quota unit — i.e.
#                             the entire host ≈ 0.6 quota units on sha256,
#                             and ≈ 1–1.3 on branchy integer work (per-core
#                             gap ~6.4x sha256, est. 2.5–4x integer). So:
#                             --cpus=1 is the host-equivalent treatment arm;
#                             2–4 are above-host controls; 0.6 is the strict
#                             sha256 match. Re-calibrate against a different
#                             dev machine before reusing these numbers.
#   CONTAINER_NAME            default bitcraft-mirror-capped
#   IMAGE                     default rust:1.93.0 (matches rust-toolchain.toml)
#   LISTEN / STATUS_ADDR / CACHE_BIND  binds INSIDE the container (default
#                             0.0.0.0:3100 / 0.0.0.0:3130 / 0.0.0.0:8089 —
#                             must not be loopback or the published ports
#                             cannot reach them); published to 127.0.0.1 on
#                             the same host ports

set -eu

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORKSPACE_ROOT=$(cd "$REPO_ROOT/.." && pwd)

# relay-cache's path dependencies (relay-protocol, relay-coordinator) resolve
# against the sibling checkout; both repos are mounted at the same relative
# positions as on the host so `../../spacetimedb-relay/...` keeps working.
BITCRAFT_DIR_IN=/work/spacetimedb-bitcraft-mirror
RELAY_DIR_IN=/work/spacetimedb-relay

UPSTREAM_BASE="${UPSTREAM_BASE:-wss://bitcraft-early-access.spacetimedb.com}"
LISTEN="${LISTEN:-0.0.0.0:3100}"
STATUS_ADDR="${STATUS_ADDR:-0.0.0.0:3130}"
CACHE_BIND="${CACHE_BIND:-0.0.0.0:8089}"
CONCURRENCY="${CONCURRENCY:-1}"
SOAK_MINUTES="${SOAK_MINUTES:-60}"
CPUS="${CPUS:-1}"
CACHE_MEM_CEILING_BYTES="${CACHE_MEM_CEILING_BYTES:-51539607552}"
RUN_DIR="${RUN_DIR:-/tmp/bitcraft-full-fleet-capped}"
JWT_KEY_DIR="${JWT_KEY_DIR:-$HOME/.config/spacetime}"
REGIONS="${REGIONS:-global 3 7 8 9 11 12 13 14 15 17 18 19 23}"
TOKEN_FILE="${TOKEN_FILE:-$WORKSPACE_ROOT/.developer-token}"
[ -f "$TOKEN_FILE" ] || TOKEN_FILE="$WORKSPACE_ROOT/spacetimedb-relay/.upstream-token"
CONTAINER_NAME="${CONTAINER_NAME:-bitcraft-mirror-capped}"
IMAGE="${IMAGE:-rust:1.93.0}"
TARGET_VOLUME="${TARGET_VOLUME:-bitcraft-mirror-target}"
CARGO_VOLUME="${CARGO_VOLUME:-bitcraft-mirror-cargo}"

LOG="$RUN_DIR/container.log"

[ -f "$TOKEN_FILE" ] || {
    echo "no token file (tried \$TOKEN_FILE, $WORKSPACE_ROOT/.developer-token," >&2
    echo "  $WORKSPACE_ROOT/spacetimedb-relay/.upstream-token)" >&2
    exit 1
}
[ -f "$JWT_KEY_DIR/id_ecdsa" ] && [ -f "$JWT_KEY_DIR/id_ecdsa.pub" ] || {
    echo "missing id_ecdsa keypair in $JWT_KEY_DIR (set JWT_KEY_DIR)" >&2
    exit 1
}

database_for() {
    case "$1" in
        global) echo "bitcraft-live-global" ;;
        bitcraft-live-*) echo "$1" ;;
        *) echo "bitcraft-live-$1" ;;
    esac
}

container_running() {
    [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null || echo false)" = true ]
}

mounts() {
    echo "--mount type=bind,src=$REPO_ROOT,dst=$BITCRAFT_DIR_IN,readonly"
    echo "--mount type=bind,src=$WORKSPACE_ROOT/spacetimedb-relay,dst=$RELAY_DIR_IN,readonly"
    echo "--mount type=volume,src=$TARGET_VOLUME,dst=/target"
    echo "--mount type=volume,src=$CARGO_VOLUME,dst=/usr/local/cargo"
}

cmd_build() {
    # protobuf-compiler: relay-cache's build script needs protoc, which the
    # rust image does not ship. Installed per-invocation (~seconds); compiled
    # deps stay cached in the target volume.
    docker run --rm \
        $(mounts) \
        -e CARGO_TARGET_DIR=/target \
        -e CARGO_INCREMENTAL=0 \
        -w "$BITCRAFT_DIR_IN" \
        "$IMAGE" \
        bash -c 'apt-get update -qq >/dev/null && apt-get install -y -qq protobuf-compiler >/dev/null &&
            cargo build --release -p spacetimedb-standalone --locked'
    echo "binary cached in docker volume $TARGET_VOLUME (target/release/spacetimedb-standalone)"
}

mirror_args() {
    set -- start \
        --data-dir /data \
        --listen-addr "$LISTEN" \
        --mirror-status-listen-addr "$STATUS_ADDR" \
        --jwt-key-dir /keys \
        --public-mirror-v1 \
        --non-interactive \
        --mirror-token-file /token \
        --mirror-subscribe-concurrency "$CONCURRENCY" \
        --bitcraft-cache \
        --cache-bind "$CACHE_BIND" \
        --cache-mem-ceiling-bytes "$CACHE_MEM_CEILING_BYTES"
    for region in $REGIONS; do
        set -- "$@" --mirror "$UPSTREAM_BASE/$(database_for "$region")"
    done
    echo "$@"
}

cmd_start() {
    [ "${1:-}" ] && CPUS="$1"
    container_running && {
        echo "container $CONTAINER_NAME already running; '$0 stop' first" >&2
        exit 1
    }
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    mkdir -p "$RUN_DIR"
    # shellcheck disable=SC2046
    docker run -d \
        --name "$CONTAINER_NAME" \
        --cpus "$CPUS" \
        --publish 127.0.0.1:3100:3100 --publish 127.0.0.1:3130:3130 --publish 127.0.0.1:8089:8089 \
        $(mounts) \
        --mount type=bind,src="$TOKEN_FILE",dst=/token,readonly \
        --mount type=bind,src="$JWT_KEY_DIR",dst=/keys,readonly \
        -e CARGO_TARGET_DIR=/target \
        -e RUST_LOG="${RUST_LOG:-spacetimedb=info,public_mirror=info,relay_cache=info}" \
        -w "$BITCRAFT_DIR_IN" \
        "$IMAGE" \
        /target/release/spacetimedb-standalone $(mirror_args) \
        >/dev/null
    echo "container $CONTAINER_NAME running, capped at $CPUS CPUs; mirroring: $REGIONS"
    echo "container log: $LOG  (populate with: $0 log >$LOG &  or docker logs -f)"
    echo "next: $0 death-modes   and   $0 watch"
}

cmd_log() {
    docker logs -f "$CONTAINER_NAME"
}

cmd_status() {
    container_running || echo "(container not running)"
    docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' "$CONTAINER_NAME" 2>/dev/null || true
    echo "--- mirrors (http://127.0.0.1:3130/v1/mirrors)"
    curl -fsS "http://127.0.0.1:3130/v1/mirrors" 2>/dev/null | python3 -m json.tool \
        || echo "(sidecar not answering)"
    echo "--- cache (http://127.0.0.1:8089/cache-health)"
    curl -fsS "http://127.0.0.1:8089/cache-health" 2>/dev/null | python3 -m json.tool \
        || echo "(cache not answering)"
}

# The repro's success criterion: any of the three production death modes,
# plus the two starvation signals from the instrumented build.
cmd_death_modes() {
    container_running || { echo "container not running; '$0 start' first" >&2; exit 1; }
    mkdir -p "$RUN_DIR"
    docker logs "$CONTAINER_NAME" >"$RUN_DIR/container-snapshot.log" 2>&1 || true
    echo "--- death modes ---"
    grep -c "Connection reset without closing handshake" "$RUN_DIR/container-snapshot.log" |
        sed 's/^/  connection-reset-without-close-handshake: /'
    grep -c "liveness probe timed out" "$RUN_DIR/container-snapshot.log" |
        sed 's/^/  liveness-probe-timeout: /'
    grep -c "unexpected EOF" "$RUN_DIR/container-snapshot.log" |
        sed 's/^/  unexpected-eof: /'
    echo "--- starvation signals (instrumented build) ---"
    grep -c "event loop gap" "$RUN_DIR/container-snapshot.log" |
        sed 's/^/  event-loop-gap-warns: /'
    grep -c "socket bytes arrived since probe send" "$RUN_DIR/container-snapshot.log" |
        sed 's/^/  probe-false-timeouts (bytes arrived): /'
    grep -c "socket silent since probe send" "$RUN_DIR/container-snapshot.log" |
        sed 's/^/  probe-timeouts (socket silent): /'
    echo "--- last 10 session lifecycle lines ---"
    grep -E "upstream error|exited cleanly|event loop gap|liveness probe timed out" \
        "$RUN_DIR/container-snapshot.log" | tail -10
}

cmd_watch() {
    container_running || { echo "container not running; '$0 start' first" >&2; exit 1; }
    mkdir -p "$RUN_DIR"
    deadline=$(( $(date +%s) + SOAK_MINUTES * 60 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        ts=$(date -u +%H:%M:%S)
        live=$(curl -fsS "http://127.0.0.1:3130/v1/mirrors" 2>/dev/null |
            python3 -c 'import json,sys; ms=json.load(sys.stdin)["mirrors"]; print(sum(1 for m in ms if m.get("connectivity")=="live"), "/", len(ms))' 2>/dev/null ||
            echo "?/?")
        cpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$CONTAINER_NAME" 2>/dev/null || echo "?")
        echo "$ts live=$live container-cpu=$cpu"
        sleep 30
    done
    echo "soak elapsed; run '$0 death-modes' for the verdict"
}

cmd_stop() {
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 && echo "removed $CONTAINER_NAME" || echo "no container"
    echo "logs kept in $RUN_DIR"
}

case "${1:-}" in
    build) cmd_build ;;
    start) cmd_start "${2:-}" ;;
    status) cmd_status ;;
    log) cmd_log ;;
    death-modes) cmd_death_modes ;;
    watch) cmd_watch ;;
    stop) cmd_stop ;;
    *)
        echo "usage: $0 build|start [CPUS]|status|log|death-modes|watch|stop" >&2
        exit 2
        ;;
esac
