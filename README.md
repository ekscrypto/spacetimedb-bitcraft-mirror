# spacetimedb-bitcraft-mirror

A custom fork of **SpacetimeDB built specifically for mirroring BitCraft game
servers** — all `bitcraft-live-*` region databases in **one process**, with a
read-cache API for game tools embedded in that same process.

This is not a generic SpacetimeDB deployment. It is a purpose-built mirroring
appliance: point it at the BitCraft upstream, and it gives your community a
low-latency, self-hosted copy of the live game data — both as the native
SpacetimeDB WebSocket protocol and as plain HTTP/protobuf endpoints for
claims, players, deposits and more.

> **Not affiliated.** BitCraft and SpacetimeDB are products of Clockwork
> Labs, Inc. This is an unofficial community project operating a public
> mirror; the upstream data and schema remain theirs. The reference
> deployment serves [relay.bitcraftsync.app](https://relay.bitcraftsync.app).

## Fork lineage

```
clockworklabs/SpacetimeDB              official repository, pinned at v2.7.1
        │  (BSL 1.1)
        ▼
ekscrypto/spacetimedb-public-mirror    adds --public-mirror-v1: in-memory
        │                              mirroring of upstream v1 BSATN
        │                              databases, preserving reducer
        │                              call-stack provenance for subscribers
        ▼
ekscrypto/spacetimedb-bitcraft-mirror  ← you are here
                                       BitCraft tuning: 14-region production
                                       hardening, serialized seeding, async
                                       lossy logging, and relay-cache
                                       embedded behind --bitcraft-cache
```

- **Expect this fork to lag mainline SpacetimeDB.** The upstream pin is
  deliberate (v2.7.1 is what the BitCraft early-access edge speaks), and
  fixes are pulled in by cherry-pick from the parent fork rather than by
  tracking `main` — see the merge policy in
  [`BITCRAFT-FORK.md`](BITCRAFT-FORK.md). New upstream features may arrive
  late or never here; security-relevant fixes are prioritized.
- Both fork layers keep SpacetimeDB's Business Source License 1.1
  ([`LICENSE.txt`](LICENSE.txt)). `crates/relay-cache` is MIT.

## Topology

```
                 ┌──────────────────────────────────────────┐
                 │     BitCraft upstream (Clockwork Labs)   │
                 │     wss://bitcraft-early-access.         │
                 │          spacetimedb.com                 │
                 │                                          │
                 │  bitcraft-live-global, -3, -7, -8, -9,   │
                 │  -11, -12, -13, -14, -15, -17, -18,      │
                 │  -19, -23          (14 databases)        │
                 └────────────┬────────────────┬────────────┘
                              │ 14 × mirrored WebSocket sessions
                              │ (Brotli-compressed BSATN subscribe)
                              ▼
 ┌────────────────────────────────────────────────────────────────┐
 │  spacetimedb-standalone — this fork, ONE process               │
 │                                                                │
 │   mirror sessions ──► in-memory relational store               │
 │          │               (per-database, client-gated)          │
 │          │                                                     │
 │          └────────► relay-cache, embedded in-process           │
 │                     (--bitcraft-cache: fed the mirror's        │
 │                      already-decoded rows directly)            │
 │                                                                │
 │   loopback listeners:                                          │
 │     127.0.0.1:3100   WS/HTTP for downstream subscribers        │
 │     127.0.0.1:3130   /v1/mirrors status sidecar                │
 │     127.0.0.1:8090   cache HTTP API + dim-buildings WebSocket  │
 └──────────┬──────────────────────────────────┬──────────────────┘
            │                                  │
            ▼                                  ▼
 ┌────────────────────────────────────────────────────────────────┐
 │  nginx (TLS termination)                                       │
 │                                                                │
 │   wss://host:3000–3025/v1/database/<db>/subscribe → :3100      │
 │   https://host/claim /player /deposits …         → :8090       │
 └──────────┬─────────────────────────────────────────────────────┘
            │
            ▼
   downstream clients: explorer UIs, claim/player tools,
   dashboards, bots — native SpacetimeDB WS or plain HTTP
```

Any port in the 3000–3025 band serves **any** mirrored database — clients
select a region by database name, not by port.

## The read-cache endpoints (relay-cache)

SpacetimeDB's WebSocket protocol is great for game clients, awkward for a
quick "show me this claim's members" tool. The embedded cache keeps a
query-shaped index of the interesting tables and serves them over plain
HTTP (protobuf-encoded bodies; the `.proto` sources self-describe the API):

| Endpoint | Serves |
|---|---|
| `/cache-health` | readiness: `{ready, regions[]}` |
| `/claim?name=` · `/claim/:id` | claim lookup / detail |
| `/claim/:id/inventory` | claim storage contents |
| `/claim/:id/members` · `/citizens` | member list; citizens with skills + activity |
| `/claim/:id/hexcoins` | per-member hexcoin totals |
| `/claim/:id/crafts` | claim craft jobs |
| `/player?name=` · `/player/:id` | player lookup / detail |
| `/player/:id/inventory` · `/housing` · `/skills` · `/crafts` | per-player data |
| `/deposits` | hexite deposit locations |
| `/storage-logs` | storage chest access logs |
| `/proto` · `/proto/:name` | protobuf schema sources |
| `/internal/dim-buildings/ws` | WebSocket push: housing-interior building entity IDs per (region, dimension) — see [`crates/relay-cache/DIM-BUILDINGS-WS.md`](crates/relay-cache/DIM-BUILDINGS-WS.md) |
| `/internal/stats` | loopback-only diagnostics (memory, row counts) — deliberately not proxied publicly |

Full endpoint documentation: [`crates/relay-cache/README.md`](crates/relay-cache/README.md).

### Why the cache runs inside the mirror process

Previously the cache was a **separate `relay-cache` process** that opened its
own WebSocket subscriptions against the mirrors. That meant, for every
region: a mirror→cache WebSocket hop, a BSATN **re-encode** for the
fan-out and a **re-decode** on ingest, and a **second full subscription
evaluation** — every row was decoded twice and serialized over a socket in
between.

With `--bitcraft-cache`, the mirror's already-decoded upstream batches feed
the cache stores **in-process**: each row is decoded once, nothing crosses a
socket, and one process replaces N mirrors + 1 cache. Co-locating them also
let the retention rules move to where the rows arrive (for example, keeping
`location_state` rows for tracked hexite deposits directly at ingest), which
removed the standalone cache's second per-deposit subscription set and its
`HexiteLocationsMissing` reconnect failure class entirely.

The standalone `relay-cache` binary still builds from `crates/relay-cache`
(WebSocket mode, unchanged) — it remains the rollback/legacy deployment
shape.

## Quick start

```sh
cargo build -p spacetimedb-standalone --release

./target/release/spacetimedb-standalone start \
  --data-dir /tmp/bitcraft-mirror-data \
  --listen-addr 127.0.0.1:3000 \
  --jwt-pub-key-path /path/to/id_ecdsa.pub \
  --jwt-priv-key-path /path/to/id_ecdsa \
  --public-mirror-v1 \
  --mirror-token-file /path/to/.developer-token \
  --mirror wss://bitcraft-early-access.spacetimedb.com/bitcraft-live-7 \
  --mirror wss://bitcraft-early-access.spacetimedb.com/bitcraft-live-8 \
  --bitcraft-cache \
  --cache-bind 127.0.0.1:8089
```

Add one `--mirror` per region database you want (production runs all 14).

**Before deploying to real hardware**, read
[`OVH-CLOUD-K5.md`](OVH-CLOUD-K5.md): the full 14-region mirror is
memory-heavy (~36 GiB RSS) and extremely sensitive to storage stalls and
mis-sized cgroup limits. That guide lists every host-level change
(async-lossy logging already in this fork, vmtouch page pinning, nginx log
buffering, swap removal, `MemoryHigh` sizing) that production required —
with the diagnostics that catch each failure mode.

## Mirroring mode reference

Enable with `spacetimedb-standalone start --public-mirror-v1` plus one or
more `--mirror` targets. Key flags (all require `--public-mirror-v1`):

| Flag | Role |
|------|------|
| `--mirror <url>/<db>` | Upstream to mirror (repeatable). Local clients select by database name. |
| `--mirror-token` / `--mirror-token-file` | Upstream bearer JWT (also `BITCRAFT_TOKEN`, `MIRROR_TOKEN`, `RELAY_UPSTREAM_TOKEN`, `MIRROR_TOKEN_FILE`). Shared across all mirrors. |
| `--mirror-table <name>` | Limit upstream subscribe set (repeatable; default: all public user tables). Shared across all mirrors. |
| `--mirror-subscribe-concurrency <n>` | Max mirrors that may run initial setup at once (default **1**; production keeps it at 1 — serialized seeding is a stability requirement, not a suggestion, on slower hosts). Slot held for connect + every table seed + local apply; released when that mirror goes `live`. |
| `--mirror-status-listen-addr <addr>` | Isolated readiness listener (default `127.0.0.1:<main-port+1>`). |
| `--reject-one-off-query` | Also reject `OneOffQuery` (allowed by default). `CallReducer` / `CallProcedure` are always rejected. |
| `--bitcraft-cache` | Embed the relay-cache (this fork). Adds `--cache-bind` and `--cache-mem-ceiling-bytes`. |

Status and per-table seed progress: `GET /v1/mirrors` (main port and the
status sidecar).

**Clients stay offline until every mirror is live.** Downstream WebSocket
subscribe is rejected with HTTP 503 until **every** configured mirror
reports `live`. This is intentional — clients must never see a half-seeded
database or miss updates. A later disconnect on any mirror re-closes the
gate until that mirror is `live` again. Poll the **status sidecar** during
large seeds; the main HTTP port may not respond until seed apply finishes.

Provisioning is nothing like a normal standalone: the schema is fetched
from upstream at bootstrap, storage is forced in-memory (no durable mirror
state across restarts), local database names/identities are derived from
upstream, and the surface is read-only fan-out (`CallReducer` /
`CallProcedure` rejected). You never `spacetime publish` into this process.
Details, seed/reconnect behavior, and the coordinator socket:
[`FORK.md`](FORK.md) and [`BITCRAFT-FORK.md`](BITCRAFT-FORK.md).

## Documentation

- [`BITCRAFT-FORK.md`](BITCRAFT-FORK.md) — this fork's design decisions,
  merge policy vs the parent fork, and what `--bitcraft-cache` adds
- [`FORK.md`](FORK.md) — the parent fork's `--public-mirror-v1` mechanics
- [`MULTI-MIRROR-UPSTREAM-RESETS.md`](MULTI-MIRROR-UPSTREAM-RESETS.md) and
  [`MULTI-MIRROR-STARVATION.md`](MULTI-MIRROR-STARVATION.md) — the full
  production incident post-mortems (why the hardening exists)
- [`OVH-CLOUD-K5.md`](OVH-CLOUD-K5.md) — operator guide: running the
  mirror on spinning-disk/legacy hardware
- [`crates/relay-cache/README.md`](crates/relay-cache/README.md) — cache
  API reference
- Upstream's own README: [clockworklabs/SpacetimeDB](https://github.com/clockworklabs/SpacetimeDB)

## License

SpacetimeDB source in this repository is licensed under the **Business
Source License 1.1** ([`LICENSE.txt`](LICENSE.txt)), © Clockwork Labs, Inc.
`crates/relay-cache` is MIT. Both notices must stay conspicuous.
