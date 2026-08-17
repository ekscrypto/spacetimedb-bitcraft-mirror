# spacetimedb-bitcraft-mirror

BitCraft-tuned fork of [`spacetimedb-public-mirror`](https://github.com/ekscrypto/spacetimedb-public-mirror)
(which is itself an unofficial fork of
[Clockwork Labs SpacetimeDB](https://github.com/clockworklabs/SpacetimeDB) pinned
at tag **v2.7.1**). Forked at `public-mirror-v1` commit
`8bc9e08f8` ("Gate client acceptance per database instead of all mirrors live")
— the commit that made multiple regions in a single process viable.

## Purpose

Mirror **all BitCraft regions in one process** and serve the read APIs that
today are served by the separate `relay-cache` process **from the same
process**, eliminating per region:

- the mirror → relay-cache WebSocket hop,
- the BSATN re-encode (subscription fan-out) / re-decode (cache ingest), and
- a second full subscription evaluation for the cache's subscriber.

`relay-cache` lives in this repo as `crates/relay-cache` and is embedded behind
`--bitcraft-cache`: the mirror's already-decoded upstream batches are handed to
the cache's ingestion path in-process (`RegionFeed`), so rows are decoded once
per row by the cache instead of serialized over a socket and re-decoded.
`bitcraft-relay` still builds the standalone `relay-cache` binary (WebSocket
mode) from this crate via a path dependency. Production cut over to the
one-process topology on 2026-08-17; it is the final shape.

## Target topology

One process, `--mirror` per region database, single `--listen-addr` for
WebSocket clients, embedded cache on `--cache-bind` (default `127.0.0.1:8089`
— same contract as the standalone `relay-cache`, so nginx and the explorer
keep working unchanged). See [`MULTI-MIRROR-STARVATION.md`](MULTI-MIRROR-STARVATION.md) (inherited) for why
per-database client gating matters in this topology, and
[`MULTI-MIRROR-UPSTREAM-RESETS.md`](MULTI-MIRROR-UPSTREAM-RESETS.md) (resolved
2026-08-17: all four failure layers were local — logging, disk, swap, memory
cap) for the incident history. Deploying on modest/legacy hardware (e.g. an
OVH K-5 with spinning disks)? Read [`OVH-CLOUD-K5.md`](OVH-CLOUD-K5.md)
first — it lists every host-level change production required.

## Quick start (mirror + embedded cache)

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

Everything from [`FORK.md`](FORK.md) applies unchanged (per-database client
gating, subscribe gate, idempotent seeds, `/v1/mirrors` sidecar, coordinator
socket). `--bitcraft-cache` adds:

- an embedded `relay-cache` fed in-process per mirrored `bitcraft-live-N`
  (schema taken from the same raw schema JSON the mirror fetched — no extra
  HTTP, no drift),
- the full cache HTTP/protobuf API + dim-buildings WebSocket on
  `--cache-bind`, identical in shape to the standalone `relay-cache`,
- an RSS ceiling sampler (`--cache-mem-ceiling-bytes`, same default as the
  standalone cache) that flips `/cache-health` to not-ready.

The cache only indexes the tables it serves (see
`crates/relay-cache/src/shard.rs`); all other mirrored tables flow to the
relational store and downstream WebSocket subscribers as usual. `location_state`
rows are kept when `dimension != 1` **or** the entity is a tracked
hexite/depleted deposit — which removes the standalone cache's second
per-deposit subscription set and its `HexiteLocationsMissing` reconnect class.

## Merge policy vs spacetimedb-public-mirror

The `public-mirror` remote tracks the parent fork. Pull fixes with:

```sh
git fetch public-mirror
git cherry-pick <sha>   # or merge public-mirror/public-mirror-v1
```

Keep `crates/public-mirror`, `crates/standalone` diffs small and one-purpose so
upstream (parent-fork) fixes apply cleanly. BitCraft-specific behavior stays
inside `crates/relay-cache` and behind `--bitcraft-cache` wherever possible.

## License

SpacetimeDB is licensed under the Business Source License 1.1 (see
[`LICENSE.txt`](LICENSE.txt)); `crates/relay-cache` retains its MIT license
notice. Keep both conspicuous.
