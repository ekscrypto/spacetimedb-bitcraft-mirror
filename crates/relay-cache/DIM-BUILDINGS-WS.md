# Housing Dimension → Buildings WebSocket

A dedicated WebSocket on `relay-cache` that streams the **building id set**
of house interior dimensions. It lets streamd learn which
`building_entity_id`s live inside a given housing dimension without
subscribing to the ~13M-row/region `location_state` table itself, and
without relay-cache fanning full inventory JSON to hundreds of clients.

The payload is just `building_entity_id` u64 arrays per dimension — **no
player names, no inventory contents, no internal counters** — so the
endpoint is safe to expose publicly. The primary consumer is
`bitcraft-streamd` on a separate host; other clients are welcome to use it.

## Endpoint

```
ws://127.0.0.1:8089/internal/dim-buildings/ws                 (loopback)
wss://relay.bitcraftsync.app/internal/dim-buildings/ws        (public, TLS)
```

Registered alongside `/internal/stats` in
[`src/serve.rs`](src/serve.rs). nginx proxies this exact path publicly
(WS-upgrade headers, 3600s read/send timeouts); the rest of `/internal/*`
stays loopback-only via an nginx `location /internal/ { return 404; }`
catch placed after this route. See
[`tools/nginx-relay-cache.snippet`](../../tools/nginx-relay-cache.snippet).

**No auth header.** If abuse ever becomes a problem, the natural throttle
is the existing per-connection cap (256 dims) + connection cap (2048
fleet-wide) — both already enforced in the handler. A shared-secret token
in `INTERNAL_TOKEN` is a deferred option, not currently wired.

## This is NOT a re-enable of the disabled `/inventory/ws`

The disabled `/inventory/ws` (commit `c7e48b6`, 2026-07-25) failed at 500+
connections because it serialized full per-entity snapshots × connections ×
change-rate, causing CPU saturation and OOM kills. **This protocol has the
opposite profile** and must not be confused with it:

| Concern | Disabled `/inventory/ws` | This `/internal/dim-buildings/ws` |
|---|---|---|
| Clients | 500+ browsers | Designed for streamd; publicly reachable (cap 2048 conns) |
| Payload per event | Full inventory/craft JSON (KB) | `Vec<u64>` of building IDs per dimension |
| Trigger | Every inventory delta | Building added/removed in a subscribed dimension |
| Per-conn state | Per-entity watch leases + JSON cache | One watch per subscribed dimension |
| Fan-out work | `entities × changes × connections` | `dimensions × changes × watching connections` |

The old multiplexed `/inventory/ws` code (`src/stream.rs`) and its
snapshot-JSON cache were removed entirely; this is a fresh handler that
reuses only the `InterestHub` pub/sub + `watch::Receiver` primitives.

## Authentication / trust

No auth header. The endpoint is public by design — the payload is
non-sensitive building ids. The sibling `/internal/stats` route stays
loopback-only (nginx `location /internal/ { return 404; }` catch).

## Subscribe (client → server), first text frame after `onopen`

```json
{
  "dims": [
    {"region": 14, "dimension": 12345},
    {"region": 14, "dimension": 12346},
    {"region": 3,  "dimension": 7890}
  ]
}
```

- `region` (`u32`): the owning region shard that holds this dimension's
  interior buildings. **Required** — `entrance_dimension_id` is
  region-scoped, so the server reads from `fleet.shards[s.region == region]`.
- `dimension` (`u32`): the `entrance_dimension_id` from
  `dimension_network_state`. `0` is invalid and **silently ignored**.
- A later subscribe frame **replaces** the entire set (full resync).
- **Cap**: 256 `(region, dimension)` pairs per connection (per-connection,
  not fleet-wide — streamd can open more connections or paginate if its
  union grows). Keys are deduped.
- Subscribe frame validation errors close the socket with `{"error": "…"}`.

### Unsubscribe (optional, client → server, after `subscribed`)

```json
{"unsubscribe": [{"region": 14, "dimension": 12345}]}
```

Removes from the active set **without** replacing the whole set. Server
replies with `{"type":"unsubscribed","count":N}` (the new live count).

## Server → client frames

### Initial burst (one frame per subscribed dimension), then ack

```json
{
  "type": "dim_snapshot",
  "region": 14,
  "dimension": 12345,
  "buildings": ["17592186044416", "17592186044417"]
}
```

- `buildings`: array of `building_entity_id` as **strings** (JS-safe; u64
  values exceed 2^53). Empty array is valid (house has no tracked storage
  buildings yet).
- Sorted ascending by `entity_id` (parity with the HTTP `/housing` join).
- Only buildings where `building_desc.include_in_claim_inventory(...)` is
  true are reported — **storage buildings only**, matching
  [`collect_housing_buildings`](src/serve.rs) exactly. Decorations and
  non-storage interiors are not reported.

Then:

```json
{"type": "subscribed", "count": 3}
```

### Live updates (after `subscribed`)

```json
{
  "type": "dim_delta",
  "region": 14,
  "dimension": 12345,
  "added":   ["17592186044418"],
  "removed": []
}
```

- Fires when a `building_state` insert/delete lands on a `location_state`
  row whose dimension matches a subscribed `(region, dimension)`, **or**
  when a building already in a tracked dimension becomes
  storage-relevant (its `building_description_id` changing to a storage
  type). See the touch hooks in `src/shard.rs`:
  `touch_location_entity` (primary) and `touch_building` (secondary).
- `added` / `removed` are incremental — apply them to the last snapshot.
  They will never overlap. A coalesced bump that yields no net change
  emits no frame.
- Coalesced ~75ms server-side.
- Filter: only buildings where
  `building_desc.include_in_claim_inventory(building_description_id)` is
  true are emitted — non-storage buildings inside a house are not reported.

### Heartbeat (every 5s after subscribe)

```json
{"ts": 1753296000123}
```

Unix ms UTC. Client treats ~15s without a frame (heartbeat or data) as
dead → reconnect with the existing exponential backoff.

## Why this protocol is safe where `/inventory/ws` wasn't

1. **Tiny fan-out surface.** Work is
   `dimensions × change-rate × connections watching that key`, with
   hard caps (2048 connections, 256 dims/conn). Primary consumer is
   streamd; the endpoint is public but the payload stays tiny.
2. **Tiny payloads.** `dim_delta` is ~80 bytes vs multi-KB inventory
   snapshots. Serialization cost is negligible — small enough that the
   old snapshot-JSON cache (`InterestHub::cached_or_build`) was removed
   entirely; the handler rebuilds the id set directly each bump.
3. **Rare triggers.** Building placement inside a house is infrequent
   (decorations, storage upgrades). Compare to `/inventory/ws`'s trigger
   of "every item movement anywhere."
4. **Reuses proven primitives.** The `InterestHub` watch model already
   filters to "only notify when a subscribed key changes." No new scaling
   axis introduced.

## Server-side implementation notes

1. **Route** — `.route("/internal/dim-buildings/ws", get(stream::dim_buildings_ws))`
   in `src/serve.rs`, under the `/internal/*` path prefix for naming.
   nginx proxies this exact path publicly; only `/internal/stats` stays
   unproxied.

2. **Topic key** — `Topic::DimensionBuildings` keyed by
   `dim_key(region, dimension)` = `(region as u64) << 32 | dimension as u64`
   (lossless; both fields are u32). See `src/interest.rs`.

3. **Touch hooks** — `src/shard.rs`:
   - `touch_location_entity` calls `touches.dimension_buildings(store.region, dimension)`
     on every non-overworld location delta (primary trigger: a building
     enters/leaves a subscribed housing interior).
   - `touch_building` looks up the building's current dimension and calls
     `touches.dimension_buildings(...)` when non-overworld (secondary
     trigger: a building becomes storage-relevant, e.g. its
     `building_description_id` changes).
   Both are gated by `InterestHub::is_watched` via `TouchBatch`, so they
   short-circuit when nobody is subscribed to that exact key.

4. **Handler** — `src/stream.rs::dim_buildings_ws` → `run_dim_stream`,
   modeled on the removed `inventory_bundle_ws`: parse subscribe frame,
   `bind_keys` over the dimension set, `select_all` over the cloned
   `watch::Receiver`s, on each bump re-scan via
   `housing_building_ids(&shard.store.read(), dimension)`, diff against
   the last-sent set, emit `dim_delta`.

5. **Snapshot helper** — `src/serve.rs::housing_building_ids` is the
   lightweight sibling of `collect_housing_buildings`: just the filtered,
   sorted `Vec<u64>` (no protobuf, no nickname/item aggregation) so it is
   cheap to run on every watch bump.

6. **Memory cost** — one `watch::Receiver` per subscribed
   `(region, dimension)` per connection. At 256 dims/conn the per-conn
   cost is negligible; fleet cost scales with live watches. No per-row
   duplication — the source of truth stays `location_dim` + `building`
   in the `RegionStore`.
