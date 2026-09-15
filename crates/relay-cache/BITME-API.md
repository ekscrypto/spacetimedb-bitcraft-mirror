# Bit-Me relay API — client reference

The HTTP endpoints the Bit-Me mobile app consumes, served by the
embedded relay-cache on **`https://relay.bitcraftsync.app`**. Plain JSON
over HTTPS, no authentication, no WebSocket, no SpacetimeDB protocol —
the phone only polls. (One exception: the resource-map window in §4 is a
packed binary payload.)

Source of truth: `src/bitme_serve.rs` (handlers) and `src/bitme.rs`
(tracker). Operator-facing inventory: `USED-TABLES.md`. Design notes:
`BITME-DATA-ASSESSMENT.md` in the workspace root.

---

## 1. Conventions

| Convention | Detail |
|---|---|
| Transport | HTTPS GET, JSON responses. `Cache-Control: no-store` on every response. CORS: `*`. |
| Entity ids | **JSON strings** — upstream ids are u64 and exceed JS `Number.MAX_SAFE_INTEGER`. Never parse them as numbers. |
| Positions | `world_x`/`world_z` are float world units (upstream milli-units ÷ 1000; one tile = 1.0). `tile_x`/`tile_z` are the integer odd-r tiles (`floor(world)`), the same coordinate space as the `/roads` endpoints. |
| Clocks | `*_ms` fields are unix **milliseconds**. Buff `start_timestamp` / `duration` are unix **seconds**. `last_decrease_at` is RFC 3339. `server_time_ms` (session) lets the client correct for clock skew. |
| Nulls | A present-but-`null` field means *unknown or not applicable* — never an error. Absent-on-wire is not used; every documented field is always present. |
| Errors | `400` with `{"error": "…"}` for bad input; `404` with `{"found": false, …}` for misses. |
| Rate limits | None enforced today (nginx 60 s timeouts only). Design for **1 Hz polling** per active session; a snapshot is ~2–6 KB. |
| Sessions | A `GET /bitme/session/:id` **is** the registration. Stop polling for >15 min and the server drops its tracking (next poll transparently re-registers). |

---

## 2. `GET /bitme/resolve?name=<username>`

Resolve a player name to the ids/region the app needs to open a game
connection or a session. **Exact match on the lowercase name** — this is
not a search. (Substring player search exists separately at
`GET /player?name=`.)

Works for players of **all 25 regions** (global-module data), independent
of which regions the mirror currently covers.

| Param | Required | Meaning |
|---|---|---|
| `name` | yes | Player name, any case (lowercased server-side, exact match) |

### Response `200`

```json
{
  "found": true,
  "entity_id": "504403158290646123",
  "username": "Whisper",
  "username_lowercase": "whisper",
  "identity": "c2003111fc31ca323b17ed6063f1522a6a446f1384d78eadb23449b6cb9ce4a6",
  "region_id": 7,
  "region_name": "Virexal",
  "host": "https://bitcraft-early-access.spacetimedb.com",
  "module": "bitcraft-live-7",
  "signed_in": true
}
```

| Field | Type | Meaning |
|---|---|---|
| `found` | bool | Always `true` on 200. |
| `entity_id` | string | Player entity id — the key for `/bitme/session/:id` and every other relay player route. |
| `username` | string | Display-cased name (from the region shard; falls back to the lowercase form). |
| `username_lowercase` | string | The exact key that matched. |
| `identity` | string \| null | SpacetimeDB identity, canonical (big-endian) hex, no `0x` prefix. |
| `region_id` | int \| null | Home region (1–25). |
| `region_name` | string \| null | Player-facing region name (e.g. `Virexal`). |
| `host` | string \| null | Region sign-in host. |
| `module` | string \| null | Database/module name (e.g. `bitcraft-live-7`). |
| `signed_in` | bool \| null | Live sign-in state. `null` only when neither the global presence set nor a mirrored home region knows. |

### Errors

```json
HTTP 404  {"found": false, "name_lowercase": "nosuchuser123"}
HTTP 400  {"error": "missing or empty `name` query parameter"}
```

---

## 3. `GET /bitme/session/:entity_id`

One snapshot with everything the activity screens render. `entity_id` is
the player entity id from **resolve** (decimal string).

Poll cadence: ~1 Hz while the player is active in the app. The poll keeps
the server-side tracker alive; target health and spawn tracking only
accumulate while sessions are being polled.

### Response `200` (annotated real example)

```json
{
  "found": true,
  "player_entity_id": "504403158290646123",
  "username": "Whisper",
  "signed_in": true,
  "region": 7,
  "position": { … },
  "claim": { … },
  "stamina": { … },
  "buffs": [ … ],
  "actions": [ … ],
  "target": { … },
  "activity_spawns": [ … ],
  "server_time_ms": 1788628492673
}
```

### `position` — last known world position

```json
{
  "world_x": 11173.0,      "world_z": 13848.002,
  "tile_x": 11173,         "tile_z": 13848,
  "destination_world_x": 11173.0, "destination_world_z": 13848.002,
  "dimension": 1,
  "is_walking": false,
  "timestamp_ms": 1788628340546,
  "age_ms": 152127
}
```

- `dimension: 1` is the overworld; `> 1` is a building/dungeon interior
  (then `world_*` are interior coords and **claim is not resolved**).
- The row **persists after logout** — it is the *last known* position.
  Judge freshness by `age_ms`, and `signed_in` for liveness.
- `null` when the entity has no mobile row (never observed in a mirrored
  region).

### `claim` — claim under the player's tile (`null` when none)

```json
{
  "entity_id": "504403158281321768",
  "name": "Hex and Highwater Port",
  "owner_player_entity_id": "1008806316547466858",
  "neutral": false
}
```

`null` in unclaimed wilderness, in interiors, or if the roads cache is
disabled (it is enabled in production).

### `stamina`

```json
{
  "current": 370.5,
  "max": 471.0,
  "max_health": 210.0,
  "last_decrease_at": "2026-09-05T17:14:52.000Z"
}
```

`current`/`max` are floats (upstream F32). Regeneration is **not**
simulated server-side — project forward client-side using the passive
stamina regen rate from bundled gamedata, anchored at
`last_decrease_at`/`current`. `null` when the player has no stamina row
(e.g. never spent stamina since row cleanup).

### `buffs` — live buffs only

```json
{ "buff_id": 5887916, "start_timestamp": 1788627036, "duration": 3600,
  "values": [0.092, 0.092] }
```

- Zeroed placeholder entries (the upstream table lists every buff type
  the entity ever saw) are filtered server-side; what remains is live.
- Countdown = `start_timestamp + duration − now` (unix seconds). Expired
  entries may linger until the next upstream flush of the row — check the
  countdown, not just presence.
- `values` are raw modifier numbers; map `buff_id` → name/icon/effects
  from bundled gamedata (`buff_desc`).

### `actions` — action lifecycle (usually ≤ 2, one per layer)

```json
{
  "auto_id": "122145",
  "action_type": "Craft",
  "layer": "Base",
  "start_time_ms": 1788628492480,
  "duration_ms": 1088,
  "ends_at_ms": 1788628493568,
  "target_entity_id": "504403158308175117",
  "recipe_id": 109007,
  "last_action_result": "Success",
  "client_cancel": false
}
```

- `action_type`: `None`, `Attack`, `Extract`, `Craft`, `Build`, `Terraform`,
  `Sleep`, `Death`, `Prospect`, … (upstream sum variants, PascalCase).
- `layer`: `Base` or `UpperBody` (a player can run one of each).
- **Rows persist after completion** — they are the *last* action on that
  layer, not necessarily a running one. An action is in progress iff
  `server_time_ms < ends_at_ms` (and `last_action_result == "Success"`).
- Progress bar: `(now − start_time_ms) / duration_ms`.
- `recipe_id` → recipe gamedata for extract/craft progress naming.

### `target` — the acted-on entity, enriched (`null` when no target)

```json
{
  "entity_id": "504403158302441789",
  "resource_id": 38,
  "name": "Flint Pile",
  "health": 2398,
  "max_health": 10000,
  "despawn_time_secs": 0.0,
  "respawn_time_secs": 600.0,
  "growth_ends_at_ms": 1788664248840,
  "location": { "tile_x": 10213, "tile_z": 12367 }
}
```

- The primary target is the **`Extract` action's target** (else the
  `Base`-layer target — e.g. a crafting station, which then has
  `resource_id: null` because it is a building, not a resource).
- `health` is tracked server-side **only while a session is polling** and
  only from the first extract tick onward: expect `null` on the first
  poll after targeting, then per-tick updates (~once per extraction tick).
- `resource_id`/`name`/`max_health` resolve via (in order) the roads
  harvestable index, the spawn log, and — the runtime-authoritative path —
  the Extract action's `recipe_id` joined against `extraction_recipe_desc`
  plus `resource_desc` gamedata. This covers **every extractable resource
  type** (mushrooms, berry bushes, flowers, herbs, …), including types added
  in future game patches; genuinely unknown targets keep `null` identity but
  still report `health`.
- `despawn_time_secs` / `respawn_time_secs` are passthrough gamedata for
  client-side countdowns.
- `growth_ends_at_ms` is the server-authoritative end of the target's
  current growth stage (unix ms) — see `activity_spawns` below. This is
  what the in-game target frame counts down for growth-stage resources
  (T2 event berry bushes show their 600 s Bountiful window here even
  though `despawn_time_secs` is 0). Null when the entity has no live
  growth timer.

### `activity_spawns` — watched spawns in the player's area

```json
{
  "entity_id": "504403163787158939",
  "resource_id": 2089325907,
  "name": "Baited School Of Muddy Auratus",
  "health": null,
  "max_health": 3000,
  "location": { "tile_x": 11448, "tile_z": 11357 },
  "spawned_at_ms": 1788622724778,
  "expires_at_ms": null,
  "growth_ends_at_ms": null
}
```

- **What is watched:** every resource the game can spawn as the
  destroy-yield of another resource (growth/respawn chains — Withering →
  Bountiful berry bushes, depleted ore → fresh ore, baited fishing
  schools, …) plus the three **Citric Giant berry bushes** (see table
  below). This is the citric-detection signal.
- **Scope:** only spawns located in the player's current claim — or in
  unclaimed wilderness when the player is outside any claim — and in the
  player's region. Interior/dungeon spawn churn is dropped.
- Entries disappear when the resource is harvested/despawned (upstream
  delete) or after 30 minutes.
- `expires_at_ms` = `spawned_at_ms + despawn_time` when the resource
  gamedata has a despawn timer; otherwise `null` (compute client-side
  from gamedata if needed).
- `growth_ends_at_ms` = when the entity's current **growth stage** ends
  (unix ms), from the scheduled reducer clock (`resource_growth_timer`,
  legacy fallback `growth_state.end_timestamp`). Null when the entity has
  no live growth timer. Growth-stage resources carry their whole life
  window here — T2 event berry bushes: 600 s Bountiful (then Citric
  spawns in place), 30 s Citric (then back to Withering). This is the
  exact clock the in-game target frame counts down; prefer it over the
  `expires_at_ms` gamedata estimate and over fixed fallbacks. Note the
  semantic differs per resource: on depleted nodes carrying a respawn
  timer (hexite) it is the respawn completion, not a despawn.
- `health` non-null means someone is already harvesting it.

#### Citric / Bountiful Berry Bush gamedata ids

| resource_id | Name | Notes |
|---|---|---|
| `1688062540` | Citric Giant Strawberry Bush | **Citric event** — watch `activity_spawns` for these |
| `65901922` | Citric Giant Savory Berry Bush | Citric event |
| `1875092977` | Citric Giant Zesty Berry Bush | Citric event |
| `1822942131` | Giant Bountiful Strawberry Bush | Harvestable bush (max_health 500) |
| `353689546` | Giant Bountiful Savory Berry Bush | Harvestable bush |
| `1713099134` | Giant Bountiful Zesty Berry Bush | Harvestable bush |
| `2022069397` | Withering Giant Strawberry Bush | Destroying yields the Bountiful bush |
| `379556870` | Withering Giant Savory Berry Bush | idem |
| `206224604` | Withering Giant Zesty Berry Bush | idem |

All nine share a 7-hex footprint and the `Bountiful Berry Bush` tag.

### Errors

```json
HTTP 404  {"found": false, "player_entity_id": "…",
           "error": "player not present in any mirrored region"}
HTTP 400  {"error": "entity_id must be a u64"}
```

A player resolves globally even when their region is not mirrored, but a
**session** requires them to be present in one of the mirrored regions
(currently 3, 7, 8, 9, 11, 12, 13, 14, 15, 17, 18, 19, 23).

---

## 4. `GET /bitme/session/:entity_id/resources`

A packed **400×400 resource map** centered on the player, served straight
from the relay's dense per-region resource tile map — every resource type
in the region (trees, ore, mushrooms, berry bushes, event resources, …),
not just the old roads harvestable list. Poll counts as session activity
(same TTL refresh as the snapshot).

- **Anchor:** the player's tile sits at relative `(200, 200)`; the window
  covers world tiles `origin … origin+399` per axis.
- **Response:** `application/octet-stream`, `Cache-Control: no-store`,
  320,024 bytes = 24-byte header + 160,000 little-endian u16 tile words
  (row-major, 800 bytes per row).

### Header (all fields little-endian)

| Offset | Type | Meaning |
|---|---|---|
| 0–3 | `[u8; 4]` | Magic `BMR1` |
| 4–5 | `u16` | Format version (`1`) |
| 6–7 | `u16` | Window width in tiles (`400`) |
| 8–11 | `i32` | `origin_world_x` (odd-r world tile of relative column 0) |
| 12–15 | `i32` | `origin_world_z` (odd-r world tile of relative row 0) |
| 16–19 | `u32` | Region id |
| 20–23 | `u32` | `dict_version` — pairs the payload with a `/resource-dictionary` |

### Tile word (u16 LE)

| Bits | Meaning |
|---|---|
| 0–9 | Resource dictionary index (`0` = empty tile; expand via the dictionary) |
| 10 | **Origin flag** — this tile is the resource's anchor tile |
| 11–13 | `direction_index` (0–5), repeated on every tile of the footprint |
| 14–15 | Reserved (zero) |

The server stamps every occupied tile of a multi-hex resource
(`resource_desc.footprint` × `direction`), so per-tile rendering needs no
footprint math. To render one icon per resource instead, reconstruct
instances client-side: each tile with the **origin flag** anchors an
instance of `direction`-rotated shape; adjacent same-type resources are
disambiguated by their origin tiles. Tile word `0` also marks tiles
outside the mirrored region (windows overhang region borders; there is no
cross-region stitching).

### Errors

```json
HTTP 400  {"error": "entity_id must be a u64"}
HTTP 404  {"found": false, "player_entity_id": "…",
           "error": "player not present in any mirrored region"}
HTTP 404  {"found": false, "player_entity_id": "…",
           "error": "player not on the overworld"}
HTTP 202  (empty body — region grid still seeding; retry with backoff)
HTTP 503  {"error": "roads cache not enabled"}
```

---

## 5. `GET /bitme/region/:region/resource-dictionary`

The `resource_id ↔ 10-bit tile index` map for expanding window payloads,
plus gamedata. Fetch once per region and refetch when a window header's
`dict_version` changes — indices are assigned in first-sighting order and
are **stable per deploy but not across deploys**.

### Response `200`

```json
{
  "region": 7,
  "ready": true,
  "dict_version": 1834726123,
  "entries": [
    {
      "index": 1,
      "resource_id": 38,
      "name": "Flint Pile",
      "harvestable": false,
      "max_health": 10000,
      "despawn_time_secs": 0.0,
      "respawn_time_secs": 600.0
    }
  ]
}
```

| Field | Meaning |
|---|---|
| `index` | The 10-bit value carried in tile words (index `0` is the empty sentinel and is not listed) |
| `name` / `max_health` / `despawn_time_secs` / `respawn_time_secs` | `resource_desc` gamedata (`null` if the desc row is missing) |
| `harvestable` | The old roads-side forestry/mining/clay/sand classification — use this to filter the map client-side; the window itself includes **all** resource types |

### Errors

```json
HTTP 400  {"error": "region must be a u32"}
HTTP 404  {"found": false, "error": "unknown region 99"}
HTTP 202  (empty body — region grid still seeding)
HTTP 503  {"error": "roads cache not enabled"}
```

---

## 6. Readiness, deploys, and failure modes

- **Readiness probe:** `GET /cache-health` → `{"ready": true, …}`. If
  `ready` is `false`, treat all relay data as stale.
- **Deploys restart the mirror** (~15–20 min reseed). During that window
  the JSON endpoints return `404 {"found": false, …}`, the resource window
  returns `202` then `404`, and `/cache-health` flips to `ready: false`.
  Retry with backoff (≥ 30 s); Bit-Me only needs
  to show a reconnecting state — countdowns already on screen can keep
  running from the last snapshot.
- Upstream game-server resets can also momentarily empty stores; the same
  backoff applies.
- Region coverage can change; read the region list from
  `GET /roads/regions` rather than hardcoding, if it matters.

## 7. Recommended client flow

1. Onboarding: `resolve` the player name → store `entity_id`,
   `region_id`, `module`.
2. Activity screens: poll `session/:entity_id` at ~1 Hz while open.
3. Resource map: fetch `region/:region_id/resource-dictionary`, then poll
   `session/:entity_id/resources` alongside the snapshot; refetch the
   dictionary when a window header's `dict_version` changes.
4. Render countdowns client-side from the snapshot + bundled gamedata
   (action progress from `ends_at_ms`; buff expiry from
   `start_timestamp + duration`; stamina regen projected from
   `last_decrease_at`; citric despawn from `expires_at_ms`).
5. Citric detection: alert when an `activity_spawns` entry with
   `resource_id ∈ {citric ids}` appears.
6. Never hold state across deploys — on `404`/`ready=false`, back off and
   re-resolve.

## 8. Rate/abuse posture

Anonymous, no API keys, no rate limits today (same posture as
`/claim`/`/player`; nginx caps request bodies and timeouts). Keep polling
at ~1 Hz per active screen; do not fan out resolve calls per keystroke —
debounce name lookups. The resource window is ~320 KB per poll (before
HTTP compression); poll it only while the map screen is open, and prefer
letting transport-level gzip handle it.
