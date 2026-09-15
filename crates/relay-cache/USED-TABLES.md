# relay-cache: synchronized tables and endpoint usage

`relay-cache` subscribes to **36 distinct upstream SpacetimeDB tables** per
region (plus 6 global-module tables for the Bit-Me resolve chain) and exposes
them through **21 GET HTTP endpoints** plus
`POST /roads/region/:id/resources`. Every table is
read by at least one endpoint except two that are subscribed-but-unused
(`rent_state`, `player_housing_desc`).

Canonical sources: table-name constants in `decode.rs`, subscribe SQL in
`shard.rs` `base_subscribe_queries()`, store modules in `store/`, routes in
`serve.rs`.

---

## Table inventory (grouped) — with endpoint usage

### Claim / Settlement

| Upstream table | Store module | Description | Endpoints |
|---|---|---|---|
| `claim_state` | `store/claim.rs` | Settlement header: owner, name, neutral flag | `/claim`, `/claim/:id`, `/claim/:id/inventory`, `/claim/:id/members`, `/claim/:id/citizens`, `/claim/:id/hexcoins`, `/claim/:id/crafts`, `/deposits`, `/storage-logs`, `/player/:id/inventory`, `/player/:id/housing` |
| `claim_local_state` | `store/claim_local.rs` | Per-claim local data (supplies, upkeep, treasury, location) | `/claim/:id`, `/deposits` |
| `claim_member_state` | `store/claim_member.rs` | Roster rows (roles per player) | `/claim/:id/members`, `/claim/:id/citizens`, `/claim/:id/hexcoins` |
| `claim_tech_state` | `store/claim_tech.rs` | Per-claim unlocked tech (tier calc) | `/claim`, `/claim/:id` |
| `claim_tech_desc` | `store/claim_tech.rs` | Tech catalog (id → descriptor) | `/claim`, `/claim/:id` |
| `claim_tile_cost` | `store/claim_tile_cost.rs` | Tile-count bracket → cost/tile | `/claim/:id` |

### Building / Storage

| Upstream table | Store module | Description | Endpoints |
|---|---|---|---|
| `building_state` | `store/building.rs` | Building header (~74K rows/region) | `/claim/:id/inventory`, `/claim/:id/crafts`, `/player/:id/inventory`, `/player/:id/housing`, `/player/:id/crafts`, `/storage-logs` |
| `building_desc` | `store/building_desc.rs` | Building catalog (name, storage flag) | `/claim/:id/inventory`, `/claim/:id/crafts`, `/player/:id/inventory`, `/player/:id/housing`, `/player/:id/crafts`, `/storage-logs` |
| `building_nickname_state` | `store/building_nickname.rs` | Player-assigned nicknames | `/claim/:id/inventory`, `/player/:id/housing`, `/storage-logs` |
| `inventory_state` | `store/inventory.rs` | Storage contents (pockets, owner) | `/claim/:id/inventory`, `/claim/:id/members`, `/claim/:id/citizens`, `/claim/:id/hexcoins`, `/player/:id/inventory`, `/player/:id/housing` |

### Player / Session

| Upstream table | Store module | Description | Endpoints |
|---|---|---|---|
| `player_username_state` | `store/player_username.rs` | Player → username map | `/claim`, `/claim/:id`, `/claim/:id/crafts`, `/player`, `/player/:id`, `/player/:id/inventory`, `/player/:id/housing`, `/player/:id/skills`, `/player/:id/crafts` |
| `player_state` | `store/player_state.rs` | Login/session timestamps | `/claim/:id/members` (+aliases), `/player`, `/player/:id`, … |
| `mobile_entity_state` | `store/mobile_entity.rs` | Last-move timestamp + live world position (milli-units; public proxy for private `player_timestamp_state`) | same as `player_state`, `/bitme/session/:id` |
| `stamina_state` | `store/stamina.rs` | Current stamina (F32) + last-decrease clock | `/bitme/session/:id` |
| `active_buff_state` | `store/active_buff.rs` | Per-entity buff lists (zeroed placeholders filtered at read) | `/bitme/session/:id` |
| `player_action_state` | `store/player_action.rs` | Action lifecycle per layer (start/duration/target/result); rows persist after completion | `/bitme/session/:id` |
| `character_stats_state` | `store/character_stats.rs` | Stat values array (index 0 = max health, 1 = max stamina) | `/bitme/session/:id` |
| `character_stat_desc` | `store/character_stat_desc.rs` | Stat catalog; resolves the max-health/max-stamina indices by name | `/bitme/session/:id` |
| `extraction_recipe_desc` | `store/extraction_recipe.rs` | Extraction catalog (`recipe_id → resource_id`); runtime-authoritative identity for forageable targets | `/bitme/session/:id` |
| `resource_desc` | `store/resource_desc.rs` + `roads/decode.rs` (footprint offsets) | Static resource gamedata (name, max health, despawn/respawn, destroy-yield chain); footprint feeds the resource tile map | `/bitme/session/:id`, spawn watch-set, resource tile map |
| `resource_health_state` | *(tracker-scoped, `src/bitme.rs`)* | Per-resource current health — hundreds of thousands of rows per region, so NOT stored wholesale; retained only for Bit-Me tracked action targets | `/bitme/session/:id` |
| `experience_state` | `store/experience.rs` | Player → skill XP stacks | `/claim/:id/members` (+aliases), `/player/:id/skills` |

### Player Housing / Rent

| Upstream table | Store module | Description | Endpoints |
|---|---|---|---|
| `player_housing_state` | `store/player_housing.rs` | Per-player housing (global; sharded by `region_index`) | `/player/:id/housing` |
| `player_housing_desc` | `store/player_housing.rs` | Housing descriptors catalog | ⚠️ none (stats count only) |
| `rent_state` | `store/rent.rs` | Rent rows + player whitelist | ⚠️ none — unused |

### Deployables

| Upstream table | Store module | Description | Endpoints |
|---|---|---|---|
| `deployable_state_v2` | `store/deployable.rs` | Live deployables (v2) | `/claim/:id/members` (+aliases), `/player/:id/inventory` |
| `deployable_desc` | `store/deployable.rs` | Deployable type catalog | `/player/:id/inventory` |

### Crafting / Skills

| Upstream table | Store module | Description | Endpoints |
|---|---|---|---|
| `progressive_action_state` | `store/progressive_action.rs` | Active progressive crafts | `/claim/:id/crafts`, `/player/:id/crafts` |
| `public_progressive_action_state` | `store/public_progressive_action.rs` | Public craft flag join | `/claim/:id/crafts`, `/player/:id/crafts` |
| `passive_craft_state` | `store/passive_craft.rs` | Passive crafts | `/claim/:id/crafts`, `/player/:id/crafts` |
| `crafting_recipe_desc` | `store/crafting_recipe_desc.rs` | Recipe catalog | `/claim/:id/crafts`, `/player/:id/crafts` |
| `skill_desc` | `store/skill_desc.rs` | Skill catalog | `/claim/:id/members` (+aliases), `/player/:id/skills` |

### World / Static descriptors

| Upstream table | Store module | Description | Endpoints |
|---|---|---|---|
| `location_state` | `store/location_dim.rs` + `roads/apply.rs` | Entity → dimension (filtered subscribe + hexite PK phase). Embedded roads path also joins overworld x/z onto the resource tile map. | `/claim/:id/inventory`, `/player/:id/housing`, `POST /roads/region/:id/resources`, `/bitme/session/:id/resources` |
| `dimension_network_state` | `store/dimension_network.rs` | Dimension-network entrances | `/claim/:id/inventory`, `/player/:id/housing` |

### Resources / Growth / Forensics

| Upstream table | Store module | Description | Endpoints |
|---|---|---|---|
| `resource_state` | `store/resource.rs` (hexite) + `roads/resource_map.rs` (all types) | Hexite deposits for `/deposits`; **every** resource entity stamped onto the dense per-region tile map (u16/tile: 10-bit dictionary index + 3-bit direction + origin flag), footprints from `resource_desc.footprint` × `direction_index`. | `/deposits`, `POST /roads/region/:id/resources`, `/bitme/session/:id/resources` |
| `resource_growth_timer` | `store/resource_growth_timer.rs` | Respawn clock (`scheduled_at`) | `/deposits` |
| `growth_state` | `store/growth.rs` | Legacy respawn snapshot (fallback) | `/deposits` |
| `storage_log_state` | `store/storage_log.rs` | Deposit/withdraw history (~15–16 day retention) | `/storage-logs` |

---

## Endpoint → tables view

Endpoints with **no** synchronized tables: `/cache-health`, `/proto`,
`/proto/:name`. `/internal/stats` counts all 30 via `.len()`.

| Endpoint | Tables read |
|---|---|
| `/claim` | claim_state, player_username_state, claim_tech_state, claim_tech_desc |
| `/claim/:id` | + claim_local_state, claim_tile_cost |
| `/claim/:id/inventory` | claim_state, building_*, location_state, inventory_state, dimension_network_state |
| `/claim/:id/members` / `citizens` / `hexcoins` | claim_state, claim_member_state, experience_state, skill_desc, player_state, mobile_entity_state, inventory_state, deployable_state_v2 |
| `/claim/:id/crafts` | claim_state, building_*, progressive_action_*, passive_craft_state, crafting_recipe_desc, player_username_state |
| `/player` / `/player/:id` | player_username_state, player_state, mobile_entity_state |
| `/player/:id/inventory` | + inventory_state, deployable_*, building_*, claim_state |
| `/player/:id/housing` | + player_housing_state, dimension_network_state, building_nickname_state, location_state |
| `/player/:id/skills` | + experience_state, skill_desc |
| `/player/:id/crafts` | + progressive_action_*, passive_craft_state, crafting_recipe_desc, building_* |
| `/deposits` | claim_state, claim_local_state, resource_state, resource_growth_timer, growth_state |
| `/storage-logs` | storage_log_state, building_*, claim_state |
| `POST /roads/region/:id/resources` | resource_map (`roads/resource_map.rs`: **all** `resource_state` types ⋈ location_state dim 1, footprints from `resource_desc.footprint` × `direction_index`); raw resource ids, client-side harvestable filtering |
| `/bitme/resolve?name=` | global: player_lowercase_username_state ⋈ user_state ⋈ user_region_state ⋈ region_connection_info (+ world_region_name_state, signed_in_player_state); region shards for display username / sign-in fallback |
| `/bitme/session/:id` | mobile_entity_state (position), claim overlay via roads grid, stamina_state, character_stats_state ⋈ character_stat_desc, active_buff_state, player_action_state, resource_health_state (tracker-scoped), resource_desc + extraction_recipe_desc (gamedata), live resource_state spawn log (`bitme.rs`) |
| `/bitme/session/:id/resources` | mobile_entity_state (player anchor tile) + resource tile map (`roads/resource_map.rs`: all resource_state ⋈ location_state); packed u16/tile binary window |
| `/bitme/region/:id/resource-dictionary` | resource tile map dictionary (resource_id ↔ 10-bit index) ⋈ resource_desc (names, timers); `harvestable` flag from vendored `harvestable_resource_ids.json` |

---

## Notable findings

- **Two subscribed-but-unused tables:** `rent_state`, `player_housing_desc`.
- **Three tables not subscribed:** `deployable_state` (v1), `player_timestamp_state`
  (private), bulk overworld `location_state` (filtered; hexite coords re-fetched).
- **Two-phase subscription:** base queries then hexite-location PK backfill.
- **`/claim/:id/citizens` and `/hexcoins`** are deprecated aliases of `/members`.
- **Live WebSocket:** `/internal/dim-buildings/ws` only; inventory/housing/crafts
  are HTTP/protobuf.
- **Bit-Me (`/bitme/*`):** poll-only JSON; a GET on `/bitme/session/:id` registers
  the session (15 min TTL). Watched spawns = destroy-yield chain targets plus the
  vendored citric bush ids (`data/bitme_watched_resource_ids.json`).
- **Resource tile map (`roads/resource_map.rs`):** dense u16-per-tile map of the
  whole 7680×7680 region (112.5 MiB/region, included in `memory_bytes()`),
  fed by all `resource_state` + overworld `location_state` rows and
  `resource_desc.footprint`. One resource per tile: multi-hex shapes
  overwrite their tiles; a single-hex newcomer onto an occupied tile (the
  world does spawn forageables under multi-hex resources) is not stamped;
  clears only zero words the entity actually wrote. Tile word: bits 0–9
  dictionary index, bit 10 origin flag, bits 11–13 `direction_index`,
  bits 14–15 reserved. Backs both `POST /roads/region/:id/resources` (all
  types now — harvestable filtering is client-side via the dictionary's
  `harvestable` flag from `data/harvestable_resource_ids.json`) and
  `GET /bitme/session/:id/resources` (400×400 packed window). The former
  `roads/harvestable.rs` 215-id allowlist index is retired.
