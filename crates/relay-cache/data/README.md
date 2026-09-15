# Vendored gamedata files

Static BitCraft gamedata compiled into the cache via `include_str!`.

## `harvestable_resource_ids.json`

The old roads-side harvestable classification: the original BitCraft
`resource.json` tag set (Tree, Sapling, Wood Logs, Stump, Ore Vein, Rock,
Rock Boulder, Rock Outcrop, Clay, Sand — 215 ids). The resource tile map
(`roads/resource_map.rs`) itself carries **all** resource types — trees,
ore, mushrooms, berry bushes, event resources — so this list is no longer
a server-side filter. It is surfaced per entry by
`GET /bitme/region/:id/resource-dictionary` (the `harvestable` flag) so
clients can reproduce the old filter without vendoring the list.

## `bitme_watched_resource_ids.json`

Bit-Me only: the three Citric Giant berry bush resource ids, added to the
spawn-watch set (they are not referenced by any `on_destroy_yield_resource_id`,
so the dynamic yield-chain rule cannot discover them). This file does not
affect the roads API.

## Where resource footprints come from now

Multi-hex occupancy is decoded live from the mirrored `resource_desc`
table (`resource_desc.footprint`, axial offsets rotated by
`resource_state.direction_index`); the former vendored
`harvestable_footprints.json` is retired. Ids whose desc row has not been
seen fall back to a single origin hex.

## Where Bit-Me resource identity actually comes from

`/bitme/session` target identity resolves through the resource tile map
first (every resource type), then the spawn log and hexite store; the
Extract action's `recipe_id` joined against the mirrored
`extraction_recipe_desc` table (`store/extraction_recipe.rs`, ~650 rows
ingested at runtime) remains the last-resort fallback; `resource_desc`
(store, live) then supplies name / max health / timers. This is
runtime-authoritative: new resource types added by game patches resolve
without any data-file regeneration.
