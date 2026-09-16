// SPDX-License-Identifier: MIT

//! HTTP handlers for `/bitme/*` — the Bit-Me mobile app read API.
//!
//! Phase-1 transport per `BITME-DATA-ASSESSMENT.md`: plain JSON polling,
//! stateless from the client's point of view. A `GET /bitme/session/:id`
//! *is* the session registration (TTL refresh in [`crate::bitme::BitmeHub`]);
//! the phone never opens a SpacetimeDB socket. All entity ids are JSON
//! strings (u64s exceed 2^53); positions are world units (milli-units / 1000)
//! with integer odd-r tiles alongside.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::roads::coords::{region_origin, world_to_local};
use crate::roads::grid::get_claim_index;
use crate::roads::resource_map::is_harvestable_resource_id;
use crate::roads::RoadsFleet;
use crate::serve::{format_rfc3339_millis, no_store_json, no_store_octets, no_store_status, Fleet};
use crate::store::RegionStore;

/// Window side in tiles for `GET /bitme/session/:id/resources` (the player
/// anchors at the center, `(width/2, width/2)`).
const RESOURCE_WINDOW_WIDTH: usize = 400;

pub fn bitme_routes() -> axum::Router<Fleet> {
    axum::Router::new()
        .route("/bitme/resolve", get(bitme_resolve))
        .route("/bitme/session/:entity_id", get(bitme_session))
        .route("/bitme/session/:entity_id/resources", get(bitme_session_resources))
        .route(
            "/bitme/region/:region/resource-dictionary",
            get(bitme_region_resource_dictionary),
        )
}

#[derive(Debug, Deserialize)]
struct BitmeNameQuery {
    name: Option<String>,
}

/// `GET /bitme/resolve?name=<lowercase or display>` → exact-match global
/// lookup: player entity_id, identity, home region, connection info, and
/// live sign-in state. Indexed lookups only — no scans.
async fn bitme_resolve(State(fleet): State<Fleet>, Query(q): Query<BitmeNameQuery>) -> Response {
    let Some(raw) = q.name.as_deref().map(str::trim).filter(|s| !s.is_empty()) else {
        return no_store_status(
            StatusCode::BAD_REQUEST,
            json!({"error": "missing or empty `name` query parameter"}),
        )
        .into_response();
    };
    let needle = raw.to_lowercase();

    let resolved = { fleet.bitme.global().read().resolve(&needle) };
    let Some(mut r) = resolved else {
        return no_store_status(StatusCode::NOT_FOUND, json!({"found": false, "name_lowercase": needle}))
            .into_response();
    };

    // Fallback: when `user_region_state` lacks the identity, derive the home
    // region from whichever region shard carries the player's rows.
    if r.region_id.is_none() {
        if let Some((region, _, _)) = shard_player_info(&fleet, r.entity_id) {
            let (name, host, module) = fleet.bitme.global().read().region_info(region);
            r.region_id = Some(region);
            r.region_name = name;
            if let Some(host) = host {
                r.host = host;
            }
            if let Some(module) = module {
                r.module = module;
            }
        }
    }

    // Display-cased username + sign-in fallback from the home region's
    // stores (the global table only carries the lowercase form; global
    // `signed_in_player_state` is authoritative when present).
    let shard_lookup = shard_player_info(&fleet, r.entity_id);
    let username = shard_lookup
        .as_ref()
        .and_then(|(_, name, _)| name.clone())
        .unwrap_or_else(|| r.username_lowercase.clone());
    let signed_in = r.signed_in.or_else(|| shard_lookup.and_then(|(_, _, signed)| signed));

    no_store_json(json!({
        "found": true,
        "entity_id": r.entity_id.to_string(),
        "username": username,
        "username_lowercase": r.username_lowercase,
        "identity": r.identity_hex,
        "region_id": r.region_id,
        "region_name": r.region_name,
        "host": if r.host.is_empty() { Value::Null } else { json!(r.host) },
        "module": if r.module.is_empty() { Value::Null } else { json!(r.module) },
        "signed_in": signed_in,
    }))
    .into_response()
}

/// `(region, display username, signed_in)` from the region shard holding
/// the player.
fn shard_player_info(fleet: &Fleet, entity_id: u64) -> Option<(u32, Option<String>, Option<bool>)> {
    for shard in &fleet.shards {
        let s = shard.store.read();
        if !s.ready {
            continue;
        }
        let username = s
            .player_username
            .find(entity_id)
            .map(|slot| s.player_username.username[slot as usize].to_string());
        let signed_in = s
            .player_state
            .find(entity_id)
            .map(|slot| s.player_state.signed_in[slot as usize]);
        if username.is_some() || signed_in.is_some() {
            return Some((s.region, username, signed_in));
        }
    }
    None
}

/// Region shard holding this player's live rows. The position shard (current
/// region) wins over the username shard (home region).
fn locate_player_region(fleet: &Fleet, pk: u64) -> Option<u32> {
    let mut home: Option<u32> = None;
    let mut live: Option<u32> = None;
    for shard in &fleet.shards {
        let s = shard.store.read();
        if !s.ready {
            continue;
        }
        if s.player_username.find(pk).is_some() || s.player_state.find(pk).is_some() {
            home = Some(s.region);
        }
        if s.mobile_entity.find(pk).is_some() {
            live = Some(s.region);
        }
        if live.is_some() && home.is_some() {
            break;
        }
    }
    live.or(home)
}

/// `GET /bitme/session/:entity_id` — one snapshot with everything the Bit-Me
/// activity screens need: position, current claim, stamina, buffs, action
/// lifecycle, action-target health, and watched-spawn (citric) events.
async fn bitme_session(State(fleet): State<Fleet>, Path(entity_id): Path<String>) -> Response {
    let Ok(pk) = entity_id.parse::<u64>() else {
        return no_store_status(StatusCode::BAD_REQUEST, json!({"error": "entity_id must be a u64"})).into_response();
    };

    // The GET is the registration: keep the tracker warm for the feed hooks.
    fleet.bitme.touch_session(pk);

    let region = locate_player_region(&fleet, pk);

    let Some(region) = region else {
        return no_store_status(
            StatusCode::NOT_FOUND,
            json!({
                "found": false,
                "player_entity_id": pk.to_string(),
                "error": "player not present in any mirrored region",
            }),
        )
        .into_response();
    };

    let Some(shard) = fleet.shards.iter().find(|s| s.region == region) else {
        return no_store_status(
            StatusCode::NOT_FOUND,
            json!({"found": false, "error": "region shard missing"}),
        )
        .into_response();
    };

    let snapshot = {
        let s = shard.store.read();
        build_session_snapshot(&fleet, &s, pk)
    };

    no_store_json(snapshot).into_response()
}

/// `GET /bitme/session/:entity_id/resources` — packed 400×400 resource
/// window anchored at the player's tile (player at relative `(200, 200)`),
/// served straight from the roads region's dense resource tile map.
///
/// Response body: 24-byte LE header, then `width²` u16 LE tile words
/// (row-major, row `r` ↔ world z `origin_world_z + r`; see
/// `roads/resource_map.rs` for the bit layout). Errors are JSON like the
/// rest of `/bitme`; an empty `202` means the region grid is still seeding.
async fn bitme_session_resources(State(fleet): State<Fleet>, Path(entity_id): Path<String>) -> Response {
    let Ok(pk) = entity_id.parse::<u64>() else {
        return no_store_status(StatusCode::BAD_REQUEST, json!({"error": "entity_id must be a u64"})).into_response();
    };

    // The window poll counts as session activity, same as the snapshot poll.
    fleet.bitme.touch_session(pk);

    let Some(region) = locate_player_region(&fleet, pk) else {
        return no_store_status(
            StatusCode::NOT_FOUND,
            json!({
                "found": false,
                "player_entity_id": pk.to_string(),
                "error": "player not present in any mirrored region",
            }),
        )
        .into_response();
    };
    let Some(shard) = fleet.shards.iter().find(|s| s.region == region) else {
        return no_store_status(
            StatusCode::NOT_FOUND,
            json!({"found": false, "error": "region shard missing"}),
        )
        .into_response();
    };
    let player_tile = {
        let s = shard.store.read();
        s.mobile_entity.overworld_tile(pk)
    };
    let Some((px, pz)) = player_tile else {
        return no_store_status(
            StatusCode::NOT_FOUND,
            json!({
                "found": false,
                "player_entity_id": pk.to_string(),
                "error": "player not on the overworld",
            }),
        )
        .into_response();
    };

    let Some(roads) = fleet.roads.as_ref() else {
        return no_store_status(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "roads cache not enabled"}),
        )
        .into_response();
    };
    let Some(handle) = roads.region_handle(region) else {
        return no_store_status(
            StatusCode::NOT_FOUND,
            json!({"found": false, "error": format!("region {region} has no resource map")}),
        )
        .into_response();
    };

    let half = (RESOURCE_WINDOW_WIDTH / 2) as i32;
    let origin_world = (px - half, pz - half);

    let body = {
        let grid = handle.grid.read();
        if !grid.ready {
            return (StatusCode::ACCEPTED, Vec::<u8>::new()).into_response();
        }
        let origin = region_origin(handle.region as u16);
        let origin_local = (origin_world.0 - origin.x, origin_world.1 - origin.z);
        let payload = grid.resource_map.window(origin_local, RESOURCE_WINDOW_WIDTH);
        let dict_version = grid.resource_map.dict_version();

        let mut body = Vec::with_capacity(24 + payload.len());
        body.extend_from_slice(b"BMR1");
        body.extend_from_slice(&1u16.to_le_bytes()); // format version
        body.extend_from_slice(&(RESOURCE_WINDOW_WIDTH as u16).to_le_bytes());
        body.extend_from_slice(&origin_world.0.to_le_bytes());
        body.extend_from_slice(&origin_world.1.to_le_bytes());
        body.extend_from_slice(&region.to_le_bytes());
        body.extend_from_slice(&dict_version.to_le_bytes());
        body.extend_from_slice(&payload);
        body
    };
    no_store_octets(body)
}

/// `GET /bitme/region/:region/resource-dictionary` — the resource_id ↔
/// 10-bit tile index map (plus the paving namespace — tile words with bit 14
/// set — as entries carrying `paving_type_id` and `paving: true`) plus
/// gamedata, so clients can expand `/bitme/session/:id/resources` windows.
/// Refetch when `dict_version` changes (indices are per-deploy, first-sight
/// order).
async fn bitme_region_resource_dictionary(State(fleet): State<Fleet>, Path(region): Path<String>) -> Response {
    let Ok(region) = region.parse::<u32>() else {
        return no_store_status(StatusCode::BAD_REQUEST, json!({"error": "region must be a u32"})).into_response();
    };
    let Some(roads) = fleet.roads.as_ref() else {
        return no_store_status(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "roads cache not enabled"}),
        )
        .into_response();
    };
    let Some(handle) = roads.region_handle(region) else {
        return no_store_status(
            StatusCode::NOT_FOUND,
            json!({"found": false, "error": format!("unknown region {region}")}),
        )
        .into_response();
    };

    let (dict_version, indexed_ids, indexed_paving_ids) = {
        let grid = handle.grid.read();
        if !grid.ready {
            return (StatusCode::ACCEPTED, Vec::<u8>::new()).into_response();
        }
        (
            grid.resource_map.dict_version(),
            grid.resource_map
                .dict_ids()
                .iter()
                .enumerate()
                .skip(1) // index 0 = empty-tile sentinel
                .map(|(index, id)| (index as u32, *id))
                .collect::<Vec<_>>(),
            grid.resource_map
                .dict_paving_ids()
                .iter()
                .enumerate()
                .skip(1)
                .map(|(index, id)| (index as u32, *id))
                .collect::<Vec<_>>(),
        )
    };

    // Gamedata descriptors live in the region store; a missing/not-ready
    // shard degrades to nulls rather than failing the whole dictionary.
    let descs: hashbrown::HashMap<i32, (String, i32, f32, f32)> = fleet
        .shards
        .iter()
        .find(|s| s.region == region)
        .map(|shard| {
            let s = shard.store.read();
            if !s.ready {
                return hashbrown::HashMap::new();
            }
            indexed_ids
                .iter()
                .filter_map(|(_, id)| {
                    s.resource_desc.get(*id).map(|d| {
                        (
                            *id,
                            (d.name.clone(), d.max_health, d.despawn_time, d.scheduled_respawn_time),
                        )
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    // Paving names come from the global paving_tile_desc catalog; ids the
    // catalog has not streamed yet degrade to null names.
    let paving_names: hashbrown::HashMap<i32, String> = roads
        .catalog
        .read()
        .paving
        .iter()
        .map(|p| (p.id, p.name.clone()))
        .collect();

    let mut entries: Vec<Value> = indexed_ids
        .iter()
        .map(|(index, id)| {
            let d = descs.get(id);
            json!({
                "index": index,
                "resource_id": id,
                "name": d.map(|(name, ..)| name.as_str()),
                "harvestable": is_harvestable_resource_id(*id),
                "max_health": d.map(|(_, max_health, ..)| *max_health),
                "despawn_time_secs": d.map(|(_, _, despawn, _)| *despawn),
                "respawn_time_secs": d.map(|(_, _, _, respawn)| *respawn),
                "paving": false,
            })
        })
        .collect();
    // Paving namespace: indices are separate from resource indices; window
    // tile words with bit 14 set refer to these entries.
    entries.extend(indexed_paving_ids.iter().map(|(index, id)| {
        json!({
            "index": index,
            "paving_type_id": id,
            "name": paving_names.get(id).map(String::as_str),
            "paving": true,
        })
    }));

    no_store_json(json!({
        "region": region,
        "ready": true,
        "dict_version": dict_version,
        "entries": entries,
    }))
    .into_response()
}

fn build_session_snapshot(fleet: &Fleet, s: &RegionStore, pk: u64) -> Value {
    let region = s.region;
    let username = s
        .player_username
        .find(pk)
        .map(|us| s.player_username.username[us as usize].to_string());

    // -- position ------------------------------------------------------------
    let mut position = Value::Null;
    let mut player_tile: Option<(i32, i32)> = None;
    if let Some(slot) = s.mobile_entity.find(pk) {
        let i = slot as usize;
        player_tile = s.mobile_entity.overworld_tile(pk);
        let now_ms = crate::bitme::now_unix_ms();
        position = json!({
            "world_x": s.mobile_entity.location_x[i] as f64 / 1000.0,
            "world_z": s.mobile_entity.location_z[i] as f64 / 1000.0,
            "tile_x": s.mobile_entity.location_x[i].div_euclid(1000),
            "tile_z": s.mobile_entity.location_z[i].div_euclid(1000),
            "destination_world_x": s.mobile_entity.destination_x[i] as f64 / 1000.0,
            "destination_world_z": s.mobile_entity.destination_z[i] as f64 / 1000.0,
            "dimension": s.mobile_entity.dimension[i],
            "is_walking": s.mobile_entity.is_walking[i],
            "timestamp_ms": s.mobile_entity.timestamp_ms[i],
            "age_ms": (now_ms - s.mobile_entity.timestamp_ms[i] as i64).max(0),
        });
    }

    // -- claim at the player's tile ------------------------------------------
    let claim = player_tile.and_then(|(x, z)| claim_at(&fleet.roads, s, region, x, z));
    let claim_entity_id: Option<u64> = claim
        .as_ref()
        .and_then(|c| c["entity_id"].as_str())
        .and_then(|s| s.parse::<u64>().ok());

    // -- stamina ---------------------------------------------------------------
    let stat_idx = s.character_stat_desc.stat_indices();
    let stamina = s.stamina.stamina_of(pk).map(|current| {
        json!({
            "current": current,
            "max": s.character_stats.value_at(pk, stat_idx.max_stamina),
            "max_health": s.character_stats.value_at(pk, stat_idx.max_health),
            "last_decrease_at": s.stamina.last_decrease_secs(pk).map(format_rfc3339_secs),
        })
    });

    // -- buffs (live entries only) ---------------------------------------------
    let buffs: Vec<Value> = s
        .active_buff
        .buffs_of(pk)
        .map(|entries| {
            entries
                .iter()
                .filter(|b| crate::store::active_buff::is_live_buff(b))
                .map(|b| {
                    json!({
                        "buff_id": b.buff_id,
                        "start_timestamp": b.start_timestamp,
                        "duration": b.duration,
                        "values": b.values,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    // -- actions -----------------------------------------------------------------
    // Primary target: the acted-on resource matters most for Bit-Me, so an
    // Extract target wins; a Base-layer target (crafting station) is the
    // fallback. UpperBody-only targets (emotes) are not surfaced as `target`.
    // The winning action's `recipe_id` rides along: joined against
    // `extraction_recipe_desc` it identifies the resource being harvested
    // as a fallback for entities the resource map has not seen.
    let mut action_targets: Vec<u64> = Vec::new();
    let mut extract_target: Option<(u64, Option<i32>)> = None;
    let mut base_target: Option<(u64, Option<i32>)> = None;
    let actions: Vec<Value> = s
        .player_action
        .by_entity(pk)
        .iter()
        .map(|&slot| {
            let i = slot as usize;
            let target = s.player_action.target[i];
            if let Some(t) = target {
                action_targets.push(t);
                let recipe = s.player_action.recipe_id[i];
                if s.player_action.action_type[i] == "Extract" && extract_target.is_none() {
                    extract_target = Some((t, recipe));
                }
                if s.player_action.layer[i] == "Base" && base_target.is_none() {
                    base_target = Some((t, recipe));
                }
            }
            json!({
                "auto_id": s.player_action.auto_id[i].to_string(),
                "action_type": s.player_action.action_type[i],
                "layer": s.player_action.layer[i],
                "start_time_ms": s.player_action.start_time_ms[i],
                "duration_ms": s.player_action.duration_ms[i],
                "ends_at_ms": s.player_action.start_time_ms[i].saturating_add(s.player_action.duration_ms[i]),
                "target_entity_id": target.map(|t| t.to_string()),
                "recipe_id": s.player_action.recipe_id[i],
                "last_action_result": s.player_action.last_action_result[i],
                "client_cancel": s.player_action.client_cancel[i],
            })
        })
        .collect();
    let (primary_target, primary_recipe_id) = extract_target
        .or(base_target)
        .map(|(t, recipe)| (Some(t), recipe))
        .unwrap_or((None, None));

    // Target health tracking: register what we see, report what we know.
    fleet.bitme.note_targets(action_targets.iter().copied());

    // -- target enrichment --------------------------------------------------------
    let target = primary_target.map(|t| build_target(fleet, s, t, primary_recipe_id));

    // -- watched spawns (citric + growth-chain), scoped to the player's area ---
    // Only located (overworld) spawns can be scoped; interior-dimension
    // spawn churn (dungeon ore depletion flips) is dropped. The scope is the
    // player's claim when inside one, otherwise the unclaimed wilderness.
    let spawns: Vec<Value> = fleet
        .bitme
        .spawns(region)
        .iter()
        .filter_map(|spawn| {
            let tile = spawn_tile(&fleet.roads, region, spawn.entity_id)?;
            let spawn_claim = claim_entity_at(&fleet.roads, region, tile.0, tile.1);
            if spawn_claim != claim_entity_id {
                return None;
            }
            let desc = s.resource_desc.get(spawn.resource_id);
            let despawn_secs = desc.map(|d| d.despawn_time).unwrap_or(0.0);
            let expires_at_ms = if despawn_secs > 0.0 {
                json!(spawn.at_unix_ms + (despawn_secs * 1000.0) as i64)
            } else {
                Value::Null
            };
            let health = fleet.bitme.target_health(spawn.entity_id);
            Some(json!({
                "entity_id": spawn.entity_id.to_string(),
                "resource_id": spawn.resource_id,
                "name": desc.map(|d| d.name.clone()),
                "health": health,
                "max_health": desc.map(|d| d.max_health),
                "location": json!({"tile_x": tile.0, "tile_z": tile.1}),
                "spawned_at_ms": spawn.at_unix_ms,
                "expires_at_ms": expires_at_ms,
                "growth_ends_at_ms": growth_window_end_ms(s, spawn.entity_id),
            }))
        })
        .collect();

    json!({
        "found": true,
        "player_entity_id": pk.to_string(),
        "username": username,
        "signed_in": s.player_state.find(pk).map(|slot| s.player_state.signed_in[slot as usize]),
        "region": region,
        "position": position,
        "claim": claim,
        "stamina": stamina,
        "buffs": buffs,
        "actions": actions,
        "target": target,
        "activity_spawns": spawns,
        "server_time_ms": crate::bitme::now_unix_ms(),
    })
}

/// Enrich one target entity: resource identity (roads resource tile map →
/// spawn log → hexite store → extraction recipe), gamedata descriptors,
/// tracked health. The resource map carries every resource type; the
/// extraction-recipe join remains the last-resort fallback for entities the
/// map has not seen (e.g. targets in regions without a roads grid).
fn build_target(fleet: &Fleet, s: &RegionStore, target: u64, recipe_id: Option<i32>) -> Value {
    let region = s.region;
    let mut resource_id = fleet
        .roads
        .as_ref()
        .and_then(|roads| roads.region_handle(region))
        .and_then(|h| {
            let grid = h.grid.read();
            grid.resource_map.resource_of(target).map(|(rid, _)| rid)
        });
    if resource_id.is_none() {
        // Citric bushes and other watched spawns land in the session log.
        resource_id = fleet
            .bitme
            .spawns(region)
            .iter()
            .find(|spawn| spawn.entity_id == target)
            .map(|spawn| spawn.resource_id);
    }
    if resource_id.is_none() {
        resource_id = s
            .resource
            .find(target)
            .map(|slot| s.resource.resource_id[slot as usize]);
    }
    if resource_id.is_none() {
        resource_id = recipe_id.and_then(|rid| s.extraction_recipe.resource_for_recipe(rid));
    }

    let desc = resource_id.and_then(|rid| s.resource_desc.get(rid));
    let location = fleet
        .roads
        .as_ref()
        .and_then(|roads| roads.region_handle(region))
        .and_then(|h| {
            let grid = h.grid.read();
            grid.join.location_by_entity.get(&target).copied()
        });
    let health = fleet.bitme.target_health(target);

    json!({
        "entity_id": target.to_string(),
        "resource_id": resource_id,
        "name": desc.map(|d| d.name.clone()),
        "health": health,
        "max_health": desc.map(|d| d.max_health),
        "despawn_time_secs": desc.map(|d| d.despawn_time),
        "respawn_time_secs": desc.map(|d| d.scheduled_respawn_time),
        "growth_ends_at_ms": growth_window_end_ms(s, target),
        "location": location.map(|(x, z)| json!({"tile_x": x, "tile_z": z})),
    })
}

/// Absolute unix-ms when the entity's current growth stage ends (growth-stage
/// despawn/wither/citric-transition — e.g. T2 event berry bushes: 600 s
/// Bountiful window, 30 s Citric window), from the authoritative
/// `resource_growth_timer.scheduled_at` reducer clock, falling back to the
/// legacy `growth_state.end_timestamp`. Null when the entity has no live
/// growth timer (plain harvestables, depleted nodes awaiting a respawn
/// row, …). Same clock `/deposits` reports as hexite `respawn_at`.
fn growth_window_end_ms(s: &RegionStore, entity_id: u64) -> Value {
    s.growth_timer
        .scheduled_at_micros(entity_id)
        .or_else(|| s.growth.end_timestamp_micros(entity_id))
        .map(|micros| json!(micros.div_euclid(1_000)))
        .unwrap_or(Value::Null)
}

/// Claim under a world tile: roads overlay claim index → claim summary from
/// the region store. `None` when roads are off or the tile is unclaimed.
fn claim_at(fleet_roads: &Option<Arc<RoadsFleet>>, s: &RegionStore, region: u32, x: i32, z: i32) -> Option<Value> {
    let claim_entity_id = claim_entity_at(fleet_roads, region, x, z)?;
    if claim_entity_id == 0 {
        return None;
    }
    let slot = s.claim.find(claim_entity_id)?;
    let i = slot as usize;
    Some(json!({
        "entity_id": claim_entity_id.to_string(),
        "name": s.claim.name[i],
        "owner_player_entity_id": s.claim.owner_player_entity_id[i].to_string(),
        "neutral": s.claim.neutral[i],
    }))
}

fn claim_entity_at(fleet_roads: &Option<Arc<RoadsFleet>>, region: u32, x: i32, z: i32) -> Option<u64> {
    let roads = fleet_roads.as_ref()?;
    let handle = roads.region_handle(region)?;
    let grid = handle.grid.read();
    let (lx, lz) = world_to_local(handle.region as u16, x, z)?;
    let cell = grid.overlay.get(lx, lz);
    let idx = get_claim_index(cell);
    if idx == 0 {
        return None;
    }
    grid.claim_index.claim_table().get(idx as usize).copied()
}

/// Spawn entity → overworld world tile, via the roads join map. Spawn-log
/// entries belong to one region, so only that region's grid is consulted.
fn spawn_tile(fleet_roads: &Option<Arc<RoadsFleet>>, region: u32, entity_id: u64) -> Option<(i32, i32)> {
    let handle = fleet_roads.as_ref()?.region_handle(region)?;
    let grid = handle.grid.read();
    grid.join.location_by_entity.get(&entity_id).copied()
}

fn format_rfc3339_secs(secs: i64) -> String {
    format_rfc3339_millis(secs.saturating_mul(1_000_000))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitme::BitmeHub;
    use crate::decode::{ExtractionRecipeRow, MobileEntityRow, ResourceDescRow};
    use crate::interest::InterestHub;
    use crate::roads::catalog::{GlobalRoadsCatalog, RoadsFleet};
    use crate::roads::store::{RoadsRegionGrid, RoadsRegionHandle};
    use crate::shard::ShardHandle;
    use axum::body::Body;
    use axum::http::Request;
    use parking_lot::RwLock;
    use std::sync::atomic::AtomicBool;
    use tower::ServiceExt;

    /// Player 42 standing at region-7 world tile (7690, 7700) — local
    /// (10, 20), on top of a mushroom. `dimension: 1` is the overworld.
    fn fleet_with_player(dimension: u32, with_roads: bool) -> Fleet {
        let mut store = RegionStore::empty(7);
        store.ready = true;
        store.mobile_entity.upsert(MobileEntityRow {
            entity_id: 42,
            timestamp_ms: 1_788_628_340_546,
            location_x: 7_690_000,
            location_z: 7_700_000,
            destination_x: 7_690_000,
            destination_z: 7_700_000,
            dimension,
            is_walking: false,
        });
        let shard = ShardHandle {
            region: 7,
            store: Arc::new(RwLock::new(store)),
        };
        let roads = if with_roads {
            let roads = Arc::new(RoadsFleet::new(Arc::new(RwLock::new(GlobalRoadsCatalog::new()))));
            let handle = Arc::new(RoadsRegionHandle {
                region: 7,
                grid: Arc::new(RwLock::new(RoadsRegionGrid::new(7))),
            });
            {
                let mut grid = handle.grid.write();
                grid.resource_map.note_desc(74, &[]);
                grid.resource_map.upsert(100, 74, 2, Some((7690, 7700)));
                grid.resource_map.stamp_paving(7691, 7700, 59838);
                grid.mark_ready();
            }
            roads.catalog.write().paving.push(crate::roads::decode::PavingDescRow {
                id: 59838,
                name: "Cobblestone Road".into(),
                paving_duration: 0.0,
                tier: 0,
                input_cargo_id: 0,
                consumed: vec![],
            });
            roads.push_region(handle);
            Some(roads)
        } else {
            None
        };
        Fleet {
            shards: vec![Arc::new(shard)],
            memory_pressure: Arc::new(AtomicBool::new(false)),
            interest: InterestHub::new(),
            roads,
            bitme: BitmeHub::new(),
        }
    }

    #[tokio::test]
    async fn resource_window_serves_packed_grid() {
        let fleet = fleet_with_player(1, true);
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/session/42/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["content-type"], "application/octet-stream");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.len(), 24 + 400 * 400 * 2);
        assert_eq!(&body[0..4], b"BMR1");
        assert_eq!(u16::from_le_bytes(body[4..6].try_into().unwrap()), 1); // version
        assert_eq!(u16::from_le_bytes(body[6..8].try_into().unwrap()), 400); // width
        assert_eq!(i32::from_le_bytes(body[8..12].try_into().unwrap()), 7490);
        assert_eq!(i32::from_le_bytes(body[12..16].try_into().unwrap()), 7500);
        assert_eq!(u32::from_le_bytes(body[16..20].try_into().unwrap()), 7);
        assert_ne!(u32::from_le_bytes(body[20..24].try_into().unwrap()), 0); // dict_version

        // The player's own tile carries resource 74 (dictionary index 1),
        // direction 2, origin flag set.
        let off = 24 + (200 * 400 + 200) * 2;
        let word = u16::from_le_bytes(body[off..off + 2].try_into().unwrap());
        assert_eq!(word & 0x03FF, 1);
        assert_eq!(word >> 11, 2);
        assert_ne!(word & (1 << 10), 0);

        // The paved tile one column east carries bit 14 + paving index 1.
        let off = 24 + (200 * 400 + 201) * 2;
        let word = u16::from_le_bytes(body[off..off + 2].try_into().unwrap());
        assert_ne!(word & (1 << 14), 0, "paving flag set");
        assert_eq!(word & 0x03FF, 1, "paving index (separate namespace)");
        assert_eq!((word >> 11) & 7, 0, "paving has no direction");
    }

    #[tokio::test]
    async fn resource_window_202_while_grid_loading() {
        let fleet = fleet_with_player(1, true);
        fleet
            .roads
            .as_ref()
            .unwrap()
            .region_handle(7)
            .unwrap()
            .grid
            .write()
            .ready = false;
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/session/42/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn resource_window_404_for_interior_player() {
        let fleet = fleet_with_player(2, true);
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/session/42/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["found"], false);
        assert_eq!(v["error"], "player not on the overworld");
    }

    #[tokio::test]
    async fn resource_window_404_for_unknown_player() {
        let fleet = fleet_with_player(1, true);
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/session/43/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn resource_window_503_without_roads() {
        let fleet = fleet_with_player(1, false);
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/session/42/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn resource_dictionary_lists_entries_with_gamedata() {
        let fleet = fleet_with_player(1, true);
        {
            let shard = &fleet.shards[0];
            let mut s = shard.store.write();
            s.resource_desc.upsert(ResourceDescRow {
                id: 74,
                name: "Button Mushrooms".into(),
                max_health: 200,
                despawn_time: 0.0,
                on_destroy_yield_resource_id: 0,
                scheduled_respawn_time: 600.0,
            });
        }
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/bitme/region/7/resource-dictionary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["region"], 7);
        assert_eq!(v["ready"], true);
        assert_ne!(v["dict_version"], 0);
        let entry = &v["entries"][0];
        assert_eq!(entry["index"], 1);
        assert_eq!(entry["resource_id"], 74);
        assert_eq!(entry["name"], "Button Mushrooms");
        // Forageable: the client filters on this instead of a server allowlist.
        assert_eq!(entry["harvestable"], false);
        assert_eq!(entry["max_health"], 200);
        assert_eq!(entry["respawn_time_secs"], 600.0);
        assert_eq!(entry["paving"], false);

        // Paving namespace: separate index space, flagged on the entry,
        // named from the global paving catalog.
        let paving_entry = v["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["paving"] == json!(true))
            .expect("paving entry");
        assert_eq!(paving_entry["paving_type_id"], 59838);
        assert_eq!(paving_entry["name"], "Cobblestone Road");
        // Index namespaces are independent — both start at 1; the tile
        // word's bit 14 is what tells them apart on the wire.

        // Unknown region → 404; garbage region id → 400.
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/bitme/region/99/resource-dictionary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// 2026-09 client bug regression: harvesting mushrooms must resolve
    /// identity even without a roads grid / map sighting — the Extract
    /// action's `recipe_id` joins against `extraction_recipe_desc`.
    #[test]
    fn target_identity_falls_back_to_extraction_recipe() {
        let fleet = Fleet {
            shards: vec![],
            memory_pressure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            interest: InterestHub::new(),
            roads: None,
            bitme: BitmeHub::new(),
        };
        let mut s = RegionStore::empty(7);
        s.extraction_recipe.upsert(ExtractionRecipeRow {
            id: 424_242,
            resource_id: 74, // Button Mushrooms
        });
        s.resource_desc.upsert(ResourceDescRow {
            id: 74,
            name: "Button Mushrooms".into(),
            max_health: 200,
            despawn_time: 0.0,
            on_destroy_yield_resource_id: 0,
            scheduled_respawn_time: 600.0,
        });

        let t = build_target(&fleet, &s, 5001, Some(424_242));
        assert_eq!(t["resource_id"], json!(74));
        assert_eq!(t["name"], json!("Button Mushrooms"));
        assert_eq!(t["max_health"], json!(200));
        assert_eq!(t["respawn_time_secs"], json!(600.0));

        // Without a recipe there is nothing to join — identity stays null,
        // never a wrong guess.
        let t = build_target(&fleet, &s, 5001, None);
        assert_eq!(t["resource_id"], json!(null));
    }

    /// 2026-09 client bug regression: T2 event berry bushes carry
    /// `despawn_time = 0` but the game client still shows a life countdown
    /// ("8m") — the authoritative clock is the entity's
    /// `resource_growth_timer.scheduled_at` (Bountiful 600 s → Citric,
    /// Citric 30 s → Withering). The target must surface it as
    /// `growth_ends_at_ms`, never fabricating one when no timer exists.
    #[test]
    fn growth_timer_surfaces_as_growth_ends_at_ms() {
        use crate::decode::ResourceGrowthTimerRow;

        let fleet = Fleet {
            shards: vec![],
            memory_pressure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            interest: InterestHub::new(),
            roads: None,
            bitme: BitmeHub::new(),
        };
        let mut s = RegionStore::empty(7);
        s.resource_desc.upsert(ResourceDescRow {
            id: 353_689_546, // Giant Bountiful Savory Berry Bush (T2 event)
            name: "Giant Bountiful Savory Berry Bush".into(),
            max_health: 500,
            despawn_time: 0.0, // static gamedata has no despawn window
            on_destroy_yield_resource_id: 0,
            scheduled_respawn_time: 0.0,
        });
        // Extract actions carry recipe_id; joining it resolves bush identity
        // (with no roads grid the map can't help; the recipe join resolves).
        s.extraction_recipe.upsert(ExtractionRecipeRow {
            id: 186_875,
            resource_id: 353_689_546,
        });

        // No timer row yet → stays null, never fabricated.
        let t = build_target(&fleet, &s, 9001, Some(186_875));
        assert_eq!(t["growth_ends_at_ms"], json!(null));
        assert_eq!(t["despawn_time_secs"], json!(0.0));

        // A live growth timer (spawned_at 1788663648856 + 600 s window).
        s.growth_timer.upsert(ResourceGrowthTimerRow {
            entity_id: 9001,
            scheduled_at_micros: Some(1_788_664_248_840_400),
            growth_recipe_id: 1_868_752_661,
        });
        let t = build_target(&fleet, &s, 9001, Some(186_875));
        assert_eq!(t["growth_ends_at_ms"], json!(1_788_664_248_840i64));
        assert_eq!(t["despawn_time_secs"], json!(0.0));

        // Legacy fallback: regions still writing growth_state continue to
        // surface a window when the timer table has no row.
        let mut legacy = RegionStore::empty(7);
        legacy.growth.upsert(crate::decode::GrowthRow {
            entity_id: 9001,
            end_timestamp_micros: 1_788_664_248_840_000,
            growth_recipe_id: 1_868_752_661,
        });
        let t = build_target(&fleet, &legacy, 9001, None);
        assert_eq!(t["growth_ends_at_ms"], json!(1_788_664_248_840i64));
    }
}
