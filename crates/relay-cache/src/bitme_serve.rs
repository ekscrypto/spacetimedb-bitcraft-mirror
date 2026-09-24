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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::roads::coords::{region_origin, small_to_super, super_center_tile, world_to_local, world_to_region};
use crate::roads::grid::get_claim_index;
use crate::roads::resource_map::is_harvestable_resource_id;
use crate::roads::store::RoadsRegionHandle;
use crate::roads::watch::WatchFrame;
use crate::roads::RoadsFleet;
use crate::serve::{format_rfc3339_millis, no_store_json, no_store_octets, no_store_status, Fleet};
use crate::store::RegionStore;

/// Window side in tiles for the resource-window endpoints (the anchor tile
/// sits at the center, `(width/2, width/2)`).
const RESOURCE_WINDOW_WIDTH: usize = 400;

pub fn bitme_routes() -> axum::Router<Fleet> {
    axum::Router::new()
        .route("/bitme/resolve", get(bitme_resolve))
        .route("/bitme/session/:entity_id", get(bitme_session))
        .route("/bitme/session/:entity_id/resources", get(bitme_session_resources))
        .route(
            "/bitme/session/:entity_id/resources/ws",
            get(bitme_session_resources_ws),
        )
        .route("/bitme/world/:x/:z/resources", get(bitme_world_resources))
        .route("/bitme/world/:x/:z/elevation", get(bitme_world_elevation))
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

    resource_window_response(&fleet, region, (px, pz))
}

/// `GET /bitme/world/:x/:z/resources` — the same packed 400×400 resource
/// window as `/bitme/session/:id/resources`, but anchored at explicit world
/// tile coordinates instead of a player. The region is derived from the
/// coordinates; window cells that fall outside it are zero (no cross-region
/// stitching). Nothing is player-scoped, so no session side effects.
async fn bitme_world_resources(State(fleet): State<Fleet>, Path((x, z)): Path<(String, String)>) -> Response {
    match resolve_world_anchor(&x, &z) {
        Ok((x, z, region)) => resource_window_response(&fleet, region, (x, z)),
        Err(resp) => resp,
    }
}

/// `GET /bitme/world/:x/:z/elevation` — the super-hex terrain plane covering
/// the same 400×400 small-tile window as `/bitme/world/:x/:z/resources`: one
/// packed u64 per super-hex (`pack_terrain`: elev | original<<16 |
/// water_level<<32 | water_body_type<<48), sliced straight from the resident
/// terrain grid.
///
/// The super grid is the game's terrain lattice — a true hex grid at 3× the
/// tile scale in odd-r offset coordinates (`small_to_super`), *not*
/// rectangular 3×3 blocks of tile indices. Cell `(r, c)` is the super at
/// offset `(origin + (c, r))`; its center tile is `(3X + (Z&1), 3Z)` (the
/// header origin names cell (0,0)'s center tile), it exclusively owns the
/// 7-tile flower around that center, and the corner tiles where three
/// supers meet blend all three (see BITME-API.md §6). The covering window is
/// padded one super on every side so edge corner tiles can still blend.
/// Because the odd-r shear makes the cover's width vary with the window's
/// row alignment (134–137 columns × 136 rows), the header carries explicit
/// width/height. Cells outside the region are zero; `generation` mirrors
/// the grid's update counter (terraform bumps it), so clients can refetch
/// on change.
async fn bitme_world_elevation(State(fleet): State<Fleet>, Path((x, z)): Path<(String, String)>) -> Response {
    match resolve_world_anchor(&x, &z) {
        Ok((x, z, region)) => elevation_window_response(&fleet, region, (x, z)),
        Err(resp) => resp,
    }
}

/// Shared `:x`/`:z` resolution for the `/bitme/world/…` endpoints: parse as
/// i32 world tile coordinates and pick the owning region. Err carries the
/// response to return (400 bad integers, 404 outside the world grid).
// The Err is a fully-built HTTP response by design, not a value type.
#[allow(clippy::result_large_err)]
fn resolve_world_anchor(x: &str, z: &str) -> Result<(i32, i32, u32), Response> {
    let (Ok(x), Ok(z)) = (x.parse::<i32>(), z.parse::<i32>()) else {
        return Err(no_store_status(
            StatusCode::BAD_REQUEST,
            json!({"error": "x and z must be i32 world tile coordinates"}),
        )
        .into_response());
    };
    match world_to_region(x, z) {
        Some(region) => Ok((x, z, u32::from(region))),
        None => Err(no_store_status(
            StatusCode::NOT_FOUND,
            json!({
                "found": false,
                "x": x,
                "z": z,
                "error": "coordinates outside the known world grid",
            }),
        )
        .into_response()),
    }
}

/// Shared prologue of the window endpoints: 503 without the roads cache,
/// 404 for a region with no roads grid.
// The Err is a fully-built HTTP response by design, not a value type.
#[allow(clippy::result_large_err)]
fn roads_region(fleet: &Fleet, region: u32) -> Result<Arc<RoadsRegionHandle>, Response> {
    let Some(roads) = fleet.roads.as_ref() else {
        return Err(no_store_status(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "roads cache not enabled"}),
        )
        .into_response());
    };
    roads.region_handle(region).ok_or_else(|| {
        no_store_status(
            StatusCode::NOT_FOUND,
            json!({"found": false, "error": format!("region {region} has no resource map")}),
        )
        .into_response()
    })
}

/// Shared tail of the resource-window endpoints: resolve the roads region,
/// slice the 400×400 window centered on `center` from the dense tile map,
/// and pack it behind the `BMR1` header (24-byte LE header, then `width²`
/// u16 LE tile words — see `roads/resource_map.rs` for the bit layout).
/// 503 without the roads cache, 404 for a region with no map, empty 202
/// while the grid is still seeding.
fn resource_window_response(fleet: &Fleet, region: u32, center: (i32, i32)) -> Response {
    let handle = match roads_region(fleet, region) {
        Ok(handle) => handle,
        Err(resp) => return resp,
    };

    let half = (RESOURCE_WINDOW_WIDTH / 2) as i32;
    let origin_world = (center.0 - half, center.1 - half);

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
        body.extend_from_slice(&handle.region.to_le_bytes());
        body.extend_from_slice(&dict_version.to_le_bytes());
        body.extend_from_slice(&payload);
        body
    };
    no_store_octets(body)
}

/// Region-local super window covering the tile window
/// `x ∈ [lx0, lx0+width)`, `z ∈ [lz0, lz0+height)`: `(origin, width, height)`
/// in super odd-r offset coords, padded one super on every side so corner
/// tiles at the window edges can still blend all three of their supers.
///
/// A tile's super *column*, `small_to_super(x, z).0`, is periodic in `z`
/// with period 6 (the odd-r shear and the ÷3 rounding realign every 6
/// rows), and monotone in `x` — so sampling 6 rows at both `x` extremes
/// bounds the covering columns exactly. Super rows are simply
/// `round(z/3)`, monotone in `z`.
fn covering_super_window(lx0: i32, lz0: i32, width: i32, height: i32) -> ((i32, i32), usize, usize) {
    let mut x_min = i32::MAX;
    let mut x_max = i32::MIN;
    for dz in 0..height.min(6) {
        let z = lz0 + dz;
        for x in [lx0, lx0 + width - 1] {
            let (sx, _) = small_to_super(x, z);
            x_min = x_min.min(sx);
            x_max = x_max.max(sx);
        }
    }
    // Corner tiles blend supers up to one offset column/row outside the
    // owning cover on either side.
    x_min -= 1;
    x_max += 1;
    let z_min = small_to_super(lx0, lz0).1 - 1;
    let z_max = small_to_super(lx0, lz0 + height - 1).1 + 1;
    (
        (x_min, z_min),
        (x_max - x_min + 1) as usize,
        (z_max - z_min + 1) as usize,
    )
}

/// Elevation counterpart of [`resource_window_response`]: slice the
/// super-hex terrain cells covering the 400×400 window centered on
/// `center` ([`covering_super_window`]) and pack them behind the `BME1`
/// header (26-byte LE: magic, version 2, width, height, center-tile world
/// x/z, region, generation). Same 503/404/202 semantics.
fn elevation_window_response(fleet: &Fleet, region: u32, center: (i32, i32)) -> Response {
    let handle = match roads_region(fleet, region) {
        Ok(handle) => handle,
        Err(resp) => return resp,
    };

    let half = (RESOURCE_WINDOW_WIDTH / 2) as i32;
    let origin_world = (center.0 - half, center.1 - half);

    let body = {
        let grid = handle.grid.read();
        if !grid.ready {
            return (StatusCode::ACCEPTED, Vec::<u8>::new()).into_response();
        }
        let origin = region_origin(handle.region as u16);
        let (super_origin, width, height) = covering_super_window(
            origin_world.0 - origin.x,
            origin_world.1 - origin.z,
            RESOURCE_WINDOW_WIDTH as i32,
            RESOURCE_WINDOW_WIDTH as i32,
        );
        let payload = grid.terrain.window(super_origin, width, height);

        let mut body = Vec::with_capacity(26 + payload.len());
        body.extend_from_slice(b"BME1");
        body.extend_from_slice(&2u16.to_le_bytes()); // format version
        body.extend_from_slice(&(width as u16).to_le_bytes());
        body.extend_from_slice(&(height as u16).to_le_bytes());
        // Header origin: world coord of cell (0,0)'s center tile — the super
        // grid's odd-r shear means that is (3X + (Z&1), 3Z), not (3X, 3Z).
        let center0 = super_center_tile(super_origin.0, super_origin.1);
        body.extend_from_slice(&(origin.x + center0.0).to_le_bytes());
        body.extend_from_slice(&(origin.z + center0.1).to_le_bytes());
        body.extend_from_slice(&handle.region.to_le_bytes());
        // u32 keeps the field compact; generation is a small counter.
        body.extend_from_slice(&(grid.generation as u32).to_le_bytes());
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

// ---------------------------------------------------------------------------
// Resource change stream — `/bitme/session/:entity_id/resources/ws`
//
// Push channel for the compacted map: every live resource-tile change whose
// world tile falls within the player's 400×400 window is streamed as a BMD1
// binary frame. Server-side per-listener state is just the window anchor
// (`roads/watch.rs`); the client owns convergence — initial state, reconnect
// recovery, and resync recovery are all "re-fetch the BMR1 window" (§4/§5 of
// BITME-API.md). See §8 of that document for the full client contract.
// ---------------------------------------------------------------------------

/// Fleet-wide concurrent resource-stream sockets, shared with the
/// dim-buildings WS budget (both are cheap push channels).
const RESOURCE_WS_MAX_CONNECTIONS: u64 = 2048;
/// Application-level heartbeat (client treats ~15 s of silence as dead).
const RESOURCE_WS_HEARTBEAT: Duration = Duration::from_secs(5);
/// How often the server re-reads the player's tile to follow movement.
/// Events carry absolute world tiles, so a stale anchor only trims edge
/// events until the client's movement-triggered BMR1 refetch.
const RESOURCE_WS_ANCHOR_TICK: Duration = Duration::from_secs(1);
/// Consecutive anchor ticks with the player gone from every ready shard
/// before the stream gives up and closes (outlives upstream batch gaps;
/// a full region re-seed outlasts it and the client simply reconnects).
const RESOURCE_WS_GONE_AFTER: u32 = 60;
/// Max tile entries per BMD1 frame (u16 count field); larger fan-outs are
/// chunked into consecutive frames.
const BMD1_MAX_TILES: usize = u16::MAX as usize;

/// `GET /bitme/session/:entity_id/resources/ws` — WebSocket push of
/// resource tile changes within the player's window. Pre-upgrade errors
/// mirror `GET /bitme/session/:id/resources` exactly (400/404/503 JSON);
/// after the upgrade the server sends a `subscribed` ack, then binary
/// BMD1 delta frames, JSON control frames, and `{"ts":…}` heartbeats.
async fn bitme_session_resources_ws(
    ws: WebSocketUpgrade,
    State(fleet): State<Fleet>,
    Path(entity_id): Path<String>,
) -> Response {
    let Ok(pk) = entity_id.parse::<u64>() else {
        return no_store_status(StatusCode::BAD_REQUEST, json!({"error": "entity_id must be a u64"})).into_response();
    };
    // Connecting counts as session activity, same as the HTTP polls.
    fleet.bitme.touch_session(pk);
    let Some(conn) = fleet.interest.try_acquire_connection(RESOURCE_WS_MAX_CONNECTIONS) else {
        return no_store_status(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "too many resource-change streams"}),
        )
        .into_response();
    };
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
    let handle = match roads_region(&fleet, region) {
        Ok(handle) => handle,
        Err(resp) => return resp,
    };
    ws.on_upgrade(move |socket| async move {
        run_resource_stream(socket, fleet, conn, pk, region, (px, pz), handle).await;
    })
    .into_response()
}

/// Live player position across all ready shards (mobile_entity rows only —
/// home-region username rows are not a position; during an own-region
/// re-seed this misses, and the stream keeps its last anchor).
fn find_player_tile(fleet: &Fleet, pk: u64) -> Option<(u32, (i32, i32))> {
    for shard in &fleet.shards {
        let s = shard.store.read();
        if !s.ready {
            continue;
        }
        if let Some(tile) = s.mobile_entity.overworld_tile(pk) {
            return Some((s.region, tile));
        }
    }
    None
}

async fn run_resource_stream(
    socket: WebSocket,
    fleet: Fleet,
    _conn: crate::interest::ConnectionGuard,
    pk: u64,
    mut region: u32,
    anchor: (i32, i32),
    handle: Arc<RoadsRegionHandle>,
) {
    let (mut sink, mut source) = socket.split();
    let Some(fleet_roads) = fleet.roads.clone() else {
        return;
    };
    let (mut listener, mut rx) = fleet_roads.watch.register(&handle, pk, anchor);

    let dict_version = {
        let grid = handle.grid.read();
        if grid.ready {
            grid.resource_map.dict_version()
        } else {
            0
        }
    };
    let ack = json!({
        "type": "subscribed",
        "player_entity_id": pk.to_string(),
        "region": region,
        "anchor": {"x": anchor.0, "z": anchor.1},
        "width": RESOURCE_WINDOW_WIDTH,
        "dict_version": dict_version,
    });
    if !send_ws_text(&mut sink, &ack.to_string()).await {
        return;
    }

    let mut heartbeat = tokio::time::interval(RESOURCE_WS_HEARTBEAT);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await; // consume the immediate first tick
    let mut anchor_tick = tokio::time::interval(RESOURCE_WS_ANCHOR_TICK);
    anchor_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    anchor_tick.tick().await; // consume the immediate first tick
    let mut gone_streak: u32 = 0;

    tracing::info!(
        target: "relay_cache::bitme",
        player_entity_id = pk,
        region,
        "resource stream connected"
    );

    loop {
        tokio::select! {
            biased;
            frame = rx.recv() => {
                let Some(frame) = frame else { break };
                if !send_watch_frame(&mut sink, frame).await {
                    break;
                }
            }
            _ = anchor_tick.tick() => match find_player_tile(&fleet, pk) {
                Some((r, (x, z))) if r == region => {
                    gone_streak = 0;
                    listener.update_anchor(x, z);
                }
                Some((r, (x, z))) => {
                    gone_streak = 0;
                    // Crossed into another region: re-anchor there and tell
                    // the client (its window is elsewhere — refetch BMR1).
                    match roads_region(&fleet, r) {
                        Ok(new_handle) => {
                            let moved = json!({
                                "type": "moved",
                                "region": r,
                                "anchor": {"x": x, "z": z},
                            });
                            if !send_ws_text(&mut sink, &moved.to_string()).await {
                                break;
                            }
                            let (new_listener, new_rx) = fleet_roads.watch.register(&new_handle, pk, (x, z));
                            listener = new_listener;
                            rx = new_rx;
                            region = r;
                        }
                        Err(resp) => {
                            let _ = resp; // region without a roads grid: nothing to stream
                            break;
                        }
                    }
                }
                None => {
                    gone_streak += 1;
                    if gone_streak >= RESOURCE_WS_GONE_AFTER {
                        let gone = json!({
                            "type": "gone",
                            "player_entity_id": pk.to_string(),
                        });
                        let _ = send_ws_text(&mut sink, &gone.to_string()).await;
                        break;
                    }
                }
            },
            _ = heartbeat.tick() => {
                fleet.bitme.touch_session(pk);
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                if !send_ws_text(&mut sink, &format!("{{\"ts\":{ts}}}")).await {
                    break;
                }
            }
            msg = source.next() => match msg {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(Message::Ping(p))) => {
                    if sink.send(Message::Pong(p)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(_)) => {} // text/binary from client: ignored
            },
        }
        if listener.poisoned() {
            break;
        }
    }
    let _ = sink.send(Message::Close(None)).await;
    tracing::info!(
        target: "relay_cache::bitme",
        player_entity_id = pk,
        region,
        "resource stream closed"
    );
}

async fn send_ws_text(sink: &mut SplitSink<WebSocket, Message>, text: &str) -> bool {
    sink.send(Message::Text(text.to_owned())).await.is_ok()
}

/// One hub frame → socket message(s). Deltas pack as `BMD1` binary: magic,
/// u16 version, u32 region, u32 dict_version, u16 count, then `count ×`
/// (i32 world_x, i32 world_z, u16 word) — world tiles so frames are
/// anchor-independent. Resyncs stay JSON (hand-formatted, matching the
/// dim-buildings house style).
async fn send_watch_frame(sink: &mut SplitSink<WebSocket, Message>, frame: WatchFrame) -> bool {
    match frame {
        WatchFrame::Resync { region, reason } => {
            send_ws_text(
                sink,
                &format!("{{\"type\":\"resync\",\"region\":{region},\"reason\":\"{reason}\"}}"),
            )
            .await
        }
        WatchFrame::Delta {
            region,
            dict_version,
            tiles,
        } => {
            for chunk in tiles.chunks(BMD1_MAX_TILES) {
                let mut body = Vec::with_capacity(16 + chunk.len() * 10);
                body.extend_from_slice(b"BMD1");
                body.extend_from_slice(&1u16.to_le_bytes());
                body.extend_from_slice(&region.to_le_bytes());
                body.extend_from_slice(&dict_version.to_le_bytes());
                body.extend_from_slice(&(chunk.len() as u16).to_le_bytes());
                for &(x, z, word) in chunk {
                    body.extend_from_slice(&x.to_le_bytes());
                    body.extend_from_slice(&z.to_le_bytes());
                    body.extend_from_slice(&word.to_le_bytes());
                }
                if sink.send(Message::Binary(body)).await.is_err() {
                    return false;
                }
            }
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitme::BitmeHub;
    use crate::decode::{ExtractionRecipeRow, MobileEntityRow, ResourceDescRow};
    use crate::interest::InterestHub;
    use crate::roads::catalog::{GlobalRoadsCatalog, RoadsFleet};
    use crate::roads::coords::{small_corner_supers, small_is_corner};
    use crate::roads::grid::{pack_terrain, unpack_terrain_water};
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
                // Terrain under the player: local small (10, 20) → super
                // (3, 7) under the official axial-rounding lattice.
                grid.terrain.set(3, 7, pack_terrain(120, 100, 50, 2));
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
        // effective rotation 1 (raw direction_index 2 / 2), origin flag set.
        let off = 24 + (200 * 400 + 200) * 2;
        let word = u16::from_le_bytes(body[off..off + 2].try_into().unwrap());
        assert_eq!(word & 0x03FF, 1);
        assert_eq!(word >> 11, 1);
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
    async fn world_resource_window_matches_player_window() {
        let fleet = fleet_with_player(1, true);
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/bitme/world/7690/7700/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["content-type"], "application/octet-stream");
        let world = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();

        // (7690, 7700) is player 42's tile → byte-identical to the session
        // window anchored on the player.
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/session/42/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let session = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(world, session);

        // Header sanity: window origin is center − 200, region derived as 7.
        assert_eq!(world.len(), 24 + 400 * 400 * 2);
        assert_eq!(&world[0..4], b"BMR1");
        assert_eq!(i32::from_le_bytes(world[8..12].try_into().unwrap()), 7490);
        assert_eq!(i32::from_le_bytes(world[12..16].try_into().unwrap()), 7500);
        assert_eq!(u32::from_le_bytes(world[16..20].try_into().unwrap()), 7);
    }

    #[tokio::test]
    async fn world_resource_window_404_outside_grid() {
        let fleet = fleet_with_player(1, true);
        let app = bitme_routes().with_state(fleet);
        for uri in [
            "/bitme/world/-1/0/resources",
            "/bitme/world/0/-1/resources",
            "/bitme/world/38400/0/resources",
        ] {
            let resp = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{uri}");
        }
    }

    #[tokio::test]
    async fn world_resource_window_404_for_unmirrored_region() {
        // (100, 100) is inside the world grid (region 1), but the fixture
        // only mirrors region 7.
        let fleet = fleet_with_player(1, true);
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/world/100/100/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn world_resource_window_400_bad_coords() {
        let fleet = fleet_with_player(1, true);
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/world/abc/0/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn world_resource_window_503_without_roads() {
        let fleet = fleet_with_player(1, false);
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/world/7690/7700/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn world_resource_window_202_while_grid_loading() {
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
                    .uri("/bitme/world/7690/7700/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn world_elevation_window_serves_packed_terrain() {
        let fleet = fleet_with_player(1, true);
        let expected_generation = fleet
            .roads
            .as_ref()
            .unwrap()
            .region_handle(7)
            .unwrap()
            .grid
            .read()
            .generation;
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/world/7690/7700/elevation")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["content-type"], "application/octet-stream");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();

        // Covering supers for local tile window [−190, 210) × [−180, 220):
        // origin (−65, −61), 137 columns × 136 rows. The odd-r shear makes
        // the column count vary with the window's row alignment (134–137).
        let (origin, width, height) = covering_super_window(-190, -180, 400, 400);
        assert_eq!((origin, width, height), ((-65, -61), 137, 136));

        assert_eq!(body.len(), 26 + width * height * 8);
        assert_eq!(&body[0..4], b"BME1");
        assert_eq!(u16::from_le_bytes(body[4..6].try_into().unwrap()), 2); // version
        assert_eq!(u16::from_le_bytes(body[6..8].try_into().unwrap()), 137); // width
        assert_eq!(u16::from_le_bytes(body[8..10].try_into().unwrap()), 136); // height
                                                                              // Cell (0,0) = super (−65, −61); its center tile is (3·(−65)+1,
                                                                              // 3·(−61)) local → world (7680 − 194, 7680 − 183) — odd super rows
                                                                              // sit one tile right.
        assert_eq!(i32::from_le_bytes(body[10..14].try_into().unwrap()), 7486);
        assert_eq!(i32::from_le_bytes(body[14..18].try_into().unwrap()), 7497);
        assert_eq!(u32::from_le_bytes(body[18..22].try_into().unwrap()), 7);
        assert_eq!(
            u32::from_le_bytes(body[22..26].try_into().unwrap()),
            expected_generation as u32
        );

        let cell = |r: usize, c: usize| {
            u64::from_le_bytes(
                body[26 + (r * width + c) * 8..26 + (r * width + c) * 8 + 8]
                    .try_into()
                    .unwrap(),
            )
        };
        // Seeded super (3, 7) → row 7 − (−61) = 68, col 3 − (−65) = 68; all
        // four packed fields survive the roundtrip.
        let word = cell(68, 68);
        assert_eq!(word, pack_terrain(120, 100, 50, 2));
        assert_eq!(unpack_terrain_water(word), (120, 50));
        assert_eq!(((word >> 16) & 0xFFFF) as u16 as i16, 100); // original
        assert_eq!((word >> 48) as u8, 2); // water_body_type
                                           // Out-of-region corner supers are zero (window origin is negative).
        assert_eq!(cell(0, 0), 0);

        // Covering guarantee: every tile of the 400×400 window finds all of
        // its supers — the primary, plus the corner triple where three
        // supers meet — inside the served window.
        for lz in -180..220 {
            for lx in -190..210 {
                let supers = if small_is_corner(lx, lz) {
                    small_corner_supers(lx, lz).to_vec()
                } else {
                    vec![small_to_super(lx, lz)]
                };
                for (sx, sz) in supers {
                    let (dr, dc) = (sz - origin.1, sx - origin.0);
                    assert!(
                        dr >= 0 && (dr as usize) < height && dc >= 0 && (dc as usize) < width,
                        "tile ({lx},{lz}) super ({sx},{sz}) outside window {origin:?} {width}×{height}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn world_elevation_window_404_outside_grid() {
        let fleet = fleet_with_player(1, true);
        let app = bitme_routes().with_state(fleet);
        for uri in [
            "/bitme/world/-1/0/elevation",
            "/bitme/world/0/-1/elevation",
            "/bitme/world/38400/0/elevation",
        ] {
            let resp = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{uri}");
        }
    }

    #[tokio::test]
    async fn world_elevation_window_404_for_unmirrored_region() {
        let fleet = fleet_with_player(1, true);
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/world/100/100/elevation")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn world_elevation_window_400_bad_coords() {
        let fleet = fleet_with_player(1, true);
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/world/abc/0/elevation")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn world_elevation_window_503_without_roads() {
        let fleet = fleet_with_player(1, false);
        let app = bitme_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/bitme/world/7690/7700/elevation")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn world_elevation_window_202_while_grid_loading() {
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
                    .uri("/bitme/world/7690/7700/elevation")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
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

    // ---- resource change stream (`/bitme/session/:id/resources/ws`) ----

    /// Serve the bitme routes on an ephemeral loopback port. Pre-upgrade
    /// errors can only be exercised over a real connection: the
    /// `WebSocketUpgrade` extractor requires hyper's `OnUpgrade` state,
    /// which `tower::ServiceExt::oneshot` never injects.
    async fn spawn_bitme(fleet: Fleet) -> std::net::SocketAddr {
        let app = bitme_routes().with_state(fleet);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    }

    /// HTTP status a WS client gets when the upgrade is refused pre-handler.
    async fn ws_refused_status(addr: std::net::SocketAddr, path: &str) -> u16 {
        match tokio_tungstenite::connect_async(format!("ws://{addr}{path}")).await {
            Ok(_) => panic!("upgrade must be refused for {path}"),
            Err(e) => match e {
                tokio_tungstenite::tungstenite::Error::Http(resp) => resp.status().as_u16(),
                other => panic!("expected HTTP rejection for {path}, got {other:?}"),
            },
        }
    }

    #[tokio::test]
    async fn resource_ws_preupgrade_errors() {
        // Bad entity id.
        let addr = spawn_bitme(fleet_with_player(1, true)).await;
        assert_eq!(ws_refused_status(addr, "/bitme/session/abc/resources/ws").await, 400);

        // Unknown player.
        assert_eq!(ws_refused_status(addr, "/bitme/session/999/resources/ws").await, 404);

        // Roads cache disabled.
        let addr = spawn_bitme(fleet_with_player(1, false)).await;
        assert_eq!(ws_refused_status(addr, "/bitme/session/42/resources/ws").await, 503);

        // Player present but not on the overworld.
        let addr = spawn_bitme(fleet_with_player(2, true)).await;
        assert_eq!(ws_refused_status(addr, "/bitme/session/42/resources/ws").await, 404);
    }

    #[tokio::test]
    async fn resource_ws_streams_deltas_and_resync() {
        use tokio_tungstenite::tungstenite::Message as WsMessage;

        let fleet = fleet_with_player(1, true);
        let roads = fleet.roads.clone().expect("roads fixture");
        let addr = spawn_bitme(fleet).await;

        let (stream, _resp) = tokio_tungstenite::connect_async(format!("ws://{addr}/bitme/session/42/resources/ws"))
            .await
            .unwrap();
        let (mut write, mut read) = stream.split();

        // Ack: subscribed with the player's region and anchor.
        let msg = tokio::time::timeout(Duration::from_secs(5), read.next())
            .await
            .expect("ack within 5s")
            .unwrap()
            .unwrap();
        let WsMessage::Text(text) = msg else {
            panic!("ack must be text, got {msg:?}");
        };
        let ack: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(ack["type"], "subscribed");
        assert_eq!(ack["region"], 7);
        assert_eq!(ack["anchor"]["x"], 7690);
        assert_eq!(ack["anchor"]["z"], 7700);
        assert_eq!(ack["width"], 400);

        // One in-window change (the player's own tile, local (10, 20) in
        // region 7) and one far out of window — only the first is pushed.
        let near = (20u32 * 7680) + 10;
        let far = (7000u32 * 7680) + 7000;
        roads.watch.fanout(7, &[(near, 0x0205), (far, 0x0007)], 99);
        let msg = tokio::time::timeout(Duration::from_secs(5), read.next())
            .await
            .expect("delta within 5s")
            .unwrap()
            .unwrap();
        let WsMessage::Binary(data) = msg else {
            panic!("delta must be binary, got {msg:?}");
        };
        assert_eq!(&data[0..4], b"BMD1");
        assert_eq!(u16::from_le_bytes(data[4..6].try_into().unwrap()), 1); // version
        assert_eq!(u32::from_le_bytes(data[6..10].try_into().unwrap()), 7); // region
        assert_eq!(u32::from_le_bytes(data[10..14].try_into().unwrap()), 99); // dict_version
        assert_eq!(u16::from_le_bytes(data[14..16].try_into().unwrap()), 1); // count
        assert_eq!(i32::from_le_bytes(data[16..20].try_into().unwrap()), 7690);
        assert_eq!(i32::from_le_bytes(data[20..24].try_into().unwrap()), 7700);
        assert_eq!(u16::from_le_bytes(data[24..26].try_into().unwrap()), 0x0205);

        // Upstream re-seed → JSON resync frame.
        let handle = roads.region_handle(7).unwrap();
        roads.watch.on_grid_replaced(&handle, "reseed");
        let msg = tokio::time::timeout(Duration::from_secs(5), read.next())
            .await
            .expect("resync within 5s")
            .unwrap()
            .unwrap();
        let WsMessage::Text(text) = msg else {
            panic!("resync must be text, got {msg:?}");
        };
        let resync: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(resync["type"], "resync");
        assert_eq!(resync["reason"], "reseed");

        write.close().await.unwrap();
    }
}
