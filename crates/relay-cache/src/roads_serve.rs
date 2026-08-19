// SPDX-License-Identifier: MIT

//! HTTP handlers for `/roads/*`.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use prost::Message;

use crate::roads::catalog::RoadsFleet;
use crate::serve::Fleet;

mod roads_pb {
    include!(concat!(env!("OUT_DIR"), "/roads_cache.rs"));
}

mod bitcraft_roads_pb {
    include!(concat!(env!("OUT_DIR"), "/bitcraft.roads.rs"));
}

const PROTOBUF_MIME: &str = "application/x-protobuf";

pub fn roads_routes() -> axum::Router<Fleet> {
    use axum::routing::get;
    axum::Router::new()
        .route("/roads/health", get(roads_health))
        .route("/roads/regions", get(roads_regions))
        .route("/roads/paving-types", get(roads_paving_types))
        .route("/roads/terraform-recipes", get(roads_terraform_recipes))
        .route("/roads/region/{region}/map", get(roads_region_map))
}

fn require_roads(fleet: &Fleet) -> Result<&Arc<RoadsFleet>, Response> {
    fleet
        .roads
        .as_ref()
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::CONTENT_TYPE, "text/plain")],
                "roads cache not enabled",
            )
                .into_response()
        })
}

fn protobuf_response(status: StatusCode, bytes: Vec<u8>) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(PROTOBUF_MIME));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("private"));
    (status, headers, bytes).into_response()
}

async fn roads_health(State(fleet): State<Fleet>) -> Response {
    let Ok(roads) = require_roads(&fleet) else {
        return require_roads(&fleet).unwrap_err();
    };
    let h = roads.health();
    let msg = roads_pb::RoadsCacheHealth {
        ready: h.ready,
        regions_ready: h.regions_ready,
        regions_total: h.regions_total,
        total_memory_bytes: h.total_memory_bytes,
        regions: h
            .regions
            .into_iter()
            .map(|r| roads_pb::RegionRoadStatus {
                region: r.region,
                state: r.state,
                connected: r.connected,
                loaded_at_unix_ms: r.loaded_at_unix_ms,
                last_update_unix_ms: r.last_update_unix_ms,
                error: r.error,
                memory_bytes: r.memory_bytes,
                claim_count: r.claim_count,
                paved_tile_count: r.paved_tile_count,
                claim_tile_count: r.claim_tile_count,
            })
            .collect(),
    };
    protobuf_response(StatusCode::OK, msg.encode_to_vec())
}

async fn roads_regions(State(fleet): State<Fleet>) -> Response {
    let Ok(roads) = require_roads(&fleet) else {
        return require_roads(&fleet).unwrap_err();
    };
    let data = roads.regions_response();
    let msg = bitcraft_roads_pb::RegionsResponse {
        regions: data
            .regions
            .into_iter()
            .map(|r| bitcraft_roads_pb::Region {
                id: r.id,
                name: r.name,
                live: r.live,
                rx: r.rx,
                rz: r.rz,
                origin_x: r.origin_x,
                origin_z: r.origin_z,
            })
            .collect(),
        region_width_chunks: data.region_width_chunks,
        region_height_chunks: data.region_height_chunks,
    };
    protobuf_response(StatusCode::OK, msg.encode_to_vec())
}

async fn roads_paving_types(State(fleet): State<Fleet>) -> Response {
    let Ok(roads) = require_roads(&fleet) else {
        return require_roads(&fleet).unwrap_err();
    };
    let catalog = roads.catalog.read();
    let msg = bitcraft_roads_pb::PavingTypesResponse {
        types: catalog
            .paving
            .iter()
            .map(|p| bitcraft_roads_pb::PavingType {
                tile_type_id: p.id,
                name: p.name.clone(),
                paving_duration: p.paving_duration,
                tier: p.tier,
                input_cargo_id: p.input_cargo_id,
                consumed: p
                    .consumed
                    .iter()
                    .map(|(item_id, quantity, cargo)| bitcraft_roads_pb::MaterialStack {
                        item_id: *item_id,
                        quantity: *quantity,
                        cargo: *cargo,
                    })
                    .collect(),
            })
            .collect(),
    };
    protobuf_response(StatusCode::OK, msg.encode_to_vec())
}

async fn roads_terraform_recipes(State(fleet): State<Fleet>) -> Response {
    let Ok(roads) = require_roads(&fleet) else {
        return require_roads(&fleet).unwrap_err();
    };
    let catalog = roads.catalog.read();
    let msg = bitcraft_roads_pb::TerraformRecipesResponse {
        recipes: catalog
            .terraform
            .iter()
            .map(|r| bitcraft_roads_pb::TerraformRecipeRow {
                difference: r.difference as i32,
                actions_count: r.actions_count,
                stamina_per_action: r.stamina_per_action,
                time_per_action: r.time_per_action,
            })
            .collect(),
    };
    protobuf_response(StatusCode::OK, msg.encode_to_vec())
}

async fn roads_region_map(
    State(fleet): State<Fleet>,
    Path(region): Path<u32>,
    headers: HeaderMap,
) -> Response {
    let Ok(roads) = require_roads(&fleet) else {
        return require_roads(&fleet).unwrap_err();
    };
    let Some(handle) = roads.region_handle(region) else {
        return (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "text/plain")],
            format!("unknown region {region}"),
        )
            .into_response();
    };
    let grid = handle.grid.read();
    if !grid.ready {
        return protobuf_response(StatusCode::ACCEPTED, Vec::new());
    }
    let snap = grid.snapshot();
    if let Some(inm) = headers.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()) {
        if inm == snap.etag {
            return StatusCode::NOT_MODIFIED.into_response();
        }
    }
    let msg = roads_pb::RegionMapSnapshot {
        region: snap.region,
        generation: snap.generation,
        last_update_unix_ms: snap.last_update_unix_ms,
        origin_x: snap.origin_x,
        origin_z: snap.origin_z,
        claim_table: snap.claim_table,
        neutral_claim_ids: snap.neutral_claim_ids,
        terrain: snap.terrain,
        overlay: snap.overlay,
        etag: snap.etag.clone(),
    };
    let mut resp = protobuf_response(StatusCode::OK, msg.encode_to_vec());
    resp.headers_mut()
        .insert(header::ETAG, HeaderValue::from_str(&snap.etag).unwrap_or(HeaderValue::from_static("")));
    resp
}
