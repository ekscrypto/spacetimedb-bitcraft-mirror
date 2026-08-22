// SPDX-License-Identifier: MIT

//! HTTP handlers for `/roads/*`.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use prost::Message;

use crate::roads::catalog::RoadsFleet;
use crate::roads::harvestable::MAX_RESOURCE_QUERY_TILES;
use crate::serve::Fleet;

mod roads_pb {
    include!(concat!(env!("OUT_DIR"), "/roads_cache.rs"));
}

mod bitcraft_roads_pb {
    include!(concat!(env!("OUT_DIR"), "/bitcraft.roads.rs"));
}

const PROTOBUF_MIME: &str = "application/x-protobuf";

pub fn roads_routes() -> axum::Router<Fleet> {
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/roads/health", get(roads_health))
        .route("/roads/regions", get(roads_regions))
        .route("/roads/paving-types", get(roads_paving_types))
        .route("/roads/terraform-recipes", get(roads_terraform_recipes))
        .route("/roads/region/:region/map", get(roads_region_map))
        .route("/roads/region/:region/resources", post(roads_region_resources))
}

fn require_roads(fleet: &Fleet) -> Result<&Arc<RoadsFleet>, Response> {
    fleet.roads.as_ref().ok_or_else(|| {
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

async fn roads_region_map(State(fleet): State<Fleet>, Path(region): Path<u32>, headers: HeaderMap) -> Response {
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
    resp.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&snap.etag).unwrap_or(HeaderValue::from_static("")),
    );
    resp
}

async fn roads_region_resources(State(fleet): State<Fleet>, Path(region): Path<u32>, body: Bytes) -> Response {
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
    let query = match roads_pb::ResourceQuery::decode(body.as_ref()) {
        Ok(q) => q,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                [(header::CONTENT_TYPE, "text/plain")],
                "invalid ResourceQuery protobuf",
            )
                .into_response();
        }
    };
    if query.tiles.len() > MAX_RESOURCE_QUERY_TILES {
        return (
            StatusCode::BAD_REQUEST,
            [(header::CONTENT_TYPE, "text/plain")],
            format!("too many tiles ({} > {MAX_RESOURCE_QUERY_TILES})", query.tiles.len()),
        )
            .into_response();
    }
    let grid = handle.grid.read();
    if !grid.ready {
        return protobuf_response(StatusCode::ACCEPTED, Vec::new());
    }
    let mut nodes = Vec::new();
    let mut seen = hashbrown::HashSet::with_capacity(query.tiles.len());
    for tile in &query.tiles {
        if !seen.insert((tile.x, tile.z)) {
            continue;
        }
        for resource_id in grid.harvestable.resource_ids_on(tile.x, tile.z) {
            nodes.push(roads_pb::ResourceNode {
                x: tile.x,
                z: tile.z,
                resource_id,
            });
        }
    }
    protobuf_response(StatusCode::OK, roads_pb::ResourcesResponse { nodes }.encode_to_vec())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use parking_lot::RwLock;
    use prost::Message;
    use tower::ServiceExt;

    use super::roads_routes;
    use crate::interest::InterestHub;
    use crate::roads::catalog::{GlobalRoadsCatalog, RoadsFleet};
    use crate::roads::harvestable::MAX_RESOURCE_QUERY_TILES;
    use crate::roads::store::{RoadsRegionGrid, RoadsRegionHandle};
    use crate::serve::Fleet;

    fn test_fleet() -> Fleet {
        Fleet {
            shards: vec![],
            memory_pressure: Arc::new(AtomicBool::new(false)),
            interest: InterestHub::new(),
            roads: Some(Arc::new(RoadsFleet::new(Arc::new(RwLock::new(
                GlobalRoadsCatalog::new(),
            ))))),
        }
    }

    /// matchit 0.7 (axum 0.7) uses `:param`, not `{param}`. A mismatched path
    /// never hits the handler — axum returns an empty 404.
    #[tokio::test]
    async fn region_map_route_reaches_handler() {
        let app = roads_routes().with_state(test_fleet());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/roads/region/9/map")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"unknown region 9");
    }

    #[tokio::test]
    async fn region_resources_202_while_loading_then_sparse_lookup() {
        let fleet = test_fleet();
        let handle = Arc::new(RoadsRegionHandle {
            region: 9,
            grid: Arc::new(RwLock::new(RoadsRegionGrid::new(9))),
        });
        fleet.roads.as_ref().unwrap().push_region(handle.clone());

        let query = super::roads_pb::ResourceQuery {
            tiles: vec![super::roads_pb::Hex { x: 10, z: 20 }],
        };
        let app = roads_routes().with_state(fleet.clone());
        let loading = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/roads/region/9/resources")
                    .header("content-type", "application/x-protobuf")
                    .body(Body::from(query.encode_to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(loading.status(), StatusCode::ACCEPTED);

        {
            let mut grid = handle.grid.write();
            grid.harvestable.upsert(1, 5, 0, Some((10, 20)));
            grid.harvestable.upsert(2, 3, 0, Some((10, 20)));
            grid.mark_ready();
        }

        let app = roads_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/roads/region/9/resources")
                    .header("content-type", "application/x-protobuf")
                    .body(Body::from(query.encode_to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let decoded = super::roads_pb::ResourcesResponse::decode(body.as_ref()).unwrap();
        let mut ids: Vec<i32> = decoded.nodes.iter().map(|n| n.resource_id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![3, 5]);
        assert!(decoded.nodes.iter().all(|n| n.x == 10 && n.z == 20));
    }

    #[tokio::test]
    async fn region_resources_rejects_oversize() {
        let fleet = test_fleet();
        let handle = Arc::new(RoadsRegionHandle {
            region: 9,
            grid: Arc::new(RwLock::new(RoadsRegionGrid::new(9))),
        });
        handle.grid.write().mark_ready();
        fleet.roads.as_ref().unwrap().push_region(handle);

        let query = super::roads_pb::ResourceQuery {
            tiles: (0..=MAX_RESOURCE_QUERY_TILES as i32)
                .map(|i| super::roads_pb::Hex { x: i, z: 0 })
                .collect(),
        };
        let app = roads_routes().with_state(fleet);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/roads/region/9/resources")
                    .body(Body::from(query.encode_to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
