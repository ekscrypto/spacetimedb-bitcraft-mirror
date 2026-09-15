// SPDX-License-Identifier: MIT

//! Integration tests for the embedded feed (`--bitcraft-cache`) against the
//! real BitCraft module schema (v9), captured from the production mirror:
//! `tests/data/bitcraft-live-schema-v9.json`.
//!
//! These verify the properties that make the in-process path safe to swap in
//! for the WebSocket path:
//!
//! - the real schema's hot tables have fixed-width layouts, so the
//!   fixed-offset fast readers engage (and agree with the generic decoder);
//! - the feed worker's seed → live → ready lifecycle, reset clearing, and
//!   stale-generation discard behave like the WS-mode shard loop.

use std::sync::Arc;

use bytes::Bytes;
use relay_protocol::parse_schema;
use spacetimedb_public_mirror_client::observer::MirrorObserver;
use spacetimedb_public_mirror_client::upstream::{UpstreamTableOps, UpstreamUpdate};

use relay_cache::decode;
use relay_cache::feed::FeedManager;
use relay_cache::interest::InterestHub;
use relay_cache::roads::catalog::{apply_global_insert, GlobalRoadsCatalog};
use relay_cache::roads::meta::RoadsTableMeta;

const SCHEMA_JSON: &[u8] = include_bytes!("data/bitcraft-live-schema-v9.json");

const HEXITE_RESOURCE_ID: i32 = 348497955;
const OTHER_RESOURCE_ID: i32 = 1001; // not a tracked deposit type

fn schema() -> Arc<relay_protocol::MirroredSchema> {
    Arc::new(parse_schema(SCHEMA_JSON).expect("fixture schema parses"))
}

fn owned_fields(schema: &relay_protocol::MirroredSchema, table: &str) -> Vec<relay_protocol::MirroredField> {
    let tbl = schema
        .tables
        .iter()
        .find(|t| t.name == table)
        .unwrap_or_else(|| panic!("schema has `{table}`"));
    schema.table_product(tbl).expect("product").to_vec()
}

/// Hand-encode a `location_state` row. Real field order: entity_id u64,
/// chunk_index u64, x i32, z i32, dimension u32 (28 bytes).
fn location_row(entity_id: u64, x: i32, z: i32, dimension: u32) -> Bytes {
    let mut buf = Vec::with_capacity(28);
    buf.extend_from_slice(&entity_id.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // chunk_index
    buf.extend_from_slice(&x.to_le_bytes());
    buf.extend_from_slice(&z.to_le_bytes());
    buf.extend_from_slice(&dimension.to_le_bytes());
    Bytes::from(buf)
}

/// Hand-encode a `claim_state` row. Real field order: entity_id u64,
/// owner_player_entity_id u64, owner_building_entity_id u64, name String
/// (u32 length + UTF-8), neutral Bool.
fn claim_row(entity_id: u64, owner_player: u64, name: &str, neutral: bool) -> Bytes {
    let mut buf = Vec::with_capacity(25 + name.len());
    buf.extend_from_slice(&entity_id.to_le_bytes());
    buf.extend_from_slice(&owner_player.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // owner_building_entity_id
    buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
    buf.extend_from_slice(name.as_bytes());
    buf.push(neutral as u8);
    Bytes::from(buf)
}

/// Hand-encode a `claim_local_state` row with `location = some {x, z, 1}`.
/// Real field order: entity_id u64, supplies i32, building_maintenance f32,
/// num_tiles i32, num_tile_neighbors u32, location Sum[some{x,z,dimension},
/// none], treasury u32, xp_gained_since_last_coin_minting u32,
/// supplies_purchase_threshold u32, supplies_purchase_price f32,
/// building_description_id i32.
fn claim_local_row(entity_id: u64, x: i32, z: i32) -> Bytes {
    let mut buf = Vec::with_capacity(57);
    buf.extend_from_slice(&entity_id.to_le_bytes());
    buf.extend_from_slice(&0i32.to_le_bytes()); // supplies
    buf.extend_from_slice(&0u32.to_le_bytes()); // building_maintenance bits
    buf.extend_from_slice(&0i32.to_le_bytes()); // num_tiles
    buf.extend_from_slice(&0u32.to_le_bytes()); // num_tile_neighbors
    buf.push(0u8); // location: some
    buf.extend_from_slice(&x.to_le_bytes());
    buf.extend_from_slice(&z.to_le_bytes());
    buf.extend_from_slice(&1u32.to_le_bytes()); // dimension
    buf.extend_from_slice(&0u32.to_le_bytes()); // treasury
    buf.extend_from_slice(&0u32.to_le_bytes()); // xp_gained_since_last_coin_minting
    buf.extend_from_slice(&0u32.to_le_bytes()); // supplies_purchase_threshold
    buf.extend_from_slice(&0u32.to_le_bytes()); // supplies_purchase_price bits
    buf.extend_from_slice(&0i32.to_le_bytes()); // building_description_id
    Bytes::from(buf)
}

/// Hand-encode a `resource_state` row. Real field order: entity_id u64,
/// resource_id i32, direction_index i32 (16 bytes).
fn resource_row(entity_id: u64, resource_id: i32) -> Bytes {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&entity_id.to_le_bytes());
    buf.extend_from_slice(&resource_id.to_le_bytes());
    buf.extend_from_slice(&0i32.to_le_bytes()); // direction_index
    Bytes::from(buf)
}

fn seed_update(tables: Vec<UpstreamTableOps>) -> UpstreamUpdate {
    UpstreamUpdate {
        provenance: None,
        tables,
        is_seed: true,
    }
}

fn live_update(tables: Vec<UpstreamTableOps>) -> UpstreamUpdate {
    UpstreamUpdate {
        provenance: None,
        tables,
        is_seed: false,
    }
}

/// The feed consumes only `delete_bytes` (the mirror's ProductValue deletes
/// exist for the relational apply path), so tests populate the raw bytes.
fn ops(table: &str, delete_bytes: Vec<Bytes>, inserts: Vec<Bytes>) -> UpstreamTableOps {
    UpstreamTableOps {
        table_name: table.to_string(),
        deletes: Vec::new(),
        delete_bytes,
        inserts,
    }
}

fn ops_with_delete_bytes(table: &str, delete_bytes: Vec<Bytes>, inserts: Vec<Bytes>) -> UpstreamTableOps {
    ops(table, delete_bytes, inserts)
}

async fn settle() {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

#[test]
fn real_schema_hot_tables_have_fixed_layouts_and_fast_readers_agree() {
    let schema = schema();
    let cols = decode::resolve_cols(&schema).expect("resolve cols");

    // location_state: the fast reader must engage on the real schema…
    let location_fields = owned_fields(&schema, "location_state");
    let location_fast = decode::LocationFast::try_from_fields(&location_fields, &schema).expect("fixed layout");
    // …and agree with the generic decoder on a sample row.
    let row = location_row(12345, 800, -900, 2);
    let fast = location_fast.decode(&row).expect("fast decode");
    assert_eq!((fast.entity_id, fast.x, fast.z, fast.dimension), (12345, 800, -900, 2));
    let generic =
        decode::decode_location_with_fields(&row, &location_fields, cols.location, &schema).expect("generic decode");
    assert_eq!(fast, generic, "fast reader must agree with the generic decoder");

    // resource_state: same cross-check.
    let resource_fields = owned_fields(&schema, "resource_state");
    let resource_fast = decode::ResourceFast::try_from_fields(&resource_fields, &schema).expect("fixed layout");
    let row = resource_row(77, HEXITE_RESOURCE_ID);
    let fast = resource_fast.decode(&row).expect("fast decode");
    assert_eq!(
        (fast.entity_id, fast.resource_id, fast.direction_index),
        (77, HEXITE_RESOURCE_ID, 0)
    );
    let generic =
        decode::decode_resource_with_fields(&row, &resource_fields, cols.resource, &schema).expect("generic decode");
    assert_eq!(fast, generic);

    // A variable-width field ahead of the targets must disable the fast path.
    let var_fields = vec![
        relay_protocol::MirroredField {
            name: Some("label".into()),
            ty: relay_protocol::MirroredType::String,
        },
        relay_protocol::MirroredField {
            name: Some("entity_id".into()),
            ty: relay_protocol::MirroredType::U64,
        },
    ];
    assert!(decode::LocationFast::try_from_fields(&var_fields, &schema).is_none());
}

#[tokio::test]
async fn feed_seed_live_lifecycle_marks_ready_and_attaches_hexite() {
    let interest = InterestHub::new();
    let manager = FeedManager::new(interest);
    let handle = manager
        .register_region("bitcraft-live-7", SCHEMA_JSON)
        .expect("register")
        .expect("regional database yields a handle");
    assert_eq!(handle.region, 7);

    // Seed in the table-alphabetical order the embedded feed dispatches:
    // claim_state → claim_local_state → location_state → resource_state.
    // The hexite claim (700) sits at world coords (30, 40); its resource
    // (501) and location row share those coords.
    const HEXITE_NAME: &str = "{0} (N: {1}, E: {2})|~Hexite Deposit|~6158|~8174";
    let seed = seed_update(vec![
        ops("claim_state", vec![], vec![claim_row(700, 0, HEXITE_NAME, true)]),
        ops("claim_local_state", vec![], vec![claim_local_row(700, 30, 40)]),
        ops(
            "location_state",
            vec![],
            vec![
                location_row(600, 10, 20, 7), // interior
                location_row(501, 30, 40, 1), // overworld hexite — BEFORE its resource
                location_row(999, 1, 2, 1),   // overworld, not tracked
            ],
        ),
        // resource_state arrives after location_state: 501's location was
        // stashed via the hexite-claim coords index and must attach here.
        ops(
            "resource_state",
            vec![],
            vec![
                resource_row(501, HEXITE_RESOURCE_ID),
                resource_row(502, OTHER_RESOURCE_ID),
            ],
        ),
    ]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![seed])
        .await
        .expect("dispatch seed");

    // A live update arriving while seeds are still applying (regions with
    // ambient traffic always interleave some) must NOT finalize the snapshot —
    // it belongs after its table's seed and applies into staging.
    let interleaved = live_update(vec![ops(
        "resource_state",
        vec![],
        vec![resource_row(503, HEXITE_RESOURCE_ID)],
    )]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![interleaved])
        .await
        .expect("dispatch interleaved live update");

    // Not ready before live.
    settle().await;
    assert!(!handle.store.read().ready, "no readiness before on_live");

    // Going live publishes the snapshot.
    manager
        .on_live(Arc::from("bitcraft-live-7"), 1)
        .await
        .expect("dispatch live");
    for _ in 0..200 {
        if handle.store.read().ready {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    {
        let store = handle.store.read();
        assert!(store.ready, "ready after on_live");
        // Tracked hexite resources only: 501 from the seed plus 503 from the
        // interleaved pre-live update; the untracked one is dropped by
        // ResourceSoA, matching the SQL-filter semantics.
        assert_eq!(store.resource.len(), 2);
        // 501's overworld location attached x/z from the dimension-1 row
        // (503 has no location row yet — its location TU hasn't arrived).
        let slot = store.resource.find_by_location(30, 40).expect("501 located");
        assert_eq!(store.resource.entity_id[slot as usize], 501);
        // The interior location row is indexed; the untracked overworld row
        // is not (LocationDimStore skips overworld).
        assert_eq!(store.location_dim.len(), 1);
        assert_eq!(store.location_dim.get_or_overworld(600), 7);
        assert_eq!(store.location_dim.get_or_overworld(999), 1);
    }

    // Live update with raw delete bytes: the hexite moves.
    let live = live_update(vec![ops_with_delete_bytes(
        "location_state",
        vec![location_row(501, 30, 40, 1)],
        vec![location_row(501, 31, 41, 1)],
    )]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![live])
        .await
        .expect("dispatch live update");
    settle().await;
    {
        let store = handle.store.read();
        let slot = store
            .resource
            .find_by_location(31, 41)
            .expect("hexite moved to new coords");
        assert_eq!(store.resource.entity_id[slot as usize], 501);
    }

    // Reset (generation 2) clears the store; a stale generation-1 batch that
    // raced behind the reset is discarded.
    manager
        .on_reset(Arc::from("bitcraft-live-7"), 2)
        .await
        .expect("dispatch reset");
    settle().await;
    {
        let store = handle.store.read();
        assert!(!store.ready);
        assert_eq!(store.resource.len(), 0);
    }

    let stale = live_update(vec![ops(
        "resource_state",
        vec![],
        vec![resource_row(501, HEXITE_RESOURCE_ID)],
    )]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![stale])
        .await
        .expect("dispatch stale batch");
    settle().await;
    assert_eq!(handle.store.read().resource.len(), 0, "stale generation discarded");
}

#[tokio::test]
async fn feed_skips_global_database() {
    let interest = InterestHub::new();
    let manager = FeedManager::new(interest);
    let handle = manager
        .register_region("bitcraft-live-global", SCHEMA_JSON)
        .expect("register");
    assert!(handle.is_none(), "global database is not cached");
    // Dispatches for it are accepted (no-op) rather than erroring.
    manager
        .on_live(Arc::from("bitcraft-live-global"), 1)
        .await
        .expect("global dispatch is a no-op");
}

/// `terraform_recipe_desc.difference` is I16. Encoding it as Smallint and
/// running it through the global catalog insert must populate recipes —
/// the production empty `/roads/terraform-recipes` was this type mismatch.
#[test]
fn terraform_recipe_desc_i16_difference_inserts_into_catalog() {
    let schema = schema();
    let meta = RoadsTableMeta::from_schema_global(&schema).expect("global roads meta");
    assert!(
        meta.terraform_recipe.is_some(),
        "v9 schema must resolve terraform_recipe_desc columns"
    );

    let mut catalog = GlobalRoadsCatalog::new();
    let row = terraform_recipe_row(-4, 8, 1.5, 0.25);
    apply_global_insert(&mut catalog, &meta, &schema, "terraform_recipe_desc", &row).expect("insert");
    assert_eq!(catalog.terraform.len(), 1);
    let recipe = catalog.terraform[0];
    assert_eq!(recipe.difference, -4);
    assert_eq!(recipe.actions_count, 8);
    assert_eq!(recipe.stamina_per_action, 1.5);
    assert_eq!(recipe.time_per_action, 0.25);
}

/// Hand-encode a `terraform_recipe_desc` row against the v9 product:
/// difference i16, actions_count i32, tool_requirement Option (none),
/// stamina_per_action f32, time_per_action f32, tool_mesh_index i32,
/// recipe_performance_id i32, output_item_stacks Option (none).
fn terraform_recipe_row(difference: i16, actions_count: i32, stamina: f32, time: f32) -> Bytes {
    let mut buf = Vec::with_capacity(24);
    buf.extend_from_slice(&difference.to_le_bytes());
    buf.extend_from_slice(&actions_count.to_le_bytes());
    buf.push(1u8); // tool_requirement: none
    buf.extend_from_slice(&stamina.to_le_bytes());
    buf.extend_from_slice(&time.to_le_bytes());
    buf.extend_from_slice(&0i32.to_le_bytes()); // tool_mesh_index
    buf.extend_from_slice(&0i32.to_le_bytes()); // recipe_performance_id
    buf.push(1u8); // output_item_stacks: none
    Bytes::from(buf)
}

#[test]
fn feed_rejects_non_regional_database_names() {
    let interest = InterestHub::new();
    let manager = FeedManager::new(interest);
    assert!(
        manager.register_region("something-else", SCHEMA_JSON).is_err(),
        "only bitcraft-live-<N> / bitcraft-live-global are valid"
    );
}

#[tokio::test]
async fn feed_roads_stamps_all_resources_with_location_join() {
    let interest = InterestHub::new();
    let manager = FeedManager::new(interest);
    let roads = manager.enable_roads();
    let handle = manager
        .register_region("bitcraft-live-7", SCHEMA_JSON)
        .expect("register")
        .expect("regional database yields a handle");

    const SAPLING: i32 = 5; // Maple Sapling
                            // Region-local (10, 20) / (11, 21); region 7's origin is (7680, 7680).
    let seed = seed_update(vec![
        ops(
            "location_state",
            vec![],
            vec![location_row(800, 7690, 7700, 1), location_row(801, 7691, 7701, 1)],
        ),
        ops(
            "resource_state",
            vec![],
            vec![resource_row(800, SAPLING), resource_row(801, OTHER_RESOURCE_ID)],
        ),
    ]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![seed])
        .await
        .expect("dispatch seed");
    manager
        .on_live(Arc::from("bitcraft-live-7"), 1)
        .await
        .expect("dispatch live");
    for _ in 0..200 {
        if handle.store.read().ready {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(handle.store.read().ready);

    let rh = roads.region_handle(7).expect("roads region");
    for _ in 0..200 {
        if rh.grid.read().ready {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let grid = rh.grid.read();
    assert!(grid.ready);
    // Every resource type lands in the map now — the sapling and the
    // untracked id 1001 alike — joined from location_by_entity.
    let at = |x: i32, z: i32| grid.resource_map.resource_at_local(x, z).map(|t| t.resource_id);
    assert_eq!(at(10, 20), Some(SAPLING));
    assert_eq!(at(11, 21), Some(OTHER_RESOURCE_ID));
    // Hexite ResourceSoA still ignores non-hexite rows.
    assert_eq!(handle.store.read().resource.len(), 0);
}

#[tokio::test]
async fn feed_roads_expands_desc_footprint_onto_neighbor_hexes() {
    let interest = InterestHub::new();
    let manager = FeedManager::new(interest);
    let roads = manager.enable_roads();
    let handle = manager
        .register_region("bitcraft-live-7", SCHEMA_JSON)
        .expect("register")
        .expect("regional database yields a handle");

    const MUD_MOUND: i32 = 66;
    let seed = seed_update(vec![
        // Region-local (10, 1).
        ops("location_state", vec![], vec![location_row(800, 7690, 7681, 1)]),
        ops("resource_state", vec![], vec![resource_row(800, MUD_MOUND)]),
    ]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![seed])
        .await
        .expect("dispatch seed");
    manager
        .on_live(Arc::from("bitcraft-live-7"), 1)
        .await
        .expect("dispatch live");
    for _ in 0..200 {
        if handle.store.read().ready {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let rh = roads.region_handle(7).expect("roads region");
    for _ in 0..200 {
        if rh.grid.read().ready {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    {
        let grid = rh.grid.read();
        assert!(grid.ready);
        // No `resource_desc` footprint seen yet: single-hex fallback on the
        // origin tile only.
        let t = grid.resource_map.resource_at_local(10, 1).expect("origin tile");
        assert_eq!(t.resource_id, MUD_MOUND);
        assert!(grid.resource_map.resource_at_local(10, 0).is_none());
    }

    // The desc row lands (in production it seeds before resource_state), then
    // a new entity arrives live with its location ahead of it in the same
    // batch: the full footprint is stamped.
    rh.grid
        .write()
        .resource_map
        .note_desc(MUD_MOUND, &[(0, 0), (0, -1), (-1, 0)]);
    let live = live_update(vec![
        // Region-local (8, 2).
        ops("location_state", vec![], vec![location_row(801, 7688, 7682, 1)]),
        ops("resource_state", vec![], vec![resource_row(801, MUD_MOUND)]),
    ]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![live])
        .await
        .expect("dispatch live update");
    settle().await;

    let grid = rh.grid.read();
    // Mud Mound: axial (0,0)/(0,-1)/(-1,0) around odd-r (8,2) — the even
    // row puts the axial (0,-1) tile at (7,1), not (8,1).
    let mut hexes: Vec<(i32, i32)> = [(8, 2), (7, 1), (7, 2)]
        .into_iter()
        .filter(|&(x, z)| {
            grid.resource_map
                .resource_at_local(x, z)
                .is_some_and(|t| t.resource_id == MUD_MOUND)
        })
        .collect();
    hexes.sort_unstable();
    assert_eq!(hexes, vec![(7, 1), (7, 2), (8, 2)]);
    // The anchor tile carries the origin flag.
    assert!(grid.resource_map.resource_at_local(8, 2).unwrap().is_origin);
    assert!(!grid.resource_map.resource_at_local(7, 1).unwrap().is_origin);
}

// --- Bit-Me (mobile companion) feed tests -----------------------------------

use relay_cache::bitme::BitmeHub;

/// `stamina_state`: entity_id u64, last_stamina_decrease_timestamp
/// (`{__timestamp_micros_since_unix_epoch__: i64}` wrapper → raw i64),
/// stamina f32.
fn stamina_row(entity_id: u64, last_decrease_micros: i64, stamina: f32) -> Bytes {
    let mut buf = Vec::with_capacity(20);
    buf.extend_from_slice(&entity_id.to_le_bytes());
    buf.extend_from_slice(&last_decrease_micros.to_le_bytes());
    buf.extend_from_slice(&stamina.to_le_bytes());
    Bytes::from(buf)
}

/// `active_buff_state`: entity_id u64, active_buffs
/// Array<{buff_id i32, buff_start_timestamp {value i32}, buff_duration i32,
/// values Array<f32>}>.
fn buff_row(entity_id: u64, entries: &[(i32, i32, i32, &[f32])]) -> Bytes {
    let mut buf = Vec::new();
    buf.extend_from_slice(&entity_id.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (buff_id, start, duration, values) in entries {
        buf.extend_from_slice(&buff_id.to_le_bytes());
        buf.extend_from_slice(&start.to_le_bytes());
        buf.extend_from_slice(&duration.to_le_bytes());
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for v in *values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
    }
    Bytes::from(buf)
}

/// `player_action_state` in fixture field order: auto_id, chunk_index,
/// entity_id, start_time, duration (u64s), target Option<u64>,
/// recipe_id Option<i32>, action_type/layer/last_action_result sums,
/// client_cancel/was_consumed bools, 3 pad bytes.
fn action_row(
    auto_id: u64,
    entity_id: u64,
    start_ms: u64,
    duration_ms: u64,
    target: Option<u64>,
    recipe_id: Option<i32>,
    action_type_tag: u8,
    layer_tag: u8,
    result_tag: u8,
) -> Bytes {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&auto_id.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // chunk_index
    buf.extend_from_slice(&entity_id.to_le_bytes());
    buf.extend_from_slice(&start_ms.to_le_bytes());
    buf.extend_from_slice(&duration_ms.to_le_bytes());
    match target {
        Some(t) => {
            buf.push(0); // some
            buf.extend_from_slice(&t.to_le_bytes());
        }
        None => buf.push(1), // none
    }
    match recipe_id {
        Some(r) => {
            buf.push(0); // some
            buf.extend_from_slice(&r.to_le_bytes());
        }
        None => buf.push(1), // none
    }
    buf.push(action_type_tag); // Extract = 4, Craft = 20, None = 0
    buf.push(layer_tag); // Base = 0, UpperBody = 1
    buf.push(result_tag); // Success = 0 … Cancel = 3
    buf.push(0); // client_cancel
    buf.push(0); // was_consumed
    buf.extend_from_slice(&[0u8; 3]); // pads
    Bytes::from(buf)
}

/// `character_stats_state`: entity_id u64, values Array<f32>.
fn character_stats_row(entity_id: u64, values: &[f32]) -> Bytes {
    let mut buf = Vec::new();
    buf.extend_from_slice(&entity_id.to_le_bytes());
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for v in values {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    Bytes::from(buf)
}

/// `character_stat_desc`: stat_type i32, name String.
fn character_stat_desc_row(stat_type: i32, name: &str) -> Bytes {
    let mut buf = Vec::new();
    buf.extend_from_slice(&stat_type.to_le_bytes());
    buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
    buf.extend_from_slice(name.as_bytes());
    Bytes::from(buf)
}

/// `mobile_entity_state`: entity_id, chunk_index, timestamp (u64),
/// location_x/z, destination_x/z (i32), dimension (u32), is_walking (bool),
/// 3 pads.
fn mobile_row(entity_id: u64, ts_ms: u64, x_milli: i32, z_milli: i32, dimension: u32) -> Bytes {
    let mut buf = Vec::with_capacity(52);
    buf.extend_from_slice(&entity_id.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // chunk_index
    buf.extend_from_slice(&ts_ms.to_le_bytes());
    buf.extend_from_slice(&x_milli.to_le_bytes());
    buf.extend_from_slice(&z_milli.to_le_bytes());
    buf.extend_from_slice(&x_milli.to_le_bytes()); // destination_x
    buf.extend_from_slice(&z_milli.to_le_bytes()); // destination_z
    buf.extend_from_slice(&dimension.to_le_bytes());
    buf.push(0); // is_walking
    buf.extend_from_slice(&[0u8; 3]); // pads
    Bytes::from(buf)
}

/// `extraction_recipe_desc` identity prefix: id i32, resource_id i32 (the
/// fast reader only touches the two leading I32s; trailing
/// yields/requirements columns are irrelevant to the store).
fn extraction_recipe_row(id: i32, resource_id: i32) -> Bytes {
    let mut buf = Vec::with_capacity(8);
    buf.extend_from_slice(&id.to_le_bytes());
    buf.extend_from_slice(&resource_id.to_le_bytes());
    Bytes::from(buf)
}

/// `resource_health_state`: entity_id u64, health i32.
fn resource_health_row(entity_id: u64, health: i32) -> Bytes {
    let mut buf = Vec::with_capacity(12);
    buf.extend_from_slice(&entity_id.to_le_bytes());
    buf.extend_from_slice(&health.to_le_bytes());
    Bytes::from(buf)
}

const CITRIC_BUSH_ID: i32 = 1_688_062_540;

#[tokio::test]
async fn bitme_region_tables_seed_into_stores_and_live_feed_tracks_targets() {
    let interest = InterestHub::new();
    let manager = FeedManager::new(interest);
    let handle = manager
        .register_region("bitcraft-live-7", SCHEMA_JSON)
        .expect("register")
        .expect("regional handle");
    let bitme: Arc<BitmeHub> = manager.bitme();

    let seed = seed_update(vec![
        ops(
            "character_stat_desc",
            vec![],
            vec![
                character_stat_desc_row(0, "Maximum Health"),
                character_stat_desc_row(1, "Maximum Stamina"),
            ],
        ),
        ops(
            "character_stats_state",
            vec![],
            vec![character_stats_row(10, &[230.0, 523.0])],
        ),
        // Forageable identity chain: recipe 424242 extracts resource 74
        // (Button Mushrooms) — nothing in any world index knows entity 7777.
        ops(
            "extraction_recipe_desc",
            vec![],
            vec![extraction_recipe_row(424_242, 74)],
        ),
        ops(
            "stamina_state",
            vec![],
            vec![stamina_row(10, 1_788_617_359_136_199, 101.9)],
        ),
        ops(
            "active_buff_state",
            vec![],
            vec![buff_row(
                10,
                &[(2, 0, 0, &[]), (5, 1_778_354_880, 300, &[-0.4, -0.4, -0.4])],
            )],
        ),
        ops(
            "player_action_state",
            vec![],
            vec![
                action_row(1, 10, 1_788_617_359_072, 1500, Some(5001), Some(412_001), 4, 0, 0),
                action_row(2, 10, 1_786_758_362_802, 150, None, None, 0, 1, 3),
            ],
        ),
        ops(
            "mobile_entity_state",
            vec![],
            vec![mobile_row(10, 1_788_616_889_357, 12_367_058, 19_746_902, 1)],
        ),
    ]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![seed])
        .await
        .expect("dispatch seed");
    manager
        .on_live(Arc::from("bitcraft-live-7"), 1)
        .await
        .expect("dispatch live");
    for _ in 0..200 {
        if handle.store.read().ready {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    {
        let store = handle.store.read();
        assert!(store.ready);
        assert_eq!(store.stamina.stamina_of(10), Some(101.9));
        assert_eq!(store.stamina.last_decrease_secs(10), Some(1_788_617_359));
        assert_eq!(store.character_stats.value_at(10, 1), Some(523.0));
        assert_eq!(store.character_stat_desc.stat_indices().max_stamina, 1);
        let buffs = store.active_buff.buffs_of(10).expect("buffs");
        // The zeroed placeholder (buff_id 2) decodes too — the live filter
        // (`is_live_buff`) is applied at read time, not here.
        assert_eq!(buffs.len(), 2);
        assert_eq!(buffs[1].buff_id, 5);
        assert_eq!(buffs[1].duration, 300);
        assert_eq!(store.player_action.by_entity(10).len(), 2);
        // Position: milli-units → world tiles.
        assert_eq!(store.mobile_entity.overworld_tile(10), Some((12_367, 19_746)));
    }

    // Live: the player targets a resource (citric bush entity 5001). The hub
    // must pick up its health once tracked, and a watched spawn must land in
    // the region's spawn log.
    let live = live_update(vec![
        ops(
            "player_action_state",
            vec![],
            vec![action_row(3, 10, 1_788_617_400_000, 10_000, Some(5001), None, 4, 0, 0)],
        ),
        ops("resource_state", vec![], vec![resource_row(5001, CITRIC_BUSH_ID)]),
    ]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![live])
        .await
        .expect("dispatch live update");
    settle().await;

    // Session registration + target tracking, exactly like the HTTP handler.
    bitme.touch_session(10);
    bitme.note_targets([5001]);
    assert_eq!(bitme.target_health(5001), None, "no health before the first tick");

    // The seeded recipe resolves forageable targets with no world index:
    assert_eq!(
        handle.store.read().extraction_recipe.resource_for_recipe(424_242),
        Some(74)
    );

    let tick = live_update(vec![ops(
        "resource_health_state",
        vec![],
        vec![resource_health_row(5001, 480)],
    )]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![tick])
        .await
        .expect("dispatch health tick");
    settle().await;
    assert_eq!(bitme.target_health(5001), Some(480), "tracked target health retained");

    let spawns = bitme.spawns(7);
    assert_eq!(spawns.len(), 1, "citric bush insert logged");
    assert_eq!(spawns[0].entity_id, 5001);
    assert_eq!(spawns[0].resource_id, CITRIC_BUSH_ID);

    // Harvesting it away removes the spawn and clears the health.
    let gone = live_update(vec![ops(
        "resource_state",
        vec![resource_row(5001, CITRIC_BUSH_ID)],
        vec![],
    )]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![gone])
        .await
        .expect("dispatch despawn");
    settle().await;
    assert!(bitme.spawns(7).is_empty(), "despawned spawn pruned");
    assert_eq!(bitme.target_health(5001), None, "health cleared on despawn");

    // A reset clears the spawn log (seed rows are not spawns).
    let live_spawn = live_update(vec![ops(
        "resource_state",
        vec![],
        vec![resource_row(5002, CITRIC_BUSH_ID)],
    )]);
    manager
        .on_updates(Arc::from("bitcraft-live-7"), 1, vec![live_spawn])
        .await
        .expect("spawn again");
    settle().await;
    assert_eq!(bitme.spawns(7).len(), 1);
    manager.on_reset(Arc::from("bitcraft-live-7"), 2).await.expect("reset");
    settle().await;
    assert!(bitme.spawns(7).is_empty(), "reset clears spawn log");
}

#[tokio::test]
async fn bitme_global_resolve_chain_builds_from_global_feed() {
    let interest = InterestHub::new();
    let manager = FeedManager::new(interest);
    let bitme = manager.bitme();

    manager
        .register_region("bitcraft-live-global", SCHEMA_JSON)
        .expect("global register");
    // Fixture field orders (bitcraft-live schema v9).
    let mut lowercase = Vec::new();
    lowercase.extend_from_slice(&1_234_567_890_123u64.to_le_bytes());
    lowercase.extend_from_slice(&("strawberry".len() as u32).to_le_bytes());
    lowercase.extend_from_slice(b"strawberry");
    let mut identity = [0u8; 32];
    for (i, b) in identity.iter_mut().enumerate() {
        *b = i as u8;
    }
    let mut user = Vec::new();
    user.extend_from_slice(&identity); // identity wrapper unwraps to 32 raw bytes
    user.extend_from_slice(&1_234_567_890_123u64.to_le_bytes());
    user.push(1); // can_sign_in
    let mut conn = Vec::new();
    conn.push(7u8);
    conn.extend_from_slice(&(b"https://game.example".len() as u32).to_le_bytes());
    conn.extend_from_slice(b"https://game.example");
    conn.extend_from_slice(&(b"bitcraft-live-7".len() as u32).to_le_bytes());
    conn.extend_from_slice(b"bitcraft-live-7");
    let mut world_name = Vec::new();
    world_name.extend_from_slice(&7u16.to_le_bytes());
    world_name.extend_from_slice(&(b"Virexal".len() as u32).to_le_bytes());
    world_name.extend_from_slice(b"Virexal");
    world_name.extend_from_slice(&0u32.to_le_bytes()); // module_name_prefix
                                                       // player_state: teleport_location{x,z,dim + location_type sum}, entity_id,
                                                       // 4 i32s, signed_in, traveler_tasks_expiration.
    let mut player_state = Vec::new();
    player_state.extend_from_slice(&0i32.to_le_bytes());
    player_state.extend_from_slice(&0i32.to_le_bytes());
    player_state.extend_from_slice(&1u32.to_le_bytes());
    player_state.push(0); // location_type: BirthLocation
    player_state.extend_from_slice(&1_234_567_890_123u64.to_le_bytes());
    player_state.extend_from_slice(&0i32.to_le_bytes());
    player_state.extend_from_slice(&0i32.to_le_bytes());
    player_state.extend_from_slice(&0i32.to_le_bytes());
    player_state.extend_from_slice(&1_788_617_359i32.to_le_bytes());
    player_state.push(1); // signed_in
    player_state.extend_from_slice(&0i32.to_le_bytes());

    let seed = seed_update(vec![
        ops(
            "player_lowercase_username_state",
            vec![],
            vec![Bytes::from(lowercase.clone())],
        ),
        ops("user_state", vec![], vec![Bytes::from(user.clone())]),
        ops("region_connection_info", vec![], vec![Bytes::from(conn.clone())]),
        ops("world_region_name_state", vec![], vec![Bytes::from(world_name.clone())]),
        ops("player_state", vec![], vec![Bytes::from(player_state.clone())]),
    ]);
    manager
        .on_updates(Arc::from("bitcraft-live-global"), 1, vec![seed])
        .await
        .expect("dispatch global seed");
    manager
        .on_live(Arc::from("bitcraft-live-global"), 1)
        .await
        .expect("dispatch global live");
    settle().await;

    // Fixture has no `user_region_state`: the chain degrades (region_id None,
    // no host/module) instead of failing the feed — the HTTP handler then
    // falls back to the region shards.
    let r = bitme
        .global()
        .read()
        .resolve("strawberry")
        .expect("exact-match resolve");
    assert_eq!(r.entity_id, 1_234_567_890_123);
    assert_eq!(r.username_lowercase, "strawberry");
    assert_eq!(r.region_id, None, "fixture lacks user_region_state");
    assert_eq!(r.host, "");
    // Region-scoped info is still reachable by explicit region id.
    let (name, host, module) = bitme.global().read().region_info(7);
    assert_eq!(name.as_deref(), Some("Virexal"));
    assert_eq!(host.as_deref(), Some("https://game.example"));
    assert_eq!(module.as_deref(), Some("bitcraft-live-7"));
    // Identity arrived as raw LE bytes; resolve must render canonical
    // (big-endian) hex — bytes [0..32] seeded LE reverse to "1f..00".
    assert_eq!(
        r.identity_hex.as_deref(),
        Some("1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100")
    );
    assert_eq!(r.region_name, None);
    assert_eq!(r.signed_in, Some(true));

    // No substring matching: only the exact lowercase name resolves.
    assert!(bitme.global().read().resolve("strawberr").is_none());

    // Delete of the username row breaks the chain.
    manager
        .on_updates(
            Arc::from("bitcraft-live-global"),
            1,
            vec![live_update(vec![ops(
                "player_lowercase_username_state",
                vec![Bytes::from(lowercase)],
                vec![],
            )])],
        )
        .await
        .expect("delete username");
    settle().await;
    assert!(bitme.global().read().resolve("strawberry").is_none());
}
