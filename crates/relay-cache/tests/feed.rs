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
    assert_eq!((fast.entity_id, fast.resource_id), (77, HEXITE_RESOURCE_ID));
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

    // Seed: an interior location row, an overworld hexite row pair
    // (resource + its dimension-1 location), and an untracked resource.
    let seed = seed_update(vec![
        ops(
            "resource_state",
            vec![],
            vec![
                resource_row(501, HEXITE_RESOURCE_ID),
                resource_row(502, OTHER_RESOURCE_ID),
            ],
        ),
        ops(
            "location_state",
            vec![],
            vec![
                location_row(600, 10, 20, 7), // interior
                location_row(501, 30, 40, 1), // overworld hexite
                location_row(999, 1, 2, 1),   // overworld, not tracked
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

#[test]
fn feed_rejects_non_regional_database_names() {
    let interest = InterestHub::new();
    let manager = FeedManager::new(interest);
    assert!(
        manager.register_region("something-else", SCHEMA_JSON).is_err(),
        "only bitcraft-live-<N> / bitcraft-live-global are valid"
    );
}
