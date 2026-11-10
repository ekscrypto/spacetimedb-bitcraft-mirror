//! Real-upstream fixture test, gated on an env var so CI / offline builds
//! never need the files:
//!
//! ```sh
//! RELAY_SCHEMA_V9=/path/s9.json RELAY_SCHEMA_V10=/path/s10.json \
//!   cargo test -p relay-protocol --test real_schema
//! ```
//!
//! Guards against envelope regressions like the 2026-09-24 crash, where a
//! v10 sectioned schema reached `parse_schema` which only understood the
//! flat v9 shape.

use relay_protocol::parse_schema;

fn load(var: &str) -> Option<Vec<u8>> {
    let path = std::env::var(var).ok()?;
    Some(std::fs::read(path).expect(&format!("{var} points at a readable file")))
}

#[test]
fn real_upstream_schemas_parse() {
    let Some(v9) = load("RELAY_SCHEMA_V9") else {
        eprintln!("skipping: RELAY_SCHEMA_V9 not set");
        return;
    };
    let Some(v10) = load("RELAY_SCHEMA_V10") else {
        eprintln!("skipping: RELAY_SCHEMA_V10 not set");
        return;
    };

    let flat = parse_schema(&v9).expect("real v9 (flat) schema parses");
    let sectioned = parse_schema(&v10).expect("real v10 (sectioned) schema parses");

    // Both envelopes describe the same module: same table set, and every
    // table's product type resolves to a Product with columns.
    let mut flat_names: Vec<_> = flat.tables.iter().map(|t| t.name.clone()).collect();
    let mut sectioned_names: Vec<_> = sectioned.tables.iter().map(|t| t.name.clone()).collect();
    flat_names.sort();
    sectioned_names.sort();
    assert_eq!(flat_names.len(), sectioned_names.len(), "table count matches");
    assert_eq!(flat_names, sectioned_names, "same table set in both envelopes");
    assert!(!sectioned.tables.is_empty());
    for t in &sectioned.tables {
        let cols = sectioned
            .table_product(t)
            .unwrap_or_else(|| panic!("table {} resolves to a product", t.name));
        assert!(!cols.is_empty(), "table {} has columns", t.name);
    }
}
