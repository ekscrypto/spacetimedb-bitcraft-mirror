// SPDX-License-Identifier: MIT

//! Apply upstream table rows to a [`RoadsRegionGrid`].

use anyhow::{anyhow, Result};
use bytes::Bytes;
use hashbrown::HashMap;
use relay_protocol::{MirroredField, MirroredSchema};

use super::decode::{
    decode_claim_state_roads, decode_claim_tile, decode_location_roads, decode_paved_tile, decode_terrain_chunk,
    CLAIM_STATE_TABLE, CLAIM_TILE_TABLE, LOCATION_TABLE, PAVED_TILE_TABLE, TERRAIN_CHUNK_TABLE,
};
use super::join::OVERWORLD_DIMENSION;
use super::meta::RoadsTableMeta;
use super::store::RoadsRegionGrid;
use crate::decode::{self as cache_decode, ResourceRow, RESOURCE_TABLE};

pub fn apply_roads_rows(
    grid: &mut RoadsRegionGrid,
    schema: &MirroredSchema,
    meta: &RoadsTableMeta,
    table: &str,
    deletes: &[Bytes],
    inserts: &[Bytes],
) -> Result<()> {
    for row in deletes {
        apply_roads_delete(grid, schema, meta, table, row.as_ref())?;
    }
    for row in inserts {
        apply_roads_insert(grid, schema, meta, table, row.as_ref())?;
    }
    Ok(())
}

fn apply_roads_delete(
    grid: &mut RoadsRegionGrid,
    schema: &MirroredSchema,
    meta: &RoadsTableMeta,
    table: &str,
    row: &[u8],
) -> Result<()> {
    match table {
        TERRAIN_CHUNK_TABLE => {
            let r = decode_terrain_chunk(row, &meta.terrain_chunk_fields, meta.terrain_chunk, schema)?;
            grid.terrain_writer.clear_chunk(&mut grid.terrain, r.chunk_x, r.chunk_z);
            grid.terrain_chunks.remove(&(r.chunk_x, r.chunk_z));
            grid.bump_generation();
        }
        PAVED_TILE_TABLE => {
            let r = decode_paved_tile(row, &meta.paved_tile_fields, meta.paved_tile, schema)?;
            grid.join.paving_by_entity.remove(&r.entity_id);
            grid.join.clear_paving_at(grid.region, &mut grid.overlay, r.entity_id);
            grid.bump_generation();
        }
        CLAIM_TILE_TABLE => {
            let r = decode_claim_tile(row, &meta.claim_tile_fields, meta.claim_tile, schema)?;
            grid.join.claim_by_entity.remove(&r.entity_id);
            grid.join.clear_claim_at(grid.region, &mut grid.overlay, r.entity_id);
            grid.bump_generation();
        }
        CLAIM_STATE_TABLE => {
            let r = decode_claim_state_roads(row, &meta.claim_state_fields, meta.claim_state, schema)?;
            grid.join.neutral_claims.remove(&r.entity_id);
            grid.bump_generation();
        }
        LOCATION_TABLE => {
            let r = decode_location_roads(row, &meta.location_fields, meta.location, schema)?;
            if r.dimension == OVERWORLD_DIMENSION {
                grid.join.location_by_entity.remove(&r.entity_id);
                grid.harvestable.clear_location(r.entity_id);
            }
            grid.bump_generation();
        }
        RESOURCE_TABLE => {
            let r = decode_resource_row(meta, schema, row)?;
            grid.harvestable.delete(r.entity_id);
        }
        _ => {}
    }
    Ok(())
}

fn apply_roads_insert(
    grid: &mut RoadsRegionGrid,
    schema: &MirroredSchema,
    meta: &RoadsTableMeta,
    table: &str,
    row: &[u8],
) -> Result<()> {
    match table {
        TERRAIN_CHUNK_TABLE => apply_terrain_chunk(grid, schema, meta, row)?,
        PAVED_TILE_TABLE => apply_paved_tile(grid, schema, meta, row)?,
        CLAIM_TILE_TABLE => apply_claim_tile(grid, schema, meta, row)?,
        CLAIM_STATE_TABLE => apply_claim_state(grid, schema, meta, row)?,
        LOCATION_TABLE => apply_location(grid, schema, meta, row)?,
        RESOURCE_TABLE => apply_resource(grid, schema, meta, row)?,
        _ => {}
    }
    Ok(())
}

fn apply_terrain_chunk(
    grid: &mut RoadsRegionGrid,
    schema: &MirroredSchema,
    meta: &RoadsTableMeta,
    row: &[u8],
) -> Result<()> {
    let r = decode_terrain_chunk(row, &meta.terrain_chunk_fields, meta.terrain_chunk, schema)?;
    *grid.dim_hist.entry(r.dimension).or_insert(0) += 1;
    grid.terrain_chunks.insert((r.chunk_x, r.chunk_z), r.dimension);
    if grid.ready {
        let dim = grid.best_terrain_dimension();
        grid.terrain_writer.set_best_dimension(dim);
        if r.dimension == dim {
            grid.terrain_writer.write_chunk(
                grid.region,
                &mut grid.terrain,
                r.chunk_x,
                r.chunk_z,
                r.dimension,
                &r.elevations,
                &r.original_elevations,
                &r.water_levels,
                &r.water_body_types,
            );
            grid.bump_generation();
        }
    } else {
        grid.pending_terrain.push(r);
    }
    Ok(())
}

fn apply_paved_tile(
    grid: &mut RoadsRegionGrid,
    schema: &MirroredSchema,
    meta: &RoadsTableMeta,
    row: &[u8],
) -> Result<()> {
    let r = decode_paved_tile(row, &meta.paved_tile_fields, meta.paved_tile, schema)?;
    grid.join.paving_by_entity.insert(r.entity_id, r.tile_type_id);
    grid.join
        .recompute_cell(grid.region, &mut grid.overlay, &mut grid.claim_index, r.entity_id);
    grid.bump_generation();
    Ok(())
}

fn apply_claim_tile(
    grid: &mut RoadsRegionGrid,
    schema: &MirroredSchema,
    meta: &RoadsTableMeta,
    row: &[u8],
) -> Result<()> {
    let r = decode_claim_tile(row, &meta.claim_tile_fields, meta.claim_tile, schema)?;
    grid.join.claim_by_entity.insert(r.entity_id, r.claim_id);
    grid.join
        .recompute_cell(grid.region, &mut grid.overlay, &mut grid.claim_index, r.entity_id);
    grid.bump_generation();
    Ok(())
}

fn apply_claim_state(
    grid: &mut RoadsRegionGrid,
    schema: &MirroredSchema,
    meta: &RoadsTableMeta,
    row: &[u8],
) -> Result<()> {
    let r = decode_claim_state_roads(row, &meta.claim_state_fields, meta.claim_state, schema)?;
    if r.neutral {
        grid.join.neutral_claims.insert(r.entity_id);
    } else {
        grid.join.neutral_claims.remove(&r.entity_id);
    }
    grid.bump_generation();
    Ok(())
}

fn apply_location(
    grid: &mut RoadsRegionGrid,
    schema: &MirroredSchema,
    meta: &RoadsTableMeta,
    row: &[u8],
) -> Result<()> {
    let r = decode_location_roads(row, &meta.location_fields, meta.location, schema)?;
    if r.dimension != OVERWORLD_DIMENSION {
        return Ok(());
    }
    grid.join.location_by_entity.insert(r.entity_id, (r.x, r.z));
    grid.harvestable.set_location(r.entity_id, r.x, r.z);
    grid.join
        .recompute_cell(grid.region, &mut grid.overlay, &mut grid.claim_index, r.entity_id);
    grid.bump_generation();
    Ok(())
}

fn apply_resource(
    grid: &mut RoadsRegionGrid,
    schema: &MirroredSchema,
    meta: &RoadsTableMeta,
    row: &[u8],
) -> Result<()> {
    let r = decode_resource_row(meta, schema, row)?;
    let loc = grid.join.location_by_entity.get(&r.entity_id).copied();
    grid.harvestable
        .upsert(r.entity_id, r.resource_id, r.direction_index, loc);
    Ok(())
}

fn decode_resource_row(meta: &RoadsTableMeta, schema: &MirroredSchema, row: &[u8]) -> Result<ResourceRow> {
    if let Some(decoded) = meta.resource_fast.and_then(|fast| fast.decode(row)) {
        return Ok(decoded);
    }
    let cols = meta
        .resource
        .ok_or_else(|| anyhow!("roads meta has no resource_state columns"))?;
    cache_decode::decode_resource_with_fields(row, &meta.resource_fields, cols, schema)
}

/// After seed completes, flush pending terrain rows with the chosen dimension.
pub fn finalize_terrain_seed(grid: &mut RoadsRegionGrid) {
    let dim = grid.best_terrain_dimension();
    grid.terrain_writer.set_best_dimension(dim);
    let pending = std::mem::take(&mut grid.pending_terrain);
    for r in pending {
        if r.dimension != dim {
            continue;
        }
        grid.terrain_writer.write_chunk(
            grid.region,
            &mut grid.terrain,
            r.chunk_x,
            r.chunk_z,
            r.dimension,
            &r.elevations,
            &r.original_elevations,
            &r.water_levels,
            &r.water_body_types,
        );
    }
}

/// Decode PK entity_id from delete row for tables keyed by entity_id.
#[allow(dead_code)]
pub fn decode_entity_id_delete(
    row: &[u8],
    fields: &[MirroredField],
    entity_col: usize,
    schema: &MirroredSchema,
) -> Result<u64> {
    use relay_protocol::bsatn::Cell;
    let cells = relay_protocol::bsatn::decode_row(row, fields, schema)?;
    match &cells[entity_col] {
        Cell::Bytea(Some(b)) if b.len() == 8 => {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(b);
            Ok(u64::from_le_bytes(arr))
        }
        Cell::Bigint(Some(n)) => Ok(*n as u64),
        other => anyhow::bail!("entity_id delete: {other:?}"),
    }
}

pub type TerrainDimHist = HashMap<u32, usize>;
