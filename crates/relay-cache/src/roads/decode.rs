// SPDX-License-Identifier: MIT

//! BSATN row decoders for roads upstream tables.

use anyhow::{anyhow, bail, Result};
use relay_protocol::bsatn::Cell;
use relay_protocol::{bsatn, MirroredField, MirroredSchema};
use serde_json::Value;

pub const TERRAIN_CHUNK_TABLE: &str = "terrain_chunk_state";
pub const PAVED_TILE_TABLE: &str = "paved_tile_state";
pub const CLAIM_TILE_TABLE: &str = "claim_tile_state";
pub const CLAIM_STATE_TABLE: &str = "claim_state";
pub const LOCATION_TABLE: &str = "location_state";
pub const PAVING_TILE_DESC_TABLE: &str = "paving_tile_desc";
pub const TERRAFORM_RECIPE_DESC_TABLE: &str = "terraform_recipe_desc";
pub const WORLD_REGION_STATE_TABLE: &str = "world_region_state";
pub const REGION_NAME_TABLE: &str = "region_name_state";

#[derive(Clone, Copy)]
pub struct TerrainChunkCols {
    pub chunk_x: usize,
    pub chunk_z: usize,
    pub dimension: usize,
    pub elevations: usize,
    pub original_elevations: usize,
    pub water_levels: usize,
    pub water_body_types: usize,
}

#[derive(Clone, Copy)]
pub struct PavedTileCols {
    pub entity_id: usize,
    pub tile_type_id: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimTileCols {
    pub entity_id: usize,
    pub claim_id: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimStateRoadsCols {
    pub entity_id: usize,
    pub neutral: usize,
}

#[derive(Clone, Copy)]
pub struct LocationRoadsCols {
    pub entity_id: usize,
    pub x: usize,
    pub z: usize,
    pub dimension: usize,
}

#[derive(Clone, Copy)]
pub struct PavingDescCols {
    pub id: usize,
    pub name: usize,
    pub paving_duration: usize,
    pub tier: usize,
    pub input_cargo_id: usize,
    pub consumed_item_stacks: usize,
}

#[derive(Clone, Copy)]
pub struct TerraformRecipeCols {
    pub difference: usize,
    pub actions_count: usize,
    pub stamina_per_action: usize,
    pub time_per_action: usize,
}

#[derive(Clone, Copy)]
pub struct RegionNameCols {
    pub id: usize,
    pub player_facing_name: usize,
}

#[derive(Clone, Copy)]
pub struct WorldRegionCols {
    pub region_width_chunks: usize,
    pub region_height_chunks: usize,
    pub region_count_sqrt: usize,
}

pub struct RoadsColMaps {
    pub terrain_chunk: TerrainChunkCols,
    pub paved_tile: PavedTileCols,
    pub claim_tile: ClaimTileCols,
    pub claim_state: ClaimStateRoadsCols,
    pub location: LocationRoadsCols,
    pub paving_desc: PavingDescCols,
    pub terraform_recipe: TerraformRecipeCols,
    pub region_name: RegionNameCols,
    pub world_region: WorldRegionCols,
}

pub fn resolve_roads_cols(schema: &MirroredSchema) -> Result<RoadsColMaps> {
    Ok(RoadsColMaps {
        terrain_chunk: resolve_terrain_chunk_cols(schema)?,
        paved_tile: resolve_paved_tile_cols(schema)?,
        claim_tile: resolve_claim_tile_cols(schema)?,
        claim_state: resolve_claim_state_roads_cols(schema)?,
        location: resolve_location_roads_cols(schema)?,
        paving_desc: resolve_paving_desc_cols(schema)?,
        terraform_recipe: resolve_terraform_recipe_cols(schema)?,
        region_name: resolve_region_name_cols(schema)?,
        world_region: resolve_world_region_cols(schema)?,
    })
}

fn fields_of<'a>(schema: &'a MirroredSchema, table: &str) -> Result<&'a [MirroredField]> {
    let tbl = schema
        .tables
        .iter()
        .find(|t| t.name == table)
        .ok_or_else(|| anyhow!("schema has no table `{table}`"))?;
    schema
        .table_product(tbl)
        .ok_or_else(|| anyhow!("table `{table}` is not a Product"))
}

fn find_field(fields: &[MirroredField], name: &str, table: &str) -> Result<usize> {
    fields
        .iter()
        .position(|f| f.name.as_deref() == Some(name))
        .ok_or_else(|| anyhow!("table `{table}` missing column `{name}`"))
}

pub fn resolve_terrain_chunk_cols(schema: &MirroredSchema) -> Result<TerrainChunkCols> {
    let f = fields_of(schema, TERRAIN_CHUNK_TABLE)?;
    Ok(TerrainChunkCols {
        chunk_x: find_field(f, "chunk_x", TERRAIN_CHUNK_TABLE)?,
        chunk_z: find_field(f, "chunk_z", TERRAIN_CHUNK_TABLE)?,
        dimension: find_field(f, "dimension", TERRAIN_CHUNK_TABLE)?,
        elevations: find_field(f, "elevations", TERRAIN_CHUNK_TABLE)?,
        original_elevations: find_field(f, "original_elevations", TERRAIN_CHUNK_TABLE)?,
        water_levels: find_field(f, "water_levels", TERRAIN_CHUNK_TABLE)?,
        water_body_types: find_field(f, "water_body_types", TERRAIN_CHUNK_TABLE)?,
    })
}

pub fn resolve_paved_tile_cols(schema: &MirroredSchema) -> Result<PavedTileCols> {
    let f = fields_of(schema, PAVED_TILE_TABLE)?;
    Ok(PavedTileCols {
        entity_id: find_field(f, "entity_id", PAVED_TILE_TABLE)?,
        tile_type_id: find_field(f, "tile_type_id", PAVED_TILE_TABLE)?,
    })
}

pub fn resolve_claim_tile_cols(schema: &MirroredSchema) -> Result<ClaimTileCols> {
    let f = fields_of(schema, CLAIM_TILE_TABLE)?;
    Ok(ClaimTileCols {
        entity_id: find_field(f, "entity_id", CLAIM_TILE_TABLE)?,
        claim_id: find_field(f, "claim_id", CLAIM_TILE_TABLE)?,
    })
}

pub fn resolve_claim_state_roads_cols(schema: &MirroredSchema) -> Result<ClaimStateRoadsCols> {
    let f = fields_of(schema, CLAIM_STATE_TABLE)?;
    Ok(ClaimStateRoadsCols {
        entity_id: find_field(f, "entity_id", CLAIM_STATE_TABLE)?,
        neutral: find_field(f, "neutral", CLAIM_STATE_TABLE)?,
    })
}

pub fn resolve_location_roads_cols(schema: &MirroredSchema) -> Result<LocationRoadsCols> {
    let f = fields_of(schema, LOCATION_TABLE)?;
    Ok(LocationRoadsCols {
        entity_id: find_field(f, "entity_id", LOCATION_TABLE)?,
        x: find_field(f, "x", LOCATION_TABLE)?,
        z: find_field(f, "z", LOCATION_TABLE)?,
        dimension: find_field(f, "dimension", LOCATION_TABLE)?,
    })
}

pub fn resolve_paving_desc_cols(schema: &MirroredSchema) -> Result<PavingDescCols> {
    let f = fields_of(schema, PAVING_TILE_DESC_TABLE)?;
    Ok(PavingDescCols {
        id: find_field(f, "id", PAVING_TILE_DESC_TABLE)?,
        name: find_field(f, "name", PAVING_TILE_DESC_TABLE)?,
        paving_duration: find_field(f, "paving_duration", PAVING_TILE_DESC_TABLE)?,
        tier: find_field(f, "tier", PAVING_TILE_DESC_TABLE)?,
        input_cargo_id: find_field(f, "input_cargo_id", PAVING_TILE_DESC_TABLE)?,
        consumed_item_stacks: find_field(f, "consumed_item_stacks", PAVING_TILE_DESC_TABLE)?,
    })
}

pub fn resolve_terraform_recipe_cols(schema: &MirroredSchema) -> Result<TerraformRecipeCols> {
    let f = fields_of(schema, TERRAFORM_RECIPE_DESC_TABLE)?;
    Ok(TerraformRecipeCols {
        difference: find_field(f, "difference", TERRAFORM_RECIPE_DESC_TABLE)?,
        actions_count: find_field(f, "actions_count", TERRAFORM_RECIPE_DESC_TABLE)?,
        stamina_per_action: find_field(f, "stamina_per_action", TERRAFORM_RECIPE_DESC_TABLE)?,
        time_per_action: find_field(f, "time_per_action", TERRAFORM_RECIPE_DESC_TABLE)?,
    })
}

pub fn resolve_region_name_cols(schema: &MirroredSchema) -> Result<RegionNameCols> {
    let f = fields_of(schema, REGION_NAME_TABLE)?;
    Ok(RegionNameCols {
        id: find_field(f, "id", REGION_NAME_TABLE)?,
        player_facing_name: find_field(f, "player_facing_name", REGION_NAME_TABLE)?,
    })
}

pub fn resolve_world_region_cols(schema: &MirroredSchema) -> Result<WorldRegionCols> {
    let f = fields_of(schema, WORLD_REGION_STATE_TABLE)?;
    Ok(WorldRegionCols {
        region_width_chunks: find_field(f, "region_width_chunks", WORLD_REGION_STATE_TABLE)?,
        region_height_chunks: find_field(f, "region_height_chunks", WORLD_REGION_STATE_TABLE)?,
        region_count_sqrt: find_field(f, "region_count_sqrt", WORLD_REGION_STATE_TABLE)?,
    })
}

#[derive(Debug, Clone)]
pub struct TerrainChunkRow {
    pub chunk_x: i32,
    pub chunk_z: i32,
    pub dimension: u32,
    pub elevations: Vec<i16>,
    pub original_elevations: Vec<i16>,
    pub water_levels: Vec<i16>,
    pub water_body_types: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub struct PavedTileRow {
    pub entity_id: u64,
    pub tile_type_id: i32,
}

#[derive(Debug, Clone, Copy)]
pub struct ClaimTileRow {
    pub entity_id: u64,
    pub claim_id: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct ClaimStateRoadsRow {
    pub entity_id: u64,
    pub neutral: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct LocationRoadsRow {
    pub entity_id: u64,
    pub x: i32,
    pub z: i32,
    pub dimension: u32,
}

#[derive(Debug, Clone)]
pub struct PavingDescRow {
    pub id: i32,
    pub name: String,
    pub paving_duration: f32,
    pub tier: i32,
    pub input_cargo_id: i32,
    pub consumed: Vec<(i32, u32, bool)>,
}

#[derive(Debug, Clone, Copy)]
pub struct TerraformRecipeRow {
    pub difference: i16,
    pub actions_count: i32,
    pub stamina_per_action: f32,
    pub time_per_action: f32,
}

#[derive(Debug, Clone)]
pub struct RegionNameRow {
    pub id: u16,
    pub name: String,
}

#[derive(Debug, Clone, Copy)]
pub struct WorldRegionRow {
    pub region_width_chunks: u16,
    pub region_height_chunks: u16,
    pub region_count_sqrt: u8,
}

pub fn decode_terrain_chunk(
    row: &[u8],
    fields: &[MirroredField],
    cols: TerrainChunkCols,
    schema: &MirroredSchema,
) -> Result<TerrainChunkRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(TerrainChunkRow {
        chunk_x: cell_i32(&cells[cols.chunk_x], "chunk_x")?,
        chunk_z: cell_i32(&cells[cols.chunk_z], "chunk_z")?,
        dimension: cell_u32(&cells[cols.dimension], "dimension")?,
        elevations: decode_i16_array(&cells[cols.elevations], "elevations")?,
        original_elevations: decode_i16_array(&cells[cols.original_elevations], "original_elevations")?,
        water_levels: decode_i16_array(&cells[cols.water_levels], "water_levels")?,
        water_body_types: decode_u8_array(&cells[cols.water_body_types], "water_body_types")?,
    })
}

pub fn decode_paved_tile(
    row: &[u8],
    fields: &[MirroredField],
    cols: PavedTileCols,
    schema: &MirroredSchema,
) -> Result<PavedTileRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PavedTileRow {
        entity_id: cell_u64(&cells[cols.entity_id], "entity_id")?,
        tile_type_id: cell_i32(&cells[cols.tile_type_id], "tile_type_id")?,
    })
}

pub fn decode_claim_tile(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimTileCols,
    schema: &MirroredSchema,
) -> Result<ClaimTileRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimTileRow {
        entity_id: cell_u64(&cells[cols.entity_id], "entity_id")?,
        claim_id: cell_u64(&cells[cols.claim_id], "claim_id")?,
    })
}

pub fn decode_claim_state_roads(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimStateRoadsCols,
    schema: &MirroredSchema,
) -> Result<ClaimStateRoadsRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimStateRoadsRow {
        entity_id: cell_u64(&cells[cols.entity_id], "entity_id")?,
        neutral: cell_bool(&cells[cols.neutral], "neutral")?,
    })
}

pub fn decode_location_roads(
    row: &[u8],
    fields: &[MirroredField],
    cols: LocationRoadsCols,
    schema: &MirroredSchema,
) -> Result<LocationRoadsRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(LocationRoadsRow {
        entity_id: cell_u64(&cells[cols.entity_id], "entity_id")?,
        x: cell_i32(&cells[cols.x], "x")?,
        z: cell_i32(&cells[cols.z], "z")?,
        dimension: cell_u32(&cells[cols.dimension], "dimension")?,
    })
}

pub fn decode_paving_desc(
    row: &[u8],
    fields: &[MirroredField],
    cols: PavingDescCols,
    schema: &MirroredSchema,
) -> Result<PavingDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PavingDescRow {
        id: cell_i32(&cells[cols.id], "id")?,
        name: cell_string(&cells[cols.name], "name")?,
        paving_duration: cell_f32(&cells[cols.paving_duration], "paving_duration")?,
        tier: cell_i32(&cells[cols.tier], "tier")?,
        input_cargo_id: cell_i32(&cells[cols.input_cargo_id], "input_cargo_id")?,
        consumed: decode_consumed_stacks(&cells[cols.consumed_item_stacks], "consumed_item_stacks")?,
    })
}

pub fn decode_terraform_recipe(
    row: &[u8],
    fields: &[MirroredField],
    cols: TerraformRecipeCols,
    schema: &MirroredSchema,
) -> Result<TerraformRecipeRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(TerraformRecipeRow {
        difference: cell_i16(&cells[cols.difference], "difference")?,
        actions_count: cell_i32(&cells[cols.actions_count], "actions_count")?,
        stamina_per_action: cell_f32(&cells[cols.stamina_per_action], "stamina_per_action")?,
        time_per_action: cell_f32(&cells[cols.time_per_action], "time_per_action")?,
    })
}

pub fn decode_region_name(
    row: &[u8],
    fields: &[MirroredField],
    cols: RegionNameCols,
    schema: &MirroredSchema,
) -> Result<RegionNameRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    let id = cell_i32(&cells[cols.id], "id")?;
    Ok(RegionNameRow {
        id: u16::try_from(id).map_err(|_| anyhow!("region id overflow"))?,
        name: cell_string(&cells[cols.player_facing_name], "player_facing_name")?,
    })
}

pub fn decode_world_region(
    row: &[u8],
    fields: &[MirroredField],
    cols: WorldRegionCols,
    schema: &MirroredSchema,
) -> Result<WorldRegionRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(WorldRegionRow {
        region_width_chunks: cell_u16(&cells[cols.region_width_chunks], "region_width_chunks")?,
        region_height_chunks: cell_u16(&cells[cols.region_height_chunks], "region_height_chunks")?,
        region_count_sqrt: cell_u8(&cells[cols.region_count_sqrt], "region_count_sqrt")?,
    })
}

fn cell_json(cell: &Cell) -> Result<&Value> {
    match cell {
        Cell::Jsonb(v) => Ok(v),
        _ => bail!("expected Jsonb, got {cell:?}"),
    }
}

fn cell_u64(cell: &Cell, ctx: &str) -> Result<u64> {
    let bytes = match cell {
        Cell::Bytea(Some(b)) => b,
        Cell::Bytea(None) => bail!("{ctx}: Bytea is NULL"),
        _ => bail!("{ctx}: expected Bytea, got {cell:?}"),
    };
    if bytes.len() != 8 {
        bail!("{ctx}: expected 8-byte Bytea, got {} bytes", bytes.len());
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(arr))
}

fn cell_i32(cell: &Cell, ctx: &str) -> Result<i32> {
    match cell {
        Cell::Integer(Some(n)) => Ok(*n),
        _ => bail!("{ctx}: expected Integer, got {cell:?}"),
    }
}

/// I16 is mapped to `Cell::Smallint` by relay-protocol. Going through
/// `cell_i32` (Integer) silently dropped every `terraform_recipe_desc` row.
fn cell_i16(cell: &Cell, ctx: &str) -> Result<i16> {
    match cell {
        Cell::Smallint(Some(n)) => Ok(*n),
        Cell::Smallint(None) => bail!("{ctx}: Smallint is NULL"),
        other => bail!("{ctx}: expected Smallint, got {other:?}"),
    }
}

fn cell_u32(cell: &Cell, ctx: &str) -> Result<u32> {
    match cell {
        Cell::Bigint(Some(n)) => u32::try_from(*n).map_err(|_| anyhow!("{ctx}: u32 overflow")),
        _ => bail!("{ctx}: expected Bigint, got {cell:?}"),
    }
}

fn cell_u16(cell: &Cell, ctx: &str) -> Result<u16> {
    let n = cell_i32(cell, ctx)?;
    u16::try_from(n).map_err(|_| anyhow!("{ctx}: u16 overflow"))
}

fn cell_u8(cell: &Cell, ctx: &str) -> Result<u8> {
    match cell {
        Cell::Smallint(Some(n)) => u8::try_from(*n).map_err(|_| anyhow!("{ctx}: u8 overflow")),
        _ => bail!("{ctx}: expected Smallint, got {cell:?}"),
    }
}

fn cell_f32(cell: &Cell, ctx: &str) -> Result<f32> {
    match cell {
        Cell::Real(Some(n)) => Ok(*n),
        Cell::DoublePrecision(Some(n)) => Ok(*n as f32),
        _ => bail!("{ctx}: expected Real, got {cell:?}"),
    }
}

fn cell_string(cell: &Cell, ctx: &str) -> Result<String> {
    match cell {
        Cell::Text(Some(s)) => Ok(s.clone()),
        _ => bail!("{ctx}: expected Text, got {cell:?}"),
    }
}

fn cell_bool(cell: &Cell, ctx: &str) -> Result<bool> {
    match cell {
        Cell::Bool(Some(b)) => Ok(*b),
        _ => bail!("{ctx}: expected Bool, got {cell:?}"),
    }
}

fn decode_i16_array(cell: &Cell, ctx: &str) -> Result<Vec<i16>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("{ctx}: expected array");
    };
    arr.iter()
        .enumerate()
        .map(|(i, v)| {
            let n = v.as_i64().ok_or_else(|| anyhow!("{ctx}[{i}]"))?;
            i16::try_from(n).map_err(|_| anyhow!("{ctx}[{i}] overflow"))
        })
        .collect()
}

fn decode_u8_array(cell: &Cell, ctx: &str) -> Result<Vec<u8>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("{ctx}: expected array");
    };
    arr.iter()
        .enumerate()
        .map(|(i, v)| {
            let n = v.as_i64().ok_or_else(|| anyhow!("{ctx}[{i}]"))?;
            u8::try_from(n).map_err(|_| anyhow!("{ctx}[{i}] overflow"))
        })
        .collect()
}

fn decode_consumed_stacks(cell: &Cell, ctx: &str) -> Result<Vec<(i32, u32, bool)>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, entry) in arr.iter().enumerate() {
        let Value::Object(obj) = entry else {
            continue;
        };
        let item_id = json_i32(obj.get("item_id"), &format!("{ctx}[{i}].item_id"))?;
        let quantity = json_i32(obj.get("quantity"), &format!("{ctx}[{i}].quantity"))?.max(0) as u32;
        let cargo = obj
            .get("item_type")
            .and_then(|v| v.as_object())
            .and_then(|o| o.keys().next())
            .is_some_and(|k| k.eq_ignore_ascii_case("cargo"));
        out.push((item_id, quantity, cargo));
    }
    Ok(out)
}

fn json_i32(v: Option<&Value>, ctx: &str) -> Result<i32> {
    let n = v.and_then(Value::as_i64).ok_or_else(|| anyhow!("{ctx} missing"))?;
    i32::try_from(n).map_err(|_| anyhow!("{ctx} overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::{MirroredField, MirroredSchema, MirroredTable, MirroredType, TableAccess, TableKind};

    fn terraform_schema() -> MirroredSchema {
        MirroredSchema {
            typespace: vec![MirroredType::Product(vec![
                MirroredField {
                    name: Some("difference".into()),
                    ty: MirroredType::I16,
                },
                MirroredField {
                    name: Some("actions_count".into()),
                    ty: MirroredType::I32,
                },
                MirroredField {
                    name: Some("stamina_per_action".into()),
                    ty: MirroredType::F32,
                },
                MirroredField {
                    name: Some("time_per_action".into()),
                    ty: MirroredType::F32,
                },
            ])],
            tables: vec![MirroredTable {
                name: TERRAFORM_RECIPE_DESC_TABLE.into(),
                product_type_ref: 0,
                primary_key: vec![0],
                access: TableAccess::Public,
                kind: TableKind::User,
            }],
        }
    }

    fn encode_recipe(difference: i16, actions_count: i32, stamina: f32, time: f32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(14);
        buf.extend_from_slice(&difference.to_le_bytes());
        buf.extend_from_slice(&actions_count.to_le_bytes());
        buf.extend_from_slice(&stamina.to_le_bytes());
        buf.extend_from_slice(&time.to_le_bytes());
        buf
    }

    #[test]
    fn terraform_recipe_decodes_i16_difference() {
        let schema = terraform_schema();
        let cols = resolve_terraform_recipe_cols(&schema).expect("cols");
        let tbl = schema
            .tables
            .iter()
            .find(|t| t.name == TERRAFORM_RECIPE_DESC_TABLE)
            .expect("table");
        let fields = schema.table_product(tbl).expect("product");
        let row = encode_recipe(-4, 8, 1.5, 0.25);
        let decoded = decode_terraform_recipe(&row, fields, cols, &schema).expect("decode");
        assert_eq!(decoded.difference, -4);
        assert_eq!(decoded.actions_count, 8);
        assert_eq!(decoded.stamina_per_action, 1.5);
        assert_eq!(decoded.time_per_action, 0.25);
    }
}
