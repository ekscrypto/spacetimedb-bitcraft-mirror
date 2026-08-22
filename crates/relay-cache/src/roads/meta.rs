// SPDX-License-Identifier: MIT

//! Cached field metadata for roads table decoders.

use anyhow::{anyhow, Result};
use relay_protocol::{MirroredField, MirroredSchema};

use super::decode::{
    resolve_claim_state_roads_cols, resolve_claim_tile_cols, resolve_location_roads_cols, resolve_paved_tile_cols,
    resolve_paving_desc_cols, resolve_region_name_cols, resolve_terraform_recipe_cols, resolve_terrain_chunk_cols,
    resolve_world_region_cols, ClaimStateRoadsCols, ClaimTileCols, LocationRoadsCols, PavedTileCols, PavingDescCols,
    RegionNameCols, TerraformRecipeCols, TerrainChunkCols, WorldRegionCols, CLAIM_STATE_TABLE, CLAIM_TILE_TABLE,
    LOCATION_TABLE, PAVED_TILE_TABLE, PAVING_TILE_DESC_TABLE, REGION_NAME_TABLE, TERRAFORM_RECIPE_DESC_TABLE,
    TERRAIN_CHUNK_TABLE, WORLD_REGION_STATE_TABLE,
};
use crate::decode::{self, ResourceCols, ResourceFast, RESOURCE_TABLE};

pub struct RoadsTableMeta {
    pub terrain_chunk: TerrainChunkCols,
    pub paved_tile: PavedTileCols,
    pub claim_tile: ClaimTileCols,
    pub claim_state: ClaimStateRoadsCols,
    pub location: LocationRoadsCols,
    pub paving_desc: Option<PavingDescCols>,
    pub terraform_recipe: Option<TerraformRecipeCols>,
    pub region_name: Option<RegionNameCols>,
    pub world_region: Option<WorldRegionCols>,
    pub terrain_chunk_fields: Vec<MirroredField>,
    pub paved_tile_fields: Vec<MirroredField>,
    pub claim_tile_fields: Vec<MirroredField>,
    pub claim_state_fields: Vec<MirroredField>,
    pub location_fields: Vec<MirroredField>,
    pub resource: Option<ResourceCols>,
    pub resource_fast: Option<ResourceFast>,
    pub resource_fields: Vec<MirroredField>,
    pub paving_desc_fields: Option<Vec<MirroredField>>,
    pub terraform_recipe_fields: Option<Vec<MirroredField>>,
    pub region_name_fields: Option<Vec<MirroredField>>,
    pub world_region_fields: Option<Vec<MirroredField>>,
}

impl RoadsTableMeta {
    pub fn from_schema_regional(schema: &MirroredSchema) -> Result<Self> {
        let resource_fields = fields_owned(schema, RESOURCE_TABLE)?;
        Ok(Self {
            terrain_chunk: resolve_terrain_chunk_cols(schema)?,
            paved_tile: resolve_paved_tile_cols(schema)?,
            claim_tile: resolve_claim_tile_cols(schema)?,
            claim_state: resolve_claim_state_roads_cols(schema)?,
            location: resolve_location_roads_cols(schema)?,
            paving_desc: None,
            terraform_recipe: None,
            region_name: None,
            world_region: None,
            terrain_chunk_fields: fields_owned(schema, TERRAIN_CHUNK_TABLE)?,
            paved_tile_fields: fields_owned(schema, PAVED_TILE_TABLE)?,
            claim_tile_fields: fields_owned(schema, CLAIM_TILE_TABLE)?,
            claim_state_fields: fields_owned(schema, CLAIM_STATE_TABLE)?,
            location_fields: fields_owned(schema, LOCATION_TABLE)?,
            resource: Some(decode::resolve_resource_cols(schema)?),
            resource_fast: ResourceFast::try_from_fields(&resource_fields, schema),
            resource_fields,
            paving_desc_fields: None,
            terraform_recipe_fields: None,
            region_name_fields: None,
            world_region_fields: None,
        })
    }

    pub fn from_schema_global(schema: &MirroredSchema) -> Result<Self> {
        Ok(Self {
            terrain_chunk: resolve_terrain_chunk_cols(schema).unwrap_or(dummy_terrain_cols()),
            paved_tile: resolve_paved_tile_cols(schema).unwrap_or(dummy_paved_cols()),
            claim_tile: resolve_claim_tile_cols(schema).unwrap_or(dummy_claim_tile_cols()),
            claim_state: resolve_claim_state_roads_cols(schema).unwrap_or(dummy_claim_state_cols()),
            location: resolve_location_roads_cols(schema).unwrap_or(dummy_location_cols()),
            paving_desc: resolve_paving_desc_cols(schema).ok(),
            terraform_recipe: resolve_terraform_recipe_cols(schema).ok(),
            region_name: resolve_region_name_cols(schema).ok(),
            world_region: resolve_world_region_cols(schema).ok(),
            terrain_chunk_fields: fields_owned(schema, TERRAIN_CHUNK_TABLE).unwrap_or_default(),
            paved_tile_fields: fields_owned(schema, PAVED_TILE_TABLE).unwrap_or_default(),
            claim_tile_fields: fields_owned(schema, CLAIM_TILE_TABLE).unwrap_or_default(),
            claim_state_fields: fields_owned(schema, CLAIM_STATE_TABLE).unwrap_or_default(),
            location_fields: fields_owned(schema, LOCATION_TABLE).unwrap_or_default(),
            resource: decode::resolve_resource_cols(schema).ok(),
            resource_fast: fields_owned(schema, RESOURCE_TABLE)
                .ok()
                .and_then(|fields| ResourceFast::try_from_fields(&fields, schema)),
            resource_fields: fields_owned(schema, RESOURCE_TABLE).unwrap_or_default(),
            paving_desc_fields: fields_owned(schema, PAVING_TILE_DESC_TABLE).ok(),
            terraform_recipe_fields: fields_owned(schema, TERRAFORM_RECIPE_DESC_TABLE).ok(),
            region_name_fields: fields_owned(schema, REGION_NAME_TABLE).ok(),
            world_region_fields: fields_owned(schema, WORLD_REGION_STATE_TABLE).ok(),
        })
    }
}

fn fields_owned(schema: &MirroredSchema, table: &str) -> Result<Vec<MirroredField>> {
    let tbl = schema
        .tables
        .iter()
        .find(|t| t.name == table)
        .ok_or_else(|| anyhow!("schema has no table `{table}`"))?;
    let fields = schema
        .table_product(tbl)
        .ok_or_else(|| anyhow!("table `{table}` is not a Product"))?;
    Ok(fields.to_vec())
}

fn dummy_terrain_cols() -> TerrainChunkCols {
    TerrainChunkCols {
        chunk_x: 0,
        chunk_z: 0,
        dimension: 0,
        elevations: 0,
        original_elevations: 0,
        water_levels: 0,
        water_body_types: 0,
    }
}

fn dummy_paved_cols() -> PavedTileCols {
    PavedTileCols {
        entity_id: 0,
        tile_type_id: 0,
    }
}

fn dummy_claim_tile_cols() -> ClaimTileCols {
    ClaimTileCols {
        entity_id: 0,
        claim_id: 0,
    }
}

fn dummy_claim_state_cols() -> ClaimStateRoadsCols {
    ClaimStateRoadsCols {
        entity_id: 0,
        neutral: 0,
    }
}

fn dummy_location_cols() -> LocationRoadsCols {
    LocationRoadsCols {
        entity_id: 0,
        x: 0,
        z: 0,
        dimension: 0,
    }
}
