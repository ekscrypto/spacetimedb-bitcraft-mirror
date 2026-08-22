// SPDX-License-Identifier: MIT

//! Global paving/terraform catalogs and region metadata.

use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;

use super::coords::{region_origin, region_rx_rz, CHUNKS_PER_SIDE};
use super::decode::{
    decode_paving_desc, decode_region_name, decode_terraform_recipe, decode_world_region, PavingDescRow,
    PAVING_TILE_DESC_TABLE, REGION_NAME_TABLE, TERRAFORM_RECIPE_DESC_TABLE, WORLD_REGION_STATE_TABLE,
};
use super::meta::RoadsTableMeta;
use super::store::{RegionRoadStatus, RoadsRegionHandle};

#[derive(Debug)]
pub struct GlobalRoadsCatalog {
    pub region_width_chunks: u32,
    pub region_height_chunks: u32,
    pub region_count_sqrt: u8,
    pub region_names: HashMap<u16, String>,
    pub paving: Vec<PavingDescRow>,
    pub terraform: Vec<TerraformRecipeRow>,
    pub ready: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct TerraformRecipeRow {
    pub difference: i16,
    pub actions_count: i32,
    pub stamina_per_action: f32,
    pub time_per_action: f32,
}

impl GlobalRoadsCatalog {
    pub fn new() -> Self {
        Self {
            region_width_chunks: CHUNKS_PER_SIDE as u32,
            region_height_chunks: CHUNKS_PER_SIDE as u32,
            region_count_sqrt: 5,
            region_names: HashMap::new(),
            paving: Vec::new(),
            terraform: Vec::new(),
            ready: false,
        }
    }

    pub fn mark_ready(&mut self) {
        self.ready = true;
    }
}

impl Default for GlobalRoadsCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct RoadsFleet {
    pub regions: Mutex<Vec<Arc<RoadsRegionHandle>>>,
    pub catalog: Arc<RwLock<GlobalRoadsCatalog>>,
}

impl RoadsFleet {
    pub fn new(catalog: Arc<RwLock<GlobalRoadsCatalog>>) -> Self {
        Self {
            regions: Mutex::new(Vec::new()),
            catalog,
        }
    }

    pub fn push_region(&self, handle: Arc<RoadsRegionHandle>) {
        self.regions.lock().push(handle);
    }

    pub fn region_handles(&self) -> Vec<Arc<RoadsRegionHandle>> {
        self.regions.lock().clone()
    }

    pub fn region_handle(&self, region: u32) -> Option<Arc<RoadsRegionHandle>> {
        self.regions.lock().iter().find(|h| h.region == region).cloned()
    }

    pub fn health(&self) -> RoadsFleetHealth {
        let mut regions_ready = 0u32;
        let mut total_memory = 0u64;
        let mut statuses = Vec::new();
        for h in self.regions.lock().iter() {
            let grid = h.grid.read();
            let st = grid.status();
            total_memory += st.memory_bytes;
            if grid.ready {
                regions_ready += 1;
            }
            statuses.push(st);
        }
        let regions_total = self.regions.lock().len() as u32;
        RoadsFleetHealth {
            ready: regions_total > 0 && regions_ready == regions_total,
            regions_ready,
            regions_total,
            total_memory_bytes: total_memory,
            regions: statuses,
        }
    }

    pub fn regions_response(&self) -> RegionsResponseData {
        let catalog = self.catalog.read();
        let mut regions = Vec::new();
        for h in self.regions.lock().iter() {
            let grid = h.grid.read();
            let (rx, rz) = region_rx_rz(h.region as u16);
            let origin = region_origin(h.region as u16);
            let name = catalog
                .region_names
                .get(&(h.region as u16))
                .cloned()
                .unwrap_or_else(|| format!("Region {}", h.region));
            regions.push(RegionEntry {
                id: h.region,
                name,
                live: grid.ready,
                rx,
                rz,
                origin_x: origin.x,
                origin_z: origin.z,
            });
        }
        regions.sort_by_key(|r| r.id);
        RegionsResponseData {
            regions,
            region_width_chunks: catalog.region_width_chunks,
            region_height_chunks: catalog.region_height_chunks,
        }
    }
}

pub struct RoadsFleetHealth {
    pub ready: bool,
    pub regions_ready: u32,
    pub regions_total: u32,
    pub total_memory_bytes: u64,
    pub regions: Vec<RegionRoadStatus>,
}

pub struct RegionsResponseData {
    pub regions: Vec<RegionEntry>,
    pub region_width_chunks: u32,
    pub region_height_chunks: u32,
}

pub struct RegionEntry {
    pub id: u32,
    pub name: String,
    pub live: bool,
    pub rx: i32,
    pub rz: i32,
    pub origin_x: i32,
    pub origin_z: i32,
}

pub fn apply_global_insert(
    catalog: &mut GlobalRoadsCatalog,
    meta: &RoadsTableMeta,
    schema: &relay_protocol::MirroredSchema,
    table: &str,
    row: &[u8],
) -> anyhow::Result<()> {
    match table {
        PAVING_TILE_DESC_TABLE => {
            let Some(cols) = meta.paving_desc else {
                return Ok(());
            };
            let Some(fields) = meta.paving_desc_fields.as_ref() else {
                return Ok(());
            };
            let r = decode_paving_desc(row, fields, cols, schema)?;
            if let Some(slot) = catalog.paving.iter_mut().find(|p| p.id == r.id) {
                *slot = r;
            } else {
                catalog.paving.push(r);
            }
        }
        TERRAFORM_RECIPE_DESC_TABLE => {
            let Some(cols) = meta.terraform_recipe else {
                return Ok(());
            };
            let Some(fields) = meta.terraform_recipe_fields.as_ref() else {
                return Ok(());
            };
            let r = decode_terraform_recipe(row, fields, cols, schema)?;
            catalog.terraform.push(TerraformRecipeRow {
                difference: r.difference,
                actions_count: r.actions_count,
                stamina_per_action: r.stamina_per_action,
                time_per_action: r.time_per_action,
            });
        }
        REGION_NAME_TABLE => {
            let Some(cols) = meta.region_name else {
                return Ok(());
            };
            let Some(fields) = meta.region_name_fields.as_ref() else {
                return Ok(());
            };
            let r = decode_region_name(row, fields, cols, schema)?;
            catalog.region_names.insert(r.id, r.name);
        }
        WORLD_REGION_STATE_TABLE => {
            let Some(cols) = meta.world_region else {
                return Ok(());
            };
            let Some(fields) = meta.world_region_fields.as_ref() else {
                return Ok(());
            };
            let r = decode_world_region(row, fields, cols, schema)?;
            catalog.region_width_chunks = r.region_width_chunks as u32;
            catalog.region_height_chunks = r.region_height_chunks as u32;
            catalog.region_count_sqrt = r.region_count_sqrt;
        }
        _ => {}
    }
    Ok(())
}

pub fn apply_global_delete(
    catalog: &mut GlobalRoadsCatalog,
    meta: &RoadsTableMeta,
    schema: &relay_protocol::MirroredSchema,
    table: &str,
    row: &[u8],
) -> anyhow::Result<()> {
    match table {
        PAVING_TILE_DESC_TABLE => {
            let Some(cols) = meta.paving_desc else {
                return Ok(());
            };
            let Some(fields) = meta.paving_desc_fields.as_ref() else {
                return Ok(());
            };
            let r = decode_paving_desc(row, fields, cols, schema)?;
            catalog.paving.retain(|p| p.id != r.id);
        }
        REGION_NAME_TABLE => {
            let Some(cols) = meta.region_name else {
                return Ok(());
            };
            let Some(fields) = meta.region_name_fields.as_ref() else {
                return Ok(());
            };
            let r = decode_region_name(row, fields, cols, schema)?;
            catalog.region_names.remove(&r.id);
        }
        _ => {}
    }
    Ok(())
}
