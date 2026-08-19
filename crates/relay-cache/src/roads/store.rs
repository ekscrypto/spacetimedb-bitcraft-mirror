// SPDX-License-Identifier: MIT

//! Per-region dense roads grid and snapshot export.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use hashbrown::HashMap;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use super::coords::region_origin;
use super::decode::TerrainChunkRow;
use super::grid::{OVERLAY_BYTES, TERRAIN_BYTES};
use super::index::ClaimIndexTable;
use super::join::{EntityJoinMaps, TerrainWriter, OVERWORLD_DIMENSION};

pub const REGION_STATE_LOADING: u32 = 2;
pub const REGION_STATE_READY: u32 = 3;

#[derive(Debug)]
pub struct RoadsRegionGrid {
    pub region: u16,
    pub ready: bool,
    pub generation: u64,
    pub loaded_at_unix_ms: i64,
    pub last_update_unix_ms: i64,
    pub error: String,
    pub terrain: super::grid::SuperHexTerrainGrid,
    pub overlay: super::grid::TileOverlayGrid,
    pub claim_index: ClaimIndexTable,
    pub join: EntityJoinMaps,
    pub terrain_writer: TerrainWriter,
    pub dim_hist: HashMap<u32, usize>,
    pub terrain_chunks: HashMap<(i32, i32), u32>,
    pub pending_terrain: Vec<TerrainChunkRow>,
}

impl RoadsRegionGrid {
    pub fn new(region: u16) -> Self {
        let now = now_ms();
        Self {
            region,
            ready: false,
            generation: 0,
            loaded_at_unix_ms: now,
            last_update_unix_ms: now,
            error: String::new(),
            terrain: super::grid::SuperHexTerrainGrid::new(),
            overlay: super::grid::TileOverlayGrid::new(),
            claim_index: ClaimIndexTable::new(),
            join: EntityJoinMaps::new(),
            terrain_writer: TerrainWriter::new(region),
            dim_hist: HashMap::new(),
            terrain_chunks: HashMap::new(),
            pending_terrain: Vec::new(),
        }
    }

    pub fn memory_bytes(&self) -> u64 {
        (TERRAIN_BYTES + OVERLAY_BYTES) as u64 + 1_000_000
    }

    pub fn bump_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
        self.last_update_unix_ms = now_ms();
    }

    pub fn mark_ready(&mut self) {
        self.ready = true;
        self.loaded_at_unix_ms = now_ms();
        self.last_update_unix_ms = self.loaded_at_unix_ms;
    }

    pub fn best_terrain_dimension(&self) -> u32 {
        if self.dim_hist.contains_key(&OVERWORLD_DIMENSION) {
            OVERWORLD_DIMENSION
        } else {
            self.dim_hist
                .iter()
                .max_by_key(|(_, n)| *n)
                .map(|(d, _)| *d)
                .unwrap_or(OVERWORLD_DIMENSION)
        }
    }

    pub fn snapshot(&self) -> RegionMapSnapshot {
        let origin = region_origin(self.region);
        let terrain_bytes = self.terrain.as_bytes().to_vec();
        let overlay_bytes = self.overlay.as_bytes().to_vec();
        let claim_table = self.claim_index.claim_table().to_vec();
        let mut neutral: Vec<u64> = self.join.neutral_claims.iter().copied().collect();
        neutral.sort_unstable();

        let etag = compute_etag(&terrain_bytes, &overlay_bytes, &claim_table);

        RegionMapSnapshot {
            region: self.region as u32,
            generation: self.generation,
            last_update_unix_ms: self.last_update_unix_ms,
            origin_x: origin.x,
            origin_z: origin.z,
            claim_table,
            neutral_claim_ids: neutral,
            terrain: terrain_bytes,
            overlay: overlay_bytes,
            etag,
        }
    }

    pub fn status(&self) -> RegionRoadStatus {
        RegionRoadStatus {
            region: self.region as u32,
            state: if self.ready {
                REGION_STATE_READY
            } else {
                REGION_STATE_LOADING
            },
            connected: true,
            loaded_at_unix_ms: self.loaded_at_unix_ms,
            last_update_unix_ms: self.last_update_unix_ms,
            error: self.error.clone(),
            memory_bytes: self.memory_bytes(),
            claim_count: self.claim_index.distinct_claim_count(),
            paved_tile_count: self.overlay.paved_tile_count(),
            claim_tile_count: self.overlay.claim_tile_count(),
        }
    }
}

pub struct RegionMapSnapshot {
    pub region: u32,
    pub generation: u64,
    pub last_update_unix_ms: i64,
    pub origin_x: i32,
    pub origin_z: i32,
    pub claim_table: Vec<u64>,
    pub neutral_claim_ids: Vec<u64>,
    pub terrain: Vec<u8>,
    pub overlay: Vec<u8>,
    pub etag: String,
}

pub struct RegionRoadStatus {
    pub region: u32,
    pub state: u32,
    pub connected: bool,
    pub loaded_at_unix_ms: i64,
    pub last_update_unix_ms: i64,
    pub error: String,
    pub memory_bytes: u64,
    pub claim_count: u32,
    pub paved_tile_count: u32,
    pub claim_tile_count: u32,
}

#[derive(Debug)]
pub struct RoadsRegionHandle {
    pub region: u32,
    pub grid: Arc<RwLock<RoadsRegionGrid>>,
}

fn compute_etag(terrain: &[u8], overlay: &[u8], claim_table: &[u64]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(terrain);
    hasher.update(overlay);
    for id in claim_table {
        hasher.update(id.to_le_bytes());
    }
    hex::encode(hasher.finalize())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roads::grid::{get_claim_index, set_claim_index, set_paving};

    #[test]
    fn snapshot_atomicity() {
        let mut grid = RoadsRegionGrid::new(1);
        grid.claim_index.alloc_or_lookup(42);
        grid.claim_index.alloc_or_lookup(99);
        if let Some(cell) = grid.overlay.cell_mut(10, 20) {
            set_paving(cell, 5);
            set_claim_index(cell, 1);
        }
        let snap = grid.snapshot();
        assert_eq!(snap.claim_table.len(), 3);
        assert_eq!(snap.claim_table[1], 42);
        let idx = (20usize * 7680 + 10) * 4;
        let cell = u32::from_le_bytes(snap.overlay[idx..idx + 4].try_into().unwrap());
        let claim_index = get_claim_index(cell);
        assert_eq!(snap.claim_table[claim_index as usize], 42);
    }
}
