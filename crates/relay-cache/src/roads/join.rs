// SPDX-License-Identifier: MIT

//! Entity join maps and facet-masked overlay updates.

use hashbrown::{HashMap, HashSet};

use super::coords::{world_to_local, CHUNKS_PER_SIDE, DEFAULT_CHUNK_SIZE};
use super::grid::{set_claim_index, set_paving, SuperHexTerrainGrid, TileOverlayGrid};
use super::index::ClaimIndexTable;

#[derive(Debug, Default)]
pub struct EntityJoinMaps {
    pub paving_by_entity: HashMap<u64, i32>,
    pub claim_by_entity: HashMap<u64, u64>,
    pub location_by_entity: HashMap<u64, (i32, i32)>,
    pub neutral_claims: HashSet<u64>,
}

impl EntityJoinMaps {
    pub fn new() -> Self {
        Self {
            paving_by_entity: HashMap::new(),
            claim_by_entity: HashMap::new(),
            location_by_entity: HashMap::new(),
            neutral_claims: HashSet::new(),
        }
    }

    pub fn recompute_cell(
        &self,
        region: u16,
        overlay: &mut TileOverlayGrid,
        claim_index: &mut ClaimIndexTable,
        entity_id: u64,
    ) {
        let Some(&(x, z)) = self.location_by_entity.get(&entity_id) else {
            return;
        };
        let Some((lx, lz)) = world_to_local(region, x, z) else {
            return;
        };
        let Some(cell) = overlay.cell_mut(lx, lz) else {
            return;
        };
        if let Some(paving) = self.paving_by_entity.get(&entity_id) {
            set_paving(cell, (*paving).max(0) as u16);
        }
        if let Some(claim_id) = self.claim_by_entity.get(&entity_id) {
            let idx = claim_index.alloc_or_lookup(*claim_id);
            set_claim_index(cell, idx);
        }
    }

    pub fn clear_paving_at(&self, region: u16, overlay: &mut TileOverlayGrid, entity_id: u64) {
        if let Some(&(x, z)) = self.location_by_entity.get(&entity_id) {
            if let Some((lx, lz)) = world_to_local(region, x, z) {
                if let Some(cell) = overlay.cell_mut(lx, lz) {
                    set_paving(cell, 0);
                }
            }
        }
    }

    pub fn clear_claim_at(&self, region: u16, overlay: &mut TileOverlayGrid, entity_id: u64) {
        if let Some(&(x, z)) = self.location_by_entity.get(&entity_id) {
            if let Some((lx, lz)) = world_to_local(region, x, z) {
                if let Some(cell) = overlay.cell_mut(lx, lz) {
                    set_claim_index(cell, 0);
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct TerrainChunkKey {
    pub chunk_x: i32,
    pub chunk_z: i32,
}

#[derive(Debug)]
pub struct TerrainWriter {
    pub terrain_dimension: u32,
    pub absolute_chunks: bool,
    pub origin_chunk_x: i32,
    pub origin_chunk_z: i32,
    pub cell_side: i32,
    pub tracked_chunks: HashMap<(i32, i32), TerrainChunkKey>,
}

impl TerrainWriter {
    pub fn new(region: u16) -> Self {
        let origin = super::coords::region_origin(region);
        Self {
            terrain_dimension: OVERWORLD_DIMENSION,
            absolute_chunks: false,
            origin_chunk_x: origin.x / DEFAULT_CHUNK_SIZE,
            origin_chunk_z: origin.z / DEFAULT_CHUNK_SIZE,
            cell_side: 32,
            tracked_chunks: HashMap::new(),
        }
    }

    pub fn note_chunk(&mut self, chunk_x: i32, chunk_z: i32) {
        if !self.absolute_chunks
            && (chunk_x < 0
                || chunk_z < 0
                || chunk_x >= CHUNKS_PER_SIDE
                || chunk_z >= CHUNKS_PER_SIDE)
        {
            self.absolute_chunks = true;
        }
    }

    pub fn write_chunk(
        &mut self,
        region: u16,
        terrain: &mut SuperHexTerrainGrid,
        chunk_x: i32,
        chunk_z: i32,
        dimension: u32,
        elevations: &[i16],
        original: &[i16],
        water_levels: &[i16],
        water_body_types: &[u8],
    ) {
        if dimension != self.terrain_dimension {
            return;
        }
        self.note_chunk(chunk_x, chunk_z);
        let side = if elevations.len() == 1024 {
            32
        } else {
            let n = elevations.len();
            let s = (n as f64).sqrt().round() as i32;
            if s > 0 && (s as usize) * (s as usize) == n {
                s
            } else {
                32
            }
        };
        self.cell_side = side;

        let (base_cx, base_cz) = if self.absolute_chunks {
            (
                chunk_x - self.origin_chunk_x,
                chunk_z - self.origin_chunk_z,
            )
        } else {
            (chunk_x, chunk_z)
        };

        for i in 0..elevations.len() {
            let lx = (i % side as usize) as i32;
            let lz = (i / side as usize) as i32;
            let super_x = base_cx * side + lx;
            let super_z = base_cz * side + lz;
            let orig = original.get(i).copied().unwrap_or(elevations[i]);
            let water = water_levels.get(i).copied().unwrap_or(i16::MIN);
            let wbt_idx = if water_body_types.len() == elevations.len() * 2 {
                i * 2
            } else {
                i
            };
            let wbt = water_body_types.get(wbt_idx).copied().unwrap_or(0);
            let packed = super::grid::pack_terrain(elevations[i], orig, water, wbt);
            terrain.set(super_x, super_z, packed);
        }

        self.tracked_chunks.insert(
            (chunk_x, chunk_z),
            TerrainChunkKey { chunk_x, chunk_z },
        );
        let _ = region;
    }

    pub fn clear_chunk(&self, terrain: &mut SuperHexTerrainGrid, chunk_x: i32, chunk_z: i32) {
        let (base_cx, base_cz) = if self.absolute_chunks {
            (
                chunk_x - self.origin_chunk_x,
                chunk_z - self.origin_chunk_z,
            )
        } else {
            (chunk_x, chunk_z)
        };
        let side = self.cell_side;
        for lz in 0..side {
            for lx in 0..side {
                let super_x = base_cx * side + lx;
                let super_z = base_cz * side + lz;
                terrain.set(super_x, super_z, 0);
            }
        }
    }

    pub fn set_best_dimension(&mut self, dim: u32) {
        self.terrain_dimension = dim;
    }
}

pub const OVERWORLD_DIMENSION: u32 = 1;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roads::grid::pack_terrain;

    #[test]
    fn region_14_chunk_indexing() {
        let mut terrain = SuperHexTerrainGrid::new();
        let mut writer = TerrainWriter::new(14);
        writer.absolute_chunks = true;
        let n = 32 * 32;
        let mut elevations = vec![0i16; n];
        elevations[0] = 7;
        writer.write_chunk(
            14,
            &mut terrain,
            240,
            160,
            1,
            &elevations,
            &vec![0; n],
            &vec![0; n],
            &vec![0; n],
        );
        assert_eq!(terrain.get(0, 0), pack_terrain(7, 0, 0, 0));
    }
}
