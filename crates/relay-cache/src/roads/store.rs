// SPDX-License-Identifier: MIT

//! Per-region dense roads grid and snapshot export.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use hashbrown::{HashMap, HashSet};
use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use super::coords::{region_origin, small_to_super, world_to_local, SMALL_PER_SUPER};
use super::decode::TerrainChunkRow;
use super::grid::{get_claim_index, get_paving, OVERLAY_BYTES, TERRAIN_BYTES};
use super::index::ClaimIndexTable;
use super::join::{EntityJoinMaps, TerrainWriter, OVERWORLD_DIMENSION};
use super::resource_map::{ResourceTileMap, RESOURCE_MAP_BYTES};

pub const REGION_STATE_LOADING: u32 = 2;
pub const REGION_STATE_READY: u32 = 3;
/// Max hexes accepted by POST /roads/region/{id}/map.
pub const MAX_MAP_QUERY_TILES: usize = 16384;

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
    pub resource_map: ResourceTileMap,
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
            resource_map: ResourceTileMap::new(region),
        }
    }

    pub fn memory_bytes(&self) -> u64 {
        (TERRAIN_BYTES + OVERLAY_BYTES + RESOURCE_MAP_BYTES) as u64 + 1_000_000
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

    /// Sparse window of the map data covering `tiles` (world odd-r coords).
    ///
    /// Same payload as [`snapshot`](Self::snapshot) but only for the
    /// requested tiles: out-of-region and duplicate tiles are skipped.
    pub fn window(&self, tiles: &[(i32, i32)]) -> RegionMapWindowData {
        let origin = region_origin(self.region);
        let claim_table = self.claim_index.claim_table();

        let mut sorted = tiles.to_vec();
        sorted.sort_unstable();
        sorted.dedup();

        let mut window = RegionMapWindowData {
            region: self.region as u32,
            generation: self.generation,
            last_update_unix_ms: self.last_update_unix_ms,
            origin_x: origin.x,
            origin_z: origin.z,
            terrain: Vec::new(),
            tiles: Vec::with_capacity(sorted.len()),
        };

        let mut supers: HashSet<(i32, i32)> = HashSet::with_capacity(sorted.len());
        for &(x, z) in &sorted {
            let Some((lx, lz)) = world_to_local(self.region, x, z) else {
                continue;
            };
            let cell = self.overlay.get(lx, lz);
            let claim_index = get_claim_index(cell);
            let claim_entity_id = if claim_index == 0 {
                0
            } else {
                claim_table.get(claim_index as usize).copied().unwrap_or(0)
            };
            window.tiles.push(MapTileData {
                x,
                z,
                paving_type_id: get_paving(cell),
                claim_entity_id,
            });
            supers.insert(small_to_super(lx, lz));
        }

        let mut super_list: Vec<(i32, i32)> = supers.into_iter().collect();
        super_list.sort_unstable();
        window.terrain = super_list
            .into_iter()
            .map(|(sx, sz)| MapSuperHexData {
                x: origin.x + sx * SMALL_PER_SUPER,
                z: origin.z + sz * SMALL_PER_SUPER,
                terrain: self.terrain.get(sx, sz),
            })
            .collect();
        window
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

#[derive(Debug)]
pub struct MapTileData {
    pub x: i32,
    pub z: i32,
    pub paving_type_id: u16,
    pub claim_entity_id: u64,
}

#[derive(Debug)]
pub struct MapSuperHexData {
    /// World coord of the super-hex block's `(0, 0)` small tile.
    pub x: i32,
    pub z: i32,
    pub terrain: u64,
}

#[derive(Debug)]
pub struct RegionMapWindowData {
    pub region: u32,
    pub generation: u64,
    pub last_update_unix_ms: i64,
    pub origin_x: i32,
    pub origin_z: i32,
    pub terrain: Vec<MapSuperHexData>,
    pub tiles: Vec<MapTileData>,
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
    use crate::roads::coords::terrain_index;
    use crate::roads::grid::{get_claim_index, pack_terrain, set_claim_index, set_paving};

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

    #[test]
    fn window_matches_full_snapshot() {
        let mut grid = RoadsRegionGrid::new(1);
        grid.claim_index.alloc_or_lookup(42);
        grid.claim_index.alloc_or_lookup(99);
        if let Some(cell) = grid.overlay.cell_mut(10, 20) {
            set_paving(cell, 5);
            set_claim_index(cell, 1);
        }
        if let Some(cell) = grid.overlay.cell_mut(12, 21) {
            set_paving(cell, 7);
            set_claim_index(cell, 2);
        }
        grid.terrain.set(3, 6, pack_terrain(10, -5, 20, 2));
        grid.terrain.set(4, 7, pack_terrain(-3, 1, -2, 9));

        // Duplicates, unsorted input, and an out-of-region tile are folded
        // away; region 1's origin is (0, 0) so world == local here.
        let window = grid.window(&[(12, 21), (10, 20), (10, 20), (-1, 0)]);
        let snap = grid.snapshot();

        assert_eq!((window.origin_x, window.origin_z), (snap.origin_x, snap.origin_z));
        assert_eq!(window.generation, snap.generation);
        assert_eq!(window.tiles.len(), 2);
        assert_eq!((window.tiles[0].x, window.tiles[0].z), (10, 20));
        assert_eq!(window.tiles[0].paving_type_id, 5);
        assert_eq!(window.tiles[0].claim_entity_id, snap.claim_table[1]);
        assert_eq!((window.tiles[1].x, window.tiles[1].z), (12, 21));
        assert_eq!(window.tiles[1].paving_type_id, 7);
        assert_eq!(window.tiles[1].claim_entity_id, snap.claim_table[2]);

        // One super hex per distinct 3x3 block, matching the dense snapshot
        // bytes at the covering terrain index.
        assert_eq!(window.terrain.len(), 2);
        assert_eq!((window.terrain[0].x, window.terrain[0].z), (9, 18));
        assert_eq!(window.terrain[0].terrain, pack_terrain(10, -5, 20, 2));
        assert_eq!((window.terrain[1].x, window.terrain[1].z), (12, 21));
        assert_eq!(window.terrain[1].terrain, pack_terrain(-3, 1, -2, 9));
        for super_hex in &window.terrain {
            let idx = terrain_index(super_hex.x / 3, super_hex.z / 3).unwrap();
            let bytes = &snap.terrain[idx * 8..idx * 8 + 8];
            assert_eq!(super_hex.terrain, u64::from_le_bytes(bytes.try_into().unwrap()));
        }
    }
}
