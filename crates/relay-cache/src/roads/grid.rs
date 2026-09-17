// SPDX-License-Identifier: MIT

//! Dense terrain and overlay grids.

use super::coords::{overlay_index, terrain_index, REGION_SIDE, SUPER_SIDE};

pub const TERRAIN_BYTES: usize = (SUPER_SIDE as usize) * (SUPER_SIDE as usize) * 8;
pub const OVERLAY_BYTES: usize = (REGION_SIDE as usize) * (REGION_SIDE as usize) * 4;

pub fn pack_terrain(elev: i16, orig: i16, water: i16, wbt: u8) -> u64 {
    (elev as u16 as u64) | ((orig as u16 as u64) << 16) | ((water as u16 as u64) << 32) | ((wbt as u64) << 48)
}

/// `(elevation, water_level)` of a packed terrain cell (both signed). A tile
/// is underwater when `elevation < water_level`; missing water data arrives
/// as `i16::MIN`, which never classifies as water.
pub fn unpack_terrain_water(packed: u64) -> (i16, i16) {
    let elev = (packed & 0xFFFF) as u16 as i16;
    let water = ((packed >> 32) & 0xFFFF) as u16 as i16;
    (elev, water)
}

pub fn pack_overlay(claim_index: u16, paving: u16) -> u32 {
    (paving as u32) | ((claim_index as u32) << 16)
}

pub fn set_paving(cell: &mut u32, paving: u16) {
    *cell = (*cell & 0xFFFF_0000) | (paving as u32);
}

pub fn set_claim_index(cell: &mut u32, idx: u16) {
    *cell = (*cell & 0x0000_FFFF) | ((idx as u32) << 16);
}

pub fn get_paving(cell: u32) -> u16 {
    (cell & 0xFFFF) as u16
}

pub fn get_claim_index(cell: u32) -> u16 {
    (cell >> 16) as u16
}

pub struct SuperHexTerrainGrid {
    cells: Vec<u64>,
}

impl SuperHexTerrainGrid {
    pub fn new() -> Self {
        Self {
            cells: vec![0u64; SUPER_SIDE as usize * SUPER_SIDE as usize],
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        bytemuck_cast(&self.cells)
    }

    pub fn set(&mut self, super_x: i32, super_z: i32, value: u64) {
        if let Some(idx) = terrain_index(super_x, super_z) {
            self.cells[idx] = value;
        }
    }

    pub fn get(&self, super_x: i32, super_z: i32) -> u64 {
        terrain_index(super_x, super_z).map(|idx| self.cells[idx]).unwrap_or(0)
    }

    /// `side`×`side` window of packed terrain cells (u64 LE, row-major)
    /// anchored at region-local super coords `origin` (which may be negative
    /// or overhang the region edge — out-of-region cells are zero). Same
    /// row-slice technique as `ResourceTileMap::window`.
    pub fn window(&self, origin: (i32, i32), side: usize) -> Vec<u8> {
        let mut out = vec![0u64; side * side];
        let grid = SUPER_SIDE;
        let w = side as i32;
        for r in 0..side {
            let sz = origin.1 + r as i32;
            if !(0..grid).contains(&sz) {
                continue;
            }
            let start = origin.0.max(0);
            let end = (origin.0 + w).min(grid);
            if start >= end {
                continue;
            }
            let row_base = (sz as usize) * (SUPER_SIDE as usize);
            let dst = r * side + (start - origin.0) as usize;
            let count = (end - start) as usize;
            out[dst..dst + count].copy_from_slice(&self.cells[row_base + start as usize..row_base + end as usize]);
        }
        let mut bytes = Vec::with_capacity(out.len() * 8);
        for cell in out {
            bytes.extend_from_slice(&cell.to_le_bytes());
        }
        bytes
    }
}

impl std::fmt::Debug for SuperHexTerrainGrid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SuperHexTerrainGrid")
            .field("cells", &self.cells.len())
            .finish()
    }
}

impl std::fmt::Debug for TileOverlayGrid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TileOverlayGrid")
            .field("cells", &self.cells.len())
            .finish()
    }
}

impl Default for SuperHexTerrainGrid {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TileOverlayGrid {
    cells: Vec<u32>,
}

impl TileOverlayGrid {
    pub fn new() -> Self {
        Self {
            cells: vec![0u32; REGION_SIDE as usize * REGION_SIDE as usize],
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        bytemuck_cast(&self.cells)
    }

    pub fn cell_mut(&mut self, lx: i32, lz: i32) -> Option<&mut u32> {
        overlay_index(lx, lz).map(|idx| &mut self.cells[idx])
    }

    pub fn get(&self, lx: i32, lz: i32) -> u32 {
        overlay_index(lx, lz).map(|idx| self.cells[idx]).unwrap_or(0)
    }

    pub fn paved_tile_count(&self) -> u32 {
        self.cells.iter().filter(|c| get_paving(**c) != 0).count() as u32
    }

    pub fn claim_tile_count(&self) -> u32 {
        self.cells.iter().filter(|c| get_claim_index(**c) != 0).count() as u32
    }
}

impl Default for TileOverlayGrid {
    fn default() -> Self {
        Self::new()
    }
}

fn bytemuck_cast<T: Sized>(slice: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(slice.as_ptr().cast::<u8>(), std::mem::size_of_val(slice)) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn facet_mask_preservation() {
        let mut cell = 0u32;
        set_paving(&mut cell, 42);
        assert_eq!(cell, 0x0000_002A);
        set_claim_index(&mut cell, 7);
        assert_eq!(cell, 0x0007_002A);
        set_paving(&mut cell, 99);
        assert_eq!(cell, 0x0007_0063);
        set_claim_index(&mut cell, 0);
        assert_eq!(cell, 0x0000_0063);
    }

    #[test]
    fn terrain_window_clamps_negative_origin_and_region_edge() {
        let mut grid = SuperHexTerrainGrid::new();
        grid.set(0, 0, pack_terrain(10, 8, 2, 1));
        grid.set(2, 1, pack_terrain(30, 20, 5, 3));

        // origin (−1, −1): cell (r, c) holds super (c−1, r−1).
        let bytes = grid.window((-1, -1), 4);
        assert_eq!(bytes.len(), 4 * 4 * 8);
        let cell =
            |r: usize, c: usize| u64::from_le_bytes(bytes[(r * 4 + c) * 8..(r * 4 + c) * 8 + 8].try_into().unwrap());
        assert_eq!(cell(0, 0), 0, "super (−1, −1) is outside");
        assert_eq!(cell(1, 1), pack_terrain(10, 8, 2, 1));
        assert_eq!(cell(2, 3), pack_terrain(30, 20, 5, 3));
        assert_eq!(cell(0, 3), 0);

        // A window fully past the region edge is all zeros.
        assert!(grid.window((SUPER_SIDE, SUPER_SIDE), 4).iter().all(|b| *b == 0));
    }
}
