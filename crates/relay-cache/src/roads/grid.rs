// SPDX-License-Identifier: MIT

//! Dense terrain and overlay grids.

use super::coords::{overlay_index, terrain_index, REGION_SIDE, SUPER_SIDE};

pub const TERRAIN_BYTES: usize = (SUPER_SIDE as usize) * (SUPER_SIDE as usize) * 8;
pub const OVERLAY_BYTES: usize = (REGION_SIDE as usize) * (REGION_SIDE as usize) * 4;

pub fn pack_terrain(elev: i16, orig: i16, water: i16, wbt: u8) -> u64 {
    (elev as u16 as u64)
        | ((orig as u16 as u64) << 16)
        | ((water as u16 as u64) << 32)
        | ((wbt as u64) << 48)
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
        terrain_index(super_x, super_z)
            .map(|idx| self.cells[idx])
            .unwrap_or(0)
    }
}

impl std::fmt::Debug for SuperHexTerrainGrid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SuperHexTerrainGrid").field("cells", &self.cells.len()).finish()
    }
}

impl std::fmt::Debug for TileOverlayGrid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TileOverlayGrid").field("cells", &self.cells.len()).finish()
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
        overlay_index(lx, lz)
            .map(|idx| self.cells[idx])
            .unwrap_or(0)
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
    unsafe {
        std::slice::from_raw_parts(
            slice.as_ptr().cast::<u8>(),
            std::mem::size_of_val(slice),
        )
    }
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
}
