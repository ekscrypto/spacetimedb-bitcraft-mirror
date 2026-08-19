// SPDX-License-Identifier: MIT

//! Coordinate helpers for the roads dense grid cache.

pub const REGION_COUNT_SQRT: i32 = 5;
pub const CHUNKS_PER_SIDE: i32 = 80;
pub const DEFAULT_CHUNK_SIZE: i32 = 96;
pub const REGION_SIDE: i32 = CHUNKS_PER_SIDE * DEFAULT_CHUNK_SIZE;
pub const SUPER_SIDE: i32 = CHUNKS_PER_SIDE * 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Hex {
    pub x: i32,
    pub z: i32,
}

pub fn region_rx_rz(id: u16) -> (i32, i32) {
    let idx = (id as i32) - 1;
    (
        idx.rem_euclid(REGION_COUNT_SQRT),
        idx.div_euclid(REGION_COUNT_SQRT),
    )
}

pub fn region_origin(id: u16) -> Hex {
    let (rx, rz) = region_rx_rz(id);
    Hex::new(rx * REGION_SIDE, rz * REGION_SIDE)
}

impl Hex {
    pub fn new(x: i32, z: i32) -> Self {
        Self { x, z }
    }
}

/// Region-local small-hex overlay index; returns None when out of bounds.
pub fn overlay_index(lx: i32, lz: i32) -> Option<usize> {
    if !(0..REGION_SIDE).contains(&lx) || !(0..REGION_SIDE).contains(&lz) {
        return None;
    }
    Some((lz as usize) * (REGION_SIDE as usize) + (lx as usize))
}

/// Region-local super-hex terrain index.
pub fn terrain_index(super_x: i32, super_z: i32) -> Option<usize> {
    if !(0..SUPER_SIDE).contains(&super_x) || !(0..SUPER_SIDE).contains(&super_z) {
        return None;
    }
    Some((super_z as usize) * (SUPER_SIDE as usize) + (super_x as usize))
}

/// World small-hex coords → region-local `(lx, lz)`.
pub fn world_to_local(region: u16, x: i32, z: i32) -> Option<(i32, i32)> {
    let origin = region_origin(region);
    let lx = x - origin.x;
    let lz = z - origin.z;
    if !(0..REGION_SIDE).contains(&lx) || !(0..REGION_SIDE).contains(&lz) {
        return None;
    }
    Some((lx, lz))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_14_origin() {
        assert_eq!(region_rx_rz(14), (3, 2));
        assert_eq!(region_origin(14), Hex::new(23040, 15360));
        assert_eq!(region_origin(1), Hex::new(0, 0));
    }
}
