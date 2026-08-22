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
    (idx.rem_euclid(REGION_COUNT_SQRT), idx.div_euclid(REGION_COUNT_SQRT))
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

/// odd-r offset `(x, z)` → axial `(q, r)`.
///
/// Matches `bitcraft-mats` `Public/roads/hex-grid.js` (`z - (z & 1)` so
/// negative odd rows floor correctly).
pub fn offset_to_axial(x: i32, z: i32) -> (i32, i32) {
    (x - (z - (z & 1)) / 2, z)
}

/// axial `(q, r)` → odd-r offset `(x, z)`.
pub fn axial_to_offset(q: i32, r: i32) -> Hex {
    Hex::new(q + (r - (r & 1)) / 2, r)
}

/// 60° counter-clockwise cube rotation, `steps` times: `(q, r) → (−r, q + r)`.
///
/// `resource_state.direction_index` / `building_state.direction_index` are
/// `0..=5`. See `bitcraft-mats/docs/footprints-rotation.md`.
pub fn rotate_ccw_axial(q: i32, r: i32, steps: i32) -> (i32, i32) {
    let mut x = q;
    let mut z = r;
    let mut y = -x - z;
    for _ in 0..steps.rem_euclid(6) {
        let nx = -z;
        let ny = -x;
        let nz = -y;
        x = nx;
        y = ny;
        z = nz;
    }
    (x, z)
}

/// World odd-r tiles occupied by an axial footprint around origin `(wx, wz)`.
///
/// Footprint cells are axial offsets (schema names them `x`/`z`). They must
/// be rotated in cube space then added to the origin in axial, not added
/// directly to odd-r world coords (that shears on odd `z` rows).
pub fn footprint_world_hexes(
    wx: i32,
    wz: i32,
    direction: i32,
    offsets: &[(i32, i32)],
) -> impl Iterator<Item = (i32, i32)> + '_ {
    let (wq, wr) = offset_to_axial(wx, wz);
    offsets.iter().copied().map(move |(fx, fz)| {
        let (rx, rz) = rotate_ccw_axial(fx, fz, direction);
        let h = axial_to_offset(wq + rx, wr + rz);
        (h.x, h.z)
    })
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

    #[test]
    fn odd_r_axial_roundtrip_and_negative_floor() {
        for z in -5..6 {
            for x in -5..6 {
                let (q, r) = offset_to_axial(x, z);
                let back = axial_to_offset(q, r);
                assert_eq!(back, Hex::new(x, z), "x={x} z={z}");
            }
        }
        // z = -1 is odd: floor(z/2) = -1, not Rust toward-zero 0.
        assert_eq!(offset_to_axial(0, -1), (1, -1));
    }

    #[test]
    fn rotate_ccw_is_axial_neg_r_q_plus_r() {
        assert_eq!(rotate_ccw_axial(1, 0, 1), (0, 1));
        assert_eq!(rotate_ccw_axial(0, -1, 1), (1, -1));
        assert_eq!(rotate_ccw_axial(1, 0, 6), (1, 0));
        assert_eq!(rotate_ccw_axial(1, 0, -1), (1, -1));
    }

    #[test]
    fn footprint_add_in_axial_avoids_odd_row_shear() {
        // docs/footprints-rotation.md: axial SE (0,+1) from world (10,1)
        // is odd-r (11,2), not naive (10,2).
        let tiles: Vec<_> = footprint_world_hexes(10, 1, 0, &[(0, 0), (0, 1)]).collect();
        assert_eq!(tiles, vec![(10, 1), (11, 2)]);
    }
}
