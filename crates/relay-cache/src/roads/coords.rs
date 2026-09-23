// SPDX-License-Identifier: MIT

//! Coordinate helpers for the roads dense grid cache.

pub const REGION_COUNT_SQRT: i32 = 5;
pub const CHUNKS_PER_SIDE: i32 = 80;
pub const DEFAULT_CHUNK_SIZE: i32 = 96;
pub const REGION_SIDE: i32 = CHUNKS_PER_SIDE * DEFAULT_CHUNK_SIZE;
pub const SUPER_SIDE: i32 = CHUNKS_PER_SIDE * 32;
/// Small hexes per super-hex side (96 per chunk side / 32 terrain cells).
pub const SMALL_PER_SUPER: i32 = REGION_SIDE / SUPER_SIDE;

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

/// World small-hex coords → 1-based region id; None outside the 5×5 grid.
/// Inverse of [`region_rx_rz`].
pub fn world_to_region(x: i32, z: i32) -> Option<u16> {
    let rx = x.div_euclid(REGION_SIDE);
    let rz = z.div_euclid(REGION_SIDE);
    if !(0..REGION_COUNT_SQRT).contains(&rx) || !(0..REGION_COUNT_SQRT).contains(&rz) {
        return None;
    }
    Some((rz * REGION_COUNT_SQRT + rx + 1) as u16)
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

/// `round(v / 3)` — the game's per-component `HexCoordinates::scale(1/3)`
/// (f32 `.round()`, half away from zero). No `.5` ties exist because the
/// residue is never ±½·3, so this integer form matches exactly.
fn round_div3(v: i32) -> i32 {
    (v + 1).div_euclid(3)
}

/// Region-local small-hex coords → the super-hex that owns the tile, in
/// super odd-r offset coords (region-local).
///
/// Matches the game server's `SmallHexTile::parent_large_tile`: convert to
/// axial, round each component to nearest thirds, convert back to odd-r at
/// the super scale. The supers are a true hex lattice at 3× the tile scale,
/// *not* rectangular 3×3 blocks of tile indices — odd super rows sit one
/// tile further right ([`super_center_tile`]).
///
/// Corner tiles — axial `x ≡ z ≡ ±1 (mod 3)`, see [`small_is_corner`] — sit
/// where three supers meet and blend all of them
/// ([`small_corner_supers`]); this returns the round-to-nearest one.
pub fn small_to_super(lx: i32, lz: i32) -> (i32, i32) {
    let (q, r) = offset_to_axial(lx, lz);
    let h = axial_to_offset(round_div3(q), round_div3(r));
    (h.x, h.z)
}

/// Is this small tile a terrain corner — the tiles where three super-hexes
/// meet (axial `x ≡ z ≡ ±1 (mod 3)`; game `SmallHexTile::is_corner`)?
pub fn small_is_corner(lx: i32, lz: i32) -> bool {
    let (q, r) = offset_to_axial(lx, lz);
    let rq = q.rem_euclid(3);
    rq != 0 && rq == r.rem_euclid(3)
}

/// The three super-hexes a corner tile blends (game
/// `SmallHexTile::get_terrain_coordinates`), in super odd-r offset coords.
/// Only defined for corner tiles — [`small_is_corner`] decides.
pub fn small_corner_supers(lx: i32, lz: i32) -> [(i32, i32); 3] {
    let (q, r) = offset_to_axial(lx, lz);
    let q0 = round_div3(q);
    let r0 = round_div3(r);
    // Down-corner (axial 3q+1, 3r+1) adds (q+1, r) and (q, r+1);
    // up-corner (axial 3q−1, 3r−1) adds (q−1, r) and (q, r−1).
    let [a, b, c] = if q.rem_euclid(3) == 1 {
        [(q0, r0), (q0 + 1, r0), (q0, r0 + 1)]
    } else {
        [(q0, r0), (q0 - 1, r0), (q0, r0 - 1)]
    };
    [axial_super(a), axial_super(b), axial_super(c)]
}

fn axial_super((q, r): (i32, i32)) -> (i32, i32) {
    let h = axial_to_offset(q, r);
    (h.x, h.z)
}

/// Super odd-r offset coords → the world/region-local small tile at the
/// super's center (game `LargeHexTile::center_small_tile`): `(3x + (z&1), 3z)`
/// — odd super rows are shifted one tile right, the odd-r shear at scale 3.
pub fn super_center_tile(sx: i32, sz: i32) -> (i32, i32) {
    (sx * SMALL_PER_SUPER + (sz & 1), sz * SMALL_PER_SUPER)
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
    fn world_to_region_roundtrips_every_region() {
        for id in 1..=(REGION_COUNT_SQRT * REGION_COUNT_SQRT) {
            let origin = region_origin(id as u16);
            // First and last tile of the region both resolve to it.
            assert_eq!(world_to_region(origin.x, origin.z), Some(id as u16));
            assert_eq!(
                world_to_region(origin.x + REGION_SIDE - 1, origin.z + REGION_SIDE - 1),
                Some(id as u16)
            );
            // The resolved region agrees with world_to_local on containment.
            assert!(world_to_local(id as u16, origin.x, origin.z).is_some());
        }
    }

    #[test]
    fn world_to_region_rejects_outside_grid() {
        assert_eq!(world_to_region(-1, 0), None);
        assert_eq!(world_to_region(0, -1), None);
        assert_eq!(world_to_region(REGION_SIDE * REGION_COUNT_SQRT, 0), None);
        assert_eq!(world_to_region(0, REGION_SIDE * REGION_COUNT_SQRT), None);
        assert_eq!(world_to_region(i32::MIN, i32::MAX), None);
        // Boundary inside the grid stays valid.
        assert_eq!(world_to_region(REGION_SIDE - 1, REGION_SIDE - 1), Some(1));
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

    #[test]
    fn super_center_tile_roundtrips_through_small_to_super() {
        for sz in -40..40 {
            for sx in -40..40 {
                let (cx, cz) = super_center_tile(sx, sz);
                assert_eq!(small_to_super(cx, cz), (sx, sz), "super ({sx},{sz})");
            }
        }
    }

    #[test]
    fn small_to_super_matches_official_axial_rounding() {
        // Hand-derived from the game's parent_large_tile (axial round(÷3)):
        // rows of 400 consecutive tiles each map to 134 consecutive supers,
        // and odd super rows start one tile further right.
        for lz in 0..12 {
            let mut cols: Vec<i32> = (0..400).map(|lx| small_to_super(lx, lz).0).collect();
            cols.dedup();
            // Consecutive columns, no gaps or repeats in the run.
            for w in cols.windows(2) {
                assert_eq!(w[1] - w[0], 1, "row {lz}");
            }
            assert_eq!(cols.len(), 134, "row {lz}");
            // Super z is round(z/3), independent of x.
            assert_eq!(small_to_super(0, lz).1, round_div3_pub(lz));
        }
        // Block semantics would give (0,0) for tile (2,0); the official
        // lattice assigns it to the next super over.
        assert_eq!(small_to_super(0, 0), (0, 0));
        assert_eq!(small_to_super(1, 0), (0, 0));
        assert_eq!(small_to_super(2, 0), (1, 0));
        assert_eq!(small_to_super(0, 1), (0, 0));
        assert_eq!(small_to_super(0, 2), (0, 1));
        assert_eq!(small_to_super(0, 3), (0, 1));
        assert_eq!(small_to_super(0, 5), (0, 2));
    }

    #[test]
    fn super_x_is_periodic_in_tile_z_with_period_6() {
        // The covering-window math relies on X(x, z+6) == X(x, z).
        for lz in -20..20 {
            for lx in [-7, 0, 5, 123] {
                assert_eq!(
                    small_to_super(lx, lz + 6).0,
                    small_to_super(lx, lz).0,
                    "tile ({lx},{lz})"
                );
            }
        }
    }

    #[test]
    fn corner_tiles_know_their_three_supers() {
        // Tile (1,1) is the down-corner of super axial (0,0) → offset (0,0);
        // official get_terrain_coordinates returns (0,0), (1,0), (0,1).
        assert!(small_is_corner(1, 1));
        assert_eq!(small_corner_supers(1, 1), [(0, 0), (1, 0), (0, 1)]);
        // Tile (6,4): axial (4,4) → down-corner of super axial (1,1) whose
        // offset is (1,1); extra supers axial (2,1)→(2,1) and (1,2)→(2,2).
        assert!(small_is_corner(6, 4));
        assert_eq!(small_corner_supers(6, 4), [(1, 1), (2, 1), (2, 2)]);
        // Up-corner: tile axial (−1,−1) is offset (−2,−1) — supers
        // axial (0,0),(−1,0),(0,−1) → offsets (0,0),(−1,0),(−1,−1).
        assert!(small_is_corner(-2, -1));
        assert_eq!(small_corner_supers(-2, -1), [(0, 0), (-1, 0), (-1, -1)]);
        // Up-corner at positive coords: tile (0,2) is axial (−1,2), the
        // up-corner of super (0,1) → supers (0,1), (−1,1), (0,0).
        assert!(small_is_corner(0, 2));
        assert_eq!(small_corner_supers(0, 2), [(0, 1), (-1, 1), (0, 0)]);
        // Regular tiles are not corners.
        assert!(!small_is_corner(0, 0));
        assert!(!small_is_corner(2, 0));
        assert!(!small_is_corner(0, 1));
        assert!(!small_is_corner(2, 1));
    }

    #[test]
    fn corner_supers_include_the_primary_and_are_distinct() {
        for lz in -15..15 {
            for lx in -15..15 {
                if !small_is_corner(lx, lz) {
                    continue;
                }
                let primary = small_to_super(lx, lz);
                let triple = small_corner_supers(lx, lz);
                // The round-to-nearest super is always one of the three the
                // game blends.
                assert!(
                    triple.contains(&primary),
                    "tile ({lx},{lz}) primary {primary:?} not in {triple:?}"
                );
                let [a, b, c] = triple;
                assert_ne!(a, b);
                assert_ne!(b, c);
                assert_ne!(a, c);
            }
        }
    }

    fn round_div3_pub(v: i32) -> i32 {
        (v + 1).div_euclid(3)
    }
}
