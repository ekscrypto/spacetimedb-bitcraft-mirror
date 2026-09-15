// SPDX-License-Identifier: MIT

//! Dense per-region resource tile map: one `u16` per odd-r tile covering the
//! whole `REGION_SIDE`×`REGION_SIDE` region. Shared by the roads
//! `POST /roads/region/{id}/resources` point-lookup (arbitrary tile arrays)
//! and the Bit-Me `GET /bitme/session/{id}/resources` window snapshot
//! (contiguous rows, copied one 800-byte slice per row).
//!
//! Tile word layout (u16 LE):
//!   bits  0-9 : dictionary index of the resource (0 = empty tile)
//!   bit  10   : origin flag — the resource's anchor tile
//!   bits 11-13: `resource_state.direction_index` (0..=5), repeated on
//!               every footprint tile so any tile can seed reconstruction
//!   bits 14-15: reserved, zero
//!
//! Footprints come from `resource_desc.footprint` (axial offsets, rotated by
//! direction via [`footprint_world_hexes`]) and are stamped server-side onto
//! every occupied tile — clients render per-tile or reconstruct instances
//! from origin tiles + direction, never doing footprint math themselves.
//!
//! Overlap policy: the world does spawn single-hex forageables on the
//! footprint tiles of multi-hex resources (verified against live data).
//! The 2-byte format holds one resource per tile, so the map prefers the
//! visible occupant: multi-hex shapes overwrite whatever is beneath them,
//! while a single-hex newcomer onto an occupied tile is not stamped. Each
//! entity records exactly which tile words it wrote ([`Node::owned`]), so
//! clearing only zeroes its own remaining words and never punches holes in
//! a resource that overwrote it later. We relay and summarize, nothing
//! more — no occupancy map is kept.
//!
//! Unlike the retired [`crate::roads::harvestable`] index this map carries
//! **all** `resource_state` rows (every resource type, including forageables
//! and event resources); consumers filter via the dictionary.

use std::sync::OnceLock;

use hashbrown::HashMap;

use super::coords::{footprint_world_hexes, overlay_index, world_to_local, REGION_SIDE};

pub const RESOURCE_MAP_BYTES: usize = (REGION_SIDE as usize) * (REGION_SIDE as usize) * 2;

/// Max hexes accepted by `POST /roads/region/{id}/resources`.
pub const MAX_RESOURCE_QUERY_TILES: usize = 16384;

const INDEX_MASK: u16 = 0x03FF;
const ORIGIN_BIT: u16 = 1 << 10;
const DIRECTION_SHIFT: u16 = 11;
/// Dictionary capacity: 10-bit index, 0 reserved for "empty".
pub const MAX_RESOURCE_INDEX: u16 = 1023;

const SINGLE_HEX: [(i32, i32); 1] = [(0, 0)];

/// Dictionary index → tile word.
fn encode_tile(index: u16, direction: u8, is_origin: bool) -> u16 {
    debug_assert!((1..=MAX_RESOURCE_INDEX).contains(&index));
    debug_assert!(direction < 6);
    (index & INDEX_MASK) | (u16::from(is_origin) << 10) | ((direction as u16) << DIRECTION_SHIFT)
}

fn tile_index(word: u16) -> u16 {
    word & INDEX_MASK
}

fn tile_direction(word: u16) -> u8 {
    ((word >> DIRECTION_SHIFT) & 0b111) as u8
}

fn tile_is_origin(word: u16) -> bool {
    word & ORIGIN_BIT != 0
}

fn allowlist() -> &'static [i32] {
    static IDS: OnceLock<Vec<i32>> = OnceLock::new();
    IDS.get_or_init(|| {
        let mut v: Vec<i32> = serde_json::from_str(include_str!("../../data/harvestable_resource_ids.json"))
            .expect("harvestable_resource_ids.json must parse");
        v.sort_unstable();
        v.dedup();
        v
    })
    .as_slice()
}

/// True when `resource_id` is in the vendored forestry/mining/clay/sand
/// harvestable allowlist. Surfaced per dictionary entry so clients can
/// reproduce the old roads-side filter without vendoring the id list.
pub fn is_harvestable_resource_id(resource_id: i32) -> bool {
    allowlist().binary_search(&resource_id).is_ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceTile {
    pub resource_id: i32,
    pub direction: u8,
    pub is_origin: bool,
}

#[derive(Debug, Clone, Default)]
struct Node {
    index: u16,
    direction: u8,
    /// World odd-r origin tile when located (overworld) — `None` until the
    /// entity's `location_state` row arrives (live inserts can precede it).
    loc: Option<(i32, i32)>,
    /// Tile indices this entity actually has words written on, with the
    /// word written. A footprint tile occupied by an existing resource when
    /// a single-hex entity spawns is skipped (big shapes win — see
    /// [`ResourceTileMap::stamp_footprint`]), so clearing must only zero
    /// what we wrote.
    owned: Vec<(u32, u16)>,
}

/// Dense resource map + per-region `resource_id` ↔ 10-bit dictionary.
///
/// Dictionary indices are assigned in first-sighting order, so they are
/// stable per deploy but may differ across redeploys; [`Self::dict_version`]
/// (FNV-1a over the id sequence) lets clients detect dictionary changes.
#[derive(Debug)]
pub struct ResourceTileMap {
    region: u16,
    tiles: Vec<u16>,
    by_entity: HashMap<u64, Node>,
    /// `ids[0] == 0` is the empty-tile sentinel; `ids[index]` for index ≥ 1.
    ids: Vec<i32>,
    index_by_id: HashMap<i32, u16>,
    /// Axial footprint offsets per dictionary index, from
    /// `resource_desc.footprint` ([`Self::note_desc`]). Single hex when the
    /// desc row has not been seen (or has an empty footprint).
    footprints: HashMap<u16, Box<[(i32, i32)]>>,
    warned_overflow: bool,
}

impl ResourceTileMap {
    pub fn new(region: u16) -> Self {
        Self {
            region,
            tiles: vec![0u16; (REGION_SIDE as usize) * (REGION_SIDE as usize)],
            by_entity: HashMap::new(),
            ids: vec![0],
            index_by_id: HashMap::new(),
            footprints: HashMap::new(),
            warned_overflow: false,
        }
    }

    pub fn len(&self) -> usize {
        self.by_entity.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_entity.is_empty()
    }

    /// Located entity count (for diagnostics parity with the old index).
    pub fn located_len(&self) -> usize {
        self.by_entity.values().filter(|n| n.loc.is_some()).count()
    }

    /// Feed a `resource_desc` row: intern the id and remember its footprint.
    /// Seeded before `resource_state` (table-alphabetical order), so
    /// footprints are known before tiles are stamped.
    pub fn note_desc(&mut self, resource_id: i32, offsets: &[(i32, i32)]) {
        let Some(index) = self.intern(resource_id) else {
            return;
        };
        let offsets: Box<[(i32, i32)]> = if offsets.is_empty() {
            Box::from(SINGLE_HEX)
        } else {
            offsets.to_vec().into_boxed_slice()
        };
        self.footprints.insert(index, offsets);
    }

    fn intern(&mut self, resource_id: i32) -> Option<u16> {
        if let Some(&index) = self.index_by_id.get(&resource_id) {
            return Some(index);
        }
        if self.ids.len() > MAX_RESOURCE_INDEX as usize {
            if !self.warned_overflow {
                self.warned_overflow = true;
                tracing::warn!(
                    target: "relay_cache::roads",
                    cap = MAX_RESOURCE_INDEX,
                    "resource dictionary overflow; further resource types are dropped"
                );
            }
            return None;
        }
        self.ids.push(resource_id);
        let index = (self.ids.len() - 1) as u16;
        self.index_by_id.insert(resource_id, index);
        Some(index)
    }

    /// Insert or update a resource entity. `loc` is the current overworld
    /// world-tile origin when known (from the roads `location_by_entity`
    /// join); `direction` is `resource_state.direction_index` (`0..=5`).
    pub fn upsert(&mut self, entity_id: u64, resource_id: i32, direction: i32, loc: Option<(i32, i32)>) {
        let Some(index) = self.intern(resource_id) else {
            // Dictionary full: keep the map consistent with the old index's
            // behavior of dropping unknown ids.
            self.delete(entity_id);
            return;
        };
        let direction = direction.rem_euclid(6) as u8;
        let previous = self.by_entity.remove(&entity_id);
        if let Some(node) = &previous {
            if node.index == index && node.direction == direction && node.loc == loc {
                self.by_entity.insert(entity_id, node.clone());
                return;
            }
        }
        self.unstamp(previous.as_ref().map(|n| n.owned.as_slice()).unwrap_or(&[]));
        let owned = loc
            .map(|new| self.stamp_footprint(new, index, direction))
            .unwrap_or_default();
        self.by_entity.insert(
            entity_id,
            Node {
                index,
                direction,
                loc,
                owned,
            },
        );
    }

    pub fn delete(&mut self, entity_id: u64) {
        if let Some(node) = self.by_entity.remove(&entity_id) {
            self.unstamp(&node.owned);
        }
    }

    /// Apply a moved overworld `location_state` row (world tiles).
    pub fn set_location(&mut self, entity_id: u64, x: i32, z: i32) {
        let Some(mut node) = self.by_entity.remove(&entity_id) else {
            return;
        };
        if node.loc != Some((x, z)) {
            let (index, direction) = (node.index, node.direction);
            self.unstamp(&node.owned);
            node.owned = self.stamp_footprint((x, z), index, direction);
            node.loc = Some((x, z));
        }
        self.by_entity.insert(entity_id, node);
    }

    pub fn clear_location(&mut self, entity_id: u64) {
        let Some(mut node) = self.by_entity.remove(&entity_id) else {
            return;
        };
        self.unstamp(&node.owned);
        node.owned.clear();
        node.loc = None;
        self.by_entity.insert(entity_id, node);
    }

    /// Resource id + origin of one entity by entity id (Bit-Me target
    /// enrichment: the acted-on resource's identity and world position).
    pub fn resource_of(&self, entity_id: u64) -> Option<(i32, Option<(i32, i32)>)> {
        let node = self.by_entity.get(&entity_id)?;
        Some((self.ids[node.index as usize], node.loc))
    }

    /// Decoded tile contents at region-local `(lx, lz)`; `None` when empty
    /// or out of bounds.
    pub fn resource_at_local(&self, lx: i32, lz: i32) -> Option<ResourceTile> {
        let idx = overlay_index(lx, lz)?;
        let word = self.tiles[idx];
        if word == 0 {
            return None;
        }
        let resource_id = self.ids.get(tile_index(word) as usize).copied().unwrap_or(0);
        if resource_id == 0 {
            return None;
        }
        Some(ResourceTile {
            resource_id,
            direction: tile_direction(word),
            is_origin: tile_is_origin(word),
        })
    }

    /// `width`×`width` window of tile words (u16 LE, row-major) anchored at
    /// region-local `origin` (which may be negative or overhang the region
    /// edge — out-of-region cells are zero).
    pub fn window(&self, origin: (i32, i32), width: usize) -> Vec<u8> {
        let mut out = vec![0u16; width * width];
        let side = REGION_SIDE;
        let ow = width as i32;
        for r in 0..width {
            let lz = origin.1 + r as i32;
            if !(0..side).contains(&lz) {
                continue;
            }
            let start = origin.0.max(0);
            let end = (origin.0 + ow).min(side);
            if start >= end {
                continue;
            }
            let row_base = (lz as usize) * (REGION_SIDE as usize);
            let dst = r * width + (start - origin.0) as usize;
            let count = (end - start) as usize;
            out[dst..dst + count].copy_from_slice(&self.tiles[row_base + start as usize..row_base + end as usize]);
        }
        let mut bytes = Vec::with_capacity(out.len() * 2);
        for word in out {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes
    }

    /// Dictionary ids by index; `ids[0] == 0` is the empty sentinel.
    pub fn dict_ids(&self) -> &[i32] {
        &self.ids
    }

    /// FNV-1a over the dictionary id sequence in index order (sentinel
    /// excluded). Stable until a new resource type is interned. Computed on
    /// demand — at most 1023 ids, so it works off a shared read guard.
    pub fn dict_version(&self) -> u32 {
        let mut h = 0x811c_9dc5u32;
        for id in &self.ids[1..] {
            for b in id.to_le_bytes() {
                h ^= u32::from(b);
                h = h.wrapping_mul(0x0100_0193);
            }
        }
        h
    }

    /// Stamp one footprint and return the tiles actually written (index +
    /// word), for exact clearing later. Multi-hex shapes overwrite whatever
    /// occupies their tiles; a single-hex newcomer onto an occupied tile is
    /// skipped — the world does spawn forageables under multi-hex resources,
    /// and the visible (bigger) occupant should keep the tile.
    fn stamp_footprint(&mut self, origin: (i32, i32), index: u16, direction: u8) -> Vec<(u32, u16)> {
        let offsets: &[(i32, i32)] = self.footprints.get(&index).map(|b| &b[..]).unwrap_or(&SINGLE_HEX);
        let multi = offsets.len() > 1;
        let region = self.region;
        let mut owned = Vec::with_capacity(offsets.len());
        for (x, z) in footprint_world_hexes(origin.0, origin.1, direction as i32, offsets) {
            let Some(idx) = world_to_local(region, x, z).and_then(|(lx, lz)| overlay_index(lx, lz)) else {
                continue;
            };
            if !multi && self.tiles[idx] != 0 {
                continue;
            }
            let word = encode_tile(index, direction, (x, z) == origin);
            self.tiles[idx] = word;
            owned.push((idx as u32, word));
        }
        owned
    }

    /// Zero exactly the tiles this entity wrote. A tile a later overlapping
    /// resource re-stamped carries a different word and is left alone.
    fn unstamp(&mut self, owned: &[(u32, u16)]) {
        for &(idx, word) in owned {
            if self.tiles[idx as usize] == word {
                self.tiles[idx as usize] = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MUD_MOUND: i32 = 66;
    const MAPLE_SAPLING: i32 = 5;
    const BUTTON_MUSHROOMS: i32 = 74;

    /// Region 1 → origin (0, 0): world coords == local coords.
    fn map() -> ResourceTileMap {
        ResourceTileMap::new(1)
    }

    fn tiles_at(m: &ResourceTileMap, hexes: &[(i32, i32)]) -> Vec<(i32, i32, ResourceTile)> {
        let mut out = Vec::new();
        for &(x, z) in hexes {
            if let Some(t) = m.resource_at_local(x, z) {
                out.push((x, z, t));
            }
        }
        out.sort_unstable_by_key(|(_, _, t)| (t.resource_id, t.is_origin));
        out
    }

    #[test]
    fn tile_word_roundtrip() {
        let word = encode_tile(742, 5, true);
        assert_eq!(tile_index(word), 742);
        assert_eq!(tile_direction(word), 5);
        assert!(tile_is_origin(word));
        // Reserved bits stay zero.
        assert_eq!(word >> 14, 0);
        assert_eq!(encode_tile(1, 0, false), 1);
    }

    #[test]
    fn dictionary_version_tracks_interning() {
        let mut m = map();
        m.note_desc(MAPLE_SAPLING, &[]);
        let v1 = m.dict_version();
        assert_eq!(m.dict_ids()[1], MAPLE_SAPLING);
        // Re-noting the same id is stable.
        m.note_desc(MAPLE_SAPLING, &[]);
        assert_eq!(m.dict_version(), v1);
        m.note_desc(BUTTON_MUSHROOMS, &[]);
        assert_ne!(m.dict_version(), v1);
        assert_eq!(m.dict_ids()[2], BUTTON_MUSHROOMS);
    }

    #[test]
    fn harvestable_allowlist_intact() {
        assert!(is_harvestable_resource_id(MAPLE_SAPLING));
        assert!(is_harvestable_resource_id(MUD_MOUND));
        assert!(!is_harvestable_resource_id(BUTTON_MUSHROOMS)); // forageable
        assert!(!is_harvestable_resource_id(348_497_955)); // Hexite Deposit
    }

    #[test]
    fn single_hex_upsert_move_and_delete() {
        let mut m = map();
        m.note_desc(BUTTON_MUSHROOMS, &[]);
        // Resource row before its location row: nothing stamped yet.
        m.upsert(10, BUTTON_MUSHROOMS, 0, None);
        assert_eq!(m.len(), 1);
        assert_eq!(m.located_len(), 0);
        assert!(m.resource_at_local(1, 2).is_none());

        m.set_location(10, 1, 2);
        assert_eq!(m.located_len(), 1);
        assert_eq!(tiles_at(&m, &[(1, 2)]).len(), 1);

        m.set_location(10, 3, 4);
        assert!(m.resource_at_local(1, 2).is_none());
        assert_eq!(tiles_at(&m, &[(3, 4)]).len(), 1);

        m.delete(10);
        assert_eq!(m.len(), 0);
        assert!(m.resource_at_local(3, 4).is_none());
    }

    #[test]
    fn multi_hex_footprint_stamped_with_origin_and_direction() {
        let mut m = map();
        m.note_desc(MUD_MOUND, &[(0, 0), (0, -1), (-1, 0)]);
        m.upsert(1, MUD_MOUND, 0, Some((10, 20)));

        // Axial (0,-1)/(-1,0) around odd-r (10,20) (even row) → (9,19)/(9,20).
        let occupied = vec![(10, 20), (9, 19), (9, 20)];
        let stamped = tiles_at(&m, &occupied);
        assert_eq!(stamped.len(), 3);
        for (x, z, t) in stamped {
            assert_eq!(t.resource_id, MUD_MOUND);
            assert_eq!(t.direction, 0);
            assert_eq!(t.is_origin, (x, z) == (10, 20));
        }
        // Neighbors stay empty.
        assert!(m.resource_at_local(10, 19).is_none());

        // Direction rotates the footprint; origin flag follows the anchor.
        // Dir 1 = ccw rotation: axial (0,-1)→(1,-1) and (-1,0)→(0,-1),
        // which land on odd-r (10,19) and (9,19) around the same origin.
        m.upsert(1, MUD_MOUND, 1, Some((10, 20)));
        assert!(m.resource_at_local(9, 20).is_none()); // dir-0-only tile vacated
        let rotated = tiles_at(&m, &[(10, 20), (10, 19)]);
        assert_eq!(rotated.len(), 2);
    }

    #[test]
    fn growth_rewrite_expands_footprint_cleanly() {
        let mut m = map();
        m.note_desc(MAPLE_SAPLING, &[]);
        m.note_desc(MUD_MOUND, &[(0, 0), (0, -1), (-1, 0)]);
        m.upsert(1, MAPLE_SAPLING, 0, Some((10, 20)));
        m.upsert(1, MUD_MOUND, 0, Some((10, 20))); // same entity grows
        let occupied = vec![(10, 20), (9, 19), (9, 20)];
        assert_eq!(tiles_at(&m, &occupied).len(), 3);
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn delete_clears_only_its_own_tiles() {
        let mut m = map();
        m.note_desc(MUD_MOUND, &[(0, 0), (0, -1), (-1, 0)]);
        m.upsert(1, MUD_MOUND, 0, Some((10, 20)));
        m.delete(1);
        for hex in [(10, 20), (10, 19), (9, 20)] {
            assert!(m.resource_at_local(hex.0, hex.1).is_none(), "{hex:?}");
        }
    }

    /// The world spawns single-hex forageables on multi-hex resources'
    /// footprint tiles. The bigger shape keeps the shared tile, and removing
    /// the forageable later must not erase the occupant's tile.
    #[test]
    fn single_hex_yields_to_multi_hex_occupant() {
        let mut m = map();
        m.note_desc(MUD_MOUND, &[(0, 0), (0, -1), (-1, 0)]);
        m.note_desc(BUTTON_MUSHROOMS, &[]);
        // Mushroom first, mound second: the mound's footprint covers the
        // mushroom's tile and wins it.
        m.upsert(1, BUTTON_MUSHROOMS, 0, Some((10, 20)));
        m.upsert(2, MUD_MOUND, 0, Some((10, 20)));
        let t = m.resource_at_local(10, 20).unwrap();
        assert_eq!(t.resource_id, MUD_MOUND);
        assert_eq!(m.resource_of(1).unwrap().0, BUTTON_MUSHROOMS); // entity kept

        // Removing the mushroom must not zero the mound's tile.
        m.delete(1);
        let t = m.resource_at_local(10, 20).unwrap();
        assert_eq!(t.resource_id, MUD_MOUND);

        // Reverse order: mound seeded first, mushroom spawns under it —
        // the mushroom is not stamped and the mound keeps everything.
        m.upsert(3, MUD_MOUND, 0, Some((20, 40)));
        m.upsert(4, BUTTON_MUSHROOMS, 0, Some((20, 40)));
        let t = m.resource_at_local(20, 40).unwrap();
        assert_eq!(t.resource_id, MUD_MOUND);
        m.delete(4);
        let t = m.resource_at_local(20, 40).unwrap();
        assert_eq!(t.resource_id, MUD_MOUND);
        m.delete(3);
        assert!(m.resource_at_local(20, 40).is_none());
    }

    #[test]
    fn window_clamps_negative_origin_and_region_edge() {
        let mut m = map();
        m.note_desc(BUTTON_MUSHROOMS, &[]);
        m.note_desc(MAPLE_SAPLING, &[]);
        // Region-1 corner tile and a tile past the 7680 side are both
        // addressable world coords; the past-edge one is out of region.
        m.upsert(1, BUTTON_MUSHROOMS, 0, Some((0, 0)));
        m.upsert(2, MAPLE_SAPLING, 0, Some((7679, 7679)));

        // Window hanging off the top-left corner: exactly one resource,
        // at relative (200, 200) for a 400-wide window anchored at
        // (-200, -200).
        let bytes = m.window((-200, -200), 400);
        assert_eq!(bytes.len(), 400 * 400 * 2);
        let off = (200 * 400 + 200) * 2;
        let word = u16::from_le_bytes(bytes[off..off + 2].try_into().unwrap());
        assert_eq!(tile_index(word), 1);

        // Window hanging off the bottom-right corner.
        let bytes = m.window((7679 - 200, 7679 - 200), 400);
        let word = u16::from_le_bytes(bytes[off..off + 2].try_into().unwrap());
        assert_eq!(tile_index(word), 2);
    }

    #[test]
    fn out_of_region_upsert_keeps_node_without_tiles() {
        let mut m = map(); // region 1: world 0..7680
        m.note_desc(BUTTON_MUSHROOMS, &[]);
        m.upsert(1, BUTTON_MUSHROOMS, 0, Some((-5, 100)));
        assert_eq!(m.len(), 1);
        assert_eq!(m.located_len(), 1); // node keeps its loc; no tile stamped
        assert!(m.resource_at_local(0, 0).is_none());
        // Later arriving in-region location still lands.
        m.set_location(1, 12, 34);
        assert_eq!(tiles_at(&m, &[(12, 34)]).len(), 1);
    }
}
