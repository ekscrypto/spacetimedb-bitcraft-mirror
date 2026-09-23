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
//!   bit  14   : paving flag — the tile is player-paved; bits 0-9 then hold
//!               a paving index from the separate paving namespace
//!               ([`Self::stamp_paving`], exposed via the dictionary's
//!               paving entries). Resources and paving share one tile word:
//!               a tile can't genuinely hold both (verified: 24 in 416k
//!               sampled tiles, always a multi-hex formation over pavement),
//!               and the resource wins those — paving stamps only into
//!               empty tiles.
//!   bit  15   : water flag — the tile's terrain elevation is below its
//!               water level. Filled once from the terrain seed
//!               ([`Self::fill_water_from_terrain`]; terrain resolution is
//!               the super-hex — 7-tile flower per super, corner tiles wet
//!               when any of their three supers is) and preserved by every
//!               later write; terrain never flips water↔land.
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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use hashbrown::HashMap;

use super::coords::{
    axial_to_offset, footprint_world_hexes, offset_to_axial, overlay_index, world_to_local, REGION_SIDE, SUPER_SIDE,
};
use super::grid::{unpack_terrain_water, SuperHexTerrainGrid};

pub const RESOURCE_MAP_BYTES: usize = (REGION_SIDE as usize) * (REGION_SIDE as usize) * 2;

/// Max hexes accepted by `POST /roads/region/{id}/resources`.
pub const MAX_RESOURCE_QUERY_TILES: usize = 16384;

const INDEX_MASK: u16 = 0x03FF;
const ORIGIN_BIT: u16 = 1 << 10;
const DIRECTION_SHIFT: u16 = 11;

/// Small tiles wet by one water super-hex at super odd-r offset `(sx, sz)`:
/// axial deltas (from the super's center tile at axial `3·(q, r)`) of the 7
/// exclusively-owned flower tiles plus the 6 shared corner tiles — the
/// official `parent_large_tile` / `get_terrain_coordinates` tessellation.
fn wet_super_hex_tiles(sx: i32, sz: i32, mut visit: impl FnMut(i32, i32)) {
    const FLOWER_AND_CORNERS: [(i32, i32); 13] = [
        // Flower: the center tile and its 6 axial neighbours.
        (0, 0),
        (1, 0),
        (-1, 0),
        (0, 1),
        (0, -1),
        (1, -1),
        (-1, 1),
        // Corners: each shared with the two adjoining supers.
        (-1, -1),
        (1, 1),
        (-2, 1),
        (1, -2),
        (2, -1),
        (-1, 2),
    ];
    let (q, r) = offset_to_axial(sx, sz);
    for (dq, dr) in FLOWER_AND_CORNERS {
        let h = axial_to_offset(3 * q + dq, 3 * r + dr);
        visit(h.x, h.z);
    }
}
/// Paving namespace flag: when set, bits 0-9 hold a paving index (separate
/// [`ResourceTileMap::intern_paving`] namespace), not a resource index.
pub const PAVING_BIT: u16 = 1 << 14;
/// Water flag: the tile's terrain is below its water level. Terrain is
/// static, so this is filled once from the terrain seed
/// ([`ResourceTileMap::fill_water_from_terrain`]) and every later write
/// into the tile preserves it — it is metadata about the ground, not
/// content, so an otherwise-empty water tile reads as `WATER_BIT` alone.
pub const WATER_BIT: u16 = 1 << 15;
/// Everything a resource/paving write owns: index + flags, excluding the
/// terrain water bit.
const CONTENT_MASK: u16 = 0x7FFF;
/// Dictionary capacity: 10-bit index, 0 reserved for "empty".
pub const MAX_RESOURCE_INDEX: u16 = 1023;
/// Same capacity for the separate paving index namespace.
pub const MAX_PAVING_INDEX: u16 = 1023;

const SINGLE_HEX: [(i32, i32); 1] = [(0, 0)];

/// Resource dictionary index → tile word.
fn encode_tile(index: u16, direction: u8, is_origin: bool) -> u16 {
    debug_assert!((1..=MAX_RESOURCE_INDEX).contains(&index));
    debug_assert!(direction < 6);
    (index & INDEX_MASK) | (u16::from(is_origin) << 10) | ((direction as u16) << DIRECTION_SHIFT)
}

/// Paving index → tile word.
fn encode_paving(index: u16) -> u16 {
    debug_assert!((1..=MAX_PAVING_INDEX).contains(&index));
    PAVING_BIT | (index & INDEX_MASK)
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

fn tile_is_paving(word: u16) -> bool {
    word & PAVING_BIT != 0
}

/// True when the tile carries resource or paving content (water alone is
/// not content).
fn tile_has_content(word: u16) -> bool {
    word & CONTENT_MASK != 0
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
/// Paving types live in a second namespace ([`Self::stamp_paving`]) with
/// their own indices and the tile word's bit 14 set.
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
    /// Paving namespace: raw `paving_type_id` ↔ 10-bit paving index.
    /// `paving_ids[0] == 0` sentinel; stamped words carry [`PAVING_BIT`].
    /// Paved tiles are never entered into `by_entity` — the buffer is
    /// written/cleared directly (a paved tile is a fixed single tile).
    paving_ids: Vec<i32>,
    paving_index_by_id: HashMap<i32, u16>,
    warned_overflow: bool,
    warned_paving_overflow: bool,
    /// Live watch recording gate ([`Self::set_watch`]). Off during seeds —
    /// the seed path applies to a staging grid whose flag is never set —
    /// so the pending buffer only grows on the live grid while at least
    /// one resource-stream listener exists.
    watch: AtomicBool,
    /// Tile changes (local tile index, final word incl. the water bit)
    /// recorded since the last [`Self::take_pending`], in write order.
    pending: Vec<(u32, u16)>,
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
            paving_ids: vec![0],
            paving_index_by_id: HashMap::new(),
            warned_overflow: false,
            warned_paving_overflow: false,
            watch: AtomicBool::new(false),
            pending: Vec::new(),
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

    /// Enable/disable tile-change recording for the live watch stream
    /// ([`crate::roads::watch`]). The watch hub flips this on the live grid
    /// when the first listener registers for the region, and off when the
    /// last leaves or the grid is swapped; a stale-true flag costs nothing
    /// beyond one drained-empty buffer per batch.
    pub fn set_watch(&self, on: bool) {
        self.watch.store(on, Ordering::Relaxed);
    }

    /// Whether change recording is on (diagnostics / tests).
    pub fn watch_enabled(&self) -> bool {
        self.watch.load(Ordering::Relaxed)
    }

    /// Drain recorded tile changes — `(local tile index, final word)` pairs,
    /// final word including the terrain water bit — since the last call.
    /// Consumed by the live feed's resource-watch fan-out.
    pub fn take_pending(&mut self) -> Vec<(u32, u16)> {
        std::mem::take(&mut self.pending)
    }

    /// Record one tile write for the watch stream (no-op when disabled).
    fn note(&mut self, idx: u32, word: u16) {
        if self.watch.load(Ordering::Relaxed) {
            self.pending.push((idx, word));
        }
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

    fn intern_paving(&mut self, paving_type_id: i32) -> Option<u16> {
        if let Some(&index) = self.paving_index_by_id.get(&paving_type_id) {
            return Some(index);
        }
        if self.paving_ids.len() > MAX_PAVING_INDEX as usize {
            if !self.warned_paving_overflow {
                self.warned_paving_overflow = true;
                tracing::warn!(
                    target: "relay_cache::roads",
                    cap = MAX_PAVING_INDEX,
                    "paving dictionary overflow; further paving types are dropped"
                );
            }
            return None;
        }
        self.paving_ids.push(paving_type_id);
        let index = (self.paving_ids.len() - 1) as u16;
        self.paving_index_by_id.insert(paving_type_id, index);
        Some(index)
    }

    /// Stamp one paved tile (world coords). Paving fills only content-empty
    /// tiles — where a resource stands, the resource keeps the tile
    /// (verified rare: multi-hex formations over pavement). Single fixed
    /// tile, no footprint, never entered into `by_entity`. The terrain
    /// water bit is preserved.
    pub fn stamp_paving(&mut self, x: i32, z: i32, paving_type_id: i32) {
        let Some(index) = self.intern_paving(paving_type_id) else {
            return;
        };
        if let Some(idx) = world_to_local(self.region, x, z).and_then(|(lx, lz)| overlay_index(lx, lz)) {
            if !tile_has_content(self.tiles[idx]) {
                let word = encode_paving(index) | (self.tiles[idx] & WATER_BIT);
                self.tiles[idx] = word;
                self.note(idx as u32, word);
            }
        }
    }

    /// Clear a paved tile — only if it still holds exactly this paving
    /// type's content (a resource that later overwrote it is never erased).
    /// The terrain water bit is preserved.
    pub fn clear_paving(&mut self, x: i32, z: i32, paving_type_id: i32) {
        let Some(index) = self.paving_index_by_id.get(&paving_type_id).copied() else {
            return;
        };
        if let Some(idx) = world_to_local(self.region, x, z).and_then(|(lx, lz)| overlay_index(lx, lz)) {
            if self.tiles[idx] & CONTENT_MASK == encode_paving(index) {
                let word = self.tiles[idx] & WATER_BIT;
                self.tiles[idx] = word;
                self.note(idx as u32, word);
            }
        }
    }

    /// Fill the water bit across the whole map from the terrain grid
    /// (called once after the terrain seed flush, and again after rare live
    /// terrain writes). A super-hex is water when its elevation is below its
    /// water level. Per the game's terrain tessellation, a water super-hex
    /// wets the 7 tiles of its flower (its center tile plus the 6 axial
    /// neighbours) and the 6 corner tiles where it meets two other
    /// supers — a corner is water when *any* of its three supers is, so
    /// stamping from every water super reproduces the official
    /// `is_submerged` exactly. Terrain never flips water↔land, so this
    /// only ever sets the bit; content words already stamped are untouched
    /// (OR semantics), and later writes preserve the bit.
    pub fn fill_water_from_terrain(&mut self, terrain: &SuperHexTerrainGrid) {
        for sz in 0..SUPER_SIDE {
            for sx in 0..SUPER_SIDE {
                let (elev, water) = unpack_terrain_water(terrain.get(sx, sz));
                if elev >= water {
                    continue; // land, or no water data (level = i16::MIN / 0)
                }
                wet_super_hex_tiles(sx, sz, |lx, lz| {
                    if let Some(idx) = overlay_index(lx, lz) {
                        let dry = self.tiles[idx];
                        if dry & WATER_BIT == 0 {
                            let word = dry | WATER_BIT;
                            self.tiles[idx] = word;
                            self.note(idx as u32, word);
                        }
                    }
                });
            }
        }
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

    /// Decoded tile contents at region-local `(lx, lz)`; `None` when empty,
    /// out of bounds, or a paving tile (the roads resources endpoint is
    /// resource-only; paving surfaces via the window + dictionary).
    pub fn resource_at_local(&self, lx: i32, lz: i32) -> Option<ResourceTile> {
        let idx = overlay_index(lx, lz)?;
        let word = self.tiles[idx];
        if !tile_has_content(word) || tile_is_paving(word) {
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

    /// Raw paving type at region-local `(lx, lz)`, if the tile is paved.
    pub fn paving_at_local(&self, lx: i32, lz: i32) -> Option<i32> {
        let idx = overlay_index(lx, lz)?;
        let word = self.tiles[idx];
        if !tile_is_paving(word) {
            return None;
        }
        self.paving_ids
            .get(tile_index(word) as usize)
            .copied()
            .filter(|&id| id != 0)
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

    /// Paving namespace ids by index; `paving_ids[0] == 0` is the sentinel.
    pub fn dict_paving_ids(&self) -> &[i32] {
        &self.paving_ids
    }

    /// FNV-1a over the resource id sequence, then the paving id sequence
    /// (sentinels excluded). Stable until a new type is interned in either
    /// namespace. Computed on demand — at most ~2046 ids, so it works off a
    /// shared read guard.
    pub fn dict_version(&self) -> u32 {
        let mut h = 0x811c_9dc5u32;
        for id in self.ids.iter().chain(self.paving_ids.iter()).skip(1) {
            for b in id.to_le_bytes() {
                h ^= u32::from(b);
                h = h.wrapping_mul(0x0100_0193);
            }
        }
        h
    }

    /// Stamp one footprint and return the content words actually written
    /// (index + word, water bit excluded), for exact clearing later.
    /// Multi-hex shapes overwrite whatever occupies their tiles; a
    /// single-hex newcomer onto an occupied tile is skipped — the world does
    /// spawn forageables under multi-hex resources, and the visible (bigger)
    /// occupant should keep the tile. Water-only tiles count as empty, and
    /// the terrain water bit rides along on every written word.
    fn stamp_footprint(&mut self, origin: (i32, i32), index: u16, direction: u8) -> Vec<(u32, u16)> {
        // Cloned so the pending-recording writes below can borrow `self`
        // mutably while iterating (footprints are ≤ ~7 offsets).
        let offsets: Vec<(i32, i32)> = match self.footprints.get(&index) {
            Some(b) => b.to_vec(),
            None => SINGLE_HEX.to_vec(),
        };
        let multi = offsets.len() > 1;
        let region = self.region;
        let mut owned = Vec::with_capacity(offsets.len());
        for (x, z) in footprint_world_hexes(origin.0, origin.1, direction as i32, &offsets) {
            let Some(idx) = world_to_local(region, x, z).and_then(|(lx, lz)| overlay_index(lx, lz)) else {
                continue;
            };
            if !multi && tile_has_content(self.tiles[idx]) {
                continue;
            }
            let word = encode_tile(index, direction, (x, z) == origin);
            let final_word = word | (self.tiles[idx] & WATER_BIT);
            self.tiles[idx] = final_word;
            self.note(idx as u32, final_word);
            owned.push((idx as u32, word));
        }
        owned
    }

    /// Zero exactly the content this entity wrote. A tile a later
    /// overlapping resource re-stamped carries different content and is left
    /// alone; the terrain water bit always survives.
    fn unstamp(&mut self, owned: &[(u32, u16)]) {
        for &(idx, word) in owned {
            if self.tiles[idx as usize] & CONTENT_MASK == word {
                let final_word = self.tiles[idx as usize] & WATER_BIT;
                self.tiles[idx as usize] = final_word;
                self.note(idx, final_word);
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

    const PAVING_TYPE_A: i32 = 1;
    const PAVING_TYPE_B: i32 = 59838;

    #[test]
    fn paving_stamps_clears_and_interns_separately() {
        let mut m = map();
        m.stamp_paving(10, 20, PAVING_TYPE_A);
        // Paving words are invisible to the resources endpoint…
        assert!(m.resource_at_local(10, 20).is_none());
        // …but present as bit-14 words, one index per paving type.
        m.stamp_paving(12, 20, PAVING_TYPE_B);
        assert_eq!(m.paving_at_local(10, 20), Some(PAVING_TYPE_A));
        assert_eq!(m.paving_at_local(12, 20), Some(PAVING_TYPE_B));

        // Dict version covers the paving namespace.
        let v1 = m.dict_version();
        m.stamp_paving(14, 20, 884); // new type interned
        assert_ne!(m.dict_version(), v1);
        assert_eq!(m.dict_paving_ids()[1], PAVING_TYPE_A);
        assert_eq!(m.dict_paving_ids()[2], PAVING_TYPE_B);

        // Clearing matches type exactly.
        m.clear_paving(10, 20, PAVING_TYPE_B); // wrong type: no-op
        assert_eq!(m.paving_at_local(10, 20), Some(PAVING_TYPE_A));
        m.clear_paving(10, 20, PAVING_TYPE_A);
        assert_eq!(m.paving_at_local(10, 20), None);
        // Out-of-region clear/stamp are no-ops.
        m.stamp_paving(-5, 0, PAVING_TYPE_A);
        m.clear_paving(-5, 0, PAVING_TYPE_A);
    }

    #[test]
    fn paving_yields_to_resources_and_never_erases_them() {
        let mut m = map();
        m.note_desc(BUTTON_MUSHROOMS, &[]);
        m.note_desc(MUD_MOUND, &[(0, 0), (0, -1), (-1, 0)]);

        // Resource first: paving does not clobber it…
        m.upsert(1, BUTTON_MUSHROOMS, 0, Some((10, 20)));
        m.stamp_paving(10, 20, PAVING_TYPE_A);
        assert_eq!(m.resource_at_local(10, 20).unwrap().resource_id, BUTTON_MUSHROOMS);
        assert_eq!(m.paving_at_local(10, 20), None);

        // …and removing the paving leaves the resource untouched.
        m.clear_paving(10, 20, PAVING_TYPE_A);
        assert_eq!(m.resource_at_local(10, 20).unwrap().resource_id, BUTTON_MUSHROOMS);

        // Paving first, multi-hex resource second: the resource overwrites,
        // and paving removal does not punch a hole in the resource.
        m.stamp_paving(30, 40, PAVING_TYPE_A);
        m.upsert(2, MUD_MOUND, 0, Some((30, 40)));
        assert_eq!(m.resource_at_local(30, 40).unwrap().resource_id, MUD_MOUND);
        assert_eq!(m.paving_at_local(30, 40), None);
        m.clear_paving(30, 40, PAVING_TYPE_A);
        assert_eq!(m.resource_at_local(30, 40).unwrap().resource_id, MUD_MOUND);

        // Multi-hex resource removed: its tiles zero (paving was consumed).
        m.delete(2);
        assert!(m.resource_at_local(30, 40).is_none());
    }

    const WATER_LEVEL: i16 = 10;

    fn terrain_with_water(water_super_hexes: &[(i32, i32)]) -> SuperHexTerrainGrid {
        let mut terrain = SuperHexTerrainGrid::new();
        for &(sx, sz) in water_super_hexes {
            terrain.set(sx, sz, crate::roads::grid::pack_terrain(0, 0, WATER_LEVEL, 1));
        }
        terrain
    }

    #[test]
    fn water_fill_and_retention_across_writes() {
        let mut m = map();
        m.note_desc(BUTTON_MUSHROOMS, &[]);

        // Water super-hex (0,0) wets its flower — tiles (0,0), (1,0), (0,1)
        // — and its corners (1,1), (0,2). (10, 20) lives in super-hex
        // (3, 7): land.
        m.fill_water_from_terrain(&terrain_with_water(&[(0, 0)]));

        // Water-only tile: no content, water bit set.
        assert!(!tile_has_content(m.tiles[overlay_index(0, 0).unwrap()]));
        assert_eq!(m.tiles[overlay_index(0, 0).unwrap()] & WATER_BIT, WATER_BIT);
        assert!(m.resource_at_local(0, 0).is_none());
        assert_eq!(m.paving_at_local(0, 0), None);

        // Land tile untouched.
        assert_eq!(m.tiles[overlay_index(10, 20).unwrap()], 0);

        // Resource stamped onto a corner tile of the water super keeps the
        // water bit…
        m.upsert(1, BUTTON_MUSHROOMS, 0, Some((1, 1)));
        let word = m.tiles[overlay_index(1, 1).unwrap()];
        assert_ne!(word & WATER_BIT, 0, "water bit rides on resource words");
        assert_eq!(m.resource_at_local(1, 1).unwrap().resource_id, BUTTON_MUSHROOMS);

        // …and survives the resource's removal.
        m.delete(1);
        assert!(m.resource_at_local(1, 1).is_none());
        assert_ne!(m.tiles[overlay_index(1, 1).unwrap()] & WATER_BIT, 0);

        // Paving on water: same retention, both ways (corner tile (0,2)).
        m.stamp_paving(0, 2, PAVING_TYPE_A);
        assert_ne!(m.tiles[overlay_index(0, 2).unwrap()] & WATER_BIT, 0);
        m.clear_paving(0, 2, PAVING_TYPE_A);
        assert_ne!(m.tiles[overlay_index(0, 2).unwrap()] & WATER_BIT, 0);
        assert_eq!(m.paving_at_local(0, 2), None);

        // A water-only tile counts as empty: paving can land on it.
        m.stamp_paving(0, 0, PAVING_TYPE_A);
        assert_eq!(m.paving_at_local(0, 0), Some(PAVING_TYPE_A));
        m.clear_paving(0, 0, PAVING_TYPE_A);
        assert_eq!(m.paving_at_local(0, 0), None);
        assert_ne!(m.tiles[overlay_index(0, 0).unwrap()] & WATER_BIT, 0);
    }

    #[test]
    fn water_fill_partitions_tiles_between_supers() {
        use crate::roads::coords::{small_corner_supers, small_is_corner, small_to_super};

        // A checkerboard of water supers: every tile's water bit must equal
        // "any of its supers is water", computed independently from the
        // official per-tile mapping (primary + corner triple).
        let water_supers: Vec<(i32, i32)> = (0..12)
            .flat_map(|sz| (0..12).filter(move |sx| (sx + sz) % 2 == 0).map(move |sx| (sx, sz)))
            .collect();
        let mut m = map();
        m.fill_water_from_terrain(&terrain_with_water(&water_supers));

        for lz in 0..36 {
            for lx in 0..36 {
                let expected = if small_is_corner(lx, lz) {
                    small_corner_supers(lx, lz).iter().any(|s| water_supers.contains(s))
                } else {
                    water_supers.contains(&small_to_super(lx, lz))
                };
                let got = m.tiles[overlay_index(lx, lz).unwrap()] & WATER_BIT != 0;
                assert_eq!(got, expected, "tile ({lx},{lz})");
            }
        }

        // Corner any-of-3: tile (1,1) blends supers (0,0), (1,0), (0,1);
        // wetting only (1,0) still wets it, and drying (1,0) with (0,0)
        // dry leaves it land.
        let mut m = map();
        m.fill_water_from_terrain(&terrain_with_water(&[(1, 0)]));
        assert_ne!(m.tiles[overlay_index(1, 1).unwrap()] & WATER_BIT, 0);
        let mut m = map();
        m.fill_water_from_terrain(&terrain_with_water(&[(0, 1)]));
        assert_ne!(m.tiles[overlay_index(1, 1).unwrap()] & WATER_BIT, 0);
        let mut m = map();
        m.fill_water_from_terrain(&terrain_with_water(&[(1, 1)]));
        // (1,1) is a corner of supers (0,0), (1,0), (0,1) — not of (1,1).
        assert_eq!(m.tiles[overlay_index(1, 1).unwrap()] & WATER_BIT, 0);
        // …but the flower of (1,1) is wet, e.g. its center tile (4,3).
        assert_ne!(m.tiles[overlay_index(4, 3).unwrap()] & WATER_BIT, 0);
    }

    #[test]
    fn land_tiles_stay_dry_and_missing_terrain_defaults_to_land() {
        let mut m = map();
        // No fill call: missing terrain defaults to ground (bit unset).
        m.note_desc(BUTTON_MUSHROOMS, &[]);
        m.upsert(1, BUTTON_MUSHROOMS, 0, Some((10, 20)));
        assert_eq!(m.tiles[overlay_index(10, 20).unwrap()] & WATER_BIT, 0);

        // A fill with no water super-hexes sets nothing.
        m.fill_water_from_terrain(&terrain_with_water(&[]));
        assert_eq!(m.tiles[overlay_index(10, 20).unwrap()] & WATER_BIT, 0);

        // Water level below elevation (dry basin) is land.
        let mut dry = SuperHexTerrainGrid::new();
        dry.set(1, 1, crate::roads::grid::pack_terrain(50, 50, WATER_LEVEL, 1));
        m.fill_water_from_terrain(&dry);
        assert_eq!(m.tiles[overlay_index(3, 3).unwrap()] & WATER_BIT, 0);
    }

    fn idx_of(x: i32, z: i32) -> u32 {
        overlay_index(x, z).unwrap() as u32
    }

    #[test]
    fn watch_records_pending_only_when_enabled_and_drains() {
        let mut m = map();
        m.note_desc(MUD_MOUND, &[(0, 0), (0, -1), (-1, 0)]);
        // Watch off: mutations record nothing.
        m.upsert(1, MUD_MOUND, 0, Some((10, 20)));
        assert!(m.take_pending().is_empty());

        m.set_watch(true);
        m.upsert(2, MUD_MOUND, 0, Some((30, 40)));
        let mut pending = m.take_pending();
        pending.sort_unstable();
        let mut expected = vec![
            (idx_of(30, 40), encode_tile(1, 0, true)),
            (idx_of(29, 39), encode_tile(1, 0, false)),
            (idx_of(29, 40), encode_tile(1, 0, false)),
        ];
        expected.sort_unstable();
        assert_eq!(pending, expected);
        // Drained: a second take returns nothing until a new write.
        assert!(m.take_pending().is_empty());

        // Deletion zeroes exactly its own tiles (final word 0 on dry land).
        m.delete(2);
        let mut pending = m.take_pending();
        pending.sort_unstable();
        let mut expected: Vec<(u32, u16)> = [(30, 40), (29, 39), (29, 40)]
            .iter()
            .map(|&(x, z)| (idx_of(x, z), 0))
            .collect();
        expected.sort_unstable();
        assert_eq!(pending, expected);

        // A move records both the vacated (zeroed) and re-stamped tiles.
        m.upsert(3, MUD_MOUND, 0, Some((50, 60)));
        assert!(!m.take_pending().is_empty());
        m.set_location(3, 52, 62);
        let pending = m.take_pending();
        for &(idx, word) in &pending {
            let lx = (idx % (super::REGION_SIDE as u32)) as i32;
            let lz = (idx / (super::REGION_SIDE as u32)) as i32;
            let vacated = [(50, 60), (49, 59), (49, 60)];
            let stamped = [(52, 62), (51, 61), (51, 62)];
            if vacated.contains(&(lx, lz)) {
                assert_eq!(word, 0, "vacated tile ({lx},{lz}) must zero");
            } else {
                assert!(stamped.contains(&(lx, lz)), "unexpected tile ({lx},{lz})");
                assert_ne!(word, 0);
            }
        }
        assert_eq!(pending.len(), 6);

        m.set_watch(false);
        m.upsert(4, MUD_MOUND, 0, Some((70, 80)));
        assert!(m.take_pending().is_empty());
    }

    #[test]
    fn watch_records_paving_and_water_flips() {
        let mut m = map();
        m.set_watch(true);

        m.stamp_paving(10, 20, PAVING_TYPE_A);
        assert_eq!(m.take_pending(), vec![(idx_of(10, 20), PAVING_BIT | 1)]);
        m.clear_paving(10, 20, PAVING_TYPE_A);
        assert_eq!(m.take_pending(), vec![(idx_of(10, 20), 0)]);

        // Water fill records flips once; a second fill records nothing.
        m.fill_water_from_terrain(&terrain_with_water(&[(0, 0)]));
        let flips = m.take_pending();
        assert!(!flips.is_empty());
        assert!(flips.iter().all(|&(_, w)| w == WATER_BIT));
        m.fill_water_from_terrain(&terrain_with_water(&[(0, 0)]));
        assert!(m.take_pending().is_empty());

        // A resource stamped onto a wet tile carries the water bit in its
        // recorded word; its removal records the bare water bit.
        m.note_desc(BUTTON_MUSHROOMS, &[]);
        m.upsert(1, BUTTON_MUSHROOMS, 0, Some((1, 1)));
        assert_eq!(
            m.take_pending(),
            vec![(idx_of(1, 1), encode_tile(1, 0, true) | WATER_BIT)]
        );
        m.delete(1);
        assert_eq!(m.take_pending(), vec![(idx_of(1, 1), WATER_BIT)]);
    }
}
