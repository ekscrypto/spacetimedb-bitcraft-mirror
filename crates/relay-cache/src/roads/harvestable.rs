// SPDX-License-Identifier: MIT

//! Allowlisted harvestable nodes joined from `resource_state` + overworld
//! `location_state`, keyed for O(tiles) hex lookup.
//!
//! Kept out of hexite [`crate::store::ResourceSoA`] (`/deposits`) and out of
//! the dense overlay snapshot. The id list is vendored from BitCraft
//! `Public/resource.json` tags: Tree, Sapling, Wood Logs, Stump, Ore Vein,
//! Rock, Rock Boulder, Rock Outcrop, Clay, Sand.
//!
//! Multi-hex occupancy uses vendored `resource_desc.footprint` axial offsets
//! rotated by `resource_state.direction_index`, then added to the origin in
//! axial space (not odd-r). See `bitcraft-mats/docs/footprints-rotation.md`.

use std::sync::OnceLock;

use hashbrown::HashMap;
use serde::Deserialize;

use super::coords::footprint_world_hexes;

/// Max hexes accepted by `POST /roads/region/{id}/resources`.
pub const MAX_RESOURCE_QUERY_TILES: usize = 16384;

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

pub fn is_harvestable_resource_id(resource_id: i32) -> bool {
    allowlist().binary_search(&resource_id).is_ok()
}

fn footprint_offsets(resource_id: i32) -> &'static [(i32, i32)] {
    #[derive(Deserialize)]
    struct Entry {
        id: i32,
        tiles: Vec<(i32, i32)>,
    }
    static MAP: OnceLock<HashMap<i32, Box<[(i32, i32)]>>> = OnceLock::new();
    static ORIGIN: [(i32, i32); 1] = [(0, 0)];
    MAP.get_or_init(|| {
        let entries: Vec<Entry> = serde_json::from_str(include_str!("../../data/harvestable_footprints.json"))
            .expect("harvestable_footprints.json must parse");
        entries
            .into_iter()
            .map(|e| (e.id, e.tiles.into_boxed_slice()))
            .collect()
    })
    .get(&resource_id)
    .map(|b| &b[..])
    .unwrap_or(&ORIGIN)
}

fn occupied_hexes(resource_id: i32, direction: i32, loc: (i32, i32)) -> impl Iterator<Item = (i32, i32)> {
    footprint_world_hexes(loc.0, loc.1, direction, footprint_offsets(resource_id))
}

#[derive(Debug)]
struct Node {
    resource_id: i32,
    direction: i32,
    loc: Option<(i32, i32)>,
}

/// Sparse per-region harvestable index. Join locations from the roads
/// `location_by_entity` map (seed order: location before resource).
#[derive(Debug, Default)]
pub struct HarvestableIndex {
    by_entity: HashMap<u64, Node>,
    by_hex: HashMap<(i32, i32), Vec<u64>>,
}

impl HarvestableIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.by_entity.len()
    }

    pub fn located_len(&self) -> usize {
        self.by_entity.values().filter(|n| n.loc.is_some()).count()
    }

    /// Insert or update an allowlisted resource. `loc` is the current
    /// overworld origin `(x, z)` when known (from `location_by_entity`).
    /// `direction` is `resource_state.direction_index` (`0..=5`).
    pub fn upsert(&mut self, entity_id: u64, resource_id: i32, direction: i32, loc: Option<(i32, i32)>) {
        if !is_harvestable_resource_id(resource_id) {
            self.delete(entity_id);
            return;
        }
        let direction = direction.rem_euclid(6);
        let loc = loc.or_else(|| self.by_entity.get(&entity_id).and_then(|n| n.loc));
        let previous = self
            .by_entity
            .get(&entity_id)
            .map(|n| (n.resource_id, n.direction, n.loc));
        if let Some((old_id, old_dir, old_loc)) = previous {
            if old_id == resource_id && old_dir == direction && old_loc == loc {
                return;
            }
            if let Some(old) = old_loc {
                self.unindex(entity_id, old_id, old_dir, old);
            }
        }
        if let Some(lz) = loc {
            self.index(entity_id, resource_id, direction, lz);
        }
        self.by_entity.insert(
            entity_id,
            Node {
                resource_id,
                direction,
                loc,
            },
        );
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(node) = self.by_entity.remove(&entity_id) else {
            return;
        };
        if let Some(old) = node.loc {
            self.unindex(entity_id, node.resource_id, node.direction, old);
        }
    }

    pub fn set_location(&mut self, entity_id: u64, x: i32, z: i32) {
        let Some(node) = self.by_entity.get(&entity_id) else {
            return;
        };
        let new = (x, z);
        if node.loc == Some(new) {
            return;
        }
        let resource_id = node.resource_id;
        let direction = node.direction;
        let old = node.loc;
        if let Some(old) = old {
            self.unindex(entity_id, resource_id, direction, old);
        }
        if let Some(node) = self.by_entity.get_mut(&entity_id) {
            node.loc = Some(new);
        }
        self.index(entity_id, resource_id, direction, new);
    }

    pub fn clear_location(&mut self, entity_id: u64) {
        let Some(node) = self.by_entity.get(&entity_id) else {
            return;
        };
        let resource_id = node.resource_id;
        let direction = node.direction;
        let old = node.loc;
        if let Some(old) = old {
            self.unindex(entity_id, resource_id, direction, old);
        }
        if let Some(node) = self.by_entity.get_mut(&entity_id) {
            node.loc = None;
        }
    }

    /// Resource ids of live allowlisted nodes occupying this hex (several may
    /// share a tile; a multi-hex node is returned on every footprint cell).
    pub fn resource_ids_on(&self, x: i32, z: i32) -> impl Iterator<Item = i32> + '_ {
        self.by_hex
            .get(&(x, z))
            .into_iter()
            .flatten()
            .filter_map(|eid| self.by_entity.get(eid).map(|n| n.resource_id))
    }

    fn index(&mut self, entity_id: u64, resource_id: i32, direction: i32, loc: (i32, i32)) {
        for hex in occupied_hexes(resource_id, direction, loc) {
            let vec = self.by_hex.entry(hex).or_default();
            if !vec.contains(&entity_id) {
                vec.push(entity_id);
            }
        }
    }

    fn unindex(&mut self, entity_id: u64, resource_id: i32, direction: i32, loc: (i32, i32)) {
        for hex in occupied_hexes(resource_id, direction, loc) {
            remove_entity_from_hex(&mut self.by_hex, hex, entity_id);
        }
    }
}

fn remove_entity_from_hex(by_hex: &mut HashMap<(i32, i32), Vec<u64>>, hex: (i32, i32), entity_id: u64) {
    let Some(vec) = by_hex.get_mut(&hex) else {
        return;
    };
    if let Some(idx) = vec.iter().position(|&e| e == entity_id) {
        vec.swap_remove(idx);
    }
    if vec.is_empty() {
        by_hex.remove(&hex);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roads::coords::footprint_world_hexes;

    const MUD_MOUND: i32 = 66;
    const CLAY_TERMITE: i32 = 702104027;
    const MAPLE_SAPLING: i32 = 5;

    fn tiles_on(idx: &HarvestableIndex, hexes: &[(i32, i32)]) -> Vec<(i32, i32, i32)> {
        let mut out = Vec::new();
        for &(x, z) in hexes {
            for id in idx.resource_ids_on(x, z) {
                out.push((x, z, id));
            }
        }
        out.sort_unstable();
        out
    }

    #[test]
    fn allowlist_covers_forestry_mining_foraging_tags() {
        assert!(is_harvestable_resource_id(3)); // Rotten Log / Wood Logs
        assert!(is_harvestable_resource_id(MAPLE_SAPLING));
        assert!(is_harvestable_resource_id(MUD_MOUND));
        assert!(is_harvestable_resource_id(204021372)); // Rough Sand Pile
        assert!(!is_harvestable_resource_id(1)); // Sticks
        assert!(!is_harvestable_resource_id(348497955)); // Hexite Deposit
        assert!(!is_harvestable_resource_id(1001));
        assert_eq!(allowlist().len(), 215);
    }

    #[test]
    fn upsert_join_query_move_and_delete() {
        let mut idx = HarvestableIndex::new();
        idx.upsert(10, MAPLE_SAPLING, 0, None);
        assert_eq!(idx.len(), 1);
        assert!(idx.resource_ids_on(1, 2).next().is_none());

        idx.set_location(10, 1, 2);
        assert_eq!(idx.resource_ids_on(1, 2).collect::<Vec<_>>(), vec![MAPLE_SAPLING]);

        idx.set_location(10, 3, 4);
        assert!(idx.resource_ids_on(1, 2).next().is_none());
        assert_eq!(idx.resource_ids_on(3, 4).collect::<Vec<_>>(), vec![MAPLE_SAPLING]);

        idx.upsert(11, 3, 0, Some((3, 4)));
        let mut ids: Vec<_> = idx.resource_ids_on(3, 4).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![3, MAPLE_SAPLING]);

        idx.upsert(11, 1, 0, Some((3, 4))); // sticks — drop
        assert_eq!(idx.resource_ids_on(3, 4).collect::<Vec<_>>(), vec![MAPLE_SAPLING]);
        assert_eq!(idx.len(), 1);

        idx.delete(10);
        assert_eq!(idx.len(), 0);
        assert!(idx.resource_ids_on(3, 4).next().is_none());
    }

    #[test]
    fn unknown_location_is_noop() {
        let mut idx = HarvestableIndex::new();
        idx.set_location(99, 1, 2);
        idx.clear_location(99);
        idx.delete(99);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn mud_mound_occupies_full_footprint_on_even_and_odd_rows() {
        let offsets = footprint_offsets(MUD_MOUND);
        assert_eq!(offsets, &[(0, 0), (0, -1), (-1, 0)]);

        let even: Vec<_> = footprint_world_hexes(10, 20, 0, offsets).collect();
        assert_eq!(even.len(), 3);
        assert!(even.contains(&(10, 20)));

        let mut idx = HarvestableIndex::new();
        idx.upsert(1, MUD_MOUND, 0, Some((10, 20)));
        for &(x, z) in &even {
            assert_eq!(
                idx.resource_ids_on(x, z).collect::<Vec<_>>(),
                vec![MUD_MOUND],
                "{x},{z}"
            );
        }
        // Neighbor not in the footprint stays empty.
        let occupied: hashbrown::HashSet<_> = even.iter().copied().collect();
        for dx in -2..=2 {
            for dz in -2..=2 {
                let hex = (10 + dx, 20 + dz);
                if !occupied.contains(&hex) {
                    assert!(idx.resource_ids_on(hex.0, hex.1).next().is_none(), "{hex:?}");
                }
            }
        }

        let odd: Vec<_> = footprint_world_hexes(10, 1, 0, offsets).collect();
        idx.set_location(1, 10, 1);
        for &(x, z) in &even {
            assert!(idx.resource_ids_on(x, z).next().is_none(), "old even {x},{z}");
        }
        for &(x, z) in &odd {
            assert_eq!(idx.resource_ids_on(x, z).collect::<Vec<_>>(), vec![MUD_MOUND]);
        }
    }

    #[test]
    fn clay_termite_se_tile_does_not_shear_on_odd_origin() {
        // Axial SE (0,+1) from odd-r (10,1) is (11,2), not naive (10,2).
        // The full 7-hex rosette also occupies (10,2) via SW (−1,+1).
        let se: Vec<_> = footprint_world_hexes(10, 1, 0, &[(0, 1)]).collect();
        assert_eq!(se, vec![(11, 2)]);

        let offsets = footprint_offsets(CLAY_TERMITE);
        assert!(offsets.contains(&(0, 1)));
        let tiles: Vec<_> = footprint_world_hexes(10, 1, 0, offsets).collect();
        assert!(tiles.contains(&(11, 2)));
        assert_eq!(tiles.len(), 7);

        let mut idx = HarvestableIndex::new();
        idx.upsert(7, CLAY_TERMITE, 0, Some((10, 1)));
        assert_eq!(idx.resource_ids_on(11, 2).collect::<Vec<_>>(), vec![CLAY_TERMITE]);
        assert_eq!(idx.resource_ids_on(10, 1).collect::<Vec<_>>(), vec![CLAY_TERMITE]);
    }

    #[test]
    fn rotation_moves_footprint_tiles() {
        let mut idx = HarvestableIndex::new();
        idx.upsert(1, MUD_MOUND, 0, Some((10, 1)));
        let dir0 = tiles_on(&idx, &[(10, 1), (10, 0), (9, 1), (11, 0)]);
        idx.upsert(1, MUD_MOUND, 1, Some((10, 1)));
        let dir1 = tiles_on(&idx, &[(10, 1), (10, 0), (9, 1), (11, 0)]);
        assert_ne!(dir0, dir1);
        assert!(idx.resource_ids_on(10, 1).next().is_some()); // origin stays
        assert!(idx.resource_ids_on(9, 1).next().is_none()); // dir-0 W tile vacated
        assert_eq!(idx.resource_ids_on(11, 0).collect::<Vec<_>>(), vec![MUD_MOUND]);
    }

    #[test]
    fn growth_to_larger_footprint_reindexes() {
        let mut idx = HarvestableIndex::new();
        idx.upsert(1, MAPLE_SAPLING, 0, Some((10, 20)));
        assert_eq!(idx.resource_ids_on(10, 20).collect::<Vec<_>>(), vec![MAPLE_SAPLING]);
        idx.upsert(1, MUD_MOUND, 0, None); // keep loc; expand
        let even: Vec<_> = footprint_world_hexes(10, 20, 0, footprint_offsets(MUD_MOUND)).collect();
        assert!(even.len() > 1);
        for &(x, z) in &even {
            assert_eq!(idx.resource_ids_on(x, z).collect::<Vec<_>>(), vec![MUD_MOUND]);
        }
        assert_eq!(idx.len(), 1);
    }
}
