// SPDX-License-Identifier: MIT

//! Hash-map store for `resource_desc` (static resource gamedata replicated
//! into every region DB). Feeds Bit-Me session enrichment (names, max
//! health, despawn/respawn timers) and the live spawn watch-set.

use hashbrown::HashMap;
use std::sync::OnceLock;

use crate::decode::ResourceDescRow;

/// Vendored ids of the three Citric Giant berry bushes (`bitme`): special
/// bushes the game spawns into harvested Bountiful bush spots. They are not
/// referenced by any `on_destroy_yield_resource_id`, so the dynamic
/// yield-chain rule cannot discover them.
fn vendored_watched_ids() -> &'static [i32] {
    static IDS: OnceLock<Vec<i32>> = OnceLock::new();
    IDS.get_or_init(|| {
        serde_json::from_str(include_str!("../../data/bitme_watched_resource_ids.json"))
            .expect("bitme_watched_resource_ids.json must parse")
    })
    .as_slice()
}

#[derive(Debug, Clone)]
pub struct ResourceDescStore {
    rows: HashMap<i32, ResourceDescRow>,
    /// Resource ids the game can spawn as the destroy-yield of another
    /// resource (growth/respawn chains), plus the vendored Bit-Me watched
    /// set (citric bushes). Drives the live `resource_state` spawn log.
    watched: HashMap<i32, ()>,
}

impl Default for ResourceDescStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceDescStore {
    pub fn new() -> Self {
        let watched = vendored_watched_ids().iter().map(|&id| (id, ())).collect();
        Self {
            rows: HashMap::new(),
            watched,
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn get(&self, id: i32) -> Option<&ResourceDescRow> {
        self.rows.get(&id)
    }

    pub fn name(&self, id: i32) -> Option<&str> {
        self.rows.get(&id).map(|r| r.name.as_str())
    }

    pub fn upsert(&mut self, row: ResourceDescRow) {
        if row.on_destroy_yield_resource_id != 0 {
            self.watched.insert(row.on_destroy_yield_resource_id, ());
        }
        self.rows.insert(row.id, row);
    }

    /// Deleting a desc row keeps its id in the watched set for the life of
    /// the store (the set is a superset by design; gamedata rows are
    /// effectively static anyway).
    pub fn delete(&mut self, id: i32) {
        self.rows.remove(&id);
    }

    /// True when `resource_id` is a spawn event the Bit-Me session feed logs:
    /// any destroy-yield chain target (respawns, citric fruits) or vendored
    /// watched id (citric giant bushes).
    pub fn is_watched_spawn(&self, resource_id: i32) -> bool {
        self.watched.contains_key(&resource_id)
    }

    pub fn watched_len(&self) -> usize {
        self.watched.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: i32, yield_id: i32) -> ResourceDescRow {
        ResourceDescRow {
            id,
            name: format!("r{id}"),
            max_health: 100,
            despawn_time: 0.0,
            on_destroy_yield_resource_id: yield_id,
            scheduled_respawn_time: 0.0,
        }
    }

    #[test]
    fn yield_chain_registers_watched() {
        let mut s = ResourceDescStore::new();
        s.upsert(row(1, 2)); // destroying 1 spawns 2
        assert!(s.is_watched_spawn(2));
        assert!(!s.is_watched_spawn(1));
    }

    #[test]
    fn citric_bushes_are_vendored_watched() {
        let s = ResourceDescStore::new();
        for id in [1_688_062_540i32, 65_901_922, 1_875_092_977] {
            assert!(s.is_watched_spawn(id), "citric id {id} must be watched");
        }
        assert!(!s.is_watched_spawn(1_822_942_131)); // plain Bountiful bush
    }

    #[test]
    fn name_lookup() {
        let mut s = ResourceDescStore::new();
        s.upsert(ResourceDescRow {
            id: 1_822_942_131,
            name: "Giant Bountiful Strawberry Bush".into(),
            max_health: 500,
            despawn_time: 0.0,
            on_destroy_yield_resource_id: 0,
            scheduled_respawn_time: 0.0,
        });
        assert_eq!(s.name(1_822_942_131), Some("Giant Bountiful Strawberry Bush"));
        assert!(!s.is_watched_spawn(1_822_942_131));
    }
}
