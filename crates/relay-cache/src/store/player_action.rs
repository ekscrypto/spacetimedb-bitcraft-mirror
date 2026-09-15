// SPDX-License-Identifier: MIT

//! Columnar store for `player_action_state` (in-progress/last actions).
//!
//! PK is `auto_id`; a player holds one row per action layer (Base +
//! UpperBody). Rows persist after the action ends — upstream keeps the last
//! state (`start_time + duration` vs now tells whether it is still running).

use hashbrown::HashMap;

use crate::decode::PlayerActionRow;

pub struct PlayerActionSoA {
    pub auto_id: Vec<u64>,
    pub entity_id: Vec<u64>,
    /// Unix milliseconds.
    pub start_time_ms: Vec<u64>,
    pub duration_ms: Vec<u64>,
    pub target: Vec<Option<u64>>,
    pub recipe_id: Vec<Option<i32>>,
    pub action_type: Vec<String>,
    pub layer: Vec<String>,
    pub last_action_result: Vec<String>,
    pub client_cancel: Vec<bool>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
    by_entity: HashMap<u64, Vec<u32>>,
}

impl PlayerActionSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            auto_id: Vec::with_capacity(cap),
            entity_id: Vec::with_capacity(cap),
            start_time_ms: Vec::with_capacity(cap),
            duration_ms: Vec::with_capacity(cap),
            target: Vec::with_capacity(cap),
            recipe_id: Vec::with_capacity(cap),
            action_type: Vec::with_capacity(cap),
            layer: Vec::with_capacity(cap),
            last_action_result: Vec::with_capacity(cap),
            client_cancel: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
            by_entity: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn find(&self, auto_id: u64) -> Option<u32> {
        self.pk.get(&auto_id).copied()
    }

    /// All action rows for one entity (usually ≤ 2, one per layer).
    pub fn by_entity(&self, entity_id: u64) -> &[u32] {
        self.by_entity.get(&entity_id).map(|v| &v[..]).unwrap_or(&[])
    }

    pub fn upsert(&mut self, row: PlayerActionRow) {
        if let Some(&slot) = self.pk.get(&row.auto_id) {
            let old_entity = self.entity_id[slot as usize];
            self.write_at(slot, &row);
            if old_entity != row.entity_id {
                Self::remove_from(&mut self.by_entity, old_entity, slot);
                self.by_entity.entry(row.entity_id).or_default().push(slot);
            }
            return;
        }
        let slot = self.alloc_slot();
        self.write_at(slot, &row);
        self.pk.insert(row.auto_id, slot);
        self.by_entity.entry(row.entity_id).or_default().push(slot);
    }

    pub fn delete(&mut self, auto_id: u64) {
        let Some(slot) = self.pk.remove(&auto_id) else {
            return;
        };
        let entity = self.entity_id[slot as usize];
        Self::remove_from(&mut self.by_entity, entity, slot);
        self.auto_id[slot as usize] = 0;
        self.entity_id[slot as usize] = 0;
        self.start_time_ms[slot as usize] = 0;
        self.duration_ms[slot as usize] = 0;
        self.target[slot as usize] = None;
        self.recipe_id[slot as usize] = None;
        self.action_type[slot as usize].clear();
        self.layer[slot as usize].clear();
        self.last_action_result[slot as usize].clear();
        self.client_cancel[slot as usize] = false;
        self.free_slots.push(slot);
    }

    fn remove_from(by_entity: &mut HashMap<u64, Vec<u32>>, entity_id: u64, slot: u32) {
        if let Some(v) = by_entity.get_mut(&entity_id) {
            if let Some(pos) = v.iter().position(|&s| s == slot) {
                v.swap_remove(pos);
            }
            if v.is_empty() {
                by_entity.remove(&entity_id);
            }
        }
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            return slot;
        }
        let slot = self.auto_id.len() as u32;
        self.auto_id.push(0);
        self.entity_id.push(0);
        self.start_time_ms.push(0);
        self.duration_ms.push(0);
        self.target.push(None);
        self.recipe_id.push(None);
        self.action_type.push(String::new());
        self.layer.push(String::new());
        self.last_action_result.push(String::new());
        self.client_cancel.push(false);
        slot
    }

    fn write_at(&mut self, slot: u32, row: &PlayerActionRow) {
        let i = slot as usize;
        self.auto_id[i] = row.auto_id;
        self.entity_id[i] = row.entity_id;
        self.start_time_ms[i] = row.start_time_ms;
        self.duration_ms[i] = row.duration_ms;
        self.target[i] = row.target;
        self.recipe_id[i] = row.recipe_id;
        self.action_type[i] = row.action_type.clone();
        self.layer[i] = row.layer.clone();
        self.last_action_result[i] = row.last_action_result.clone();
        self.client_cancel[i] = row.client_cancel;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(auto_id: u64, entity_id: u64, target: Option<u64>) -> PlayerActionRow {
        PlayerActionRow {
            auto_id,
            entity_id,
            start_time_ms: 1_788_617_359_072,
            duration_ms: 150,
            target,
            recipe_id: Some(412_001),
            action_type: "Craft".into(),
            layer: "Base".into(),
            last_action_result: "Success".into(),
            client_cancel: false,
        }
    }

    #[test]
    fn by_entity_indexes_and_cleans_up() {
        let mut s = PlayerActionSoA::with_capacity(2);
        s.upsert(row(1, 10, Some(99)));
        s.upsert(row(2, 10, None)); // second layer
        assert_eq!(s.by_entity(10).len(), 2);
        s.delete(1);
        assert_eq!(s.by_entity(10).len(), 1);
        let slot = s.by_entity(10)[0] as usize;
        assert_eq!(s.auto_id[slot], 2);
        assert_eq!(s.target[slot], None); // the survivor is the layer-2 row
        s.delete(2);
        assert!(s.by_entity(10).is_empty());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn entity_change_reindexes() {
        let mut s = PlayerActionSoA::with_capacity(2);
        s.upsert(row(1, 10, None));
        s.upsert(row(1, 11, None)); // auto_id reused with a new actor?
        assert!(s.by_entity(10).is_empty());
        assert_eq!(s.by_entity(11).len(), 1);
    }
}
