// SPDX-License-Identifier: MIT

//! Columnar store for `character_stats_state` (per-entity stat values).
//!
//! `values` is indexed by `character_stat_desc.stat_type` — resolved by name
//! at read time ([`CharacterStatDescStore`]); index 0 is Maximum Health and
//! index 1 is Maximum Stamina on current live data.

use hashbrown::HashMap;

use crate::decode::CharacterStatsRow;

pub struct CharacterStatsSoA {
    pub entity_id: Vec<u64>,
    pub values: Vec<Box<[f32]>>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
}

impl CharacterStatsSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            values: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn find(&self, entity_id: u64) -> Option<u32> {
        self.pk.get(&entity_id).copied()
    }

    /// `values[idx]` when both the row and the index exist.
    pub fn value_at(&self, entity_id: u64, idx: usize) -> Option<f32> {
        let slot = self.find(entity_id)?;
        self.values[slot as usize].get(idx).copied()
    }

    pub fn upsert(&mut self, row: CharacterStatsRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            let i = slot as usize;
            self.entity_id[i] = row.entity_id;
            self.values[i] = row.values;
            return;
        }
        let slot = self.alloc_slot();
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.values[i] = row.values;
        self.pk.insert(row.entity_id, slot);
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        let i = slot as usize;
        self.entity_id[i] = 0;
        self.values[i] = Box::default();
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.values.push(Box::default());
            slot
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_at_indexed_lookup() {
        let mut s = CharacterStatsSoA::with_capacity(2);
        s.upsert(CharacterStatsRow {
            entity_id: 1,
            values: vec![230.0, 523.0].into(),
        });
        assert_eq!(s.value_at(1, 0), Some(230.0)); // Maximum Health
        assert_eq!(s.value_at(1, 1), Some(523.0)); // Maximum Stamina
        assert_eq!(s.value_at(1, 90), None);
        assert_eq!(s.value_at(2, 0), None);
    }
}
