// SPDX-License-Identifier: MIT

//! Columnar store for `active_buff_state` (per-entity buff lists).

use hashbrown::HashMap;

use crate::decode::{ActiveBuffRow, BuffEntry};

pub struct ActiveBuffSoA {
    pub entity_id: Vec<u64>,
    /// Live buff entries only — upstream rows carry zeroed placeholders for
    /// every buff type the entity has ever seen; those are dropped at decode.
    pub buffs: Vec<Box<[BuffEntry]>>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
}

impl ActiveBuffSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            buffs: Vec::with_capacity(cap),
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

    pub fn buffs_of(&self, entity_id: u64) -> Option<&[BuffEntry]> {
        let slot = self.find(entity_id)?;
        Some(&self.buffs[slot as usize])
    }

    pub fn upsert(&mut self, row: ActiveBuffRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            let i = slot as usize;
            self.entity_id[i] = row.entity_id;
            self.buffs[i] = row.buffs;
            return;
        }
        let slot = self.alloc_slot();
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.buffs[i] = row.buffs;
        self.pk.insert(row.entity_id, slot);
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        let i = slot as usize;
        self.entity_id[i] = 0;
        self.buffs[i] = Box::default();
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.buffs.push(Box::default());
            slot
        }
    }
}

/// Upstream rows list every buff type with `start == 0 && duration == 0`
/// placeholders; only nonzero entries are live.
pub fn is_live_buff(b: &BuffEntry) -> bool {
    b.start_timestamp != 0 || b.duration != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(entity_id: u64, entries: Vec<BuffEntry>) -> ActiveBuffRow {
        ActiveBuffRow {
            entity_id,
            buffs: entries.into(),
        }
    }

    #[test]
    fn upsert_replaces_buff_set() {
        let mut s = ActiveBuffSoA::with_capacity(2);
        s.upsert(row(
            1,
            vec![BuffEntry {
                buff_id: 5,
                start_timestamp: 1_778_354_880,
                duration: 300,
                values: vec![-0.4, -0.4].into(),
            }],
        ));
        assert_eq!(s.buffs_of(1).unwrap().len(), 1);
        s.upsert(row(1, vec![]));
        assert!(s.buffs_of(1).unwrap().is_empty());
        s.delete(1);
        assert!(s.buffs_of(1).is_none());
    }

    #[test]
    fn is_live_buff_filters_placeholders() {
        let placeholder = BuffEntry {
            buff_id: 2,
            start_timestamp: 0,
            duration: 0,
            values: Box::default(),
        };
        let live = BuffEntry {
            buff_id: 9,
            start_timestamp: 1_788_616_889,
            duration: 15,
            values: Box::default(),
        };
        assert!(!is_live_buff(&placeholder));
        assert!(is_live_buff(&live));
    }
}
