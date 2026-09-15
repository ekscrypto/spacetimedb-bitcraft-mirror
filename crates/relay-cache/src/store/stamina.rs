// SPDX-License-Identifier: MIT

//! Columnar store for `stamina_state` (current stamina + regen clock).

use hashbrown::HashMap;

use crate::decode::StaminaRow;

pub struct StaminaSoA {
    pub entity_id: Vec<u64>,
    /// Unix micros of the last stamina decrease (0 when unknown).
    pub last_decrease_micros: Vec<i64>,
    pub stamina: Vec<f32>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
}

impl StaminaSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            last_decrease_micros: Vec::with_capacity(cap),
            stamina: Vec::with_capacity(cap),
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

    /// Current stamina when a row exists (F32 upstream — e.g. `155.0`).
    pub fn stamina_of(&self, entity_id: u64) -> Option<f32> {
        let slot = self.find(entity_id)?;
        Some(self.stamina[slot as usize])
    }

    /// Last stamina decrease in unix seconds when known.
    pub fn last_decrease_secs(&self, entity_id: u64) -> Option<i64> {
        let slot = self.find(entity_id)?;
        let micros = self.last_decrease_micros[slot as usize];
        if micros == 0 {
            None
        } else {
            Some(micros.div_euclid(1_000_000))
        }
    }

    pub fn upsert(&mut self, row: StaminaRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            self.write_at(slot, &row);
            return;
        }
        let slot = self.alloc_slot();
        self.write_at(slot, &row);
        self.pk.insert(row.entity_id, slot);
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        self.entity_id[slot as usize] = 0;
        self.last_decrease_micros[slot as usize] = 0;
        self.stamina[slot as usize] = 0.0;
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.last_decrease_micros.push(0);
            self.stamina.push(0.0);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &StaminaRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.last_decrease_micros[i] = row.last_decrease_micros;
        self.stamina[i] = row.stamina;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_query_delete() {
        let mut s = StaminaSoA::with_capacity(2);
        s.upsert(StaminaRow {
            entity_id: 1,
            last_decrease_micros: 1_788_617_359_136_199,
            stamina: 101.918_945,
        });
        assert_eq!(s.stamina_of(1), Some(101.918_945));
        assert_eq!(s.last_decrease_secs(1), Some(1_788_617_359));
        s.upsert(StaminaRow {
            entity_id: 1,
            last_decrease_micros: 0,
            stamina: 220.0,
        });
        assert_eq!(s.stamina_of(1), Some(220.0));
        assert_eq!(s.last_decrease_secs(1), None);
        s.delete(1);
        assert_eq!(s.stamina_of(1), None);
        assert_eq!(s.len(), 0);
    }
}
