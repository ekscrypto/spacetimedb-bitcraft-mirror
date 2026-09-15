// SPDX-License-Identifier: MIT

//! Columnar store for `mobile_entity_state` (last-active proxy + live
//! world position).
//!
//! BitCraft zeros `player_state.sign_in_timestamp` on logout, but
//! `mobile_entity_state.timestamp` (u64 unix **milliseconds**) stays on
//! the row and tracks the last position/movement update — including for
//! signed-out players. That is the public stand-in for the private
//! `player_timestamp_state` table the game client uses.
//!
//! `location_x/z` are world **milli-units** (1000 = one tile) and are the
//! live player position for the Bit-Me session feed.

use hashbrown::HashMap;

use crate::decode::MobileEntityRow;

pub struct MobileEntitySoA {
    pub entity_id: Vec<u64>,
    /// Unix milliseconds from upstream `mobile_entity_state.timestamp`.
    pub timestamp_ms: Vec<u64>,
    /// World position in milli-units (1000 = one tile).
    pub location_x: Vec<i32>,
    pub location_z: Vec<i32>,
    pub destination_x: Vec<i32>,
    pub destination_z: Vec<i32>,
    pub dimension: Vec<u32>,
    pub is_walking: Vec<bool>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
}

impl MobileEntitySoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            timestamp_ms: Vec::with_capacity(cap),
            location_x: Vec::with_capacity(cap),
            location_z: Vec::with_capacity(cap),
            destination_x: Vec::with_capacity(cap),
            destination_z: Vec::with_capacity(cap),
            dimension: Vec::with_capacity(cap),
            is_walking: Vec::with_capacity(cap),
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

    /// Last-active unix seconds when known (ms rounded down).
    pub fn last_active_timestamp(&self, entity_id: u64) -> Option<i64> {
        let slot = self.find(entity_id)?;
        let ms = self.timestamp_ms[slot as usize];
        if ms == 0 {
            return None;
        }
        i64::try_from(ms / 1000).ok()
    }

    /// Overworld world tile `(x, z)` — milli-units floored to tiles.
    /// `None` for interior dimensions (their row is not an overworld one).
    pub fn overworld_tile(&self, entity_id: u64) -> Option<(i32, i32)> {
        let slot = self.find(entity_id)?;
        let i = slot as usize;
        if self.dimension[i] != crate::decode::OVERWORLD_DIMENSION {
            return None;
        }
        Some((self.location_x[i].div_euclid(1000), self.location_z[i].div_euclid(1000)))
    }

    pub fn upsert(&mut self, row: MobileEntityRow) {
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
        self.timestamp_ms[slot as usize] = 0;
        self.location_x[slot as usize] = 0;
        self.location_z[slot as usize] = 0;
        self.destination_x[slot as usize] = 0;
        self.destination_z[slot as usize] = 0;
        self.dimension[slot as usize] = 0;
        self.is_walking[slot as usize] = false;
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.timestamp_ms.push(0);
            self.location_x.push(0);
            self.location_z.push(0);
            self.destination_x.push(0);
            self.destination_z.push(0);
            self.dimension.push(0);
            self.is_walking.push(false);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &MobileEntityRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.timestamp_ms[i] = row.timestamp_ms;
        self.location_x[i] = row.location_x;
        self.location_z[i] = row.location_z;
        self.destination_x[i] = row.destination_x;
        self.destination_z[i] = row.destination_z;
        self.dimension[i] = row.dimension;
        self.is_walking[i] = row.is_walking;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_active_converts_ms_and_skips_zero() {
        let mut s = MobileEntitySoA::with_capacity(2);
        s.upsert(MobileEntityRow {
            entity_id: 1,
            timestamp_ms: 0,
            location_x: 0,
            location_z: 0,
            destination_x: 0,
            destination_z: 0,
            dimension: 1,
            is_walking: false,
        });
        assert_eq!(s.last_active_timestamp(1), None);
        s.upsert(MobileEntityRow {
            entity_id: 1,
            timestamp_ms: 1_784_779_859_028,
            location_x: 0,
            location_z: 0,
            destination_x: 0,
            destination_z: 0,
            dimension: 1,
            is_walking: false,
        });
        assert_eq!(s.last_active_timestamp(1), Some(1_784_779_859));
        assert_eq!(s.last_active_timestamp(99), None);
    }

    #[test]
    fn overworld_tile_floors_milli_units() {
        let mut s = MobileEntitySoA::with_capacity(2);
        s.upsert(MobileEntityRow {
            entity_id: 7,
            timestamp_ms: 1_788_616_889_357,
            // Live wire sample: (12367058, 19746902) → tiles (12367, 19746).
            location_x: 12_367_058,
            location_z: 19_746_902,
            destination_x: 12_367_058,
            destination_z: 19_746_902,
            dimension: 1,
            is_walking: false,
        });
        assert_eq!(s.overworld_tile(7), Some((12_367, 19_746)));
        s.upsert(MobileEntityRow {
            entity_id: 7,
            timestamp_ms: 1_788_616_889_357,
            location_x: -1500,
            location_z: -1,
            destination_x: 0,
            destination_z: 0,
            dimension: 1,
            is_walking: true,
        });
        assert_eq!(s.overworld_tile(7), Some((-2, -1)));
        // Interior rows have no overworld position.
        s.upsert(MobileEntityRow {
            entity_id: 8,
            timestamp_ms: 5,
            location_x: 1000,
            location_z: 1000,
            destination_x: 0,
            destination_z: 0,
            dimension: 42,
            is_walking: false,
        });
        assert_eq!(s.overworld_tile(8), None);
    }
}
