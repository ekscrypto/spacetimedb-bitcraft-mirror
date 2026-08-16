// SPDX-License-Identifier: MIT

//! Lookup store for `resource_growth_timer` — the authoritative scheduled
//! respawn clock for depleted resources.
//!
//! Hexite Deposits use this instead of `growth_state`: when a deposit is
//! depleted, the game schedules a `resource_growth_scheduled` reducer via a
//! row here whose `scheduled_at` (the `Time` arm of `ScheduleAt`) carries
//! the absolute completion timestamp. `growth_state` is a near-empty legacy
//! snapshot on current builds; this table is what `/deposits` should read
//! for `respawn_at`.

use hashbrown::HashMap;

use crate::decode::ResourceGrowthTimerRow;

pub struct ResourceGrowthTimerStore {
    /// entity_id → `scheduled_at` micros since unix epoch. Absent from the
    /// map = no growth timer (harvestable). `Some(None)` would mean a timer
    /// row exists but used the `Interval` arm — we don't expect that for
    /// hexite, so we collapse to "no known time" by storing only `Some(micros)`.
    by_entity: HashMap<u64, i64>,
}

impl ResourceGrowthTimerStore {
    pub fn new() -> Self {
        Self {
            by_entity: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_entity.len()
    }

    /// `scheduled_at` micros for this entity, or `None` if no timer / unknown.
    pub fn scheduled_at_micros(&self, entity_id: u64) -> Option<i64> {
        self.by_entity.get(&entity_id).copied()
    }

    pub fn upsert(&mut self, row: ResourceGrowthTimerRow) {
        // Only retain rows that carry an absolute `Time` timestamp. An
        // `Interval`-only row carries no usable completion time, so we drop
        // it (and remove any prior absolute entry) rather than mislead
        // `/deposits` with a stale value.
        match row.scheduled_at_micros {
            Some(micros) => {
                self.by_entity.insert(row.entity_id, micros);
            }
            None => {
                self.by_entity.remove(&row.entity_id);
            }
        }
    }

    pub fn delete(&mut self, entity_id: u64) {
        self.by_entity.remove(&entity_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_get_delete() {
        let mut s = ResourceGrowthTimerStore::new();
        s.upsert(ResourceGrowthTimerRow {
            entity_id: 10,
            scheduled_at_micros: Some(1_000_000),
            growth_recipe_id: 42,
        });
        assert_eq!(s.scheduled_at_micros(10), Some(1_000_000));
        s.delete(10);
        assert_eq!(s.scheduled_at_micros(10), None);
    }

    #[test]
    fn interval_only_row_is_dropped() {
        let mut s = ResourceGrowthTimerStore::new();
        // Seed an absolute time, then an Interval-only update replaces it.
        s.upsert(ResourceGrowthTimerRow {
            entity_id: 20,
            scheduled_at_micros: Some(2_000_000),
            growth_recipe_id: 1,
        });
        assert_eq!(s.scheduled_at_micros(20), Some(2_000_000));
        s.upsert(ResourceGrowthTimerRow {
            entity_id: 20,
            scheduled_at_micros: None,
            growth_recipe_id: 1,
        });
        assert_eq!(
            s.scheduled_at_micros(20),
            None,
            "Interval-only row must not leave a stale absolute time"
        );
    }
}
