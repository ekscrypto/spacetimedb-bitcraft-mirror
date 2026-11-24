// SPDX-License-Identifier: MIT

//! Derived store: one leaderboard row per player, maintained as the player's
//! source rows are applied (`experience.rs`, `player_username.rs`,
//! `player_state.rs`). It lives inside each [`super::RegionStore`], so the
//! seed builds it alongside the raw stores and a re-seed rebuilds it.
//!
//! A delete only clears the row's "present" bit for that source. The row
//! stays readable until every source has been absent for [`DELETE_GRACE`]:
//! upstream sends an UPDATE as delete + insert in one batch, and a region
//! transfer drops a player's rows from one shard moments before they appear
//! on another. Neither should knock the player off the board.

use std::time::{Duration, Instant};

use hashbrown::HashMap;

use crate::decode::is_player_skill_id;
use crate::xp::xp_to_level;

/// How long a row whose source rows are all gone keeps being served.
pub const DELETE_GRACE: Duration = Duration::from_secs(120);

/// Which source table a presence bit belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayerStatsSource {
    Experience,
    Username,
    PlayerState,
}

impl PlayerStatsSource {
    fn bit(self) -> u8 {
        match self {
            Self::Experience => 1,
            Self::Username => 2,
            Self::PlayerState => 4,
        }
    }
}

/// Level and XP of one skill. `level` is derived from `xp` at apply time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlayerSkillStat {
    pub skill_id: i32,
    pub level: i32,
    pub xp: i32,
}

/// Read view of one player's row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlayerStatsRow<'a> {
    pub entity_id: u64,
    /// Empty until the username row has been applied.
    pub username: &'a str,
    pub signed_in: bool,
    /// Sorted by `skill_id`; excludes the `"ANY"` sentinel.
    pub skills: &'a [PlayerSkillStat],
    pub total_level: i64,
    pub total_xp: i64,
    /// Every source row is gone, but the grace period is still running.
    pub departing: bool,
}

impl PlayerStatsRow<'_> {
    pub fn skill(&self, skill_id: i32) -> Option<PlayerSkillStat> {
        self.skills
            .binary_search_by_key(&skill_id, |s| s.skill_id)
            .ok()
            .map(|i| self.skills[i])
    }
}

pub struct PlayerStatsSoA {
    pub entity_id: Vec<u64>,
    pub username: Vec<Box<str>>,
    pub skills: Vec<Box<[PlayerSkillStat]>>,
    pub total_level: Vec<i64>,
    pub total_xp: Vec<i64>,
    pub signed_in: Vec<bool>,
    /// Bitmask of [`PlayerStatsSource`]s whose row is currently applied.
    present: Vec<u8>,
    /// Set when `present` reaches zero; cleared by any later apply.
    pending_since: Vec<Option<Instant>>,
    /// Slots with `pending_since` set, so the expiry sweep never scans the
    /// whole store.
    pending: Vec<u32>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
}

impl PlayerStatsSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            username: Vec::with_capacity(cap),
            skills: Vec::with_capacity(cap),
            total_level: Vec::with_capacity(cap),
            total_xp: Vec::with_capacity(cap),
            signed_in: Vec::with_capacity(cap),
            present: Vec::with_capacity(cap),
            pending_since: Vec::with_capacity(cap),
            pending: Vec::new(),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
        }
    }

    /// Rows currently held, including any still inside the delete grace.
    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pk.is_empty()
    }

    pub fn get(&self, entity_id: u64, now: Instant) -> Option<PlayerStatsRow<'_>> {
        let slot = *self.pk.get(&entity_id)?;
        self.row_at(slot as usize, now)
    }

    /// Every row still being served at `now`.
    pub fn iter(&self, now: Instant) -> impl Iterator<Item = PlayerStatsRow<'_>> {
        self.pk
            .values()
            .filter_map(move |&slot| self.row_at(slot as usize, now))
    }

    /// Replace the row's skills and totals from an `experience_state` row.
    pub fn apply_experience(&mut self, entity_id: u64, stacks: &[(i32, i32)], now: Instant) {
        let mut skills: Vec<PlayerSkillStat> = stacks
            .iter()
            .filter(|(skill_id, _)| is_player_skill_id(*skill_id))
            .map(|&(skill_id, xp)| PlayerSkillStat {
                skill_id,
                level: i32::try_from(xp_to_level(i64::from(xp))).unwrap_or(i32::MAX),
                xp,
            })
            .collect();
        skills.sort_unstable_by_key(|s| s.skill_id);
        skills.dedup_by_key(|s| s.skill_id);
        let total_level = skills.iter().map(|s| i64::from(s.level)).sum();
        let total_xp = skills.iter().map(|s| i64::from(s.xp)).sum();

        self.sweep(now);
        let i = self.slot_for_insert(entity_id);
        self.skills[i] = skills.into_boxed_slice();
        self.total_level[i] = total_level;
        self.total_xp[i] = total_xp;
        self.mark_present(i, PlayerStatsSource::Experience);
    }

    pub fn apply_username(&mut self, entity_id: u64, username: &str, now: Instant) {
        self.sweep(now);
        let i = self.slot_for_insert(entity_id);
        self.username[i] = Box::from(username);
        self.mark_present(i, PlayerStatsSource::Username);
    }

    pub fn apply_player_state(&mut self, entity_id: u64, signed_in: bool, now: Instant) {
        self.sweep(now);
        let i = self.slot_for_insert(entity_id);
        self.signed_in[i] = signed_in;
        self.mark_present(i, PlayerStatsSource::PlayerState);
    }

    /// One source row was deleted. The row is only dropped once every source
    /// has been gone for [`DELETE_GRACE`] (see the module doc).
    pub fn mark_deleted(&mut self, entity_id: u64, source: PlayerStatsSource, now: Instant) {
        self.sweep(now);
        let Some(&slot) = self.pk.get(&entity_id) else {
            return;
        };
        let i = slot as usize;
        self.present[i] &= !source.bit();
        if self.present[i] == 0 && self.pending_since[i].is_none() {
            self.pending_since[i] = Some(now);
            self.pending.push(slot);
        }
    }

    /// Slot for `entity_id`, allocated on first sight. Seed rows arrive a
    /// table at a time, so the first source to land creates the row.
    fn slot_for_insert(&mut self, entity_id: u64) -> usize {
        if let Some(&slot) = self.pk.get(&entity_id) {
            return slot as usize;
        }
        let slot = match self.free_slots.pop() {
            Some(slot) => slot,
            None => {
                let slot = self.entity_id.len() as u32;
                self.entity_id.push(0);
                self.username.push(Box::from(""));
                self.skills.push(Box::from([]));
                self.total_level.push(0);
                self.total_xp.push(0);
                self.signed_in.push(false);
                self.present.push(0);
                self.pending_since.push(None);
                slot
            }
        };
        self.entity_id[slot as usize] = entity_id;
        self.pk.insert(entity_id, slot);
        slot as usize
    }

    fn mark_present(&mut self, i: usize, source: PlayerStatsSource) {
        self.present[i] |= source.bit();
        self.pending_since[i] = None;
    }

    /// Drop rows whose sources have all been gone for [`DELETE_GRACE`].
    fn sweep(&mut self, now: Instant) {
        if self.pending.is_empty() {
            return;
        }
        let pending = std::mem::take(&mut self.pending);
        self.pending = pending
            .into_iter()
            .filter(|&slot| {
                let i = slot as usize;
                let since = match self.pending_since[i] {
                    // Re-applied since it was marked: no longer pending.
                    None => return false,
                    Some(since) => since,
                };
                if now.saturating_duration_since(since) < DELETE_GRACE {
                    return true;
                }
                self.pk.remove(&self.entity_id[i]);
                self.entity_id[i] = 0;
                self.username[i] = Box::from("");
                self.skills[i] = Box::from([]);
                self.free_slots.push(slot);
                false
            })
            .collect();
    }

    fn row_at(&self, i: usize, now: Instant) -> Option<PlayerStatsRow<'_>> {
        let since = self.pending_since[i];
        if self.present[i] == 0 && since.is_some_and(|s| now.saturating_duration_since(s) >= DELETE_GRACE) {
            return None;
        }
        Some(PlayerStatsRow {
            entity_id: self.entity_id[i],
            username: &self.username[i],
            signed_in: self.signed_in[i],
            skills: &self.skills[i],
            total_level: self.total_level[i],
            total_xp: self.total_xp[i],
            departing: self.present[i] == 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: u64) -> Instant {
        Instant::now() + Duration::from_secs(secs)
    }

    fn fully_applied(s: &mut PlayerStatsSoA, id: u64, name: &str) {
        s.apply_experience(id, &[(1, 0), (5, 26539), (2, 14790)], t(0));
        s.apply_username(id, name, t(0));
        s.apply_player_state(id, true, t(0));
    }

    #[test]
    fn totals_skip_the_any_sentinel_and_sort_by_skill() {
        let mut s = PlayerStatsSoA::with_capacity(4);
        // Seed order: experience lands before the name and session rows.
        s.apply_experience(7, &[(1, 0), (5, 26539), (2, 14790)], t(0));
        let row = s.get(7, t(0)).unwrap();
        assert_eq!(row.username, "");
        assert_eq!(row.skills.iter().map(|k| k.skill_id).collect::<Vec<_>>(), [2, 5]);
        assert_eq!(row.skill(5).unwrap().level, 18);
        assert_eq!(row.total_xp, 41_329);
        assert_eq!(row.total_level, i64::from(row.skill(2).unwrap().level) + 18);
        assert!(row.skill(1).is_none(), "ANY sentinel is not a skill");

        s.apply_username(7, "Spiellegend", t(0));
        s.apply_player_state(7, true, t(0));
        let row = s.get(7, t(0)).unwrap();
        assert_eq!(
            (row.username, row.signed_in, row.departing),
            ("Spiellegend", true, false)
        );
    }

    #[test]
    fn update_in_one_batch_keeps_the_row() {
        let mut s = PlayerStatsSoA::with_capacity(4);
        fully_applied(&mut s, 7, "Seven");
        // Upstream UPDATE = delete + insert of the same key.
        s.mark_deleted(7, PlayerStatsSource::Experience, t(1));
        s.apply_experience(7, &[(2, 14790), (5, 27_000)], t(1));
        let row = s.get(7, t(1)).unwrap();
        assert!(!row.departing);
        assert_eq!(row.total_xp, 41_790);
        assert_eq!(row.username, "Seven", "untouched sources keep their values");
    }

    #[test]
    fn row_is_served_through_the_grace_period_then_dropped() {
        let mut s = PlayerStatsSoA::with_capacity(4);
        fully_applied(&mut s, 7, "Seven");
        // A region transfer removes every source row from this shard.
        for source in [
            PlayerStatsSource::Experience,
            PlayerStatsSource::Username,
            PlayerStatsSource::PlayerState,
        ] {
            s.mark_deleted(7, source, t(10));
        }
        let row = s.get(7, t(10) + DELETE_GRACE - Duration::from_secs(1)).unwrap();
        assert!(row.departing, "a transfer gap must not knock the player off the board");
        assert_eq!(row.username, "Seven");
        assert_eq!(s.iter(t(10) + DELETE_GRACE - Duration::from_secs(1)).count(), 1);

        let after = t(10) + DELETE_GRACE;
        assert_eq!(s.iter(after).count(), 0, "expired rows are no longer served");
        assert_eq!(s.len(), 1, "…but are only reclaimed on the next write");
        s.apply_username(8, "Eight", after);
        assert!(s.get(7, after).is_none());
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn deleting_one_source_keeps_the_row_live() {
        let mut s = PlayerStatsSoA::with_capacity(4);
        fully_applied(&mut s, 7, "Seven");
        s.mark_deleted(7, PlayerStatsSource::Username, t(0));
        let row = s.get(7, t(0) + DELETE_GRACE + Duration::from_secs(1)).unwrap();
        assert!(!row.departing);
        assert_eq!(row.total_xp, 41_329);
    }

    #[test]
    fn reinsert_inside_the_grace_period_cancels_the_pending_delete() {
        let mut s = PlayerStatsSoA::with_capacity(4);
        fully_applied(&mut s, 7, "Seven");
        for source in [
            PlayerStatsSource::Experience,
            PlayerStatsSource::Username,
            PlayerStatsSource::PlayerState,
        ] {
            s.mark_deleted(7, source, t(0));
        }
        s.apply_username(7, "Seven", t(5));
        let row = s.get(7, t(5) + DELETE_GRACE + Duration::from_secs(1)).unwrap();
        assert!(!row.departing);
        assert_eq!(row.username, "Seven");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn rename_and_sign_out_update_in_place() {
        let mut s = PlayerStatsSoA::with_capacity(4);
        fully_applied(&mut s, 7, "Old");
        s.apply_username(7, "New", t(1));
        s.apply_player_state(7, false, t(1));
        let row = s.get(7, t(1)).unwrap();
        assert_eq!((row.username, row.signed_in), ("New", false));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn delete_of_an_unknown_player_is_a_noop() {
        let mut s = PlayerStatsSoA::with_capacity(2);
        s.mark_deleted(9, PlayerStatsSource::Experience, t(0));
        assert_eq!(s.len(), 0);
    }
}
