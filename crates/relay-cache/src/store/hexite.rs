// SPDX-License-Identifier: MIT

//! World coordinates of Hexite Deposit claims, for the seed-order-safe
//! resource location attach.
//!
//! The `/deposits` join links a deposit's claim to its `resource_state` row
//! **by world coordinates** (`claim_local.location_x/z` ==
//! the resource's `location_state` x/z — different entity ids). In the
//! embedded feed the tables seed alphabetically — `claim_state` /
//! `claim_local_state` before `location_state` before `resource_state` — so
//! by the time a deposit's location row streams past, the claim side is
//! already known, but the resource row is not, and
//! [`super::ResourceSoA::set_location`] is a no-op for unknown entities.
//!
//! This index inverts that: the location arm can ask "is (x, z) a hexite
//! deposit's coords?" in O(1) and stash the row for the resource arm to
//! consume at upsert. Bounded by the number of deposits per region (~tens),
//! rebuilt naturally from live claim / claim_local updates.

use hashbrown::HashMap;

use super::claim::is_hexite_claim;

#[derive(Default)]
struct Entry {
    is_hexite: bool,
    coords: Option<(i32, i32)>,
}

/// Per-region index of hexite-deposit claim world coordinates.
#[derive(Default)]
pub struct HexiteIndex {
    /// Keyed by claim entity id. Entries persist after a claim stops being
    /// hexite (renamed / owner assigned) so stale coords cannot resurrect.
    claims: HashMap<u64, Entry>,
    /// Refcount of located hexite claims per coords (coords can collide).
    by_coords: HashMap<(i32, i32), u32>,
}

impl HexiteIndex {
    /// `claim_state` upsert; `name: None` for a delete.
    pub fn update_claim(&mut self, entity_id: u64, owner_player_entity_id: u64, name: Option<&str>) {
        let entry = self.claims.entry(entity_id).or_default();
        let now = name.is_some_and(|n| is_hexite_claim(owner_player_entity_id, n));
        if entry.is_hexite != now {
            let old_coords = entry.coords;
            entry.is_hexite = now;
            let is_hexite = entry.is_hexite;
            // `entry`'s borrow ends at its last use above. A coords entry is
            // counted only while the claim is hexite; re-count on flip.
            if let Some(c) = old_coords {
                self.decrement(c);
                if is_hexite {
                    *self.by_coords.entry(c).or_insert(0) += 1;
                }
            }
        }
        if name.is_none() {
            let entry = self.claims.get_mut(&entity_id).expect("inserted above");
            let old_coords = entry.coords.take();
            entry.coords = None;
            entry.is_hexite = false;
            if let Some(c) = old_coords {
                self.decrement(c);
            }
        }
    }

    /// `claim_local_state` upsert / delete for a claim entity. Safe in either
    /// table order — `claim_local_state` seeds before `claim_state`
    /// alphabetically, so the coords may arrive before the name is known;
    /// they are recorded and counted when `update_claim` marks the claim.
    pub fn update_claim_location(&mut self, entity_id: u64, has_location: bool, x: i32, z: i32) {
        let entry = self.claims.entry(entity_id).or_default();
        let now = has_location.then_some((x, z));
        if entry.coords == now {
            return;
        }
        let old = entry.coords.take();
        entry.coords = now;
        let is_hexite = entry.is_hexite;
        // `entry`'s borrow ends at its last use above; safe to touch the map.
        if is_hexite {
            if let Some(c) = old {
                self.decrement(c);
            }
            if let Some(c) = now {
                *self.by_coords.entry(c).or_insert(0) += 1;
            }
        }
    }

    /// True when a hexite deposit claim is located at exactly these coords.
    pub fn contains(&self, x: i32, z: i32) -> bool {
        self.by_coords.contains_key(&(x, z))
    }

    fn decrement(&mut self, c: (i32, i32)) {
        if let Some(n) = self.by_coords.get_mut(&c) {
            *n -= 1;
            if *n == 0 {
                self.by_coords.remove(&c);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEXITE_NAME: &str = "{0} (N: {1}, E: {2})|~Hexite Deposit|~6158|~8174";

    #[test]
    fn index_tracks_claim_and_location_lifecycle() {
        let mut ix = HexiteIndex::default();
        // Claim seeds first (no coords yet).
        ix.update_claim(10, 0, Some(HEXITE_NAME));
        assert!(!ix.contains(8647, 8382));
        // claim_local supplies the world coords.
        ix.update_claim_location(10, true, 8647, 8382);
        assert!(ix.contains(8647, 8382));
        // Claim-name coords are player-facing, not world coords.
        assert!(!ix.contains(6158, 8174));
        // Coords move.
        ix.update_claim_location(10, true, 1, 2);
        assert!(!ix.contains(8647, 8382));
        assert!(ix.contains(1, 2));
        // Claim renamed away from hexite drops the coords.
        ix.update_claim(10, 0, Some("UMB Concordia"));
        assert!(!ix.contains(1, 2));
        // Owned claims are never deposits.
        ix.update_claim(11, 5, Some(HEXITE_NAME));
        ix.update_claim_location(11, true, 3, 4);
        assert!(!ix.contains(3, 4));
    }

    #[test]
    fn index_accepts_claim_local_before_claim() {
        // Seed order is table-alphabetical: claim_local_state seeds BEFORE
        // claim_state. Coords recorded for an unknown claim must be counted
        // when the hexite name arrives.
        let mut ix = HexiteIndex::default();
        ix.update_claim_location(20, true, 5, 6);
        assert!(!ix.contains(5, 6));
        ix.update_claim(20, 0, Some(HEXITE_NAME));
        assert!(ix.contains(5, 6));
        // And the name arriving with no coords on record still counts later.
        ix.update_claim(21, 0, Some(HEXITE_NAME));
        assert!(!ix.contains(7, 8));
        ix.update_claim_location(21, true, 7, 8);
        assert!(ix.contains(7, 8));
    }
}
