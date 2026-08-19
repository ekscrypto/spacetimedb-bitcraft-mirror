// SPDX-License-Identifier: MIT

//! Claim entity_id → u16 index table (0 reserved sentinel).

use hashbrown::HashMap;
use tracing::warn;

pub const MAX_CLAIM_INDEX: usize = 65535;

#[derive(Debug)]
pub struct ClaimIndexTable {
    /// `claim_table[0] = 0` sentinel; `claim_table[i]` is entity_id for `i >= 1`.
    claim_table: Vec<u64>,
    entity_to_index: HashMap<u64, u16>,
}

impl ClaimIndexTable {
    pub fn new() -> Self {
        Self {
            claim_table: vec![0],
            entity_to_index: HashMap::new(),
        }
    }

    pub fn claim_table(&self) -> &[u64] {
        &self.claim_table
    }

    pub fn distinct_claim_count(&self) -> u32 {
        self.claim_table.len().saturating_sub(1) as u32
    }

    pub fn alloc_or_lookup(&mut self, claim_entity_id: u64) -> u16 {
        if claim_entity_id == 0 {
            return 0;
        }
        if let Some(&idx) = self.entity_to_index.get(&claim_entity_id) {
            return idx;
        }
        if self.claim_table.len() >= MAX_CLAIM_INDEX + 1 {
            warn!(
                target: "relay_cache::roads",
                claim_entity_id,
                "claim index table full (65535); refusing new index"
            );
            return 0;
        }
        let idx = self.claim_table.len() as u16;
        self.claim_table.push(claim_entity_id);
        self.entity_to_index.insert(claim_entity_id, idx);
        idx
    }
}

impl Default for ClaimIndexTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_guard() {
        let mut table = ClaimIndexTable::new();
        for i in 1..=65535u64 {
            let idx = table.alloc_or_lookup(i);
            assert_eq!(idx as u64, i);
        }
        assert_eq!(table.alloc_or_lookup(65536), 0);
        assert_eq!(table.claim_table[0], 0);
    }
}
