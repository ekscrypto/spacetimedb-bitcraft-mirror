// SPDX-License-Identifier: MIT

//! Set of progressive craft entity ids marked public via
//! `public_progressive_action_state`.

use hashbrown::HashSet;

pub struct PublicProgressiveActionStore {
    ids: HashSet<u64>,
}

impl PublicProgressiveActionStore {
    pub fn new() -> Self {
        Self {
            ids: HashSet::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn contains(&self, entity_id: u64) -> bool {
        self.ids.contains(&entity_id)
    }

    pub fn upsert(&mut self, entity_id: u64) {
        self.ids.insert(entity_id);
    }

    pub fn delete(&mut self, entity_id: u64) {
        self.ids.remove(&entity_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_contains_delete() {
        let mut s = PublicProgressiveActionStore::new();
        assert_eq!(s.len(), 0);
        assert!(!s.contains(42));
        s.upsert(42);
        assert_eq!(s.len(), 1);
        assert!(s.contains(42));
        s.upsert(42);
        assert_eq!(s.len(), 1);
        s.delete(42);
        assert_eq!(s.len(), 0);
        assert!(!s.contains(42));
    }
}
