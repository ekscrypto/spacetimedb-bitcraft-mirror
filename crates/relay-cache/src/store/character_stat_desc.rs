// SPDX-License-Identifier: MIT

//! Hash-map store for `character_stat_desc` (static stat catalog).
//!
//! Resolves the `character_stats_state.values` indices for the stats Bit-Me
//! needs by player-facing name, instead of hard-coding enum ordinals.

use hashbrown::HashMap;
use std::sync::OnceLock;

use crate::decode::CharacterStatDescRow;

#[derive(Debug, Default, Clone)]
pub struct CharacterStatDescStore {
    by_stat_type: HashMap<i32, String>,
    resolved: OnceLock<StatIndices>,
}

/// Indices into `character_stats_state.values`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatIndices {
    pub max_health: usize,
    pub max_stamina: usize,
}

impl Default for StatIndices {
    fn default() -> Self {
        // Live data: stat_type 0 = "Maximum Health", 1 = "Maximum Stamina".
        Self {
            max_health: 0,
            max_stamina: 1,
        }
    }
}

impl CharacterStatDescStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.by_stat_type.len()
    }

    pub fn name(&self, stat_type: i32) -> Option<&str> {
        self.by_stat_type.get(&stat_type).map(String::as_str)
    }

    pub fn upsert(&mut self, row: CharacterStatDescRow) {
        self.by_stat_type.insert(row.stat_type, row.name);
        self.resolved.take(); // re-resolve lazily on next query
    }

    pub fn delete(&mut self, stat_type: i32) {
        self.by_stat_type.remove(&stat_type);
        self.resolved.take();
    }

    /// Indices for (Maximum Health, Maximum Stamina), matched case- and
    /// punctuation-insensitively ("maximum stamina" / "MaxStamina").
    pub fn stat_indices(&self) -> StatIndices {
        *self.resolved.get_or_init(|| {
            let find = |needle: &str| {
                self.by_stat_type
                    .iter()
                    .find(|(_, name)| {
                        let n = normalize(name);
                        n == needle || n.contains(needle)
                    })
                    .map(|(&k, _)| usize::try_from(k).unwrap_or(0))
            };
            StatIndices {
                max_health: find("maximumhealth").or_else(|| find("maxhealth")).unwrap_or(0),
                max_stamina: find("maximumstamina").or_else(|| find("maxstamina")).unwrap_or(1),
            }
        })
    }
}

fn normalize(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(stat_type: i32, name: &str) -> CharacterStatDescRow {
        CharacterStatDescRow {
            stat_type,
            name: name.into(),
        }
    }

    #[test]
    fn resolves_live_names() {
        let mut s = CharacterStatDescStore::new();
        s.upsert(row(0, "Maximum Health"));
        s.upsert(row(1, "Maximum Stamina"));
        let idx = s.stat_indices();
        assert_eq!(idx.max_health, 0);
        assert_eq!(idx.max_stamina, 1);
    }

    #[test]
    fn resolves_camelcase_names() {
        let mut s = CharacterStatDescStore::new();
        s.upsert(row(3, "MaxHealth"));
        s.upsert(row(4, "MaxStamina"));
        let idx = s.stat_indices();
        assert_eq!(idx.max_health, 3);
        assert_eq!(idx.max_stamina, 4);
    }

    #[test]
    fn falls_back_to_live_defaults() {
        let mut s = CharacterStatDescStore::new();
        s.upsert(row(5, "Mining Speed"));
        let idx = s.stat_indices();
        assert_eq!(idx.max_health, 0);
        assert_eq!(idx.max_stamina, 1);
    }
}
