// SPDX-License-Identifier: MIT

//! Hash-map store for `extraction_recipe_desc` (static extraction catalog).
//!
//! Only the identity join is kept: `recipe_id → resource_id`. This is the
//! Bit-Me session feed's last-resort target mapping — the Extract action's
//! `recipe_id` resolves here, covering every extractable resource type
//! (mushrooms, forageables, …) even when the resource tile map has not
//! seen the entity, and self-healing on game patches because the table is
//! mirrored live.

use hashbrown::HashMap;

use crate::decode::ExtractionRecipeRow;

#[derive(Debug, Default, Clone)]
pub struct ExtractionRecipeStore {
    by_recipe: HashMap<i32, i32>,
}

impl ExtractionRecipeStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.by_recipe.len()
    }

    /// Resource extracted by `recipe_id` (the Extract action's `recipe_id`).
    pub fn resource_for_recipe(&self, recipe_id: i32) -> Option<i32> {
        self.by_recipe.get(&recipe_id).copied().filter(|&r| r != 0)
    }

    pub fn upsert(&mut self, row: ExtractionRecipeRow) {
        self.by_recipe.insert(row.id, row.resource_id);
    }

    pub fn delete(&mut self, id: i32) {
        self.by_recipe.remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recipe_to_resource_join() {
        let mut s = ExtractionRecipeStore::new();
        s.upsert(ExtractionRecipeRow {
            id: 424_242,
            resource_id: 74, // Button Mushrooms
        });
        assert_eq!(s.resource_for_recipe(424_242), Some(74));
        assert_eq!(s.resource_for_recipe(1), None);
        // A zero resource id is not an identity.
        s.upsert(ExtractionRecipeRow { id: 5, resource_id: 0 });
        assert_eq!(s.resource_for_recipe(5), None);
        s.delete(424_242);
        assert_eq!(s.resource_for_recipe(424_242), None);
    }
}
