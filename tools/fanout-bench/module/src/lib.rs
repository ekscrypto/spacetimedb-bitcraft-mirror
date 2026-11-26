//! Synthetic stand-in for the hot BitCraft tables. Row sizes are in the same
//! range as the real ones; contents are arbitrary.
use spacetimedb::{ReducerContext, Table};

const ENTITIES: u64 = 20_000;

#[spacetimedb::table(accessor = experience_state, public)]
pub struct ExperienceState {
    #[primary_key]
    pub entity_id: u64,
    pub stacks: Vec<i32>, // ~18 skills x (id, xp)
}

#[spacetimedb::table(accessor = progressive_action_state, public)]
pub struct ProgressiveActionState {
    #[primary_key]
    pub entity_id: u64,
    pub building_entity_id: u64,
    pub function_type: i32,
    pub progress: i32,
    pub recipe_id: i32,
    pub craft_count: i32,
    pub last_crit_outcome: i32,
    pub owner_entity_id: u64,
    pub lock_expiration: u64,
    pub preparation: bool,
}

#[spacetimedb::table(accessor = claim_local_state, public)]
pub struct ClaimLocalState {
    #[primary_key]
    pub entity_id: u64,
    pub supplies: i32,
    pub building_maintenance: f32,
    pub num_tiles: i32,
    pub num_tile_neighbors: u32,
    pub treasury: u32,
    pub xp_gained_since_last_coin_minting: i32,
    pub supplies_purchase_threshold: u32,
    pub supplies_purchase_price: f32,
    pub building_description_id: i32,
}

#[spacetimedb::table(accessor = stamina_state, public)]
pub struct StaminaState {
    #[primary_key]
    pub entity_id: u64,
    pub last_stamina_decrease_timestamp: u64,
    pub stamina: f32,
}

#[spacetimedb::table(accessor = counter)]
pub struct Counter {
    #[primary_key]
    pub id: u8,
    pub n: u64,
}

#[spacetimedb::reducer(init)]
pub fn init(ctx: &ReducerContext) {
    ctx.db.counter().insert(Counter { id: 0, n: 0 });
    for e in 0..ENTITIES {
        ctx.db.experience_state().insert(ExperienceState { entity_id: e, stacks: vec![0; 36] });
        ctx.db.progressive_action_state().insert(ProgressiveActionState {
            entity_id: e, building_entity_id: e, function_type: 0, progress: 0, recipe_id: 0,
            craft_count: 0, last_crit_outcome: 0, owner_entity_id: e, lock_expiration: 0, preparation: false,
        });
        ctx.db.claim_local_state().insert(ClaimLocalState {
            entity_id: e, supplies: 0, building_maintenance: 0.0, num_tiles: 0, num_tile_neighbors: 0,
            treasury: 0, xp_gained_since_last_coin_minting: 0, supplies_purchase_threshold: 0,
            supplies_purchase_price: 0.0, building_description_id: 0,
        });
        ctx.db.stamina_state().insert(StaminaState { entity_id: e, last_stamina_decrease_timestamp: 0, stamina: 0.0 });
    }
}

/// One transaction touching one row in each of the four tables.
#[spacetimedb::reducer]
pub fn tick(ctx: &ReducerContext) {
    let mut c = ctx.db.counter().id().find(0).unwrap();
    c.n += 1;
    let n = c.n;
    ctx.db.counter().id().update(c);
    let e = n.wrapping_mul(0x9E37_79B9_7F4A_7C15) % ENTITIES;
    let v = n as i32;
    ctx.db.experience_state().entity_id().update(ExperienceState { entity_id: e, stacks: vec![v; 36] });
    ctx.db.progressive_action_state().entity_id().update(ProgressiveActionState {
        entity_id: e, building_entity_id: e, function_type: 1, progress: v, recipe_id: v,
        craft_count: 1, last_crit_outcome: 0, owner_entity_id: e, lock_expiration: n, preparation: false,
    });
    ctx.db.claim_local_state().entity_id().update(ClaimLocalState {
        entity_id: e, supplies: v, building_maintenance: 1.0, num_tiles: v, num_tile_neighbors: 1,
        treasury: 1, xp_gained_since_last_coin_minting: v, supplies_purchase_threshold: 1,
        supplies_purchase_price: 1.0, building_description_id: 1,
    });
    ctx.db.stamina_state().entity_id().update(StaminaState { entity_id: e, last_stamina_decrease_timestamp: n, stamina: v as f32 });
}
