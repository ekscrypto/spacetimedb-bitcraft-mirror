// SPDX-License-Identifier: MIT

//! Projection from raw BSATN row bytes → typed row structs.
//!
//! The actual BSATN walk lives in `relay_protocol::bsatn::decode_row`,
//! which is schema-driven and produces `Vec<Cell>` per row. We resolve
//! column indices by name once at shard init and then index into `cells`
//! — no per-row name lookup, no JSON in the hot path (except for the
//! nested `pockets` array, which the decoder renders as `Cell::Jsonb`
//! because it's a sum-typed product array; we walk that JSON exactly
//! once per insert to build a typed `Box<[Pocket]>`).
//!
//! ## Cell → Rust mapping
//!
//! The relay-protocol decoder maps U64 to `Cell::Bytea` (8 LE bytes) to
//! avoid a NUMERIC dependency on the Postgres side. We convert back to
//! `u64` via `from_le_bytes`. Documented invariant: BitCraft entity IDs
//! are well below `i64::MAX` (verified by `bitcraft-relay-sync` and
//! enforced by the relay), so the `Bytea` roundtrip preserves full u64
//! precision without loss.

use anyhow::{anyhow, bail, Result};
use relay_protocol::bsatn::Cell;
use relay_protocol::{bsatn, MirroredField, MirroredSchema, MirroredType};
use serde_json::Value;

use crate::store::Pocket;

pub const CLAIM_TABLE: &str = "claim_state";
pub const CLAIM_LOCAL_TABLE: &str = "claim_local_state";
pub const CLAIM_MEMBER_TABLE: &str = "claim_member_state";
pub const CLAIM_TECH_STATE_TABLE: &str = "claim_tech_state";
pub const CLAIM_TECH_DESC_TABLE: &str = "claim_tech_desc";
pub const CLAIM_TILE_COST_TABLE: &str = "claim_tile_cost";
pub const BUILDING_TABLE: &str = "building_state";
pub const INVENTORY_TABLE: &str = "inventory_state";
pub const BUILDING_DESC_TABLE: &str = "building_desc";
pub const BUILDING_NICKNAME_TABLE: &str = "building_nickname_state";
pub const LOCATION_TABLE: &str = "location_state";
pub const DIMENSION_NETWORK_TABLE: &str = "dimension_network_state";
pub const PLAYER_USERNAME_TABLE: &str = "player_username_state";
pub const PLAYER_STATE_TABLE: &str = "player_state";
/// Last movement/position clock (u64 unix ms). Persists after logout;
/// public proxy for private `player_timestamp_state`.
pub const MOBILE_ENTITY_TABLE: &str = "mobile_entity_state";
/// Live deployables (carts, mounts, caches, boats, …). Prefer v2 — newer
/// boats (Skiff II+, Clipper, …) exist only here; v1 `deployable_state` is
/// a stale subset (~2k fewer rows on bc14).
pub const DEPLOYABLE_TABLE: &str = "deployable_state_v2";
pub const DEPLOYABLE_DESC_TABLE: &str = "deployable_desc";
pub const PLAYER_HOUSING_TABLE: &str = "player_housing_state";
pub const PLAYER_HOUSING_DESC_TABLE: &str = "player_housing_desc";
pub const RENT_TABLE: &str = "rent_state";
pub const EXPERIENCE_TABLE: &str = "experience_state";
pub const SKILL_DESC_TABLE: &str = "skill_desc";
pub const PROGRESSIVE_ACTION_TABLE: &str = "progressive_action_state";
/// Presence set written by `craft_set_public` — progressive crafts whose
/// `entity_id` appears here are shared (`is_public`). Passive crafts never
/// appear; used only as a flag join onto `progressive_action_state`.
pub const PUBLIC_PROGRESSIVE_ACTION_TABLE: &str = "public_progressive_action_state";
pub const PASSIVE_CRAFT_TABLE: &str = "passive_craft_state";
pub const CRAFTING_RECIPE_DESC_TABLE: &str = "crafting_recipe_desc";
pub const RESOURCE_TABLE: &str = "resource_state";
pub const GROWTH_TABLE: &str = "growth_state";
/// Scheduled growth timer — the authoritative respawn clock for depleted
/// resources (Hexite Deposits in particular). One row per growing entity,
/// `scheduled_at` carries the absolute completion timestamp. `growth_state`
/// is a near-empty legacy snapshot on current builds; this table is what
/// the game's `resource_growth_scheduled` reducer fires against.
pub const RESOURCE_GROWTH_TIMER_TABLE: &str = "resource_growth_timer";
pub const STORAGE_LOG_TABLE: &str = "storage_log_state";

// --- Bit-Me (mobile companion) session tables ------------------------------
/// Per-entity stamina: `stamina` (F32, 0..=max) + the timestamp of the last
/// decrease (regen clock). One row per entity that has spent stamina.
pub const STAMINA_TABLE: &str = "stamina_state";
/// Per-entity active buff list. Rows carry placeholder entries (start 0 /
/// duration 0) for every buff type the entity has ever seen; only entries
/// with a nonzero start or duration are live.
pub const ACTIVE_BUFF_TABLE: &str = "active_buff_state";
/// Per-entity in-progress/last action rows. PK is `auto_id`; a player can
/// hold one row per action layer (Base + UpperBody). Rows persist after the
/// action ends — `start_time`/`duration` (unix ms / ms) say whether it is
/// still running, `last_action_result` how it ended.
pub const PLAYER_ACTION_TABLE: &str = "player_action_state";
/// Per-resource current health. ~one row per damageable resource entity
/// (hundreds of thousands per region) — NOT stored wholesale; the Bit-Me
/// tracker retains rows only for tracked action targets (`crate::bitme`).
pub const RESOURCE_HEALTH_TABLE: &str = "resource_health_state";
/// Static resource gamedata (name, max_health, despawn/respawn timers,
/// destroy-yield resource chain). Replicated per region DB; ~few k rows.
pub const RESOURCE_DESC_TABLE: &str = "resource_desc";
/// Static extraction catalog: `recipe_id → resource_id` (+ yields, tool and
/// skill requirements). ~650 rows, replicated per region DB. The Extract
/// action's `recipe_id` joins here — the last-resort "what is being
/// harvested" mapping for the Bit-Me session feed, used when the resource
/// tile map has not seen the entity.
pub const EXTRACTION_RECIPE_TABLE: &str = "extraction_recipe_desc";
/// Static stat catalog; `stat_type` indexes `character_stats_state.values`.
pub const CHARACTER_STAT_DESC_TABLE: &str = "character_stat_desc";
/// Per-entity stat values; `values[stat_type]` (0 = Maximum Health,
/// 1 = Maximum Stamina — resolved from `character_stat_desc` by name).
pub const CHARACTER_STATS_TABLE: &str = "character_stats_state";

// --- Bit-Me resolve chain (global module tables) ----------------------------
/// Exact-match lowercase username → player entity_id (the name→id index the
/// substring-scan `/player?name=` route lacks).
pub const PLAYER_LOWERCASE_USERNAME_TABLE: &str = "player_lowercase_username_state";
/// user identity (U256) → user entity_id. PK is `entity_id`.
pub const USER_STATE_TABLE: &str = "user_state";
/// user identity → home region id (U8). PK is `identity`.
pub const USER_REGION_STATE_TABLE: &str = "user_region_state";
/// region id (U8) → connection info (`host`, `module`).
pub const REGION_CONNECTION_INFO_TABLE: &str = "region_connection_info";
/// region id (U16) → player-facing region name.
pub const WORLD_REGION_NAME_TABLE: &str = "world_region_name_state";
/// Presence set of signed-in players (global; one row per online player).
/// Global `player_state` is event-driven and effectively empty at seed time,
/// so this table is the authoritative live sign-in signal.
pub const SIGNED_IN_PLAYER_TABLE: &str = "signed_in_player_state";

/// Hexite Deposit (`resource_desc.id`). Live / harvestable form.
pub const HEXITE_DEPOSIT_RESOURCE_ID: i32 = 348497955;
/// Depleted Hexite Deposit — same entity; respawn clock from
/// `resource_growth_timer` (legacy fallback: `growth_state`).
pub const DEPLETED_HEXITE_DEPOSIT_RESOURCE_ID: i32 = 854132798;

/// `skill_desc.id` for the recipe-wildcard sentinel named `"ANY"`.
/// Present on every player's `experience_stacks` at 0 XP; not a real skill.
pub const SKILL_ID_ANY: i32 = 1;

pub fn is_hexite_resource_id(resource_id: i32) -> bool {
    resource_id == HEXITE_DEPOSIT_RESOURCE_ID || resource_id == DEPLETED_HEXITE_DEPOSIT_RESOURCE_ID
}

pub fn is_player_skill_id(skill_id: i32) -> bool {
    skill_id != SKILL_ID_ANY
}

/// Overworld dimension id used when a building has no interior location row.
pub const OVERWORLD_DIMENSION: u32 = 1;

/// Resolved column indices for `claim_state`, looked up once per shard.
#[derive(Clone, Copy)]
pub struct ClaimCols {
    pub entity_id: usize,
    pub owner_player_entity_id: usize,
    pub owner_building_entity_id: usize,
    pub name: usize,
    pub neutral: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimLocalCols {
    pub entity_id: usize,
    pub supplies: usize,
    pub building_maintenance: usize,
    pub num_tiles: usize,
    pub location: usize,
    pub treasury: usize,
    pub supplies_purchase_threshold: usize,
    pub supplies_purchase_price: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimMemberCols {
    pub entity_id: usize,
    pub claim_entity_id: usize,
    pub player_entity_id: usize,
    pub user_name: usize,
    pub inventory_permission: usize,
    pub build_permission: usize,
    pub officer_permission: usize,
    pub co_owner_permission: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimTechStateCols {
    pub entity_id: usize,
    pub learned: usize,
    pub researching: usize,
    pub start_timestamp: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimTechDescCols {
    pub id: usize,
    pub name: usize,
    pub description: usize,
    pub tier: usize,
    pub tech_type: usize,
    pub supplies_cost: usize,
    pub research_time: usize,
    pub requirements: usize,
    pub members: usize,
    pub area: usize,
    pub unlocks_techs: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimTileCostCols {
    pub tile_count: usize,
    pub cost_per_tile: usize,
}

#[derive(Clone, Copy)]
pub struct ExperienceCols {
    pub entity_id: usize,
    pub experience_stacks: usize,
}

#[derive(Clone, Copy)]
pub struct SkillDescCols {
    pub id: usize,
    pub name: usize,
    pub title: usize,
    pub max_level: usize,
}

#[derive(Clone, Copy)]
pub struct ProgressiveActionCols {
    pub entity_id: usize,
    pub building_entity_id: usize,
    pub progress: usize,
    pub recipe_id: usize,
    pub craft_count: usize,
    pub owner_entity_id: usize,
}

#[derive(Clone, Copy)]
pub struct PublicProgressiveActionCols {
    pub entity_id: usize,
    pub building_entity_id: usize,
    pub owner_entity_id: usize,
}

#[derive(Clone, Copy)]
pub struct PassiveCraftCols {
    pub entity_id: usize,
    pub owner_entity_id: usize,
    pub recipe_id: usize,
    pub building_entity_id: usize,
    pub status: usize,
}

#[derive(Clone, Copy)]
pub struct CraftingRecipeDescCols {
    pub id: usize,
    pub crafted_item_stacks: usize,
    pub actions_required: usize,
}

/// Resolved column indices for `building_state`.
#[derive(Clone, Copy)]
pub struct BuildingCols {
    pub entity_id: usize,
    pub claim_entity_id: usize,
    pub building_description_id: usize,
}

/// Resolved column indices for `inventory_state`.
#[derive(Clone, Copy)]
pub struct InventoryCols {
    pub entity_id: usize,
    pub pockets: usize,
    pub inventory_index: usize,
    pub cargo_index: usize,
    pub owner_entity_id: usize,
    pub player_owner_entity_id: usize,
}

/// Resolved column indices for `building_desc` (catalog).
#[derive(Clone, Copy)]
pub struct BuildingDescCols {
    pub id: usize,
    pub name: usize,
    pub functions: usize,
}

/// Resolved column indices for `building_nickname_state`.
#[derive(Clone, Copy)]
pub struct BuildingNicknameCols {
    pub entity_id: usize,
    pub nickname: usize,
}

/// Resolved column indices for `location_state`.
#[derive(Clone, Copy)]
pub struct LocationCols {
    pub entity_id: usize,
    pub x: usize,
    pub z: usize,
    pub dimension: usize,
}

#[derive(Clone, Copy)]
pub struct ResourceCols {
    pub entity_id: usize,
    pub resource_id: usize,
    pub direction_index: usize,
}

#[derive(Clone, Copy)]
pub struct GrowthCols {
    pub entity_id: usize,
    pub end_timestamp: usize,
    pub growth_recipe_id: usize,
}

/// Resolved column indices for `resource_growth_timer`.
#[derive(Clone, Copy)]
pub struct ResourceGrowthTimerCols {
    pub entity_id: usize,
    pub scheduled_at: usize,
    pub growth_recipe_id: usize,
}

#[derive(Clone, Copy)]
pub struct StorageLogCols {
    pub id: usize,
    pub object_entity_id: usize,
    pub subject_entity_id: usize,
    pub subject_name: usize,
    pub data: usize,
    pub timestamp: usize,
    pub days_since_epoch: usize,
}

/// Resolved column indices for `dimension_network_state`.
#[derive(Clone, Copy)]
pub struct DimensionNetworkCols {
    pub entity_id: usize,
    pub building_id: usize,
    pub claim_entity_id: usize,
    pub rent_entity_id: usize,
    pub entrance_dimension_id: usize,
    pub is_collapsed: usize,
}

#[derive(Clone, Copy)]
pub struct PlayerUsernameCols {
    pub entity_id: usize,
    pub username: usize,
}

#[derive(Clone, Copy)]
pub struct PlayerStateCols {
    pub entity_id: usize,
    pub sign_in_timestamp: usize,
    pub session_start_timestamp: usize,
    pub signed_in: usize,
}

#[derive(Clone, Copy)]
pub struct MobileEntityCols {
    pub entity_id: usize,
    pub timestamp: usize,
    pub location_x: usize,
    pub location_z: usize,
    pub destination_x: usize,
    pub destination_z: usize,
    pub dimension: usize,
    pub is_walking: usize,
}

#[derive(Clone, Copy)]
pub struct StaminaCols {
    pub entity_id: usize,
    pub last_stamina_decrease_timestamp: usize,
    pub stamina: usize,
}

#[derive(Clone, Copy)]
pub struct ActiveBuffCols {
    pub entity_id: usize,
    pub active_buffs: usize,
}

#[derive(Clone, Copy)]
pub struct PlayerActionCols {
    pub auto_id: usize,
    pub entity_id: usize,
    pub start_time: usize,
    pub duration: usize,
    pub target: usize,
    pub recipe_id: usize,
    pub action_type: usize,
    pub layer: usize,
    pub last_action_result: usize,
    pub client_cancel: usize,
}

#[derive(Clone, Copy)]
pub struct ResourceHealthCols {
    pub entity_id: usize,
    pub health: usize,
}

#[derive(Clone, Copy)]
pub struct ResourceDescCols {
    pub id: usize,
    pub name: usize,
    pub max_health: usize,
    pub despawn_time: usize,
    pub on_destroy_yield_resource_id: usize,
    pub scheduled_respawn_time: usize,
}

#[derive(Clone, Copy)]
pub struct ExtractionRecipeCols {
    pub id: usize,
    pub resource_id: usize,
}

#[derive(Clone, Copy)]
pub struct CharacterStatDescCols {
    pub stat_type: usize,
    pub name: usize,
}

#[derive(Clone, Copy)]
pub struct CharacterStatsCols {
    pub entity_id: usize,
    pub values: usize,
}

// --- Bit-Me resolve chain (global module) ----------------------------------

/// Optional column bundle for the global resolve tables. Every field is
/// `Option` because the global schema may legitimately lack a table (the
/// resolve chain degrades, it does not fail the feed).
#[derive(Clone, Copy, Default)]
pub struct BitmeGlobalCols {
    pub lowercase_username: Option<(usize, usize)>, // (entity_id, username_lowercase)
    pub user_state: Option<(usize, usize)>,         // (entity_id, identity)
    pub user_region: Option<(usize, usize)>,        // (identity, region_id)
    pub region_connection: Option<(usize, usize, usize)>, // (id, host, module)
    pub world_region_name: Option<(usize, usize)>,  // (id, player_facing_name)
    pub signed_in_player: Option<usize>,            // (entity_id)
}

/// Resolve the global Bit-Me column bundle. Missing tables/columns yield
/// `None` arms — a schema change degrades resolution instead of crashing.
pub fn resolve_bitme_global_cols(schema: &MirroredSchema) -> Result<BitmeGlobalCols> {
    fn pair(schema: &MirroredSchema, table: &str, a: &str, b: &str) -> Result<Option<(usize, usize)>> {
        let Some(f) = schema.tables.iter().find(|t| t.name == table) else {
            return Ok(None);
        };
        let f = schema
            .table_product(f)
            .ok_or_else(|| anyhow!("table `{table}` is not a Product"))?;
        Ok(Some((find_field(f, a, table)?, find_field(f, b, table)?)))
    }
    let region_connection = {
        let table = REGION_CONNECTION_INFO_TABLE;
        match schema.tables.iter().find(|t| t.name == table) {
            Some(t) => {
                let f = schema
                    .table_product(t)
                    .ok_or_else(|| anyhow!("table `{table}` is not a Product"))?;
                Some((
                    find_field(f, "id", table)?,
                    find_field(f, "host", table)?,
                    find_field(f, "module", table)?,
                ))
            }
            None => None,
        }
    };
    let signed_in_player = match schema.tables.iter().find(|t| t.name == SIGNED_IN_PLAYER_TABLE) {
        Some(t) => {
            let f = schema
                .table_product(t)
                .ok_or_else(|| anyhow!("table `{SIGNED_IN_PLAYER_TABLE}` is not a Product"))?;
            Some(find_field(f, "entity_id", SIGNED_IN_PLAYER_TABLE)?)
        }
        None => None,
    };
    Ok(BitmeGlobalCols {
        lowercase_username: pair(
            schema,
            PLAYER_LOWERCASE_USERNAME_TABLE,
            "entity_id",
            "username_lowercase",
        )?,
        user_state: pair(schema, USER_STATE_TABLE, "entity_id", "identity")?,
        user_region: pair(schema, USER_REGION_STATE_TABLE, "identity", "region_id")?,
        region_connection,
        world_region_name: pair(schema, WORLD_REGION_NAME_TABLE, "id", "player_facing_name")?,
        signed_in_player,
    })
}

#[derive(Clone, Copy)]
pub struct DeployableCols {
    pub entity_id: usize,
    pub owner_id: usize,
    pub claim_entity_id: usize,
    pub deployable_description_id: usize,
    pub nickname: usize,
}

#[derive(Clone, Copy)]
pub struct DeployableDescCols {
    pub id: usize,
    pub name: usize,
    pub deployable_type: usize,
}

#[derive(Clone, Copy)]
pub struct PlayerHousingCols {
    pub entity_id: usize,
    pub entrance_building_entity_id: usize,
    pub network_entity_id: usize,
    pub rank: usize,
    pub is_empty: usize,
    pub region_index: usize,
}

#[derive(Clone, Copy)]
pub struct PlayerHousingDescCols {
    pub secondary_knowledge_id: usize,
    pub rank: usize,
    pub name: usize,
}

#[derive(Clone, Copy)]
pub struct RentCols {
    pub entity_id: usize,
    pub dimension_network_id: usize,
    pub claim_entity_id: usize,
    pub white_list: usize,
    pub active: usize,
}

/// Per-shard bundle of column indices. Built once at shard init from the
/// shared schema.
pub struct ColMaps {
    pub claim: ClaimCols,
    pub claim_local: ClaimLocalCols,
    pub claim_member: ClaimMemberCols,
    pub claim_tech_state: ClaimTechStateCols,
    pub claim_tech_desc: ClaimTechDescCols,
    pub claim_tile_cost: ClaimTileCostCols,
    pub building: BuildingCols,
    pub inventory: InventoryCols,
    pub building_desc: BuildingDescCols,
    pub building_nickname: BuildingNicknameCols,
    pub location: LocationCols,
    pub dimension_network: DimensionNetworkCols,
    pub player_username: PlayerUsernameCols,
    pub player_state: PlayerStateCols,
    pub mobile_entity: MobileEntityCols,
    pub deployable: DeployableCols,
    pub deployable_desc: DeployableDescCols,
    pub player_housing: PlayerHousingCols,
    pub player_housing_desc: PlayerHousingDescCols,
    pub rent: RentCols,
    pub experience: ExperienceCols,
    pub skill_desc: SkillDescCols,
    pub progressive_action: ProgressiveActionCols,
    pub public_progressive_action: PublicProgressiveActionCols,
    pub passive_craft: PassiveCraftCols,
    pub crafting_recipe_desc: CraftingRecipeDescCols,
    pub resource: ResourceCols,
    pub growth: GrowthCols,
    pub growth_timer: ResourceGrowthTimerCols,
    pub storage_log: StorageLogCols,
    pub stamina: StaminaCols,
    pub active_buff: ActiveBuffCols,
    pub player_action: PlayerActionCols,
    pub resource_health: ResourceHealthCols,
    pub resource_desc: ResourceDescCols,
    pub extraction_recipe: ExtractionRecipeCols,
    pub character_stat_desc: CharacterStatDescCols,
    pub character_stats: CharacterStatsCols,
}

/// Resolve column indices for the tables we hold. Errors if any expected
/// column is missing — a sign of upstream schema drift.
pub fn resolve_cols(schema: &MirroredSchema) -> Result<ColMaps> {
    Ok(ColMaps {
        claim: resolve_claim_cols(schema)?,
        claim_local: resolve_claim_local_cols(schema)?,
        claim_member: resolve_claim_member_cols(schema)?,
        claim_tech_state: resolve_claim_tech_state_cols(schema)?,
        claim_tech_desc: resolve_claim_tech_desc_cols(schema)?,
        claim_tile_cost: resolve_claim_tile_cost_cols(schema)?,
        building: resolve_building_cols(schema)?,
        inventory: resolve_inventory_cols(schema)?,
        building_desc: resolve_building_desc_cols(schema)?,
        building_nickname: resolve_building_nickname_cols(schema)?,
        location: resolve_location_cols(schema)?,
        dimension_network: resolve_dimension_network_cols(schema)?,
        player_username: resolve_player_username_cols(schema)?,
        player_state: resolve_player_state_cols(schema)?,
        mobile_entity: resolve_mobile_entity_cols(schema)?,
        deployable: resolve_deployable_cols(schema)?,
        deployable_desc: resolve_deployable_desc_cols(schema)?,
        player_housing: resolve_player_housing_cols(schema)?,
        player_housing_desc: resolve_player_housing_desc_cols(schema)?,
        rent: resolve_rent_cols(schema)?,
        experience: resolve_experience_cols(schema)?,
        skill_desc: resolve_skill_desc_cols(schema)?,
        progressive_action: resolve_progressive_action_cols(schema)?,
        public_progressive_action: resolve_public_progressive_action_cols(schema)?,
        passive_craft: resolve_passive_craft_cols(schema)?,
        crafting_recipe_desc: resolve_crafting_recipe_desc_cols(schema)?,
        resource: resolve_resource_cols(schema)?,
        growth: resolve_growth_cols(schema)?,
        growth_timer: resolve_resource_growth_timer_cols(schema)?,
        storage_log: resolve_storage_log_cols(schema)?,
        stamina: resolve_stamina_cols(schema)?,
        active_buff: resolve_active_buff_cols(schema)?,
        player_action: resolve_player_action_cols(schema)?,
        resource_health: resolve_resource_health_cols(schema)?,
        resource_desc: resolve_resource_desc_cols(schema)?,
        extraction_recipe: resolve_extraction_recipe_cols(schema)?,
        character_stat_desc: resolve_character_stat_desc_cols(schema)?,
        character_stats: resolve_character_stats_cols(schema)?,
    })
}

fn fields_of<'a>(schema: &'a MirroredSchema, table: &str) -> Result<&'a [MirroredField]> {
    let tbl = schema
        .tables
        .iter()
        .find(|t| t.name == table)
        .ok_or_else(|| anyhow!("schema has no table `{table}`"))?;
    let ty = schema
        .typespace
        .get(tbl.product_type_ref as usize)
        .ok_or_else(|| anyhow!("typespace has no type at ref {}", tbl.product_type_ref))?;
    let resolved = schema.resolve(ty);
    match resolved {
        MirroredType::Product(f) => Ok(f),
        _ => bail!("table {table} product_type_ref did not resolve to a Product"),
    }
}

fn find_field(fields: &[MirroredField], name: &str, table: &str) -> Result<usize> {
    fields
        .iter()
        .position(|f| f.name.as_deref() == Some(name))
        .ok_or_else(|| anyhow!("table `{table}` has no column `{name}`"))
}

fn resolve_claim_cols(schema: &MirroredSchema) -> Result<ClaimCols> {
    let f = fields_of(schema, CLAIM_TABLE)?;
    Ok(ClaimCols {
        entity_id: find_field(f, "entity_id", CLAIM_TABLE)?,
        owner_player_entity_id: find_field(f, "owner_player_entity_id", CLAIM_TABLE)?,
        owner_building_entity_id: find_field(f, "owner_building_entity_id", CLAIM_TABLE)?,
        name: find_field(f, "name", CLAIM_TABLE)?,
        neutral: find_field(f, "neutral", CLAIM_TABLE)?,
    })
}

fn resolve_claim_local_cols(schema: &MirroredSchema) -> Result<ClaimLocalCols> {
    let f = fields_of(schema, CLAIM_LOCAL_TABLE)?;
    Ok(ClaimLocalCols {
        entity_id: find_field(f, "entity_id", CLAIM_LOCAL_TABLE)?,
        supplies: find_field(f, "supplies", CLAIM_LOCAL_TABLE)?,
        building_maintenance: find_field(f, "building_maintenance", CLAIM_LOCAL_TABLE)?,
        num_tiles: find_field(f, "num_tiles", CLAIM_LOCAL_TABLE)?,
        location: find_field(f, "location", CLAIM_LOCAL_TABLE)?,
        treasury: find_field(f, "treasury", CLAIM_LOCAL_TABLE)?,
        supplies_purchase_threshold: find_field(f, "supplies_purchase_threshold", CLAIM_LOCAL_TABLE)?,
        supplies_purchase_price: find_field(f, "supplies_purchase_price", CLAIM_LOCAL_TABLE)?,
    })
}

fn resolve_claim_member_cols(schema: &MirroredSchema) -> Result<ClaimMemberCols> {
    let f = fields_of(schema, CLAIM_MEMBER_TABLE)?;
    Ok(ClaimMemberCols {
        entity_id: find_field(f, "entity_id", CLAIM_MEMBER_TABLE)?,
        claim_entity_id: find_field(f, "claim_entity_id", CLAIM_MEMBER_TABLE)?,
        player_entity_id: find_field(f, "player_entity_id", CLAIM_MEMBER_TABLE)?,
        user_name: find_field(f, "user_name", CLAIM_MEMBER_TABLE)?,
        inventory_permission: find_field(f, "inventory_permission", CLAIM_MEMBER_TABLE)?,
        build_permission: find_field(f, "build_permission", CLAIM_MEMBER_TABLE)?,
        officer_permission: find_field(f, "officer_permission", CLAIM_MEMBER_TABLE)?,
        co_owner_permission: find_field(f, "co_owner_permission", CLAIM_MEMBER_TABLE)?,
    })
}

fn resolve_claim_tech_state_cols(schema: &MirroredSchema) -> Result<ClaimTechStateCols> {
    let f = fields_of(schema, CLAIM_TECH_STATE_TABLE)?;
    Ok(ClaimTechStateCols {
        entity_id: find_field(f, "entity_id", CLAIM_TECH_STATE_TABLE)?,
        learned: find_field(f, "learned", CLAIM_TECH_STATE_TABLE)?,
        researching: find_field(f, "researching", CLAIM_TECH_STATE_TABLE)?,
        start_timestamp: find_field(f, "start_timestamp", CLAIM_TECH_STATE_TABLE)?,
    })
}

fn resolve_claim_tech_desc_cols(schema: &MirroredSchema) -> Result<ClaimTechDescCols> {
    let f = fields_of(schema, CLAIM_TECH_DESC_TABLE)?;
    Ok(ClaimTechDescCols {
        id: find_field(f, "id", CLAIM_TECH_DESC_TABLE)?,
        name: find_field(f, "name", CLAIM_TECH_DESC_TABLE)?,
        description: find_field(f, "description", CLAIM_TECH_DESC_TABLE)?,
        tier: find_field(f, "tier", CLAIM_TECH_DESC_TABLE)?,
        tech_type: find_field(f, "tech_type", CLAIM_TECH_DESC_TABLE)?,
        supplies_cost: find_field(f, "supplies_cost", CLAIM_TECH_DESC_TABLE)?,
        research_time: find_field(f, "research_time", CLAIM_TECH_DESC_TABLE)?,
        requirements: find_field(f, "requirements", CLAIM_TECH_DESC_TABLE)?,
        members: find_field(f, "members", CLAIM_TECH_DESC_TABLE)?,
        area: find_field(f, "area", CLAIM_TECH_DESC_TABLE)?,
        unlocks_techs: find_field(f, "unlocks_techs", CLAIM_TECH_DESC_TABLE)?,
    })
}

fn resolve_claim_tile_cost_cols(schema: &MirroredSchema) -> Result<ClaimTileCostCols> {
    let f = fields_of(schema, CLAIM_TILE_COST_TABLE)?;
    Ok(ClaimTileCostCols {
        tile_count: find_field(f, "tile_count", CLAIM_TILE_COST_TABLE)?,
        cost_per_tile: find_field(f, "cost_per_tile", CLAIM_TILE_COST_TABLE)?,
    })
}

fn resolve_experience_cols(schema: &MirroredSchema) -> Result<ExperienceCols> {
    let f = fields_of(schema, EXPERIENCE_TABLE)?;
    Ok(ExperienceCols {
        entity_id: find_field(f, "entity_id", EXPERIENCE_TABLE)?,
        experience_stacks: find_field(f, "experience_stacks", EXPERIENCE_TABLE)?,
    })
}

fn resolve_skill_desc_cols(schema: &MirroredSchema) -> Result<SkillDescCols> {
    let f = fields_of(schema, SKILL_DESC_TABLE)?;
    Ok(SkillDescCols {
        id: find_field(f, "id", SKILL_DESC_TABLE)?,
        name: find_field(f, "name", SKILL_DESC_TABLE)?,
        title: find_field(f, "title", SKILL_DESC_TABLE)?,
        max_level: find_field(f, "max_level", SKILL_DESC_TABLE)?,
    })
}

fn resolve_progressive_action_cols(schema: &MirroredSchema) -> Result<ProgressiveActionCols> {
    let f = fields_of(schema, PROGRESSIVE_ACTION_TABLE)?;
    Ok(ProgressiveActionCols {
        entity_id: find_field(f, "entity_id", PROGRESSIVE_ACTION_TABLE)?,
        building_entity_id: find_field(f, "building_entity_id", PROGRESSIVE_ACTION_TABLE)?,
        progress: find_field(f, "progress", PROGRESSIVE_ACTION_TABLE)?,
        recipe_id: find_field(f, "recipe_id", PROGRESSIVE_ACTION_TABLE)?,
        craft_count: find_field(f, "craft_count", PROGRESSIVE_ACTION_TABLE)?,
        owner_entity_id: find_field(f, "owner_entity_id", PROGRESSIVE_ACTION_TABLE)?,
    })
}

fn resolve_public_progressive_action_cols(schema: &MirroredSchema) -> Result<PublicProgressiveActionCols> {
    let f = fields_of(schema, PUBLIC_PROGRESSIVE_ACTION_TABLE)?;
    Ok(PublicProgressiveActionCols {
        entity_id: find_field(f, "entity_id", PUBLIC_PROGRESSIVE_ACTION_TABLE)?,
        building_entity_id: find_field(f, "building_entity_id", PUBLIC_PROGRESSIVE_ACTION_TABLE)?,
        owner_entity_id: find_field(f, "owner_entity_id", PUBLIC_PROGRESSIVE_ACTION_TABLE)?,
    })
}

fn resolve_passive_craft_cols(schema: &MirroredSchema) -> Result<PassiveCraftCols> {
    let f = fields_of(schema, PASSIVE_CRAFT_TABLE)?;
    Ok(PassiveCraftCols {
        entity_id: find_field(f, "entity_id", PASSIVE_CRAFT_TABLE)?,
        owner_entity_id: find_field(f, "owner_entity_id", PASSIVE_CRAFT_TABLE)?,
        recipe_id: find_field(f, "recipe_id", PASSIVE_CRAFT_TABLE)?,
        building_entity_id: find_field(f, "building_entity_id", PASSIVE_CRAFT_TABLE)?,
        status: find_field(f, "status", PASSIVE_CRAFT_TABLE)?,
    })
}

fn resolve_crafting_recipe_desc_cols(schema: &MirroredSchema) -> Result<CraftingRecipeDescCols> {
    let f = fields_of(schema, CRAFTING_RECIPE_DESC_TABLE)?;
    Ok(CraftingRecipeDescCols {
        id: find_field(f, "id", CRAFTING_RECIPE_DESC_TABLE)?,
        crafted_item_stacks: find_field(f, "crafted_item_stacks", CRAFTING_RECIPE_DESC_TABLE)?,
        actions_required: find_field(f, "actions_required", CRAFTING_RECIPE_DESC_TABLE)?,
    })
}

fn resolve_building_cols(schema: &MirroredSchema) -> Result<BuildingCols> {
    let f = fields_of(schema, BUILDING_TABLE)?;
    Ok(BuildingCols {
        entity_id: find_field(f, "entity_id", BUILDING_TABLE)?,
        claim_entity_id: find_field(f, "claim_entity_id", BUILDING_TABLE)?,
        building_description_id: find_field(f, "building_description_id", BUILDING_TABLE)?,
    })
}

fn resolve_building_desc_cols(schema: &MirroredSchema) -> Result<BuildingDescCols> {
    let f = fields_of(schema, BUILDING_DESC_TABLE)?;
    Ok(BuildingDescCols {
        id: find_field(f, "id", BUILDING_DESC_TABLE)?,
        name: find_field(f, "name", BUILDING_DESC_TABLE)?,
        functions: find_field(f, "functions", BUILDING_DESC_TABLE)?,
    })
}

fn resolve_building_nickname_cols(schema: &MirroredSchema) -> Result<BuildingNicknameCols> {
    let f = fields_of(schema, BUILDING_NICKNAME_TABLE)?;
    Ok(BuildingNicknameCols {
        entity_id: find_field(f, "entity_id", BUILDING_NICKNAME_TABLE)?,
        nickname: find_field(f, "nickname", BUILDING_NICKNAME_TABLE)?,
    })
}

fn resolve_inventory_cols(schema: &MirroredSchema) -> Result<InventoryCols> {
    let f = fields_of(schema, INVENTORY_TABLE)?;
    Ok(InventoryCols {
        entity_id: find_field(f, "entity_id", INVENTORY_TABLE)?,
        pockets: find_field(f, "pockets", INVENTORY_TABLE)?,
        inventory_index: find_field(f, "inventory_index", INVENTORY_TABLE)?,
        cargo_index: find_field(f, "cargo_index", INVENTORY_TABLE)?,
        owner_entity_id: find_field(f, "owner_entity_id", INVENTORY_TABLE)?,
        player_owner_entity_id: find_field(f, "player_owner_entity_id", INVENTORY_TABLE)?,
    })
}

fn resolve_location_cols(schema: &MirroredSchema) -> Result<LocationCols> {
    let f = fields_of(schema, LOCATION_TABLE)?;
    Ok(LocationCols {
        entity_id: find_field(f, "entity_id", LOCATION_TABLE)?,
        x: find_field(f, "x", LOCATION_TABLE)?,
        z: find_field(f, "z", LOCATION_TABLE)?,
        dimension: find_field(f, "dimension", LOCATION_TABLE)?,
    })
}

pub(crate) fn resolve_resource_cols(schema: &MirroredSchema) -> Result<ResourceCols> {
    let f = fields_of(schema, RESOURCE_TABLE)?;
    Ok(ResourceCols {
        entity_id: find_field(f, "entity_id", RESOURCE_TABLE)?,
        resource_id: find_field(f, "resource_id", RESOURCE_TABLE)?,
        direction_index: find_field(f, "direction_index", RESOURCE_TABLE)?,
    })
}

fn resolve_growth_cols(schema: &MirroredSchema) -> Result<GrowthCols> {
    let f = fields_of(schema, GROWTH_TABLE)?;
    Ok(GrowthCols {
        entity_id: find_field(f, "entity_id", GROWTH_TABLE)?,
        end_timestamp: find_field(f, "end_timestamp", GROWTH_TABLE)?,
        growth_recipe_id: find_field(f, "growth_recipe_id", GROWTH_TABLE)?,
    })
}

fn resolve_resource_growth_timer_cols(schema: &MirroredSchema) -> Result<ResourceGrowthTimerCols> {
    let f = fields_of(schema, RESOURCE_GROWTH_TIMER_TABLE)?;
    Ok(ResourceGrowthTimerCols {
        entity_id: find_field(f, "entity_id", RESOURCE_GROWTH_TIMER_TABLE)?,
        scheduled_at: find_field(f, "scheduled_at", RESOURCE_GROWTH_TIMER_TABLE)?,
        growth_recipe_id: find_field(f, "growth_recipe_id", RESOURCE_GROWTH_TIMER_TABLE)?,
    })
}

fn resolve_storage_log_cols(schema: &MirroredSchema) -> Result<StorageLogCols> {
    let f = fields_of(schema, STORAGE_LOG_TABLE)?;
    Ok(StorageLogCols {
        id: find_field(f, "id", STORAGE_LOG_TABLE)?,
        object_entity_id: find_field(f, "object_entity_id", STORAGE_LOG_TABLE)?,
        subject_entity_id: find_field(f, "subject_entity_id", STORAGE_LOG_TABLE)?,
        subject_name: find_field(f, "subject_name", STORAGE_LOG_TABLE)?,
        data: find_field(f, "data", STORAGE_LOG_TABLE)?,
        timestamp: find_field(f, "timestamp", STORAGE_LOG_TABLE)?,
        days_since_epoch: find_field(f, "days_since_epoch", STORAGE_LOG_TABLE)?,
    })
}

fn resolve_dimension_network_cols(schema: &MirroredSchema) -> Result<DimensionNetworkCols> {
    let f = fields_of(schema, DIMENSION_NETWORK_TABLE)?;
    Ok(DimensionNetworkCols {
        entity_id: find_field(f, "entity_id", DIMENSION_NETWORK_TABLE)?,
        building_id: find_field(f, "building_id", DIMENSION_NETWORK_TABLE)?,
        claim_entity_id: find_field(f, "claim_entity_id", DIMENSION_NETWORK_TABLE)?,
        rent_entity_id: find_field(f, "rent_entity_id", DIMENSION_NETWORK_TABLE)?,
        entrance_dimension_id: find_field(f, "entrance_dimension_id", DIMENSION_NETWORK_TABLE)?,
        is_collapsed: find_field(f, "is_collapsed", DIMENSION_NETWORK_TABLE)?,
    })
}

fn resolve_player_username_cols(schema: &MirroredSchema) -> Result<PlayerUsernameCols> {
    let f = fields_of(schema, PLAYER_USERNAME_TABLE)?;
    Ok(PlayerUsernameCols {
        entity_id: find_field(f, "entity_id", PLAYER_USERNAME_TABLE)?,
        username: find_field(f, "username", PLAYER_USERNAME_TABLE)?,
    })
}

pub(crate) fn resolve_player_state_cols(schema: &MirroredSchema) -> Result<PlayerStateCols> {
    let f = fields_of(schema, PLAYER_STATE_TABLE)?;
    Ok(PlayerStateCols {
        entity_id: find_field(f, "entity_id", PLAYER_STATE_TABLE)?,
        sign_in_timestamp: find_field(f, "sign_in_timestamp", PLAYER_STATE_TABLE)?,
        session_start_timestamp: find_field(f, "session_start_timestamp", PLAYER_STATE_TABLE)?,
        signed_in: find_field(f, "signed_in", PLAYER_STATE_TABLE)?,
    })
}

fn resolve_mobile_entity_cols(schema: &MirroredSchema) -> Result<MobileEntityCols> {
    let f = fields_of(schema, MOBILE_ENTITY_TABLE)?;
    Ok(MobileEntityCols {
        entity_id: find_field(f, "entity_id", MOBILE_ENTITY_TABLE)?,
        timestamp: find_field(f, "timestamp", MOBILE_ENTITY_TABLE)?,
        location_x: find_field(f, "location_x", MOBILE_ENTITY_TABLE)?,
        location_z: find_field(f, "location_z", MOBILE_ENTITY_TABLE)?,
        destination_x: find_field(f, "destination_x", MOBILE_ENTITY_TABLE)?,
        destination_z: find_field(f, "destination_z", MOBILE_ENTITY_TABLE)?,
        dimension: find_field(f, "dimension", MOBILE_ENTITY_TABLE)?,
        is_walking: find_field(f, "is_walking", MOBILE_ENTITY_TABLE)?,
    })
}

fn resolve_stamina_cols(schema: &MirroredSchema) -> Result<StaminaCols> {
    let f = fields_of(schema, STAMINA_TABLE)?;
    Ok(StaminaCols {
        entity_id: find_field(f, "entity_id", STAMINA_TABLE)?,
        last_stamina_decrease_timestamp: find_field(f, "last_stamina_decrease_timestamp", STAMINA_TABLE)?,
        stamina: find_field(f, "stamina", STAMINA_TABLE)?,
    })
}

fn resolve_active_buff_cols(schema: &MirroredSchema) -> Result<ActiveBuffCols> {
    let f = fields_of(schema, ACTIVE_BUFF_TABLE)?;
    Ok(ActiveBuffCols {
        entity_id: find_field(f, "entity_id", ACTIVE_BUFF_TABLE)?,
        active_buffs: find_field(f, "active_buffs", ACTIVE_BUFF_TABLE)?,
    })
}

fn resolve_player_action_cols(schema: &MirroredSchema) -> Result<PlayerActionCols> {
    let f = fields_of(schema, PLAYER_ACTION_TABLE)?;
    Ok(PlayerActionCols {
        auto_id: find_field(f, "auto_id", PLAYER_ACTION_TABLE)?,
        entity_id: find_field(f, "entity_id", PLAYER_ACTION_TABLE)?,
        start_time: find_field(f, "start_time", PLAYER_ACTION_TABLE)?,
        duration: find_field(f, "duration", PLAYER_ACTION_TABLE)?,
        target: find_field(f, "target", PLAYER_ACTION_TABLE)?,
        recipe_id: find_field(f, "recipe_id", PLAYER_ACTION_TABLE)?,
        action_type: find_field(f, "action_type", PLAYER_ACTION_TABLE)?,
        layer: find_field(f, "layer", PLAYER_ACTION_TABLE)?,
        last_action_result: find_field(f, "last_action_result", PLAYER_ACTION_TABLE)?,
        client_cancel: find_field(f, "client_cancel", PLAYER_ACTION_TABLE)?,
    })
}

pub(crate) fn resolve_resource_health_cols(schema: &MirroredSchema) -> Result<ResourceHealthCols> {
    let f = fields_of(schema, RESOURCE_HEALTH_TABLE)?;
    Ok(ResourceHealthCols {
        entity_id: find_field(f, "entity_id", RESOURCE_HEALTH_TABLE)?,
        health: find_field(f, "health", RESOURCE_HEALTH_TABLE)?,
    })
}

fn resolve_resource_desc_cols(schema: &MirroredSchema) -> Result<ResourceDescCols> {
    let f = fields_of(schema, RESOURCE_DESC_TABLE)?;
    Ok(ResourceDescCols {
        id: find_field(f, "id", RESOURCE_DESC_TABLE)?,
        name: find_field(f, "name", RESOURCE_DESC_TABLE)?,
        max_health: find_field(f, "max_health", RESOURCE_DESC_TABLE)?,
        despawn_time: find_field(f, "despawn_time", RESOURCE_DESC_TABLE)?,
        on_destroy_yield_resource_id: find_field(f, "on_destroy_yield_resource_id", RESOURCE_DESC_TABLE)?,
        scheduled_respawn_time: find_field(f, "scheduled_respawn_time", RESOURCE_DESC_TABLE)?,
    })
}

fn resolve_extraction_recipe_cols(schema: &MirroredSchema) -> Result<ExtractionRecipeCols> {
    let f = fields_of(schema, EXTRACTION_RECIPE_TABLE)?;
    Ok(ExtractionRecipeCols {
        id: find_field(f, "id", EXTRACTION_RECIPE_TABLE)?,
        resource_id: find_field(f, "resource_id", EXTRACTION_RECIPE_TABLE)?,
    })
}

fn resolve_character_stat_desc_cols(schema: &MirroredSchema) -> Result<CharacterStatDescCols> {
    let f = fields_of(schema, CHARACTER_STAT_DESC_TABLE)?;
    Ok(CharacterStatDescCols {
        stat_type: find_field(f, "stat_type", CHARACTER_STAT_DESC_TABLE)?,
        name: find_field(f, "name", CHARACTER_STAT_DESC_TABLE)?,
    })
}

fn resolve_character_stats_cols(schema: &MirroredSchema) -> Result<CharacterStatsCols> {
    let f = fields_of(schema, CHARACTER_STATS_TABLE)?;
    Ok(CharacterStatsCols {
        entity_id: find_field(f, "entity_id", CHARACTER_STATS_TABLE)?,
        values: find_field(f, "values", CHARACTER_STATS_TABLE)?,
    })
}

fn resolve_deployable_cols(schema: &MirroredSchema) -> Result<DeployableCols> {
    let f = fields_of(schema, DEPLOYABLE_TABLE)?;
    Ok(DeployableCols {
        entity_id: find_field(f, "entity_id", DEPLOYABLE_TABLE)?,
        owner_id: find_field(f, "owner_id", DEPLOYABLE_TABLE)?,
        claim_entity_id: find_field(f, "claim_entity_id", DEPLOYABLE_TABLE)?,
        deployable_description_id: find_field(f, "deployable_description_id", DEPLOYABLE_TABLE)?,
        nickname: find_field(f, "nickname", DEPLOYABLE_TABLE)?,
    })
}

fn resolve_deployable_desc_cols(schema: &MirroredSchema) -> Result<DeployableDescCols> {
    let f = fields_of(schema, DEPLOYABLE_DESC_TABLE)?;
    Ok(DeployableDescCols {
        id: find_field(f, "id", DEPLOYABLE_DESC_TABLE)?,
        name: find_field(f, "name", DEPLOYABLE_DESC_TABLE)?,
        deployable_type: find_field(f, "deployable_type", DEPLOYABLE_DESC_TABLE)?,
    })
}

fn resolve_player_housing_cols(schema: &MirroredSchema) -> Result<PlayerHousingCols> {
    let f = fields_of(schema, PLAYER_HOUSING_TABLE)?;
    Ok(PlayerHousingCols {
        entity_id: find_field(f, "entity_id", PLAYER_HOUSING_TABLE)?,
        entrance_building_entity_id: find_field(f, "entrance_building_entity_id", PLAYER_HOUSING_TABLE)?,
        network_entity_id: find_field(f, "network_entity_id", PLAYER_HOUSING_TABLE)?,
        rank: find_field(f, "rank", PLAYER_HOUSING_TABLE)?,
        is_empty: find_field(f, "is_empty", PLAYER_HOUSING_TABLE)?,
        // Global table replicated into every region DB; selects the
        // region shard that owns the house's buildings / inventories.
        region_index: find_field(f, "region_index", PLAYER_HOUSING_TABLE)?,
    })
}

fn resolve_player_housing_desc_cols(schema: &MirroredSchema) -> Result<PlayerHousingDescCols> {
    let f = fields_of(schema, PLAYER_HOUSING_DESC_TABLE)?;
    Ok(PlayerHousingDescCols {
        secondary_knowledge_id: find_field(f, "secondary_knowledge_id", PLAYER_HOUSING_DESC_TABLE)?,
        rank: find_field(f, "rank", PLAYER_HOUSING_DESC_TABLE)?,
        name: find_field(f, "name", PLAYER_HOUSING_DESC_TABLE)?,
    })
}

fn resolve_rent_cols(schema: &MirroredSchema) -> Result<RentCols> {
    let f = fields_of(schema, RENT_TABLE)?;
    Ok(RentCols {
        entity_id: find_field(f, "entity_id", RENT_TABLE)?,
        dimension_network_id: find_field(f, "dimension_network_id", RENT_TABLE)?,
        claim_entity_id: find_field(f, "claim_entity_id", RENT_TABLE)?,
        white_list: find_field(f, "white_list", RENT_TABLE)?,
        active: find_field(f, "active", RENT_TABLE)?,
    })
}

// --- Typed row structs (mirrors bitcraft-relay-sync::decode shapes) ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRow {
    pub entity_id: u64,
    pub owner_player_entity_id: u64,
    pub owner_building_entity_id: u64,
    pub name: String,
    pub neutral: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildingRow {
    pub entity_id: u64,
    pub claim_entity_id: u64,
    pub building_description_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildingDescRow {
    pub id: i32,
    pub name: String,
    /// True when any function entry has `storage_slots > 0` or
    /// `cargo_slots > 0` — the BitCraft signal for a storage-capable
    /// building type (chests, banks, cargo stockpiles, etc.).
    pub is_storage: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildingNicknameRow {
    pub entity_id: u64,
    pub nickname: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryRow {
    pub entity_id: u64,
    pub pockets: Box<[Pocket]>,
    pub inventory_index: i32,
    pub cargo_index: i32,
    pub owner_entity_id: u64,
    pub player_owner_entity_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationDimRow {
    pub entity_id: u64,
    pub dimension: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationRow {
    pub entity_id: u64,
    pub x: i32,
    pub z: i32,
    pub dimension: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResourceRow {
    pub entity_id: u64,
    pub resource_id: i32,
    /// `0..=5` — 60° CCW cube steps applied to `resource_desc.footprint`.
    pub direction_index: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrowthRow {
    pub entity_id: u64,
    pub end_timestamp_micros: i64,
    pub growth_recipe_id: i32,
}

/// One row of `resource_growth_timer`. `scheduled_at_micros` is the absolute
/// completion timestamp (the `Time` arm of SpacetimeDB's `ScheduleAt` sum).
/// `None` means the timer used the `Interval` arm — not currently produced for
/// hexite deposits; treated as "no known respawn time".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceGrowthTimerRow {
    pub entity_id: u64,
    pub scheduled_at_micros: Option<i64>,
    pub growth_recipe_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageLogRow {
    pub id: u64,
    pub storage_entity_id: u64,
    pub player_entity_id: u64,
    pub player_username: String,
    /// `ACTION_RESERVED` / `ACTION_WITHDRAW` / `ACTION_DEPOSIT`.
    pub action: u8,
    pub item_id: i32,
    pub item_type: u8,
    pub quantity: i32,
    pub timestamp_micros: i64,
    pub days_since_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DimensionNetworkRow {
    pub entity_id: u64,
    pub building_id: u64,
    pub claim_entity_id: u64,
    pub rent_entity_id: u64,
    pub entrance_dimension_id: u32,
    pub is_collapsed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerUsernameRow {
    pub entity_id: u64,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerStateRow {
    pub entity_id: u64,
    pub sign_in_timestamp: i32,
    pub session_start_timestamp: i32,
    pub signed_in: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MobileEntityRow {
    pub entity_id: u64,
    /// Unix milliseconds (`mobile_entity_state.timestamp` is U64).
    pub timestamp_ms: u64,
    /// World position in milli-units (1000 = one tile). Players standing in
    /// an interior dimension keep their overworld row untouched.
    pub location_x: i32,
    pub location_z: i32,
    pub destination_x: i32,
    pub destination_z: i32,
    pub dimension: u32,
    pub is_walking: bool,
}

/// One live entry of `active_buff_state.active_buffs`.
#[derive(Debug, Clone, PartialEq)]
pub struct BuffEntry {
    pub buff_id: i32,
    /// Unix seconds when the buff started (0 = inactive placeholder).
    pub start_timestamp: i32,
    /// Duration in seconds (0 = inactive placeholder / permanent marker).
    pub duration: i32,
    /// Raw stat-modifier values; meaning is per `buff_desc` gamedata.
    pub values: Box<[f32]>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ActiveBuffRow {
    pub entity_id: u64,
    pub buffs: Box<[BuffEntry]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StaminaRow {
    pub entity_id: u64,
    /// Unix micros of the last stamina decrease (regen clock).
    pub last_decrease_micros: i64,
    pub stamina: f32,
}

/// One `player_action_state` row. `target` is the acted-on entity (resource,
/// building, …) when present; `recipe_id` for craft/extract actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerActionRow {
    pub auto_id: u64,
    pub entity_id: u64,
    /// Unix milliseconds.
    pub start_time_ms: u64,
    /// Milliseconds; `start_time_ms + duration_ms` is the completion instant.
    pub duration_ms: u64,
    pub target: Option<u64>,
    pub recipe_id: Option<i32>,
    /// Upstream sum variant, e.g. `Extract`, `Craft`, `None`.
    pub action_type: String,
    /// `Base` or `UpperBody`.
    pub layer: String,
    /// `Success`, `TimingFail`, `Fail` or `Cancel`.
    pub last_action_result: String,
    pub client_cancel: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceHealthRow {
    pub entity_id: u64,
    pub health: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResourceDescRow {
    pub id: i32,
    pub name: String,
    pub max_health: i32,
    /// Seconds the resource lingers after its despawn clock starts (F32).
    pub despawn_time: f32,
    /// Resource spawned in this one's place when destroyed (growth cycle).
    pub on_destroy_yield_resource_id: i32,
    /// Seconds until a depleted resource reappears (F32).
    pub scheduled_respawn_time: f32,
}

/// The two identity fields of `extraction_recipe_desc` (the full row has
/// ~23 columns of yields/requirements we don't need).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractionRecipeRow {
    pub id: i32,
    pub resource_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CharacterStatDescRow {
    pub stat_type: i32,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CharacterStatsRow {
    pub entity_id: u64,
    /// Indexed by `character_stat_desc.stat_type` (0 = Maximum Health,
    /// 1 = Maximum Stamina).
    pub values: Box<[f32]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeployableKind {
    Cart,
    Cache,
    Mount,
    Boat,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployableRow {
    pub entity_id: u64,
    pub owner_id: u64,
    pub claim_entity_id: u64,
    pub deployable_description_id: i32,
    pub nickname: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployableDescRow {
    pub id: i32,
    pub name: String,
    pub kind: DeployableKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerHousingRow {
    /// Player entity id (PK) — not a separate housing entity.
    pub entity_id: u64,
    pub entrance_building_entity_id: u64,
    pub network_entity_id: u64,
    pub rank: i32,
    pub is_empty: bool,
    pub region_index: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerHousingDescRow {
    pub secondary_knowledge_id: i32,
    pub rank: i32,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RentRow {
    pub entity_id: u64,
    pub dimension_network_id: u64,
    pub claim_entity_id: u64,
    pub white_list: Box<[u64]>,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimLocalRow {
    pub entity_id: u64,
    pub supplies: i32,
    pub building_maintenance: f32,
    pub num_tiles: i32,
    pub treasury: u32,
    pub supplies_purchase_threshold: u32,
    pub supplies_purchase_price: f32,
    pub location_x: i32,
    pub location_z: i32,
    pub location_dimension: u32,
    pub has_location: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimMemberRow {
    pub entity_id: u64,
    pub claim_entity_id: u64,
    pub player_entity_id: u64,
    pub user_name: String,
    pub inventory_permission: bool,
    pub build_permission: bool,
    pub officer_permission: bool,
    pub co_owner_permission: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimTechStateRow {
    pub entity_id: u64,
    pub learned: Box<[i32]>,
    pub researching: i32,
    pub start_timestamp_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimTechDescRow {
    pub id: i32,
    pub name: String,
    pub description: String,
    pub tier: i32,
    pub tech_type: String,
    pub supplies_cost: i32,
    pub research_time: i32,
    pub requirements: Box<[i32]>,
    pub members: i32,
    pub area: i32,
    pub unlocks_techs: Box<[i32]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimTileCostRow {
    pub tile_count: i32,
    pub cost_per_tile: f32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExperienceRow {
    pub entity_id: u64,
    /// `(skill_id, xp quantity)` stacks.
    pub stacks: Box<[(i32, i32)]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillDescRow {
    pub id: i32,
    pub name: String,
    pub title: String,
    pub max_level: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassiveCraftStatus {
    Queued,
    Processing,
    Complete,
}

impl PassiveCraftStatus {
    pub fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressiveActionRow {
    pub entity_id: u64,
    pub building_entity_id: u64,
    pub progress: i32,
    pub recipe_id: i32,
    pub craft_count: i32,
    pub owner_entity_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicProgressiveActionRow {
    pub entity_id: u64,
    pub building_entity_id: u64,
    pub owner_entity_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassiveCraftRow {
    pub entity_id: u64,
    pub owner_entity_id: u64,
    pub recipe_id: i32,
    pub building_entity_id: u64,
    pub status: PassiveCraftStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CraftedItemStack {
    pub item_id: i32,
    pub quantity: i32,
    /// `Pocket::ITEM` or `Pocket::CARGO`.
    pub item_type: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CraftingRecipeDescRow {
    pub id: i32,
    pub actions_required: i32,
    pub crafted_item: Box<[CraftedItemStack]>,
}

// --- Decoders ---

/// Decode one BSATN row using the pre-resolved claim fields. The fields
/// slice comes from `fields_of(schema, CLAIM_TABLE)` resolved once per
/// shard (alongside `ClaimCols`) — we don't re-resolve per row.
pub fn decode_claim_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimCols,
    schema: &MirroredSchema,
) -> Result<ClaimRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimRow {
        entity_id: cell_u64(&cells[cols.entity_id], "claim.entity_id")?,
        owner_player_entity_id: cell_u64(&cells[cols.owner_player_entity_id], "claim.owner_player_entity_id")?,
        owner_building_entity_id: cell_u64(&cells[cols.owner_building_entity_id], "claim.owner_building_entity_id")?,
        name: cell_string(&cells[cols.name], "claim.name")?,
        neutral: cell_bool(&cells[cols.neutral], "claim.neutral")?,
    })
}

pub fn decode_building_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: BuildingCols,
    schema: &MirroredSchema,
) -> Result<BuildingRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(BuildingRow {
        entity_id: cell_u64(&cells[cols.entity_id], "building.entity_id")?,
        claim_entity_id: cell_u64(&cells[cols.claim_entity_id], "building.claim_entity_id")?,
        building_description_id: cell_i32(&cells[cols.building_description_id], "building.building_description_id")?,
    })
}

pub fn decode_building_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: BuildingDescCols,
    schema: &MirroredSchema,
) -> Result<BuildingDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(BuildingDescRow {
        id: cell_i32(&cells[cols.id], "building_desc.id")?,
        name: cell_string(&cells[cols.name], "building_desc.name")?,
        is_storage: functions_is_storage(&cells[cols.functions])?,
    })
}

/// `building_desc.functions` is an array of products; the BSATN decoder
/// renders it as `Cell::Jsonb`. A type is storage-capable when any entry
/// advertises item or cargo pockets.
fn functions_is_storage(cell: &Cell) -> Result<bool> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("building_desc.functions is not a JSON array: {json}");
    };
    for entry in arr {
        let Value::Object(obj) = entry else {
            continue;
        };
        let storage = obj.get("storage_slots").and_then(Value::as_i64).unwrap_or(0);
        let cargo = obj.get("cargo_slots").and_then(Value::as_i64).unwrap_or(0);
        if storage > 0 || cargo > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn decode_building_nickname_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: BuildingNicknameCols,
    schema: &MirroredSchema,
) -> Result<BuildingNicknameRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(BuildingNicknameRow {
        entity_id: cell_u64(&cells[cols.entity_id], "building_nickname.entity_id")?,
        nickname: cell_string(&cells[cols.nickname], "building_nickname.nickname")?,
    })
}

pub fn decode_inventory_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: InventoryCols,
    schema: &MirroredSchema,
) -> Result<InventoryRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(InventoryRow {
        entity_id: cell_u64(&cells[cols.entity_id], "inventory.entity_id")?,
        pockets: decode_pockets(&cells[cols.pockets])?,
        inventory_index: cell_i32(&cells[cols.inventory_index], "inventory.inventory_index")?,
        cargo_index: cell_i32(&cells[cols.cargo_index], "inventory.cargo_index")?,
        owner_entity_id: cell_u64(&cells[cols.owner_entity_id], "inventory.owner_entity_id")?,
        player_owner_entity_id: cell_u64(&cells[cols.player_owner_entity_id], "inventory.player_owner_entity_id")?,
    })
}

pub fn decode_location_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: LocationCols,
    schema: &MirroredSchema,
) -> Result<LocationRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(LocationRow {
        entity_id: cell_u64(&cells[cols.entity_id], "location.entity_id")?,
        x: cell_i32(&cells[cols.x], "location.x")?,
        z: cell_i32(&cells[cols.z], "location.z")?,
        dimension: cell_u32(&cells[cols.dimension], "location.dimension")?,
    })
}

pub fn decode_resource_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ResourceCols,
    schema: &MirroredSchema,
) -> Result<ResourceRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ResourceRow {
        entity_id: cell_u64(&cells[cols.entity_id], "resource.entity_id")?,
        resource_id: cell_i32(&cells[cols.resource_id], "resource.resource_id")?,
        direction_index: cell_i32(&cells[cols.direction_index], "resource.direction_index")?,
    })
}

pub fn decode_growth_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: GrowthCols,
    schema: &MirroredSchema,
) -> Result<GrowthRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(GrowthRow {
        entity_id: cell_u64(&cells[cols.entity_id], "growth.entity_id")?,
        end_timestamp_micros: decode_timestamp_micros(&cells[cols.end_timestamp], "growth.end_timestamp")?,
        growth_recipe_id: cell_i32(&cells[cols.growth_recipe_id], "growth.growth_recipe_id")?,
    })
}

pub fn decode_resource_growth_timer_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ResourceGrowthTimerCols,
    schema: &MirroredSchema,
) -> Result<ResourceGrowthTimerRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ResourceGrowthTimerRow {
        entity_id: cell_u64(&cells[cols.entity_id], "growth_timer.entity_id")?,
        scheduled_at_micros: decode_schedule_at_timestamp(&cells[cols.scheduled_at], "growth_timer.scheduled_at")?,
        growth_recipe_id: cell_i32(&cells[cols.growth_recipe_id], "growth_timer.growth_recipe_id")?,
    })
}

pub fn decode_storage_log_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: StorageLogCols,
    schema: &MirroredSchema,
) -> Result<StorageLogRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    let (action, item_id, item_type, quantity) = decode_action_log_data(&cells[cols.data])?;
    Ok(StorageLogRow {
        id: cell_u64(&cells[cols.id], "storage_log.id")?,
        storage_entity_id: cell_u64(&cells[cols.object_entity_id], "storage_log.object_entity_id")?,
        player_entity_id: cell_u64(&cells[cols.subject_entity_id], "storage_log.subject_entity_id")?,
        player_username: cell_string(&cells[cols.subject_name], "storage_log.subject_name")?,
        action,
        item_id,
        item_type,
        quantity,
        timestamp_micros: decode_timestamp_micros(&cells[cols.timestamp], "storage_log.timestamp")?,
        days_since_epoch: cell_i32(&cells[cols.days_since_epoch], "storage_log.days_since_epoch")?,
    })
}

/// `ActionLogData` sum → (action, item_id, item_type, quantity).
/// Reserved uses `item1`'s stack fields.
fn decode_action_log_data(cell: &Cell) -> Result<(u8, i32, u8, i32)> {
    use crate::store::storage_log::{ACTION_DEPOSIT, ACTION_RESERVED, ACTION_WITHDRAW};

    let json = cell_json(cell)?;
    let Value::Object(obj) = json else {
        bail!("storage_log.data: expected object, got {json}");
    };
    if let Some(stack) = obj.get("DepositItem") {
        let (item_id, item_type, quantity) = decode_item_stack(stack, "DepositItem")?;
        return Ok((ACTION_DEPOSIT, item_id, item_type, quantity));
    }
    if let Some(stack) = obj.get("WithdrawItem") {
        let (item_id, item_type, quantity) = decode_item_stack(stack, "WithdrawItem")?;
        return Ok((ACTION_WITHDRAW, item_id, item_type, quantity));
    }
    if let Some(reserved) = obj.get("Reserved") {
        let Value::Object(r) = reserved else {
            bail!("storage_log.data.Reserved: expected object, got {reserved}");
        };
        let stack = r
            .get("item1")
            .ok_or_else(|| anyhow!("storage_log.data.Reserved: missing item1"))?;
        let (item_id, item_type, quantity) = decode_item_stack(stack, "Reserved.item1")?;
        return Ok((ACTION_RESERVED, item_id, item_type, quantity));
    }
    bail!("storage_log.data: unknown ActionLogData variant {obj:?}")
}

fn decode_item_stack(v: &Value, ctx: &str) -> Result<(i32, u8, i32)> {
    let Value::Object(obj) = v else {
        bail!("{ctx}: expected ItemStack object, got {v}");
    };
    let item_id = json_i32(obj.get("item_id"), &format!("{ctx}.item_id"))?;
    let quantity = json_i32(obj.get("quantity"), &format!("{ctx}.quantity"))?;
    let item_type = match obj.get("item_type") {
        Some(Value::Object(t)) if t.contains_key("Item") => Pocket::ITEM,
        Some(Value::Object(t)) if t.contains_key("Cargo") => Pocket::CARGO,
        other => bail!("{ctx}.item_type unexpected: {other:?}"),
    };
    Ok((item_id, item_type, quantity))
}

pub fn decode_dimension_network_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: DimensionNetworkCols,
    schema: &MirroredSchema,
) -> Result<DimensionNetworkRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(DimensionNetworkRow {
        entity_id: cell_u64(&cells[cols.entity_id], "dimension_network.entity_id")?,
        building_id: cell_u64(&cells[cols.building_id], "dimension_network.building_id")?,
        claim_entity_id: cell_u64(&cells[cols.claim_entity_id], "dimension_network.claim_entity_id")?,
        rent_entity_id: cell_u64(&cells[cols.rent_entity_id], "dimension_network.rent_entity_id")?,
        entrance_dimension_id: cell_u32(
            &cells[cols.entrance_dimension_id],
            "dimension_network.entrance_dimension_id",
        )?,
        is_collapsed: cell_bool(&cells[cols.is_collapsed], "dimension_network.is_collapsed")?,
    })
}

pub fn decode_player_username_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PlayerUsernameCols,
    schema: &MirroredSchema,
) -> Result<PlayerUsernameRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PlayerUsernameRow {
        entity_id: cell_u64(&cells[cols.entity_id], "player_username.entity_id")?,
        username: cell_string(&cells[cols.username], "player_username.username")?,
    })
}

pub fn decode_player_state_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PlayerStateCols,
    schema: &MirroredSchema,
) -> Result<PlayerStateRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PlayerStateRow {
        entity_id: cell_u64(&cells[cols.entity_id], "player_state.entity_id")?,
        sign_in_timestamp: cell_i32(&cells[cols.sign_in_timestamp], "player_state.sign_in_timestamp")?,
        session_start_timestamp: cell_i32(
            &cells[cols.session_start_timestamp],
            "player_state.session_start_timestamp",
        )?,
        signed_in: cell_bool(&cells[cols.signed_in], "player_state.signed_in")?,
    })
}

pub fn decode_mobile_entity_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: MobileEntityCols,
    schema: &MirroredSchema,
) -> Result<MobileEntityRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(MobileEntityRow {
        entity_id: cell_u64(&cells[cols.entity_id], "mobile_entity.entity_id")?,
        timestamp_ms: cell_u64(&cells[cols.timestamp], "mobile_entity.timestamp")?,
        location_x: cell_i32(&cells[cols.location_x], "mobile_entity.location_x")?,
        location_z: cell_i32(&cells[cols.location_z], "mobile_entity.location_z")?,
        destination_x: cell_i32(&cells[cols.destination_x], "mobile_entity.destination_x")?,
        destination_z: cell_i32(&cells[cols.destination_z], "mobile_entity.destination_z")?,
        dimension: cell_u32(&cells[cols.dimension], "mobile_entity.dimension")?,
        is_walking: cell_bool(&cells[cols.is_walking], "mobile_entity.is_walking")?,
    })
}

pub fn decode_stamina_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: StaminaCols,
    schema: &MirroredSchema,
) -> Result<StaminaRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(StaminaRow {
        entity_id: cell_u64(&cells[cols.entity_id], "stamina.entity_id")?,
        last_decrease_micros: decode_timestamp_micros(
            &cells[cols.last_stamina_decrease_timestamp],
            "stamina.last_stamina_decrease_timestamp",
        )?,
        stamina: cell_f32(&cells[cols.stamina], "stamina.stamina")?,
    })
}

pub fn decode_active_buff_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ActiveBuffCols,
    schema: &MirroredSchema,
) -> Result<ActiveBuffRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ActiveBuffRow {
        entity_id: cell_u64(&cells[cols.entity_id], "active_buff.entity_id")?,
        buffs: decode_buff_entries(&cells[cols.active_buffs])?,
    })
}

/// `active_buffs` is an array of products rendered as JSON:
/// `{"buff_id": 5, "buff_start_timestamp": {"value": 1778354880},
///   "buff_duration": 300, "values": [-0.4, -0.4, -0.4]}`.
fn decode_buff_entries(cell: &Cell) -> Result<Box<[BuffEntry]>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("active_buffs is not a JSON array: {json}");
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, entry) in arr.iter().enumerate() {
        let Value::Object(obj) = entry else {
            bail!("active_buffs[{i}] is not an object: {entry}");
        };
        let buff_id = json_i32(obj.get("buff_id"), &format!("active_buffs[{i}].buff_id"))?;
        let start_timestamp = match obj.get("buff_start_timestamp") {
            Some(Value::Object(t)) => json_i32(t.get("value"), &format!("active_buffs[{i}].buff_start_timestamp"))?,
            Some(v) if v.is_i64() => v.as_i64().unwrap_or(0) as i32,
            other => bail!("active_buffs[{i}].buff_start_timestamp unexpected: {other:?}"),
        };
        let duration = json_i32(obj.get("buff_duration"), &format!("active_buffs[{i}].buff_duration"))?;
        let values = decode_f32_array(obj.get("values"), &format!("active_buffs[{i}].values"))?;
        out.push(BuffEntry {
            buff_id,
            start_timestamp,
            duration,
            values,
        });
    }
    Ok(out.into())
}

fn decode_f32_array(v: Option<&Value>, ctx: &str) -> Result<Box<[f32]>> {
    let Some(Value::Array(arr)) = v else {
        bail!("{ctx}: expected JSON array");
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, v) in arr.iter().enumerate() {
        let n = v
            .as_f64()
            .ok_or_else(|| anyhow!("{ctx}[{i}]: expected number, got {v}"))?;
        out.push(n as f32);
    }
    Ok(out.into())
}

pub fn decode_player_action_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PlayerActionCols,
    schema: &MirroredSchema,
) -> Result<PlayerActionRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PlayerActionRow {
        auto_id: cell_u64(&cells[cols.auto_id], "player_action.auto_id")?,
        entity_id: cell_u64(&cells[cols.entity_id], "player_action.entity_id")?,
        start_time_ms: cell_u64(&cells[cols.start_time], "player_action.start_time")?,
        duration_ms: cell_u64(&cells[cols.duration], "player_action.duration")?,
        target: cell_opt_u64_sum(&cells[cols.target], "player_action.target")?,
        recipe_id: cell_opt_i32_sum(&cells[cols.recipe_id], "player_action.recipe_id")?,
        action_type: sum_variant_pascal(&cells[cols.action_type], "player_action.action_type")?,
        layer: sum_variant_pascal(&cells[cols.layer], "player_action.layer")?,
        last_action_result: sum_variant_pascal(&cells[cols.last_action_result], "player_action.last_action_result")?,
        client_cancel: cell_bool(&cells[cols.client_cancel], "player_action.client_cancel")?,
    })
}

pub fn decode_resource_health_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ResourceHealthCols,
    schema: &MirroredSchema,
) -> Result<ResourceHealthRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ResourceHealthRow {
        entity_id: cell_u64(&cells[cols.entity_id], "resource_health.entity_id")?,
        health: cell_i32(&cells[cols.health], "resource_health.health")?,
    })
}

pub fn decode_resource_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ResourceDescCols,
    schema: &MirroredSchema,
) -> Result<ResourceDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ResourceDescRow {
        id: cell_i32(&cells[cols.id], "resource_desc.id")?,
        name: cell_string(&cells[cols.name], "resource_desc.name")?,
        max_health: cell_i32(&cells[cols.max_health], "resource_desc.max_health")?,
        despawn_time: cell_f32(&cells[cols.despawn_time], "resource_desc.despawn_time")?,
        on_destroy_yield_resource_id: cell_i32(
            &cells[cols.on_destroy_yield_resource_id],
            "resource_desc.on_destroy_yield_resource_id",
        )?,
        scheduled_respawn_time: cell_f32(
            &cells[cols.scheduled_respawn_time],
            "resource_desc.scheduled_respawn_time",
        )?,
    })
}

pub fn decode_extraction_recipe_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ExtractionRecipeCols,
    schema: &MirroredSchema,
) -> Result<ExtractionRecipeRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ExtractionRecipeRow {
        id: cell_i32(&cells[cols.id], "extraction_recipe.id")?,
        resource_id: cell_i32(&cells[cols.resource_id], "extraction_recipe.resource_id")?,
    })
}

pub fn decode_character_stat_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: CharacterStatDescCols,
    schema: &MirroredSchema,
) -> Result<CharacterStatDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(CharacterStatDescRow {
        stat_type: cell_i32(&cells[cols.stat_type], "character_stat_desc.stat_type")?,
        name: cell_string(&cells[cols.name], "character_stat_desc.name")?,
    })
}

pub fn decode_character_stats_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: CharacterStatsCols,
    schema: &MirroredSchema,
) -> Result<CharacterStatsRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(CharacterStatsRow {
        entity_id: cell_u64(&cells[cols.entity_id], "character_stats.entity_id")?,
        values: decode_f32_array(Some(cell_json(&cells[cols.values])?), "character_stats.values")?,
    })
}

// --- Bit-Me resolve chain row decoders (global module) ----------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmeLowercaseUsernameRow {
    pub entity_id: u64,
    pub username_lowercase: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmeUserRow {
    pub entity_id: u64,
    /// Raw little-endian identity bytes (canonical hex via [`identity_hex`]).
    pub identity: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmeUserRegionRow {
    pub identity: [u8; 32],
    pub region_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmeRegionConnectionRow {
    pub id: u8,
    pub host: String,
    pub module: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmeWorldRegionNameRow {
    pub id: u16,
    pub player_facing_name: String,
}

pub fn decode_bitme_lowercase_username(
    row: &[u8],
    fields: &[MirroredField],
    cols: (usize, usize),
    schema: &MirroredSchema,
) -> Result<BitmeLowercaseUsernameRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(BitmeLowercaseUsernameRow {
        entity_id: cell_u64(&cells[cols.0], "player_lowercase_username.entity_id")?,
        username_lowercase: cell_string(&cells[cols.1], "player_lowercase_username.username_lowercase")?,
    })
}

pub fn decode_bitme_user(
    row: &[u8],
    fields: &[MirroredField],
    cols: (usize, usize),
    schema: &MirroredSchema,
) -> Result<BitmeUserRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(BitmeUserRow {
        entity_id: cell_u64(&cells[cols.0], "user_state.entity_id")?,
        identity: cell_identity(&cells[cols.1], "user_state.identity")?,
    })
}

pub fn decode_bitme_user_region(
    row: &[u8],
    fields: &[MirroredField],
    cols: (usize, usize),
    schema: &MirroredSchema,
) -> Result<BitmeUserRegionRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(BitmeUserRegionRow {
        identity: cell_identity(&cells[cols.0], "user_region_state.identity")?,
        region_id: cell_u8(&cells[cols.1], "user_region_state.region_id")?,
    })
}

pub fn decode_bitme_region_connection(
    row: &[u8],
    fields: &[MirroredField],
    cols: (usize, usize, usize),
    schema: &MirroredSchema,
) -> Result<BitmeRegionConnectionRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(BitmeRegionConnectionRow {
        id: cell_u8(&cells[cols.0], "region_connection_info.id")?,
        host: cell_string(&cells[cols.1], "region_connection_info.host")?,
        module: cell_string(&cells[cols.2], "region_connection_info.module")?,
    })
}

/// `signed_in_player_state` PK (entity_id) — presence means signed in.
pub fn decode_bitme_signed_in_player(
    row: &[u8],
    fields: &[MirroredField],
    col: usize,
    schema: &MirroredSchema,
) -> Result<u64> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    cell_u64(&cells[col], "signed_in_player.entity_id")
}

pub fn decode_bitme_world_region_name(
    row: &[u8],
    fields: &[MirroredField],
    cols: (usize, usize),
    schema: &MirroredSchema,
) -> Result<BitmeWorldRegionNameRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    let id = cell_u16(&cells[cols.0], "world_region_name_state.id")?;
    Ok(BitmeWorldRegionNameRow {
        id,
        player_facing_name: cell_string(&cells[cols.1], "world_region_name_state.player_facing_name")?,
    })
}

/// Identity cells are the `__identity__` U256 wrapper — relay-protocol
/// unwraps it to a 32-byte `Cell::Bytea`.
fn cell_identity(cell: &Cell, ctx: &str) -> Result<[u8; 32]> {
    let bytes = match cell {
        Cell::Bytea(Some(b)) => b,
        _ => bail!("{ctx}: expected Bytea, got {cell:?}"),
    };
    if bytes.len() != 32 {
        bail!("{ctx}: expected 32-byte identity, got {} bytes", bytes.len());
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(bytes);
    Ok(arr)
}

/// U16 is mapped to `Cell::Integer` by relay-protocol.
fn cell_u16(cell: &Cell, ctx: &str) -> Result<u16> {
    match cell {
        Cell::Integer(Some(n)) => u16::try_from(*n).map_err(|_| anyhow!("{ctx}: Integer {n} out of u16 range")),
        _ => bail!("{ctx}: expected Integer, got {cell:?}"),
    }
}

pub fn decode_deployable_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: DeployableCols,
    schema: &MirroredSchema,
) -> Result<DeployableRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(DeployableRow {
        entity_id: cell_u64(&cells[cols.entity_id], "deployable.entity_id")?,
        owner_id: cell_u64(&cells[cols.owner_id], "deployable.owner_id")?,
        claim_entity_id: cell_u64(&cells[cols.claim_entity_id], "deployable.claim_entity_id")?,
        deployable_description_id: cell_i32(
            &cells[cols.deployable_description_id],
            "deployable.deployable_description_id",
        )?,
        nickname: cell_string(&cells[cols.nickname], "deployable.nickname")?,
    })
}

pub fn decode_deployable_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: DeployableDescCols,
    schema: &MirroredSchema,
) -> Result<DeployableDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(DeployableDescRow {
        id: cell_i32(&cells[cols.id], "deployable_desc.id")?,
        name: cell_string(&cells[cols.name], "deployable_desc.name")?,
        kind: deployable_kind_from_cell(&cells[cols.deployable_type])?,
    })
}

fn deployable_kind_from_cell(cell: &Cell) -> Result<DeployableKind> {
    let json = cell_json(cell)?;
    // BSATN → Jsonb emits `{"Boat":{}}`; ignore Stall / SiegeEngine as Other.
    let Value::Object(obj) = json else {
        bail!("deployable_type is not an object: {json}");
    };
    if obj.contains_key("Cart") {
        Ok(DeployableKind::Cart)
    } else if obj.contains_key("Cache") {
        Ok(DeployableKind::Cache)
    } else if obj.contains_key("Mount") {
        Ok(DeployableKind::Mount)
    } else if obj.contains_key("Boat") {
        Ok(DeployableKind::Boat)
    } else {
        Ok(DeployableKind::Other)
    }
}

pub fn decode_player_housing_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PlayerHousingCols,
    schema: &MirroredSchema,
) -> Result<PlayerHousingRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PlayerHousingRow {
        entity_id: cell_u64(&cells[cols.entity_id], "player_housing.entity_id")?,
        entrance_building_entity_id: cell_u64(
            &cells[cols.entrance_building_entity_id],
            "player_housing.entrance_building_entity_id",
        )?,
        network_entity_id: cell_u64(&cells[cols.network_entity_id], "player_housing.network_entity_id")?,
        rank: cell_i32(&cells[cols.rank], "player_housing.rank")?,
        is_empty: cell_bool(&cells[cols.is_empty], "player_housing.is_empty")?,
        region_index: cell_u8(&cells[cols.region_index], "player_housing.region_index")?,
    })
}

pub fn decode_player_housing_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PlayerHousingDescCols,
    schema: &MirroredSchema,
) -> Result<PlayerHousingDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PlayerHousingDescRow {
        secondary_knowledge_id: cell_i32(
            &cells[cols.secondary_knowledge_id],
            "player_housing_desc.secondary_knowledge_id",
        )?,
        rank: cell_i32(&cells[cols.rank], "player_housing_desc.rank")?,
        name: cell_string(&cells[cols.name], "player_housing_desc.name")?,
    })
}

pub fn decode_rent_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: RentCols,
    schema: &MirroredSchema,
) -> Result<RentRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(RentRow {
        entity_id: cell_u64(&cells[cols.entity_id], "rent.entity_id")?,
        dimension_network_id: cell_u64(&cells[cols.dimension_network_id], "rent.dimension_network_id")?,
        claim_entity_id: cell_u64(&cells[cols.claim_entity_id], "rent.claim_entity_id")?,
        white_list: decode_u64_array(&cells[cols.white_list], "rent.white_list")?,
        active: cell_bool(&cells[cols.active], "rent.active")?,
    })
}

pub fn decode_claim_local_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimLocalCols,
    schema: &MirroredSchema,
) -> Result<ClaimLocalRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    let (has_location, location_x, location_z, location_dimension) = decode_optional_location(&cells[cols.location])?;
    Ok(ClaimLocalRow {
        entity_id: cell_u64(&cells[cols.entity_id], "claim_local.entity_id")?,
        supplies: cell_i32(&cells[cols.supplies], "claim_local.supplies")?,
        building_maintenance: cell_f32(&cells[cols.building_maintenance], "claim_local.building_maintenance")?,
        num_tiles: cell_i32(&cells[cols.num_tiles], "claim_local.num_tiles")?,
        treasury: cell_u32(&cells[cols.treasury], "claim_local.treasury")?,
        supplies_purchase_threshold: cell_u32(
            &cells[cols.supplies_purchase_threshold],
            "claim_local.supplies_purchase_threshold",
        )?,
        supplies_purchase_price: cell_f32(
            &cells[cols.supplies_purchase_price],
            "claim_local.supplies_purchase_price",
        )?,
        location_x,
        location_z,
        location_dimension,
        has_location,
    })
}

pub fn decode_claim_member_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimMemberCols,
    schema: &MirroredSchema,
) -> Result<ClaimMemberRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimMemberRow {
        entity_id: cell_u64(&cells[cols.entity_id], "claim_member.entity_id")?,
        claim_entity_id: cell_u64(&cells[cols.claim_entity_id], "claim_member.claim_entity_id")?,
        player_entity_id: cell_u64(&cells[cols.player_entity_id], "claim_member.player_entity_id")?,
        user_name: cell_string(&cells[cols.user_name], "claim_member.user_name")?,
        inventory_permission: cell_bool(&cells[cols.inventory_permission], "claim_member.inventory_permission")?,
        build_permission: cell_bool(&cells[cols.build_permission], "claim_member.build_permission")?,
        officer_permission: cell_bool(&cells[cols.officer_permission], "claim_member.officer_permission")?,
        co_owner_permission: cell_bool(&cells[cols.co_owner_permission], "claim_member.co_owner_permission")?,
    })
}

pub fn decode_claim_tech_state_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimTechStateCols,
    schema: &MirroredSchema,
) -> Result<ClaimTechStateRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimTechStateRow {
        entity_id: cell_u64(&cells[cols.entity_id], "claim_tech_state.entity_id")?,
        learned: decode_i32_array(&cells[cols.learned], "claim_tech_state.learned")?,
        researching: cell_i32(&cells[cols.researching], "claim_tech_state.researching")?,
        start_timestamp_micros: decode_timestamp_micros(
            &cells[cols.start_timestamp],
            "claim_tech_state.start_timestamp",
        )?,
    })
}

pub fn decode_claim_tech_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimTechDescCols,
    schema: &MirroredSchema,
) -> Result<ClaimTechDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimTechDescRow {
        id: cell_i32(&cells[cols.id], "claim_tech_desc.id")?,
        name: cell_string(&cells[cols.name], "claim_tech_desc.name")?,
        description: cell_string(&cells[cols.description], "claim_tech_desc.description")?,
        tier: cell_i32(&cells[cols.tier], "claim_tech_desc.tier")?,
        tech_type: sum_variant_snake(&cells[cols.tech_type], "claim_tech_desc.tech_type")?,
        supplies_cost: cell_i32(&cells[cols.supplies_cost], "claim_tech_desc.supplies_cost")?,
        research_time: cell_i32(&cells[cols.research_time], "claim_tech_desc.research_time")?,
        requirements: decode_i32_array(&cells[cols.requirements], "claim_tech_desc.requirements")?,
        members: cell_i32(&cells[cols.members], "claim_tech_desc.members")?,
        area: cell_i32(&cells[cols.area], "claim_tech_desc.area")?,
        unlocks_techs: decode_i32_array(&cells[cols.unlocks_techs], "claim_tech_desc.unlocks_techs")?,
    })
}

pub fn decode_claim_tile_cost_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimTileCostCols,
    schema: &MirroredSchema,
) -> Result<ClaimTileCostRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimTileCostRow {
        tile_count: cell_i32(&cells[cols.tile_count], "claim_tile_cost.tile_count")?,
        cost_per_tile: cell_f32(&cells[cols.cost_per_tile], "claim_tile_cost.cost_per_tile")?,
    })
}

pub fn decode_experience_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ExperienceCols,
    schema: &MirroredSchema,
) -> Result<ExperienceRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ExperienceRow {
        entity_id: cell_u64(&cells[cols.entity_id], "experience.entity_id")?,
        stacks: decode_experience_stacks(&cells[cols.experience_stacks])?,
    })
}

pub fn decode_skill_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: SkillDescCols,
    schema: &MirroredSchema,
) -> Result<SkillDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(SkillDescRow {
        id: cell_i32(&cells[cols.id], "skill_desc.id")?,
        name: cell_string(&cells[cols.name], "skill_desc.name")?,
        title: cell_string(&cells[cols.title], "skill_desc.title")?,
        max_level: cell_i32(&cells[cols.max_level], "skill_desc.max_level")?,
    })
}

pub fn decode_progressive_action_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ProgressiveActionCols,
    schema: &MirroredSchema,
) -> Result<ProgressiveActionRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ProgressiveActionRow {
        entity_id: cell_u64(&cells[cols.entity_id], "progressive_action.entity_id")?,
        building_entity_id: cell_u64(&cells[cols.building_entity_id], "progressive_action.building_entity_id")?,
        progress: cell_i32(&cells[cols.progress], "progressive_action.progress")?,
        recipe_id: cell_i32(&cells[cols.recipe_id], "progressive_action.recipe_id")?,
        craft_count: cell_i32(&cells[cols.craft_count], "progressive_action.craft_count")?,
        owner_entity_id: cell_u64(&cells[cols.owner_entity_id], "progressive_action.owner_entity_id")?,
    })
}

pub fn decode_public_progressive_action_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PublicProgressiveActionCols,
    schema: &MirroredSchema,
) -> Result<PublicProgressiveActionRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PublicProgressiveActionRow {
        entity_id: cell_u64(&cells[cols.entity_id], "public_progressive_action.entity_id")?,
        building_entity_id: cell_u64(
            &cells[cols.building_entity_id],
            "public_progressive_action.building_entity_id",
        )?,
        owner_entity_id: cell_u64(
            &cells[cols.owner_entity_id],
            "public_progressive_action.owner_entity_id",
        )?,
    })
}

pub fn decode_passive_craft_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PassiveCraftCols,
    schema: &MirroredSchema,
) -> Result<PassiveCraftRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PassiveCraftRow {
        entity_id: cell_u64(&cells[cols.entity_id], "passive_craft.entity_id")?,
        owner_entity_id: cell_u64(&cells[cols.owner_entity_id], "passive_craft.owner_entity_id")?,
        recipe_id: cell_i32(&cells[cols.recipe_id], "passive_craft.recipe_id")?,
        building_entity_id: cell_u64(&cells[cols.building_entity_id], "passive_craft.building_entity_id")?,
        status: decode_passive_craft_status(&cells[cols.status])?,
    })
}

pub fn decode_crafting_recipe_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: CraftingRecipeDescCols,
    schema: &MirroredSchema,
) -> Result<CraftingRecipeDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(CraftingRecipeDescRow {
        id: cell_i32(&cells[cols.id], "crafting_recipe_desc.id")?,
        actions_required: cell_i32(&cells[cols.actions_required], "crafting_recipe_desc.actions_required")?,
        crafted_item: decode_crafted_item_stacks(&cells[cols.crafted_item_stacks])?,
    })
}

fn decode_passive_craft_status(cell: &Cell) -> Result<PassiveCraftStatus> {
    let json = cell_json(cell)?;
    let Value::Object(obj) = json else {
        bail!("passive_craft.status: expected object, got {json}");
    };
    let key = obj
        .keys()
        .next()
        .ok_or_else(|| anyhow!("passive_craft.status: empty sum object"))?;
    match key.as_str() {
        "Queued" => Ok(PassiveCraftStatus::Queued),
        "Processing" => Ok(PassiveCraftStatus::Processing),
        "Complete" => Ok(PassiveCraftStatus::Complete),
        other => bail!("passive_craft.status: unknown variant {other}"),
    }
}

fn decode_crafted_item_stacks(cell: &Cell) -> Result<Box<[CraftedItemStack]>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("crafted_item_stacks is not a JSON array: {json}");
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, entry) in arr.iter().enumerate() {
        let Value::Object(obj) = entry else {
            bail!("crafted_item_stacks[{i}] is not an object: {entry}");
        };
        let item_id = json_i32(obj.get("item_id"), &format!("crafted_item_stacks[{i}].item_id"))?;
        let quantity = json_i32(obj.get("quantity"), &format!("crafted_item_stacks[{i}].quantity"))?;
        let item_type = match obj.get("item_type") {
            Some(Value::Object(t)) if t.contains_key("Item") => Pocket::ITEM,
            Some(Value::Object(t)) if t.contains_key("Cargo") => Pocket::CARGO,
            other => bail!("crafted_item_stacks[{i}].item_type unexpected: {other:?}"),
        };
        out.push(CraftedItemStack {
            item_id,
            quantity,
            item_type,
        });
    }
    Ok(out.into())
}

/// `Array<U64>` is rendered as JSON array of hex-encoded LE byte strings.
fn decode_u64_array(cell: &Cell, ctx: &str) -> Result<Box<[u64]>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("{ctx}: expected JSON array, got {json}");
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, v) in arr.iter().enumerate() {
        let s = v
            .as_str()
            .ok_or_else(|| anyhow!("{ctx}[{i}]: expected hex string, got {v}"))?;
        let bytes = hex::decode(s).map_err(|e| anyhow!("{ctx}[{i}]: hex decode: {e}"))?;
        if bytes.len() != 8 {
            bail!("{ctx}[{i}]: expected 8 bytes, got {}", bytes.len());
        }
        let mut arr8 = [0u8; 8];
        arr8.copy_from_slice(&bytes);
        out.push(u64::from_le_bytes(arr8));
    }
    Ok(out.into())
}

/// Walk a `pockets` JSON array (as produced by `relay_protocol::bsatn`'s
/// fallback for nested sum-product arrays) into a typed `Box<[Pocket]>`.
/// The JSON only exists transiently during decode — the stored row carries
/// the typed version.
///
/// The decoder renders the upstream `pockets: Array<Pocket>` shape
/// (where `Pocket = Product{ volume: I32, contents: Option<Contents>,
/// locked: Bool }` and `Contents = Product{ item_id: I32, quantity: I32,
/// item_type: Sum{Item, Cargo}, durability: Option<I32> }`) as a JSON
/// array of objects:
///
/// ```json
/// [
///   {"volume": 100, "contents": {"some": {"item_id": 1, "quantity": 50, "item_type": {"Item": {}}, "durability": {"some": 100}}}, "locked": false},
///   {"volume": 0,   "contents": {"none": {}}, "locked": false}
/// ]
/// ```
fn decode_pockets(cell: &Cell) -> Result<Box<[Pocket]>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("pockets is not a JSON array: {json}");
    };
    let mut out = Vec::with_capacity(arr.len());
    for pocket_val in arr {
        let Value::Object(obj) = pocket_val else {
            bail!("pocket is not an object: {pocket_val}");
        };
        let volume = json_i32(obj.get("volume"), "pocket.volume")?;
        let (has_contents, item_id, quantity, item_type, has_durability, durability) = match obj.get("contents") {
            // `relay_protocol::bsatn` renders Option<T> as a one-key
            // object: `{"some": payload}` or `{"none": {}}`.
            Some(Value::Object(c)) if c.contains_key("some") => {
                let inner = &c["some"];
                let Value::Object(contents) = inner else {
                    bail!("contents.some is not an object: {inner}");
                };
                let item_id = json_i32(contents.get("item_id"), "contents.item_id")?;
                let quantity = json_i32(contents.get("quantity"), "contents.quantity")?;
                let item_type = match contents.get("item_type") {
                    Some(Value::Object(t)) if t.contains_key("Item") => Pocket::ITEM,
                    Some(Value::Object(t)) if t.contains_key("Cargo") => Pocket::CARGO,
                    other => bail!("contents.item_type unexpected: {other:?}"),
                };
                let (has_durability, durability) = match contents.get("durability") {
                    Some(Value::Object(d)) if d.contains_key("some") => {
                        let v = d["some"].as_i64().and_then(|n| i32::try_from(n).ok());
                        (true, v.unwrap_or(0))
                    }
                    _ => (false, 0),
                };
                (true, item_id, quantity, item_type, has_durability, durability)
            }
            _ => (false, 0, 0, Pocket::ITEM, false, 0),
        };
        out.push(Pocket {
            volume,
            has_contents,
            item_id,
            quantity,
            item_type,
            has_durability,
            durability,
        });
    }
    Ok(out.into())
}

fn json_i32(v: Option<&Value>, ctx: &str) -> Result<i32> {
    v.and_then(Value::as_i64)
        .and_then(|n| i32::try_from(n).ok())
        .ok_or_else(|| anyhow!("{ctx}: missing or not i32"))
}

// --- Cell accessors ---

/// Pull the inner JSON value out of `Cell::Jsonb`. Errors on any other variant.
fn cell_json(cell: &Cell) -> Result<&Value> {
    match cell {
        Cell::Jsonb(v) => Ok(v),
        _ => bail!("expected Jsonb, got {cell:?}"),
    }
}

fn cell_f32(cell: &Cell, ctx: &str) -> Result<f32> {
    match cell {
        Cell::Real(Some(n)) => Ok(*n),
        Cell::Real(None) => bail!("{ctx}: Real is NULL"),
        Cell::DoublePrecision(Some(n)) => Ok(*n as f32),
        other => bail!("{ctx}: expected Real, got {other:?}"),
    }
}

/// `Option<{x,z,dimension}>`. relay-protocol unwraps Option to a nullable
/// Cell, so `some` is bare `{"x","z","dimension"}` and `none` is `Jsonb(Null)`.
/// Also accepts the wrapped `{"some":{...}}` form for tests / older dumps.
fn decode_optional_location(cell: &Cell) -> Result<(bool, i32, i32, u32)> {
    let json = match cell {
        Cell::Jsonb(Value::Null) => return Ok((false, 0, 0, 0)),
        Cell::Jsonb(v) => v,
        other => bail!("claim_local.location: expected Jsonb, got {other:?}"),
    };
    let Value::Object(obj) = json else {
        bail!("claim_local.location is not an object: {json}");
    };
    let loc = if let Some(inner) = obj.get("some") {
        let Value::Object(loc) = inner else {
            bail!("claim_local.location.some is not an object: {inner}");
        };
        loc
    } else if obj.contains_key("x") && obj.contains_key("z") {
        obj
    } else {
        // `{"none":{}}` or empty.
        return Ok((false, 0, 0, 0));
    };
    let x = json_i32(loc.get("x"), "location.x")?;
    let z = json_i32(loc.get("z"), "location.z")?;
    let dimension = loc
        .get("dimension")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("location.dimension missing"))?;
    let dimension = u32::try_from(dimension).map_err(|_| anyhow!("location.dimension overflow"))?;
    Ok((true, x, z, dimension))
}

fn decode_i32_array(cell: &Cell, ctx: &str) -> Result<Box<[i32]>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("{ctx}: expected JSON array, got {json}");
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, v) in arr.iter().enumerate() {
        let n = v.as_i64().ok_or_else(|| anyhow!("{ctx}[{i}]: expected i64, got {v}"))?;
        out.push(i32::try_from(n).map_err(|_| anyhow!("{ctx}[{i}]: i32 overflow"))?);
    }
    Ok(out.into())
}

fn decode_timestamp_micros(cell: &Cell, ctx: &str) -> Result<i64> {
    match cell {
        Cell::Bigint(Some(n)) => Ok(*n),
        Cell::Jsonb(Value::Object(obj)) => {
            if let Some(v) = obj.get("__timestamp_micros_since_unix_epoch__") {
                return v.as_i64().ok_or_else(|| anyhow!("{ctx}: timestamp not i64"));
            }
            bail!("{ctx}: unexpected timestamp object {obj:?}")
        }
        other => bail!("{ctx}: expected timestamp, got {other:?}"),
    }
}

/// Decode SpacetimeDB's `ScheduleAt` sum (`Time(timestamp_micros)` |
/// `Interval(duration_micros)`) into the absolute completion timestamp.
/// `Time` → `Some(micros)`. `Interval` → `None` (hexite deposits use the
/// `Time` arm in practice; an interval would need a start reference we don't
/// have, so we treat it as "no known respawn time" rather than guess).
///
/// The BSATN decoder renders sums as `{"VariantName": payload}` objects
/// (same shape as all other sum columns — confirmed by live data).
/// Guard against the alternative `[tag, payload]` array form in case
/// a future protocol version switches back.
fn decode_schedule_at_timestamp(cell: &Cell, ctx: &str) -> Result<Option<i64>> {
    let json = cell_json(cell).map_err(|e| anyhow!("{ctx}: {e}"))?;
    let obj: &serde_json::Map<String, Value> = match &json {
        // Object form: {"Time": {...}} or {"Interval": {...}}
        Value::Object(map) => {
            let payload = map
                .values()
                .next()
                .ok_or_else(|| anyhow!("{ctx}: empty ScheduleAt object"))?;
            payload
                .as_object()
                .ok_or_else(|| anyhow!("{ctx}: ScheduleAt object payload not an object: {payload}"))?
        }
        // Array form: [tag, {...}] — kept for forward-compat
        Value::Array(arr) => {
            let payload = arr
                .get(1)
                .ok_or_else(|| anyhow!("{ctx}: ScheduleAt array missing payload"))?;
            payload
                .as_object()
                .ok_or_else(|| anyhow!("{ctx}: ScheduleAt array payload not an object: {payload}"))?
        }
        _ => bail!("{ctx}: expected ScheduleAt object or array, got {json}"),
    };
    // Presence of the micros key distinguishes the Time arm from Interval.
    if let Some(v) = obj.get("__timestamp_micros_since_unix_epoch__") {
        return Ok(Some(v.as_i64().ok_or_else(|| anyhow!("{ctx}: timestamp not i64"))?));
    }
    Ok(None)
}

/// Sum unit variants decode as `{"VariantName":{}}` → snake_case label.
fn sum_variant_snake(cell: &Cell, ctx: &str) -> Result<String> {
    let json = cell_json(cell)?;
    let Value::Object(obj) = json else {
        bail!("{ctx}: expected object, got {json}");
    };
    let key = obj.keys().next().ok_or_else(|| anyhow!("{ctx}: empty sum object"))?;
    Ok(pascal_to_snake(key))
}

fn pascal_to_snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

fn decode_experience_stacks(cell: &Cell) -> Result<Box<[(i32, i32)]>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("experience_stacks is not a JSON array: {json}");
    };
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        let Value::Object(obj) = entry else {
            bail!("experience stack is not an object: {entry}");
        };
        let skill_id = json_i32(obj.get("skill_id"), "stack.skill_id")?;
        let quantity = json_i32(obj.get("quantity"), "stack.quantity")?;
        out.push((skill_id, quantity));
    }
    Ok(out.into())
}

/// Read a `Cell::Bytea(8 bytes LE)` as `u64`. The relay-protocol decoder
/// intentionally maps U64 to Bytea to avoid NUMERIC; we recover the u64.
fn cell_u64(cell: &Cell, ctx: &str) -> Result<u64> {
    let bytes = match cell {
        Cell::Bytea(Some(b)) => b,
        Cell::Bytea(None) => bail!("{ctx}: Bytea is NULL"),
        _ => bail!("{ctx}: expected Bytea, got {cell:?}"),
    };
    if bytes.len() != 8 {
        bail!("{ctx}: expected 8-byte Bytea, got {} bytes", bytes.len());
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(arr))
}

fn cell_i32(cell: &Cell, ctx: &str) -> Result<i32> {
    match cell {
        Cell::Integer(Some(n)) => Ok(*n),
        _ => bail!("{ctx}: expected Integer, got {cell:?}"),
    }
}

/// U32 is mapped to `Cell::Bigint` by relay-protocol.
fn cell_u32(cell: &Cell, ctx: &str) -> Result<u32> {
    match cell {
        Cell::Bigint(Some(n)) => u32::try_from(*n).map_err(|_| anyhow!("{ctx}: Bigint {n} out of u32 range")),
        _ => bail!("{ctx}: expected Bigint, got {cell:?}"),
    }
}

fn cell_string(cell: &Cell, ctx: &str) -> Result<String> {
    match cell {
        Cell::Text(Some(s)) => Ok(s.clone()),
        _ => bail!("{ctx}: expected Text, got {cell:?}"),
    }
}

/// U8 is mapped to `Cell::Smallint` by relay-protocol.
fn cell_u8(cell: &Cell, ctx: &str) -> Result<u8> {
    match cell {
        Cell::Smallint(Some(n)) => u8::try_from(*n).map_err(|_| anyhow!("{ctx}: Smallint {n} out of u8 range")),
        Cell::Smallint(None) => bail!("{ctx}: Smallint is NULL"),
        other => bail!("{ctx}: expected Smallint, got {other:?}"),
    }
}

fn cell_bool(cell: &Cell, ctx: &str) -> Result<bool> {
    match cell {
        Cell::Bool(Some(b)) => Ok(*b),
        _ => bail!("{ctx}: expected Bool, got {cell:?}"),
    }
}

/// `Option<U64>` — relay-protocol unwraps options to a nullable cell
/// (`Cell::Bytea(Some(8 LE bytes))` / `Bytea(None)`); the wrapped
/// `{"some": …}` sum form is tolerated for tests / older dumps.
fn cell_opt_u64_sum(cell: &Cell, ctx: &str) -> Result<Option<u64>> {
    match cell {
        Cell::Bytea(None) | Cell::Jsonb(Value::Null) => Ok(None),
        Cell::Bytea(Some(b)) => {
            if b.len() != 8 {
                bail!("{ctx}: expected 8-byte Bytea, got {} bytes", b.len());
            }
            let mut arr = [0u8; 8];
            arr.copy_from_slice(b);
            Ok(Some(u64::from_le_bytes(arr)))
        }
        Cell::Jsonb(Value::Object(obj)) => {
            if let Some(v) = obj.get("some") {
                let s = v
                    .as_str()
                    .ok_or_else(|| anyhow!("{ctx}.some: expected hex string, got {v}"))?;
                let bytes = hex::decode(s).map_err(|e| anyhow!("{ctx}.some: hex decode: {e}"))?;
                if bytes.len() != 8 {
                    bail!("{ctx}.some: expected 8 bytes, got {}", bytes.len());
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes);
                return Ok(Some(u64::from_le_bytes(arr)));
            }
            if obj.contains_key("none") {
                return Ok(None);
            }
            bail!("{ctx}: unknown option shape {obj:?}")
        }
        other => bail!("{ctx}: expected optional u64, got {other:?}"),
    }
}

/// `Option<I32>` — nullable cell (`Cell::Integer(Some/None)`), with the
/// wrapped `{"some": n}` form tolerated.
fn cell_opt_i32_sum(cell: &Cell, ctx: &str) -> Result<Option<i32>> {
    match cell {
        Cell::Integer(None) | Cell::Jsonb(Value::Null) => Ok(None),
        Cell::Integer(Some(n)) => Ok(Some(*n)),
        Cell::Jsonb(Value::Object(obj)) => {
            if let Some(v) = obj.get("some") {
                let n = v.as_i64().ok_or_else(|| anyhow!("{ctx}.some: expected i64, got {v}"))?;
                return Ok(Some(i32::try_from(n).map_err(|_| anyhow!("{ctx}.some: i32 overflow"))?));
            }
            if obj.contains_key("none") {
                return Ok(None);
            }
            bail!("{ctx}: unknown option shape {obj:?}")
        }
        other => bail!("{ctx}: expected optional i32, got {other:?}"),
    }
}

/// Sum unit variants decode as `{"VariantName":{}}`; keep the upstream
/// PascalCase spelling (client gamedata keys on it).
fn sum_variant_pascal(cell: &Cell, ctx: &str) -> Result<String> {
    let json = cell_json(cell)?;
    let Value::Object(obj) = json else {
        bail!("{ctx}: expected object, got {json}");
    };
    let key = obj.keys().next().ok_or_else(|| anyhow!("{ctx}: empty sum object"))?;
    Ok(key.clone())
}

/// Identity (U256) raw little-endian bytes → canonical hex, matching how
/// SpacetimeDB prints identities elsewhere (`hex(c2007a…)`).
pub fn identity_hex(le_bytes: [u8; 32]) -> String {
    let mut be = le_bytes;
    be.reverse();
    hex::encode(be)
}

// ---------------------------------------------------------------------------
// Fixed-offset fast readers
//
// BSATN product rows are concatenated little-endian fields with no padding or
// tags, so a row whose leading fields are all fixed-width primitives can be
// read by offset instead of the generic [`bsatn::decode_row`] path (which
// allocates a `Vec<Cell>` per row). `location_state` (~13M rows per region
// seed) and `resource_state` qualify: every field is a fixed primitive, and
// the generic path is the hot cost of a seed. When the layout does not fit
// (schema drift, reordered columns, a variable-width field preceding the
// target), `try_from_fields` yields `None` and callers fall back to the
// generic decoder — the fast readers are an optimization, never a semantic
// change.
// ---------------------------------------------------------------------------

/// Fixed byte width of a BSATN primitive, or `None` for variable-width kinds.
fn fixed_width(ty: &MirroredType) -> Option<usize> {
    Some(match ty {
        MirroredType::Bool | MirroredType::U8 | MirroredType::I8 => 1,
        MirroredType::U16 | MirroredType::I16 => 2,
        MirroredType::U32 | MirroredType::I32 | MirroredType::F32 => 4,
        MirroredType::U64 | MirroredType::I64 | MirroredType::F64 => 8,
        MirroredType::U128 | MirroredType::I128 => 16,
        MirroredType::U256 | MirroredType::I256 => 32,
        _ => return None,
    })
}

/// Byte offset of `name` in a row whose preceding fields (and `name` itself)
/// are all fixed-width primitives; `None` otherwise.
fn fixed_field_offset(fields: &[MirroredField], schema: &MirroredSchema, name: &str) -> Option<usize> {
    let mut offset = 0usize;
    for field in fields {
        let width = fixed_width(schema.resolve(&field.ty))?;
        if field.name.as_deref() == Some(name) {
            return Some(offset);
        }
        offset += width;
    }
    None
}

fn read_u64(row: &[u8], off: usize) -> Option<u64> {
    let b: [u8; 8] = row.get(off..off + 8)?.try_into().ok()?;
    Some(u64::from_le_bytes(b))
}

fn read_i32(row: &[u8], off: usize) -> Option<i32> {
    let b: [u8; 4] = row.get(off..off + 4)?.try_into().ok()?;
    Some(i32::from_le_bytes(b))
}

fn read_u32(row: &[u8], off: usize) -> Option<u32> {
    read_i32(row, off).map(|v| v as u32)
}

/// Fixed-offset reader for `location_state` rows.
#[derive(Debug, Clone, Copy)]
pub struct LocationFast {
    entity_id: usize,
    x: usize,
    z: usize,
    dimension: usize,
}

impl LocationFast {
    pub fn try_from_fields(fields: &[MirroredField], schema: &MirroredSchema) -> Option<Self> {
        Some(Self {
            entity_id: fixed_field_offset(fields, schema, "entity_id")?,
            x: fixed_field_offset(fields, schema, "x")?,
            z: fixed_field_offset(fields, schema, "z")?,
            dimension: fixed_field_offset(fields, schema, "dimension")?,
        })
    }

    #[inline]
    pub fn decode(&self, row: &[u8]) -> Option<LocationRow> {
        Some(LocationRow {
            entity_id: read_u64(row, self.entity_id)?,
            x: read_i32(row, self.x)?,
            z: read_i32(row, self.z)?,
            dimension: read_u32(row, self.dimension)?,
        })
    }
}

/// Fixed-offset reader for `resource_state` rows.
#[derive(Debug, Clone, Copy)]
pub struct ResourceFast {
    entity_id: usize,
    resource_id: usize,
    direction_index: usize,
}

impl ResourceFast {
    pub fn try_from_fields(fields: &[MirroredField], schema: &MirroredSchema) -> Option<Self> {
        Some(Self {
            entity_id: fixed_field_offset(fields, schema, "entity_id")?,
            resource_id: fixed_field_offset(fields, schema, "resource_id")?,
            direction_index: fixed_field_offset(fields, schema, "direction_index")?,
        })
    }

    #[inline]
    pub fn decode(&self, row: &[u8]) -> Option<ResourceRow> {
        Some(ResourceRow {
            entity_id: read_u64(row, self.entity_id)?,
            resource_id: read_i32(row, self.resource_id)?,
            direction_index: read_i32(row, self.direction_index)?,
        })
    }
}

/// Fixed-offset reader for `extraction_recipe_desc` identity fields —
/// `id` and `resource_id` are the row's two leading I32s, so both offsets
/// are fixed. Yields/requirements after them are never decoded.
#[derive(Debug, Clone, Copy)]
pub struct ExtractionRecipeFast {
    id: usize,
    resource_id: usize,
}

impl ExtractionRecipeFast {
    pub fn try_from_fields(fields: &[MirroredField], schema: &MirroredSchema) -> Option<Self> {
        Some(Self {
            id: fixed_field_offset(fields, schema, "id")?,
            resource_id: fixed_field_offset(fields, schema, "resource_id")?,
        })
    }

    #[inline]
    pub fn decode(&self, row: &[u8]) -> Option<ExtractionRecipeRow> {
        Some(ExtractionRecipeRow {
            id: read_i32(row, self.id)?,
            resource_id: read_i32(row, self.resource_id)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn any_sentinel_is_not_a_player_skill() {
        assert!(!is_player_skill_id(SKILL_ID_ANY));
        assert!(is_player_skill_id(2)); // Forestry
        assert!(is_player_skill_id(22)); // Hexite Gathering
    }

    #[test]
    fn decode_optional_location_accepts_bare_and_wrapped() {
        let bare = Cell::Jsonb(json!({"x": 24521, "z": 18473, "dimension": 1}));
        assert_eq!(decode_optional_location(&bare).unwrap(), (true, 24521, 18473, 1));
        let wrapped = Cell::Jsonb(json!({"some": {"x": 1, "z": 2, "dimension": 1}}));
        assert_eq!(decode_optional_location(&wrapped).unwrap(), (true, 1, 2, 1));
        assert_eq!(
            decode_optional_location(&Cell::Jsonb(Value::Null)).unwrap(),
            (false, 0, 0, 0)
        );
    }

    #[test]
    fn decode_pockets_walks_mixed_array() {
        // Matches the shape produced by relay_protocol::bsatn::decode_json
        // for `Array<Pocket>` where Pocket has `contents: Option<...>`:
        // each option renders as `{"some": payload}` or `{"none": {}}`,
        // and the Item/Cargo sum renders as `{"Item": {}}` / `{"Cargo": {}}`.
        let cell = Cell::Jsonb(json!([
            {"volume": 100,  "contents": {"some": {"item_id": 1020003, "quantity": 50, "item_type": {"Item": {}},  "durability": {"some": 100}}}, "locked": false},
            {"volume": 0,    "contents": {"none": {}},                                                                                              "locked": false},
            {"volume": 6000, "contents": {"some": {"item_id": 5001,    "quantity": 1,  "item_type": {"Cargo": {}}, "durability": {"none": {}}}},  "locked": false}
        ]));
        let pockets = decode_pockets(&cell).unwrap();
        assert_eq!(pockets.len(), 3);
        assert!(pockets[0].has_contents);
        assert_eq!(pockets[0].item_id, 1020003);
        assert_eq!(pockets[0].quantity, 50);
        assert_eq!(pockets[0].item_type, Pocket::ITEM);
        assert!(pockets[0].has_durability);
        assert_eq!(pockets[0].durability, 100);

        assert!(!pockets[1].has_contents);

        assert!(pockets[2].has_contents);
        assert_eq!(pockets[2].item_id, 5001);
        assert_eq!(pockets[2].item_type, Pocket::CARGO);
        assert!(!pockets[2].has_durability);
    }

    #[test]
    fn decode_pockets_handles_empty_array() {
        let cell = Cell::Jsonb(json!([]));
        let pockets = decode_pockets(&cell).unwrap();
        assert!(pockets.is_empty());
    }

    #[test]
    fn decode_pockets_rejects_non_array() {
        let cell = Cell::Jsonb(json!({"not": "an array"}));
        assert!(decode_pockets(&cell).is_err());
    }

    #[test]
    fn decode_pockets_rejects_unknown_item_type() {
        let cell = Cell::Jsonb(json!([
            {"volume": 1, "contents": {"some": {"item_id": 1, "quantity": 1, "item_type": {"Quest": {}}, "durability": {"none": {}}}}, "locked": false}
        ]));
        let err = decode_pockets(&cell).unwrap_err();
        assert!(err.to_string().contains("item_type"));
    }

    #[test]
    fn decode_crafted_item_stacks_walks_item_and_cargo() {
        let cell = Cell::Jsonb(json!([
            {"item_id": 11006, "quantity": 10, "item_type": {"Item": {}}, "durability": {"none": {}}},
            {"item_id": 5001, "quantity": 1, "item_type": {"Cargo": {}}, "durability": {"some": 0}}
        ]));
        let stacks = decode_crafted_item_stacks(&cell).unwrap();
        assert_eq!(stacks.len(), 2);
        assert_eq!(stacks[0].item_id, 11006);
        assert_eq!(stacks[0].quantity, 10);
        assert_eq!(stacks[0].item_type, Pocket::ITEM);
        assert_eq!(stacks[1].item_id, 5001);
        assert_eq!(stacks[1].item_type, Pocket::CARGO);
    }

    #[test]
    fn functions_is_storage_detects_slots() {
        let storage = Cell::Jsonb(json!([
            {"function_type": 3, "storage_slots": 18, "cargo_slots": 0}
        ]));
        assert!(functions_is_storage(&storage).unwrap());

        let cargo = Cell::Jsonb(json!([
            {"function_type": 4, "storage_slots": 0, "cargo_slots": 8}
        ]));
        assert!(functions_is_storage(&cargo).unwrap());

        let totem = Cell::Jsonb(json!([
            {"function_type": 28, "storage_slots": 0, "cargo_slots": 0}
        ]));
        assert!(!functions_is_storage(&totem).unwrap());

        let empty = Cell::Jsonb(json!([]));
        assert!(!functions_is_storage(&empty).unwrap());
    }

    #[test]
    fn deployable_kind_recognizes_boat() {
        assert_eq!(
            deployable_kind_from_cell(&Cell::Jsonb(json!({"Boat": {}}))).unwrap(),
            DeployableKind::Boat
        );
        assert_eq!(
            deployable_kind_from_cell(&Cell::Jsonb(json!({"Mount": {}}))).unwrap(),
            DeployableKind::Mount
        );
        assert_eq!(
            deployable_kind_from_cell(&Cell::Jsonb(json!({"Stall": {}}))).unwrap(),
            DeployableKind::Other
        );
    }
}
