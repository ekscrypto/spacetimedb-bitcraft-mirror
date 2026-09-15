// SPDX-License-Identifier: MIT

//! Per-region columnar store. One `RegionStore` lives behind each shard's
//! `Arc<RwLock<RegionStore>>`. Read paths acquire a read lock, project to
//! response DTOs, and release; the shard's WS task holds the write lock
//! briefly to apply each `TransactionUpdate`.

pub mod active_buff;
pub mod building;
pub mod building_desc;
pub mod building_nickname;
pub mod character_stat_desc;
pub mod character_stats;
pub mod claim;
pub mod claim_local;
pub mod claim_member;
pub mod claim_tech;
pub mod claim_tile_cost;
pub mod crafting_recipe_desc;
pub mod deployable;
pub mod dimension_network;
pub mod experience;
pub mod extraction_recipe;
pub mod growth;
pub mod hexite;
pub mod inventory;
pub mod location_dim;
pub mod mobile_entity;
pub mod passive_craft;
pub mod player_action;
pub mod player_housing;
pub mod player_state;
pub mod player_username;
pub mod progressive_action;
pub mod public_progressive_action;
pub mod rent;
pub mod resource;
pub mod resource_desc;
pub mod resource_growth_timer;
pub mod skill_desc;
pub mod stamina;
pub mod storage_log;

pub use active_buff::ActiveBuffSoA;
pub use building::BuildingSoA;
pub use building_desc::BuildingDescStore;
pub use building_nickname::BuildingNicknameStore;
pub use character_stat_desc::CharacterStatDescStore;
pub use character_stats::CharacterStatsSoA;
pub use claim::ClaimSoA;
pub use claim_local::ClaimLocalSoA;
pub use claim_member::ClaimMemberSoA;
pub use claim_tech::{ClaimTechDescStore, ClaimTechStateStore};
pub use claim_tile_cost::ClaimTileCostStore;
pub use crafting_recipe_desc::CraftingRecipeDescStore;
pub use deployable::{DeployableDescStore, DeployableSoA};
pub use dimension_network::DimensionNetworkStore;
pub use experience::ExperienceSoA;
pub use extraction_recipe::ExtractionRecipeStore;
pub use growth::GrowthStore;
pub use hexite::HexiteIndex;
pub use inventory::{InventorySoA, Pocket};
pub use location_dim::LocationDimStore;
pub use mobile_entity::MobileEntitySoA;
pub use passive_craft::PassiveCraftSoA;
pub use player_action::PlayerActionSoA;
pub use player_housing::{PlayerHousingDescStore, PlayerHousingSoA};
pub use player_state::PlayerStateSoA;
pub use player_username::PlayerUsernameSoA;
pub use progressive_action::ProgressiveActionSoA;
pub use public_progressive_action::PublicProgressiveActionStore;
pub use rent::RentSoA;
pub use resource::ResourceSoA;
pub use resource_desc::ResourceDescStore;
pub use resource_growth_timer::ResourceGrowthTimerStore;
pub use skill_desc::SkillDescStore;
pub use stamina::StaminaSoA;
pub use storage_log::StorageLogSoA;

/// One region's worth of in-memory state. The `ready` flag is `false`
/// during initial `SubscribeApplied` load and after a disconnect; the
/// HTTP layer treats not-ready shards as contributing nothing to fan-out
/// queries (and reports them in `/cache-health`).
pub struct RegionStore {
    pub region: u32,
    pub ready: bool,
    pub claim: ClaimSoA,
    pub claim_local: ClaimLocalSoA,
    pub claim_member: ClaimMemberSoA,
    pub claim_tech_state: ClaimTechStateStore,
    pub claim_tech_desc: ClaimTechDescStore,
    pub claim_tile_cost: ClaimTileCostStore,
    pub building: BuildingSoA,
    pub inventory: InventorySoA,
    pub building_desc: BuildingDescStore,
    pub building_nickname: BuildingNicknameStore,
    pub location_dim: LocationDimStore,
    pub dimension_network: DimensionNetworkStore,
    pub player_username: PlayerUsernameSoA,
    pub player_state: PlayerStateSoA,
    pub mobile_entity: MobileEntitySoA,
    pub deployable: DeployableSoA,
    pub deployable_desc: DeployableDescStore,
    pub player_housing: PlayerHousingSoA,
    pub player_housing_desc: PlayerHousingDescStore,
    pub rent: RentSoA,
    pub experience: ExperienceSoA,
    pub skill_desc: SkillDescStore,
    pub progressive_action: ProgressiveActionSoA,
    pub public_progressive_action: PublicProgressiveActionStore,
    pub passive_craft: PassiveCraftSoA,
    pub crafting_recipe_desc: CraftingRecipeDescStore,
    pub resource: ResourceSoA,
    pub growth: GrowthStore,
    pub growth_timer: ResourceGrowthTimerStore,
    pub storage_log: StorageLogSoA,
    /// Bit-Me session feed stores (one-row-per-entity tables; bounded and
    /// small — see `crates/relay-cache/USED-TABLES.md`).
    pub stamina: StaminaSoA,
    pub active_buff: ActiveBuffSoA,
    pub player_action: PlayerActionSoA,
    pub character_stats: CharacterStatsSoA,
    pub character_stat_desc: CharacterStatDescStore,
    pub resource_desc: ResourceDescStore,
    pub extraction_recipe: ExtractionRecipeStore,
    /// Hexite-deposit claim world coords (see `hexite.rs`) — lets
    /// `location_state` rows that stream before `resource_state` (table
    /// -alphabetical seed order in the embedded feed) attach to their
    /// resource at upsert time.
    pub hexite: HexiteIndex,
}

impl RegionStore {
    pub fn empty(region: u32) -> Self {
        Self {
            region,
            ready: false,
            claim: ClaimSoA::with_capacity(0),
            claim_local: ClaimLocalSoA::with_capacity(0),
            claim_member: ClaimMemberSoA::with_capacity(0),
            claim_tech_state: ClaimTechStateStore::new(),
            claim_tech_desc: ClaimTechDescStore::new(),
            claim_tile_cost: ClaimTileCostStore::new(),
            building: BuildingSoA::with_capacity(0),
            inventory: InventorySoA::with_capacity(0),
            building_desc: BuildingDescStore::new(),
            building_nickname: BuildingNicknameStore::new(),
            location_dim: LocationDimStore::new(),
            dimension_network: DimensionNetworkStore::new(),
            player_username: PlayerUsernameSoA::with_capacity(0),
            player_state: PlayerStateSoA::with_capacity(0),
            mobile_entity: MobileEntitySoA::with_capacity(0),
            deployable: DeployableSoA::with_capacity(0),
            deployable_desc: DeployableDescStore::new(),
            player_housing: PlayerHousingSoA::with_capacity(0),
            player_housing_desc: PlayerHousingDescStore::new(),
            rent: RentSoA::with_capacity(0),
            experience: ExperienceSoA::with_capacity(0),
            skill_desc: SkillDescStore::new(),
            progressive_action: ProgressiveActionSoA::with_capacity(0),
            public_progressive_action: PublicProgressiveActionStore::new(),
            passive_craft: PassiveCraftSoA::with_capacity(0),
            crafting_recipe_desc: CraftingRecipeDescStore::new(),
            resource: ResourceSoA::with_capacity(0),
            growth: GrowthStore::new(),
            growth_timer: ResourceGrowthTimerStore::new(),
            storage_log: StorageLogSoA::with_capacity(0),
            stamina: StaminaSoA::with_capacity(0),
            active_buff: ActiveBuffSoA::with_capacity(0),
            player_action: PlayerActionSoA::with_capacity(0),
            character_stats: CharacterStatsSoA::with_capacity(0),
            character_stat_desc: CharacterStatDescStore::new(),
            resource_desc: ResourceDescStore::new(),
            extraction_recipe: ExtractionRecipeStore::new(),
            hexite: HexiteIndex::default(),
        }
    }
}
