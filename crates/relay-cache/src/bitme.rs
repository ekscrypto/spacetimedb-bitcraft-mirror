// SPDX-License-Identifier: MIT

//! Bit-Me mobile-app support: claim-scoped session tracking plus the
//! global name → identity → region resolve chain.
//!
//! Design (see `BITME-DATA-ASSESSMENT.md` in the workspace root): the phone
//! never opens a SpacetimeDB socket. Two tiny JSON endpoints
//! (`/bitme/resolve`, `/bitme/session/:entity_id`) are served from memory;
//! a `GET` on the session route *is* the registration (TTL refresh), so the
//! client stays stateless HTTP.
//!
//! Feed cost is O(commit-size):
//!
//! - `player_action_state` / `stamina_state` / `active_buff_state` /
//!   `character_stats_state` / gamedata desc tables are plain one-row-per-
//!   entity stores on [`crate::store::RegionStore`] (all entities — the
//!   tables are bounded).
//! - `resource_health_state` is NOT stored wholesale (hundreds of thousands
//!   of rows per region). [`BitmeHub`] retains values only for *tracked
//!   action targets*, registered when a session GET sees an action target.
//! - live `resource_state` inserts whose resource id is a watched spawn
//!   (destroy-yield chain targets + the vendored citric bushes) are appended
//!   to a per-region bounded spawn log — the citric-detection signal.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};
use relay_protocol::{MirroredField, MirroredSchema};

use crate::decode::{
    self, BitmeGlobalCols, PlayerStateCols, ResourceCols, ResourceFast, PLAYER_LOWERCASE_USERNAME_TABLE,
    PLAYER_STATE_TABLE, REGION_CONNECTION_INFO_TABLE, RESOURCE_HEALTH_TABLE, RESOURCE_TABLE, SIGNED_IN_PLAYER_TABLE,
    USER_REGION_STATE_TABLE, USER_STATE_TABLE, WORLD_REGION_NAME_TABLE,
};

/// Session re-registration window. Bit-Me polls at ~1 Hz; anything quieter
/// than this is treated as a closed session and its targets are dropped.
const SESSION_TTL: Duration = Duration::from_secs(15 * 60);
/// Hard cap on concurrent sessions (abuse backstop; each is ~50 bytes).
const MAX_SESSIONS: usize = 8192;
/// Hard cap on tracked action targets fleet-wide.
const MAX_TARGETS: usize = 8192;
/// Targets unused for this long are dropped even if sessions persist.
const TARGET_TTL: Duration = Duration::from_secs(30 * 60);
/// Spawn-log entries expire after this long even if never deleted upstream.
const SPAWN_TTL: Duration = Duration::from_secs(30 * 60);
/// Max retained spawn entries per region.
const SPAWN_CAP: usize = 512;

#[derive(Debug, Clone)]
struct SessionEntry {
    last_seen: Instant,
}

#[derive(Debug, Clone)]
struct TargetHealth {
    health: Option<i32>,
    last_update: Instant,
}

/// A live watched-resource spawn (citric bush, growth-chain respawn, …).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnEntry {
    pub entity_id: u64,
    pub resource_id: i32,
    pub at_unix_ms: i64,
}

/// Cached field slices + column indices for the tables [`BitmeHub`] decodes
/// on the region live path. Resolved once per region feed registration.
pub struct BitmeRegionMeta {
    pub resource_health_cols: crate::decode::ResourceHealthCols,
    pub resource_health_fields: Vec<MirroredField>,
    pub resource_cols: ResourceCols,
    pub resource_fast: Option<ResourceFast>,
    pub resource_fields: Vec<MirroredField>,
}

impl BitmeRegionMeta {
    pub fn from_schema(schema: &MirroredSchema) -> Result<Self, anyhow::Error> {
        let resource_health_fields = fields_of(schema, RESOURCE_HEALTH_TABLE)?;
        let resource_fields = fields_of(schema, RESOURCE_TABLE)?;
        Ok(Self {
            resource_health_cols: decode::resolve_resource_health_cols(schema)?,
            resource_health_fields,
            resource_cols: decode::resolve_resource_cols(schema)?,
            resource_fast: ResourceFast::try_from_fields(&resource_fields, schema),
            resource_fields,
        })
    }
}

/// Field slices for the global resolve tables. Everything is optional: the
/// global schema lacking a table degrades that lookup arm, it never fails
/// the feed (mirrors the roads `RoadsTableMeta::from_schema_global` stance).
pub struct BitmeGlobalMeta {
    pub cols: BitmeGlobalCols,
    pub lowercase_fields: Option<Vec<MirroredField>>,
    pub user_fields: Option<Vec<MirroredField>>,
    pub user_region_fields: Option<Vec<MirroredField>>,
    pub conn_fields: Option<Vec<MirroredField>>,
    pub world_name_fields: Option<Vec<MirroredField>>,
    pub player_state_fields: Option<Vec<MirroredField>>,
    pub player_state_cols: Option<PlayerStateCols>,
    pub signed_in_fields: Option<Vec<MirroredField>>,
}

impl BitmeGlobalMeta {
    pub fn from_schema_global(schema: &MirroredSchema) -> Self {
        fn fields(schema: &MirroredSchema, table: &str) -> Option<Vec<MirroredField>> {
            let tbl = schema.tables.iter().find(|t| t.name == table)?;
            schema.table_product(tbl).map(|f| f.to_vec())
        }
        let cols = decode::resolve_bitme_global_cols(schema).unwrap_or_default();
        Self {
            lowercase_fields: fields(schema, PLAYER_LOWERCASE_USERNAME_TABLE),
            user_fields: fields(schema, USER_STATE_TABLE),
            user_region_fields: fields(schema, USER_REGION_STATE_TABLE),
            conn_fields: fields(schema, REGION_CONNECTION_INFO_TABLE),
            world_name_fields: fields(schema, WORLD_REGION_NAME_TABLE),
            player_state_fields: fields(schema, PLAYER_STATE_TABLE),
            player_state_cols: decode::resolve_player_state_cols(schema).ok(),
            signed_in_fields: fields(schema, SIGNED_IN_PLAYER_TABLE),
            cols,
        }
    }
}

/// Global name → identity → region resolve chain, built from
/// `player_lowercase_username_state` ⋈ `user_state` ⋈ `user_region_state`
/// ⋈ `region_connection_info` (+ `world_region_name_state`, global
/// `player_state.signed_in`). Exact-match index lookups only.
#[derive(Debug, Default)]
pub struct BitmeGlobalStore {
    ready: bool,
    entity_by_lowercase: HashMap<String, u64>,
    lowercase_by_entity: HashMap<u64, String>,
    identity_by_entity: HashMap<u64, [u8; 32]>,
    entity_by_identity: HashMap<[u8; 32], u64>,
    region_by_identity: HashMap<[u8; 32], u8>,
    conn_by_region: HashMap<u8, (String, String)>,
    name_by_region: HashMap<u16, String>,
    signed_in_by_entity: HashMap<u64, bool>,
}

impl BitmeGlobalStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mark_ready(&mut self) {
        self.ready = true;
    }

    pub fn ready(&self) -> bool {
        self.ready
    }

    pub fn clear(&mut self) {
        *self = Self::new();
    }

    /// Apply one global insert; unknown tables are a no-op. Decode failures
    /// are returned so the caller can log them like the roads catalog does.
    pub fn apply_insert(
        &mut self,
        meta: &BitmeGlobalMeta,
        schema: &MirroredSchema,
        table: &str,
        row: &[u8],
    ) -> Result<(), anyhow::Error> {
        match table {
            PLAYER_LOWERCASE_USERNAME_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.lowercase_fields, meta.cols.lowercase_username) else {
                    return Ok(());
                };
                let r = decode::decode_bitme_lowercase_username(row, fields, cols, schema)?;
                self.entity_by_lowercase
                    .insert(r.username_lowercase.clone(), r.entity_id);
                self.lowercase_by_entity.insert(r.entity_id, r.username_lowercase);
            }
            USER_STATE_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.user_fields, meta.cols.user_state) else {
                    return Ok(());
                };
                let r = decode::decode_bitme_user(row, fields, cols, schema)?;
                self.identity_by_entity.insert(r.entity_id, r.identity);
                self.entity_by_identity.insert(r.identity, r.entity_id);
            }
            USER_REGION_STATE_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.user_region_fields, meta.cols.user_region) else {
                    return Ok(());
                };
                let r = decode::decode_bitme_user_region(row, fields, cols, schema)?;
                self.region_by_identity.insert(r.identity, r.region_id);
            }
            REGION_CONNECTION_INFO_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.conn_fields, meta.cols.region_connection) else {
                    return Ok(());
                };
                let r = decode::decode_bitme_region_connection(row, fields, cols, schema)?;
                self.conn_by_region.insert(r.id, (r.host, r.module));
            }
            WORLD_REGION_NAME_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.world_name_fields, meta.cols.world_region_name) else {
                    return Ok(());
                };
                let r = decode::decode_bitme_world_region_name(row, fields, cols, schema)?;
                self.name_by_region.insert(r.id, r.player_facing_name);
            }
            PLAYER_STATE_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.player_state_fields, meta.player_state_cols) else {
                    return Ok(());
                };
                let r = decode::decode_player_state_with_fields(row, fields, cols, schema)?;
                self.signed_in_by_entity.insert(r.entity_id, r.signed_in);
            }
            SIGNED_IN_PLAYER_TABLE => {
                let (Some(fields), Some(col)) = (&meta.signed_in_fields, meta.cols.signed_in_player) else {
                    return Ok(());
                };
                let entity_id = decode::decode_bitme_signed_in_player(row, fields, col, schema)?;
                self.signed_in_by_entity.insert(entity_id, true);
            }
            _ => {}
        }
        Ok(())
    }

    pub fn apply_delete(
        &mut self,
        meta: &BitmeGlobalMeta,
        schema: &MirroredSchema,
        table: &str,
        row: &[u8],
    ) -> Result<(), anyhow::Error> {
        match table {
            PLAYER_LOWERCASE_USERNAME_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.lowercase_fields, meta.cols.lowercase_username) else {
                    return Ok(());
                };
                let r = decode::decode_bitme_lowercase_username(row, fields, cols, schema)?;
                self.entity_by_lowercase.remove(&r.username_lowercase);
                self.lowercase_by_entity.remove(&r.entity_id);
            }
            USER_STATE_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.user_fields, meta.cols.user_state) else {
                    return Ok(());
                };
                let r = decode::decode_bitme_user(row, fields, cols, schema)?;
                self.identity_by_entity.remove(&r.entity_id);
                self.entity_by_identity.remove(&r.identity);
            }
            USER_REGION_STATE_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.user_region_fields, meta.cols.user_region) else {
                    return Ok(());
                };
                let r = decode::decode_bitme_user_region(row, fields, cols, schema)?;
                self.region_by_identity.remove(&r.identity);
            }
            REGION_CONNECTION_INFO_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.conn_fields, meta.cols.region_connection) else {
                    return Ok(());
                };
                let r = decode::decode_bitme_region_connection(row, fields, cols, schema)?;
                self.conn_by_region.remove(&r.id);
            }
            WORLD_REGION_NAME_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.world_name_fields, meta.cols.world_region_name) else {
                    return Ok(());
                };
                let r = decode::decode_bitme_world_region_name(row, fields, cols, schema)?;
                self.name_by_region.remove(&r.id);
            }
            PLAYER_STATE_TABLE => {
                let (Some(fields), Some(cols)) = (&meta.player_state_fields, meta.player_state_cols) else {
                    return Ok(());
                };
                let r = decode::decode_player_state_with_fields(row, fields, cols, schema)?;
                self.signed_in_by_entity.remove(&r.entity_id);
            }
            SIGNED_IN_PLAYER_TABLE => {
                let (Some(fields), Some(col)) = (&meta.signed_in_fields, meta.cols.signed_in_player) else {
                    return Ok(());
                };
                let entity_id = decode::decode_bitme_signed_in_player(row, fields, col, schema)?;
                self.signed_in_by_entity.remove(&entity_id);
            }
            _ => {}
        }
        Ok(())
    }

    /// Connection info + player-facing name for one region id. Used both by
    /// the resolve chain and by the handler fallback that derives the region
    /// from the region shards when `user_region_state` lacks the identity.
    pub fn region_info(&self, region_id: u32) -> (Option<String>, Option<String>, Option<String>) {
        let (host, module) = self
            .conn_by_region
            .get(&(region_id as u8))
            .cloned()
            .map(|(h, m)| (Some(h), Some(m)))
            .unwrap_or((None, None));
        let name = self.name_by_region.get(&(region_id as u16)).cloned();
        (name, host, module)
    }

    /// Exact-match lowercase resolve. Every field is an independent index
    /// hit; missing chain links simply stay `None`.
    pub fn resolve(&self, name_lowercase: &str) -> Option<ResolvedPlayer> {
        let entity_id = *self.entity_by_lowercase.get(name_lowercase)?;
        let identity = self.identity_by_entity.get(&entity_id).copied();
        let region_id = identity
            .as_ref()
            .and_then(|id| self.region_by_identity.get(id).copied());
        let (host, module) = region_id
            .and_then(|r| self.conn_by_region.get(&r).cloned())
            .unwrap_or_default();
        let region_name = region_id.and_then(|r| self.name_by_region.get(&(r as u16)).cloned());
        Some(ResolvedPlayer {
            entity_id,
            username_lowercase: self
                .lowercase_by_entity
                .get(&entity_id)
                .cloned()
                .unwrap_or_else(|| name_lowercase.to_owned()),
            identity_hex: identity.map(decode::identity_hex),
            region_id: region_id.map(u32::from),
            region_name,
            host,
            module,
            signed_in: self.signed_in_by_entity.get(&entity_id).copied(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedPlayer {
    pub entity_id: u64,
    pub username_lowercase: String,
    pub identity_hex: Option<String>,
    pub region_id: Option<u32>,
    pub region_name: Option<String>,
    pub host: String,
    pub module: String,
    pub signed_in: Option<bool>,
}

/// Shared Bit-Me state: the global resolve store, poll-registered sessions,
/// tracked action-target health, and per-region live spawn logs.
#[derive(Debug, Default)]
pub struct BitmeHub {
    global: RwLock<BitmeGlobalStore>,
    sessions: Mutex<HashMap<u64, SessionEntry>>,
    targets: Mutex<HashMap<u64, TargetHealth>>,
    spawns: Mutex<HashMap<u32, Vec<SpawnEntry>>>,
}

impl BitmeHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            global: RwLock::new(BitmeGlobalStore::new()),
            sessions: Mutex::new(HashMap::new()),
            targets: Mutex::new(HashMap::new()),
            spawns: Mutex::new(HashMap::new()),
        })
    }

    pub fn global(&self) -> &RwLock<BitmeGlobalStore> {
        &self.global
    }

    // -- sessions ------------------------------------------------------------

    /// Register/refresh a session (the GET itself is the registration).
    pub fn touch_session(&self, player_entity_id: u64) {
        let mut sessions = self.sessions.lock();
        let now = Instant::now();
        if sessions.len() >= MAX_SESSIONS && !sessions.contains_key(&player_entity_id) {
            self.sweep_sessions(&mut sessions, now);
        }
        if sessions.len() >= MAX_SESSIONS {
            // Still full after the TTL sweep (hot abuse): evict the oldest.
            if let Some(oldest) = sessions.iter().min_by_key(|(_, s)| s.last_seen).map(|(&k, _)| k) {
                sessions.remove(&oldest);
            }
        }
        sessions.insert(player_entity_id, SessionEntry { last_seen: now });
    }

    fn sweep_sessions(&self, sessions: &mut HashMap<u64, SessionEntry>, now: Instant) {
        let before = sessions.len();
        sessions.retain(|_, s| now.duration_since(s.last_seen) < SESSION_TTL);
        if sessions.len() != before {
            tracing::debug!(target: "relay_cache::bitme", dropped = before - sessions.len(), "sessions expired");
        }
        // Opportunistic target GC alongside its sessions.
        let mut targets = self.targets.lock();
        targets.retain(|_, t| now.duration_since(t.last_update) < TARGET_TTL);
    }

    /// Track a session's action targets so `resource_health_state` updates
    /// for them are retained. Called by the session handler (each poll) and
    /// whenever a target is (re)observed.
    pub fn note_targets<I: IntoIterator<Item = u64>>(&self, targets: I) {
        let mut map = self.targets.lock();
        let now = Instant::now();
        for t in targets {
            if t == 0 {
                continue;
            }
            if map.len() >= MAX_TARGETS && !map.contains_key(&t) {
                return; // abuse backstop; existing targets keep their slots
            }
            map.entry(t).or_insert_with(|| TargetHealth {
                health: None,
                last_update: now,
            });
        }
    }

    /// Latest tracked health for a target, when known.
    pub fn target_health(&self, target: u64) -> Option<i32> {
        self.targets.lock().get(&target).and_then(|t| t.health)
    }

    // -- spawns ----------------------------------------------------------------

    /// Live watched spawns for a region, oldest first, TTL-filtered.
    pub fn spawns(&self, region: u32) -> Vec<SpawnEntry> {
        let cutoff = now_unix_ms() - SPAWN_TTL.as_millis() as i64;
        let spawns = self.spawns.lock();
        spawns
            .get(&region)
            .map(|v| v.iter().filter(|s| s.at_unix_ms >= cutoff).cloned().collect())
            .unwrap_or_default()
    }

    /// Drop everything a region's feed has logged (feed reset / reseed).
    pub fn clear_region(&self, region: u32) {
        self.spawns.lock().remove(&region);
    }

    // -- feed hooks --------------------------------------------------------------

    /// Consume one live (post-seed) region update. `store` is the caller's
    /// read-locked region store — used for the watched-spawn check.
    pub fn observe_live(
        &self,
        region: u32,
        update: &spacetimedb_public_mirror_client::upstream::UpstreamUpdate,
        meta: &BitmeRegionMeta,
        schema: &MirroredSchema,
        store: &crate::store::RegionStore,
    ) {
        for table in &update.tables {
            match table.table_name.as_ref() {
                RESOURCE_HEALTH_TABLE => {
                    for row in &table.delete_bytes {
                        let Ok(r) = decode::decode_resource_health_with_fields(
                            row,
                            &meta.resource_health_fields,
                            meta.resource_health_cols,
                            schema,
                        ) else {
                            continue;
                        };
                        let mut targets = self.targets.lock();
                        if let Some(t) = targets.get_mut(&r.entity_id) {
                            t.health = None;
                            t.last_update = Instant::now();
                        }
                    }
                    for row in &table.inserts {
                        let Ok(r) = decode::decode_resource_health_with_fields(
                            row,
                            &meta.resource_health_fields,
                            meta.resource_health_cols,
                            schema,
                        ) else {
                            continue;
                        };
                        let mut targets = self.targets.lock();
                        if let Some(t) = targets.get_mut(&r.entity_id) {
                            t.health = Some(r.health);
                            t.last_update = Instant::now();
                        }
                    }
                }
                RESOURCE_TABLE => {
                    let mut spawns = self.spawns.lock();
                    let log = spawns.entry(region).or_default();
                    for row in &table.delete_bytes {
                        let Ok(r) = decode_resource(meta, schema, row) else {
                            continue;
                        };
                        log.retain(|s| s.entity_id != r.entity_id);
                        let mut targets = self.targets.lock();
                        if let Some(t) = targets.get_mut(&r.entity_id) {
                            t.health = None;
                            t.last_update = Instant::now();
                        }
                    }
                    for row in &table.inserts {
                        let Ok(r) = decode_resource(meta, schema, row) else {
                            continue;
                        };
                        // Watched check needs the store's gamedata (yield
                        // chains + vendored citric ids).
                        if !store.resource_desc.is_watched_spawn(r.resource_id) {
                            continue;
                        }
                        let now = now_unix_ms();
                        let cutoff = now - SPAWN_TTL.as_millis() as i64;
                        log.retain(|s| s.at_unix_ms >= cutoff);
                        if log.len() >= SPAWN_CAP {
                            log.remove(0);
                        }
                        log.push(SpawnEntry {
                            entity_id: r.entity_id,
                            resource_id: r.resource_id,
                            at_unix_ms: now,
                        });
                    }
                }
                _ => {}
            }
        }
    }

    /// Apply one global-module row to the resolve chain.
    pub fn apply_global(
        &self,
        meta: &BitmeGlobalMeta,
        schema: &MirroredSchema,
        table: &str,
        insert: Option<&[u8]>,
        delete: Option<&[u8]>,
    ) {
        let mut store = self.global.write();
        if let Some(row) = delete {
            if let Err(e) = store.apply_delete(meta, schema, table, row) {
                tracing::debug!(target: "relay_cache::bitme", table, error = %e, "global delete decode failed");
            }
        }
        if let Some(row) = insert {
            if let Err(e) = store.apply_insert(meta, schema, table, row) {
                tracing::debug!(target: "relay_cache::bitme", table, error = %e, "global insert decode failed");
            }
        }
    }

    /// Operational counters for `/internal/stats`.
    pub fn stats(&self) -> serde_json::Value {
        let spawns = self.spawns.lock();
        let cutoff = now_unix_ms() - SPAWN_TTL.as_millis() as i64;
        let regions: Vec<serde_json::Value> = spawns
            .iter()
            .map(|(&region, v)| {
                serde_json::json!({
                    "region": region,
                    "spawns": v.iter().filter(|s| s.at_unix_ms >= cutoff).count(),
                })
            })
            .collect();
        let g = self.global.read();
        serde_json::json!({
            "sessions": self.sessions.lock().len(),
            "tracked_targets": self.targets.lock().len(),
            "global": {
                "ready": g.ready(),
                "lowercase_usernames": g.entity_by_lowercase.len(),
                "users": g.identity_by_entity.len(),
                "user_regions": g.region_by_identity.len(),
                "region_connections": g.conn_by_region.len(),
                "region_names": g.name_by_region.len(),
                "signed_in": g.signed_in_by_entity.len(),
            },
            "spawns": regions,
        })
    }
}

fn decode_resource(
    meta: &BitmeRegionMeta,
    schema: &MirroredSchema,
    row: &[u8],
) -> Result<decode::ResourceRow, anyhow::Error> {
    if let Some(decoded) = meta.resource_fast.and_then(|fast| fast.decode(row)) {
        return Ok(decoded);
    }
    decode::decode_resource_with_fields(row, &meta.resource_fields, meta.resource_cols, schema)
}

fn fields_of(schema: &MirroredSchema, table: &str) -> Result<Vec<MirroredField>, anyhow::Error> {
    let tbl = schema
        .tables
        .iter()
        .find(|t| t.name == table)
        .ok_or_else(|| anyhow::anyhow!("schema has no table `{table}`"))?;
    let fields = schema
        .table_product(tbl)
        .ok_or_else(|| anyhow::anyhow!("table `{table}` is not a Product"))?;
    Ok(fields.to_vec())
}

pub fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_touch_and_cap() {
        let hub = BitmeHub::new();
        hub.touch_session(1);
        hub.touch_session(2);
        hub.touch_session(1); // refresh, no dup
        assert_eq!(hub.sessions.lock().len(), 2);
    }

    #[test]
    fn targets_track_and_report_health() {
        let hub = BitmeHub::new();
        hub.note_targets([10, 20]);
        assert_eq!(hub.target_health(10), None);
        assert_eq!(hub.target_health(30), None);
        assert_eq!(hub.targets.lock().len(), 2);
        // note_targets is idempotent.
        hub.note_targets([10]);
        assert_eq!(hub.targets.lock().len(), 2);
    }

    #[test]
    fn spawn_log_ttl_and_dedup_by_delete() {
        let hub = BitmeHub::new();
        hub.spawns.lock().insert(
            7,
            vec![SpawnEntry {
                entity_id: 100,
                resource_id: 1_688_062_540,
                at_unix_ms: now_unix_ms(),
            }],
        );
        assert_eq!(hub.spawns(7).len(), 1);
        hub.clear_region(7);
        assert!(hub.spawns(7).is_empty());
    }
}
