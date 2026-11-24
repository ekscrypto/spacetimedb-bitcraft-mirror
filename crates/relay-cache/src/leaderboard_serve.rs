// SPDX-License-Identifier: MIT

//! HTTP handlers for `/leaderboard/*` — ranked player boards built from the
//! derived per-region rows in [`crate::store::player_stats`].
//!
//! The rows are maintained on the write path, so a query only folds, filters
//! and orders them: each ready shard is read under its own lock in turn, one
//! at a time. Browsers poll these routes directly and nothing rate-limits
//! the relay per client, so every response is reused for a few seconds
//! ([`ResponseCache`]).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use prost::Message;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::serve::{cache_syncing_response, format_rfc3339_millis, no_store_status, pb, wants_protobuf, Fleet};
use crate::store::claim::contains_ascii_ci;
use crate::store::PlayerStatsRow;

/// Rows returned when `limit` is omitted.
pub const DEFAULT_LIMIT: usize = 100;
/// Largest `limit` a client may ask for.
pub const MAX_LIMIT: usize = 500;
const MAX_OFFSET: usize = 100_000;
const MAX_TEXT_LEN: usize = 64;

const JSON_MIME: &str = "application/json";
const PROTOBUF_MIME: &str = "application/x-protobuf";
/// How long one board / one options document is reused across requests.
const BOARD_TTL: Duration = Duration::from_secs(5);
const OPTIONS_TTL: Duration = Duration::from_secs(10);
const MAX_CACHE_ENTRIES: usize = 128;
const SYNCING_MESSAGE: &str = "leaderboard unavailable; cache is still syncing";

#[cfg(test)]
thread_local! {
    /// Tests build a fresh fleet each; they must not share cached bodies.
    static BYPASS_CACHE: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

pub fn leaderboard_routes() -> axum::Router<Fleet> {
    axum::Router::new()
        .route("/leaderboard", get(leaderboard))
        .route("/leaderboard/options", get(leaderboard_options))
}

#[derive(Debug, Default, Deserialize)]
struct LeaderboardQuery {
    profession: Option<String>,
    region: Option<String>,
    q: Option<String>,
    sort: Option<String>,
    limit: Option<String>,
    offset: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sort {
    Xp,
    Level,
}

impl Sort {
    fn as_str(self) -> &'static str {
        match self {
            Self::Xp => "xp",
            Self::Level => "level",
        }
    }
}

/// A validated query: which rows are in scope and how they are ordered.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Scope {
    /// `None` = the overall board (totals across every skill).
    skill_id: Option<i32>,
    /// `None` = every region.
    region: Option<u32>,
    /// ASCII-case-insensitive substring of the username.
    q: Option<String>,
    sort: Sort,
    limit: usize,
    offset: usize,
}

impl Scope {
    fn parse(raw: &LeaderboardQuery, catalog: &[Skill], fleet: &Fleet) -> Result<Self, String> {
        /// A query value, trimmed; blank counts as absent.
        fn text(v: &Option<String>) -> Option<&str> {
            v.as_deref().map(str::trim).filter(|s| !s.is_empty())
        }

        let skill_id = match text(&raw.profession) {
            None => None,
            Some(want) => {
                if want.len() > MAX_TEXT_LEN {
                    return Err(format!("`profession` must be at most {MAX_TEXT_LEN} bytes"));
                }
                // A numeric skill id, else a case-insensitive skill name.
                let by_id = want
                    .parse::<i32>()
                    .ok()
                    .filter(|id| catalog.iter().any(|s| s.skill_id == *id));
                let found = by_id.or_else(|| {
                    catalog
                        .iter()
                        .find(|s| s.name.eq_ignore_ascii_case(want))
                        .map(|s| s.skill_id)
                });
                Some(found.ok_or_else(|| format!("unknown profession `{want}`"))?)
            }
        };

        let region = match text(&raw.region) {
            None => None,
            Some(want) => {
                let region = want
                    .parse::<u32>()
                    .map_err(|_| "`region` must be a region id".to_string())?;
                if !fleet.shards.iter().any(|shard| shard.region == region) {
                    return Err(format!("region {region} is not mirrored"));
                }
                Some(region)
            }
        };

        let q = match text(&raw.q) {
            None => None,
            Some(want) => {
                if want.len() > MAX_TEXT_LEN {
                    return Err(format!("`q` must be at most {MAX_TEXT_LEN} bytes"));
                }
                Some(want.to_owned())
            }
        };

        let sort = match text(&raw.sort) {
            None => Sort::Xp,
            Some(want) if want.eq_ignore_ascii_case("xp") => Sort::Xp,
            Some(want) if want.eq_ignore_ascii_case("level") => Sort::Level,
            Some(_) => return Err("`sort` must be `xp` or `level`".to_string()),
        };

        let limit = match text(&raw.limit) {
            None => DEFAULT_LIMIT,
            Some(want) => {
                let limit = want
                    .parse::<usize>()
                    .map_err(|_| "`limit` must be an integer".to_string())?;
                if limit == 0 {
                    return Err("`limit` must be >= 1".to_string());
                }
                if limit > MAX_LIMIT {
                    return Err(format!("`limit` must be <= {MAX_LIMIT}"));
                }
                limit
            }
        };

        let offset = match text(&raw.offset) {
            None => 0,
            Some(want) => {
                let offset = want
                    .parse::<usize>()
                    .map_err(|_| "`offset` must be an integer".to_string())?;
                if offset > MAX_OFFSET {
                    return Err(format!("`offset` must be <= {MAX_OFFSET}"));
                }
                offset
            }
        };

        Ok(Self {
            skill_id,
            region,
            q,
            sort,
            limit,
            offset,
        })
    }

    fn cache_key(&self) -> String {
        format!(
            "{:?}|{:?}|{:?}|{}|{}|{}",
            self.skill_id,
            self.region,
            self.q,
            self.sort.as_str(),
            self.limit,
            self.offset
        )
    }
}

/// One skill of the catalog (`skill_desc`, identical on every shard).
struct Skill {
    skill_id: i32,
    name: String,
    title: String,
    max_level: i32,
}

fn skill_catalog(fleet: &Fleet) -> Option<Vec<Skill>> {
    fleet.shards.iter().find_map(|shard| {
        let s = shard.store.read();
        s.ready.then(|| {
            s.skill_desc
                .iter()
                .filter(|(skill_id, _)| crate::decode::is_player_skill_id(*skill_id))
                .map(|(skill_id, e)| Skill {
                    skill_id,
                    name: e.name.to_string(),
                    title: e.title.to_string(),
                    max_level: e.max_level,
                })
                .collect()
        })
    })
}

/// Per-shard readiness, read once per request.
fn readiness(fleet: &Fleet) -> Vec<bool> {
    fleet.shards.iter().map(|shard| shard.store.read().ready).collect()
}

/// In-game names for the given regions, when the global mirror supplies them.
fn region_names(fleet: &Fleet, regions: &[u32]) -> HashMap<u32, Option<String>> {
    let global = fleet.bitme.global().read();
    regions
        .iter()
        .map(|&region| (region, global.region_info(region).0))
        .collect()
}

async fn leaderboard(State(fleet): State<Fleet>, headers: HeaderMap, Query(raw): Query<LeaderboardQuery>) -> Response {
    let protobuf = wants_protobuf(&headers);
    let now = Instant::now();

    let readiness = readiness(&fleet);
    let ready = readiness.iter().filter(|ready| **ready).count();
    if ready == 0 {
        return cache_syncing_response("leaderboard", SYNCING_MESSAGE.to_owned());
    }
    let Some(catalog) = skill_catalog(&fleet) else {
        return cache_syncing_response("leaderboard", SYNCING_MESSAGE.to_owned());
    };
    let scope = match Scope::parse(&raw, &catalog, &fleet) {
        Ok(scope) => scope,
        Err(error) => return bad_request(error),
    };
    // A board scoped to one region is only that shard's rows; the others
    // may still be loading without making the board partial.
    let partial = if let Some(region) = scope.region {
        let shard_ready = fleet
            .shards
            .iter()
            .zip(&readiness)
            .find(|(shard, _)| shard.region == region)
            .is_some_and(|(_, ready)| *ready);
        if !shard_ready {
            return cache_syncing_response("leaderboard", format!("region {region} is still syncing"));
        }
        false
    } else {
        ready < fleet.shards.len()
    };

    let key = format!("{}|{ready}|pb={protobuf}", scope.cache_key());
    if let Some(body) = cache_get(board_cache(), &key, now) {
        return cached_response(body);
    }

    let mut entries = collect(&fleet, scope.region, scope.skill_id, &readiness, now);
    match scope.sort {
        Sort::Xp => entries.sort_unstable_by(|a, b| {
            b.xp.cmp(&a.xp)
                .then_with(|| b.level.cmp(&a.level))
                .then_with(|| a.entity_id.cmp(&b.entity_id))
        }),
        Sort::Level => entries.sort_unstable_by(|a, b| {
            b.level
                .cmp(&a.level)
                .then_with(|| b.xp.cmp(&a.xp))
                .then_with(|| a.entity_id.cmp(&b.entity_id))
        }),
    }
    // Rank is the position on the whole (profession, region) board. A name
    // search narrows what is returned, never renumbers it, and paging then
    // applies to the matches.
    let needle = scope.q.as_deref().map(|q| q.to_ascii_lowercase());
    let matched: Vec<(usize, &Entry)> = entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| needle.as_deref().is_none_or(|n| contains_ascii_ci(&entry.username, n)))
        .map(|(i, entry)| (i + 1, entry))
        .collect();
    let matches = matched.len();
    let rows: Vec<(usize, Entry)> = matched
        .into_iter()
        .skip(scope.offset)
        .take(scope.limit)
        .map(|(rank, entry)| (rank, entry.clone()))
        .collect();

    let mut regions: Vec<u32> = scope.region.into_iter().collect();
    for (_, entry) in &rows {
        if !regions.contains(&entry.region) {
            regions.push(entry.region);
        }
    }
    let names = region_names(&fleet, &regions);
    let board = Board {
        skill_id: scope.skill_id,
        profession: scope
            .skill_id
            .and_then(|id| catalog.iter().find(|s| s.skill_id == id))
            .map(|s| s.name.clone()),
        region: scope.region,
        region_name: scope.region.and_then(|region| names.get(&region).cloned().flatten()),
        q: scope.q.clone(),
        sort: scope.sort,
        total_players: entries.len(),
        matches,
        offset: scope.offset,
        limit: scope.limit,
        partial,
        rows,
        generated_at: format_rfc3339_millis(unix_micros()),
        names,
    };

    let body = if protobuf {
        CachedBody {
            bytes: board.to_proto().encode_to_vec(),
            content_type: PROTOBUF_MIME,
        }
    } else {
        CachedBody {
            bytes: serde_json::to_vec(&board.to_json()).expect("board JSON serializes"),
            content_type: JSON_MIME,
        }
    };
    cache_put(board_cache(), key, body.clone(), now);
    cached_response(body)
}

async fn leaderboard_options(State(fleet): State<Fleet>, headers: HeaderMap) -> Response {
    let protobuf = wants_protobuf(&headers);
    let now = Instant::now();

    let readiness = readiness(&fleet);
    let ready = readiness.iter().filter(|ready| **ready).count();
    let key = format!("{ready}|pb={protobuf}");
    if let Some(body) = cache_get(options_cache(), &key, now) {
        return cached_response(body);
    }

    let Some(catalog) = skill_catalog(&fleet) else {
        return cache_syncing_response("leaderboard", SYNCING_MESSAGE.to_owned());
    };
    let mut regions: Vec<(u32, bool, usize)> = fleet
        .shards
        .iter()
        .map(|shard| {
            let s = shard.store.read();
            (shard.region, s.ready, if s.ready { s.player_stats.len() } else { 0 })
        })
        .collect();
    regions.sort_unstable_by_key(|(region, _, _)| *region);
    let ids: Vec<u32> = regions.iter().map(|(region, _, _)| *region).collect();
    let names = region_names(&fleet, &ids);

    let professions: Vec<pb::LeaderboardProfession> = catalog
        .iter()
        .map(|s| pb::LeaderboardProfession {
            skill_id: s.skill_id,
            name: s.name.clone(),
            title: s.title.clone(),
            max_level: s.max_level,
        })
        .collect();
    let region_msgs: Vec<pb::LeaderboardRegion> = regions
        .iter()
        .map(|&(region, ready, players)| pb::LeaderboardRegion {
            region,
            name: names.get(&region).cloned().flatten(),
            players: players as i32,
            ready,
        })
        .collect();
    let generated_at = format_rfc3339_millis(unix_micros());

    let body = if protobuf {
        let msg = pb::LeaderboardOptions {
            professions,
            regions: region_msgs,
            default_limit: DEFAULT_LIMIT as i32,
            max_limit: MAX_LIMIT as i32,
            generated_at,
        };
        CachedBody {
            bytes: msg.encode_to_vec(),
            content_type: PROTOBUF_MIME,
        }
    } else {
        let doc = json!({
            "professions": professions.iter().map(|p| json!({
                "skill_id": p.skill_id,
                "name": p.name,
                "title": p.title,
                "max_level": p.max_level,
            })).collect::<Vec<_>>(),
            "regions": region_msgs.iter().map(|r| json!({
                "region": r.region,
                "name": r.name,
                "players": r.players,
                "ready": r.ready,
            })).collect::<Vec<_>>(),
            "defaults": { "limit": DEFAULT_LIMIT, "max_limit": MAX_LIMIT },
            "generated_at": generated_at,
        });
        CachedBody {
            bytes: serde_json::to_vec(&doc).expect("options JSON serializes"),
            content_type: JSON_MIME,
        }
    };
    cache_put(options_cache(), key, body.clone(), now);
    cached_response(body)
}

/// One player as ranked on a board. `level` / `xp` belong to the selected
/// profession, or are the totals on the overall board.
#[derive(Debug, Clone)]
struct Entry {
    entity_id: u64,
    region: u32,
    username: String,
    level: i64,
    xp: i64,
    total_level: i64,
    total_xp: i64,
    signed_in: bool,
    /// Every source row is gone; kept only through the delete grace.
    departing: bool,
}

impl Entry {
    fn from_row(row: PlayerStatsRow<'_>, region: u32, skill_id: Option<i32>) -> Self {
        let (level, xp) = match skill_id {
            None => (row.total_level, row.total_xp),
            Some(skill_id) => row
                .skill(skill_id)
                .map_or((0, 0), |s| (i64::from(s.level), i64::from(s.xp))),
        };
        Self {
            entity_id: row.entity_id,
            region,
            username: row.username.to_owned(),
            level,
            xp,
            total_level: row.total_level,
            total_xp: row.total_xp,
            signed_in: row.signed_in,
            departing: row.departing,
        }
    }

    /// Whether `self` should replace `other` when the same player is on two
    /// shards at once.
    fn outranks(&self, other: &Self) -> bool {
        match (self.departing, other.departing) {
            (false, true) => true,
            (true, false) => false,
            // Both mid-transfer or both present: the larger total is the
            // fresher experience row.
            _ => self.total_xp > other.total_xp,
        }
    }
}

/// Every player in scope, in shard order (unranked).
fn collect(fleet: &Fleet, region: Option<u32>, skill_id: Option<i32>, readiness: &[bool], now: Instant) -> Vec<Entry> {
    let mut entries: Vec<Entry> = Vec::new();
    let mut index: HashMap<u64, usize> = HashMap::new();
    for (shard, ready) in fleet.shards.iter().zip(readiness) {
        if !ready || region.is_some_and(|region| region != shard.region) {
            continue;
        }
        let s = shard.store.read();
        if !s.ready {
            continue;
        }
        for row in s.player_stats.iter(now) {
            let entry = Entry::from_row(row, shard.region, skill_id);
            match index.get(&entry.entity_id) {
                None => {
                    index.insert(entry.entity_id, entries.len());
                    entries.push(entry);
                }
                // A region transfer can land the player's rows on the new
                // shard before the delete reaches the old one.
                Some(&i) => {
                    if entry.outranks(&entries[i]) {
                        entries[i] = entry;
                    }
                }
            }
        }
    }
    entries
}

/// The result of one board query.
struct Board {
    skill_id: Option<i32>,
    profession: Option<String>,
    region: Option<u32>,
    region_name: Option<String>,
    q: Option<String>,
    sort: Sort,
    /// Players in the profession/region scope — the denominator of a rank.
    total_players: usize,
    /// Of those, players matching `q` (all of them when `q` is absent).
    matches: usize,
    offset: usize,
    limit: usize,
    /// A region in scope was still loading and contributed no rows.
    partial: bool,
    /// `(rank, player)` in rank order.
    rows: Vec<(usize, Entry)>,
    generated_at: String,
    names: HashMap<u32, Option<String>>,
}

impl Board {
    fn to_json(&self) -> Value {
        json!({
            "scope": {
                "skill_id": self.skill_id,
                "profession": self.profession,
                "region": self.region,
                "region_name": self.region_name,
                "q": self.q,
                "sort": self.sort.as_str(),
            },
            "total_players": self.total_players,
            "matches": self.matches,
            "offset": self.offset,
            "limit": self.limit,
            "partial": self.partial,
            "players": self
                .rows
                .iter()
                .map(|(rank, e)| {
                    json!({
                        "rank": rank,
                        "entity_id": e.entity_id.to_string(),
                        "username": e.username,
                        "region": e.region,
                        "region_name": self.names.get(&e.region).cloned().flatten(),
                        "level": e.level,
                        "xp": e.xp,
                        "total_level": e.total_level,
                        "total_xp": e.total_xp,
                        "signed_in": e.signed_in,
                    })
                })
                .collect::<Vec<_>>(),
            "generated_at": self.generated_at,
        })
    }

    fn to_proto(&self) -> pb::Leaderboard {
        pb::Leaderboard {
            scope: Some(pb::LeaderboardScope {
                skill_id: self.skill_id,
                profession: self.profession.clone(),
                region: self.region,
                region_name: self.region_name.clone(),
                q: self.q.clone(),
                sort: self.sort.as_str().to_owned(),
            }),
            players: self
                .rows
                .iter()
                .map(|(rank, e)| pb::LeaderboardEntry {
                    entity_id: e.entity_id,
                    username: e.username.clone(),
                    region: e.region,
                    region_name: self.names.get(&e.region).cloned().flatten(),
                    rank: *rank as i64,
                    level: e.level,
                    xp: e.xp,
                    total_level: e.total_level,
                    total_xp: e.total_xp,
                    signed_in: e.signed_in,
                })
                .collect(),
            total_players: self.total_players as i32,
            matches: self.matches as i32,
            offset: self.offset as i32,
            limit: self.limit as i32,
            partial: self.partial,
            generated_at: self.generated_at.clone(),
        }
    }
}

fn bad_request(error: String) -> Response {
    no_store_status(StatusCode::BAD_REQUEST, json!({ "error": error }))
}

fn unix_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or_default()
}

/// A serialized success body, reused across requests for the cache TTL.
#[derive(Clone)]
struct CachedBody {
    bytes: Vec<u8>,
    content_type: &'static str,
}

fn cached_response(body: CachedBody) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(body.content_type));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    (StatusCode::OK, headers, body.bytes).into_response()
}

/// Reuses a serialized response for a few seconds, so a polling crowd costs
/// the relay one fold per scope per TTL instead of one per request. Only
/// request handlers touch it — never the row-apply path.
struct ResponseCache {
    ttl: Duration,
    entries: HashMap<String, (Instant, CachedBody)>,
}

impl ResponseCache {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: HashMap::new(),
        }
    }

    fn get(&mut self, key: &str, now: Instant) -> Option<CachedBody> {
        self.purge(now);
        self.entries.get(key).map(|(_, body)| body.clone())
    }

    fn insert(&mut self, key: String, body: CachedBody, now: Instant) {
        self.purge(now);
        // Every distinct search string is its own entry; past the cap, drop
        // the oldest rather than grow with the traffic.
        while self.entries.len() >= MAX_CACHE_ENTRIES {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, (at, _))| *at)
                .map(|(key, _)| key.clone());
            let Some(oldest) = oldest else {
                break;
            };
            self.entries.remove(&oldest);
        }
        self.entries.insert(key, (now, body));
    }

    fn purge(&mut self, now: Instant) {
        self.entries
            .retain(|_, (at, _)| now.saturating_duration_since(*at) < self.ttl);
    }
}

fn board_cache() -> &'static Mutex<ResponseCache> {
    static CACHE: OnceLock<Mutex<ResponseCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(ResponseCache::new(BOARD_TTL)))
}

fn options_cache() -> &'static Mutex<ResponseCache> {
    static CACHE: OnceLock<Mutex<ResponseCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(ResponseCache::new(OPTIONS_TTL)))
}

fn cache_get(cache: &Mutex<ResponseCache>, key: &str, now: Instant) -> Option<CachedBody> {
    #[cfg(test)]
    if BYPASS_CACHE.with(|bypass| bypass.get()) {
        return None;
    }
    cache.lock().unwrap_or_else(|e| e.into_inner()).get(key, now)
}

fn cache_put(cache: &Mutex<ResponseCache>, key: String, body: CachedBody, now: Instant) {
    #[cfg(test)]
    if BYPASS_CACHE.with(|bypass| bypass.get()) {
        return;
    }
    cache.lock().unwrap_or_else(|e| e.into_inner()).insert(key, body, now);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::SkillDescRow;
    use crate::shard::ShardHandle;
    use crate::store::{PlayerStatsSource, RegionStore};
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn get(fleet: Fleet, uri: &str, accept: Option<&str>) -> (StatusCode, HeaderMap, Vec<u8>) {
        let mut request = Request::builder().uri(uri);
        if let Some(accept) = accept {
            request = request.header(header::ACCEPT, accept);
        }
        let response = leaderboard_routes()
            .with_state(fleet)
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, headers, body.to_vec())
    }

    async fn get_json(fleet: Fleet, uri: &str) -> (StatusCode, Value) {
        let (status, _, body) = get(fleet, uri, None).await;
        let json = serde_json::from_slice(&body).unwrap_or_else(|e| panic!("{uri}: not JSON ({e}): {body:?}"));
        (status, json)
    }

    /// `(rank, username, xp)` of every row, in board order.
    fn ranks(body: &Value) -> Vec<(u64, &str, i64)> {
        body["players"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| {
                (
                    p["rank"].as_u64().unwrap(),
                    p["username"].as_str().unwrap(),
                    p["xp"].as_i64().unwrap(),
                )
            })
            .collect()
    }

    /// `(entity_id, username, signed_in, experience stacks)` of a seeded player.
    type SeededPlayer = (u64, &'static str, bool, &'static [(i32, i32)]);

    /// A shard carrying the skill catalog and the given players.
    fn shard(region: u32, ready: bool, players: &[SeededPlayer]) -> Arc<ShardHandle> {
        let mut store = RegionStore::empty(region);
        let now = Instant::now();
        for (id, name) in [(1, "ANY"), (2, "Forestry"), (5, "Mining"), (12, "Fishing")] {
            store.skill_desc.upsert(SkillDescRow {
                id,
                name: name.into(),
                title: name.into(),
                max_level: 100,
            });
        }
        for &(id, name, signed_in, stacks) in players {
            store.player_stats.apply_experience(id, stacks, now);
            store.player_stats.apply_username(id, name, now);
            store.player_stats.apply_player_state(id, signed_in, now);
        }
        store.ready = ready;
        Arc::new(ShardHandle {
            region,
            store: Arc::new(parking_lot::RwLock::new(store)),
        })
    }

    fn fleet(shards: Vec<Arc<ShardHandle>>) -> Fleet {
        Fleet {
            shards,
            memory_pressure: Arc::new(AtomicBool::new(false)),
            interest: crate::interest::InterestHub::new(),
            roads: None,
            bitme: crate::bitme::BitmeHub::new(),
        }
    }

    /// Two regions whose overall and per-skill orders differ:
    ///
    /// | player  | Mining | Fishing | total |
    /// |---------|-------:|--------:|------:|
    /// | Ann     |  5 000 |   1 000 | 6 000 |
    /// | Bob     |  3 000 |   4 000 | 7 000 |
    /// | Cy      |  2 000 |   6 000 | 8 000 |
    /// | Dee-ann |      0 |       0 |     0 |
    fn two_regions() -> Fleet {
        fleet(vec![
            shard(
                7,
                true,
                &[
                    (11, "Ann", true, &[(5, 5_000), (12, 1_000)]),
                    (12, "Bob", false, &[(5, 3_000), (12, 4_000)]),
                ],
            ),
            shard(
                12,
                true,
                &[
                    (21, "Cy", false, &[(5, 2_000), (12, 6_000)]),
                    (22, "Dee-ann", true, &[]),
                ],
            ),
        ])
    }

    #[tokio::test]
    async fn overall_board_ranks_by_total_xp_across_shards() {
        let (status, body) = get_json(two_regions(), "/leaderboard").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            ranks(&body),
            [
                (1, "Cy", 8_000),
                (2, "Bob", 7_000),
                (3, "Ann", 6_000),
                (4, "Dee-ann", 0)
            ]
        );
        assert_eq!(
            (body["total_players"].as_u64(), body["matches"].as_u64()),
            (Some(4), Some(4)),
            "no search: every player matches"
        );
        assert_eq!((body["offset"].as_u64(), body["limit"].as_u64()), (Some(0), Some(100)));
        assert_eq!(body["partial"], false);
        let scope = &body["scope"];
        assert_eq!(
            scope,
            &json!({
                "skill_id": null, "profession": null, "region": null, "region_name": null, "q": null, "sort": "xp",
            })
        );

        let top = &body["players"][0];
        assert_eq!(top["entity_id"], "21", "entity ids are strings (u64 exceeds 2^53)");
        assert_eq!(
            (top["region"].as_u64(), top["signed_in"].as_bool()),
            (Some(12), Some(false))
        );
        assert_eq!(top["region_name"], Value::Null, "no global mirror in this fixture");
        assert!(top["generated_at"].is_null() && body["generated_at"].is_string());
    }

    #[tokio::test]
    async fn profession_board_by_id_or_name_uses_that_skill() {
        let (status, body) = get_json(two_regions(), "/leaderboard?profession=Mining").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            ranks(&body),
            [
                (1, "Ann", 5_000),
                (2, "Bob", 3_000),
                (3, "Cy", 2_000),
                (4, "Dee-ann", 0)
            ]
        );
        assert_eq!(
            (body["scope"]["skill_id"].as_i64(), body["scope"]["profession"].as_str()),
            (Some(5), Some("Mining"))
        );
        assert_eq!(
            (
                body["players"][0]["level"].as_i64(),
                body["players"][0]["total_xp"].as_i64()
            ),
            (Some(crate::xp::xp_to_level(5_000)), Some(6_000))
        );

        let (status, by_id) = get_json(two_regions(), "/leaderboard?profession=5").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(by_id["players"], body["players"]);
    }

    #[tokio::test]
    async fn level_sort_can_disagree_with_xp_sort_on_the_overall_board() {
        // Spread XP buys more levels than the same XP in one skill... nearly:
        // 520 is the level-2 threshold and 1100 the level-3 threshold, so
        // two skills at 520 give total level 4 for 1 040 XP, while 1 100 XP
        // in one skill is only total level 3.
        let f = fleet(vec![shard(
            7,
            true,
            &[
                (1, "Spread", false, &[(5, 520), (12, 520)]),
                (2, "Focused", false, &[(5, 1_100)]),
            ],
        )]);
        let (_, by_xp) = get_json(f.clone(), "/leaderboard?sort=xp").await;
        assert_eq!(ranks(&by_xp), [(1, "Focused", 1_100), (2, "Spread", 1_040)]);
        let (_, by_level) = get_json(f.clone(), "/leaderboard?sort=level").await;
        assert_eq!(by_level["scope"]["sort"], "level");
        assert_eq!(
            ranks(&by_level),
            [(1, "Spread", 1_040), (2, "Focused", 1_100)],
            "level order is the tie-break-free total level, then xp"
        );
        assert_eq!(
            (
                by_level["players"][0]["level"].as_i64(),
                by_level["players"][1]["level"].as_i64()
            ),
            (Some(4), Some(3))
        );
    }

    #[tokio::test]
    async fn region_filter_ranks_within_that_region() {
        let (status, body) = get_json(two_regions(), "/leaderboard?region=12").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ranks(&body), [(1, "Cy", 8_000), (2, "Dee-ann", 0)]);
        assert_eq!(body["total_players"], 2);
        assert_eq!(body["scope"]["region"], 12);
        assert_eq!(body["partial"], false);
    }

    #[tokio::test]
    async fn not_ready_shard_is_excluded_and_flagged_partial() {
        let fleet = fleet(vec![
            shard(7, true, &[(11, "Ann", true, &[(5, 5_000)])]),
            shard(12, false, &[(21, "Cy", true, &[(5, 9_000)])]),
        ]);
        let (status, body) = get_json(fleet, "/leaderboard").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["partial"], true);
        assert_eq!(ranks(&body), [(1, "Ann", 5_000)]);
    }

    #[tokio::test]
    async fn nothing_ready_is_503_cache_syncing() {
        for uri in ["/leaderboard", "/leaderboard/options"] {
            let fleet = fleet(vec![shard(7, false, &[])]);
            let (status, headers, body) = get(fleet, uri, None).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{uri}");
            assert_eq!(headers[header::RETRY_AFTER], "30");
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["code"], "cache_syncing", "{uri}");
        }
    }

    #[tokio::test]
    async fn region_still_syncing_is_503_but_other_regions_still_serve() {
        let f = fleet(vec![
            shard(7, true, &[(11, "Ann", true, &[(5, 5_000)])]),
            shard(12, false, &[]),
        ]);
        let (status, body) = get_json(f.clone(), "/leaderboard?region=12").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "cache_syncing");
        let (status, body) = get_json(f, "/leaderboard?region=7").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ranks(&body), [(1, "Ann", 5_000)]);
    }

    #[tokio::test]
    async fn q_is_an_ascii_case_insensitive_substring_and_keeps_full_board_rank() {
        let (_, body) = get_json(two_regions(), "/leaderboard?q=%20ANN%20").await;
        assert_eq!(
            ranks(&body),
            [(3, "Ann", 6_000), (4, "Dee-ann", 0)],
            "search narrows, never renumbers"
        );
        assert_eq!(
            (body["total_players"].as_u64(), body["matches"].as_u64()),
            (Some(4), Some(2))
        );
        assert_eq!(body["scope"]["q"], "ANN", "trimmed");

        // Paging applies to the matches.
        let (_, page) = get_json(two_regions(), "/leaderboard?q=ann&limit=1").await;
        assert_eq!(ranks(&page), [(3, "Ann", 6_000)]);
        assert_eq!(
            (page["total_players"].as_u64(), page["matches"].as_u64()),
            (Some(4), Some(2))
        );
        let (_, none) = get_json(two_regions(), "/leaderboard?q=nobody").await;
        assert_eq!(
            (none["players"].as_array().unwrap().len(), none["matches"].as_u64()),
            (0, Some(0))
        );
        assert_eq!(none["total_players"], 4, "the scope size does not depend on the search");
    }

    #[tokio::test]
    async fn limit_and_offset_page_with_stable_ranks() {
        let (_, page) = get_json(two_regions(), "/leaderboard?limit=2&offset=2").await;
        assert_eq!(
            ranks(&page),
            [(3, "Ann", 6_000), (4, "Dee-ann", 0)],
            "ranks continue across pages"
        );
        assert_eq!(
            (
                page["total_players"].as_u64(),
                page["offset"].as_u64(),
                page["limit"].as_u64()
            ),
            (Some(4), Some(2), Some(2))
        );
        // Beyond the end is an empty page, not an error.
        let (status, page) = get_json(two_regions(), "/leaderboard?offset=50").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page["players"].as_array().unwrap().len(), 0);
        assert_eq!(page["total_players"], 4);
    }

    #[tokio::test]
    async fn invalid_params_are_json_400s() {
        let cases = [
            "/leaderboard?profession=bogus",
            // The "ANY" sentinel is on every player but is not a profession.
            "/leaderboard?profession=1",
            "/leaderboard?region=99",
            "/leaderboard?region=abc",
            "/leaderboard?sort=nope",
            "/leaderboard?limit=0",
            "/leaderboard?limit=501",
            "/leaderboard?limit=abc",
            "/leaderboard?offset=-1",
            "/leaderboard?q=zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        ];
        for uri in cases {
            let (status, _, body) = get(two_regions(), uri, None).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
            let body: Value = serde_json::from_slice(&body).unwrap_or_else(|e| panic!("{uri}: not JSON ({e})"));
            assert!(body["error"].as_str().is_some_and(|e| !e.is_empty()), "{uri}: {body}");
        }
    }

    #[tokio::test]
    async fn duplicate_entity_across_shards_keeps_the_present_row() {
        // Mid-transfer: the player already has live rows on region 12 and is
        // still inside the delete grace on region 7.
        let departing = shard(7, true, &[(9, "Ann", true, &[(5, 1_000)])]);
        {
            let mut old = departing.store.write();
            let now = Instant::now();
            for source in [
                PlayerStatsSource::Experience,
                PlayerStatsSource::Username,
                PlayerStatsSource::PlayerState,
            ] {
                old.player_stats.mark_deleted(9, source, now);
            }
        }
        let fleet = fleet(vec![
            departing,
            shard(
                12,
                true,
                &[(9, "Ann", true, &[(5, 1_100)]), (10, "Zed", false, &[(5, 900)])],
            ),
        ]);
        let (_, body) = get_json(fleet, "/leaderboard").await;
        assert_eq!(body["total_players"], 2, "the player is ranked once");
        assert_eq!(ranks(&body), [(1, "Ann", 1_100), (2, "Zed", 900)]);
        assert_eq!(body["players"][0]["region"], 12, "the row with live source rows wins");
    }

    #[tokio::test]
    async fn protobuf_board_via_accept_header() {
        let (status, headers, body) = get(
            two_regions(),
            "/leaderboard?profession=Mining&limit=2",
            Some(PROTOBUF_MIME),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::CONTENT_TYPE], PROTOBUF_MIME);
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
        let board = pb::Leaderboard::decode(&body[..]).unwrap();
        assert_eq!(
            (board.total_players, board.matches, board.limit, board.offset),
            (4, 4, 2, 0)
        );
        let scope = board.scope.unwrap();
        assert_eq!(
            (scope.skill_id, scope.profession.as_deref(), scope.sort.as_str()),
            (Some(5), Some("Mining"), "xp")
        );
        let names: Vec<&str> = board.players.iter().map(|p| p.username.as_str()).collect();
        assert_eq!(names, ["Ann", "Bob"]);
        assert_eq!((board.players[0].rank, board.players[0].entity_id), (1, 11));
    }

    #[tokio::test]
    async fn options_lists_professions_and_regions() {
        let fleet = fleet(vec![
            shard(12, true, &[(21, "Cy", false, &[(5, 2_000)]), (22, "Dee", true, &[])]),
            shard(7, false, &[]),
        ]);
        let (status, body) = get_json(fleet.clone(), "/leaderboard/options").await;
        assert_eq!(status, StatusCode::OK);
        let names: Vec<&str> = body["professions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["Forestry", "Mining", "Fishing"], "sorted by id, ANY excluded");
        let mining = &body["professions"][1];
        assert_eq!(
            (
                mining["skill_id"].as_i64(),
                mining["title"].as_str(),
                mining["max_level"].as_i64()
            ),
            (Some(5), Some("Mining"), Some(100))
        );
        let regions = body["regions"].as_array().unwrap();
        let ids: Vec<u64> = regions.iter().map(|r| r["region"].as_u64().unwrap()).collect();
        assert_eq!(ids, [7, 12], "sorted by region id");
        assert_eq!(
            (regions[0]["ready"].as_bool(), regions[0]["players"].as_u64()),
            (Some(false), Some(0))
        );
        assert_eq!(
            (regions[1]["ready"].as_bool(), regions[1]["players"].as_u64()),
            (Some(true), Some(2))
        );
        assert_eq!(regions[1]["name"], Value::Null);
        assert_eq!(body["defaults"], json!({ "limit": 100, "max_limit": 500 }));
        assert!(body["generated_at"].is_string());

        let (status, _, body) = get(fleet, "/leaderboard/options", Some(PROTOBUF_MIME)).await;
        assert_eq!(status, StatusCode::OK);
        let options = pb::LeaderboardOptions::decode(&body[..]).unwrap();
        assert_eq!(options.professions.len(), 3);
        assert_eq!((options.default_limit, options.max_limit), (100, 500));
        assert_eq!(
            options
                .regions
                .iter()
                .map(|r| (r.region, r.ready, r.players))
                .collect::<Vec<_>>(),
            [(7, false, 0), (12, true, 2)]
        );
    }

    #[tokio::test]
    async fn repeated_request_reuses_the_body_for_the_same_scope() {
        BYPASS_CACHE.with(|bypass| bypass.set(false));
        let fleet = two_regions();
        let uri = "/leaderboard?profession=Forestry&limit=3&q=cache-probe";
        let (.., first) = get(fleet.clone(), uri, None).await;
        // Same scope, data changed underneath: the cached body still wins.
        fleet.shards[0]
            .store
            .write()
            .player_stats
            .apply_username(11, "cache-probe", Instant::now());
        let (.., second) = get(fleet.clone(), uri, None).await;
        // Another scope is computed on its own.
        let (.., other) = get(fleet.clone(), "/leaderboard?profession=Forestry&limit=3", None).await;
        let (.., options) = get(fleet.clone(), "/leaderboard/options", None).await;
        let (.., options_again) = get(fleet.clone(), "/leaderboard/options", None).await;
        BYPASS_CACHE.with(|bypass| bypass.set(true));

        assert_eq!(first, second);
        assert_ne!(first, other);
        let probe: Value = serde_json::from_slice(&first).unwrap();
        assert_eq!(
            (probe["total_players"].as_u64(), probe["matches"].as_u64()),
            (Some(4), Some(0)),
            "no player matched the probe string when it was cached"
        );
        assert_eq!(options, options_again);
    }
}
