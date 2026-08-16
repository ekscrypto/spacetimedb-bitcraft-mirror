// SPDX-License-Identifier: MIT

//! Interest hub for the loopback `/internal/dim-buildings/ws` stream.
//!
//! Keys are `(Topic, entity_id)`. Shard apply paths call
//! [`InterestHub::notify`] after mutating rows; the WS task holds a
//! [`watch::Receiver`] and re-scans the housing-interior building set when
//! the generation advances. Idle keys cost nothing beyond the DashMap entry
//! while at least one receiver is alive.
//!
//! [`InterestHub`] also keeps a fleet-wide `player → claims` membership
//! index so experience / inventory / login touches on a player's home
//! region can invalidate claim-member rosters on other regions. (No WS
//! handler subscribes to those roster topics today — the dim-buildings WS
//! is the only live consumer — but the shard touch path keeps the wiring
//! so future topics can subscribe without touching shard.rs again.)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use hashbrown::HashSet;
use tokio::sync::watch;

/// Stream topic — mirrors HTTP inventory / housing / crafts / members sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Topic {
    PlayerInventory,
    PlayerHousing,
    ClaimInventory,
    PlayerCrafts,
    ClaimCrafts,
    ClaimMembers,
    /// Housing-interior building ids, keyed by `dim_key(region, dimension)`.
    /// Consumed only by the loopback `/internal/dim-buildings/ws` handler.
    DimensionBuildings,
}

/// Pack an `(region, dimension)` pair into the `u64` key space used by
/// [`Topic::DimensionBuildings`]. Both fields are `u32`, so the pack is
/// lossless: high 32 bits = region, low 32 bits = dimension.
pub fn dim_key(region: u32, dimension: u32) -> u64 {
    ((region as u64) << 32) | (dimension as u64)
}

/// Inverse of [`dim_key`]: returns `(region, dimension)`.
pub fn unpack_dim_key(key: u64) -> (u32, u32) {
    ((key >> 32) as u32, key as u32)
}

type Key = (Topic, u64);

/// Shared notify hub wired into every shard and the dim-buildings WS.
///
/// Keys are `(Topic, entity_id)`; shard apply paths call [`Self::notify`]
/// after mutating rows, and WS tasks hold a [`watch::Receiver`] that wakes
/// when the generation advances. Idle keys cost nothing beyond the
/// `DashMap` entry while at least one receiver is alive.
///
/// The hub also keeps a fleet-wide `player → claims` membership index so
/// experience / inventory / login touches on a player's home region can
/// invalidate claim-member rosters on other regions.
pub struct InterestHub {
    map: DashMap<Key, watch::Sender<u64>>,
    /// player_entity_id → claim_entity_ids (cross-region roster invalidate).
    member_of: DashMap<u64, HashSet<u64>>,
    /// Live WebSocket connections (one per upgraded socket).
    active_connections: AtomicU64,
    /// Active interest leases (`(Topic, entity_id)` receivers).
    active_leases: AtomicU64,
    /// Lifetime notify calls that reached at least one receiver.
    lifetime_notifies: AtomicU64,
    /// Lifetime coalesced pushes from WS tasks.
    lifetime_pushes: AtomicU64,
}

impl InterestHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            map: DashMap::new(),
            member_of: DashMap::new(),
            active_connections: AtomicU64::new(0),
            active_leases: AtomicU64::new(0),
            lifetime_notifies: AtomicU64::new(0),
            lifetime_pushes: AtomicU64::new(0),
        })
    }

    /// Fast path: no WS clients → shards skip touch collection.
    pub fn has_subscribers(&self) -> bool {
        self.active_leases.load(Ordering::Relaxed) > 0
    }

    /// True when at least one WS lease is listening on this key.
    pub fn is_watched(&self, topic: Topic, entity_id: u64) -> bool {
        self.map
            .get(&(topic, entity_id))
            .is_some_and(|tx| tx.receiver_count() > 0)
    }

    pub fn active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    pub fn active_leases(&self) -> u64 {
        self.active_leases.load(Ordering::Relaxed)
    }

    pub fn lifetime_notifies(&self) -> u64 {
        self.lifetime_notifies.load(Ordering::Relaxed)
    }

    pub fn lifetime_pushes(&self) -> u64 {
        self.lifetime_pushes.load(Ordering::Relaxed)
    }

    pub fn record_push(&self) {
        self.lifetime_pushes.fetch_add(1, Ordering::Relaxed);
    }

    /// Try to reserve a connection slot. Drop the guard when the socket closes.
    pub fn try_acquire_connection(self: &Arc<Self>, max: u64) -> Option<ConnectionGuard> {
        let prev = self.active_connections.fetch_add(1, Ordering::Relaxed);
        if prev >= max {
            self.active_connections.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(ConnectionGuard {
            hub: Arc::clone(self),
        })
    }

    /// Record that `player` is a member of `claim` (idempotent).
    pub fn note_member(&self, player: u64, claim: u64) {
        if player == 0 || claim == 0 {
            return;
        }
        self.member_of.entry(player).or_default().insert(claim);
    }

    /// Drop one membership edge.
    pub fn forget_member(&self, player: u64, claim: u64) {
        if player == 0 || claim == 0 {
            return;
        }
        let Some(mut set) = self.member_of.get_mut(&player) else {
            return;
        };
        set.remove(&claim);
        let empty = set.is_empty();
        drop(set);
        if empty {
            self.member_of.remove(&player);
        }
    }

    /// Swap a region's membership edges after bulk reload / reconnect.
    pub fn replace_region_members(&self, before: &[(u64, u64)], after: &[(u64, u64)]) {
        for &(player, claim) in before {
            self.forget_member(player, claim);
        }
        for &(player, claim) in after {
            self.note_member(player, claim);
        }
    }

    /// Invalidate every claim roster that includes `player`.
    pub fn notify_member_rosters(&self, player: u64) {
        let Some(claims) = self.member_of.get(&player) else {
            return;
        };
        for &claim in claims.iter() {
            self.notify(Topic::ClaimMembers, claim);
        }
    }

    /// Subscribe to generation bumps for `(topic, entity_id)`.
    /// Drop the returned [`Subscription`] to decrement the active count
    /// and remove the map entry when the last receiver goes away.
    pub fn subscribe(self: &Arc<Self>, topic: Topic, entity_id: u64) -> Subscription {
        let key = (topic, entity_id);
        let rx = {
            let entry = self.map.entry(key).or_insert_with(|| {
                let (tx, _) = watch::channel(0u64);
                tx
            });
            entry.subscribe()
        };
        self.active_leases.fetch_add(1, Ordering::Relaxed);
        Subscription {
            hub: Arc::clone(self),
            topic,
            entity_id,
            rx,
        }
    }

    /// Bump generation for a key. No-op when nobody is listening.
    pub fn notify(&self, topic: Topic, entity_id: u64) {
        let key = (topic, entity_id);
        let Some(tx) = self.map.get(&key) else {
            return;
        };
        if tx.receiver_count() == 0 {
            return;
        }
        tx.send_modify(|g| *g = g.wrapping_add(1));
        self.lifetime_notifies.fetch_add(1, Ordering::Relaxed);
    }

    fn unsubscribe(&self, topic: Topic, entity_id: u64) {
        self.active_leases.fetch_sub(1, Ordering::Relaxed);
        let key = (topic, entity_id);
        self.map
            .remove_if(&key, |_, tx| tx.receiver_count() == 0);
    }
}

/// RAII connection slot — released when the WebSocket task ends.
pub struct ConnectionGuard {
    hub: Arc<InterestHub>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.hub
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// RAII lease on one hub subscription.
pub struct Subscription {
    hub: Arc<InterestHub>,
    topic: Topic,
    entity_id: u64,
    rx: watch::Receiver<u64>,
}

impl Subscription {
    pub fn clone_receiver(&self) -> watch::Receiver<u64> {
        self.rx.clone()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.hub.unsubscribe(self.topic, self.entity_id);
    }
}

/// Deduped touch set collected during one TransactionUpdate apply.
pub struct TouchBatch {
    hub: Arc<InterestHub>,
    player_inv: Vec<u64>,
    player_housing: Vec<u64>,
    claim_inv: Vec<u64>,
    player_crafts: Vec<u64>,
    claim_crafts: Vec<u64>,
    claim_members: Vec<u64>,
    /// Packed `dim_key(region, dimension)` keys for the dim-buildings WS.
    dim_buildings: Vec<u64>,
    /// Players whose claim rosters need a refresh (skills / hexcoins / login).
    member_players: Vec<u64>,
}

impl TouchBatch {
    pub fn new(hub: &Arc<InterestHub>) -> Self {
        Self {
            hub: Arc::clone(hub),
            player_inv: Vec::new(),
            player_housing: Vec::new(),
            claim_inv: Vec::new(),
            player_crafts: Vec::new(),
            claim_crafts: Vec::new(),
            claim_members: Vec::new(),
            dim_buildings: Vec::new(),
            member_players: Vec::new(),
        }
    }

    pub fn player_inv(&mut self, id: u64) {
        if id != 0 && self.hub.is_watched(Topic::PlayerInventory, id) {
            self.player_inv.push(id);
        }
    }

    pub fn player_housing(&mut self, id: u64) {
        if id != 0 && self.hub.is_watched(Topic::PlayerHousing, id) {
            self.player_housing.push(id);
        }
    }

    pub fn claim_inv(&mut self, id: u64) {
        if id != 0 && self.hub.is_watched(Topic::ClaimInventory, id) {
            self.claim_inv.push(id);
        }
    }

    pub fn player_crafts(&mut self, id: u64) {
        if id != 0 && self.hub.is_watched(Topic::PlayerCrafts, id) {
            self.player_crafts.push(id);
        }
    }

    pub fn claim_crafts(&mut self, id: u64) {
        if id != 0 && self.hub.is_watched(Topic::ClaimCrafts, id) {
            self.claim_crafts.push(id);
        }
    }

    pub fn claim_members(&mut self, id: u64) {
        if id != 0 && self.hub.is_watched(Topic::ClaimMembers, id) {
            self.claim_members.push(id);
        }
    }

    pub fn member_player(&mut self, id: u64) {
        // Roster fan-out is filtered in `notify_member_rosters` / `notify`.
        if id != 0 {
            self.member_players.push(id);
        }
    }

    /// Record a housing-interior dimension whose building set may have
    /// changed. `region` is the owning shard (`store.region`); `dimension`
    /// is the `entrance_dimension_id`. No-op when nobody is subscribed to
    /// that exact `(region, dimension)` pair.
    pub fn dimension_buildings(&mut self, region: u32, dimension: u32) {
        if dimension == 0 {
            return;
        }
        let key = dim_key(region, dimension);
        if self.hub.is_watched(Topic::DimensionBuildings, key) {
            self.dim_buildings.push(key);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.player_inv.is_empty()
            && self.player_housing.is_empty()
            && self.claim_inv.is_empty()
            && self.player_crafts.is_empty()
            && self.claim_crafts.is_empty()
            && self.claim_members.is_empty()
            && self.dim_buildings.is_empty()
            && self.member_players.is_empty()
    }

    /// Sort+dedup then notify the hub.
    pub fn flush(mut self) {
        if self.is_empty() {
            return;
        }
        let hub = &self.hub;
        dedup_ids(&mut self.player_inv);
        dedup_ids(&mut self.player_housing);
        dedup_ids(&mut self.claim_inv);
        dedup_ids(&mut self.player_crafts);
        dedup_ids(&mut self.claim_crafts);
        dedup_ids(&mut self.claim_members);
        dedup_ids(&mut self.dim_buildings);
        dedup_ids(&mut self.member_players);
        for id in &self.player_inv {
            hub.notify(Topic::PlayerInventory, *id);
        }
        for id in &self.player_housing {
            hub.notify(Topic::PlayerHousing, *id);
        }
        for id in &self.claim_inv {
            hub.notify(Topic::ClaimInventory, *id);
        }
        for id in &self.player_crafts {
            hub.notify(Topic::PlayerCrafts, *id);
        }
        for id in &self.claim_crafts {
            hub.notify(Topic::ClaimCrafts, *id);
        }
        for id in &self.claim_members {
            hub.notify(Topic::ClaimMembers, *id);
        }
        for &key in &self.dim_buildings {
            hub.notify(Topic::DimensionBuildings, key);
        }
        for player in &self.member_players {
            hub.notify_member_rosters(*player);
        }
    }
}

fn dedup_ids(ids: &mut Vec<u64>) {
    ids.sort_unstable();
    ids.dedup();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn notify_wakes_subscriber() {
        let hub = InterestHub::new();
        let sub = hub.subscribe(Topic::PlayerInventory, 42);
        let mut rx = sub.clone_receiver();
        assert_eq!(hub.active_leases(), 1);
        assert!(hub.has_subscribers());
        assert!(hub.is_watched(Topic::PlayerInventory, 42));

        hub.notify(Topic::PlayerInventory, 42);
        rx.changed()
            .await
            .expect("generation should advance");
        assert_eq!(*rx.borrow(), 1);
        assert_eq!(hub.lifetime_notifies(), 1);

        // Drop both the lease and the cloned receiver so the key has zero
        // receivers and gets removed from the map; a stale notify must then
        // be a no-op (lifetime_notifies unchanged).
        drop(sub);
        drop(rx);
        assert_eq!(hub.active_leases(), 0);
        assert!(!hub.has_subscribers());
        hub.notify(Topic::PlayerInventory, 42);
        assert_eq!(hub.lifetime_notifies(), 1);
    }

    #[test]
    fn dimension_buildings_touch_only_when_watched() {
        // The dim-buildings touch must short-circuit when nobody is subscribed
        // to that exact (region, dimension) pair, and fire a notify otherwise.
        let hub = InterestHub::new();
        let key = dim_key(14, 12345);
        let _sub = hub.subscribe(Topic::DimensionBuildings, key);
        let mut batch = TouchBatch::new(&hub);
        batch.dimension_buildings(14, 12345); // watched → collected
        batch.dimension_buildings(14, 99999); // not watched → dropped
        batch.dimension_buildings(14, 0); // dimension 0 → invalid, dropped
        batch.flush();
        // One notify for the subscribed key only.
        assert_eq!(hub.lifetime_notifies(), 1);
    }

    #[test]
    fn touch_batch_skips_unwatched_and_dedups() {
        let hub = InterestHub::new();
        let _sub = hub.subscribe(Topic::ClaimInventory, 7);
        let mut batch = TouchBatch::new(&hub);
        batch.claim_inv(7);
        batch.claim_inv(7);
        batch.claim_inv(9); // no subscriber → not collected
        batch.flush();
        assert_eq!(hub.lifetime_notifies(), 1);
    }

    #[test]
    fn member_roster_notify_via_index() {
        let hub = InterestHub::new();
        hub.note_member(10, 100);
        hub.note_member(10, 200);
        let _a = hub.subscribe(Topic::ClaimMembers, 100);
        let _b = hub.subscribe(Topic::ClaimMembers, 200);
        let mut batch = TouchBatch::new(&hub);
        batch.member_player(10);
        batch.flush();
        assert_eq!(hub.lifetime_notifies(), 2);
    }

    #[test]
    fn connection_guard_respects_cap() {
        let hub = InterestHub::new();
        let a = hub.try_acquire_connection(1).expect("first");
        assert!(hub.try_acquire_connection(1).is_none());
        assert_eq!(hub.active_connections(), 1);
        drop(a);
        assert_eq!(hub.active_connections(), 0);
        let _b = hub.try_acquire_connection(1).expect("after drop");
    }
}
