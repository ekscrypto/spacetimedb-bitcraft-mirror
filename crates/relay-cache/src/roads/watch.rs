// SPDX-License-Identifier: MIT

//! Player-keyed change stream over the dense resource tile map
//! (`/bitme/session/:id/resources/ws`).
//!
//! Per-listener state is **only the window anchor coordinates** — the server
//! keeps no copy of any window. Whenever the compacted map is mutated, the
//! region worker (which applies upstream batches strictly in order on one
//! task) drains the tile writes recorded by
//! [`ResourceTileMap::take_pending`](super::resource_map::ResourceTileMap::take_pending)
//! and pushes every changed tile that falls inside a listener's 400×400
//! window: `changed tiles × listeners` bounds checks per batch, nothing
//! else. The client owns convergence — it fetches the BMR1 window over HTTP
//! and applies the streamed tile words on top; on reconnect, resync, or a
//! `dict_version` change it simply refetches.
//!
//! Frames travel a bounded per-connection channel; a full queue **poisons**
//! the listener (its socket task closes the connection) — frames are never
//! silently dropped to a live socket, so per-connection ordering plus the
//! single-worker apply order give a total order over delivered events.
//!
//! Lock ordering: hub → grid. `register` and `on_grid_replaced` take the
//! registry lock and then the grid's recording flag (a read guard — the
//! flag is an atomic); `fanout` touches only the registry and must be
//! called with no grid guard held (the feed path releases the write guard
//! first).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc::error::TrySendError;

use super::coords::{region_origin, REGION_SIDE};
use super::store::{RoadsRegionGrid, RoadsRegionHandle};

/// Window width in tiles — the BMR1 window width (`RESOURCE_WINDOW_WIDTH`
/// in `bitme_serve`). The anchor is the center tile; the window covers
/// world tiles `[anchor − width/2, anchor + width/2 − 1]` per axis.
pub const WINDOW_WIDTH: i32 = 400;

/// Per-listener queue bound. Reaching it poisons the listener — its socket
/// closes and the client reconnects + refetches — rather than dropping
/// frames.
const LISTENER_QUEUE: usize = 256;

/// One tile change: world odd-r tile `(x, z)` + the final tile word
/// (bit layout of `roads/resource_map.rs`, water bit included).
pub type TileDelta = (i32, i32, u16);

/// Frame sent down a listener's channel.
#[derive(Debug)]
pub enum WatchFrame {
    /// Tile words that changed in one applied batch, world coords.
    Delta {
        region: u32,
        dict_version: u32,
        tiles: Vec<TileDelta>,
    },
    /// The region grid was replaced (upstream re-seed) or went live; the
    /// client must refetch the BMR1 window — and the dictionary when the
    /// frame's `dict_version` differs from its snapshot's.
    Resync { region: u32, reason: &'static str },
}

struct ResourceListener {
    player_entity_id: u64,
    anchor_x: AtomicI32,
    anchor_z: AtomicI32,
    poison: AtomicBool,
    tx: tokio::sync::mpsc::Sender<WatchFrame>,
}

impl ResourceListener {
    fn anchor(&self) -> (i32, i32) {
        (
            self.anchor_x.load(Ordering::Relaxed),
            self.anchor_z.load(Ordering::Relaxed),
        )
    }
}

/// RAII registration. Dropping unregisters the listener and, when the
/// region empties, turns change recording back off on that grid.
struct ListenerLease {
    hub: Arc<ResourceWatchHub>,
    region: u32,
    listener: Arc<ResourceListener>,
    grid: Arc<RwLock<RoadsRegionGrid>>,
}

impl Drop for ListenerLease {
    fn drop(&mut self) {
        let mut guard = self.hub.regions.write();
        if let Some(listeners) = guard.get_mut(&self.region) {
            listeners.retain(|l| !Arc::ptr_eq(l, &self.listener));
            if listeners.is_empty() {
                guard.remove(&self.region);
                self.grid.read().resource_map.set_watch(false);
            }
        }
    }
}

/// Handle held by the socket task; keeps the registration alive.
pub struct ListenerHandle {
    listener: Arc<ResourceListener>,
    _lease: ListenerLease,
}

impl ListenerHandle {
    pub fn player_entity_id(&self) -> u64 {
        self.listener.player_entity_id
    }

    pub fn anchor(&self) -> (i32, i32) {
        self.listener.anchor()
    }

    /// Move the window anchor (world tile of the window center).
    pub fn update_anchor(&self, x: i32, z: i32) {
        self.listener.anchor_x.store(x, Ordering::Relaxed);
        self.listener.anchor_z.store(z, Ordering::Relaxed);
    }

    /// True once a fan-out overflowed this listener's queue — close the
    /// socket; the client reconnects and refetches.
    pub fn poisoned(&self) -> bool {
        self.listener.poison.load(Ordering::Relaxed)
    }
}

/// Registry of resource-stream listeners, one fleet-wide instance inside
/// [`super::catalog::RoadsFleet`] so the HTTP handlers and the ingest feed
/// share it.
pub struct ResourceWatchHub {
    regions: RwLock<HashMap<u32, Vec<Arc<ResourceListener>>>>,
}

impl Default for ResourceWatchHub {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ResourceWatchHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let regions = self.regions.read();
        f.debug_struct("ResourceWatchHub")
            .field("regions", &regions.keys().len())
            .field("listeners", &regions.values().map(|l| l.len()).sum::<usize>())
            .finish()
    }
}

impl ResourceWatchHub {
    pub fn new() -> Self {
        Self {
            regions: RwLock::new(HashMap::new()),
        }
    }

    /// Fast gate for the apply path — one read lock + map lookup.
    pub fn has_listeners(&self, region: u32) -> bool {
        self.regions.read().get(&region).is_some_and(|l| !l.is_empty())
    }

    /// Listener count for a region (diagnostics / tests).
    pub fn listener_count(&self, region: u32) -> usize {
        self.regions.read().get(&region).map_or(0, |l| l.len())
    }

    /// Register a listener anchored at `anchor` (world tile). The grid's
    /// change recording is enabled **before** the listener becomes visible,
    /// so no apply can slip in between the two. Returns the socket task's
    /// handle and the receiving half of its frame queue.
    pub fn register(
        self: &Arc<Self>,
        handle: &RoadsRegionHandle,
        player_entity_id: u64,
        anchor: (i32, i32),
    ) -> (ListenerHandle, tokio::sync::mpsc::Receiver<WatchFrame>) {
        let (tx, rx) = tokio::sync::mpsc::channel(LISTENER_QUEUE);
        let listener = Arc::new(ResourceListener {
            player_entity_id,
            anchor_x: AtomicI32::new(anchor.0),
            anchor_z: AtomicI32::new(anchor.1),
            poison: AtomicBool::new(false),
            tx,
        });
        let mut guard = self.regions.write();
        handle.grid.read().resource_map.set_watch(true);
        guard.entry(handle.region).or_default().push(listener.clone());
        let lease = ListenerLease {
            hub: Arc::clone(self),
            region: handle.region,
            listener: listener.clone(),
            grid: Arc::clone(&handle.grid),
        };
        (
            ListenerHandle {
                listener,
                _lease: lease,
            },
            rx,
        )
    }

    /// Push one applied batch's tile changes — `(local tile index, final
    /// word)` pairs straight from `take_pending` — to every listener whose
    /// window contains them. Duplicates collapse to the last write. Called
    /// on the region worker after the grid write guard is released.
    pub fn fanout(&self, region: u32, changes: &[(u32, u16)], dict_version: u32) {
        let mut guard = self.regions.write();
        let Some(listeners) = guard.get_mut(&region) else {
            return;
        };
        if listeners.is_empty() {
            return;
        }

        // Collapse repeat writes to one tile, keeping the final word
        // (stable sort preserves write order within a tile).
        let mut dedup = changes.to_vec();
        dedup.sort_by_key(|&(idx, _)| idx);
        dedup.dedup_by(|later, kept| {
            if later.0 == kept.0 {
                *kept = *later;
                true
            } else {
                false
            }
        });

        let origin = region_origin(region as u16);
        let world: Vec<TileDelta> = dedup
            .iter()
            .map(|&(idx, word)| {
                let lx = (idx % REGION_SIDE as u32) as i32;
                let lz = (idx / REGION_SIDE as u32) as i32;
                (origin.x + lx, origin.z + lz, word)
            })
            .collect();

        let half = WINDOW_WIDTH / 2;
        let mut i = 0;
        while i < listeners.len() {
            let l = listeners[i].clone();
            let (ax, az) = l.anchor();
            let in_window = |&(x, z, _): &TileDelta| {
                let dx = x - (ax - half);
                let dz = z - (az - half);
                u32::try_from(dx).is_ok_and(|dx| dx < WINDOW_WIDTH as u32)
                    && u32::try_from(dz).is_ok_and(|dz| dz < WINDOW_WIDTH as u32)
            };
            let tiles: Vec<TileDelta> = world.iter().copied().filter(in_window).collect();
            if tiles.is_empty() {
                i += 1;
                continue;
            }
            let frame = WatchFrame::Delta {
                region,
                dict_version,
                tiles,
            };
            match l.tx.try_send(frame) {
                Ok(()) => i += 1,
                Err(TrySendError::Full(_)) => {
                    l.poison.store(true, Ordering::Relaxed);
                    listeners.swap_remove(i);
                }
                Err(TrySendError::Closed(_)) => {
                    listeners.swap_remove(i);
                }
            }
        }
    }

    /// Announce that a region's grid was wholesale-replaced (`"reseed"` on
    /// upstream disconnect) or freshly staged-in (`"live"` at seed
    /// completion), and re-apply change recording on the new grid while
    /// listeners remain.
    pub fn on_grid_replaced(&self, handle: &RoadsRegionHandle, reason: &'static str) {
        let region = handle.region;
        let mut guard = self.regions.write();
        let Some(listeners) = guard.get_mut(&region) else {
            return;
        };
        if listeners.is_empty() {
            return;
        }
        handle.grid.read().resource_map.set_watch(true);
        let mut dead: Vec<usize> = Vec::new();
        for (i, l) in listeners.iter().enumerate() {
            match l.tx.try_send(WatchFrame::Resync { region, reason }) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    l.poison.store(true, Ordering::Relaxed);
                    dead.push(i);
                }
                Err(TrySendError::Closed(_)) => dead.push(i),
            }
        }
        for i in dead.into_iter().rev() {
            listeners.swap_remove(i);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(region: u16) -> Arc<RoadsRegionHandle> {
        Arc::new(RoadsRegionHandle {
            region: u32::from(region),
            grid: Arc::new(RwLock::new(RoadsRegionGrid::new(region))),
        })
    }

    fn idx(x: i32, z: i32) -> u32 {
        (z as u32) * (REGION_SIDE as u32) + x as u32
    }

    #[test]
    fn fanout_filters_by_window_and_sets_flag() {
        let hub = Arc::new(ResourceWatchHub::new());
        let h = handle(1); // origin (0, 0): world == local
        assert!(!h.grid.read().resource_map.watch_enabled());
        let (l1, mut rx1) = hub.register(&h, 42, (1000, 1000));
        let (_l2, mut rx2) = hub.register(&h, 43, (5000, 5000));
        assert!(h.grid.read().resource_map.watch_enabled());

        // Tile (900, 950) is within ±200 of (1000, 1000) but far from
        // (5000, 5000); tile (4800, 5000) the reverse; (6000, 6000) in
        // neither.
        hub.fanout(1, &[(idx(900, 950), 7), (idx(4800, 5000), 9), (idx(6000, 6000), 11)], 5);
        let WatchFrame::Delta {
            region,
            dict_version,
            tiles,
        } = rx1.try_recv().unwrap()
        else {
            panic!("listener 1 must receive a delta");
        };
        assert_eq!((region, dict_version), (1, 5));
        assert_eq!(tiles, vec![(900, 950, 7)]);
        let WatchFrame::Delta { tiles, .. } = rx2.try_recv().unwrap() else {
            panic!("listener 2 must receive a delta");
        };
        assert_eq!(tiles, vec![(4800, 5000, 9)]);
        assert!(rx1.try_recv().is_err());
        assert!(rx2.try_recv().is_err());

        // Window edges: anchor±199 is the last in-window tile, anchor±200
        // the first outside one.
        l1.update_anchor(1000, 1000);
        hub.fanout(1, &[(idx(1200, 801), 1), (idx(1199, 801), 2), (idx(800, 1199), 3)], 5);
        let WatchFrame::Delta { tiles, .. } = rx1.try_recv().unwrap() else {
            panic!("edge delta expected");
        };
        assert_eq!(tiles, vec![(1199, 801, 2), (800, 1199, 3)]);

        // Dropping both leases clears the recording flag.
        drop(l1);
        assert!(hub.listener_count(1) == 1);
        drop(_l2);
        assert_eq!(hub.listener_count(1), 0);
        assert!(!h.grid.read().resource_map.watch_enabled());
    }

    #[test]
    fn fanout_dedups_to_last_write() {
        let hub = Arc::new(ResourceWatchHub::new());
        let h = handle(1);
        let (_l, mut rx) = hub.register(&h, 42, (100, 100));
        hub.fanout(1, &[(idx(50, 50), 1), (idx(50, 50), 2), (idx(60, 60), 3)], 0);
        let WatchFrame::Delta { tiles, .. } = rx.try_recv().unwrap() else {
            panic!("delta expected");
        };
        assert_eq!(tiles, vec![(50, 50, 2), (60, 60, 3)]);
    }

    #[test]
    fn full_queue_poisons_and_drops_listener() {
        let hub = Arc::new(ResourceWatchHub::new());
        let h = handle(1);
        let (l, mut rx) = hub.register(&h, 42, (100, 100));
        // Fill the queue without draining.
        for i in 0..LISTENER_QUEUE {
            hub.fanout(1, &[(idx(100, 100), i as u16)], 0);
        }
        assert_eq!(hub.listener_count(1), 1);
        // The next fan-out overflows: listener is removed and poisoned.
        hub.fanout(1, &[(idx(100, 100), 999)], 0);
        assert_eq!(hub.listener_count(1), 0);
        assert!(l.poisoned());
        // Queued frames remain readable (ordered), then the channel closes.
        for i in 0..LISTENER_QUEUE {
            let WatchFrame::Delta { tiles, .. } = rx.try_recv().unwrap() else {
                panic!("queued frame {i}");
            };
            assert_eq!(tiles, vec![(100, 100, i as u16)]);
        }
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn closed_receiver_drops_listener_without_poison() {
        let hub = Arc::new(ResourceWatchHub::new());
        let h = handle(1);
        let (l, rx) = hub.register(&h, 42, (100, 100));
        drop(rx);
        hub.fanout(1, &[(idx(100, 100), 1)], 0);
        assert_eq!(hub.listener_count(1), 0);
        assert!(!l.poisoned());
    }

    #[test]
    fn grid_replacement_resyncs_and_reapplies_flag() {
        let hub = Arc::new(ResourceWatchHub::new());
        let h = handle(1);
        let (_l, mut rx) = hub.register(&h, 42, (100, 100));
        hub.on_grid_replaced(&h, "reseed");
        let WatchFrame::Resync { region, reason } = rx.try_recv().unwrap() else {
            panic!("resync expected");
        };
        assert_eq!((region, reason), (1, "reseed"));

        // A replaced grid object defaults to recording off; listeners
        // present → on_grid_replaced re-enables it.
        let fresh = handle(1);
        assert!(!fresh.grid.read().resource_map.watch_enabled());
        hub.on_grid_replaced(&fresh, "live");
        assert!(fresh.grid.read().resource_map.watch_enabled());
        let WatchFrame::Resync { reason, .. } = rx.try_recv().unwrap() else {
            panic!("second resync expected");
        };
        assert_eq!(reason, "live");
    }
}
