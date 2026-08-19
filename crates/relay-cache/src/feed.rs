// SPDX-License-Identifier: MIT

//! In-process region feed for the embedded mode (`--bitcraft-cache`).
//!
//! When `--roads-cache` is also enabled, a parallel dense [`RoadsRegionGrid`]
//! is maintained per region and a [`GlobalRoadsCatalog`] from
//! `bitcraft-live-global`.

use std::collections::HashMap;
use std::future::ready;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use futures_util::FutureExt;
use parking_lot::{Mutex, RwLock};
use relay_protocol::{parse_schema, MirroredSchema};
use spacetimedb_public_mirror_client::observer::{MirrorObserver, ObserverFuture};
use spacetimedb_public_mirror_client::upstream::UpstreamUpdate;
use tokio::sync::mpsc;

use crate::interest::{InterestHub, TouchBatch};
use crate::roads::apply::{apply_roads_rows, finalize_terrain_seed};
use crate::roads::catalog::{apply_global_delete, apply_global_insert, GlobalRoadsCatalog, RoadsFleet};
use crate::roads::meta::RoadsTableMeta;
use crate::roads::store::{RoadsRegionGrid, RoadsRegionHandle};
use crate::shard::{apply_rows, ShardHandle, TableMeta};
use crate::store::RegionStore;

const FEED_CHANNEL_CAPACITY: usize = 64;

const DATABASE_PREFIX: &str = "bitcraft-live-";
const DATABASE_GLOBAL: &str = "bitcraft-live-global";

enum FeedMsg {
    Updates {
        generation: u64,
        updates: Vec<UpstreamUpdate>,
    },
    Live {
        generation: u64,
    },
    Reset {
        generation: u64,
    },
}

enum GlobalFeedMsg {
    Updates {
        generation: u64,
        updates: Vec<UpstreamUpdate>,
    },
    Live {
        generation: u64,
    },
    Reset {
        generation: u64,
    },
}

struct RegionFeed {
    region: u32,
    database: Arc<str>,
    schema: Arc<MirroredSchema>,
    meta: Arc<TableMeta>,
    roads_meta: Option<Arc<RoadsTableMeta>>,
    interest: Arc<InterestHub>,
    handle: Arc<ShardHandle>,
    roads: Option<Arc<RoadsRegionHandle>>,
}

struct GlobalFeed {
    database: Arc<str>,
    schema: Arc<MirroredSchema>,
    meta: Arc<RoadsTableMeta>,
    catalog: Arc<RwLock<GlobalRoadsCatalog>>,
}

/// Owns one [`RegionFeed`] worker per registered database and implements
/// [`MirrorObserver`] for all of them.
pub struct FeedManager {
    interest: Arc<InterestHub>,
    feeds: Mutex<HashMap<Arc<str>, mpsc::Sender<FeedMsg>>>,
    global_tx: Mutex<Option<mpsc::Sender<GlobalFeedMsg>>>,
    shards: Mutex<Vec<Arc<ShardHandle>>>,
    roads_fleet: Mutex<Option<Arc<RoadsFleet>>>,
}

impl FeedManager {
    pub fn new(interest: Arc<InterestHub>) -> Arc<Self> {
        Arc::new(Self {
            interest,
            feeds: Mutex::new(HashMap::new()),
            global_tx: Mutex::new(None),
            shards: Mutex::new(Vec::new()),
            roads_fleet: Mutex::new(None),
        })
    }

    /// Enable dense roads grids (~289 MiB/region) and global recipe catalogs.
    pub fn enable_roads(self: &Arc<Self>) -> Arc<RoadsFleet> {
        let catalog = Arc::new(RwLock::new(GlobalRoadsCatalog::new()));
        let fleet = Arc::new(RoadsFleet::new(catalog));
        *self.roads_fleet.lock() = Some(fleet.clone());
        fleet
    }

    pub fn roads_fleet(&self) -> Option<Arc<RoadsFleet>> {
        self.roads_fleet.lock().clone()
    }

    pub fn roads_enabled(&self) -> bool {
        self.roads_fleet.lock().is_some()
    }

    pub fn register_region(&self, database: &str, schema_json: &[u8]) -> Result<Option<Arc<ShardHandle>>> {
        if database == DATABASE_GLOBAL {
            if self.roads_enabled() {
                self.register_global(database, schema_json)?;
            } else {
                tracing::debug!(
                    target: "relay_cache::feed",
                    database,
                    "global database carries no regional tables; no cache feed"
                );
            }
            return Ok(None);
        }
        let region: u32 = database
            .strip_prefix(DATABASE_PREFIX)
            .and_then(|suffix| suffix.parse().ok())
            .with_context(|| format!("`{database}` is not `{DATABASE_PREFIX}<N>` or {DATABASE_GLOBAL}"))?;

        let schema = Arc::new(
            parse_schema(schema_json).with_context(|| format!("parse schema for embedded feed `{database}`"))?,
        );
        let meta = Arc::new(TableMeta::from_schema(&schema)?);
        let handle = Arc::new(ShardHandle {
            region,
            store: Arc::new(RwLock::new(RegionStore::empty(region))),
        });

        let roads_meta = if self.roads_enabled() {
            Some(Arc::new(RoadsTableMeta::from_schema_regional(&schema)?))
        } else {
            None
        };

        let roads_handle = if self.roads_enabled() {
            let rh = Arc::new(RoadsRegionHandle {
                region,
                grid: Arc::new(RwLock::new(RoadsRegionGrid::new(region as u16))),
            });
            if let Some(fleet) = self.roads_fleet.lock().as_ref() {
                fleet.push_region(rh.clone());
            }
            Some(rh)
        } else {
            None
        };

        let (tx, rx) = mpsc::channel::<FeedMsg>(FEED_CHANNEL_CAPACITY);
        self.feeds.lock().insert(Arc::from(database), tx);
        self.shards.lock().push(handle.clone());

        let feed = Arc::new(RegionFeed {
            region,
            database: Arc::from(database),
            schema,
            meta,
            roads_meta,
            interest: self.interest.clone(),
            handle: handle.clone(),
            roads: roads_handle,
        });
        tokio::spawn(run_worker(feed, rx));
        tracing::info!(
            target: "relay_cache::feed",
            database,
            region,
            roads = self.roads_enabled(),
            "embedded feed registered"
        );
        Ok(Some(handle))
    }

    fn register_global(&self, database: &str, schema_json: &[u8]) -> Result<()> {
        let schema = Arc::new(
            parse_schema(schema_json).with_context(|| format!("parse global schema for `{database}`"))?,
        );
        let meta = Arc::new(RoadsTableMeta::from_schema_global(&schema)?);
        let catalog = self
            .roads_fleet
            .lock()
            .as_ref()
            .map(|f| f.catalog.clone())
            .ok_or_else(|| anyhow!("roads fleet missing during global register"))?;

        let (tx, rx) = mpsc::channel::<GlobalFeedMsg>(FEED_CHANNEL_CAPACITY);
        *self.global_tx.lock() = Some(tx);

        let feed = Arc::new(GlobalFeed {
            database: Arc::from(database),
            schema,
            meta,
            catalog,
        });
        tokio::spawn(run_global_worker(feed, rx));
        tracing::info!(
            target: "relay_cache::feed",
            database,
            "global roads catalog feed registered"
        );
        Ok(())
    }

    pub fn shard_handles(&self) -> Vec<Arc<ShardHandle>> {
        self.shards.lock().clone()
    }

    fn sender(&self, database: &str) -> Option<mpsc::Sender<FeedMsg>> {
        self.feeds.lock().get(database).cloned()
    }

    fn global_sender(&self) -> Option<mpsc::Sender<GlobalFeedMsg>> {
        self.global_tx.lock().clone()
    }
}

impl MirrorObserver for FeedManager {
    fn on_updates(&self, database: Arc<str>, generation: u64, updates: Vec<UpstreamUpdate>) -> ObserverFuture {
        if database.as_ref() == DATABASE_GLOBAL {
            if let Some(tx) = self.global_sender() {
                return async move {
                    tx.send(GlobalFeedMsg::Updates { generation, updates })
                        .await
                        .map_err(|_| anyhow!("global feed worker is gone"))?;
                    Ok(())
                }
                .boxed();
            }
            return ready(Ok(())).boxed();
        }
        match self.sender(&database) {
            Some(tx) => async move {
                tx.send(FeedMsg::Updates { generation, updates })
                    .await
                    .map_err(|_| anyhow!("feed worker for `{database}` is gone"))?;
                Ok(())
            }
            .boxed(),
            None => ready(Ok(())).boxed(),
        }
    }

    fn on_live(&self, database: Arc<str>, generation: u64) -> ObserverFuture {
        if database.as_ref() == DATABASE_GLOBAL {
            if let Some(tx) = self.global_sender() {
                return async move {
                    tx.send(GlobalFeedMsg::Live { generation })
                        .await
                        .map_err(|_| anyhow!("global feed worker is gone"))?;
                    Ok(())
                }
                .boxed();
            }
            return ready(Ok(())).boxed();
        }
        match self.sender(&database) {
            Some(tx) => async move {
                tx.send(FeedMsg::Live { generation })
                    .await
                    .map_err(|_| anyhow!("feed worker for `{database}` is gone"))?;
                Ok(())
            }
            .boxed(),
            None => ready(Ok(())).boxed(),
        }
    }

    fn on_reset(&self, database: Arc<str>, generation: u64) -> ObserverFuture {
        if database.as_ref() == DATABASE_GLOBAL {
            if let Some(tx) = self.global_sender() {
                return async move {
                    tx.send(GlobalFeedMsg::Reset { generation })
                        .await
                        .map_err(|_| anyhow!("global feed worker is gone"))?;
                    Ok(())
                }
                .boxed();
            }
            return ready(Ok(())).boxed();
        }
        match self.sender(&database) {
            Some(tx) => async move {
                tx.send(FeedMsg::Reset { generation })
                    .await
                    .map_err(|_| anyhow!("feed worker for `{database}` is gone"))?;
                Ok(())
            }
            .boxed(),
            None => ready(Ok(())).boxed(),
        }
    }
}

enum Phase {
    Seeding(Box<RegionStore>),
    Live,
}

enum RoadsPhase {
    Seeding(Box<RoadsRegionGrid>),
    Live,
}

async fn run_worker(feed: Arc<RegionFeed>, mut rx: mpsc::Receiver<FeedMsg>) {
    let region = feed.region;
    let mut generation: u64 = 1;
    let mut phase = Phase::Seeding(Box::new(RegionStore::empty(region)));
    let mut roads_phase = feed.roads.as_ref().map(|_| {
        RoadsPhase::Seeding(Box::new(RoadsRegionGrid::new(region as u16)))
    });

    while let Some(msg) = rx.recv().await {
        match msg {
            FeedMsg::Reset { generation: next } => {
                if next < generation {
                    continue;
                }
                generation = next;
                phase = Phase::Seeding(Box::new(RegionStore::empty(region)));
                *feed.handle.store.write() = RegionStore::empty(region);
                if let Some(rh) = &feed.roads {
                    roads_phase = Some(RoadsPhase::Seeding(Box::new(RoadsRegionGrid::new(region as u16))));
                    *rh.grid.write() = RoadsRegionGrid::new(region as u16);
                }
                tracing::info!(
                    target: "relay_cache::feed",
                    region,
                    generation,
                    "feed reset; store cleared"
                );
            }
            FeedMsg::Live { generation: gen } => {
                if gen != generation {
                    continue;
                }
                if let Phase::Seeding(staging) = phase {
                    finalize(&feed, staging);
                    phase = Phase::Live;
                }
                if let (Some(rh), Some(RoadsPhase::Seeding(staging))) = (&feed.roads, roads_phase.take()) {
                    finalize_roads(&feed, rh, staging);
                    roads_phase = Some(RoadsPhase::Live);
                }
            }
            FeedMsg::Updates {
                generation: gen,
                updates,
            } => {
                if gen != generation {
                    continue;
                }
                for update in updates {
                    if update.is_seed {
                        if !matches!(phase, Phase::Seeding(_)) {
                            phase = Phase::Seeding(Box::new(RegionStore::empty(region)));
                        }
                        let Phase::Seeding(staging) = &mut phase else {
                            unreachable!();
                        };
                        let _ = apply_update(&feed, staging, &update);
                        if let (Some(meta), Some(RoadsPhase::Seeding(roads_staging))) =
                            (&feed.roads_meta, roads_phase.as_mut())
                        {
                            let _ = apply_roads_update(roads_staging, &feed.schema, meta, &update);
                        }
                    } else if let Phase::Seeding(staging) = &mut phase {
                        let _ = apply_update(&feed, staging, &update);
                        if let (Some(meta), Some(RoadsPhase::Seeding(roads_staging))) =
                            (&feed.roads_meta, roads_phase.as_mut())
                        {
                            let _ = apply_roads_update(roads_staging, &feed.schema, meta, &update);
                        }
                    } else {
                        apply_live_update(&feed, &update);
                        if let (Some(rh), Some(meta), Some(RoadsPhase::Live)) =
                            (&feed.roads, &feed.roads_meta, roads_phase.as_ref())
                        {
                            apply_roads_live(rh, &feed.schema, meta, &update);
                        }
                    }
                }
            }
        }
    }
}

async fn run_global_worker(feed: Arc<GlobalFeed>, mut rx: mpsc::Receiver<GlobalFeedMsg>) {
    let mut generation: u64 = 1;
    let mut seeding = true;

    while let Some(msg) = rx.recv().await {
        match msg {
            GlobalFeedMsg::Reset { generation: next } => {
                if next >= generation {
                    generation = next;
                    seeding = true;
                    *feed.catalog.write() = GlobalRoadsCatalog::new();
                }
            }
            GlobalFeedMsg::Live { generation: gen } if gen == generation => {
                feed.catalog.write().mark_ready();
                seeding = false;
            }
            GlobalFeedMsg::Updates { generation: gen, updates } if gen == generation => {
                for update in updates {
                    let mut catalog = feed.catalog.write();
                    for table in &update.tables {
                        for row in &table.delete_bytes {
                            let _ = apply_global_delete(&mut catalog, &feed.meta, &feed.schema, &table.table_name, row);
                        }
                        for row in &table.inserts {
                            let _ = apply_global_insert(
                                &mut catalog,
                                &feed.meta,
                                &feed.schema,
                                &table.table_name,
                                row,
                            );
                        }
                    }
                    drop(catalog);
                    if seeding {
                        continue;
                    }
                }
            }
            _ => {}
        }
    }
}

fn finalize(feed: &RegionFeed, staging: Box<RegionStore>) {
    let mut staging = *staging;
    staging.ready = true;
    {
        let mut guard = feed.handle.store.write();
        let old_pairs = guard.claim_member.player_claim_pairs();
        *guard = staging;
        let new_pairs = guard.claim_member.player_claim_pairs();
        feed.interest.replace_region_members(&old_pairs, &new_pairs);
    }
    tracing::info!(
        target: "relay_cache::feed",
        region = feed.region,
        database = %feed.database,
        "columnar seed complete; region ready"
    );
}

fn finalize_roads(feed: &RegionFeed, handle: &RoadsRegionHandle, staging: Box<RoadsRegionGrid>) {
    let mut staging = *staging;
    finalize_terrain_seed(&mut staging);
    staging.mark_ready();
    *handle.grid.write() = staging;
    tracing::info!(
        target: "relay_cache::feed",
        region = feed.region,
        database = %feed.database,
        "roads seed complete; region ready"
    );
}

fn apply_update(feed: &RegionFeed, store: &mut RegionStore, update: &UpstreamUpdate) -> Result<()> {
    for table in &update.tables {
        apply_rows(
            store,
            &feed.schema,
            &feed.meta,
            &table.table_name,
            &table.delete_bytes,
            &table.inserts,
            None,
            None,
        )?;
    }
    Ok(())
}

fn apply_roads_update(
    grid: &mut RoadsRegionGrid,
    schema: &MirroredSchema,
    meta: &RoadsTableMeta,
    update: &UpstreamUpdate,
) -> Result<()> {
    for table in &update.tables {
        apply_roads_rows(
            grid,
            schema,
            meta,
            &table.table_name,
            &table.delete_bytes,
            &table.inserts,
        )?;
    }
    Ok(())
}

fn apply_live_update(feed: &RegionFeed, update: &UpstreamUpdate) {
    let interest = feed.interest.clone();
    let mut touches = interest.has_subscribers().then(|| TouchBatch::new(&interest));
    {
        let mut guard = feed.handle.store.write();
        let result = apply_rows_into(&feed, &mut guard, update, Some(interest.as_ref()), touches.as_mut());
        if let Err(e) = result {
            tracing::error!(
                target: "relay_cache::feed",
                region = feed.region,
                error = %e,
                "live batch apply failed"
            );
        }
    }
    if let Some(batch) = touches {
        batch.flush();
    }
}

fn apply_roads_live(
    handle: &RoadsRegionHandle,
    schema: &MirroredSchema,
    meta: &RoadsTableMeta,
    update: &UpstreamUpdate,
) {
    let mut guard = handle.grid.write();
    if let Err(e) = apply_roads_update(&mut guard, schema, meta, update) {
        tracing::error!(
            target: "relay_cache::feed",
            region = handle.region,
            error = %e,
            "roads live batch apply failed"
        );
    }
}

fn apply_rows_into(
    feed: &RegionFeed,
    store: &mut RegionStore,
    update: &UpstreamUpdate,
    interest: Option<&InterestHub>,
    mut touches: Option<&mut TouchBatch>,
) -> Result<()> {
    for table in &update.tables {
        apply_rows(
            store,
            &feed.schema,
            &feed.meta,
            &table.table_name,
            &table.delete_bytes,
            &table.inserts,
            interest,
            touches.as_deref_mut(),
        )?;
    }
    Ok(())
}
