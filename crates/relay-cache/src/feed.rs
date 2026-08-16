// SPDX-License-Identifier: MIT

//! In-process region feed for the embedded mode (`--bitcraft-cache`).
//!
//! The BitCraft-tuned mirror (`spacetimedb-bitcraft-mirror`) dispatches every
//! decoded upstream batch to a [`MirrorObserver`] before applying it to its
//! relational store (see `crates/public-mirror/src/observer.rs`). This module
//! turns those dispatches back into the same store/interest operations the
//! WebSocket-mode shard loop performs — without a WebSocket hop, without
//! re-encoding the subscription stream, and without a second subscription
//! evaluation:
//!
//! - seed batches (and live updates interleaved among them — a region with
//!   ambient traffic always has some) build a staging [`RegionStore`]
//!   off-lock; the `on_live` dispatch — which the mirror sends strictly
//!   after every seed has been dispatched, before it accepts clients — swaps
//!   it in under the write lock, marks it ready, and re-indexes the interest
//!   members. `on_live` is the **only** finalize trigger: a live batch while
//!   still seeding is an interleaved post-snapshot update, not a go-live
//!   signal;
//! - live batches take the write lock per batch and run the same
//!   `apply_rows` path (decode → touch hooks → columnar upsert) with the
//!   same `TouchBatch` semantics for the dim-buildings WS;
//! - `on_reset` (mirror reconnect) clears the store; the dispatch generation
//!   discards any stale in-flight batches from the dead session.
//!
//! Two BitCraft-specific simplifications fall out of consuming the mirror's
//! full-table batches:
//!
//! - the per-hexite `location_state` second subscription set is gone — the
//!   feed sees every location row. Because the tables seed alphabetically
//!   (location_state before resource_state), a deposit's location row can
//!   stream past before its resource row; the `HexiteIndex` (claim name ×
//!   claim_local coords, both seeded earlier) lets the location arm stash
//!   those rows for the resource arm to consume at upsert — order-safe both
//!   ways, with no follow-up subscription;
//! - the hexite-integrity reconnect class is gone for the same reason (the
//!   location rows cannot be missing from the batch stream).

use std::collections::HashMap;
use std::future::ready;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use futures_util::FutureExt;
use parking_lot::Mutex;
use relay_protocol::{parse_schema, MirroredSchema};
use spacetimedb_public_mirror_client::observer::{MirrorObserver, ObserverFuture};
use spacetimedb_public_mirror_client::upstream::UpstreamUpdate;
use tokio::sync::mpsc;

use crate::interest::{InterestHub, TouchBatch};
use crate::shard::{apply_rows, ShardHandle, TableMeta};
use crate::store::RegionStore;

/// Bound on queued batches per region. A full channel blocks the sender —
/// which is the mirror's applier — reproducing the TCP backpressure the
/// split-process cache had, with the mirror's own 1 GiB live-queue cap still
/// bounding the failure mode.
const FEED_CHANNEL_CAPACITY: usize = 64;

const DATABASE_PREFIX: &str = "bitcraft-live-";
const DATABASE_GLOBAL: &str = "bitcraft-live-global";

/// Message to a region's feed worker. Carries the mirror session generation
/// so batches from a dead session (sent concurrently with its reset) are
/// discarded.
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

struct RegionFeed {
    region: u32,
    database: Arc<str>,
    schema: Arc<MirroredSchema>,
    meta: Arc<TableMeta>,
    interest: Arc<InterestHub>,
    handle: Arc<ShardHandle>,
}

/// Owns one [`RegionFeed`] worker per registered database and implements
/// [`MirrorObserver`] for all of them. Created once by the mirror binary
/// when `--bitcraft-cache` is enabled; mirrors register themselves via
/// [`FeedManager::register_region`] during bootstrap.
pub struct FeedManager {
    interest: Arc<InterestHub>,
    feeds: Mutex<HashMap<Arc<str>, mpsc::Sender<FeedMsg>>>,
    shards: Mutex<Vec<Arc<ShardHandle>>>,
}

impl FeedManager {
    pub fn new(interest: Arc<InterestHub>) -> Arc<Self> {
        Arc::new(Self {
            interest,
            feeds: Mutex::new(HashMap::new()),
            shards: Mutex::new(Vec::new()),
        })
    }

    /// Register one mirrored database and spawn its feed worker.
    ///
    /// `schema_json` is the raw `/v1/database/<db>/schema?version=9` body the
    /// mirror itself fetched and retained, so the feed's decode metadata can
    /// never drift from the mirror's own tables (and no schema HTTP fetch
    /// happens at all).
    ///
    /// `bitcraft-live-global` is skipped — it carries none of the regional
    /// tables the cache serves (same rule as WS-mode discovery).
    ///
    /// Returns the [`ShardHandle`] HTTP queries read through, or `None` for
    /// a skipped database.
    pub fn register_region(&self, database: &str, schema_json: &[u8]) -> Result<Option<Arc<ShardHandle>>> {
        if database == DATABASE_GLOBAL {
            tracing::debug!(
                target: "relay_cache::feed",
                database,
                "global database carries no regional tables; no cache feed"
            );
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
            store: Arc::new(parking_lot::RwLock::new(RegionStore::empty(region))),
        });

        let (tx, rx) = mpsc::channel::<FeedMsg>(FEED_CHANNEL_CAPACITY);
        self.feeds.lock().insert(Arc::from(database), tx);
        self.shards.lock().push(handle.clone());

        let feed = Arc::new(RegionFeed {
            region,
            database: Arc::from(database),
            schema,
            meta,
            interest: self.interest.clone(),
            handle: handle.clone(),
        });
        tokio::spawn(run_worker(feed, rx));
        tracing::info!(
            target: "relay_cache::feed",
            database,
            region,
            "embedded feed registered"
        );
        Ok(Some(handle))
    }

    /// Shard handles in registration order, for assembling the serving
    /// [`crate::serve::Fleet`].
    pub fn shard_handles(&self) -> Vec<Arc<ShardHandle>> {
        self.shards.lock().clone()
    }

    fn sender(&self, database: &str) -> Option<mpsc::Sender<FeedMsg>> {
        self.feeds.lock().get(database).cloned()
    }
}

impl MirrorObserver for FeedManager {
    fn on_updates(&self, database: Arc<str>, generation: u64, updates: Vec<UpstreamUpdate>) -> ObserverFuture {
        match self.sender(&database) {
            Some(tx) => async move {
                tx.send(FeedMsg::Updates { generation, updates })
                    .await
                    .map_err(|_| anyhow!("feed worker for `{database}` is gone"))?;
                Ok(())
            }
            .boxed(),
            // Not a cached regional database (e.g. bitcraft-live-global).
            None => ready(Ok(())).boxed(),
        }
    }

    fn on_live(&self, database: Arc<str>, generation: u64) -> ObserverFuture {
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
    /// Accumulating seed rows (plus interleaved live updates) into a fresh
    /// store, not yet serving. Only `on_live` promotes it.
    Seeding(Box<RegionStore>),
    /// Staging swapped in; live updates apply under the write lock.
    Live,
}

async fn run_worker(feed: Arc<RegionFeed>, mut rx: mpsc::Receiver<FeedMsg>) {
    let region = feed.region;
    // The mirror's first session dispatches with generation 1
    // (see `public-mirror` observer docs); resets adopt the next one.
    let mut generation: u64 = 1;
    let mut phase = Phase::Seeding(Box::new(RegionStore::empty(region)));

    while let Some(msg) = rx.recv().await {
        match msg {
            FeedMsg::Reset { generation: next } => {
                if next < generation {
                    tracing::warn!(
                        target: "relay_cache::feed",
                        region,
                        next,
                        generation,
                        "reset generation went backwards; ignoring"
                    );
                    continue;
                }
                generation = next;
                phase = Phase::Seeding(Box::new(RegionStore::empty(region)));
                *feed.handle.store.write() = RegionStore::empty(region);
                tracing::info!(
                    target: "relay_cache::feed",
                    region,
                    generation,
                    "feed reset; store cleared (mirror reconnecting)"
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
            }
            FeedMsg::Updates {
                generation: gen,
                updates,
            } => {
                if gen != generation {
                    tracing::debug!(
                        target: "relay_cache::feed",
                        region,
                        gen,
                        generation,
                        "dropping stale-generation batch"
                    );
                    continue;
                }
                for update in updates {
                    if update.is_seed {
                        if !matches!(phase, Phase::Seeding(_)) {
                            tracing::warn!(
                                target: "relay_cache::feed",
                                region,
                                "seed batch after live without a reset; restarting staging"
                            );
                            phase = Phase::Seeding(Box::new(RegionStore::empty(region)));
                        }
                        let Phase::Seeding(staging) = &mut phase else {
                            unreachable!("restarted above");
                        };
                        if let Err(e) = apply_update(&feed, staging, &update) {
                            tracing::error!(
                                target: "relay_cache::feed",
                                region,
                                error = %e,
                                "seed batch apply failed; snapshot may be incomplete"
                            );
                        }
                    } else if let Phase::Seeding(staging) = &mut phase {
                        // Interleaved live update while seeds still apply (a
                        // region with ambient traffic always has some). The
                        // applier is FIFO, so this update lands after its
                        // table's seed; apply it into the staging store —
                        // exactly what WS-mode's `expect_subscribe_applied`
                        // forwarding does with pre-Applied transactions. Only
                        // `on_live` finalizes.
                        if let Err(e) = apply_update(&feed, staging, &update) {
                            tracing::error!(
                                target: "relay_cache::feed",
                                region,
                                error = %e,
                                "interleaved live batch apply failed during seeding"
                            );
                        }
                    } else {
                        apply_live_update(&feed, &update);
                    }
                }
            }
        }
    }
    tracing::info!(
        target: "relay_cache::feed",
        region,
        "feed worker exiting (channel closed; process shutting down)"
    );
}

/// Publish the staged snapshot: swap under the write lock, mark ready, and
/// re-index interest members — mirroring `shard.rs`'s bulk-load completion.
fn finalize(feed: &RegionFeed, staging: Box<RegionStore>) {
    let mut staging = *staging;
    let region = feed.region;
    if staging.claim.len() == 0 {
        // The mirror only goes live after full seeds, so an empty claim table
        // means the upstream region is genuinely empty (or severe drift).
        // WS-mode refused readiness here; the mirror's own live gate is the
        // authority now, so serve it — but say so loudly.
        tracing::warn!(
            target: "relay_cache::feed",
            region,
            "seed snapshot has 0 claims"
        );
    }
    staging.ready = true;
    let n_resource = staging.resource.len();
    let n_claim = staging.claim.len();
    let n_growth = staging.growth.len();
    {
        let mut guard = feed.handle.store.write();
        let old_pairs = guard.claim_member.player_claim_pairs();
        *guard = staging;
        let new_pairs = guard.claim_member.player_claim_pairs();
        feed.interest.replace_region_members(&old_pairs, &new_pairs);
    }
    tracing::info!(
        target: "relay_cache::feed",
        region,
        database = %feed.database,
        n_resource,
        n_claim,
        n_growth,
        "seed complete; store swapped; region ready"
    );
}

/// Seed batches: no interest touches (nothing served until the swap), same
/// as the WS-mode bulk load.
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

/// Live batches: write lock per batch, touch hooks when the dim-buildings WS
/// has subscribers — the same shape as the WS-mode `apply_transaction`.
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
                "live batch apply failed; store may lag until the rows change again"
            );
        }
    }
    if let Some(batch) = touches {
        batch.flush();
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
