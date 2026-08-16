//! In-process observers of decoded upstream batches — the seam the embedded
//! BitCraft relay-cache consumes (`--bitcraft-cache`).
//!
//! Dispatch contract, per database, strictly FIFO at every observer:
//!
//! 1. `on_updates` — one decoded batch (seed rows and/or live updates) in
//!    arrival order. Dispatched (and awaited) inside the mirror's applier, so
//!    a slow observer applies backpressure to the session exactly like a slow
//!    consumer did in the split-process architecture.
//! 2. `on_live` — every seed update of this generation has been dispatched
//!    (after `await_all_seeds_applied`, before the mirror accepts clients).
//!    Lets an observer that staged the snapshot publish it even if the region
//!    has no live traffic yet.
//! 3. `on_reset` — the session died; observers must drop derived state. It
//!    carries the **next** generation: every later dispatch for this database
//!    carries that generation (or greater, after another reset), so observers
//!    can discard stale in-flight batches that were sent concurrently with the
//!    reset.
//!
//! Generations start at 1 for the first session and increase by one per
//! reconnect cycle.

use std::collections::HashMap;
use std::future::ready;
use std::sync::{Arc, RwLock};

use futures::future::{BoxFuture, FutureExt};

use crate::upstream::UpstreamUpdate;

pub type ObserverFuture = BoxFuture<'static, anyhow::Result<()>>;

pub trait MirrorObserver: Send + Sync + 'static {
    /// A batch of decoded updates (seeds and/or live) for one database.
    fn on_updates(&self, database: Arc<str>, generation: u64, updates: Vec<UpstreamUpdate>) -> ObserverFuture;

    /// All seed updates for this generation have been dispatched; going live.
    fn on_live(&self, _database: Arc<str>, _generation: u64) -> ObserverFuture {
        ready(Ok(())).boxed()
    }

    /// Session torn down; drop derived state. `generation` is the one the
    /// **next** session will dispatch with.
    fn on_reset(&self, _database: Arc<str>, _generation: u64) -> ObserverFuture {
        ready(Ok(())).boxed()
    }
}

/// Registry of per-database observers, consulted by the mirror runtime.
///
/// Observers must be registered before the database's upstream loop is
/// spawned so no dispatch is missed. Unregistered databases dispatch to
/// nobody (zero overhead).
#[derive(Clone, Default)]
pub struct MirrorObserverRegistry {
    by_database: Arc<RwLock<HashMap<Arc<str>, Vec<Arc<dyn MirrorObserver>>>>>,
}

impl MirrorObserverRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, database: &str, observer: Arc<dyn MirrorObserver>) {
        let key: Arc<str> = Arc::from(database);
        self.inner_mut().entry(key).or_default().push(observer);
    }

    fn observers_for(&self, database: &str) -> Vec<Arc<dyn MirrorObserver>> {
        self.by_database
            .read()
            .expect("mirror observer registry poisoned")
            .get(database)
            .cloned()
            .unwrap_or_default()
    }

    fn inner_mut(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<Arc<str>, Vec<Arc<dyn MirrorObserver>>>> {
        self.by_database.write().expect("mirror observer registry poisoned")
    }

    pub async fn dispatch_updates(
        &self,
        database: &str,
        generation: u64,
        updates: Vec<UpstreamUpdate>,
    ) -> anyhow::Result<()> {
        for observer in self.observers_for(database) {
            observer
                .on_updates(Arc::from(database), generation, updates.clone())
                .await?;
        }
        Ok(())
    }

    pub async fn dispatch_live(&self, database: &str, generation: u64) -> anyhow::Result<()> {
        for observer in self.observers_for(database) {
            observer.on_live(Arc::from(database), generation).await?;
        }
        Ok(())
    }

    pub async fn dispatch_reset(&self, database: &str, next_generation: u64) -> anyhow::Result<()> {
        for observer in self.observers_for(database) {
            observer.on_reset(Arc::from(database), next_generation).await?;
        }
        Ok(())
    }
}
