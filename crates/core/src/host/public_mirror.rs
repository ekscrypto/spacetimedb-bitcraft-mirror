//! Host-side helpers for `--public-mirror-v1`.
//!
//! Applies externally observed row ops into an in-memory [`RelationalDB`] and
//! broadcasts [`ModuleEvent`]s that preserve upstream reducer provenance.

use super::module_host::{
    create_table_from_view_def, DatabaseUpdate, EventStatus, ModuleEvent, ModuleFunctionCall,
};
use super::ArgsTuple;
use crate::db::relational_db::RelationalDB;
use crate::error::DBError;
use crate::subscription::module_subscription_actor::{commit_and_broadcast_event, ModuleSubscriptions};
use bytes::Bytes;
use spacetimedb_client_api_messages::energy::FunctionBudget;
use spacetimedb_datastore::execution_context::Workload;
use spacetimedb_datastore::locking_tx_datastore::MutTxId;
use spacetimedb_datastore::system_tables::ModuleKind;
use spacetimedb_datastore::traits::{IsolationLevel, Program};
use spacetimedb_execution::dml::MutDatastore;
use spacetimedb_lib::{ConnectionId, Identity, ProductValue, Timestamp};
use spacetimedb_primitives::TableId;
use spacetimedb_schema::def::ModuleDef;
use spacetimedb_schema::identifier::Identifier;
use spacetimedb_schema::reducer_name::ReducerName;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Upstream reducer call-stack / provenance to attach to a mirrored update.
pub struct ExternalProvenance {
    pub reducer_name: String,
    pub caller_identity: Identity,
    pub caller_connection_id: ConnectionId,
    pub timestamp: Timestamp,
    pub request_id: u32,
    pub args: Bytes, // upstream reducer args blob
}

/// Row ops for a single table within an external update.
pub struct TableOps {
    pub table_id: TableId,
    pub deletes: Vec<ProductValue>,
    pub inserts: Vec<Bytes>, // BSATN row bytes for `RelationalDB::insert`
}

/// One externally observed update ready to apply into the mirror.
///
/// A batch of these is applied in one executor job (each update in its own
/// transaction + broadcast) by [`super::module_host::ModuleHost::apply_mirrored_updates`].
pub struct MirroredUpdate {
    pub provenance: Option<ExternalProvenance>,
    pub ops: Vec<TableOps>,
    pub is_seed: bool,
}

/// Shared counters updated during a large seed insert (for `/v1/mirrors`).
#[derive(Clone)]
pub struct SeedApplyProgress {
    pub rows_applied: Arc<AtomicU64>,
    pub last_apply_unix_ms: Arc<AtomicU64>,
}

impl SeedApplyProgress {
    fn record(&self, total_applied: u64) {
        self.rows_applied.store(total_applied, Ordering::Relaxed);
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        self.last_apply_unix_ms.store(ms, Ordering::Relaxed);
    }
}

/// Rows committed per transaction during seed apply. Matches
/// relay-mirror-driver's `max_rows_per_apply`: a multi-million-row table like
/// `location_state` in one transaction keeps a giant in-memory tx state and
/// defers all index work until a multi-minute final squash.
const SEED_CHUNK_ROWS: usize = 4096;

/// Yield / report periodically so a multi-million-row seed insert yields the
/// dedicated JobCores thread under CPU contention, and so `/v1/mirrors`
/// can show forward progress during `applying_seed`.
const PROGRESS_EVERY: u64 = 50_000;

fn mirror_event(provenance: &Option<ExternalProvenance>) -> ModuleEvent {
    let (timestamp, caller_identity, caller_connection_id, request_id, function_call) = match provenance {
        Some(p) => {
            let reducer = match Identifier::new(p.reducer_name.clone().into()) {
                Ok(id) => ReducerName::new(id),
                Err(_) => ReducerName::new(Identifier::new_assume_valid(p.reducer_name.clone().into())),
            };
            (
                p.timestamp,
                p.caller_identity,
                Some(p.caller_connection_id),
                Some(p.request_id),
                ModuleFunctionCall {
                    reducer: Some(reducer),
                    reducer_id: u32::MAX.into(),
                    args: ArgsTuple::from_bsatn_unchecked(p.args.clone()),
                },
            )
        }
        None => (
            Timestamp::now(),
            Identity::ZERO,
            None,
            None,
            ModuleFunctionCall::update(),
        ),
    };

    ModuleEvent {
        timestamp,
        caller_identity,
        caller_connection_id,
        function_call,
        // Filled from committed writes inside `commit_and_broadcast_event`.
        status: EventStatus::Committed(DatabaseUpdate::default()),
        reducer_return_value: None,
        execution_budget_used: FunctionBudget::ZERO,
        host_execution_duration: Duration::ZERO,
        request_id,
        timer: None,
    }
}

fn tick_apply_progress(
    progress: &Option<SeedApplyProgress>,
    total_applied: &mut u64,
    since_tick: &mut u64,
) {
    *total_applied += 1;
    *since_tick += 1;
    if *since_tick >= PROGRESS_EVERY {
        if let Some(p) = progress {
            p.record(*total_applied);
        }
        std::thread::yield_now();
        *since_tick = 0;
    }
}

/// Commit a seed chunk without subscription eval / client broadcast.
///
/// Downstream clients are rejected until the mirror is `live`, so seed commits
/// can skip `commit_and_broadcast_event` (no `from_writes`, no
/// `eval_updates_sequential`). Clients that connect after `set_live` get a
/// fresh InitialSubscription from current state.
fn commit_seed_tx(subs: &ModuleSubscriptions, tx: MutTxId) -> Result<(), DBError> {
    let _ = subs.relational_db().commit_tx(tx)?;
    Ok(())
}

fn commit_live_tx(subs: &ModuleSubscriptions, event: ModuleEvent, tx: MutTxId) -> Result<(), DBError> {
    let _ = commit_and_broadcast_event(subs, None, event, tx);
    Ok(())
}

/// Apply row ops in one mut tx and (for live updates) broadcast with upstream provenance.
///
/// When `is_seed` is set, each table is cleared on the first chunk then rows
/// are inserted in [`SEED_CHUNK_ROWS`] commits (same chunk size as
/// relay-mirror-driver). Seed commits intentionally skip subscription
/// evaluation — clients are not accepted until the mirror is fully live.
/// Reconnect re-seeds otherwise collide with rows left over from the previous
/// session (unique constraint violation → endless reconnect loop).
pub fn apply_external_update(
    subs: &ModuleSubscriptions,
    provenance: Option<ExternalProvenance>,
    ops: impl IntoIterator<Item = TableOps>,
    progress: Option<SeedApplyProgress>,
    is_seed: bool,
) -> Result<(), DBError> {
    let stdb = subs.relational_db();
    let ops: Vec<TableOps> = ops.into_iter().collect();

    if is_seed {
        let mut total_applied = 0u64;
        let mut since_tick = 0u64;
        // One summary line instead of one per table: 274 INFO lines per cold
        // reset was real journal volume on the slow-disk host (attempt #4).
        let mut total_cleared = 0u64;
        let mut tables_cleared = 0u64;

        for table_ops in ops {
            let mut clear_first = true;
            let mut chunk_start = 0usize;

            while chunk_start <= table_ops.inserts.len() {
                let chunk_end = (chunk_start + SEED_CHUNK_ROWS).min(table_ops.inserts.len());
                let is_empty_seed = table_ops.inserts.is_empty() && clear_first;
                if chunk_start == chunk_end && !is_empty_seed {
                    break;
                }

                let mut tx = stdb.begin_mut_tx(IsolationLevel::Serializable, Workload::Update);
                if clear_first {
                    let removed = tx.clear_table(table_ops.table_id)?;
                    if removed > 0 {
                        total_cleared += removed;
                        tables_cleared += 1;
                    }
                    clear_first = false;
                }
                for row_bytes in &table_ops.inserts[chunk_start..chunk_end] {
                    stdb.insert(&mut tx, table_ops.table_id, row_bytes)?;
                    tick_apply_progress(&progress, &mut total_applied, &mut since_tick);
                }
                commit_seed_tx(subs, tx)?;

                if is_empty_seed {
                    break;
                }
                chunk_start = chunk_end;
            }
        }

        if tables_cleared > 0 {
            log::info!(
                "public-mirror: cleared {total_cleared} stale rows across {tables_cleared} tables before re-seed"
            );
        }

        if let Some(p) = &progress {
            p.record(total_applied);
        }
        return Ok(());
    }

    let mut tx = stdb.begin_mut_tx(IsolationLevel::Serializable, Workload::Update);
    for table_ops in ops {
        for row in &table_ops.deletes {
            tx.delete_product_value(table_ops.table_id, row)?;
        }
        for row_bytes in &table_ops.inserts {
            stdb.insert(&mut tx, table_ops.table_id, row_bytes)?;
        }
    }

    commit_live_tx(subs, mirror_event(&provenance), tx)
}

/// Bootstrap user tables (and views) from a [`ModuleDef`] without running an init reducer.
pub fn create_tables_from_module_def(stdb: &RelationalDB, module_def: &ModuleDef) -> anyhow::Result<()> {
    let tx = stdb.begin_mut_tx(IsolationLevel::Serializable, Workload::Internal);
    let (tx, ()) = stdb.with_auto_rollback(tx, |tx| {
        let mut table_defs: Vec<_> = module_def.tables().collect();
        table_defs.sort_by_key(|x| &x.name);
        for def in table_defs {
            spacetimedb_engine::update::create_table_from_def(stdb, tx, module_def, def)?;
        }

        let mut view_defs: Vec<_> = module_def.views().collect();
        view_defs.sort_by_key(|x| &x.name);
        for def in view_defs {
            create_table_from_view_def(stdb, tx, module_def, def)?;
        }

        let program = Program::empty(ModuleKind::MIRROR);
        stdb.set_initialized(tx, program)?;
        anyhow::Ok(())
    })?;

    if let Some((_tx_offset, tx_data, tx_metrics, reducer)) = stdb.commit_tx(tx)? {
        stdb.report_mut_tx_metrics(reducer, tx_metrics, Some(tx_data));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use parking_lot::RwLock;
    use spacetimedb_client_api_messages::websocket::v1 as ws_v1;
    use spacetimedb_lib::error::ResultTest;
    use spacetimedb_lib::identity::AuthCtx;
    use spacetimedb_lib::{AlgebraicType, ConnectionId, Identity};
    use spacetimedb_primitives::TableId;
    use spacetimedb_subscription::SubscriptionPlan;

    use super::{apply_external_update, TableOps};
    use crate::client::{
        ClientActorId, ClientConfig, ClientConnectionReceiver, ClientConnectionSender, ClientName,
    };
    use crate::db::relational_db::tests_utils::{with_read_only, TestDB};
    use crate::db::sql::ast::SchemaViewer;
    use crate::subscription::execution_unit::QueryHash;
    use crate::subscription::module_subscription_actor::ModuleSubscriptions;
    use crate::subscription::module_subscription_manager::{spawn_send_worker, Plan, SubscriptionManager};
    use crate::subscription::row_list_builder_pool::BsatnRowListBuilderPool;

    use bytes::Bytes;

    /// One mirrored region: in-memory DB, one table, a `ModuleSubscriptions`
    /// wired like `make_replica_ctx` wires it in production, and one
    /// subscribed client.
    struct Region {
        db: TestDB,
        table: TableId,
        subs: ModuleSubscriptions,
        sender: Arc<ClientConnectionSender>,
        rx: ClientConnectionReceiver,
    }

    fn setup_region(connection_id: u128) -> ResultTest<Region> {
        let db = TestDB::in_memory()?;
        let table = db.create_table_for_test("T", &[("a", AlgebraicType::U8)], &[])?;

        let sql = "SELECT * FROM T";
        let auth = AuthCtx::for_testing();
        let plan = with_read_only(&db, |tx| {
            let viewer = SchemaViewer::new(&*tx, &auth);
            let (plans, has_param) = SubscriptionPlan::compile(sql, &viewer, &auth).unwrap();
            let hash = QueryHash::from_string(sql, auth.caller(), has_param);
            Arc::new(Plan::new(plans, hash, sql.into()))
        });

        let queue = spawn_send_worker(Some(db.database_identity()));
        let manager = Arc::new(RwLock::new(SubscriptionManager::new(queue.clone())));
        let subs = ModuleSubscriptions::new(
            db.db.clone(),
            manager.clone(),
            queue,
            BsatnRowListBuilderPool::new(),
        );

        let (sender, rx) = ClientConnectionSender::dummy_with_channel(
            ClientActorId {
                identity: Identity::ZERO,
                connection_id: ConnectionId::from_u128(connection_id),
                name: ClientName(0),
            },
            ClientConfig::for_test(),
            (*db).clone(),
        );
        let sender = Arc::new(sender);
        let query_id: ws_v1::QueryId = ws_v1::QueryId::new(1);
        manager.write().add_subscription(sender.clone(), plan, query_id)?;

        Ok(Region { db, table, subs, sender, rx })
    }

    fn live_insert(region: &Region, value: u8) -> ResultTest<()> {
        apply_external_update(
            &region.subs,
            None,
            [TableOps {
                table_id: region.table,
                deletes: vec![],
                inserts: vec![Bytes::from(vec![value])],
            }],
            None,
            false,
        )?;
        Ok(())
    }

    /// Flush `region`'s tables the way a reconnect re-seed does: an empty
    /// seed clears every table it names.
    fn flush_tables(region: &Region) -> ResultTest<()> {
        apply_external_update(
            &region.subs,
            None,
            [TableOps {
                table_id: region.table,
                deletes: vec![],
                inserts: vec![],
            }],
            None,
            true,
        )?;
        Ok(())
    }

    fn row_count(region: &Region) -> usize {
        with_read_only(&region.db, |tx| region.db.iter(tx, region.table).unwrap().count())
    }

    fn recv_one(runtime: &tokio::runtime::Runtime, rx: &mut ClientConnectionReceiver, ctx: &str) {
        runtime
            .block_on(async { tokio::time::timeout(Duration::from_secs(5), rx.recv()).await })
            .unwrap_or_else(|_| panic!("timed out waiting for a client message ({ctx})"))
            .unwrap_or_else(|| panic!("client channel closed ({ctx})"));
    }

    fn assert_no_message(
        runtime: &tokio::runtime::Runtime,
        rx: &mut ClientConnectionReceiver,
        ctx: &str,
    ) {
        let got = runtime.block_on(async {
            tokio::time::timeout(Duration::from_millis(150), rx.recv()).await
        });
        assert!(got.is_err(), "expected no further client message ({ctx})");
    }

    /// A mirror session dying must kick and flush only its own database: the
    /// healthy region's client keeps its subscription and keeps receiving
    /// updates committed after the other region's reset.
    ///
    /// This drives the exact sequence `ModuleHost::reset_mirror_for_reconnect`
    /// runs when `run_public_mirror_loop` ends a session (kick this
    /// database's subscribers, then truncate its tables); per-database client
    /// gating is covered by the registry/gate tests in the public-mirror
    /// crate.
    #[test]
    fn mirror_reset_kicks_only_its_own_databases_clients() -> ResultTest<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _rt = runtime.enter();

        let mut a = setup_region(1)?;
        let mut b = setup_region(2)?;

        // Both regions live: each region's client receives its own update.
        live_insert(&a, 7)?;
        live_insert(&b, 8)?;
        recv_one(&runtime, &mut a.rx, "region A pre-reset");
        recv_one(&runtime, &mut b.rx, "region B pre-reset");
        assert_eq!(row_count(&a), 1);
        assert_eq!(row_count(&b), 1);

        // Region A's upstream session dies: the reconnect cold reset kicks
        // A's subscribers, then truncates A's tables for the re-seed.
        let kicked = a.subs.kick_all_subscribers();
        assert_eq!(kicked, 1, "region A's client should have been kicked");
        flush_tables(&a)?;

        assert!(a.sender.is_cancelled());
        assert_eq!(row_count(&a), 0, "region A's table should be flushed");
        assert!(!b.sender.is_cancelled());
        assert_eq!(row_count(&b), 1, "region B's table must be untouched");

        // The healthy region keeps serving: a live update committed after
        // the other region's reset still reaches region B's client, while
        // region A's kicked client receives nothing further.
        live_insert(&b, 9)?;
        recv_one(&runtime, &mut b.rx, "region B post-reset");
        assert_no_message(&runtime, &mut a.rx, "region A post-kick");

        Ok(())
    }
}
