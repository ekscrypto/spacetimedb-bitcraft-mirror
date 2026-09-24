//! Public-mirror apply loop: upstream → `ModuleHost::apply_mirrored_updates`.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;
use futures::FutureExt;
use spacetimedb::db::relational_db::RelationalDB;
use spacetimedb::host::module_host::ModuleHost;
use spacetimedb::host::public_mirror::{ExternalProvenance, MirroredUpdate, TableOps};
use spacetimedb_datastore::execution_context::Workload;
use spacetimedb_primitives::TableId;
use spacetimedb_schema::def::ModuleDef;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::coordinator_client::CoordinatorClient;

use crate::observer::MirrorObserverRegistry;
use crate::schema::{event_table_names, fetch_and_parse_schema, public_user_table_names};
use crate::status::{MirrorConnectivity, MirrorStatusHandle, MirrorStatusRegistry};
use crate::upstream::{self, UpstreamConfig, UpstreamError, UpstreamUpdate};

/// A successfully fetched upstream schema no longer matches the schema used to
/// bootstrap this in-memory mirror.
///
/// The standalone process treats this as fatal so its service manager can
/// restart it, recreating every mirror table and embedded-cache decoder from
/// the new schemas before performing a full re-seed.
#[derive(Debug, thiserror::Error)]
#[error("upstream schema changed for `{database}`: {old_hash} -> {new_hash} (tables {old_tables} -> {new_tables})")]
pub struct SchemaChanged {
    pub database: String,
    pub old_hash: spacetimedb_lib::Hash,
    pub new_hash: spacetimedb_lib::Hash,
    pub old_tables: usize,
    pub new_tables: usize,
}

/// Configuration for the public-mirror upstream loop.
#[derive(Debug, Clone)]
pub struct PublicMirrorConfig {
    pub upstream: Url,
    pub database: String,
    pub auth_token: Option<String>,
    /// When `None`, subscribe to all public user tables from the module def.
    pub tables: Option<Vec<String>>,
    pub connect_timeout: Duration,
    /// Hash of the raw schema JSON used to bootstrap the local mirror.
    pub bootstrap_schema_hash: spacetimedb_lib::Hash,
    /// Allowlisted `*_event` tables whose live wire events are forwarded to
    /// downstream subscribers (broadcast-only; the local tables stay empty —
    /// see `apply_external_update`). Event tables not on this list are
    /// excluded from the upstream subscribe set entirely — high-frequency
    /// tables (`player_move_event`, `enemy_move_event`, …) would dwarf the
    /// state tables for zero downstream value.
    pub event_tables: Vec<String>,
}

/// Errors that can be caused by continuing a session with an obsolete module
/// definition. Network/connectivity failures keep their existing reconnect
/// behavior and do not add a schema fetch to every transient outage.
fn should_refetch_schema(error: &UpstreamError) -> bool {
    matches!(
        error,
        UpstreamError::WebSocket(_)
            | UpstreamError::Decode(_)
            | UpstreamError::Subscription(_)
            | UpstreamError::UnknownTable(_)
            | UpstreamError::NotProduct(_)
            | UpstreamError::Apply(_)
            | UpstreamError::Closed(_)
    )
}

async fn detect_schema_change(
    config: &PublicMirrorConfig,
    error: &UpstreamError,
    old_tables: usize,
) -> Option<SchemaChanged> {
    if !should_refetch_schema(error) {
        return None;
    }

    log::warn!(
        "public-mirror: checking upstream schema after session error (database={}, error={error})",
        config.database
    );
    let (schema_bytes, module_def) = match fetch_and_parse_schema(&config.upstream, &config.database).await {
        Ok(schema) => schema,
        Err(fetch_error) => {
            log::warn!(
                "public-mirror: schema re-fetch failed after session error; retaining reconnect behavior \
                 (database={}, error={fetch_error})",
                config.database
            );
            return None;
        }
    };

    let new_hash = schema_program_hash(&schema_bytes);
    if new_hash == config.bootstrap_schema_hash {
        log::debug!(
            "public-mirror: upstream schema hash unchanged after session error \
             (database={}, schema_hash={new_hash})",
            config.database
        );
        return None;
    }

    let change = SchemaChanged {
        database: config.database.clone(),
        old_hash: config.bootstrap_schema_hash,
        new_hash,
        old_tables,
        new_tables: module_def.tables().count(),
    };
    log::error!(
        "public-mirror: SCHEMA HASH CHANGED; requesting full daemon recycle \
         (database={}, old_schema_hash={}, new_schema_hash={}, tables={} -> {})",
        change.database,
        change.old_hash,
        change.new_hash,
        change.old_tables,
        change.new_tables
    );
    Some(change)
}

/// Resolve table name → [`TableId`] via the local relational DB.
fn resolve_table_ids(stdb: &RelationalDB, names: &[String]) -> anyhow::Result<HashMap<String, TableId>> {
    let tx = stdb.begin_tx(Workload::Internal);
    let mut map = HashMap::with_capacity(names.len());
    for name in names {
        let Some(id) = stdb.table_id_from_name(&tx, name)? else {
            anyhow::bail!("local mirror has no table `{name}`");
        };
        map.insert(name.clone(), id);
    }
    // Tx is dropped without commit — read-only.
    drop(tx);
    Ok(map)
}

fn update_to_mirrored(update: UpstreamUpdate, table_ids: &HashMap<String, TableId>) -> Option<MirroredUpdate> {
    let is_seed = update.is_seed;
    let provenance = update.provenance.map(|p| ExternalProvenance {
        reducer_name: p.reducer_name,
        caller_identity: p.caller_identity,
        caller_connection_id: p.caller_connection_id,
        timestamp: p.timestamp,
        request_id: p.request_id,
        args: p.args,
    });
    let mut ops = Vec::with_capacity(update.tables.len());
    for t in update.tables {
        let Some(&table_id) = table_ids.get(&t.table_name) else {
            log::warn!("public-mirror: skipping ops for unknown local table `{}`", t.table_name);
            continue;
        };
        ops.push(TableOps {
            table_id,
            deletes: t.deletes,
            inserts: t.inserts,
        });
    }
    if ops.is_empty() {
        return None;
    }
    Some(MirroredUpdate {
        provenance,
        ops,
        is_seed,
    })
}

/// Split the candidate subscribe set around the event-table forwarding allowlist.
///
/// `event_tables` is the module def's authoritative event-table set (v10
/// `is_event`) — never the `_event` suffix, which misclassifies persistent
/// tables like `player_notification_event`.
///
/// - `forwarded`: allowlisted event tables — subscribed, with live event rows
///   forwarded to downstream v2 subscribers as proper `EventTable` frames.
/// - `excluded`: other event tables — dropped from the subscribe set so
///   their (high-frequency) events never reach this host.
pub fn partition_event_tables(
    tables: &[String],
    allowlist: &[String],
    event_tables: &HashSet<String>,
) -> (Vec<String>, Vec<String>) {
    let allow: HashSet<&str> = allowlist.iter().map(String::as_str).collect();
    let mut captured = Vec::new();
    let mut excluded = Vec::new();
    for table in tables {
        if event_tables.contains(table.as_str()) {
            if allow.contains(table.as_str()) {
                captured.push(table.clone());
            } else {
                excluded.push(table.clone());
            }
        }
    }
    (captured, excluded)
}

/// Connect upstream, sequential-subscribe public tables, apply seed + live updates into `module_host`.
///
/// Updates are applied in **batches** (one executor job per batch) onto the mirror's
/// dedicated [`spacetimedb::util::jobs::SingleThreadedExecutor`] via
/// [`ModuleHost::apply_mirrored_updates`], matching SpacetimeDB's one-thread-per-database
/// model while amortizing the cross-thread round trip when a backlog has built up.
///
/// `subscribe_gate` serialises the **setup** phase across mirrors: a permit is
/// acquired before connecting and held through every table's wire seed
/// (`SubscribeMultiApplied` received and enqueued); released so the next mirror
/// can start downloading while this one drains local apply. On disconnect the
/// loop cold-resets (kick clients, flush tables) then must reacquire the gate
/// (and coordinator permit) before reconnecting — so a fleet-wide blip does not
/// stampede all regions' re-seeds at once.
pub async fn run_public_mirror_loop(
    module_host: ModuleHost,
    config: PublicMirrorConfig,
    module_def: ModuleDef,
    status: MirrorStatusHandle,
    registry: Arc<MirrorStatusRegistry>,
    subscribe_gate: Arc<Semaphore>,
    coordinator_socket: Option<PathBuf>,
    observers: Option<Arc<MirrorObserverRegistry>>,
) -> anyhow::Result<()> {
    let tables = match &config.tables {
        Some(t) if !t.is_empty() => t.clone(),
        _ => public_user_table_names(&module_def),
    };
    if tables.is_empty() {
        anyhow::bail!("no public user tables to mirror");
    }

    // Event-table forwarding: keep allowlisted event tables in the subscribe
    // set (forwarding their live rows to subscribers), drop the rest from the
    // wire entirely.
    let module_event_tables = event_table_names(&module_def);
    let (forwarded_events, excluded_events) = partition_event_tables(&tables, &config.event_tables, &module_event_tables);
    let forwarded_set: HashSet<&str> = forwarded_events.iter().map(String::as_str).collect();
    // State tables keep their order; forwarded event tables subscribe last —
    // their seeds are always-empty and instant, so they never hold the
    // subscribe gate behind a state seed.
    let mut tables: Vec<String> = tables
        .into_iter()
        .filter(|t| !module_event_tables.contains(t.as_str()))
        .collect();
    tables.extend(forwarded_events.iter().cloned());
    if !excluded_events.is_empty() {
        log::info!(
            "public-mirror: excluding {} non-allowlisted event table(s) from subscribe (database={}): {}",
            excluded_events.len(),
            config.database,
            excluded_events.join(", ")
        );
    }
    let allowlist_unknown: Vec<&String> = config
        .event_tables
        .iter()
        .filter(|t| !forwarded_set.contains(t.as_str()))
        .collect();
    if !allowlist_unknown.is_empty() {
        // Not in the module def, or filtered out by --mirror-table. Warn, don't
        // fail: an upstream schema change must not take the state mirror down.
        log::warn!(
            "public-mirror: event allowlist entries not in the subscribe set (database={}): {:?}",
            config.database,
            allowlist_unknown
        );
    }

    status.set_tables_total(tables.len() as u32);
    if !forwarded_events.is_empty() {
        status.init_event_tables(forwarded_events.clone());
        log::info!(
            "public-mirror: forwarding {} event table(s) (database={}): {}",
            forwarded_events.len(),
            config.database,
            forwarded_events.join(", ")
        );
    }
    log::info!(
        "public-mirror: mirroring {} tables from {} database={}",
        tables.len(),
        config.upstream,
        config.database
    );

    let stdb = module_host.relational_db().clone();
    let table_ids = resolve_table_ids(&stdb, &tables)?;
    // Reconnect cold-reset flushes STATE tables only: event tables never
    // hold committed rows (wire-only) and no upstream seed could rebuild
    // them anyway.
    let flush_table_ids: Vec<TableId> = tables
        .iter()
        .filter(|t| !forwarded_set.contains(t.as_str()))
        .filter_map(|t| table_ids.get(t).copied())
        .collect();
    let event_allowlist: Arc<HashSet<String>> = Arc::new(config.event_tables.iter().cloned().collect());
    let module_for_reset = module_host.clone();

    let upstream_cfg = UpstreamConfig {
        host: config.upstream.clone(),
        database: config.database.clone(),
        auth_token: config.auth_token.clone(),
        connect_timeout: config.connect_timeout,
    };

    // Exponential reconnect backoff (same shape as spacetimedb-relay upstream):
    // 1s → 2s → 4s → … capped at 30s. Reset only after a session that reached
    // the live loop and stayed up ≥ STABLE_THRESHOLD — so connect/subscribe
    // failures (including the 60s connect timeout) keep growing backoff.
    const BACKOFF_MAX_SECS: u64 = 30;
    const STABLE_THRESHOLD: Duration = Duration::from_secs(5);
    let mut backoff_secs: u64 = 1;

    // Subscribe order. When a session dies on a specific table, that table is
    // moved to the front so the next attempt transfers the riskiest (largest /
    // slowest) seed while the connection is freshest, instead of after
    // re-seeding every other table first.
    let mut table_order = tables.clone();
    let completed_tables = Arc::new(Mutex::new(HashSet::new()));
    let coordinator = coordinator_socket
        .as_ref()
        .map(|path| CoordinatorClient::new(path, config.database.clone()));

    // Session generations for observer dispatch (see `crate::observer`):
    // session N dispatches carry N; the reset after session N carries N+1.
    let mut session_generation: u64 = 0;

    loop {
        session_generation += 1;
        let generation = session_generation;

        // Built per session so the closure captures this session's generation.
        let on_update = {
            let module_host = module_host.clone();
            let table_ids = table_ids.clone();
            let database = config.database.clone();
            let observers = observers.clone();
            Arc::new(
                move |updates: Vec<UpstreamUpdate>,
                      progress: Option<crate::status::SeedApplyProgress>|
                      -> BoxFuture<'static, Result<(), anyhow::Error>> {
                    let module_host = module_host.clone();
                    let table_ids = table_ids.clone();
                    let database = database.clone();
                    let observers = observers.clone();
                    async move {
                        // Observers first, then the relational apply. The applier
                        // awaits this future, so the dispatch is FIFO with the
                        // apply queue and a slow observer backpressures the session
                        // exactly like a slow consumer did over the wire; the
                        // embedded cache never runs ahead of the relational store.
                        if let Some(registry) = observers.as_ref() {
                            registry
                                .dispatch_updates(&database, generation, updates.clone())
                                .await?;
                        }
                        let batch: Vec<MirroredUpdate> = updates
                            .into_iter()
                            .filter_map(|u| update_to_mirrored(u, &table_ids))
                            .collect();
                        if batch.is_empty() {
                            return Ok(());
                        }
                        let progress = progress.map(|p| spacetimedb::host::public_mirror::SeedApplyProgress {
                            rows_applied: p.rows_applied,
                            last_apply_unix_ms: p.last_apply_unix_ms,
                        });
                        module_host
                            .apply_mirrored_updates(batch, progress)
                            .await
                            .map_err(|e| anyhow::anyhow!(e))?;
                        Ok(())
                    }
                    .boxed()
                },
            )
        };

        // Gate + coordinator: serialise re-seeds so a shared upstream blip
        // cannot pull every region through SubscribeMulti at once.
        let coordinator_permit = match &coordinator {
            Some(client) => client.acquire().await,
            None => None,
        };
        let gate_permit = acquire_subscribe_slot(&subscribe_gate, &config.database, &status).await?;
        // Seed pacing applies to reconnects, and to cold-start sessions once
        // any *other* mirror is already live: both 2026-08-17 production
        // cascades ran in exactly that window — the cold-start tail, with
        // live regions ingesting while later regions seeded unpaced. Our own
        // status is Waiting/Connecting here, so any Live mirror is another
        // mirror's session.
        let others_live = registry
            .snapshot()
            .mirrors
            .iter()
            .any(|m| matches!(m.connectivity, MirrorConnectivity::Live));
        let pace_seeds = generation > 1 || others_live;
        let mut live_started = None;
        let mut failed_table = None;
        let result = upstream::connect_and_mirror(
            upstream_cfg.clone(),
            &module_def,
            &table_order,
            Arc::clone(&event_allowlist),
            on_update.clone(),
            &mut live_started,
            &mut failed_table,
            status.clone(),
            gate_permit,
            coordinator_permit,
            Arc::clone(&completed_tables),
            observers.clone(),
            generation,
            // Bounded seed bursts on reconnects and once other mirrors are
            // live — see the pacing decision above.
            pace_seeds,
        )
        .await;
        let lived_for = live_started.map(|t| t.elapsed()).unwrap_or(Duration::ZERO);
        if lived_for >= STABLE_THRESHOLD {
            backoff_secs = 1;
        }
        let sleep_for = Duration::from_secs(backoff_secs);
        let next_attempt_at = SystemTime::now() + sleep_for;
        // Mark disconnected first so `public_mirror_accepts_clients_for`
        // rejects new WS for this database until its mirror is live again.
        status.set_disconnected(next_attempt_at);
        if let Err(error) = &result
            && let Some(change) = detect_schema_change(&config, error, module_def.tables().count()).await
        {
            return Err(change.into());
        }
        match result {
            Ok(()) => {
                log::warn!(
                    "public-mirror: upstream loop exited cleanly; cold-reset then reconnect in {backoff_secs}s (lived {lived_for:?}, database={})",
                    config.database
                );
            }
            Err(e) => {
                log::error!(
                    "public-mirror: upstream error: {e:#}; cold-reset then reconnect in {backoff_secs}s (lived {lived_for:?}, database={})",
                    config.database
                );
            }
        }
        // Treat the next attempt as a brand-new mirror: drop subscribers,
        // truncate local tables, forget prior seed progress. Re-subscribe goes
        // through the gate above after backoff.
        completed_tables.lock().await.clear();
        if let Err(e) = module_for_reset
            .reset_mirror_for_reconnect(flush_table_ids.iter().copied())
            .await
        {
            log::error!("public-mirror: reconnect cold-reset failed: {e:#}");
        }
        // Observers drop their derived state too. The next session dispatches
        // with `session_generation + 1`; late in-flight batches from the dead
        // session carry the old (smaller) generation and are discarded.
        if let Some(registry) = observers.as_ref() {
            let next_generation = session_generation + 1;
            if let Err(e) = registry.dispatch_reset(&config.database, next_generation).await {
                log::error!(
                    "public-mirror: observer on_reset for `{}` failed: {e:#}",
                    config.database
                );
            }
        }
        if let Some(failed) = failed_table
            && let Some(pos) = table_order.iter().position(|t| *t == failed)
            && pos != 0
        {
            let t = table_order.remove(pos);
            log::info!("public-mirror: prioritizing previously failed table `{t}` on next attempt");
            table_order.insert(0, t);
        }
        tokio::time::sleep(sleep_for).await;
        backoff_secs = (backoff_secs * 2).min(BACKOFF_MAX_SECS);
    }
}

async fn acquire_subscribe_slot(
    gate: &Arc<Semaphore>,
    database: &str,
    status: &MirrorStatusHandle,
) -> anyhow::Result<OwnedSemaphorePermit> {
    status.set_waiting();
    let available = gate.available_permits();
    if available == 0 {
        log::info!("public-mirror: `{database}` waiting for subscribe slot (all slots in use)");
    }
    let permit = Arc::clone(gate)
        .acquire_owned()
        .await
        .map_err(|_| anyhow::anyhow!("subscribe gate closed"))?;
    log::info!("public-mirror: `{database}` acquired subscribe slot");
    status.set_connecting();
    Ok(permit)
}

/// Convenience: hash schema bytes into a SpacetimeDB [`spacetimedb_lib::Hash`].
pub fn schema_program_hash(schema_bytes: &[u8]) -> spacetimedb_lib::Hash {
    spacetimedb_sats::hash::hash_bytes(schema_bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use super::{should_refetch_schema, partition_event_tables, SchemaChanged};
    use crate::upstream::UpstreamError;
    use spacetimedb_lib::Hash;

    #[test]
    fn event_tables_partition_into_forwarded_and_excluded() {
        let tables: Vec<String> = [
            "player_state",
            "market_trade_event",
            "player_move_event",
            "claim_treasury_event",
            "enemy_move_event",
            "location_state",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let allowlist: Vec<String> = ["market_trade_event", "claim_treasury_event"]
            .into_iter()
            .map(String::from)
            .collect();
        let event_tables: HashSet<String> = ["market_trade_event", "player_move_event", "claim_treasury_event", "enemy_move_event"]
            .into_iter()
            .map(String::from)
            .collect();

        let (captured, excluded) = partition_event_tables(&tables, &allowlist, &event_tables);
        assert_eq!(captured, vec!["market_trade_event".to_string(), "claim_treasury_event".to_string()]);
        assert_eq!(excluded, vec!["player_move_event".to_string(), "enemy_move_event".to_string()]);
    }

    #[test]
    fn event_partition_uses_schema_truth_not_suffix() {
        // `player_notification_event` is a persistent table despite the
        // suffix: not in the event set → stays in the subscribe list.
        let tables: Vec<String> = ["player_state", "player_notification_event", "craft_event"]
            .into_iter()
            .map(String::from)
            .collect();
        let allowlist: Vec<String> = ["craft_event"].into_iter().map(String::from).collect();
        let event_tables: HashSet<String> = ["craft_event"].into_iter().map(String::from).collect();

        let (captured, excluded) = partition_event_tables(&tables, &allowlist, &event_tables);
        assert_eq!(captured, vec!["craft_event".to_string()]);
        assert!(excluded.is_empty());
    }

    #[test]
    fn event_partition_empty_allowlist_excludes_all_events() {
        let tables: Vec<String> = ["player_state", "craft_event"].into_iter().map(String::from).collect();
        let event_tables: HashSet<String> = ["craft_event"].into_iter().map(String::from).collect();
        let (captured, excluded) = partition_event_tables(&tables, &[], &event_tables);
        assert!(captured.is_empty());
        assert_eq!(excluded, vec!["craft_event".to_string()]);
    }

    #[test]
    fn event_partition_ignores_allowlist_entries_absent_from_tables() {
        let tables: Vec<String> = ["player_state"].into_iter().map(String::from).collect();
        let allowlist: Vec<String> = ["ghost_event"].into_iter().map(String::from).collect();
        let event_tables: HashSet<String> = HashSet::new();
        let (captured, excluded) = partition_event_tables(&tables, &allowlist, &event_tables);
        assert!(captured.is_empty());
        assert!(excluded.is_empty());
    }

    #[test]
    fn schema_shaped_errors_trigger_refetch() {
        let errors = [
            UpstreamError::Decode("unknown tag 0x4".into()),
            UpstreamError::Subscription("unknown table".into()),
            UpstreamError::UnknownTable("new_table".into()),
            UpstreamError::NotProduct("changed_table".into()),
            UpstreamError::Apply("row schema mismatch".into()),
            UpstreamError::Closed("module updated".into()),
        ];

        for error in &errors {
            assert!(should_refetch_schema(error), "{error} should trigger schema re-fetch");
        }
    }

    #[test]
    fn operational_errors_keep_normal_reconnect_behavior() {
        let errors = [
            UpstreamError::Connect("connection refused".into()),
            UpstreamError::SubscribeTimeout("large_table".into(), Duration::from_secs(60)),
            UpstreamError::SubscribeStalled("large_table".into(), Duration::from_secs(60)),
            UpstreamError::Backlog { queued: 2, max: 1 },
            UpstreamError::ProbeTimeout(30),
        ];

        for error in &errors {
            assert!(
                !should_refetch_schema(error),
                "{error} should use normal reconnect behavior"
            );
        }
    }

    #[test]
    fn schema_change_error_includes_both_hashes() {
        let old_hash = Hash::from_byte_array([0x11; 32]);
        let new_hash = Hash::from_byte_array([0x22; 32]);
        let error = SchemaChanged {
            database: "bitcraft-live-7".into(),
            old_hash,
            new_hash,
            old_tables: 274,
            new_tables: 275,
        }
        .to_string();

        assert!(error.contains(&old_hash.to_string()));
        assert!(error.contains(&new_hash.to_string()));
        assert!(error.contains("tables 274 -> 275"));
    }
}
