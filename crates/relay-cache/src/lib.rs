// SPDX-License-Identifier: MIT

//! `relay_cache` — BitCraft read cache library.
//!
//! Two modes share this crate:
//!
//! - **WebSocket mode** (the `relay-cache` binary, unchanged behavior): discovers
//!   regional mirror frontends (`/v1/mirrors` or systemd units), holds one
//!   long-lived v2 BSATN subscription per region, and serves the HTTP/protobuf
//!   read API on loopback.
//! - **Embedded mode** (driven by `spacetimedb-bitcraft-mirror` with
//!   `--bitcraft-cache`): [`feed`] consumes the mirror's already-decoded
//!   upstream batches in-process — no WebSocket hop, no re-encode/re-decode of
//!   the subscription stream, no second subscription evaluation. See the
//!   repo's `BITCRAFT-FORK.md`.
//!
//! Module map: [`config`] CLI/env args, [`decode`] BSATN row bytes → typed
//! rows, [`store`] columnar per-table stores (one [`store::RegionStore`] per
//! region), [`interest`] pub/sub invalidation feeding the dim-buildings
//! WebSocket ([`stream`]), [`serve`] the HTTP read API, and the
//! WebSocket-mode-only ingest trio [`discovery`] / [`shard`] / [`wire`].
//! [`xp`] holds the vendored XP-threshold table.

pub mod roads_serve;
pub mod config;
pub mod decode;
pub mod discovery;
pub mod feed;
pub mod interest;
pub mod serve;
pub mod shard;
pub mod store;
pub mod stream;
pub mod wire;
pub mod xp;
pub mod roads;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Sample this process's RSS every 15s and flip `pressure` at the ceiling.
///
/// Shared by the WebSocket-mode binary and the embedded mode so
/// `/cache-health` reflects real process memory in both.
pub async fn run_memory_sampler(
    ceiling_bytes: u64,
    pressure: Arc<AtomicBool>,
    mut shutdown: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) {
    let mut sys = sysinfo::System::new();
    let pid = sysinfo::get_current_pid().ok();
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return,
            _ = interval.tick() => {
                let Some(pid) = pid else { continue };
                sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
                let Some(proc) = sys.process(pid) else { continue };
                let rss = proc.memory(); // bytes on recent sysinfo
                let over = rss >= ceiling_bytes;
                let was = pressure.swap(over, Ordering::Relaxed);
                if over && !was {
                    tracing::warn!(
                        target: "relay_cache",
                        rss_bytes = rss,
                        ceiling_bytes,
                        "resident set at/above memory ceiling — /cache-health ready=false"
                    );
                } else if !over && was {
                    tracing::info!(
                        target: "relay_cache",
                        rss_bytes = rss,
                        ceiling_bytes,
                        "resident set back under memory ceiling"
                    );
                }
            }
        }
    }
}
