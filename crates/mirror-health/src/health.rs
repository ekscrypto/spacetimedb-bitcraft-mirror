// SPDX-License-Identifier: MIT

//! Fleet `/health` aggregator — polls `GET /v1/mirrors` on the bitcraft-mirror
//! status sidecar and serves aggregated JSON at `/health`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::Mutex as TokioMutex;

use crate::sys_metrics::SysState;

pub const SOURCES_POLL_INTERVAL: Duration = Duration::from_secs(30);
pub const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(4);
pub const TX_RATE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
pub const TX_RATE_BUCKETS: usize = 60;
pub const MIRROR_POLL_GRACE_CYCLES: u32 = 1;

#[derive(Clone, Serialize)]
pub struct SourceSnapshot {
    pub port: u16,
    pub database: String,
    pub schema_cached: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connectivity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables_live: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables_total: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions_processed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disconnected_since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_attempt_eta_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_unix: Option<u64>,
}

#[derive(Clone)]
pub struct HealthState {
    inner: Arc<Inner>,
}

struct SourceTxRate {
    last_tx: Option<u64>,
    buckets: [u64; TX_RATE_BUCKETS],
    head: usize,
    filled: usize,
}

impl Default for SourceTxRate {
    fn default() -> Self {
        Self {
            last_tx: None,
            buckets: [0; TX_RATE_BUCKETS],
            head: 0,
            filled: 0,
        }
    }
}

impl SourceTxRate {
    fn record(&mut self, tx: u64) {
        if let Some(prev) = self.last_tx {
            let delta = tx.saturating_sub(prev);
            self.buckets[self.head] = delta;
            self.head = (self.head + 1) % TX_RATE_BUCKETS;
            if self.filled < TX_RATE_BUCKETS {
                self.filled += 1;
            }
        }
        self.last_tx = Some(tx);
    }

    fn rate(&self) -> Option<f64> {
        if self.filled == 0 {
            return None;
        }
        let sum: u64 = if self.filled < TX_RATE_BUCKETS {
            self.buckets[..self.filled].iter().sum()
        } else {
            self.buckets.iter().sum()
        };
        Some(sum as f64 / self.filled as f64)
    }
}

#[derive(Default)]
struct TxRateTracker {
    sources: HashMap<String, SourceTxRate>,
}

impl TxRateTracker {
    fn record(&mut self, name: &str, tx: u64) {
        self.sources.entry(name.to_string()).or_default().record(tx);
    }

    fn rates(&self) -> HashMap<String, f64> {
        self.sources
            .iter()
            .filter_map(|(name, ring)| ring.rate().map(|r| (name.clone(), r)))
            .collect()
    }

    fn retain(&mut self, live: &HashMap<String, u64>) {
        self.sources.retain(|name, _| live.contains_key(name));
    }
}

struct Inner {
    mirrors_url: String,
    fetch_timeout: Duration,
    http: Client,
    sources: RwLock<BTreeMap<String, SourceSnapshot>>,
    tx_rates: Mutex<TxRateTracker>,
    mirror_fail_counts: Mutex<HashMap<String, u32>>,
    refresh_lock: TokioMutex<()>,
    sys: SysState,
}

impl HealthState {
    pub fn new(mirrors_url: String, sys: SysState) -> Self {
        let http = Client::builder()
            .timeout(DEFAULT_FETCH_TIMEOUT)
            .build()
            .expect("reqwest client build");
        Self {
            inner: Arc::new(Inner {
                mirrors_url,
                fetch_timeout: DEFAULT_FETCH_TIMEOUT,
                http,
                sources: RwLock::new(BTreeMap::new()),
                tx_rates: Mutex::new(TxRateTracker::default()),
                mirror_fail_counts: Mutex::new(HashMap::new()),
                refresh_lock: TokioMutex::new(()),
                sys,
            }),
        }
    }

    pub async fn refresh_sources(&self) {
        let _guard = self.inner.refresh_lock.lock().await;
        let map = self
            .fetch_mirror_snapshots(&self.inner.mirrors_url)
            .await;
        self.inner.sources.write().clone_from(&map);
        self.stamp_tx_rates();
    }

    async fn fetch_mirror_snapshots(&self, url: &str) -> BTreeMap<String, SourceSnapshot> {
        let urls: Vec<&str> = url
            .split(',')
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .collect();
        if urls.is_empty() {
            return BTreeMap::new();
        }

        let mut next: BTreeMap<String, SourceSnapshot> = BTreeMap::new();
        let mut degraded: BTreeMap<String, SourceSnapshot> = BTreeMap::new();
        for u in urls {
            for (name, snap) in self.fetch_mirror_snapshots_one(u).await {
                if snap.connectivity.as_deref() == Some("unreachable") {
                    degraded.entry(name).or_insert(snap);
                } else {
                    next.insert(name, snap);
                }
            }
        }
        for (name, snap) in degraded {
            next.entry(name).or_insert(snap);
        }
        next
    }

    async fn fetch_mirror_snapshots_one(&self, url: &str) -> BTreeMap<String, SourceSnapshot> {
        let fetched = fetch_mirrors(&self.inner.http, self.inner.fetch_timeout, url).await;
        let Some(body) = fetched else {
            return self.degraded_mirror_rows(url, "poll failed");
        };
        let Some(arr) = body.get("mirrors").and_then(|v| v.as_array()) else {
            return self.degraded_mirror_rows(url, "missing mirrors[]");
        };
        self.inner
            .mirror_fail_counts
            .lock()
            .insert(url.to_string(), 0);

        let now = now_unix();
        let mut next: BTreeMap<String, SourceSnapshot> = BTreeMap::new();
        for m in arr {
            let database = m
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if database.is_empty() {
                continue;
            }
            let name = source_name_for_database(&database);
            let connectivity = m
                .get("connectivity")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let tables_live = m
                .get("tables_live")
                .and_then(|v| v.as_u64())
                .map(|n| n as u32);
            let tables_total = m
                .get("tables_total")
                .and_then(|v| v.as_u64())
                .map(|n| n as u32);
            let live = connectivity.as_deref() == Some("live")
                && tables_live.is_some()
                && tables_live == tables_total;
            next.insert(
                name,
                SourceSnapshot {
                    port: public_port_for_database(&database),
                    database,
                    schema_cached: live || tables_live.unwrap_or(0) > 0,
                    connectivity,
                    tables_live,
                    tables_total,
                    transactions_processed: m
                        .get("transactions_processed")
                        .and_then(|v| v.as_u64()),
                    transactions_per_sec: None,
                    connected_since: opt_string(m.get("connected_since")),
                    disconnected_since: opt_string(m.get("disconnected_since")),
                    next_attempt_at: opt_string(m.get("next_attempt_at")),
                    next_attempt_eta_secs: m.get("next_attempt_eta_secs").and_then(|v| v.as_u64()),
                    last_success_unix: Some(now),
                },
            );
        }
        next
    }

    fn degraded_mirror_rows(&self, url: &str, why: &str) -> BTreeMap<String, SourceSnapshot> {
        let fails = {
            let mut counts = self.inner.mirror_fail_counts.lock();
            let c = counts.entry(url.to_string()).or_insert(0);
            *c += 1;
            *c
        };
        let prior: BTreeMap<String, SourceSnapshot> = self
            .inner
            .sources
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if prior.is_empty() || fails <= MIRROR_POLL_GRACE_CYCLES {
            tracing::warn!(
                target: "mirror_health::health",
                %url,
                why,
                fails,
                "/v1/mirrors failed; keeping prior sources (grace)"
            );
            return prior;
        }
        tracing::warn!(
            target: "mirror_health::health",
            %url,
            why,
            fails,
            count = prior.len(),
            "/v1/mirrors unreachable past grace; marking sources unreachable"
        );
        prior
            .into_iter()
            .map(|(k, mut v)| {
                v.connectivity = Some("unreachable".to_string());
                (k, v)
            })
            .collect()
    }

    pub async fn run_sources_poller(self, shutdown: impl std::future::Future<Output = ()>) {
        let mut shutdown = std::pin::pin!(shutdown);
        let mut tick = tokio::time::interval(SOURCES_POLL_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = tick.tick() => {
                    self.refresh_sources().await;
                }
            }
        }
    }

    pub async fn run_tx_rate_sampler(self, shutdown: impl std::future::Future<Output = ()>) {
        let mut shutdown = std::pin::pin!(shutdown);
        let mut tick = tokio::time::interval(TX_RATE_SAMPLE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = tick.tick() => {
                    self.sample_tx_rates().await;
                }
            }
        }
    }

    async fn sample_tx_rates(&self) {
        let urls: Vec<&str> = self
            .inner
            .mirrors_url
            .split(',')
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .collect();
        if urls.is_empty() {
            return;
        }

        let mut observed: HashMap<String, u64> = HashMap::new();
        for url in urls {
            let Some(body) = fetch_mirrors(&self.inner.http, self.inner.fetch_timeout, url).await
            else {
                continue;
            };
            let Some(arr) = body.get("mirrors").and_then(|v| v.as_array()) else {
                continue;
            };
            for m in arr {
                let Some(database) = m.get("database").and_then(|v| v.as_str()) else {
                    continue;
                };
                let Some(tx) = m.get("transactions_processed").and_then(|v| v.as_u64()) else {
                    continue;
                };
                let name = source_name_for_database(database);
                observed.insert(name, tx);
            }
        }

        {
            let mut tracker = self.inner.tx_rates.lock();
            for (name, tx) in &observed {
                tracker.record(name, *tx);
            }
            tracker.retain(&observed);
        }

        {
            let mut sources = self.inner.sources.write();
            for (name, tx) in &observed {
                if let Some(snap) = sources.get_mut(name) {
                    snap.transactions_processed = Some(*tx);
                }
            }
        }
        self.stamp_tx_rates();
    }

    fn stamp_tx_rates(&self) {
        let rates = self.inner.tx_rates.lock().rates();
        let mut sources = self.inner.sources.write();
        for (name, snap) in sources.iter_mut() {
            snap.transactions_per_sec = rates.get(name).copied();
        }
    }

    pub fn snapshot_json(&self) -> Value {
        let sources = self.inner.sources.read().clone();
        let sys = self.inner.sys.snapshot();
        json!({
            "sources": sources,
            "schema_count": sources.len(),
            "system": sys,
        })
    }
}

async fn fetch_mirrors(http: &Client, timeout: Duration, url: &str) -> Option<Value> {
    let resp = tokio::time::timeout(timeout, http.get(url).send()).await;
    let resp = match resp {
        Ok(Ok(r)) => r,
        _ => return None,
    };
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<Value>().await.ok()
}

fn opt_string(v: Option<&Value>) -> Option<String> {
    v.and_then(|x| x.as_str()).map(|s| s.to_string())
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn public_port_for_database(database: &str) -> u16 {
    if database == "bitcraft-live-global" || database.ends_with("-global") {
        return 3000;
    }
    if let Some(n) = database.strip_prefix("bitcraft-live-")
        && let Ok(id) = n.parse::<u16>()
    {
        return 3000 + id;
    }
    3000
}

pub fn source_name_for_database(database: &str) -> String {
    if database == "bitcraft-live-global" {
        "global".to_string()
    } else {
        database.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn tx_rate_ring_needs_two_samples() {
        let mut ring = SourceTxRate::default();
        ring.record(100);
        assert!(ring.rate().is_none());
        ring.record(110);
        assert_eq!(ring.rate(), Some(10.0));
    }

    #[test]
    fn public_port_and_source_name_from_database() {
        assert_eq!(public_port_for_database("bitcraft-live-global"), 3000);
        assert_eq!(public_port_for_database("bitcraft-live-14"), 3014);
        assert_eq!(source_name_for_database("bitcraft-live-global"), "global");
        assert_eq!(source_name_for_database("bitcraft-live-14"), "bitcraft-live-14");
    }

    #[test]
    fn snapshot_json_shape_matches_index_html_contract() {
        let sys = SysState::new();
        let state = HealthState::new("http://127.0.0.1:3130/v1/mirrors".into(), sys);
        let snap = state.snapshot_json();
        assert!(snap.get("sources").unwrap().is_object());
        assert_eq!(snap["schema_count"].as_u64(), Some(0));
        let la = &snap["system"]["cpu"]["load_average"];
        for k in ["one", "five", "fifteen"] {
            assert!(la.get(k).is_some(), "load_average.{k} must be present");
        }
        let mem = &snap["system"]["memory"];
        for k in ["total_bytes", "free_bytes", "available_bytes"] {
            assert!(mem.get(k).is_some(), "memory.{k} must be present");
        }
        let net = &snap["system"]["network"];
        assert!(net.get("bytes_per_sec_in").is_some());
        assert_eq!(net["window_seconds"].as_u64(), Some(300));
    }

    async fn spawn_json_server(body: String) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral");
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let body = body.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let _ = sock.read(&mut buf).await;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        (format!("http://127.0.0.1:{port}/v1/mirrors"), handle)
    }

    async fn dead_port() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral");
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        port
    }

    #[tokio::test]
    async fn mirror_poll_failure_marks_unreachable_after_grace() {
        let payload = json!({
            "mirrors": [{
                "database": "bitcraft-live-global",
                "connectivity": "live",
                "tables_live": 12,
                "tables_total": 12,
                "transactions_processed": 100
            }]
        })
        .to_string();
        let (url, server) = spawn_json_server(payload).await;
        let state = HealthState::new(url, SysState::new());

        state.refresh_sources().await;
        {
            let sources = state.inner.sources.read();
            let row = sources.get("global").expect("row present");
            assert_eq!(row.connectivity.as_deref(), Some("live"));
            assert!(row.last_success_unix.expect("success stamped") > 0);
        }

        server.abort();
        state.refresh_sources().await;
        {
            let sources = state.inner.sources.read();
            assert_eq!(
                sources.get("global").unwrap().connectivity.as_deref(),
                Some("live")
            );
        }
        state.refresh_sources().await;
        {
            let sources = state.inner.sources.read();
            let row = sources.get("global").unwrap();
            assert_eq!(row.connectivity.as_deref(), Some("unreachable"));
            assert_eq!(row.database, "bitcraft-live-global");
            assert_eq!(row.port, 3000);
        }
    }

    #[tokio::test]
    async fn degraded_url_does_not_shadow_fresh_rows() {
        let a_payload = json!({
            "mirrors": [{
                "database": "bitcraft-live-global",
                "connectivity": "live",
                "tables_live": 12,
                "tables_total": 12
            }]
        })
        .to_string();
        let (url_a, server_a) = spawn_json_server(a_payload).await;
        let url_b = format!("http://127.0.0.1:{}/v1/mirrors", dead_port().await);
        let state = HealthState::new(format!("{url_a},{url_b}"), SysState::new());

        state.refresh_sources().await;
        assert_eq!(
            state
                .inner
                .sources
                .read()
                .get("global")
                .unwrap()
                .connectivity
                .as_deref(),
            Some("live")
        );

        for _ in 0..3 {
            state.refresh_sources().await;
            assert_eq!(
                state
                    .inner
                    .sources
                    .read()
                    .get("global")
                    .unwrap()
                    .connectivity
                    .as_deref(),
                Some("live")
            );
        }

        server_a.abort();
        state.refresh_sources().await;
        assert_eq!(
            state
                .inner
                .sources
                .read()
                .get("global")
                .unwrap()
                .connectivity
                .as_deref(),
            Some("live")
        );
        state.refresh_sources().await;
        assert_eq!(
            state
                .inner
                .sources
                .read()
                .get("global")
                .unwrap()
                .connectivity
                .as_deref(),
            Some("unreachable")
        );
    }
}
