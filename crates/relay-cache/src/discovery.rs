// SPDX-License-Identifier: MIT

//! Region discovery for relay-cache ingest.
//!
//! Two sources, merged with public-mirror rows taking precedence:
//!
//! 1. **`GET /v1/mirrors`** when `--mirrors-url` is set — maps each live
//!    `bitcraft-live-N` row to a WS target (monolithic `--mirror-ws-host`
//!    or per-region `127.0.0.1:3000+N`).
//! 2. **Systemd unit walk** under `--unit-dir` — legacy `relay-bc<N>` fleet;
//!    regions already covered by (1) are skipped so a partial cutover can
//!    leave old units installed but unused.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, Result};
use relay_coordinator::health::{public_port_for_database, source_name_for_database};
use reqwest::Client;
use serde_json::Value;
use url::Url;

/// How a region's subscribe target was discovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionBackend {
    /// Legacy per-region relay frontend + loopback `/metrics` ready gate.
    RelayFleet,
    /// Public-mirror row from `/v1/mirrors`; ready gate polls that URL.
    PublicMirror,
}

/// One regional subscribe target.
#[derive(Debug, Clone)]
pub struct DiscoveredRegion {
    pub region: u32,
    pub database: String,
    pub bind_url: Url,
    /// Loopback dashboard port for relay `/metrics` polls. `None` for
    /// public-mirror-backed regions.
    pub dashboard_port: Option<u16>,
    pub backend: RegionBackend,
    /// Sidecar `/v1/mirrors` URL used for the ready gate (mirror backend only).
    pub mirrors_url: Option<String>,
}

/// Walk units and optionally merge `/v1/mirrors` rows.
pub async fn discover_regions(
    unit_dir: &Path,
    mirrors_url: Option<&str>,
    mirror_ws_host: Option<&str>,
) -> Result<Vec<DiscoveredRegion>> {
    let mut mirror_regions = Vec::new();
    let mut mirror_region_ids = BTreeSet::new();

    if let Some(url) = mirrors_url.filter(|u| !u.trim().is_empty()) {
        mirror_regions = discover_from_mirrors(url.trim(), mirror_ws_host)
            .await
            .context("discover regions from /v1/mirrors")?;
        for r in &mirror_regions {
            mirror_region_ids.insert(r.region);
        }
    }

    let relay_regions = discover_from_units(unit_dir)?
        .into_iter()
        .filter(|r| !mirror_region_ids.contains(&r.region))
        .collect::<Vec<_>>();

    let mut out = mirror_regions;
    out.extend(relay_regions);
    out.sort_by_key(|r| r.region);
    Ok(out)
}

fn discover_from_units(unit_dir: &Path) -> Result<Vec<DiscoveredRegion>> {
    let sources = relay_coordinator::health::discover(
        unit_dir,
        &relay_coordinator::health::NamingSpec {
            template: Some("bitcraft-live-{stem}".into()),
            stem_prefix: Some("relay-bc".into()),
        },
    );
    let mut out = Vec::with_capacity(sources.len());
    for src in sources {
        if src.name == "global" {
            continue;
        }
        let Some(region) = parse_region_number(&src.name) else {
            tracing::warn!(
                target: "relay_cache::discovery",
                name = %src.name,
                "skipping source: cannot parse region number"
            );
            continue;
        };
        let bind_url = Url::parse(&format!("ws://127.0.0.1:{}", src.frontend_port))
            .context("build relay fleet bind URL")?;
        out.push(DiscoveredRegion {
            region,
            database: src.database,
            bind_url,
            dashboard_port: Some(src.dashboard_port),
            backend: RegionBackend::RelayFleet,
            mirrors_url: None,
        });
    }
    Ok(out)
}

async fn discover_from_mirrors(mirrors_url: &str, mirror_ws_host: Option<&str>) -> Result<Vec<DiscoveredRegion>> {
    let urls: Vec<&str> = mirrors_url
        .split(',')
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .collect();
    if urls.is_empty() {
        anyhow::bail!("mirrors_url is empty");
    }

    let mut out = Vec::new();
    for url in urls {
        out.extend(
            discover_from_mirrors_one(url, mirror_ws_host)
                .await
                .with_context(|| format!("discover from {url}"))?,
        );
    }
    out.sort_by_key(|r| r.region);
    out.dedup_by_key(|r| r.region);
    Ok(out)
}

async fn discover_from_mirrors_one(
    mirrors_url: &str,
    mirror_ws_host: Option<&str>,
) -> Result<Vec<DiscoveredRegion>> {
    let http = Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .context("build mirrors HTTP client")?;
    let resp = http
        .get(mirrors_url)
        .send()
        .await
        .with_context(|| format!("GET {mirrors_url}"))?;
    if !resp.status().is_success() {
        anyhow::bail!("GET {mirrors_url} returned HTTP {}", resp.status());
    }
    let body: Value = resp
        .json()
        .await
        .with_context(|| format!("decode JSON from {mirrors_url}"))?;
    let Some(arr) = body.get("mirrors").and_then(Value::as_array) else {
        anyhow::bail!("{mirrors_url} missing mirrors[]");
    };

    let mut out = Vec::new();
    for m in arr {
        let database = m
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if database.is_empty() || database == "bitcraft-live-global" {
            continue;
        }
        let name = source_name_for_database(&database);
        let Some(region) = parse_region_number(&name) else {
            tracing::warn!(
                target: "relay_cache::discovery",
                database = %database,
                "skipping mirror row: cannot parse region number"
            );
            continue;
        };
        let host_port = match mirror_ws_host.filter(|h| !h.trim().is_empty()) {
            Some(h) => h.trim().to_string(),
            None => format!("127.0.0.1:{}", public_port_for_database(&database)),
        };
        let bind_url =
            Url::parse(&format!("ws://{host_port}")).context("build public-mirror bind URL")?;
        out.push(DiscoveredRegion {
            region,
            database,
            bind_url,
            dashboard_port: None,
            backend: RegionBackend::PublicMirror,
            mirrors_url: Some(mirrors_url.to_string()),
        });
    }
    Ok(out)
}

/// `"bitcraft-live-14"` → `Some(14)`.
fn parse_region_number(name: &str) -> Option<u32> {
    name.strip_prefix("bitcraft-live-")?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn parse_region_from_source_name() {
        assert_eq!(parse_region_number("bitcraft-live-14"), Some(14));
        assert_eq!(parse_region_number("bitcraft-live-3"), Some(3));
        assert_eq!(parse_region_number("global"), None);
        assert_eq!(parse_region_number("bitcraft-live-"), None);
        assert_eq!(parse_region_number("relay-bc14"), None);
    }

    #[tokio::test]
    async fn discover_forwards_dashboard_port_for_relay_fleet() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("relay-bc14.service"),
            "[Service]\n\
             ExecStart=/relay \\\n\
             --mirror-database relay-mirror-bc14 \\\n\
             --frontend-bind 127.0.0.1:3014 \\\n\
             --dashboard-bind 127.0.0.1:3114\n",
        )
        .unwrap();
        let regions = discover_regions(dir.path(), None, None).await.unwrap();
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].region, 14);
        assert_eq!(regions[0].bind_url.as_str(), "ws://127.0.0.1:3014/");
        assert_eq!(regions[0].dashboard_port, Some(3114));
        assert_eq!(regions[0].database, "relay-mirror-bc14");
        assert_eq!(regions[0].backend, RegionBackend::RelayFleet);
    }
}
