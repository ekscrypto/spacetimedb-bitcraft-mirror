// SPDX-License-Identifier: MIT

mod health;
mod server;
mod sys_metrics;

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "mirror-health",
    about = "Fleet /health aggregator (polls bitcraft-mirror GET /v1/mirrors sidecar)"
)]
struct Args {
    /// Bind address for the `/health` and `/` (dashboard) HTTP endpoint.
    /// Empty string disables the HTTP server.
    #[arg(long, env = "MIRROR_HEALTH_BIND", default_value = "127.0.0.1:8082")]
    health_bind: String,

    /// Public-mirror readiness URL (`GET /v1/mirrors`). Comma-separated for
    /// multiple sidecars. Required.
    #[arg(
        long,
        env = "MIRROR_MIRRORS_URL",
        default_value = "http://127.0.0.1:3130/v1/mirrors"
    )]
    mirrors_url: String,

    /// Path to an HTML file served as the `/` dashboard page. If unset
    /// a minimal stub linking to `/health` is served instead.
    #[arg(long, env = "MIRROR_INDEX_HTML")]
    index_html: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let health_bind = if args.health_bind.trim().is_empty() {
        None
    } else {
        Some(args.health_bind.parse::<SocketAddr>().map_err(|e| {
            anyhow::anyhow!("invalid --health-bind {:?}: {e}", args.health_bind)
        })?)
    };

    let mirrors_url = args.mirrors_url.trim();
    if mirrors_url.is_empty() {
        anyhow::bail!("--mirrors-url is required (empty string)");
    }

    let index_html = match args.index_html {
        Some(ref path) => match std::fs::read_to_string(path) {
            Ok(content) => Some(content),
            Err(e) => {
                tracing::warn!(
                    target: "mirror_health",
                    path = %path.display(),
                    error = %e,
                    "--index-html file unreadable; serving stub page instead"
                );
                None
            }
        },
        None => None,
    };

    let shutdown = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    };

    server::run(health_bind, mirrors_url.to_string(), index_html, shutdown).await
}
