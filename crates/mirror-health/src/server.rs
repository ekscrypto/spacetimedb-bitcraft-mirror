// SPDX-License-Identifier: MIT

//! HTTP server for `/health` and the fleet dashboard.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

use crate::health::HealthState;
use crate::sys_metrics::SysState;

const INDEX_STUB: &str = "<!doctype html>\
<html><head><title>mirror-health</title></head>\
<body><p>mirror-health. \
See <a href=\"/health\">/health</a> for fleet JSON. \
Pass <code>--index-html &lt;path&gt;</code> to serve a custom dashboard.</p>\
</body></html>";

/// Run the health aggregator until `shutdown` resolves.
pub async fn run(
    health_bind: Option<SocketAddr>,
    mirrors_url: String,
    index_html: Option<String>,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    let Some(bind) = health_bind else {
        tracing::info!(target: "mirror_health", "HTTP disabled (--health-bind empty)");
        shutdown.await;
        return Ok(());
    };

    let sys = SysState::new();
    let health = HealthState::new(mirrors_url.clone(), sys.clone());
    tracing::info!(
        target: "mirror_health",
        url = %mirrors_url,
        "health sources from GET /v1/mirrors"
    );

    {
        let h = health.clone();
        let shutdown = shutdown_signal_clone();
        tokio::spawn(async move {
            h.run_sources_poller(shutdown).await;
        });
    }
    {
        let h = health.clone();
        let shutdown = shutdown_signal_clone();
        tokio::spawn(async move {
            h.run_tx_rate_sampler(shutdown).await;
        });
    }
    {
        let s = sys.clone();
        let shutdown = shutdown_signal_clone();
        tokio::spawn(async move {
            s.run(shutdown).await;
        });
    }

    let page: Arc<str> = index_html.as_deref().unwrap_or(INDEX_STUB).into();
    let app = Router::new()
        .route("/", get({
            let page = page.clone();
            move || async move { Html(page.to_string()) }
        }))
        .route("/health", get(health_json))
        .layer(CorsLayer::permissive())
        .with_state(health);

    let tcp = TcpListener::bind(bind).await.map_err(|e| {
        anyhow::anyhow!("health endpoint bind failed on {bind}: {e}")
    })?;
    tracing::info!(
        target: "mirror_health",
        %bind,
        "health endpoint listening"
    );

    let mut shutdown = std::pin::pin!(shutdown);
    let serve = axum::serve(tcp, app);
    tokio::select! {
        _ = &mut shutdown => {
            tracing::info!(target: "mirror_health", "shutdown signal received");
        }
        result = serve => {
            if let Err(e) = result {
                tracing::error!(
                    target: "mirror_health",
                    error = %e,
                    "health HTTP server exited"
                );
            }
        }
    }
    Ok(())
}

async fn health_json(State(state): State<HealthState>) -> impl IntoResponse {
    (
        [("Cache-Control", "no-store")],
        axum::Json(state.snapshot_json()),
    )
}

fn shutdown_signal_clone() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    })
}
