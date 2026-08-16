// SPDX-License-Identifier: MIT

//! WebSocket that streams the building-entity-id set of house interior
//! dimensions. The primary consumer is `bitcraft-streamd` (on a separate
//! host, connecting over public `wss://`), but the endpoint carries no
//! PII and no admin surface — just u64 ids per `(region, dimension)` —
//! so it is open to any client.
//!
//! Endpoint:
//!
//! ```text
//! ws://127.0.0.1:8089/internal/dim-buildings/ws        (loopback)
//! wss://relay.bitcraftsync.app/internal/dim-buildings/ws   (public, TLS)
//! ```
//!
//! nginx proxies this exact path; the rest of `/internal/*` (notably
//! `/internal/stats`) stays loopback-only via an nginx
//! `location /internal/ { return 404; }` catch placed after this route.
//!
//! After connect the client sends one text frame listing the
//! `(region, dimension)` pairs it cares about:
//!
//! ```json
//! { "dims": [ {"region": 14, "dimension": 12345}, ... ] }
//! ```
//!
//! The server replies with one `dim_snapshot` per subscribed dimension,
//! then `{"type":"subscribed","count":N}`, then live `dim_delta` frames
//! (added/removed building entity ids) as `building_state` / `location_state`
//! rows land on a subscribed dimension. A later subscribe frame replaces
//! the whole set; an `{"unsubscribe":[…]}` frame removes entries without
//! replacing the set. Heartbeat every 5s: `{"ts":<unix ms UTC>}`.
//!
//! This is NOT a re-enable of the disabled multiplexed `/inventory/ws`
//! (commit `c7e48b6`, 2026-07-25). That protocol fanned full per-entity
//! inventory/craft JSON to 500+ browsers and saturated the host. This one
//! has the opposite profile: tiny `Vec<u64>` payloads, rare triggers
//! (building placement inside a house), and hard connection/dim caps.
//! Fan-out work is `dimensions × change-rate × watching connections`.
//!
//! Reuses the same `InterestHub` / `watch::Receiver` / coalesced-push
//! primitives as the disabled layer; the new piece is just the
//! `Topic::DimensionBuildings` key space (packed `(region, dimension)`).

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures_util::future::select_all;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::watch;

use crate::interest::{
    unpack_dim_key, ConnectionGuard, InterestHub, Subscription, Topic, dim_key,
};
use crate::serve::{housing_building_ids, Fleet};

/// Coalesce window between a watch bump and the re-scan/push. Matches the
/// disabled `/inventory/ws` layer — short enough to feel live, long enough
/// to fold a burst of row applies into one delta.
const COALESCE: Duration = Duration::from_millis(75);
/// Soft cap on concurrent WS connections (not interest leases). Bounds
/// accidental connection leaks and multi-client fan-out; streamd is the
/// primary consumer but the endpoint is public.
const MAX_CONNECTIONS: u64 = 2048;
/// Max `(region, dimension)` pairs per connection. Per-connection, not
/// fleet-wide — streamd can open more connections or paginate if its union
/// grows beyond this.
const MAX_DIM_KEYS: usize = 256;
/// Wait for the first subscribe frame after `onopen`.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(15);
/// Application-level heartbeat so the client can detect a half-open socket.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// `GET /internal/dim-buildings/ws` — public housing-interior building-id push
/// (nginx-proxied; `/internal/stats` stays unproxied).
pub async fn dim_buildings_ws(
    ws: WebSocketUpgrade,
    State(fleet): State<Fleet>,
) -> impl IntoResponse {
    let Some(conn) = fleet.interest.try_acquire_connection(MAX_CONNECTIONS) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({"error": "too many active dim-buildings streams"})),
        )
            .into_response();
    };
    ws.on_upgrade(move |socket| async move {
        run_dim_stream(socket, fleet, conn).await;
    })
    .into_response()
}

#[derive(Debug, Deserialize)]
struct SubscribeMsg {
    dims: Vec<DimRef>,
}

#[derive(Debug, Deserialize, Clone, Copy)]
struct DimRef {
    region: u32,
    dimension: u32,
}

#[derive(Debug, Deserialize)]
struct UnsubscribeMsg {
    unsubscribe: Vec<DimRef>,
}

async fn run_dim_stream(socket: WebSocket, fleet: Fleet, _conn: ConnectionGuard) {
    let (mut sink, mut source) = socket.split();

    tracing::info!(
        target: "relay_cache::stream",
        connections = fleet.interest.active_connections(),
        leases = fleet.interest.active_leases(),
        "dim-buildings stream connected"
    );

    // First frame must be the subscribe set. Tolerate a leading Ping
    // (some clients probe before sending the payload).
    let first = tokio::time::timeout(SUBSCRIBE_TIMEOUT, source.next()).await;
    let subscribe_text = match first {
        Ok(Some(Ok(Message::Text(t)))) => t,
        Ok(Some(Ok(Message::Ping(p)))) => {
            let _ = sink.send(Message::Pong(p)).await;
            match tokio::time::timeout(SUBSCRIBE_TIMEOUT, source.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => t,
                _ => {
                    let _ = send_text(
                        &mut sink,
                        r#"{"error":"expected subscribe JSON text frame"}"#,
                    )
                    .await;
                    let _ = sink.send(Message::Close(None)).await;
                    return;
                }
            }
        }
        _ => {
            let _ = send_text(
                &mut sink,
                r#"{"error":"expected subscribe JSON text frame within 15s"}"#,
            )
            .await;
            let _ = sink.send(Message::Close(None)).await;
            return;
        }
    };

    let mut keys = match parse_subscribe(&subscribe_text) {
        Ok(k) => k,
        Err(err) => {
            let _ = send_text(&mut sink, &format!(r#"{{"error":{}}}"#, json!(err))).await;
            let _ = sink.send(Message::Close(None)).await;
            return;
        }
    };

    let mut watches = bind_keys(&fleet.interest, &keys);

    // Per-dimension last-sent id set, used to diff the next re-scan into
    // added/removed. Seeded by the initial burst so the first live delta
    // is relative to the snapshot we just pushed.
    let mut last_sent: Vec<(u64, Vec<u64>)> = Vec::with_capacity(keys.len());
    if push_snapshots(&mut sink, &fleet, &keys, &mut last_sent)
        .await
        .is_err()
    {
        return;
    }

    let mut heartbeat = new_heartbeat_interval();
    heartbeat.tick().await;

    loop {
        tokio::select! {
            biased;
            msg = source.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(p))) => {
                        if sink.send(Message::Pong(p)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Text(t))) => {
                        // Try unsubscribe first; fall back to full re-subscribe.
                        if let Ok(unsub) = serde_json::from_str::<UnsubscribeMsg>(&t) {
                            let to_remove: HashSet<u64> = unsub.unsubscribe.iter()
                                .map(|d| dim_key(d.region, d.dimension))
                                .collect();
                            keys.retain(|k| !to_remove.contains(k));
                            watches = bind_keys(&fleet.interest, &keys);
                            last_sent.retain(|(k, _)| !to_remove.contains(k));
                            let _ = fleet.interest.active_leases();
                            if send_text(
                                &mut sink,
                                &format!(r#"{{"type":"unsubscribed","count":{}}}"#, keys.len()),
                            ).await.is_err() {
                                break;
                            }
                        } else {
                            match parse_subscribe(&t) {
                                Ok(new_keys) => {
                                    keys = new_keys;
                                    watches = bind_keys(&fleet.interest, &keys);
                                    // Resync the diff baseline for the new set.
                                    last_sent.clear();
                                    if push_snapshots(&mut sink, &fleet, &keys, &mut last_sent)
                                        .await.is_err() {
                                        break;
                                    }
                                }
                                Err(err) => {
                                    if send_text(
                                        &mut sink,
                                        &format!(r#"{{"error":{}}}"#, json!(err)),
                                    ).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) => {}
                    Some(Err(_)) => break,
                }
            }
            _ = heartbeat.tick() => {
                if send_text(&mut sink, &heartbeat_text()).await.is_err() {
                    break;
                }
            }
            changed = wait_any_change(&mut watches.rxs) => {
                let Some(key) = changed else { break; };
                let mut pending: HashSet<u64> = HashSet::new();
                pending.insert(key);
                tokio::time::sleep(COALESCE).await;
                drain_changed(&mut watches.rxs, &mut pending);
                let live: HashSet<u64> = keys.iter().copied().collect();
                for key in pending {
                    if !live.contains(&key) {
                        continue;
                    }
                    if push_delta(&mut sink, &fleet, key, &mut last_sent)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    fleet.interest.record_push();
                }
            }
        }
    }

    drop(watches);
    tracing::info!(
        target: "relay_cache::stream",
        "dim-buildings stream disconnected"
    );
}

/// Parse and validate a subscribe frame into a deduped `Vec<dim_key>`.
fn parse_subscribe(text: &str) -> Result<Vec<u64>, String> {
    let msg: SubscribeMsg =
        serde_json::from_str(text).map_err(|e| format!("invalid subscribe JSON: {e}"))?;
    let mut keys: Vec<u64> = Vec::with_capacity(msg.dims.len());
    for d in &msg.dims {
        if d.dimension == 0 {
            // dimension 0 is invalid; silently skip rather than fail the
            // whole frame (parity with the spec: "0 is invalid and silently
            // ignored").
            continue;
        }
        keys.push(dim_key(d.region, d.dimension));
    }
    keys.sort_unstable();
    keys.dedup();
    if keys.is_empty() {
        return Err("subscribe requires at least one {region, dimension} pair".into());
    }
    if keys.len() > MAX_DIM_KEYS {
        return Err(format!(
            "too many dimensions (max {MAX_DIM_KEYS}, got {})",
            keys.len()
        ));
    }
    Ok(keys)
}

struct KeyWatches {
    /// Keep leases alive until replaced/dropped.
    _leases: Vec<Subscription>,
    rxs: Vec<(u64, watch::Receiver<u64>)>,
}

fn bind_keys(hub: &std::sync::Arc<InterestHub>, keys: &[u64]) -> KeyWatches {
    let mut leases = Vec::with_capacity(keys.len());
    let mut rxs = Vec::with_capacity(keys.len());
    for &key in keys {
        let sub = hub.subscribe(Topic::DimensionBuildings, key);
        let rx = sub.clone_receiver();
        leases.push(sub);
        rxs.push((key, rx));
    }
    KeyWatches {
        _leases: leases,
        rxs,
    }
}

/// Wait until any watch receiver advances; returns the dirty packed key.
async fn wait_any_change(rxs: &mut [(u64, watch::Receiver<u64>)]) -> Option<u64> {
    if rxs.is_empty() {
        std::future::pending::<()>().await;
        return None;
    }
    type ChangeFut<'a> = Pin<Box<dyn Future<Output = Option<u64>> + Send + 'a>>;
    let mut futs: Vec<ChangeFut<'_>> = Vec::with_capacity(rxs.len());
    for (key, rx) in rxs.iter_mut() {
        let key = *key;
        futs.push(Box::pin(async move {
            rx.changed().await.ok()?;
            Some(key)
        }));
    }
    let (res, _idx, _rest) = select_all(futs).await;
    res
}

fn drain_changed(rxs: &mut [(u64, watch::Receiver<u64>)], pending: &mut HashSet<u64>) {
    for (key, rx) in rxs.iter_mut() {
        if rx.has_changed().unwrap_or(false) {
            let _ = rx.borrow_and_update();
            pending.insert(*key);
        }
    }
}

/// Initial burst: one `dim_snapshot` per subscribed dimension, then the
/// `subscribed` ack. Seeds `last_sent` so the first live `dim_delta` is
/// relative to the snapshot.
async fn push_snapshots(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    fleet: &Fleet,
    keys: &[u64],
    last_sent: &mut Vec<(u64, Vec<u64>)>,
) -> Result<(), ()> {
    for &key in keys {
        let (region, dimension) = unpack_dim_key(key);
        let ids = scan_dim(fleet, region, dimension);
        if send_text(sink, &snapshot_frame(region, dimension, &ids)).await.is_err() {
            return Err(());
        }
        last_sent.push((key, ids));
    }
    send_text(
        sink,
        &format!(r#"{{"type":"subscribed","count":{}}}"#, keys.len()),
    )
    .await
}

/// Re-scan one dimension, diff against the last-sent set, and emit a
/// `dim_delta` (skipped when nothing actually changed). Updates `last_sent`.
async fn push_delta(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    fleet: &Fleet,
    key: u64,
    last_sent: &mut Vec<(u64, Vec<u64>)>,
) -> Result<(), ()> {
    let (region, dimension) = unpack_dim_key(key);
    let new_ids = scan_dim(fleet, region, dimension);
    let prev_slot = last_sent.iter_mut().find(|(k, _)| *k == key);
    let prev = prev_slot.map(|(_, v)| std::mem::replace(v, new_ids.clone()));
    match prev {
        Some(old) => {
            let old_set: HashSet<u64> = old.iter().copied().collect();
            let new_set: HashSet<u64> = new_ids.iter().copied().collect();
            let added: Vec<u64> = new_set.difference(&old_set).copied().collect();
            let removed: Vec<u64> = old_set.difference(&new_set).copied().collect();
            if added.is_empty() && removed.is_empty() {
                return Ok(()); // coalesced bump with no net change
            }
            send_text(sink, &delta_frame(region, dimension, &added, &removed)).await
        }
        None => {
            // Key not in last_sent (e.g. brand-new subscribe mid-stream):
            // emit a snapshot so the client has a baseline.
            if let Some(slot) = last_sent.iter_mut().find(|(k, _)| *k == key) {
                slot.1 = new_ids.clone();
            } else {
                last_sent.push((key, new_ids.clone()));
            }
            send_text(sink, &snapshot_frame(region, dimension, &new_ids)).await
        }
    }
}

/// Read the filtered, sorted building ids for `(region, dimension)` from
/// the owning region shard. Empty when the shard isn't ready yet (the
/// client gets an empty snapshot; the first delta will catch up).
fn scan_dim(fleet: &Fleet, region: u32, dimension: u32) -> Vec<u64> {
    let Some(shard) = fleet.shards.iter().find(|s| s.region == region) else {
        return Vec::new();
    };
    let store = shard.store.read();
    housing_building_ids(&store, dimension)
}

/// `{"type":"dim_snapshot","region":R,"dimension":D,"buildings":["…","…"]}`
fn snapshot_frame(region: u32, dimension: u32, ids: &[u64]) -> String {
    let mut s = String::with_capacity(64 + ids.len() * 20);
    s.push_str(r#"{"type":"dim_snapshot","region":"#);
    s.push_str(&region.to_string());
    s.push_str(r#","dimension":"#);
    s.push_str(&dimension.to_string());
    s.push_str(r#","buildings":["#);
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push('"');
        s.push_str(&id.to_string());
        s.push('"');
    }
    s.push_str("]}");
    s
}

/// `{"type":"dim_delta","region":R,"dimension":D,"added":[…],"removed":[…]}`
fn delta_frame(region: u32, dimension: u32, added: &[u64], removed: &[u64]) -> String {
    let mut s = String::with_capacity(80 + (added.len() + removed.len()) * 20);
    s.push_str(r#"{"type":"dim_delta","region":"#);
    s.push_str(&region.to_string());
    s.push_str(r#","dimension":"#);
    s.push_str(&dimension.to_string());
    s.push_str(r#","added":["#);
    for (i, id) in added.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push('"');
        s.push_str(&id.to_string());
        s.push('"');
    }
    s.push_str(r#"],"removed":["#);
    for (i, id) in removed.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push('"');
        s.push_str(&id.to_string());
        s.push('"');
    }
    s.push_str("]}");
    s
}

fn heartbeat_text() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    format!(r#"{{"ts":{ts}}}"#)
}

fn new_heartbeat_interval() -> tokio::time::Interval {
    let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

async fn send_text(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    text: &str,
) -> Result<(), ()> {
    sink.send(Message::Text(text.to_owned()))
        .await
        .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_subscribe_accepts_dims() {
        let keys = parse_subscribe(
            r#"{"dims":[{"region":14,"dimension":12345},{"region":14,"dimension":12346},{"region":3,"dimension":7890}]}"#,
        )
        .unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&dim_key(14, 12345)));
        assert!(keys.contains(&dim_key(14, 12346)));
        assert!(keys.contains(&dim_key(3, 7890)));
    }

    #[test]
    fn parse_subscribe_rejects_empty() {
        assert!(parse_subscribe(r#"{"dims":[]}"#).is_err());
        // All-zero dimensions are silently dropped → empty set → error.
        assert!(parse_subscribe(r#"{"dims":[{"region":0,"dimension":0}]}"#).is_err());
    }

    #[test]
    fn parse_subscribe_silently_skips_dimension_zero() {
        // dimension=0 is invalid per spec; skipped, not fatal.
        let keys = parse_subscribe(
            r#"{"dims":[{"region":14,"dimension":0},{"region":14,"dimension":99}]}"#,
        )
        .unwrap();
        assert_eq!(keys, vec![dim_key(14, 99)]);
    }

    #[test]
    fn parse_subscribe_dedups() {
        let keys = parse_subscribe(
            r#"{"dims":[{"region":14,"dimension":12345},{"region":14,"dimension":12345}]}"#,
        )
        .unwrap();
        assert_eq!(keys, vec![dim_key(14, 12345)]);
    }

    #[test]
    fn parse_subscribe_rejects_oversize() {
        let mut dims = String::from("[");
        for i in 1..=257 {
            if i > 1 {
                dims.push(',');
            }
            dims.push_str(&format!(r#"{{"region":14,"dimension":{i}}}"#));
        }
        dims.push(']');
        let frame = format!(r#"{{"dims":{dims}}}"#);
        assert!(parse_subscribe(&frame).is_err());
    }

    #[test]
    fn dim_key_round_trips() {
        for (region, dimension) in [(0, 1), (14, 12345), (u32::MAX, u32::MAX), (3, 7890)] {
            let (r, d) = unpack_dim_key(dim_key(region, dimension));
            assert_eq!((r, d), (region, dimension));
        }
    }

    #[test]
    fn snapshot_and_delta_frame_shapes() {
        let snap = snapshot_frame(14, 12345, &[17592186044416, 17592186044417]);
        assert_eq!(
            snap,
            r#"{"type":"dim_snapshot","region":14,"dimension":12345,"buildings":["17592186044416","17592186044417"]}"#
        );
        let delta = delta_frame(14, 12345, &[17592186044418], &[]);
        assert_eq!(
            delta,
            r#"{"type":"dim_delta","region":14,"dimension":12345,"added":["17592186044418"],"removed":[]}"#
        );
    }

    #[test]
    fn unsubscribe_frame_parses() {
        let msg: UnsubscribeMsg =
            serde_json::from_str(r#"{"unsubscribe":[{"region":14,"dimension":12345}]}"#).unwrap();
        assert_eq!(msg.unsubscribe.len(), 1);
        assert_eq!(msg.unsubscribe[0].region, 14);
        assert_eq!(msg.unsubscribe[0].dimension, 12345);
    }
}
