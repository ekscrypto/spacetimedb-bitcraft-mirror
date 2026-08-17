//! v1.bsatn.spacetimedb WebSocket upstream client.
//!
//! # Session architecture
//!
//! The socket loop **never awaits database work inline**. Decoded updates are
//! pushed into an in-memory apply queue ([`Applier`]) and the in-flight apply
//! future is polled *concurrently* with socket reads, client Pings, and the
//! application-level liveness probe (see [`SessionCtx::next_event`]). A slow
//! or stuck multi-million-row insert therefore costs queue depth (bounded by
//! [`LIVE_QUEUE_BYTES_MAX`]), never the WebSocket connection: Pings keep
//! flowing, server Pings keep being answered, and reads keep draining the
//! kernel buffer so upstream never sees a slow consumer.
//!
//! Apply ordering is preserved: the queue is strictly FIFO — seeds and live
//! updates apply in arrival order. On the initial cold start, wire seeds are
//! queued as they arrive and the next table's `SubscribeMulti` is sent
//! immediately; local apply catches up on the dedicated DB thread while
//! upstream keeps sending. **Re-seeds are paced** (each table's seed applies
//! locally before the next downloads) so a reconnecting region's decode and
//! apply bursts cannot starve the live sessions sharing the runtime. Heavy
//! frames — anything at/above [`OFFLOAD_DECODE_BYTES`], or compressed
//! at/above [`OFFLOAD_COMPRESSED_BYTES`] — decompress + decode on the
//! (deprioritized) blocking pool; frames read while such a decode is in
//! flight are **deferred** (buffered, not applied) and replayed in arrival
//! order once the decoded message has been routed — applying them eagerly
//! would reorder post-snapshot updates ahead of the seed, whose
//! truncate-then-insert would erase their effects and leave stale rows
//! behind.
//!
//! The liveness probe is **silence-aware**: a late probe on a socket that is
//! still receiving indicates a starved local runtime, not a dead upstream,
//! and is tolerated (up to [`PROBE_LATE_HARD_TIMEOUT`]) instead of
//! cold-resetting the session into a re-seed cascade.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use http::header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL};
use spacetimedb::util::thread_scheduling::deprioritize_mirror_background_thread;
use spacetimedb_client_api_messages::websocket::common::{
    QuerySetId, SERVER_MSG_COMPRESSION_TAG_BROTLI, SERVER_MSG_COMPRESSION_TAG_GZIP, SERVER_MSG_COMPRESSION_TAG_NONE,
};
use spacetimedb_client_api_messages::websocket::v1::{
    BsatnFormat, ClientMessage, CompressableQueryUpdate, DatabaseUpdate, OneOffQuery, QueryUpdate, ServerMessage,
    SubscribeMulti, TransactionUpdate, TransactionUpdateLight, UpdateStatus,
};
use spacetimedb_lib::{bsatn, ConnectionId, Identity, ProductValue, Timestamp};
use spacetimedb_sats::{ProductType, WithTypespace};
use spacetimedb_schema::def::ModuleDef;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, OwnedSemaphorePermit};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{client_async_tls_with_config, WebSocketStream};
use url::Url;

use crate::byte_count::ByteCountStream;
use crate::coordinator_client::CoordinatorPermit;
use crate::observer::MirrorObserverRegistry;
use crate::status::{ByteCounter, MirrorStatusHandle, SeedApplyProgress};

const SUBPROTOCOL_V1: &str = "v1.bsatn.spacetimedb";

/// Client-initiated WS Ping interval. Matches relay-upstream / relay-cache wire:
/// keep the write path polled so tungstenite auto-Pongs flush during multi-GiB
/// fragmented-message reassembly (un-split socket — never `futures::split`).
const CLIENT_PING_INTERVAL: Duration = Duration::from_secs(10);

/// Application-level liveness probe (matches legacy `relay-upstream`).
///
/// WS Ping/Pong only proves the TCP flow (or a proxy) is alive — not that the
/// upstream *application* is still processing requests. Every
/// [`PROBE_INTERVAL`] after live starts we send a v1 `OneOffQuery` ("SELECT 1");
/// if no `OneOffQueryResponse` arrives within [`PROBE_TIMEOUT`], the session
/// errors and reconnects. That catches the "up but silent" failure mode where
/// connectivity stays `live` but transactions freeze.
const PROBE_INTERVAL: Duration = Duration::from_secs(60);
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);
/// Silence-awareness (2026-08-17 upstream-resets post-mortem): a probe that
/// is late while the socket is still receiving is a starved runtime, not a
/// dead upstream — killing it triggers the re-seed cascade (6 of 7 probe
/// timeouts in the calibrated repro were false). Only a probe unanswered
/// this long **with bytes flowing** is treated as a real failure.
const PROBE_LATE_HARD_TIMEOUT: Duration = Duration::from_secs(150);
const PROBE_MESSAGE_ID: &[u8] = b"PUBLIC_MIRROR_PROBE";
/// How often to poll an in-flight probe against [`PROBE_TIMEOUT`].
const PROBE_DEADLINE_POLL: Duration = Duration::from_secs(5);

/// Warn when the gap between `next_event` completions exceeds this: the
/// session task was starved off the shared Tokio workers (host CPU
/// saturation), and the client-ping (10s), upstream-pong (~30s), and probe
/// (30s) deadlines above have started to slip.
const EVENT_LOOP_LAG_WARN: Duration = Duration::from_secs(5);

/// Absolute per-table cap waiting for `SubscribeMultiApplied`. Safety net only:
/// the *stall* timeout below is the operative one, so a slow-but-progressing
/// multi-GiB seed is never killed while bytes are still arriving.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);

/// Fail a subscribe wait when the socket has been completely silent this long.
/// Measured from the TCP byte counter, so a seed that is still trickling in —
/// however slowly — keeps the wait alive.
const SUBSCRIBE_STALL_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Wait for the post-connect `IdentityToken`. This wait holds a subscribe-gate
/// slot, so it must not be generous: a dead server here would otherwise park
/// the whole fleet.
const IDENTITY_TOKEN_TIMEOUT: Duration = Duration::from_secs(60);

/// Frames at/above this size are decompressed + BSATN-decoded on the blocking
/// pool so multi-second CPU work cannot stall other mirrors' live WebSocket
/// tasks on the shared Tokio runtime. Live transaction updates stay well under
/// this; only seed snapshots exceed it.
const OFFLOAD_DECODE_BYTES: usize = 256 * 1024;
/// Compressed frames at/above this (compressed) size also decode on the
/// blocking pool even though they are under [`OFFLOAD_DECODE_BYTES`]:
/// Brotli-decompressing busy-region live updates inline on a worker is
/// exactly what stretched poll gaps to 5–10 s and starved Ping/Pong/probe
/// servicing in the 2026-08-17 incident. Below this the decompress is
/// microseconds and not worth a blocking-pool round trip.
const OFFLOAD_COMPRESSED_BYTES: usize = 4 * 1024;

/// Cap on decoded-but-unapplied *live* update bytes queued in memory. Seeds are
/// excluded from this cap: their size is dictated by upstream and they queue in
/// FIFO order during pipelined subscribe. If live traffic outruns the local
/// apply rate for long enough to hit this, the session errors (and reconnects)
/// instead of growing without bound.
const LIVE_QUEUE_BYTES_MAX: usize = 1024 * 1024 * 1024;

/// Max live updates applied in one database job. Batching amortizes the
/// cross-thread round trip to the mirror's dedicated executor when a backlog
/// has built up (e.g. TUs queued behind a multi-minute seed apply).
const APPLY_BATCH_MAX: usize = 128;

/// Estimated per-delete queue cost in bytes (deletes are decoded
/// `ProductValue`s whose exact heap size is not cheaply known).
const DELETE_COST_ESTIMATE: usize = 128;

#[derive(Debug, Clone)]
pub struct UpstreamConfig {
    pub host: Url,
    pub database: String,
    pub auth_token: Option<String>,
    pub connect_timeout: Duration,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            host: Url::parse("wss://localhost").expect("valid url"),
            database: String::new(),
            auth_token: None,
            connect_timeout: Duration::from_secs(60),
        }
    }
}

/// Upstream reducer provenance from a committed v1 `TransactionUpdate`.
#[derive(Debug, Clone)]
pub struct UpstreamProvenance {
    pub reducer_name: String,
    pub caller_identity: Identity,
    pub caller_connection_id: ConnectionId,
    pub timestamp: Timestamp,
    pub request_id: u32,
    pub args: Bytes,
}

/// Decoded row ops for one table.
///
/// `inserts` are zero-copy slices into the decoded frame's row blob (one
/// shared allocation), not per-row copies.
#[derive(Debug, Clone)]
pub struct UpstreamTableOps {
    pub table_name: String,
    pub deletes: Vec<ProductValue>,
    /// Raw BSATN delete rows, parallel to `deletes` and likewise zero-copy.
    /// Kept for in-process observers (the embedded relay-cache) that decode
    /// deletes with their own schema-driven decoder. Empty for seeds — an
    /// upstream snapshot carries no delete rows.
    pub delete_bytes: Vec<Bytes>,
    pub inserts: Vec<Bytes>,
}

/// One applyable update from upstream (seed snapshot or live TU).
#[derive(Debug, Clone)]
pub struct UpstreamUpdate {
    /// `None` for subscribe-applied seed rows; `Some` for committed transaction updates.
    pub provenance: Option<UpstreamProvenance>,
    pub tables: Vec<UpstreamTableOps>,
    /// `true` for a `SubscribeMultiApplied` snapshot: the local table is cleared
    /// before inserting so reconnect re-seeds converge instead of hitting unique
    /// constraint violations on rows left over from the previous session.
    pub is_seed: bool,
}

#[derive(Debug, Error)]
pub enum UpstreamError {
    #[error("invalid upstream url: {0}")]
    Url(String),
    #[error("connection failed: {0}")]
    Connect(String),
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("BSATN decode failed: {0}")]
    Decode(String),
    #[error("BSATN encode failed: {0}")]
    Encode(String),
    #[error("frame too short ({0} bytes)")]
    FrameTooShort(usize),
    #[error("unknown compression tag {0}")]
    UnknownCompression(u8),
    #[error("subscription error: {0}")]
    Subscription(String),
    #[error("unknown table `{0}` in module def")]
    UnknownTable(String),
    #[error("row type for table `{0}` is not a product")]
    NotProduct(String),
    #[error("timed out waiting for SubscribeMultiApplied for `{0}` ({1:?} elapsed)")]
    SubscribeTimeout(String, Duration),
    #[error("subscribe for `{0}` stalled: no socket bytes for {1:?}")]
    SubscribeStalled(String, Duration),
    #[error("local apply failed: {0}")]
    Apply(String),
    #[error("apply backlog exceeded: {queued} bytes queued (max {max})")]
    Backlog { queued: usize, max: usize },
    #[error("upstream closed: {0}")]
    Closed(String),
    #[error("liveness probe timed out — upstream did not respond to OneOffQuery within {0}s")]
    ProbeTimeout(u64),
}

/// Applies a batch of updates (in order) into the local mirror database.
///
/// The optional [`SeedApplyProgress`] is only meaningful when the batch is a
/// single seed update.
type ApplyFn = Arc<
    dyn Fn(Vec<UpstreamUpdate>, Option<SeedApplyProgress>) -> BoxFuture<'static, Result<(), anyhow::Error>>
        + Send
        + Sync,
>;

/// Connect to upstream v1, sequentially subscribe to each table, apply seeds and live updates.
///
/// When the live update loop begins, `live_started` is set to `Instant::now()` so the
/// caller can measure how long the session was actually live (for reconnect backoff).
/// It is left `None` if connect/subscribe fails before the live loop.
///
/// If the session fails while subscribing a specific table, `failed_table` is set to
/// that table's name so the caller can prioritize it (subscribe it first, while the
/// connection is freshest) on the next attempt.
///
/// `subscribe_permit` is the subscribe-gate slot acquired by the caller before
/// connecting. It is held through connect and every table's **wire** seed
/// (SubscribeMultiApplied received and enqueued); released once all tables are
/// subscribed on the wire so the next mirror can start downloading while this
/// one drains its FIFO apply queue. Local seed applies continue on the dedicated
/// DB thread after the gate is released.
///
/// **Live invariant:** once this session reaches `status.set_live()`, it never
/// reacquires the gate until disconnect. Another mirror's subscribe/seed must not
/// stall live applies — those run on this DB's JobCores thread, and heavy frame
/// decode is offloaded off the shared Tokio runtime.
///
/// `pace_seeds` (reconnects only) serializes each table's wire seed with its
/// local apply: the next table's `SubscribeMulti` is not sent until the
/// previous seed has applied, bounding a re-seed's decode/apply burst to one
/// table at a time instead of the whole snapshot queueing at once.
#[allow(clippy::too_many_arguments)]
pub async fn connect_and_mirror(
    config: UpstreamConfig,
    module_def: &ModuleDef,
    tables: &[String],
    on_update: ApplyFn,
    live_started: &mut Option<tokio::time::Instant>,
    failed_table: &mut Option<String>,
    status: MirrorStatusHandle,
    subscribe_permit: OwnedSemaphorePermit,
    coordinator_permit: Option<CoordinatorPermit>,
    completed_tables: Arc<Mutex<HashSet<String>>>,
    observers: Option<Arc<MirrorObserverRegistry>>,
    generation: u64,
    pace_seeds: bool,
) -> Result<(), UpstreamError> {
    *live_started = None;
    *failed_table = None;
    let row_types = build_row_types(module_def)?;
    let request = build_connect_request(&config)?;
    log::info!(
        "public-mirror: connecting to {} (database={}, auth_bytes={})",
        request.uri(),
        config.database,
        config.auth_token.as_ref().map(|t| t.len()).unwrap_or(0)
    );

    let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None);

    let counter = ByteCounter::new();
    status.attach_byte_counter(counter.clone());

    let connect_fut = async {
        let host = request
            .uri()
            .host()
            .ok_or_else(|| UpstreamError::Connect("request URI missing host".into()))?;
        let port = request
            .uri()
            .port_u16()
            .unwrap_or_else(|| match request.uri().scheme_str() {
                Some("wss") | Some("https") => 443,
                _ => 80,
            });
        let addr = format!("{host}:{port}");
        let tcp = TcpStream::connect(&addr)
            .await
            .map_err(|e| UpstreamError::Connect(format!("tcp connect {addr}: {e}")))?;
        let peer = tcp
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".into());
        let counted = ByteCountStream::new(tcp, counter.clone());
        client_async_tls_with_config(request, counted, Some(ws_config), None)
            .await
            .map_err(|e| UpstreamError::Connect(e.to_string()))
            .map(|(sock, response)| ((sock, response), peer))
    };
    let ((sock, _response), peer) = tokio::time::timeout(config.connect_timeout, connect_fut)
        .await
        .map_err(|_| UpstreamError::Connect("connect timeout".into()))??;

    // Keep `sock` un-split for the whole session (same invariant as
    // spacetimedb-relay `relay-upstream` / bitcraft-relay `wire.rs`).
    // Splitting read/write means auto-Pongs queued during a multi-GiB
    // fragmented Binary never flush until the write half is polled —
    // upstream's ~30s ping timeout then RSTs the connection mid-seed.
    log::info!(
        "public-mirror: upstream websocket established (database={}, peer={peer})",
        config.database
    );
    status.set_connected();

    let mut ping_interval = tokio::time::interval(CLIENT_PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick.
    ping_interval.tick().await;

    // Probe timers start dormant (`probe_enabled = false`) until the live
    // loop; subscribe already has its own stall timeout on socket bytes.
    let mut probe_interval = tokio::time::interval_at(tokio::time::Instant::now() + PROBE_INTERVAL, PROBE_INTERVAL);
    probe_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut probe_deadline = tokio::time::interval(PROBE_DEADLINE_POLL);
    probe_deadline.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick.
    probe_deadline.tick().await;

    let mut ctx = SessionCtx {
        sock,
        ping: ping_interval,
        probe: probe_interval,
        probe_deadline,
        probe_enabled: false,
        probe_answered: true,
        probe_sent_at: None,
        applier: Applier::default(),
        row_types: Arc::new(row_types),
        on_update,
        status,
        observers: SessionObservers {
            database: Arc::from(config.database.as_str()),
            generation,
            registry: observers,
        },
        counter,
        last_event_at: None,
        max_event_gap: Duration::ZERO,
        probe_sent_bytes: 0,
        probe_late_warned: false,
        pace_seeds,
        deferred: VecDeque::new(),
    };

    let result = mirror_session(
        &mut ctx,
        tables,
        live_started,
        failed_table,
        subscribe_permit,
        coordinator_permit,
        completed_tables,
    )
    .await;

    // Apply whatever was already received and decoded before tearing down, so a
    // reconnect's re-seed starts from the freshest local state and the next
    // session's applies cannot interleave with this one's.
    ctx.drain_applier().await;
    result
}

/// Run one connected session: identity handshake, per-table subscribe, live loop.
async fn mirror_session<S>(
    ctx: &mut SessionCtx<S>,
    tables: &[String],
    live_started: &mut Option<tokio::time::Instant>,
    failed_table: &mut Option<String>,
    subscribe_permit: OwnedSemaphorePermit,
    coordinator_permit: Option<CoordinatorPermit>,
    completed_tables: Arc<Mutex<HashSet<String>>>,
) -> Result<(), UpstreamError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    ctx.await_identity_token().await?;

    // Subscriptions are per WebSocket connection. Always SubscribeMulti every
    // table on a new socket. `completed_tables` is within-session seed-apply
    // bookkeeping only — the reconnect loop clears it and cold-resets local
    // state (kick clients + flush tables) before re-acquiring the subscribe gate.
    completed_tables.lock().await.clear();

    if ctx.pace_seeds {
        log::info!(
            "public-mirror: re-seed pacing active (database={}) — each table's seed applies locally before the next downloads",
            ctx.observers.database
        );
    }

    for (idx, table) in tables.iter().enumerate() {
        if let Err(e) = subscribe_table(ctx, table, idx, tables.len()).await {
            *failed_table = Some(table.clone());
            return Err(e);
        }
        // Paced re-seeds: let this table's seed finish applying before the
        // next one downloads, so the re-seeding region's decode/apply burst
        // stays bounded to one table while other regions' live ingest needs
        // the CPU. Client Pings keep the socket byte counter moving, so the
        // subscribe stall timeout cannot fire while we wait.
        if ctx.pace_seeds {
            await_all_seeds_applied(ctx, &completed_tables).await?;
        }
    }

    // All wire seeds received — release the gate so another mirror can start
    // downloading while this session drains its FIFO apply queue.
    drop(subscribe_permit);
    drop(coordinator_permit);

    await_all_seeds_applied(ctx, &completed_tables).await?;

    // Observers see `on_live` strictly after every seed dispatch (the applier
    // drains before `await_all_seeds_applied` returns) and before the mirror
    // accepts clients — so an embedded cache publishes its snapshot before
    // downstream traffic can query it.
    if let Err(e) = ctx.observers.dispatch_live().await {
        return Err(UpstreamError::Apply(format!("observer on_live: {e:#}")));
    }

    log::info!(
        "public-mirror: all {} tables subscribed; entering live update loop",
        tables.len()
    );
    ctx.status.set_live();
    *live_started = Some(tokio::time::Instant::now());

    ctx.live_loop().await
}

/// Subscribe one table: send `SubscribeMulti`, await the wire seed snapshot,
/// enqueue it for FIFO apply, and return — **without** waiting for local apply.
///
/// The next table's subscribe is sent while prior seeds drain from the apply
/// queue. Ordering is preserved because [`Applier`] is strictly FIFO.
async fn subscribe_table<S>(ctx: &mut SessionCtx<S>, table: &str, idx: usize, total: usize) -> Result<(), UpstreamError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request_id = (idx as u32).saturating_add(1);
    let query_id = request_id;
    let query = format!("SELECT * FROM {table}");
    let frame = encode_subscribe_multi(request_id, query_id, &query)?;
    log::info!(
        "public-mirror: subscribe [{}/{}] {table} (request_id={request_id})",
        idx + 1,
        total
    );
    ctx.status.set_subscribing_table(table.to_owned());
    ctx.sock.send(Message::Binary(frame.into())).await?;

    let (mut tables_ops, n_rows, wire_bytes) = ctx.await_seed(query_id, table).await?;
    log::info!("public-mirror: SubscribeMultiApplied for {table} ({n_rows} seed rows, {wire_bytes} wire bytes)");

    // An empty seed must still clear the local table: after a reconnect the
    // previous session's rows may be stale (upstream table now empty).
    if tables_ops.is_empty() {
        tables_ops.push(UpstreamTableOps {
            table_name: table.to_owned(),
            deletes: Vec::new(),
            delete_bytes: Vec::new(),
            inserts: Vec::new(),
        });
    }
    ctx.applier.enqueue_seed(
        table.to_owned(),
        UpstreamUpdate {
            provenance: None,
            tables: tables_ops,
            is_seed: true,
        },
        n_rows as u64,
        (idx as u32).saturating_add(1),
    );
    ctx.applier.maybe_start(&ctx.on_update, &ctx.status);

    // Now that the seed is queued, replay frames that arrived while it was
    // decoding off-thread. These are post-snapshot updates: applying them
    // before the seed would let the truncate-and-seed erase their effects.
    ctx.drain_deferred_background().await?;

    Ok(())
}

/// Wait until every enqueued seed has committed locally, servicing the socket
/// and interleaved live TUs the whole time.
async fn await_all_seeds_applied<S>(
    ctx: &mut SessionCtx<S>,
    completed_tables: &Arc<Mutex<HashSet<String>>>,
) -> Result<(), UpstreamError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    while ctx.applier.has_pending_seeds() {
        match ctx.next_event().await? {
            Event::Frame(frame) => ctx.handle_frames_from(frame).await?,
            Event::Applied => ctx.flush_completed_seeds(completed_tables).await,
            Event::Tick | Event::Probe => {}
        }
    }
    ctx.flush_completed_seeds(completed_tables).await;
    Ok(())
}

/// An event produced by [`SessionCtx::next_event`].
enum Event {
    /// A binary frame arrived (not yet decoded).
    Frame(Bytes),
    /// An in-flight apply finished (bookkeeping already done).
    Applied,
    /// The ping interval ticked (a client Ping was sent).
    Tick,
    /// The liveness probe was sent or its deadline was checked (no frame).
    Probe,
}

/// Raw select outcome, before any `SessionCtx` bookkeeping. Keeping borrows out
/// of the `select!` handlers sidesteps the macro's borrow limitations.
enum RawEvent {
    ApplyFinished(Result<(), anyhow::Error>),
    Frame(Bytes),
    Tick,
    ProbeSend,
    ProbeCheck,
}

/// One connected upstream session: socket, ping timer, apply queue, and
/// decode/apply context.
struct SessionCtx<S> {
    sock: WebSocketStream<S>,
    ping: tokio::time::Interval,
    /// Fires every [`PROBE_INTERVAL`] once [`Self::probe_enabled`].
    probe: tokio::time::Interval,
    /// Polls an in-flight probe against [`PROBE_TIMEOUT`].
    probe_deadline: tokio::time::Interval,
    /// Armed only in the live update loop (after all tables subscribed).
    probe_enabled: bool,
    /// `true` = no probe in flight (or last probe was answered).
    probe_answered: bool,
    probe_sent_at: Option<tokio::time::Instant>,
    applier: Applier,
    row_types: Arc<HashMap<String, ProductType>>,
    on_update: ApplyFn,
    status: MirrorStatusHandle,
    observers: SessionObservers,
    counter: ByteCounter,
    /// When `next_event` last completed; gaps mean the session task was not
    /// polled (shared-runtime worker starvation) long enough to threaten the
    /// ping/probe deadlines.
    last_event_at: Option<tokio::time::Instant>,
    /// Largest inter-poll gap observed this session.
    max_event_gap: Duration,
    /// `counter.bytes_total()` when the in-flight probe was sent — at deadline
    /// time distinguishes "upstream silent" from "response arrived but was
    /// processed late" (a starved-runtime false timeout).
    probe_sent_bytes: u64,
    /// Whether the current probe's "late but receiving" warning has fired
    /// (avoid re-warning every [`PROBE_DEADLINE_POLL`] while it lags).
    probe_late_warned: bool,
    /// Re-seed pacing (reconnects only): wait for each table's seed to apply
    /// locally before subscribing the next, so a re-seeding region's decode
    /// and apply bursts cannot pile up behind live ingest. Cold starts keep
    /// the pipelined behavior — nothing is live yet to starve.
    pace_seeds: bool,
    /// Frames received while a heavy frame is decoding off-thread.
    /// Applying them immediately would reorder them **ahead of the seed**
    /// they arrived after (the truncate-and-seed then erases their effects,
    /// leaving stale rows whose next update hits a unique-constraint
    /// violation). They are replayed in arrival order once the decoded seed
    /// (or non-seed message) has been routed.
    deferred: VecDeque<Bytes>,
}

/// In-process observer context for one session (see [`crate::observer`]).
struct SessionObservers {
    database: Arc<str>,
    generation: u64,
    registry: Option<Arc<MirrorObserverRegistry>>,
}

impl SessionObservers {
    async fn dispatch_live(&self) -> anyhow::Result<()> {
        match &self.registry {
            Some(registry) => registry.dispatch_live(&self.database, self.generation).await,
            None => Ok(()),
        }
    }
}

impl<S> SessionCtx<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Wait for the next session event, servicing everything concurrently:
    /// socket reads (which also flush auto-Pongs), client Pings, the
    /// application-level liveness probe (when armed), and the in-flight apply
    /// future. **Never blocks socket servicing on an apply.**
    async fn next_event(&mut self) -> Result<Event, UpstreamError> {
        let Self {
            sock,
            ping,
            probe,
            probe_deadline,
            probe_enabled,
            applier,
            ..
        } = self;
        let probe_armed = *probe_enabled;
        // Biased order: finished applies first (frees the queue), then the ping
        // timer (so a continuous stream of ready frames cannot postpone client
        // Pings indefinitely), then the liveness probe, then the socket.
        let raw = tokio::select! {
            biased;
            result = poll_in_flight(&mut applier.in_flight), if applier.in_flight.is_some() => {
                RawEvent::ApplyFinished(result)
            }
            _ = ping.tick() => RawEvent::Tick,
            _ = probe.tick(), if probe_armed => RawEvent::ProbeSend,
            _ = probe_deadline.tick(), if probe_armed => RawEvent::ProbeCheck,
            msg = next_binary(sock) => RawEvent::Frame(msg?),
        };
        let result = match raw {
            RawEvent::ApplyFinished(result) => {
                self.applier.finish_in_flight(result, &self.status)?;
                self.applier.maybe_start(&self.on_update, &self.status);
                Ok(Event::Applied)
            }
            RawEvent::Frame(frame) => Ok(Event::Frame(frame)),
            RawEvent::Tick => {
                send_client_ping(&mut self.sock).await?;
                Ok(Event::Tick)
            }
            RawEvent::ProbeSend => {
                send_liveness_probe(&mut self.sock).await?;
                self.probe_answered = false;
                self.probe_sent_at = Some(tokio::time::Instant::now());
                self.probe_sent_bytes = self.counter.bytes_total();
                self.probe_late_warned = false;
                Ok(Event::Probe)
            }
            RawEvent::ProbeCheck => {
                if !self.probe_answered {
                    if let Some(sent_at) = self.probe_sent_at {
                        let elapsed = sent_at.elapsed();
                        if elapsed >= PROBE_TIMEOUT {
                            let bytes_since_probe = self
                                .counter
                                .bytes_total()
                                .saturating_sub(self.probe_sent_bytes);
                            if bytes_since_probe == 0 {
                                // Socket silent since the probe was sent: the
                                // upstream genuinely stopped responding.
                                log::warn!(
                                    "public-mirror: liveness probe timed out after {}s — forcing reconnect (database={}, socket silent since probe send — upstream non-response, max event gap {:?})",
                                    PROBE_TIMEOUT.as_secs(),
                                    self.observers.database,
                                    self.max_event_gap
                                );
                                return Err(UpstreamError::ProbeTimeout(PROBE_TIMEOUT.as_secs()));
                            }
                            if elapsed >= PROBE_LATE_HARD_TIMEOUT {
                                // Bytes flow but the probe response was never
                                // processed even given a generous starvation
                                // allowance — treat as a real failure.
                                log::warn!(
                                    "public-mirror: liveness probe unanswered for {elapsed:?} despite {bytes_since_probe}B received — exceeding hard cap, forcing reconnect (database={}, max event gap {:?})",
                                    self.observers.database,
                                    self.max_event_gap
                                );
                                return Err(UpstreamError::ProbeTimeout(PROBE_TIMEOUT.as_secs()));
                            }
                            // Late but receiving: a starved runtime, not a dead
                            // upstream. Tolerate (once per probe) instead of
                            // cold-resetting into the re-seed cascade.
                            if !self.probe_late_warned {
                                self.probe_late_warned = true;
                                log::warn!(
                                    "public-mirror: liveness probe late by {elapsed:?} but socket is receiving ({bytes_since_probe}B since send) — tolerating starved runtime, not killing the session (database={}, max event gap {:?})",
                                    self.observers.database,
                                    self.max_event_gap
                                );
                            }
                        }
                    }
                }
                Ok(Event::Probe)
            }
        };
        self.note_event_gap();
        result
    }

    /// Record the gap since the previous `next_event` completion. A gap means
    /// the session task was not polled at all for that long — the direct
    /// signature of shared-runtime worker starvation — at which scale the
    /// client-ping (10s), upstream-pong (~30s), and probe (30s) deadlines
    /// slip and the upstream edge RSTs us.
    fn note_event_gap(&mut self) {
        let now = tokio::time::Instant::now();
        if let Some(last) = self.last_event_at.replace(now) {
            let gap = now - last;
            if gap > self.max_event_gap {
                self.max_event_gap = gap;
            }
            if gap >= EVENT_LOOP_LAG_WARN {
                log::warn!(
                    "public-mirror: event loop gap {gap:?} between polls (database={}, max {:?}) — shared-runtime worker starvation suspected",
                    self.observers.database,
                    self.max_event_gap
                );
            }
        }
    }

    /// Route an already-decoded background message: enqueue live TUs, mark
    /// probe responses, surface subscription errors, ignore the rest.
    fn apply_decoded_background(&mut self, decoded: DecodedBackground) -> Result<(), UpstreamError> {
        match decoded {
            DecodedBackground::Update(Some(update)) => self.enqueue_live(update),
            DecodedBackground::Update(None) => Ok(()),
            DecodedBackground::ProbeResponse => {
                // Liveness probe reply — any response (ok or error) proves the
                // upstream application is still processing requests.
                if !self.probe_answered {
                    log::debug!("public-mirror: liveness probe response received");
                    self.probe_answered = true;
                    self.probe_sent_at = None;
                }
                Ok(())
            }
            DecodedBackground::SubscriptionError(e) => Err(UpstreamError::Subscription(e)),
            DecodedBackground::Ignored(name) => {
                log::debug!("public-mirror: ignoring message: {name}");
                Ok(())
            }
        }
    }

    /// Decode one background frame and route it, then any frames deferred
    /// while it decoded off-thread — strictly in arrival order. Heavy frames
    /// (see [`is_heavy_frame`]) decode on the blocking pool: keeping the
    /// per-poll work on the shared Tokio workers tiny is what keeps
    /// Pings/Pongs and probe responses flowing when many mirrors ingest at
    /// once on a saturated host.
    async fn handle_frames_from(&mut self, first: Bytes) -> Result<(), UpstreamError> {
        let mut current = Some(first);
        loop {
            let frame = match current.take() {
                Some(f) => f,
                None => match self.deferred.pop_front() {
                    Some(f) => f,
                    None => return Ok(()),
                },
            };
            let decoded = if is_heavy_frame(&frame) {
                let row_types = Arc::clone(&self.row_types);
                self.run_blocking_decode(move || decode_background_frame(&frame, &row_types))
                    .await?
            } else {
                decode_background_frame(&frame, &self.row_types)?
            };
            self.apply_decoded_background(decoded)?;
        }
    }

    /// Like [`Self::handle_frames_from`], but classifies with the awaited
    /// `query_id` so the session's own wire seed is never swallowed as a
    /// background message. Returns the awaited seed if found (remaining
    /// deferred frames are left for the caller to replay **after** the seed
    /// is enqueued), or `None` once every deferred frame is routed.
    async fn handle_frames_from_seed_wait(
        &mut self,
        first: Bytes,
        query_id: u32,
    ) -> Result<Option<(Vec<UpstreamTableOps>, usize, usize)>, UpstreamError> {
        let mut current = Some(first);
        loop {
            let frame = match current.take() {
                Some(f) => f,
                None => match self.deferred.pop_front() {
                    Some(f) => f,
                    None => return Ok(None),
                },
            };
            let decoded = if is_heavy_frame(&frame) {
                let row_types = Arc::clone(&self.row_types);
                self.run_blocking_decode(move || decode_seed_wait_frame(&frame, query_id, &row_types))
                    .await?
            } else {
                decode_seed_wait_frame(&frame, query_id, &self.row_types)?
            };
            match decoded {
                SeedWaitDecode::Seed {
                    tables_ops,
                    n_rows,
                    wire_bytes,
                } => return Ok(Some((tables_ops, n_rows, wire_bytes))),
                SeedWaitDecode::WrongQueryId { got, want } => {
                    log::warn!("public-mirror: unexpected SubscribeMultiApplied query_id={got} (want {want})");
                }
                SeedWaitDecode::Background(bg) => self.apply_decoded_background(bg)?,
            }
        }
    }

    /// Replay frames deferred during offloaded decodes (background mode).
    async fn drain_deferred_background(&mut self) -> Result<(), UpstreamError> {
        if let Some(frame) = self.deferred.pop_front() {
            self.handle_frames_from(frame).await?;
        }
        Ok(())
    }

    /// Run `decode` on the blocking pool (deprioritized) while continuing to
    /// service the socket, Pings, probes, and in-flight applies. Frames that
    /// arrive meanwhile are deferred; the callers above replay them in
    /// arrival order once the decode returns.
    async fn run_blocking_decode<T, F>(&mut self, decode: F) -> Result<T, UpstreamError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, UpstreamError> + Send + 'static,
    {
        let mut join = tokio::task::spawn_blocking(move || {
            deprioritize_mirror_background_thread();
            decode()
        });
        loop {
            tokio::select! {
                biased;
                joined = &mut join => {
                    return joined
                        .map_err(|e| UpstreamError::Decode(format!("offloaded decode join failed: {e}")))?;
                }
                ev = self.next_event() => {
                    if let Event::Frame(frame) = ev? {
                        self.deferred.push_back(frame);
                    }
                }
            }
        }
    }

    fn enqueue_live(&mut self, update: UpstreamUpdate) -> Result<(), UpstreamError> {
        self.applier.enqueue_live(update)?;
        self.applier.maybe_start(&self.on_update, &self.status);
        Ok(())
    }

    /// Move completed seed table names into `completed_tables` (within-session
    /// apply bookkeeping; cleared on reconnect cold-reset).
    async fn flush_completed_seeds(&mut self, completed_tables: &Arc<Mutex<HashSet<String>>>) {
        for table in self.applier.drain_completed_seed_tables() {
            completed_tables.lock().await.insert(table);
        }
    }

    /// Wait for the post-connect `IdentityToken` (bounded by
    /// [`IDENTITY_TOKEN_TIMEOUT`]).
    async fn await_identity_token(&mut self) -> Result<(), UpstreamError> {
        let deadline = tokio::time::Instant::now() + IDENTITY_TOKEN_TIMEOUT;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(UpstreamError::Connect(format!(
                    "timed out waiting for IdentityToken after {IDENTITY_TOKEN_TIMEOUT:?}"
                )));
            }
            match self.next_event().await? {
                Event::Frame(frame) => {
                    let server = decode_server_message(&frame)?;
                    match server {
                        ServerMessage::IdentityToken(it) => {
                            log::info!("public-mirror: identity token received (identity={})", it.identity);
                            return Ok(());
                        }
                        other => self.apply_decoded_background(classify_background(other, &self.row_types)?)?,
                    }
                }
                Event::Applied | Event::Tick | Event::Probe => {}
            }
        }
    }

    /// Wait for `SubscribeMultiApplied` for `query_id`.
    ///
    /// The timeout is **stall-based**: the wait only fails when the socket has
    /// been silent for [`SUBSCRIBE_STALL_TIMEOUT`] (or after the generous
    /// absolute [`SUBSCRIBE_TIMEOUT`] cap). A multi-GiB seed that keeps
    /// arriving — however slowly — is never killed mid-transfer.
    async fn await_seed(
        &mut self,
        query_id: u32,
        table: &str,
    ) -> Result<(Vec<UpstreamTableOps>, usize, usize), UpstreamError> {
        let started = tokio::time::Instant::now();
        let mut stall = StallTracker::new(&self.counter);
        loop {
            let elapsed = started.elapsed();
            if elapsed >= SUBSCRIBE_TIMEOUT {
                return Err(UpstreamError::SubscribeTimeout(table.to_owned(), elapsed));
            }
            if let Some(stalled_for) = stall.stalled_for(&self.counter, SUBSCRIBE_STALL_TIMEOUT) {
                return Err(UpstreamError::SubscribeStalled(table.to_owned(), stalled_for));
            }
            match self.next_event().await? {
                Event::Frame(frame) => {
                    if let Some(seed) = self.handle_frames_from_seed_wait(frame, query_id).await? {
                        // Deferred frames (if any) are replayed by the caller,
                        // *after* it enqueues this seed.
                        return Ok(seed);
                    }
                }
                Event::Applied | Event::Tick | Event::Probe => {}
            }
        }
    }

    /// Live update loop — read, decode, enqueue; applies complete concurrently.
    /// Arms the application-level OneOffQuery liveness probe (legacy relay
    /// behaviour) so a silent-but-open upstream WebSocket forces reconnect.
    async fn live_loop(&mut self) -> Result<(), UpstreamError> {
        self.probe_enabled = true;
        self.probe_answered = true;
        self.probe_sent_at = None;
        // First probe fires one full interval after live starts.
        self.probe.reset_at(tokio::time::Instant::now() + PROBE_INTERVAL);
        loop {
            if let Event::Frame(frame) = self.next_event().await? {
                self.handle_frames_from(frame).await?;
            }
        }
    }

    /// Apply everything already queued (socket no longer serviced — the
    /// session is over). Runs after both clean exits and failures so received
    /// data is never dropped and the next session's applies cannot interleave
    /// with this one's.
    async fn drain_applier(&mut self) {
        loop {
            if let Some(in_flight) = self.applier.in_flight.as_mut() {
                let result = in_flight.fut.as_mut().await;
                if let Err(e) = self.applier.finish_in_flight(result, &self.status) {
                    log::warn!("public-mirror: apply error while draining session backlog: {e:#}");
                    self.applier.queue.clear();
                    return;
                }
            } else if !self.applier.queue.is_empty() {
                self.applier.maybe_start(&self.on_update, &self.status);
            } else {
                return;
            }
        }
    }
}

/// Poll the in-flight apply future. Only called when one exists (select guard);
/// pends forever otherwise so a spurious poll cannot panic.
async fn poll_in_flight(in_flight: &mut Option<InFlightApply>) -> Result<(), anyhow::Error> {
    match in_flight {
        Some(f) => f.fut.as_mut().await,
        None => std::future::pending().await,
    }
}

/// Tracks socket-level receive progress for the stall timeout.
struct StallTracker {
    last_total: u64,
    changed_at: tokio::time::Instant,
}

impl StallTracker {
    fn new(counter: &ByteCounter) -> Self {
        Self {
            last_total: counter.bytes_total(),
            changed_at: tokio::time::Instant::now(),
        }
    }

    /// Returns how long the socket has been silent if it exceeds `threshold`.
    fn stalled_for(&mut self, counter: &ByteCounter, threshold: Duration) -> Option<Duration> {
        let total = counter.bytes_total();
        if total != self.last_total {
            self.last_total = total;
            self.changed_at = tokio::time::Instant::now();
            return None;
        }
        let silent = self.changed_at.elapsed();
        (silent >= threshold).then_some(silent)
    }
}

/// A decoded update waiting to be applied.
struct PendingApply {
    update: UpstreamUpdate,
    kind: ApplyKind,
    /// Approximate decoded size, for the live backlog cap.
    cost: usize,
}

enum ApplyKind {
    /// One live update; `transactions` is 1 for a provenance-carrying TU.
    Live { transactions: u64 },
    /// A table seed snapshot (applied alone, with progress reporting).
    Seed {
        table_name: String,
        seed_rows: u64,
        table_number: u32,
    },
}

/// A batch of updates currently executing on the mirror's DB thread.
struct InFlightApply {
    fut: BoxFuture<'static, Result<(), anyhow::Error>>,
    transactions: u64,
    seed_table_name: Option<String>,
    seed_table_number: Option<u32>,
    cost: usize,
}

/// FIFO apply queue + the single in-flight apply job.
///
/// Ordering: strictly arrival order. Seeds run alone (one DB job per table);
/// consecutive live updates are batched (up to [`APPLY_BATCH_MAX`]) into one DB
/// job to amortize the cross-thread round trip when a backlog has built up.
/// During pipelined subscribe, multiple seeds may queue while upstream keeps
/// sending; they still apply in table order.
#[derive(Default)]
struct Applier {
    queue: VecDeque<PendingApply>,
    /// Approximate bytes of queued + in-flight *live* updates.
    queued_live_bytes: usize,
    in_flight: Option<InFlightApply>,
    /// Seed table names whose apply jobs have finished (FIFO drain).
    completed_seed_tables: VecDeque<String>,
}

impl Applier {
    fn enqueue_live(&mut self, update: UpstreamUpdate) -> Result<(), UpstreamError> {
        let cost = update_cost(&update);
        self.queued_live_bytes = self.queued_live_bytes.saturating_add(cost);
        if self.queued_live_bytes > LIVE_QUEUE_BYTES_MAX {
            return Err(UpstreamError::Backlog {
                queued: self.queued_live_bytes,
                max: LIVE_QUEUE_BYTES_MAX,
            });
        }
        let transactions = update.provenance.is_some() as u64;
        self.queue.push_back(PendingApply {
            update,
            kind: ApplyKind::Live { transactions },
            cost,
        });
        Ok(())
    }

    fn enqueue_seed(&mut self, table_name: String, update: UpstreamUpdate, seed_rows: u64, table_number: u32) {
        self.queue.push_back(PendingApply {
            update,
            kind: ApplyKind::Seed {
                table_name,
                seed_rows,
                table_number,
            },
            cost: 0,
        });
    }

    fn has_pending_seeds(&self) -> bool {
        if self.in_flight.as_ref().is_some_and(|f| f.seed_table_number.is_some()) {
            return true;
        }
        self.queue.iter().any(|p| matches!(p.kind, ApplyKind::Seed { .. }))
    }

    fn drain_completed_seed_tables(&mut self) -> Vec<String> {
        self.completed_seed_tables.drain(..).collect()
    }

    /// Start the next apply job if none is in flight.
    fn maybe_start(&mut self, on_update: &ApplyFn, status: &MirrorStatusHandle) {
        if self.in_flight.is_some() {
            return;
        }
        let Some(front) = self.queue.front() else {
            return;
        };
        if matches!(front.kind, ApplyKind::Seed { .. }) {
            let pending = self.queue.pop_front().expect("front checked above");
            let ApplyKind::Seed {
                table_name,
                seed_rows,
                table_number,
            } = pending.kind
            else {
                unreachable!("seed front");
            };
            let progress = status.set_applying_seed(seed_rows);
            let fut = on_update(vec![pending.update], Some(progress));
            self.in_flight = Some(InFlightApply {
                fut,
                transactions: 0,
                seed_table_name: Some(table_name),
                seed_table_number: Some(table_number),
                cost: pending.cost,
            });
            return;
        }
        // Batch consecutive live updates into one DB job.
        let mut updates = Vec::new();
        let mut transactions = 0u64;
        let mut cost = 0usize;
        while updates.len() < APPLY_BATCH_MAX {
            match self.queue.front() {
                Some(p) if matches!(p.kind, ApplyKind::Live { .. }) => {
                    let p = self.queue.pop_front().expect("front checked above");
                    if let ApplyKind::Live { transactions: t } = p.kind {
                        transactions += t;
                    }
                    cost = cost.saturating_add(p.cost);
                    updates.push(p.update);
                }
                _ => break,
            }
        }
        let fut = on_update(updates, None);
        self.in_flight = Some(InFlightApply {
            fut,
            transactions,
            seed_table_name: None,
            seed_table_number: None,
            cost,
        });
    }

    /// Bookkeeping for a completed in-flight apply.
    fn finish_in_flight(
        &mut self,
        result: Result<(), anyhow::Error>,
        status: &MirrorStatusHandle,
    ) -> Result<(), UpstreamError> {
        let done = self.in_flight.take().expect("finish_in_flight without in-flight apply");
        self.queued_live_bytes = self.queued_live_bytes.saturating_sub(done.cost);
        result.map_err(|e| UpstreamError::Apply(format!("{e:#}")))?;
        if done.transactions > 0 {
            status.inc_transactions_by(done.transactions);
        }
        if let Some(table_number) = done.seed_table_number {
            status.set_table_live(table_number);
            if let Some(table_name) = done.seed_table_name {
                self.completed_seed_tables.push_back(table_name);
            }
        }
        Ok(())
    }
}

/// Approximate decoded size of an update, for the backlog cap.
fn update_cost(update: &UpstreamUpdate) -> usize {
    update
        .tables
        .iter()
        .map(|t| {
            t.inserts.iter().map(Bytes::len).sum::<usize>()
                + t.deletes.len() * DELETE_COST_ESTIMATE
                + t.table_name.len()
        })
        .sum()
}

async fn send_client_ping<S>(sock: &mut WebSocketStream<S>) -> Result<(), UpstreamError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    sock.send(Message::Ping(Bytes::new()))
        .await
        .map_err(UpstreamError::from)
}

async fn send_liveness_probe<S>(sock: &mut WebSocketStream<S>) -> Result<(), UpstreamError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = encode_one_off_query(PROBE_MESSAGE_ID, "SELECT 1")?;
    log::debug!("public-mirror: sending liveness probe");
    sock.send(Message::Binary(frame.into()))
        .await
        .map_err(UpstreamError::from)
}

fn encode_one_off_query(message_id: &[u8], query: &str) -> Result<Vec<u8>, UpstreamError> {
    let msg = ClientMessage::<Box<[u8]>>::OneOffQuery(OneOffQuery {
        message_id: message_id.to_vec().into_boxed_slice(),
        query_string: query.to_string().into_boxed_str(),
    });
    bsatn::to_vec(&msg).map_err(|e| UpstreamError::Encode(e.to_string()))
}

/// A background (non-seed-wait) message after decoding and TU/TUL
/// conversion — the CPU-heavy part — ready to route.
#[derive(Debug)]
enum DecodedBackground {
    Update(Option<UpstreamUpdate>),
    ProbeResponse,
    SubscriptionError(String),
    Ignored(&'static str),
}

/// Classify a decoded server message for the background path, doing the
/// expensive TU/TUL conversion (Brotli row lists, delete-row decode) eagerly
/// so it can run on the blocking pool instead of a Tokio worker.
fn classify_background(
    server: ServerMessage<BsatnFormat>,
    row_types: &HashMap<String, ProductType>,
) -> Result<DecodedBackground, UpstreamError> {
    Ok(match server {
        ServerMessage::TransactionUpdate(tu) => DecodedBackground::Update(tu_to_update(tu, row_types)?),
        ServerMessage::TransactionUpdateLight(tul) => DecodedBackground::Update(tul_to_update(tul, row_types)?),
        ServerMessage::OneOffQueryResponse(_) => DecodedBackground::ProbeResponse,
        ServerMessage::SubscriptionError(err) => DecodedBackground::SubscriptionError(err.error.to_string()),
        other => DecodedBackground::Ignored(variant_name(&other)),
    })
}

fn decode_background_frame(
    frame: &[u8],
    row_types: &HashMap<String, ProductType>,
) -> Result<DecodedBackground, UpstreamError> {
    classify_background(decode_server_message(frame)?, row_types)
}

/// Whether a frame must be decompressed + decoded on the blocking pool: any
/// frame at/above [`OFFLOAD_DECODE_BYTES`], or a compressed frame at/above
/// [`OFFLOAD_COMPRESSED_BYTES`]. Keeping these off the workers is what keeps
/// Pong/Ping/probe servicing alive under ingest saturation.
fn is_heavy_frame(frame: &[u8]) -> bool {
    if frame.len() >= OFFLOAD_DECODE_BYTES {
        return true;
    }
    matches!(
        frame.first(),
        Some(&SERVER_MSG_COMPRESSION_TAG_BROTLI) | Some(&SERVER_MSG_COMPRESSION_TAG_GZIP)
    ) && frame.len() >= OFFLOAD_COMPRESSED_BYTES
}

enum SeedWaitDecode {
    Seed {
        tables_ops: Vec<UpstreamTableOps>,
        n_rows: usize,
        wire_bytes: usize,
    },
    WrongQueryId {
        got: u32,
        want: u32,
    },
    Background(DecodedBackground),
}

/// Decode one binary frame received while awaiting `SubscribeMultiApplied`.
fn decode_seed_wait_frame(
    frame: &[u8],
    query_id: u32,
    row_types: &HashMap<String, ProductType>,
) -> Result<SeedWaitDecode, UpstreamError> {
    let wire_bytes = frame.len();
    let server = decode_server_message(frame)?;
    match server {
        ServerMessage::SubscribeMultiApplied(sma) => {
            if sma.query_id.id != query_id {
                return Ok(SeedWaitDecode::WrongQueryId {
                    got: sma.query_id.id,
                    want: query_id,
                });
            }
            let tables_ops = database_update_to_ops(&sma.update, row_types, /*seed*/ true)?;
            let n_rows = tables_ops.iter().map(|t| t.inserts.len()).sum();
            Ok(SeedWaitDecode::Seed {
                tables_ops,
                n_rows,
                wire_bytes,
            })
        }
        other => Ok(SeedWaitDecode::Background(classify_background(other, row_types)?)),
    }
}

fn tu_to_update(
    tu: TransactionUpdate<BsatnFormat>,
    row_types: &HashMap<String, ProductType>,
) -> Result<Option<UpstreamUpdate>, UpstreamError> {
    let UpdateStatus::Committed(db) = tu.status else {
        return Ok(None);
    };
    let tables = database_update_to_ops(&db, row_types, false)?;
    if tables.is_empty() {
        return Ok(None);
    }
    let provenance = UpstreamProvenance {
        reducer_name: tu.reducer_call.reducer_name.to_string(),
        caller_identity: tu.caller_identity,
        caller_connection_id: tu.caller_connection_id,
        timestamp: tu.timestamp,
        request_id: tu.reducer_call.request_id,
        args: Bytes::copy_from_slice(tu.reducer_call.args.as_ref()),
    };
    Ok(Some(UpstreamUpdate {
        provenance: Some(provenance),
        tables,
        is_seed: false,
    }))
}

fn tul_to_update(
    tul: TransactionUpdateLight<BsatnFormat>,
    row_types: &HashMap<String, ProductType>,
) -> Result<Option<UpstreamUpdate>, UpstreamError> {
    let tables = database_update_to_ops(&tul.update, row_types, false)?;
    if tables.is_empty() {
        return Ok(None);
    }
    Ok(Some(UpstreamUpdate {
        provenance: None,
        tables,
        is_seed: false,
    }))
}

fn build_row_types(module_def: &ModuleDef) -> Result<HashMap<String, ProductType>, UpstreamError> {
    let mut map = HashMap::new();
    let typespace = module_def.typespace();
    for table in module_def.tables() {
        let name = table.name.to_string();
        let alg = typespace
            .get(table.product_type_ref)
            .ok_or_else(|| UpstreamError::UnknownTable(name.clone()))?;
        // Inline AlgebraicType::Ref… so ProductValue::decode (empty typespace) works.
        // BitCraft row types nest refs (e.g. timestamps / enums); leaving them unresolved
        // panics on the first live delete with "len is 0 but the index is N".
        let resolved = WithTypespace::new(typespace, alg)
            .resolve_refs()
            .map_err(|e| UpstreamError::Decode(format!("resolve row type for {name}: {e}")))?;
        let product = resolved
            .as_product()
            .ok_or_else(|| UpstreamError::NotProduct(name.clone()))?
            .clone();
        map.insert(name, product);
    }
    Ok(map)
}

fn database_update_to_ops(
    db: &DatabaseUpdate<BsatnFormat>,
    row_types: &HashMap<String, ProductType>,
    seed: bool,
) -> Result<Vec<UpstreamTableOps>, UpstreamError> {
    let mut out = Vec::with_capacity(db.tables.len());
    for table in &db.tables {
        let table_name = table.table_name.to_string();
        let row_ty = row_types
            .get(&table_name)
            .ok_or_else(|| UpstreamError::UnknownTable(table_name.clone()))?;

        let mut inserts = Vec::new();
        let mut deletes = Vec::new();
        let mut delete_bytes = Vec::new();
        for update in &table.updates {
            let qu = query_update_owned(update)?;
            // `BsatnRowList` iteration yields zero-copy `Bytes` slices into the
            // shared row blob — one allocation per table, not one per row.
            inserts.extend(&qu.inserts);
            delete_bytes.extend(&qu.deletes);
            if !seed {
                for row in &qu.deletes {
                    let mut bytes: &[u8] = row.as_ref();
                    let pv = ProductValue::decode(row_ty, &mut bytes)
                        .map_err(|e| UpstreamError::Decode(format!("delete row for {table_name}: {e}")))?;
                    deletes.push(pv);
                }
            }
        }
        if inserts.is_empty() && deletes.is_empty() {
            continue;
        }
        out.push(UpstreamTableOps {
            table_name,
            deletes,
            delete_bytes,
            inserts,
        });
    }
    Ok(out)
}

/// Materialize a possibly-compressed query update. The uncompressed arm is a
/// cheap clone (`Bytes` refcount bumps); compressed arms decompress + decode.
fn query_update_owned(u: &CompressableQueryUpdate<BsatnFormat>) -> Result<QueryUpdate<BsatnFormat>, UpstreamError> {
    match u {
        CompressableQueryUpdate::Uncompressed(qu) => Ok(qu.clone()),
        CompressableQueryUpdate::Brotli(bytes) => {
            let raw = brotli_decompress(bytes)?;
            bsatn::from_slice::<QueryUpdate<BsatnFormat>>(&raw)
                .map_err(|e| UpstreamError::Decode(format!("brotli query update: {e}")))
        }
        CompressableQueryUpdate::Gzip(bytes) => {
            let raw = gzip_decompress(bytes)?;
            bsatn::from_slice::<QueryUpdate<BsatnFormat>>(&raw)
                .map_err(|e| UpstreamError::Decode(format!("gzip query update: {e}")))
        }
    }
}

fn brotli_decompress(data: &[u8]) -> Result<Vec<u8>, UpstreamError> {
    let mut out = Vec::new();
    let mut reader = data;
    brotli::BrotliDecompress(&mut reader, &mut out).map_err(|e| UpstreamError::Decode(format!("brotli: {e}")))?;
    Ok(out)
}

fn gzip_decompress(data: &[u8]) -> Result<Vec<u8>, UpstreamError> {
    use std::io::Read;
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(data)
        .read_to_end(&mut out)
        .map_err(|e| UpstreamError::Decode(format!("gzip: {e}")))?;
    Ok(out)
}

fn encode_subscribe_multi(request_id: u32, query_id: u32, query: &str) -> Result<Vec<u8>, UpstreamError> {
    let msg = ClientMessage::<Box<[u8]>>::SubscribeMulti(SubscribeMulti {
        query_strings: vec![query.to_string().into_boxed_str()].into_boxed_slice(),
        request_id,
        query_id: QuerySetId::new(query_id),
    });
    bsatn::to_vec(&msg).map_err(|e| UpstreamError::Encode(e.to_string()))
}

/// Decode a server frame, transparently handling whole-message brotli/gzip
/// compression (leading tag byte).
fn decode_server_message(data: &[u8]) -> Result<ServerMessage<BsatnFormat>, UpstreamError> {
    let Some((&tag, payload)) = data.split_first() else {
        return Err(UpstreamError::FrameTooShort(0));
    };
    let decompressed;
    let payload: &[u8] = match tag {
        SERVER_MSG_COMPRESSION_TAG_NONE => payload,
        SERVER_MSG_COMPRESSION_TAG_BROTLI => {
            decompressed = brotli_decompress(payload)?;
            &decompressed
        }
        SERVER_MSG_COMPRESSION_TAG_GZIP => {
            decompressed = gzip_decompress(payload)?;
            &decompressed
        }
        other => return Err(UpstreamError::UnknownCompression(other)),
    };
    if payload.is_empty() {
        return Err(UpstreamError::FrameTooShort(data.len()));
    }
    bsatn::from_slice::<ServerMessage<BsatnFormat>>(payload).map_err(|e| UpstreamError::Decode(e.to_string()))
}

async fn next_binary<S>(sock: &mut WebSocketStream<S>) -> Result<Bytes, UpstreamError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let Some(msg) = sock.next().await else {
            return Err(UpstreamError::Closed("stream ended".into()));
        };
        match msg? {
            Message::Binary(data) => return Ok(data),
            Message::Close(frame) => {
                let reason = frame
                    .map(|f| format!("{}: {}", f.code, f.reason))
                    .unwrap_or_else(|| "no close frame".into());
                return Err(UpstreamError::Closed(reason));
            }
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Text(t) => {
                log::warn!("public-mirror: unexpected text frame ({} bytes)", t.len());
            }
            Message::Frame(_) => {}
        }
    }
}

fn build_connect_request(
    config: &UpstreamConfig,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, UpstreamError> {
    let mut url = config.host.clone();
    match url.scheme() {
        "ws" | "wss" => {}
        "http" => url
            .set_scheme("ws")
            .map_err(|_| UpstreamError::Url("scheme rewrite failed".into()))?,
        "https" => url
            .set_scheme("wss")
            .map_err(|_| UpstreamError::Url("scheme rewrite failed".into()))?,
        other => return Err(UpstreamError::Url(format!("unsupported scheme: {other}"))),
    }
    {
        let mut path = url.path().trim_end_matches('/').to_string();
        path.push_str("/v1/database/");
        path.push_str(&config.database);
        path.push_str("/subscribe");
        url.set_path(&path);
    }
    // Brotli cuts large seed snapshots ~7–10x on the wire (upstream compresses
    // at its fastest level), which shrinks subscribe-gate hold time and the
    // window in which a mid-seed disconnect forces a full re-seed.
    url.query_pairs_mut().clear().append_pair("compression", "Brotli");

    let mut request = url
        .to_string()
        .into_client_request()
        .map_err(|e| UpstreamError::Url(e.to_string()))?;
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        SUBPROTOCOL_V1
            .parse()
            .map_err(|_| UpstreamError::Url("invalid subprotocol header".into()))?,
    );
    if let Some(token) = &config.auth_token {
        let value = format!("Bearer {token}");
        request.headers_mut().insert(
            AUTHORIZATION,
            value
                .parse()
                .map_err(|_| UpstreamError::Url("invalid auth header".into()))?,
        );
    }
    Ok(request)
}

fn variant_name(msg: &ServerMessage<BsatnFormat>) -> &'static str {
    match msg {
        ServerMessage::InitialSubscription(_) => "InitialSubscription",
        ServerMessage::TransactionUpdate(_) => "TransactionUpdate",
        ServerMessage::TransactionUpdateLight(_) => "TransactionUpdateLight",
        ServerMessage::IdentityToken(_) => "IdentityToken",
        ServerMessage::OneOffQueryResponse(_) => "OneOffQueryResponse",
        ServerMessage::SubscribeApplied(_) => "SubscribeApplied",
        ServerMessage::UnsubscribeApplied(_) => "UnsubscribeApplied",
        ServerMessage::SubscriptionError(_) => "SubscriptionError",
        ServerMessage::SubscribeMultiApplied(_) => "SubscribeMultiApplied",
        ServerMessage::UnsubscribeMultiApplied(_) => "UnsubscribeMultiApplied",
        ServerMessage::ProcedureResult(_) => "ProcedureResult",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::MirrorStatusRegistry;
    use futures::FutureExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_status() -> MirrorStatusHandle {
        let reg = MirrorStatusRegistry::new();
        reg.register(
            &Url::parse("wss://test.example").unwrap(),
            "db",
            Identity::from_claims("public-mirror-v1", "db"),
            1,
        )
    }

    fn live_update(n_tx: bool, insert_bytes: usize) -> UpstreamUpdate {
        UpstreamUpdate {
            provenance: n_tx.then(|| UpstreamProvenance {
                reducer_name: "r".into(),
                caller_identity: Identity::ZERO,
                caller_connection_id: ConnectionId::ZERO,
                timestamp: Timestamp::now(),
                request_id: 0,
                args: Bytes::new(),
            }),
            tables: vec![UpstreamTableOps {
                table_name: "t".into(),
                deletes: Vec::new(),
                delete_bytes: Vec::new(),
                inserts: vec![Bytes::from(vec![0u8; insert_bytes])],
            }],
            is_seed: false,
        }
    }

    fn counting_apply(counter: Arc<AtomicUsize>) -> ApplyFn {
        Arc::new(move |updates, _progress| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(updates.len(), Ordering::SeqCst);
                Ok(())
            }
            .boxed()
        })
    }

    #[tokio::test]
    async fn applier_batches_live_updates_and_isolates_seeds() {
        let status = test_status();
        let applied = Arc::new(AtomicUsize::new(0));
        let on_update = counting_apply(Arc::clone(&applied));
        let mut applier = Applier::default();

        for _ in 0..5 {
            applier.enqueue_live(live_update(true, 8)).unwrap();
        }
        applier.enqueue_seed(
            "seed_table".into(),
            UpstreamUpdate {
                provenance: None,
                tables: Vec::new(),
                is_seed: true,
            },
            0,
            1,
        );
        for _ in 0..3 {
            applier.enqueue_live(live_update(false, 8)).unwrap();
        }

        // Job 1: the 5 live updates batched together.
        applier.maybe_start(&on_update, &status);
        let in_flight = applier.in_flight.as_mut().unwrap();
        let result = in_flight.fut.as_mut().await;
        assert_eq!(applier.in_flight.as_ref().unwrap().transactions, 5);
        applier.finish_in_flight(result, &status).unwrap();
        assert_eq!(applied.load(Ordering::SeqCst), 5);

        // Job 2: the seed, alone.
        assert!(applier.has_pending_seeds());
        applier.maybe_start(&on_update, &status);
        assert!(applier.in_flight.as_ref().unwrap().seed_table_number == Some(1));
        let result = applier.in_flight.as_mut().unwrap().fut.as_mut().await;
        applier.finish_in_flight(result, &status).unwrap();
        assert!(!applier.has_pending_seeds());
        assert_eq!(applier.drain_completed_seed_tables(), vec!["seed_table".to_string()]);
        assert_eq!(applied.load(Ordering::SeqCst), 6);

        // Job 3: the trailing live updates.
        applier.maybe_start(&on_update, &status);
        let result = applier.in_flight.as_mut().unwrap().fut.as_mut().await;
        applier.finish_in_flight(result, &status).unwrap();
        assert_eq!(applied.load(Ordering::SeqCst), 9);
        assert!(applier.queue.is_empty());
        assert_eq!(applier.queued_live_bytes, 0);
    }

    #[tokio::test]
    async fn applier_tracks_multiple_pending_seeds_in_fifo_order() {
        let status = test_status();
        let on_update = counting_apply(Arc::new(AtomicUsize::new(0)));
        let mut applier = Applier::default();

        for (name, n) in [("t_a", 1u32), ("t_b", 2), ("t_c", 3)] {
            applier.enqueue_seed(
                name.into(),
                UpstreamUpdate {
                    provenance: None,
                    tables: Vec::new(),
                    is_seed: true,
                },
                0,
                n,
            );
        }
        assert!(applier.has_pending_seeds());

        let mut finished = Vec::new();
        for _ in 0..3 {
            applier.maybe_start(&on_update, &status);
            let result = applier.in_flight.as_mut().unwrap().fut.as_mut().await;
            applier.finish_in_flight(result, &status).unwrap();
            finished.extend(applier.drain_completed_seed_tables());
        }
        assert!(!applier.has_pending_seeds());
        assert_eq!(finished, vec!["t_a", "t_b", "t_c"]);
    }

    #[test]
    fn applier_backlog_cap_errors_instead_of_growing() {
        let mut applier = Applier::default();
        // One update just over the cap must trip the backlog error.
        let big = live_update(false, LIVE_QUEUE_BYTES_MAX + 1);
        let err = applier.enqueue_live(big).unwrap_err();
        assert!(matches!(err, UpstreamError::Backlog { .. }));
    }

    #[test]
    fn decode_server_message_rejects_unknown_tag_and_empty() {
        assert!(matches!(
            decode_server_message(&[]),
            Err(UpstreamError::FrameTooShort(0))
        ));
        assert!(matches!(
            decode_server_message(&[9, 1, 2]),
            Err(UpstreamError::UnknownCompression(9))
        ));
    }

    #[test]
    fn heavy_frame_detection_matches_thresholds() {
        // Uncompressed frames only offload at the large-frame threshold.
        let big_plain = vec![SERVER_MSG_COMPRESSION_TAG_NONE; OFFLOAD_DECODE_BYTES];
        assert!(is_heavy_frame(&big_plain));
        let small_plain = vec![SERVER_MSG_COMPRESSION_TAG_NONE; OFFLOAD_DECODE_BYTES - 1];
        assert!(!is_heavy_frame(&small_plain));
        // Compressed frames offload from the much smaller compressed threshold.
        let small_brotli = vec![SERVER_MSG_COMPRESSION_TAG_BROTLI; OFFLOAD_COMPRESSED_BYTES - 1];
        assert!(!is_heavy_frame(&small_brotli));
        let brotli_at_threshold = vec![SERVER_MSG_COMPRESSION_TAG_BROTLI; OFFLOAD_COMPRESSED_BYTES];
        assert!(is_heavy_frame(&brotli_at_threshold));
        let gzip_at_threshold = vec![SERVER_MSG_COMPRESSION_TAG_GZIP; OFFLOAD_COMPRESSED_BYTES];
        assert!(is_heavy_frame(&gzip_at_threshold));
        // Uncompressed tag does not get the compressed threshold.
        let plain_below_large = vec![SERVER_MSG_COMPRESSION_TAG_NONE; OFFLOAD_COMPRESSED_BYTES];
        assert!(!is_heavy_frame(&plain_below_large));
    }

    #[test]
    fn decode_server_message_brotli_and_gzip_roundtrip() {
        use spacetimedb_client_api_messages::websocket::v1::IdentityToken;
        let msg = ServerMessage::<BsatnFormat>::IdentityToken(IdentityToken {
            identity: Identity::ZERO,
            token: "tok".into(),
            connection_id: ConnectionId::ZERO,
        });
        let raw = bsatn::to_vec(&msg).unwrap();

        // Uncompressed (tag 0).
        let mut frame = vec![SERVER_MSG_COMPRESSION_TAG_NONE];
        frame.extend_from_slice(&raw);
        assert!(matches!(
            decode_server_message(&frame).unwrap(),
            ServerMessage::IdentityToken(_)
        ));

        // Brotli (tag 1).
        let mut compressed = Vec::new();
        let params = brotli::enc::BrotliEncoderParams {
            quality: 1,
            ..Default::default()
        };
        brotli::BrotliCompress(&mut raw.as_slice(), &mut compressed, &params).unwrap();
        let mut frame = vec![SERVER_MSG_COMPRESSION_TAG_BROTLI];
        frame.extend_from_slice(&compressed);
        assert!(matches!(
            decode_server_message(&frame).unwrap(),
            ServerMessage::IdentityToken(_)
        ));

        // Gzip (tag 2).
        let mut compressed = Vec::new();
        {
            use std::io::Write;
            let mut enc = flate2::write::GzEncoder::new(&mut compressed, flate2::Compression::fast());
            enc.write_all(&raw).unwrap();
            enc.finish().unwrap();
        }
        let mut frame = vec![SERVER_MSG_COMPRESSION_TAG_GZIP];
        frame.extend_from_slice(&compressed);
        assert!(matches!(
            decode_server_message(&frame).unwrap(),
            ServerMessage::IdentityToken(_)
        ));
    }

    #[test]
    fn connect_request_asks_for_brotli() {
        let cfg = UpstreamConfig {
            host: Url::parse("wss://ea.example").unwrap(),
            database: "bitcraft-live-1".into(),
            auth_token: None,
            connect_timeout: Duration::from_secs(1),
        };
        let req = build_connect_request(&cfg).unwrap();
        assert!(req.uri().query().unwrap().contains("compression=Brotli"));
    }
}
