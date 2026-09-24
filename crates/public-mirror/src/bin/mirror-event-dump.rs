//! Debug tool: subscribe to a table on a running public-mirror like a real
//! downstream client and dump decoded rows as JSON.
//!
//! Used to verify forwarded `*_event` rows end to end (row content, not just
//! counts — `/v1/mirrors` has the counts). The mirror's local schema equals
//! the upstream schema (tables are created from it), so row types are
//! resolved from the upstream module def.
//!
//! Event tables are v2-only downstream (the mirror republishes them with
//! their native `is_event` marker, so v1 subscriptions are rejected with the
//! standard "requires WebSocket v2" error) — pass `--v2` for those. In v2
//! mode the end-of-window summary reports how many rows arrived as
//! `EventTable` vs `PersistentTable` frames.
//!
//! Usage:
//! ```text
//! mirror-event-dump \
//!   --url ws://127.0.0.1:3150 \
//!   --database bitcraft-live-14 \
//!   --upstream wss://bitcraft-early-access.spacetimedb.com \
//!   --table market_trade_event \
//!   --v2 \
//!   --seconds 120
//! ```

use std::collections::HashMap;
use std::time::Duration;

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use http::header::SEC_WEBSOCKET_PROTOCOL;
use spacetimedb_client_api_messages::websocket::common::{BsatnRowList, QuerySetId, RowListLen};
use spacetimedb_client_api_messages::websocket::v1::{
    BsatnFormat, ClientMessage, CompressableQueryUpdate, DatabaseUpdate, ServerMessage, UpdateStatus,
};
use spacetimedb_lib::bsatn;
use spacetimedb_lib::ProductValue;
use spacetimedb_sats::{ProductType, WithTypespace};
use spacetimedb_schema::def::ModuleDef;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::MaybeTlsStream;
use url::Url;

use spacetimedb_public_mirror_client::schema::fetch_and_parse_schema;

const SUBPROTOCOL_V1: &str = "v1.bsatn.spacetimedb";

#[derive(Parser, Debug)]
#[command(
    name = "mirror-event-dump",
    about = "Dump decoded rows streamed by a public-mirror table (downstream v1 client)"
)]
struct Args {
    /// Mirror WebSocket base URL (ws://127.0.0.1:3150).
    #[arg(long)]
    url: String,

    /// Mirror database name.
    #[arg(long)]
    database: String,

    /// Upstream host, for schema fetch (mirror tables are created from it).
    #[arg(long)]
    upstream: String,

    /// Table to subscribe.
    #[arg(long = "table")]
    tables: Vec<String>,

    /// How long to listen (seconds).
    #[arg(long, default_value_t = 60)]
    seconds: u64,

    /// Run a one-off SQL query over v2 instead of subscribing, print raw rows.
    #[arg(long)]
    sql: Option<String>,

    /// Subscribe over the v2 protocol (v2.bsatn.spacetimedb) instead of v1.
    #[arg(long, default_value_t = false)]
    v2: bool,
}

fn build_row_types(module_def: &ModuleDef) -> anyhow::Result<HashMap<String, ProductType>> {
    let typespace = module_def.typespace();
    let mut map = HashMap::new();
    for table in module_def.tables() {
        let name = table.name.to_string();
        let alg = typespace
            .get(table.product_type_ref)
            .ok_or_else(|| anyhow::anyhow!("no type for table {name}"))?;
        let resolved = WithTypespace::new(typespace, alg)
            .resolve_refs()
            .map_err(|e| anyhow::anyhow!("resolve row type for {name}: {e}"))?;
        map.insert(name, resolved.as_product().expect("row type").clone());
    }
    Ok(map)
}

fn dump_update(db: &DatabaseUpdate<BsatnFormat>, row_types: &HashMap<String, ProductType>) {
    for t in &db.tables {
        let Some(row_ty) = row_types.get(&*t.table_name) else {
            continue;
        };
        for u in &t.updates {
            let CompressableQueryUpdate::Uncompressed(qu) = u else { continue };
            for row in inserts_of(&qu.inserts) {
                let mut bytes: &[u8] = &row;
                match ProductValue::decode(row_ty, &mut bytes) {
                    Ok(pv) => println!(
                        "{} {}",
                        t.table_name,
                        serde_json::to_string(&pv).unwrap_or_else(|e| format!("<serde error: {e}>"))
                    ),
                    Err(e) => println!("{} <decode error: {e}>", t.table_name),
                }
            }
        }
    }
}

/// BsatnFormat lists flatten row bytes; collect them as owned slices.
fn inserts_of(list: &BsatnRowList) -> Vec<bytes::Bytes> {
    list.into_iter().collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let upstream: Url = args.upstream.parse()?;
    let (_, module_def) = fetch_and_parse_schema(&upstream, &args.database).await?;
    let row_types = build_row_types(&module_def)?;
    for t in &args.tables {
        anyhow::ensure!(row_types.contains_key(t), "unknown table `{t}` in module def");
    }

    if let Some(sql) = args.sql {
        return one_off_query(&args.url, &args.database, &sql, &row_types).await;
    }
    if args.v2 {
        return subscribe_v2(&args, &row_types).await;
    }

    let mut url: Url = args.url.parse()?;
    match url.scheme() {
        "ws" | "wss" => {}
        "http" => url.set_scheme("ws").map_err(|_| anyhow::anyhow!("scheme rewrite"))?,
        "https" => url.set_scheme("wss").map_err(|_| anyhow::anyhow!("scheme rewrite"))?,
        other => anyhow::bail!("unsupported scheme {other}"),
    }
    let mut path = url.path().trim_end_matches('/').to_string();
    path.push_str("/v1/database/");
    path.push_str(&args.database);
    path.push_str("/subscribe");
    url.set_path(&path);
    url.query_pairs_mut().clear().append_pair("compression", "None");

    let mut request = url.as_str().into_client_request()?;
    request
        .headers_mut()
        .insert(SEC_WEBSOCKET_PROTOCOL, SUBPROTOCOL_V1.parse()?);
    let (mut sock, _) = tokio_tungstenite::connect_async(request).await?;

    // Drain IdentityToken.
    let server = loop {
        let msg = next_binary(&mut sock).await?;
        let server: ServerMessage<BsatnFormat> = bsatn::from_slice(&msg[1..])?;
        if matches!(server, ServerMessage::IdentityToken(_)) {
            break;
        }
    };
    let _ = server;

    for (i, table) in args.tables.iter().enumerate() {
        let request_id = (i as u32) + 1;
        let frame = encode_subscribe_multi(request_id, table)?;
        sock.send(Message::Binary(frame.into())).await?;
        loop {
            let msg = next_binary(&mut sock).await?;
            let server: ServerMessage<BsatnFormat> = bsatn::from_slice(&msg[1..])?;
            match server {
                ServerMessage::SubscribeMultiApplied(sma) if sma.request_id == request_id => {
                    let mut n = 0;
                    for t in &sma.update.tables {
                        for u in &t.updates {
                            if let CompressableQueryUpdate::Uncompressed(qu) = u {
                                n += qu.inserts.len();
                            }
                        }
                    }
                    eprintln!("subscribed {table} ({n} seed rows)");
                    dump_update(&sma.update, &row_types);
                    break;
                }
                ServerMessage::SubscriptionError(e) => anyhow::bail!("subscribe error: {}", e.error),
                ServerMessage::TransactionUpdate(tu) => {
                    if let UpdateStatus::Committed(db) = tu.status {
                        dump_update(&db, &row_types);
                    }
                }
                _ => {}
            }
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.seconds);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let msg = match tokio::time::timeout(remaining, next_binary(&mut sock)).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(e),
            Err(_) => break,
        };
        let server: ServerMessage<BsatnFormat> = bsatn::from_slice(&msg[1..])?;
        if let ServerMessage::TransactionUpdate(tu) = server {
            if let UpdateStatus::Committed(db) = tu.status {
                dump_update(&db, &row_types);
            }
        }
    }
    eprintln!("window ended");
    Ok(())
}

fn query_all(table: &str) -> String {
    format!("SELECT * FROM {table}")
}

/// Subscribe over the v2 protocol: `Subscribe` → `SubscribeApplied`, then
/// live `TransactionUpdate`s. Event-table rows arrive as ordinary
/// `PersistentTable` inserts because the mirror publishes them as plain
/// tables.
async fn subscribe_v2(args: &Args, row_types: &HashMap<String, ProductType>) -> anyhow::Result<()> {
    use spacetimedb_client_api_messages::websocket::v2::{
        ClientMessage as V2Client, ServerMessage as V2Server, Subscribe, TableUpdateRows,
        TransactionUpdate,
    };

    let mut request = build_subscribe_request(&args.url, &args.database)?;
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        "v2.bsatn.spacetimedb"
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid subprotocol header"))?,
    );
    let (mut sock, _) = tokio_tungstenite::connect_async(request).await?;

    // Drain InitialConnection.
    loop {
        let msg = next_binary(&mut sock).await?;
        let server: V2Server = bsatn::from_slice(&msg[1..])?;
        if matches!(server, V2Server::InitialConnection(_)) {
            break;
        }
    }

    let mut stats = V2RowStats::default();
    for (i, table) in args.tables.iter().enumerate() {
        let id = (i as u32) + 1;
        let frame = bsatn::to_vec(&V2Client::Subscribe(Subscribe {
            query_strings: vec![query_all(table).into()].into(),
            request_id: id,
            query_set_id: QuerySetId::new(id),
        }))?;
        sock.send(Message::Binary(frame.into())).await?;
        loop {
            let msg = next_binary(&mut sock).await?;
            let server: V2Server = bsatn::from_slice(&msg[1..])?;
            match server {
                V2Server::SubscribeApplied(sap) if sap.query_set_id.id == id => {
                    let n: usize = sap.rows.tables.iter().map(|t| t.rows.len()).sum();
                    eprintln!("subscribed(v2) {table} ({n} seed rows)");
                    break;
                }
                V2Server::SubscriptionError(e) => anyhow::bail!("subscribe error: {}", e.error),
                V2Server::TransactionUpdate(tu) => dump_v2_update(&tu, row_types, &mut stats),
                _ => {}
            }
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.seconds);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let msg = match tokio::time::timeout(remaining, next_binary(&mut sock)).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(e),
            Err(_) => break,
        };
        let server: V2Server = bsatn::from_slice(&msg[1..])?;
        if let V2Server::TransactionUpdate(tu) = server {
            dump_v2_update(&tu, row_types, &mut stats);
        }
    }
    eprintln!(
        "window ended (rows: persistent_table={} event_table={})",
        stats.persistent, stats.event
    );
    Ok(())
}

#[derive(Default)]
struct V2RowStats {
    persistent: usize,
    event: usize,
}

fn dump_v2_update(tu: &spacetimedb_client_api_messages::websocket::v2::TransactionUpdate, row_types: &HashMap<String, ProductType>, stats: &mut V2RowStats) {
    use spacetimedb_client_api_messages::websocket::v2::TableUpdateRows;
    for query_set in &tu.query_sets {
        for t in &query_set.tables {
            let Some(ty) = row_types.get(&*t.table_name) else { continue };
            for rows in &t.rows {
                let list = match rows {
                    TableUpdateRows::PersistentTable(p) => {
                        stats.persistent += p.inserts.len();
                        inserts_of(&p.inserts)
                    }
                    TableUpdateRows::EventTable(e) => {
                        stats.event += e.events.len();
                        inserts_of(&e.events)
                    }
                };
                for row in list {
                    let mut bytes: &[u8] = &row;
                    match ProductValue::decode(ty, &mut bytes) {
                        Ok(pv) => println!(
                            "{} {}",
                            t.table_name,
                            serde_json::to_string(&pv).unwrap_or_else(|e| format!("<serde error: {e}>"))
                        ),
                        Err(e) => println!("{} <decode error: {e}>", t.table_name),
                    }
                }
            }
        }
    }
}

/// Build a `/v1/database/{db}/subscribe?compression=None` WS request without
/// subprotocol headers (caller sets the subprotocol).
fn build_subscribe_request(
    url: &str,
    database: &str,
) -> anyhow::Result<tokio_tungstenite::tungstenite::handshake::client::Request> {
    let mut url: Url = url.parse()?;
    match url.scheme() {
        "ws" | "wss" => {}
        "http" => url.set_scheme("ws").map_err(|_| anyhow::anyhow!("scheme rewrite"))?,
        "https" => url.set_scheme("wss").map_err(|_| anyhow::anyhow!("scheme rewrite"))?,
        other => anyhow::bail!("unsupported scheme {other}"),
    }
    let mut path = url.path().trim_end_matches('/').to_string();
    path.push_str("/v1/database/");
    path.push_str(database);
    path.push_str("/subscribe");
    url.set_path(&path);
    url.query_pairs_mut().clear().append_pair("compression", "None");
    Ok(url.as_str().into_client_request()?)
}

/// One-off SQL over the v2 protocol: `OneOffQuery` → `OneOffQueryResult`.
/// Prints raw result rows (hex) — intentionally schema-free.
async fn one_off_query(
    url: &str,
    database: &str,
    sql: &str,
    row_types: &HashMap<String, ProductType>,
) -> anyhow::Result<()> {
    use spacetimedb_client_api_messages::websocket::v2::{
        ClientMessage as V2Client, OneOffQuery, ServerMessage as V2Server,
    };

    let mut url: Url = url.parse()?;
    match url.scheme() {
        "ws" => {}
        "wss" => {}
        "http" => url.set_scheme("ws").map_err(|_| anyhow::anyhow!("scheme rewrite"))?,
        "https" => url.set_scheme("wss").map_err(|_| anyhow::anyhow!("scheme rewrite"))?,
        other => anyhow::bail!("unsupported scheme {other}"),
    }
    let mut path = url.path().trim_end_matches('/').to_string();
    path.push_str("/v1/database/");
    path.push_str(database);
    path.push_str("/subscribe");
    url.set_path(&path);
    url.query_pairs_mut().clear().append_pair("compression", "None");

    let mut request = url.as_str().into_client_request()?;
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        "v2.bsatn.spacetimedb"
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid subprotocol header"))?,
    );
    let (mut sock, _) = tokio_tungstenite::connect_async(request).await?;

    // Drain InitialConnection.
    loop {
        let msg = next_binary(&mut sock).await?;
        let server: V2Server = bsatn::from_slice(&msg[1..])?;
        if matches!(server, V2Server::InitialConnection(_)) {
            break;
        }
    }

    let frame = bsatn::to_vec(&V2Client::OneOffQuery(OneOffQuery {
        request_id: 1,
        query_string: sql.to_string().into_boxed_str(),
    }))?;
    sock.send(Message::Binary(frame.into())).await?;

    loop {
        let msg = next_binary(&mut sock).await?;
        let server: V2Server = bsatn::from_slice(&msg[1..])?;
        match server {
            V2Server::OneOffQueryResult(res) => {
                match &res.result {
                    Ok(rows) => {
                        for table in &rows.tables {
                            let n = table.rows.len();
                            println!("table {} ({} rows)", table.table, n);
                            if let Some(ty) = row_types.get(&*table.table) {
                                for row in inserts_of(&table.rows) {
                                    let mut bytes: &[u8] = &row;
                                    match ProductValue::decode(ty, &mut bytes) {
                                        Ok(pv) => println!(
                                            "  {}",
                                            serde_json::to_string(&pv)
                                                .unwrap_or_else(|e| format!("<serde error: {e}>"))
                                        ),
                                        Err(e) => println!("  <decode error: {e}>"),
                                    }
                                }
                            } else {
                                for row in inserts_of(&table.rows) {
                                    println!("  {}", hex(&row));
                                }
                            }
                        }
                        return Ok(());
                    }
                    Err(e) => anyhow::bail!("query error: {e}"),
                }
            }
            _ => {}
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn encode_subscribe_multi(request_id: u32, table: &str) -> anyhow::Result<Vec<u8>> {
    let msg = ClientMessage::<Box<[u8]>>::SubscribeMulti(spacetimedb_client_api_messages::websocket::v1::SubscribeMulti {
        query_strings: vec![query_all(table).into()].into(),
        request_id,
        query_id: QuerySetId::new(request_id),
    });
    Ok(bsatn::to_vec(&msg)?)
}

async fn next_binary(
    sock: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
) -> anyhow::Result<Vec<u8>> {
    loop {
        let Some(msg) = sock.next().await else {
            anyhow::bail!("websocket closed");
        };
        match msg? {
            Message::Binary(b) => return Ok(b.to_vec()),
            Message::Close(_) => anyhow::bail!("websocket closed"),
            _ => {}
        }
    }
}
