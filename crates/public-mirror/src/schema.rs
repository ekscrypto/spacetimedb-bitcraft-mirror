//! Fetch and parse SpacetimeDB module schema (RawModuleDefV10 / ModuleDef).

use std::collections::HashSet;

use spacetimedb_lib::db::raw_def::v10::RawModuleDefV10;
use spacetimedb_lib::de::serde::DeserializeWrapper;
use spacetimedb_schema::def::ModuleDef;
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum SchemaFetchError {
    #[error("invalid url: {0}")]
    Url(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("server returned status {0}")]
    Status(u16),
    #[error("schema JSON deserialize failed: {0}")]
    Deserialize(String),
    #[error("module def validation failed: {0}")]
    Validate(String),
}

/// GET `{host}/v1/database/{database}/schema?version=10`.
///
/// Rewrites `wss`→`https` and `ws`→`http`. Returns raw JSON bytes and a validated [`ModuleDef`].
///
/// v10 (not v9) is required: it is the earliest schema version carrying the
/// `is_event` marker, which the mirror depends on both to forward event rows
/// with proper v2 `EventTable` frames and to get the datastore's native
/// wire-only semantics for event tables (commitlog + subscriptions, never
/// committed state). The v9 def reports event tables as plain user tables.
pub async fn fetch_and_parse_schema(host_url: &Url, database: &str) -> Result<(Vec<u8>, ModuleDef), SchemaFetchError> {
    let raw = fetch_schema(host_url, database).await?;
    let module_def = parse_module_def(&raw)?;
    Ok((raw, module_def))
}

/// GET `{host}/v1/database/{database}/schema?version=10` — raw SATS-JSON body.
pub async fn fetch_schema(host_url: &Url, database: &str) -> Result<Vec<u8>, SchemaFetchError> {
    let mut url = host_url.clone();
    match url.scheme() {
        "ws" => url
            .set_scheme("http")
            .map_err(|_| SchemaFetchError::Url("scheme rewrite ws->http failed".into()))?,
        "wss" => url
            .set_scheme("https")
            .map_err(|_| SchemaFetchError::Url("scheme rewrite wss->https failed".into()))?,
        "http" | "https" => {}
        other => {
            return Err(SchemaFetchError::Url(format!("unsupported scheme {other}")));
        }
    }
    let mut path = url.path().trim_end_matches('/').to_string();
    path.push_str("/v1/database/");
    path.push_str(database);
    path.push_str("/schema");
    url.set_path(&path);
    url.query_pairs_mut().clear().append_pair("version", "10");

    let response = reqwest::get(url).await?;
    let status = response.status();
    if !status.is_success() {
        return Err(SchemaFetchError::Status(status.as_u16()));
    }
    Ok(response.bytes().await?.to_vec())
}

/// Parse schema JSON (`DeserializeWrapper<RawModuleDefV10>`) into a validated [`ModuleDef`].
pub fn parse_module_def(raw_json: &[u8]) -> Result<ModuleDef, SchemaFetchError> {
    let DeserializeWrapper(raw): DeserializeWrapper<RawModuleDefV10> =
        serde_json::from_slice(raw_json).map_err(|e| SchemaFetchError::Deserialize(e.to_string()))?;
    ModuleDef::try_from(raw).map_err(|e| SchemaFetchError::Validate(e.to_string()))
}

/// Public user table names from a [`ModuleDef`] (excludes system / private tables).
pub fn public_user_table_names(module_def: &ModuleDef) -> Vec<String> {
    let mut names: Vec<String> = module_def
        .tables()
        .filter(|t| t.table_type == spacetimedb_lib::db::raw_def::v9::TableType::User && t.table_access == spacetimedb_lib::db::raw_def::v9::TableAccess::Public)
        .map(|t| t.name.to_string())
        .collect();
    names.sort();
    names
}

/// Event-table names from a [`ModuleDef`]. Authoritative (v10 `is_event`) —
/// do NOT infer from the `_event` suffix: BitCraft has persistent tables
/// suffixed `_event` (e.g. `player_notification_event`) that must keep
/// normal state-table mirroring.
pub fn event_table_names(module_def: &ModuleDef) -> HashSet<String> {
    module_def
        .tables()
        .filter(|t| t.is_event)
        .map(|t| t.name.to_string())
        .collect()
}
