use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use chrono::{DateTime, Utc};
use datafusion::catalog::Session;
use datafusion::execution::context::SessionContext;
use deltalake::delta_datafusion::{DeltaScanConfig, DeltaTableProvider};
use deltalake::errors::DeltaTableError;
use deltalake::kernel::models::Add;
use deltalake::{ensure_table_uri, DeltaTable, DeltaTableBuilder};

use crate::delta_protocol::{
    DeltaFeatureGate,
    DeltaSnapshotInfo,
    delta_snapshot_info,
    protocol_gate,
};

#[derive(Debug, Clone, Default)]
pub struct DeltaScanOverrides {
    pub file_column_name: Option<String>,
    pub enable_parquet_pushdown: Option<bool>,
    pub schema_force_view_types: Option<bool>,
    pub wrap_partition_values: Option<bool>,
    pub schema: Option<SchemaRef>,
}

#[derive(Debug, Clone)]
pub struct DeltaAddActionPayload {
    pub path: String,
    pub size: i64,
    pub modification_time: i64,
    pub data_change: bool,
    pub partition_values: BTreeMap<String, Option<String>>,
    pub stats: Option<String>,
    pub tags: BTreeMap<String, Option<String>>,
}

fn decode_add_path(path: &str) -> String {
    urlencoding::decode(path)
        .map(|decoded| decoded.into_owned())
        .unwrap_or_else(|_| path.to_owned())
}

fn delta_table_builder(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
) -> Result<DeltaTableBuilder, DeltaTableError> {
    if version.is_some() && timestamp.is_some() {
        return Err(DeltaTableError::Generic(
            "Specify either version or timestamp, not both.".to_owned(),
        ));
    }
    let table_url = ensure_table_uri(table_uri)?;
    let mut builder = DeltaTableBuilder::from_url(table_url)?;
    if let Some(options) = storage_options {
        builder = builder.with_storage_options(options);
    }
    if let Some(v) = version {
        builder = builder.with_version(v);
    }
    if let Some(ts) = timestamp {
        builder = builder.with_datestring(ts)?;
    }
    Ok(builder)
}

pub async fn load_delta_table(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
) -> Result<DeltaTable, DeltaTableError> {
    let builder = delta_table_builder(table_uri, storage_options, version, timestamp)?;
    builder.load().await
}

pub async fn snapshot_info_with_gate(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    gate: Option<DeltaFeatureGate>,
) -> Result<DeltaSnapshotInfo, DeltaTableError> {
    let table = load_delta_table(table_uri, storage_options, version, timestamp).await?;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    if let Some(gate) = gate {
        protocol_gate(&snapshot, &gate)?;
    }
    Ok(snapshot)
}

fn apply_overrides(mut scan_config: DeltaScanConfig, overrides: DeltaScanOverrides) -> DeltaScanConfig {
    if let Some(name) = overrides.file_column_name {
        scan_config.file_column_name = Some(name);
    }
    if let Some(pushdown) = overrides.enable_parquet_pushdown {
        scan_config.enable_parquet_pushdown = pushdown;
    }
    if let Some(force_view) = overrides.schema_force_view_types {
        scan_config.schema_force_view_types = force_view;
    }
    if let Some(wrap) = overrides.wrap_partition_values {
        scan_config.wrap_partition_values = wrap;
    }
    if let Some(schema) = overrides.schema {
        scan_config.schema = Some(schema);
    }
    scan_config
}

pub fn scan_config_from_session(
    session: &dyn Session,
    overrides: DeltaScanOverrides,
) -> DeltaScanConfig {
    let base = DeltaScanConfig::new_from_session(session);
    apply_overrides(base, overrides)
}

pub async fn delta_provider_from_session(
    session_ctx: &SessionContext,
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    overrides: DeltaScanOverrides,
    gate: Option<DeltaFeatureGate>,
) -> Result<(DeltaTableProvider, DeltaSnapshotInfo, DeltaScanConfig), DeltaTableError> {
    let table = load_delta_table(table_uri, storage_options, version, timestamp).await?;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    if let Some(gate) = gate {
        protocol_gate(&snapshot, &gate)?;
    }
    let eager_snapshot = table.snapshot()?.snapshot().clone();
    let log_store = table.log_store();
    let session_state = session_ctx.state();
    let scan_config = scan_config_from_session(&session_state, overrides);
    let provider = DeltaTableProvider::try_new(eager_snapshot, log_store, scan_config.clone())?;
    Ok((provider, snapshot, scan_config))
}

pub async fn delta_provider_with_files(
    session_ctx: &SessionContext,
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    overrides: DeltaScanOverrides,
    files: Vec<String>,
    gate: Option<DeltaFeatureGate>,
) -> Result<(DeltaTableProvider, DeltaSnapshotInfo, DeltaScanConfig, Vec<Add>), DeltaTableError> {
    let table = load_delta_table(table_uri, storage_options, version, timestamp).await?;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    if let Some(gate) = gate {
        protocol_gate(&snapshot, &gate)?;
    }
    let eager_snapshot = table.snapshot()?.snapshot().clone();
    let log_store = table.log_store();
    let session_state = session_ctx.state();
    let scan_config = scan_config_from_session(&session_state, overrides);
    let provider = DeltaTableProvider::try_new(eager_snapshot, log_store, scan_config.clone())?;
    let add_actions = add_actions_for_paths(&table, &files)?;
    let provider = provider.with_files(add_actions.clone());
    Ok((provider, snapshot, scan_config, add_actions))
}

pub async fn delta_add_actions(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    gate: Option<DeltaFeatureGate>,
) -> Result<(DeltaSnapshotInfo, Vec<DeltaAddActionPayload>), DeltaTableError> {
    let table = load_delta_table(table_uri, storage_options, version, timestamp).await?;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    if let Some(gate) = gate {
        protocol_gate(&snapshot, &gate)?;
    }
    let eager = table.snapshot()?.snapshot().clone();
    let adds = eager.file_actions()?;
    let payloads = adds.map(delta_add_payload).collect();
    Ok((snapshot, payloads))
}

fn delta_add_payload(add: Add) -> DeltaAddActionPayload {
    let mut partition_values: BTreeMap<String, Option<String>> = BTreeMap::new();
    for (key, value) in add.partition_values {
        partition_values.insert(key, value);
    }
    let mut tags: BTreeMap<String, Option<String>> = BTreeMap::new();
    if let Some(add_tags) = add.tags {
        for (key, value) in add_tags {
            tags.insert(key, value);
        }
    }
    DeltaAddActionPayload {
        path: decode_add_path(add.path.as_str()),
        size: add.size,
        modification_time: add.modification_time,
        data_change: add.data_change,
        partition_values,
        stats: add.stats,
        tags,
    }
}

pub fn add_actions_for_paths(
    table: &DeltaTable,
    files: &[String],
) -> Result<Vec<Add>, DeltaTableError> {
    let snapshot = table.snapshot()?.snapshot().clone();
    let add_actions: Vec<Add> = snapshot.file_actions()?.collect();
    let mut by_path: HashMap<String, Add> = HashMap::new();
    for add in add_actions {
        by_path.insert(decode_add_path(add.path.as_str()), add);
    }
    let root = table.table_uri();
    let root_path = Path::new(root);
    let mut resolved: Vec<Add> = Vec::new();
    let mut missing: Vec<String> = Vec::new();
    for file in files {
        let relative = make_relative_path(file, root_path);
        match by_path.get(relative.as_str()) {
            Some(add) => resolved.push(add.clone()),
            None => missing.push(relative),
        }
    }
    if !missing.is_empty() {
        return Err(DeltaTableError::Generic(format!(
            "Delta pruning file list not found in table: {missing:?}"
        )));
    }
    Ok(resolved)
}

fn make_relative_path(candidate: &str, root_path: &Path) -> String {
    let candidate_path = Path::new(candidate);
    if let Ok(stripped) = candidate_path.strip_prefix(root_path) {
        return stripped.to_string_lossy().trim_start_matches('/').to_owned();
    }
    let root_str = root_path.to_string_lossy();
    let prefix = format!("{root_str}/");
    if let Some(stripped) = candidate.strip_prefix(prefix.as_str()) {
        return stripped.trim_start_matches('/').to_owned();
    }
    candidate.trim_start_matches('/').to_owned()
}

pub fn parse_timestamp(timestamp: &str) -> Result<DateTime<Utc>, DeltaTableError> {
    let parsed = DateTime::parse_from_rfc3339(timestamp)
        .map_err(|err| DeltaTableError::Generic(format!("Invalid Delta timestamp: {err}")))?;
    Ok(parsed.with_timezone(&Utc))
}

