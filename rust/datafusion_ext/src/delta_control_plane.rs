use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use chrono::{DateTime, Utc};
use datafusion::catalog::Session;
use datafusion::common::ToDFSchema;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use deltalake::delta_datafusion::{
    DataFusionMixins, DeltaCdfTableProvider, DeltaScanConfig, DeltaScanConfigBuilder,
    DeltaTableProvider,
};
use deltalake::errors::DeltaTableError;
use deltalake::kernel::models::Add;
use deltalake::kernel::scalars::ScalarExt;
use deltalake::kernel::{EagerSnapshot, LogDataHandler, LogicalFileView};
use deltalake::{ensure_table_uri, DeltaTable, DeltaTableBuilder};
use object_store::DynObjectStore;
use url::{Position, Url};

use crate::delta_protocol::{delta_snapshot_info, protocol_gate, DeltaSnapshotInfo};
use crate::DeltaFeatureGate;

#[derive(Debug, Clone, Default)]
pub struct DeltaScanOverrides {
    pub file_column_name: Option<String>,
    pub enable_parquet_pushdown: Option<bool>,
    pub schema_force_view_types: Option<bool>,
    pub wrap_partition_values: Option<bool>,
    pub schema: Option<SchemaRef>,
}

#[derive(Debug, Clone, Default)]
pub struct DeltaCdfScanOptions {
    pub starting_version: Option<i64>,
    pub ending_version: Option<i64>,
    pub starting_timestamp: Option<String>,
    pub ending_timestamp: Option<String>,
    pub allow_out_of_range: bool,
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

fn object_store_url_for_table(table_url: &Url) -> Result<ObjectStoreUrl, DeltaTableError> {
    let authority = &table_url[Position::BeforeHost..Position::AfterPort];
    let base = format!("{}://{}", table_url.scheme(), authority);
    ObjectStoreUrl::parse(base).map_err(|err| {
        DeltaTableError::Generic(format!("Invalid object store URL for Delta table: {err}"))
    })
}

fn session_object_store(
    session_ctx: Option<&SessionContext>,
    table_url: &Url,
) -> Result<Option<Arc<DynObjectStore>>, DeltaTableError> {
    let Some(session_ctx) = session_ctx else {
        return Ok(None);
    };
    let store_url = object_store_url_for_table(table_url)?;
    let registry = session_ctx.runtime_env().object_store_registry.clone();
    match registry.get_store(store_url.as_ref()) {
        Ok(store) => Ok(Some(store)),
        Err(_) => Ok(None),
    }
}

fn delta_table_builder(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    session_ctx: Option<&SessionContext>,
) -> Result<DeltaTableBuilder, DeltaTableError> {
    if version.is_some() && timestamp.is_some() {
        return Err(DeltaTableError::Generic(
            "Specify either version or timestamp, not both.".to_owned(),
        ));
    }
    let table_url = ensure_table_uri(table_uri)?;
    let mut builder = DeltaTableBuilder::from_url(table_url.clone())?;
    if let Some(store) = session_object_store(session_ctx, &table_url)? {
        builder = builder.with_storage_backend(store, table_url.clone());
    }
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
    session_ctx: Option<&SessionContext>,
) -> Result<DeltaTable, DeltaTableError> {
    let builder = delta_table_builder(
        table_uri,
        storage_options,
        version,
        timestamp,
        session_ctx,
    )?;
    builder.load().await
}

fn update_datafusion_session(
    table: &DeltaTable,
    session_ctx: &SessionContext,
) -> Result<(), DeltaTableError> {
    let session_state = session_ctx.state();
    table.update_datafusion_session(&session_state)?;
    Ok(())
}

pub async fn snapshot_info_with_gate(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    gate: Option<DeltaFeatureGate>,
) -> Result<DeltaSnapshotInfo, DeltaTableError> {
    let table = load_delta_table(table_uri, storage_options, version, timestamp, None).await?;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    if let Some(gate) = gate {
        protocol_gate(&snapshot, &gate)?;
    }
    Ok(snapshot)
}

fn apply_overrides(
    mut scan_config: DeltaScanConfig,
    overrides: DeltaScanOverrides,
) -> DeltaScanConfig {
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

fn apply_file_column_builder(
    scan_config: DeltaScanConfig,
    snapshot: &EagerSnapshot,
) -> Result<DeltaScanConfig, DeltaTableError> {
    let Some(file_column_name) = scan_config.file_column_name.clone() else {
        return Ok(scan_config);
    };
    let mut builder = DeltaScanConfigBuilder::new().with_file_column_name(&file_column_name);
    builder = builder.wrap_partition_values(scan_config.wrap_partition_values);
    builder = builder.with_parquet_pushdown(scan_config.enable_parquet_pushdown);
    if let Some(schema) = scan_config.schema.clone() {
        builder = builder.with_schema(schema);
    }
    let built = builder.build(snapshot)?;
    Ok(DeltaScanConfig {
        file_column_name: built.file_column_name,
        wrap_partition_values: built.wrap_partition_values,
        enable_parquet_pushdown: built.enable_parquet_pushdown,
        schema: built.schema,
        schema_force_view_types: scan_config.schema_force_view_types,
    })
}

pub fn scan_config_from_session(
    session: &dyn Session,
    snapshot: Option<&EagerSnapshot>,
    overrides: DeltaScanOverrides,
) -> Result<DeltaScanConfig, DeltaTableError> {
    let base = DeltaScanConfig::new_from_session(session);
    let scan_config = apply_overrides(base, overrides);
    let Some(snapshot) = snapshot else {
        return Ok(scan_config);
    };
    apply_file_column_builder(scan_config, snapshot)
}

fn files_matching_predicate(
    session: &dyn Session,
    log_data: LogDataHandler<'_>,
    filters: &[Expr],
) -> Result<Vec<Add>, DeltaTableError> {
    if let Some(Some(predicate)) =
        (!filters.is_empty()).then_some(conjunction(filters.iter().cloned()))
    {
        let expr = session.create_physical_expr(predicate, &log_data.read_schema().to_dfschema()?)?;
        let pruning_predicate = PruningPredicate::try_new(expr, log_data.read_schema())?;
        let mask = pruning_predicate.prune(&log_data)?;
        Ok(log_data
            .into_iter()
            .zip(mask)
            .filter_map(|(file, keep_file)| keep_file.then_some(logical_view_to_add(&file)))
            .collect())
    } else {
        Ok(log_data
            .into_iter()
            .map(|file| logical_view_to_add(&file))
            .collect())
    }
}

fn parse_rfc3339_timestamp(value: &str) -> Result<DateTime<Utc>, DeltaTableError> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| DeltaTableError::Generic(format!("Invalid Delta timestamp: {err}")))
}

pub async fn delta_provider_from_session(
    session_ctx: &SessionContext,
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    predicate: Option<String>,
    overrides: DeltaScanOverrides,
    gate: Option<DeltaFeatureGate>,
) -> Result<
    (
        DeltaTableProvider,
        DeltaSnapshotInfo,
        DeltaScanConfig,
        Option<Vec<DeltaAddActionPayload>>,
        Option<String>,
    ),
    DeltaTableError,
> {
    let table = load_delta_table(
        table_uri,
        storage_options,
        version,
        timestamp,
        Some(session_ctx),
    )
    .await?;
    update_datafusion_session(&table, session_ctx)?;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    if let Some(gate) = gate {
        protocol_gate(&snapshot, &gate)?;
    }
    let eager_snapshot = table.snapshot()?.snapshot().clone();
    let log_store = table.log_store();
    let session_state = session_ctx.state();
    let scan_config = scan_config_from_session(&session_state, Some(&eager_snapshot), overrides)?;
    let mut provider = DeltaTableProvider::try_new(eager_snapshot.clone(), log_store, scan_config.clone())?;
    let mut add_payloads: Option<Vec<DeltaAddActionPayload>> = None;
    let mut predicate_error: Option<String> = None;
    if let Some(predicate) = predicate {
        match eager_snapshot.parse_predicate_expression(predicate, &session_state) {
            Ok(expr) => match files_matching_predicate(&session_state, eager_snapshot.log_data(), &[expr]) {
                Ok(add_actions) => {
                    provider = provider.with_files(add_actions.clone());
                    add_payloads = Some(add_actions.into_iter().map(delta_add_payload).collect());
                }
                Err(err) => {
                    predicate_error = Some(err.to_string());
                }
            },
            Err(err) => {
                predicate_error = Some(err.to_string());
            }
        }
    }
    Ok((provider, snapshot, scan_config, add_payloads, predicate_error))
}

pub async fn delta_cdf_provider(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    options: DeltaCdfScanOptions,
    gate: Option<DeltaFeatureGate>,
) -> Result<(DeltaCdfTableProvider, DeltaSnapshotInfo), DeltaTableError> {
    let table = load_delta_table(table_uri, storage_options, version, timestamp, None).await?;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    if let Some(gate) = gate {
        protocol_gate(&snapshot, &gate)?;
    }
    let mut cdf_builder = table.scan_cdf();
    if let Some(starting_version) = options.starting_version {
        cdf_builder = cdf_builder.with_starting_version(starting_version);
    }
    if let Some(ending_version) = options.ending_version {
        cdf_builder = cdf_builder.with_ending_version(ending_version);
    }
    if let Some(starting_timestamp) = options.starting_timestamp {
        let dt = parse_rfc3339_timestamp(starting_timestamp.as_str())?;
        cdf_builder = cdf_builder.with_starting_timestamp(dt);
    }
    if let Some(ending_timestamp) = options.ending_timestamp {
        let dt = parse_rfc3339_timestamp(ending_timestamp.as_str())?;
        cdf_builder = cdf_builder.with_ending_timestamp(dt);
    }
    if options.allow_out_of_range {
        cdf_builder = cdf_builder.with_allow_out_of_range();
    }
    let provider = DeltaCdfTableProvider::try_new(cdf_builder)?;
    Ok((provider, snapshot))
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
) -> Result<
    (
        DeltaTableProvider,
        DeltaSnapshotInfo,
        DeltaScanConfig,
        Vec<DeltaAddActionPayload>,
    ),
    DeltaTableError,
> {
    let table = load_delta_table(
        table_uri,
        storage_options,
        version,
        timestamp,
        Some(session_ctx),
    )
    .await?;
    update_datafusion_session(&table, session_ctx)?;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    if let Some(gate) = gate {
        protocol_gate(&snapshot, &gate)?;
    }
    let eager_snapshot = table.snapshot()?.snapshot().clone();
    let log_store = table.log_store();
    let session_state = session_ctx.state();
    let scan_config = scan_config_from_session(&session_state, Some(&eager_snapshot), overrides)?;
    let provider = DeltaTableProvider::try_new(eager_snapshot, log_store, scan_config.clone())?;
    let add_actions = add_actions_for_paths(&table, &files)?;
    let provider = provider.with_files(add_actions.clone());
    let add_payloads = add_actions.into_iter().map(delta_add_payload).collect();
    Ok((provider, snapshot, scan_config, add_payloads))
}

fn logical_view_to_add(view: &LogicalFileView) -> Add {
    let partition_values = view
        .partition_values()
        .map(|data| {
            data.fields()
                .iter()
                .zip(data.values().iter())
                .map(|(field, value)| {
                    let serialized = if value.is_null() {
                        None
                    } else {
                        Some(value.serialize())
                    };
                    (field.name().to_owned(), serialized)
                })
                .collect::<HashMap<String, Option<String>>>()
        })
        .unwrap_or_default();
    Add {
        path: view.path().to_string(),
        partition_values,
        size: view.size(),
        modification_time: view.modification_time(),
        data_change: true,
        stats: view.stats(),
        tags: None,
        deletion_vector: view.deletion_vector_descriptor(),
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    }
}

fn add_actions_from_snapshot(snapshot: &EagerSnapshot) -> Vec<Add> {
    snapshot
        .log_data()
        .iter()
        .map(|view| logical_view_to_add(&view))
        .collect()
}

pub async fn delta_add_actions(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    gate: Option<DeltaFeatureGate>,
) -> Result<(DeltaSnapshotInfo, Vec<DeltaAddActionPayload>), DeltaTableError> {
    let table = load_delta_table(table_uri, storage_options, version, timestamp, None).await?;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    if let Some(gate) = gate {
        protocol_gate(&snapshot, &gate)?;
    }
    let eager = table.snapshot()?.snapshot().clone();
    let payloads = add_actions_from_snapshot(&eager)
        .into_iter()
        .map(delta_add_payload)
        .collect();
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
    let add_actions = add_actions_from_snapshot(&snapshot);
    let mut by_path: HashMap<String, Add> = HashMap::new();
    for add in add_actions {
        by_path.insert(decode_add_path(add.path.as_str()), add);
    }
    let root = table.table_url().to_string();
    let root_path = Path::new(root.as_str());
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
        return stripped
            .to_string_lossy()
            .trim_start_matches('/')
            .to_owned();
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
