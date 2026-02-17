use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
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

use crate::delta_common::{eager_snapshot, parse_rfc3339, snapshot_with_gate};
use crate::delta_protocol::{DeltaSnapshotInfo, TableVersion};
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

#[derive(Clone)]
pub struct DeltaProviderFromSessionRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub predicate: Option<String>,
    pub overrides: DeltaScanOverrides,
    pub gate: Option<DeltaFeatureGate>,
}

#[derive(Clone)]
pub struct DeltaProviderWithFilesRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub overrides: DeltaScanOverrides,
    pub files: Vec<String>,
    pub gate: Option<DeltaFeatureGate>,
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
    table_version: TableVersion,
    session_ctx: Option<&SessionContext>,
) -> Result<DeltaTableBuilder, DeltaTableError> {
    let table_url = ensure_table_uri(table_uri)?;
    let mut builder = DeltaTableBuilder::from_url(table_url.clone())?;
    if let Some(store) = session_object_store(session_ctx, &table_url)? {
        builder = builder.with_storage_backend(store, table_url.clone());
    }
    if let Some(options) = storage_options {
        builder = builder.with_storage_options(options);
    }
    builder = table_version.apply(builder)?;
    Ok(builder)
}

pub async fn load_delta_table(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    table_version: TableVersion,
    session_ctx: Option<&SessionContext>,
) -> Result<DeltaTable, DeltaTableError> {
    let builder = delta_table_builder(table_uri, storage_options, table_version, session_ctx)?;
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

fn can_use_table_provider_builder(config: &DeltaScanConfig) -> bool {
    config.enable_parquet_pushdown
        && config.wrap_partition_values
        && config.schema_force_view_types
        && config.schema.is_none()
}

async fn build_table_provider(
    table: &DeltaTable,
    eager_snapshot: EagerSnapshot,
    scan_config: DeltaScanConfig,
    files: Option<Vec<Add>>,
) -> Result<Arc<dyn TableProvider>, DeltaTableError> {
    if files.is_none() && can_use_table_provider_builder(&scan_config) {
        let mut builder = table.table_provider();
        if let Some(file_column_name) = scan_config.file_column_name.as_deref() {
            builder = builder.with_file_column(file_column_name);
        }
        return builder
            .await
            .map_err(|err| DeltaTableError::Generic(err.to_string()));
    }

    let mut provider = DeltaTableProvider::try_new(eager_snapshot, table.log_store(), scan_config)?;
    if let Some(files) = files {
        provider = provider.with_files(files);
    }
    Ok(Arc::new(provider))
}

pub async fn snapshot_info_with_gate(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    table_version: TableVersion,
    gate: Option<DeltaFeatureGate>,
) -> Result<DeltaSnapshotInfo, DeltaTableError> {
    let table = load_delta_table(table_uri, storage_options, table_version, None).await?;
    snapshot_with_gate(table_uri, &table, gate).await
}

pub fn scan_config_from_session(
    session: &dyn Session,
    snapshot: Option<&EagerSnapshot>,
    overrides: DeltaScanOverrides,
) -> Result<DeltaScanConfig, DeltaTableError> {
    let base = DeltaScanConfig::new_from_session(session);
    match snapshot {
        None => {
            let mut config = base
                .clone()
                .with_parquet_pushdown(
                    overrides
                        .enable_parquet_pushdown
                        .unwrap_or(base.enable_parquet_pushdown),
                )
                .with_wrap_partition_values(
                    overrides
                        .wrap_partition_values
                        .unwrap_or(base.wrap_partition_values),
                );
            if let Some(file_column_name) = overrides.file_column_name.or(base.file_column_name) {
                config = config.with_file_column_name(file_column_name);
            }
            if let Some(schema) = overrides.schema.or(base.schema) {
                config = config.with_schema(schema);
            }
            Ok(DeltaScanConfig {
                schema_force_view_types: overrides
                    .schema_force_view_types
                    .unwrap_or(base.schema_force_view_types),
                ..config
            })
        }
        Some(snapshot) => {
            let mut builder = DeltaScanConfigBuilder::new()
                .with_parquet_pushdown(
                    overrides
                        .enable_parquet_pushdown
                        .unwrap_or(base.enable_parquet_pushdown),
                )
                .wrap_partition_values(
                    overrides
                        .wrap_partition_values
                        .unwrap_or(base.wrap_partition_values),
                );
            if let Some(file_column_name) = overrides.file_column_name.or(base.file_column_name) {
                builder = builder.with_file_column_name(&file_column_name);
            }
            if let Some(schema) = overrides.schema.or(base.schema) {
                builder = builder.with_schema(schema);
            }
            let config = builder.build(snapshot)?;
            Ok(DeltaScanConfig {
                schema_force_view_types: overrides
                    .schema_force_view_types
                    .unwrap_or(base.schema_force_view_types),
                ..config
            })
        }
    }
}

fn files_matching_predicate(
    session: &dyn Session,
    log_data: LogDataHandler<'_>,
    filters: &[Expr],
) -> Result<Vec<Add>, DeltaTableError> {
    if let Some(Some(predicate)) =
        (!filters.is_empty()).then_some(conjunction(filters.iter().cloned()))
    {
        let expr =
            session.create_physical_expr(predicate, &log_data.read_schema().to_dfschema()?)?;
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

pub async fn delta_provider_from_session_request(
    request: DeltaProviderFromSessionRequest<'_>,
) -> Result<
    (
        Arc<dyn TableProvider>,
        DeltaSnapshotInfo,
        DeltaScanConfig,
        Option<Vec<DeltaAddActionPayload>>,
        Option<String>,
    ),
    DeltaTableError,
> {
    let DeltaProviderFromSessionRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        predicate,
        overrides,
        gate,
    } = request;
    let table = load_delta_table(
        table_uri,
        storage_options,
        table_version,
        Some(session_ctx),
    )
    .await?;
    update_datafusion_session(&table, session_ctx)?;
    let snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let eager_snapshot = eager_snapshot(&table)?;
    let session_state = session_ctx.state();
    let scan_config = scan_config_from_session(&session_state, Some(&eager_snapshot), overrides)?;
    let mut selected_files: Option<Vec<Add>> = None;
    let mut add_payloads: Option<Vec<DeltaAddActionPayload>> = None;
    let mut predicate_error: Option<String> = None;
    if let Some(predicate) = predicate {
        match eager_snapshot.parse_predicate_expression(predicate, &session_state) {
            Ok(expr) => {
                match files_matching_predicate(&session_state, eager_snapshot.log_data(), &[expr]) {
                    Ok(add_actions) => {
                        selected_files = Some(add_actions.clone());
                        add_payloads =
                            Some(add_actions.into_iter().map(delta_add_payload).collect());
                    }
                    Err(err) => {
                        predicate_error = Some(err.to_string());
                    }
                }
            }
            Err(err) => {
                predicate_error = Some(err.to_string());
            }
        }
    }
    let provider =
        build_table_provider(&table, eager_snapshot, scan_config.clone(), selected_files).await?;
    Ok((
        provider,
        snapshot,
        scan_config,
        add_payloads,
        predicate_error,
    ))
}

pub async fn delta_cdf_provider(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    options: DeltaCdfScanOptions,
    gate: Option<DeltaFeatureGate>,
) -> Result<(DeltaCdfTableProvider, DeltaSnapshotInfo), DeltaTableError> {
    let table_version = TableVersion::from_options(version, timestamp)?;
    let table = load_delta_table(table_uri, storage_options, table_version, None).await?;
    let snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let mut cdf_builder = table.scan_cdf();
    if let Some(starting_version) = options.starting_version {
        cdf_builder = cdf_builder.with_starting_version(starting_version);
    }
    if let Some(ending_version) = options.ending_version {
        cdf_builder = cdf_builder.with_ending_version(ending_version);
    }
    if let Some(starting_timestamp) = options.starting_timestamp {
        let dt = parse_rfc3339(starting_timestamp.as_str())?;
        cdf_builder = cdf_builder.with_starting_timestamp(dt);
    }
    if let Some(ending_timestamp) = options.ending_timestamp {
        let dt = parse_rfc3339(ending_timestamp.as_str())?;
        cdf_builder = cdf_builder.with_ending_timestamp(dt);
    }
    if options.allow_out_of_range {
        cdf_builder = cdf_builder.with_allow_out_of_range();
    }
    let provider = DeltaCdfTableProvider::try_new(cdf_builder)?;
    Ok((provider, snapshot))
}

pub async fn delta_provider_with_files_request(
    request: DeltaProviderWithFilesRequest<'_>,
) -> Result<
    (
        Arc<dyn TableProvider>,
        DeltaSnapshotInfo,
        DeltaScanConfig,
        Vec<DeltaAddActionPayload>,
    ),
    DeltaTableError,
> {
    let DeltaProviderWithFilesRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        overrides,
        files,
        gate,
    } = request;
    let table = load_delta_table(
        table_uri,
        storage_options,
        table_version,
        Some(session_ctx),
    )
    .await?;
    update_datafusion_session(&table, session_ctx)?;
    let snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let eager_snapshot = eager_snapshot(&table)?;
    let session_state = session_ctx.state();
    let scan_config = scan_config_from_session(&session_state, Some(&eager_snapshot), overrides)?;
    let add_actions = add_actions_for_paths(&table, &files)?;
    let provider =
        build_table_provider(&table, eager_snapshot, scan_config.clone(), Some(add_actions.clone()))
            .await?;
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
    let table_version = TableVersion::from_options(version, timestamp)?;
    let table = load_delta_table(table_uri, storage_options, table_version, None).await?;
    let snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let eager = eager_snapshot(&table)?;
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
    let snapshot = eager_snapshot(table)?;
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

pub fn parse_timestamp(timestamp: &str) -> Result<chrono::DateTime<chrono::Utc>, DeltaTableError> {
    parse_rfc3339(timestamp)
}
