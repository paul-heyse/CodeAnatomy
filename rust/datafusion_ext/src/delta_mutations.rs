use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::execution::context::SessionContext;
use deltalake::delta_datafusion::DeltaDataChecker;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::models::Transaction;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::EagerSnapshot;
use deltalake::operations::write::SchemaMode;
use deltalake::protocol::SaveMode;
use deltalake::table::Constraint;

use crate::delta_common::{eager_snapshot, latest_operation_metrics, snapshot_with_gate};
use crate::delta_control_plane::load_delta_table;
use crate::delta_protocol::{delta_snapshot_info, DeltaSnapshotInfo, TableVersion};
use crate::{DeltaCommitOptions, DeltaFeatureGate};

#[derive(Debug, Clone)]
pub struct DeltaMutationReport {
    pub operation: String,
    pub version: i64,
    pub snapshot: DeltaSnapshotInfo,
    pub metrics: serde_json::Value,
}

#[derive(Clone)]
pub struct DeltaDataCheckRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub gate: Option<DeltaFeatureGate>,
    pub data_ipc: &'a [u8],
    pub extra_constraints: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct DeltaWriteIpcRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub data_ipc: &'a [u8],
    pub save_mode: SaveMode,
    pub schema_mode_label: Option<String>,
    pub partition_columns: Option<Vec<String>>,
    pub target_file_size: Option<usize>,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
    pub extra_constraints: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct DeltaWriteBatchesRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub batches: Vec<RecordBatch>,
    pub save_mode: SaveMode,
    pub schema_mode_label: Option<String>,
    pub partition_columns: Option<Vec<String>>,
    pub target_file_size: Option<usize>,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
    pub extra_constraints: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct DeltaDeleteRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub predicate: Option<String>,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
}

#[derive(Clone)]
pub struct DeltaUpdateRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub predicate: Option<String>,
    pub updates: HashMap<String, String>,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
}

#[derive(Clone)]
pub struct DeltaMergeRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub source_table: &'a str,
    pub predicate: String,
    pub source_alias: Option<String>,
    pub target_alias: Option<String>,
    pub matched_predicate: Option<String>,
    pub matched_updates: HashMap<String, String>,
    pub not_matched_predicate: Option<String>,
    pub not_matched_inserts: HashMap<String, String>,
    pub not_matched_by_source_predicate: Option<String>,
    pub delete_not_matched_by_source: bool,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
    pub extra_constraints: Option<Vec<String>>,
}

pub(crate) fn commit_properties(options: Option<DeltaCommitOptions>) -> CommitProperties {
    let mut commit = CommitProperties::default();
    let Some(options) = options else {
        return commit;
    };
    if let Some(max_retries) = options.max_retries {
        if max_retries >= 0 {
            commit = commit.with_max_retries(max_retries as usize);
        }
    }
    if let Some(create_checkpoint) = options.create_checkpoint {
        commit = commit.with_create_checkpoint(create_checkpoint);
    }
    if !options.metadata.is_empty() {
        let metadata = options
            .metadata
            .into_iter()
            .map(|(key, value)| (key, serde_json::Value::String(value)));
        commit = commit.with_metadata(metadata);
    }
    if let Some(app_txn) = options.app_transaction {
        let txn = Transaction::new_with_last_update(
            app_txn.app_id,
            app_txn.version,
            app_txn.last_updated,
        );
        commit = commit.with_application_transaction(txn);
    }
    commit
}

fn delta_constraints(extra_constraints: Option<Vec<String>>) -> Vec<Constraint> {
    let mut constraints: Vec<Constraint> = Vec::new();
    let Some(extra_constraints) = extra_constraints else {
        return constraints;
    };
    for (index, expr) in extra_constraints.into_iter().enumerate() {
        let trimmed = expr.trim();
        if trimmed.is_empty() {
            continue;
        }
        let name = format!("extra_{index}");
        constraints.push(Constraint::new(&name, trimmed));
    }
    constraints
}

fn batches_from_ipc(data_ipc: &[u8]) -> Result<Vec<RecordBatch>, DeltaTableError> {
    let reader = StreamReader::try_new(Cursor::new(data_ipc.to_vec()), None).map_err(|err| {
        DeltaTableError::Generic(format!("Failed to decode Arrow IPC stream: {err}"))
    })?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch in reader {
        let batch = batch
            .map_err(|err| DeltaTableError::Generic(format!("Invalid Arrow IPC batch: {err}")))?;
        batches.push(batch);
    }
    Ok(batches)
}

async fn run_constraint_check(
    session_ctx: &SessionContext,
    snapshot: &EagerSnapshot,
    batches: &[RecordBatch],
    extra_constraints: Option<Vec<String>>,
) -> Result<Vec<String>, DeltaTableError> {
    let constraints = delta_constraints(extra_constraints);
    let mut checker = DeltaDataChecker::new(snapshot).with_session_context(session_ctx.clone());
    if !constraints.is_empty() {
        checker = checker.with_extra_constraints(constraints);
    }
    let mut violations: Vec<String> = Vec::new();
    for batch in batches {
        match checker.check_batch(batch).await {
            Ok(()) => {}
            Err(DeltaTableError::InvalidData {
                violations: batch_violations,
            }) => {
                violations.extend(batch_violations);
            }
            Err(err) => return Err(err),
        }
    }
    Ok(violations)
}

fn ensure_no_violations(violations: Vec<String>) -> Result<(), DeltaTableError> {
    if violations.is_empty() {
        return Ok(());
    }
    Err(DeltaTableError::InvalidData { violations })
}

fn schema_mode_from_label(label: Option<String>) -> Result<Option<SchemaMode>, DeltaTableError> {
    let Some(label) = label else {
        return Ok(None);
    };
    match label.as_str() {
        "merge" => Ok(Some(SchemaMode::Merge)),
        "overwrite" => Ok(Some(SchemaMode::Overwrite)),
        other => Err(DeltaTableError::Generic(format!(
            "Unsupported Delta schema_mode: {other}"
        ))),
    }
}

pub async fn delta_data_check_request(
    request: DeltaDataCheckRequest<'_>,
) -> Result<Vec<String>, DeltaTableError> {
    let DeltaDataCheckRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        gate,
        data_ipc,
        extra_constraints,
    } = request;
    let table = load_delta_table(
        table_uri,
        storage_options,
        table_version,
        Some(session_ctx),
    )
    .await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let snapshot = eager_snapshot(&table)?;
    let batches = batches_from_ipc(data_ipc)?;
    run_constraint_check(session_ctx, &snapshot, &batches, extra_constraints).await
}

pub async fn delta_write_ipc_request(
    request: DeltaWriteIpcRequest<'_>,
) -> Result<DeltaMutationReport, DeltaTableError> {
    let DeltaWriteIpcRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        data_ipc,
        save_mode,
        schema_mode_label,
        partition_columns,
        target_file_size,
        gate,
        commit_options,
        extra_constraints,
    } = request;
    let batches = batches_from_ipc(data_ipc)?;
    delta_write_batches_request(DeltaWriteBatchesRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        batches,
        save_mode,
        schema_mode_label,
        partition_columns,
        target_file_size,
        gate,
        commit_options,
        extra_constraints,
    })
    .await
}

pub async fn delta_write_batches_request(
    request: DeltaWriteBatchesRequest<'_>,
) -> Result<DeltaMutationReport, DeltaTableError> {
    let DeltaWriteBatchesRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        batches,
        save_mode,
        schema_mode_label,
        partition_columns,
        target_file_size,
        gate,
        commit_options,
        extra_constraints,
    } = request;
    let table = load_delta_table(
        table_uri,
        storage_options,
        table_version,
        Some(session_ctx),
    )
    .await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let eager_snapshot = eager_snapshot(&table)?;
    let violations =
        run_constraint_check(session_ctx, &eager_snapshot, &batches, extra_constraints).await?;
    ensure_no_violations(violations)?;
    let schema_mode = schema_mode_from_label(schema_mode_label)?;
    let mut builder = table.write(batches).with_save_mode(save_mode);
    if let Some(schema_mode) = schema_mode {
        builder = builder.with_schema_mode(schema_mode);
    }
    if let Some(columns) = partition_columns {
        builder = builder.with_partition_columns(columns);
    }
    if let Some(target_file_size) = target_file_size {
        builder = builder.with_target_file_size(target_file_size);
    }
    builder = builder.with_commit_properties(commit_properties(commit_options));
    let table = builder.await?;
    let metrics = latest_operation_metrics(&table).await;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMutationReport {
        operation: "write".to_owned(),
        version,
        snapshot,
        metrics,
    })
}

pub async fn delta_delete_request(
    request: DeltaDeleteRequest<'_>,
) -> Result<DeltaMutationReport, DeltaTableError> {
    let DeltaDeleteRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        predicate,
        gate,
        commit_options,
    } = request;
    let table = load_delta_table(
        table_uri,
        storage_options,
        table_version,
        Some(session_ctx),
    )
    .await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let mut builder = table.delete();
    if let Some(predicate) = predicate {
        builder = builder.with_predicate(predicate);
    }
    builder = builder.with_commit_properties(commit_properties(commit_options));
    let (table, _metrics) = builder.await?;
    let metrics = latest_operation_metrics(&table).await;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMutationReport {
        operation: "delete".to_owned(),
        version,
        snapshot,
        metrics,
    })
}

pub async fn delta_update_request(
    request: DeltaUpdateRequest<'_>,
) -> Result<DeltaMutationReport, DeltaTableError> {
    let DeltaUpdateRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        predicate,
        updates,
        gate,
        commit_options,
    } = request;
    let table = load_delta_table(
        table_uri,
        storage_options,
        table_version,
        Some(session_ctx),
    )
    .await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let session_state: Arc<dyn Session> = Arc::new(session_ctx.state());
    let mut builder = table.update().with_session_state(session_state);
    if let Some(predicate) = predicate {
        builder = builder.with_predicate(predicate);
    }
    for (column, expr) in updates {
        builder = builder.with_update(column, expr);
    }
    builder = builder.with_commit_properties(commit_properties(commit_options));
    let (table, _metrics) = builder.await?;
    let metrics = latest_operation_metrics(&table).await;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMutationReport {
        operation: "update".to_owned(),
        version,
        snapshot,
        metrics,
    })
}

pub async fn delta_merge_request(
    request: DeltaMergeRequest<'_>,
) -> Result<DeltaMutationReport, DeltaTableError> {
    let DeltaMergeRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        source_table,
        predicate,
        source_alias,
        target_alias,
        matched_predicate,
        matched_updates,
        not_matched_predicate,
        not_matched_inserts,
        not_matched_by_source_predicate,
        delete_not_matched_by_source,
        gate,
        commit_options,
        extra_constraints,
    } = request;
    let table = load_delta_table(
        table_uri,
        storage_options,
        table_version,
        Some(session_ctx),
    )
    .await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let eager_snapshot = eager_snapshot(&table)?;
    let source_df = session_ctx.table(source_table).await.map_err(|err| {
        DeltaTableError::Generic(format!("Delta merge source table resolution failed: {err}"))
    })?;
    let source_batches = source_df.clone().collect().await.map_err(|err| {
        DeltaTableError::Generic(format!("Delta merge source collection failed: {err}"))
    })?;
    let violations = run_constraint_check(
        session_ctx,
        &eager_snapshot,
        &source_batches,
        extra_constraints,
    )
    .await?;
    ensure_no_violations(violations)?;
    let session_state: Arc<dyn Session> = Arc::new(session_ctx.state());
    let mut builder = table
        .merge(source_df, predicate)
        .with_session_state(session_state);
    if let Some(alias) = source_alias {
        builder = builder.with_source_alias(alias);
    }
    if let Some(alias) = target_alias {
        builder = builder.with_target_alias(alias);
    }
    if !matched_updates.is_empty() {
        let matched_predicate = matched_predicate.clone();
        let matched_pairs: Vec<(String, String)> = matched_updates.into_iter().collect();
        builder = builder.when_matched_update(|update| {
            let mut clause = update;
            if let Some(predicate) = matched_predicate {
                clause = clause.predicate(predicate);
            }
            for (column, expr) in matched_pairs {
                clause = clause.update(column, expr);
            }
            clause
        })?;
    }
    if !not_matched_inserts.is_empty() {
        let not_matched_predicate = not_matched_predicate.clone();
        let insert_pairs: Vec<(String, String)> = not_matched_inserts.into_iter().collect();
        builder = builder.when_not_matched_insert(|insert| {
            let mut clause = insert;
            if let Some(predicate) = not_matched_predicate {
                clause = clause.predicate(predicate);
            }
            for (column, expr) in insert_pairs {
                clause = clause.set(column, expr);
            }
            clause
        })?;
    }
    if delete_not_matched_by_source {
        let not_matched_by_source_predicate = not_matched_by_source_predicate.clone();
        builder = builder.when_not_matched_by_source_delete(|delete| {
            let mut clause = delete;
            if let Some(predicate) = not_matched_by_source_predicate {
                clause = clause.predicate(predicate);
            }
            clause
        })?;
    }
    builder = builder.with_commit_properties(commit_properties(commit_options));
    let (table, _metrics) = builder.await?;
    let metrics = latest_operation_metrics(&table).await;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMutationReport {
        operation: "merge".to_owned(),
        version,
        snapshot,
        metrics,
    })
}
