use std::collections::HashMap;
use std::str::FromStr;

use chrono::Duration;
use datafusion::execution::context::SessionContext;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::models::TableFeatures;
use deltalake::protocol::checkpoints::{cleanup_metadata, create_checkpoint};
use serde_json::json;
use std::sync::Arc;
use tracing::instrument;

use crate::delta_common::{latest_operation_metrics, parse_rfc3339, snapshot_with_gate};
use crate::delta_control_plane::load_delta_table;
use crate::delta_mutations::commit_properties;
use crate::delta_protocol::{delta_snapshot_info, DeltaSnapshotInfo, TableVersion};
use crate::{DeltaCommitOptions, DeltaFeatureGate};

#[derive(Debug, Clone)]
pub struct DeltaMaintenanceReport {
    pub operation: String,
    pub version: i64,
    pub snapshot: DeltaSnapshotInfo,
    pub metrics: serde_json::Value,
}

#[derive(Clone)]
pub struct DeltaOptimizeCompactRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub target_size: Option<u64>,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
}

#[derive(Clone)]
pub struct DeltaVacuumRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub retention_hours: Option<i64>,
    pub dry_run: bool,
    pub enforce_retention_duration: bool,
    pub require_vacuum_protocol_check: bool,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
}

#[derive(Clone)]
pub struct DeltaRestoreRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub restore_target: TableVersion,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
}

#[derive(Clone)]
pub struct DeltaSetPropertiesRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub properties: HashMap<String, String>,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
}

#[derive(Clone)]
pub struct DeltaAddFeaturesRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub features: Vec<String>,
    pub allow_protocol_versions_increase: bool,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
}

#[derive(Clone)]
pub struct DeltaAddConstraintsRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub constraints: Vec<(String, String)>,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
}

#[derive(Clone)]
pub struct DeltaDropConstraintsRequest<'a> {
    pub session_ctx: &'a SessionContext,
    pub table_uri: &'a str,
    pub storage_options: Option<HashMap<String, String>>,
    pub table_version: TableVersion,
    pub constraints: Vec<String>,
    pub raise_if_not_exists: bool,
    pub gate: Option<DeltaFeatureGate>,
    pub commit_options: Option<DeltaCommitOptions>,
}

fn retention_duration_hours(hours: Option<i64>) -> Option<Duration> {
    let hours = hours?;
    if hours <= 0 {
        return None;
    }
    Some(Duration::hours(hours))
}

#[instrument(skip(request))]
pub async fn delta_optimize_compact_request(
    request: DeltaOptimizeCompactRequest<'_>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let DeltaOptimizeCompactRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        target_size,
        gate,
        commit_options,
    } = request;
    let table =
        load_delta_table(table_uri, storage_options, table_version, Some(session_ctx)).await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let mut builder = table.optimize();
    if let Some(target_size) = target_size {
        builder = builder.with_target_size(target_size);
    }
    builder = builder.with_commit_properties(commit_properties(commit_options));
    let (table, _metrics) = builder.await?;
    let metrics = latest_operation_metrics(&table).await;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMaintenanceReport {
        operation: "optimize".to_owned(),
        version,
        snapshot,
        metrics,
    })
}

#[instrument(skip(request))]
pub async fn delta_vacuum_request(
    request: DeltaVacuumRequest<'_>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let DeltaVacuumRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        retention_hours,
        dry_run,
        enforce_retention_duration,
        require_vacuum_protocol_check,
        gate,
        commit_options,
    } = request;
    let table =
        load_delta_table(table_uri, storage_options, table_version, Some(session_ctx)).await?;
    let snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    if require_vacuum_protocol_check
        && !snapshot
            .writer_features
            .iter()
            .any(|feature| feature == "vacuumProtocolCheck")
    {
        return Err(DeltaTableError::Generic(
            "Delta table does not advertise vacuumProtocolCheck writer feature.".to_owned(),
        ));
    }
    let mut builder = table
        .vacuum()
        .with_dry_run(dry_run)
        .with_enforce_retention_duration(enforce_retention_duration)
        .with_commit_properties(commit_properties(commit_options));
    if let Some(duration) = retention_duration_hours(retention_hours) {
        builder = builder.with_retention_period(duration);
    }
    let (table, _metrics) = builder.await?;
    let metrics = latest_operation_metrics(&table).await;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMaintenanceReport {
        operation: "vacuum".to_owned(),
        version,
        snapshot,
        metrics,
    })
}

#[instrument(skip(request))]
pub async fn delta_restore_request(
    request: DeltaRestoreRequest<'_>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let DeltaRestoreRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        restore_target,
        gate,
        commit_options,
    } = request;
    let table =
        load_delta_table(table_uri, storage_options, table_version, Some(session_ctx)).await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let mut builder = table
        .restore()
        .with_commit_properties(commit_properties(commit_options));
    match restore_target {
        TableVersion::Version(restore_version) => {
            builder = builder.with_version_to_restore(restore_version);
        }
        TableVersion::Timestamp(restore_timestamp) => {
            let parsed = parse_rfc3339(restore_timestamp.as_str())?;
            builder = builder.with_datetime_to_restore(parsed);
        }
        TableVersion::Latest => {
            return Err(DeltaTableError::Generic(
                "restore_target must be version or timestamp".to_string(),
            ));
        }
    }
    let (table, _metrics) = builder.await?;
    let metrics = latest_operation_metrics(&table).await;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMaintenanceReport {
        operation: "restore".to_owned(),
        version,
        snapshot,
        metrics,
    })
}

#[instrument(skip(request))]
pub async fn delta_set_properties_request(
    request: DeltaSetPropertiesRequest<'_>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let DeltaSetPropertiesRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        properties,
        gate,
        commit_options,
    } = request;
    let table =
        load_delta_table(table_uri, storage_options, table_version, Some(session_ctx)).await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let builder = table
        .set_tbl_properties()
        .with_properties(properties)
        .with_commit_properties(commit_properties(commit_options));
    let table = builder.await?;
    let metrics = latest_operation_metrics(&table).await;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMaintenanceReport {
        operation: "set_properties".to_owned(),
        version,
        snapshot,
        metrics,
    })
}

#[instrument(skip(request))]
pub async fn delta_add_features_request(
    request: DeltaAddFeaturesRequest<'_>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let DeltaAddFeaturesRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        features,
        allow_protocol_versions_increase,
        gate,
        commit_options,
    } = request;
    let table =
        load_delta_table(table_uri, storage_options, table_version, Some(session_ctx)).await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let mut parsed_features: Vec<TableFeatures> = Vec::new();
    for feature in features {
        let parsed = TableFeatures::from_str(feature.as_str()).map_err(|_err| {
            DeltaTableError::Generic(format!("Invalid Delta table feature {feature:?}"))
        })?;
        parsed_features.push(parsed);
    }
    let builder = table
        .add_feature()
        .with_features(parsed_features)
        .with_allow_protocol_versions_increase(allow_protocol_versions_increase)
        .with_commit_properties(commit_properties(commit_options));
    let table = builder.await?;
    let metrics = latest_operation_metrics(&table).await;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMaintenanceReport {
        operation: "add_feature".to_owned(),
        version,
        snapshot,
        metrics,
    })
}

#[instrument(skip(session_ctx, storage_options))]
pub async fn delta_create_checkpoint(
    session_ctx: &SessionContext,
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    table_version: TableVersion,
    gate: Option<DeltaFeatureGate>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let table =
        load_delta_table(table_uri, storage_options, table_version, Some(session_ctx)).await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    create_checkpoint(&table, None).await?;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMaintenanceReport {
        operation: "create_checkpoint".to_owned(),
        version,
        snapshot,
        metrics: json!({"checkpoint": "created"}),
    })
}

#[instrument(skip(session_ctx, storage_options))]
pub async fn delta_cleanup_metadata(
    session_ctx: &SessionContext,
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    table_version: TableVersion,
    gate: Option<DeltaFeatureGate>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let table =
        load_delta_table(table_uri, storage_options, table_version, Some(session_ctx)).await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let deleted = cleanup_metadata(&table, None).await?;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMaintenanceReport {
        operation: "cleanup_metadata".to_owned(),
        version,
        snapshot,
        metrics: json!({"deleted_logs": deleted}),
    })
}

#[instrument(skip(request))]
pub async fn delta_add_constraints_request(
    request: DeltaAddConstraintsRequest<'_>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let DeltaAddConstraintsRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        constraints,
        gate,
        commit_options,
    } = request;
    if constraints.is_empty() {
        return Err(DeltaTableError::Generic(
            "Delta add-constraints requires at least one constraint.".to_owned(),
        ));
    }
    let table =
        load_delta_table(table_uri, storage_options, table_version, Some(session_ctx)).await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let mut constraint_map: HashMap<String, String> = HashMap::new();
    for (name, expr) in constraints {
        constraint_map.insert(name, expr);
    }
    let delta_session: SessionContext = deltalake::delta_datafusion::create_session().into();
    let session_state = Arc::new(delta_session.state());
    let builder = table
        .add_constraint()
        .with_constraints(constraint_map)
        .with_session_state(session_state)
        .with_commit_properties(commit_properties(commit_options));
    let table = builder.await?;
    let metrics = latest_operation_metrics(&table).await;
    let snapshot = delta_snapshot_info(table_uri, &table).await?;
    let version = table.version().unwrap_or(snapshot.version);
    Ok(DeltaMaintenanceReport {
        operation: "add_constraints".to_owned(),
        version,
        snapshot,
        metrics,
    })
}

#[instrument(skip(request))]
pub async fn delta_drop_constraints_request(
    request: DeltaDropConstraintsRequest<'_>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let DeltaDropConstraintsRequest {
        session_ctx,
        table_uri,
        storage_options,
        table_version,
        constraints,
        raise_if_not_exists,
        gate,
        commit_options,
    } = request;
    if constraints.is_empty() {
        return Err(DeltaTableError::Generic(
            "Delta drop-constraints requires at least one constraint name.".to_owned(),
        ));
    }
    let table =
        load_delta_table(table_uri, storage_options, table_version, Some(session_ctx)).await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let mut updated_table = table;
    for name in constraints {
        let builder = updated_table
            .drop_constraints()
            .with_constraint(name)
            .with_raise_if_not_exists(raise_if_not_exists)
            .with_commit_properties(commit_properties(commit_options.clone()));
        updated_table = builder.await?;
    }
    let metrics = latest_operation_metrics(&updated_table).await;
    let snapshot = delta_snapshot_info(table_uri, &updated_table).await?;
    let version = updated_table.version().unwrap_or(snapshot.version);
    Ok(DeltaMaintenanceReport {
        operation: "drop_constraints".to_owned(),
        version,
        snapshot,
        metrics,
    })
}
