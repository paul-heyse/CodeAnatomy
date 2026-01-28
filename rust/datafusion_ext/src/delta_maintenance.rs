use std::collections::HashMap;
use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use datafusion::execution::context::SessionContext;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::models::TableFeatures;

use crate::delta_control_plane::load_delta_table;
use crate::delta_mutations::{commit_properties, DeltaCommitOptions};
use crate::delta_protocol::{
    delta_snapshot_info, protocol_gate, DeltaFeatureGate, DeltaSnapshotInfo,
};

#[derive(Debug, Clone)]
pub struct DeltaMaintenanceReport {
    pub operation: String,
    pub version: i64,
    pub snapshot: DeltaSnapshotInfo,
    pub metrics: serde_json::Value,
}

async fn latest_operation_metrics(table: &deltalake::DeltaTable) -> serde_json::Value {
    let mut history = match table.history(Some(1)).await {
        Ok(history) => history,
        Err(err) => {
            return serde_json::Value::String(format!(
                "Failed to fetch Delta history for metrics: {err}"
            ));
        }
    };
    match history
        .next()
        .and_then(|commit| commit.info.get("operationMetrics").cloned())
    {
        Some(metrics) => metrics,
        None => serde_json::Value::Null,
    }
}

async fn snapshot_with_gate(
    table_uri: &str,
    table: &deltalake::DeltaTable,
    gate: Option<DeltaFeatureGate>,
) -> Result<DeltaSnapshotInfo, DeltaTableError> {
    let snapshot = delta_snapshot_info(table_uri, table).await?;
    if let Some(gate) = gate {
        protocol_gate(&snapshot, &gate)?;
    }
    Ok(snapshot)
}

fn retention_duration_hours(hours: Option<i64>) -> Option<Duration> {
    let Some(hours) = hours else {
        return None;
    };
    if hours <= 0 {
        return None;
    }
    Some(Duration::hours(hours))
}

fn parse_rfc3339(timestamp: &str) -> Result<DateTime<Utc>, DeltaTableError> {
    let parsed = DateTime::parse_from_rfc3339(timestamp)
        .map_err(|err| DeltaTableError::Generic(format!("Invalid RFC3339 timestamp: {err}")))?;
    Ok(parsed.with_timezone(&Utc))
}

pub async fn delta_optimize_compact(
    session_ctx: &SessionContext,
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    target_size: Option<u64>,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let table = load_delta_table(
        table_uri,
        storage_options,
        version,
        timestamp,
        Some(session_ctx),
    )
    .await?;
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

pub async fn delta_vacuum(
    session_ctx: &SessionContext,
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    retention_hours: Option<i64>,
    dry_run: bool,
    enforce_retention_duration: bool,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let table = load_delta_table(
        table_uri,
        storage_options,
        version,
        timestamp,
        Some(session_ctx),
    )
    .await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
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

pub async fn delta_restore(
    session_ctx: &SessionContext,
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    restore_version: Option<i64>,
    restore_timestamp: Option<String>,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    if restore_version.is_some() && restore_timestamp.is_some() {
        return Err(DeltaTableError::Generic(
            "Specify either restore_version or restore_timestamp, not both.".to_owned(),
        ));
    }
    let table = load_delta_table(
        table_uri,
        storage_options,
        version,
        timestamp,
        Some(session_ctx),
    )
    .await?;
    let _snapshot = snapshot_with_gate(table_uri, &table, gate).await?;
    let mut builder = table.restore().with_commit_properties(commit_properties(commit_options));
    if let Some(restore_version) = restore_version {
        builder = builder.with_version_to_restore(restore_version);
    }
    if let Some(restore_timestamp) = restore_timestamp {
        let parsed = parse_rfc3339(restore_timestamp.as_str())?;
        builder = builder.with_datetime_to_restore(parsed);
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

pub async fn delta_set_properties(
    session_ctx: &SessionContext,
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    properties: HashMap<String, String>,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let table = load_delta_table(
        table_uri,
        storage_options,
        version,
        timestamp,
        Some(session_ctx),
    )
    .await?;
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

pub async fn delta_add_features(
    session_ctx: &SessionContext,
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    features: Vec<String>,
    allow_protocol_versions_increase: bool,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
) -> Result<DeltaMaintenanceReport, DeltaTableError> {
    let table = load_delta_table(
        table_uri,
        storage_options,
        version,
        timestamp,
        Some(session_ctx),
    )
    .await?;
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
