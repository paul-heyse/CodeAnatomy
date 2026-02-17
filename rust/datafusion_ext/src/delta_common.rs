use chrono::{DateTime, Utc};
use deltalake::errors::DeltaTableError;
use deltalake::kernel::EagerSnapshot;
use deltalake::DeltaTable;

use crate::delta_protocol::{delta_snapshot_info, protocol_gate, DeltaSnapshotInfo};
use crate::DeltaFeatureGate;

pub async fn latest_operation_metrics(table: &DeltaTable) -> serde_json::Value {
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

pub fn eager_snapshot(table: &DeltaTable) -> Result<EagerSnapshot, DeltaTableError> {
    Ok(table.snapshot()?.snapshot().clone())
}

pub async fn snapshot_with_gate(
    table_uri: &str,
    table: &DeltaTable,
    gate: Option<DeltaFeatureGate>,
) -> Result<DeltaSnapshotInfo, DeltaTableError> {
    let snapshot = delta_snapshot_info(table_uri, table).await?;
    if let Some(gate) = gate {
        protocol_gate(&snapshot, &gate)?;
    }
    Ok(snapshot)
}

pub fn parse_rfc3339(timestamp: &str) -> Result<DateTime<Utc>, DeltaTableError> {
    DateTime::parse_from_rfc3339(timestamp)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| DeltaTableError::Generic(format!("Invalid Delta timestamp: {err}")))
}
