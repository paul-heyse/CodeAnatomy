use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;

use arrow::array::new_empty_array;
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use deltalake::delta_datafusion::DeltaScanConfig;
use deltalake::errors::DeltaTableError;

use crate::delta_control_plane::DeltaAddActionPayload;
use crate::delta_maintenance::DeltaMaintenanceReport;
use crate::delta_mutations::DeltaMutationReport;
use crate::delta_protocol::DeltaSnapshotInfo;

fn map_strings_to_value(map: &BTreeMap<String, String>) -> serde_json::Value {
    let mut payload: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (key, value) in map {
        payload.insert(key.clone(), serde_json::Value::String(value.clone()));
    }
    match serde_json::to_value(payload) {
        Ok(value) => value,
        Err(_) => serde_json::Value::Null,
    }
}

fn schema_to_ipc(schema: &SchemaRef) -> Result<Vec<u8>, DeltaTableError> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        columns.push(new_empty_array(field.data_type()));
    }
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|err| {
        DeltaTableError::Generic(format!(
            "Failed to construct empty batch for schema IPC: {err}"
        ))
    })?;
    let mut buffer = Cursor::new(Vec::<u8>::new());
    {
        let mut writer = StreamWriter::try_new(&mut buffer, schema.as_ref()).map_err(|err| {
            DeltaTableError::Generic(format!("Failed to open Arrow IPC writer: {err}"))
        })?;
        writer.write(&batch).map_err(|err| {
            DeltaTableError::Generic(format!("Failed to write Arrow IPC schema: {err}"))
        })?;
        writer.finish().map_err(|err| {
            DeltaTableError::Generic(format!("Failed to finalize Arrow IPC schema: {err}"))
        })?;
    }
    Ok(buffer.into_inner())
}

pub fn snapshot_info_as_values(snapshot: &DeltaSnapshotInfo) -> HashMap<String, serde_json::Value> {
    let mut payload: HashMap<String, serde_json::Value> = HashMap::new();
    payload.insert(
        "table_uri".to_owned(),
        serde_json::Value::String(snapshot.table_uri.clone()),
    );
    payload.insert(
        "version".to_owned(),
        serde_json::Value::Number(snapshot.version.into()),
    );
    if let Some(timestamp) = snapshot.snapshot_timestamp {
        payload.insert(
            "snapshot_timestamp".to_owned(),
            serde_json::Value::Number(timestamp.into()),
        );
    }
    payload.insert(
        "min_reader_version".to_owned(),
        serde_json::Value::Number(snapshot.min_reader_version.into()),
    );
    payload.insert(
        "min_writer_version".to_owned(),
        serde_json::Value::Number(snapshot.min_writer_version.into()),
    );
    payload.insert(
        "reader_features".to_owned(),
        match serde_json::to_value(snapshot.reader_features.clone()) {
            Ok(value) => value,
            Err(_) => serde_json::Value::Null,
        },
    );
    payload.insert(
        "writer_features".to_owned(),
        match serde_json::to_value(snapshot.writer_features.clone()) {
            Ok(value) => value,
            Err(_) => serde_json::Value::Null,
        },
    );
    payload.insert(
        "table_properties".to_owned(),
        map_strings_to_value(&snapshot.table_properties),
    );
    payload.insert(
        "schema_json".to_owned(),
        serde_json::Value::String(snapshot.schema_json.clone()),
    );
    payload.insert(
        "partition_columns".to_owned(),
        match serde_json::to_value(snapshot.partition_columns.clone()) {
            Ok(value) => value,
            Err(_) => serde_json::Value::Null,
        },
    );
    payload
}

pub fn scan_config_payload(
    scan_config: &DeltaScanConfig,
) -> Result<HashMap<String, serde_json::Value>, DeltaTableError> {
    let mut payload: HashMap<String, serde_json::Value> = HashMap::new();
    payload.insert(
        "file_column_name".to_owned(),
        match &scan_config.file_column_name {
            Some(name) => serde_json::Value::String(name.clone()),
            None => serde_json::Value::Null,
        },
    );
    payload.insert(
        "enable_parquet_pushdown".to_owned(),
        serde_json::Value::Bool(scan_config.enable_parquet_pushdown),
    );
    payload.insert(
        "schema_force_view_types".to_owned(),
        serde_json::Value::Bool(scan_config.schema_force_view_types),
    );
    payload.insert(
        "wrap_partition_values".to_owned(),
        serde_json::Value::Bool(scan_config.wrap_partition_values),
    );
    payload.insert(
        "has_schema".to_owned(),
        serde_json::Value::Bool(scan_config.schema.is_some()),
    );
    Ok(payload)
}

pub fn scan_config_schema_ipc(
    scan_config: &DeltaScanConfig,
) -> Result<Option<Vec<u8>>, DeltaTableError> {
    let Some(schema) = &scan_config.schema else {
        return Ok(None);
    };
    schema_to_ipc(schema).map(Some)
}

pub fn add_action_payloads(adds: &[DeltaAddActionPayload]) -> serde_json::Value {
    match serde_json::to_value(adds.iter().map(add_payload_row).collect::<Vec<_>>()) {
        Ok(value) => value,
        Err(_) => serde_json::Value::Null,
    }
}

fn add_payload_row(add: &DeltaAddActionPayload) -> HashMap<String, serde_json::Value> {
    let mut payload: HashMap<String, serde_json::Value> = HashMap::new();
    payload.insert(
        "path".to_owned(),
        serde_json::Value::String(add.path.clone()),
    );
    payload.insert(
        "size".to_owned(),
        serde_json::Value::Number(add.size.into()),
    );
    payload.insert(
        "modification_time".to_owned(),
        serde_json::Value::Number(add.modification_time.into()),
    );
    payload.insert(
        "data_change".to_owned(),
        serde_json::Value::Bool(add.data_change),
    );
    payload.insert(
        "partition_values".to_owned(),
        match serde_json::to_value(add.partition_values.clone()) {
            Ok(value) => value,
            Err(_) => serde_json::Value::Null,
        },
    );
    payload.insert(
        "stats".to_owned(),
        match &add.stats {
            Some(stats) => serde_json::Value::String(stats.clone()),
            None => serde_json::Value::Null,
        },
    );
    payload.insert(
        "tags".to_owned(),
        match serde_json::to_value(add.tags.clone()) {
            Ok(value) => value,
            Err(_) => serde_json::Value::Null,
        },
    );
    payload
}

pub fn mutation_report_payload(report: &DeltaMutationReport) -> HashMap<String, serde_json::Value> {
    let mut payload = snapshot_info_as_values(&report.snapshot);
    payload.insert(
        "operation".to_owned(),
        serde_json::Value::String(report.operation.clone()),
    );
    payload.insert(
        "mutation_version".to_owned(),
        serde_json::Value::Number(report.version.into()),
    );
    payload.insert("metrics".to_owned(), report.metrics.clone());
    payload
}

pub fn maintenance_report_payload(
    report: &DeltaMaintenanceReport,
) -> HashMap<String, serde_json::Value> {
    let mut payload = snapshot_info_as_values(&report.snapshot);
    payload.insert(
        "operation".to_owned(),
        serde_json::Value::String(report.operation.clone()),
    );
    payload.insert(
        "maintenance_version".to_owned(),
        serde_json::Value::Number(report.version.into()),
    );
    payload.insert("metrics".to_owned(), report.metrics.clone());
    payload
}
