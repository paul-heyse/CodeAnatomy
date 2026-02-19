//! Delta mutation bridge surface.

use std::collections::HashMap;

use crate::delta_mutations::{
    delta_delete_request as delta_delete_native, delta_merge_request as delta_merge_native,
    delta_update_request as delta_update_native, DeltaDeleteRequest, DeltaMergeRequest,
    DeltaUpdateRequest,
};
use codeanatomy_engine::executor::delta_writer::{
    execute_delta_write_ipc as execute_delta_write_native, DeltaWritePayload,
};
use datafusion_ext::delta_observability::mutation_report_payload;
use datafusion_ext::{DeltaCommitOptions, DeltaFeatureGate};
use deltalake::protocol::SaveMode;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde::Deserialize;
use serde_json::json;
use tracing::instrument;

use super::helpers::{
    extract_session_ctx, json_to_py, mutation_report_to_pydict, parse_msgpack_payload, runtime,
    table_version_from_options,
};

#[derive(Debug, Deserialize)]
struct DeltaWriteRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    data_ipc: Vec<u8>,
    mode: String,
    schema_mode: Option<String>,
    partition_columns: Option<Vec<String>>,
    target_file_size: Option<usize>,
    extra_constraints: Option<Vec<String>>,
    table_properties: Option<HashMap<String, String>>,
    enable_features: Option<Vec<String>>,
    commit_metadata_required: Option<HashMap<String, String>>,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
}

#[derive(Debug, Deserialize)]
struct DeltaDeleteRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    predicate: Option<String>,
    extra_constraints: Option<Vec<String>>,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
}

#[derive(Debug, Deserialize)]
struct DeltaUpdateRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    predicate: Option<String>,
    updates: HashMap<String, String>,
    extra_constraints: Option<Vec<String>>,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
}

#[derive(Debug, Deserialize)]
struct DeltaMergeRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    source_table: String,
    predicate: String,
    source_alias: Option<String>,
    target_alias: Option<String>,
    matched_predicate: Option<String>,
    matched_updates: HashMap<String, String>,
    not_matched_predicate: Option<String>,
    not_matched_inserts: HashMap<String, String>,
    not_matched_by_source_predicate: Option<String>,
    delete_not_matched_by_source: bool,
    extra_constraints: Option<Vec<String>>,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
}

fn save_mode_from_str(mode: &str) -> PyResult<SaveMode> {
    match mode.to_ascii_lowercase().as_str() {
        "append" => Ok(SaveMode::Append),
        "overwrite" => Ok(SaveMode::Overwrite),
        other => Err(PyValueError::new_err(format!(
            "Unsupported Delta save mode: {other}. Expected 'append' or 'overwrite'."
        ))),
    }
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_write_ipc_request(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaWriteRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_write_ipc_request")?;
    let save_mode = save_mode_from_str(request.mode.as_str())?;
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(execute_delta_write_native(
            &extract_session_ctx(ctx)?,
            DeltaWritePayload {
                table_uri: request.table_uri,
                storage_options: request.storage_options,
                table_version,
                save_mode,
                schema_mode_label: request.schema_mode,
                partition_columns: request.partition_columns,
                target_file_size: request.target_file_size,
                gate: request.gate,
                commit_options: request.commit_options,
                extra_constraints: request.extra_constraints.clone(),
                table_properties: request.table_properties.clone(),
                enable_features: request.enable_features.clone(),
                commit_metadata_required: request.commit_metadata_required.clone(),
            },
            request.data_ipc.as_slice(),
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta write failed: {err}")))?;
    let mutation_report = mutation_report_payload(&report);
    let constraint_status = if request
        .extra_constraints
        .as_ref()
        .is_some_and(|constraints| !constraints.is_empty())
    {
        "checked"
    } else {
        "skipped"
    };
    let metadata_required = request.commit_metadata_required.unwrap_or_default();
    let metadata_present = metadata_required.iter().all(|(key, expected)| {
        mutation_report
            .get(key)
            .is_some_and(|value| value == expected)
    });
    let enabled_features = request.table_properties.unwrap_or_default();
    let response = json!({
        "operation": "write",
        "final_version": report.version,
        "mutation_report": mutation_report,
        "enabled_features": enabled_features,
        "constraint_status": constraint_status,
        "constraint_outcome": {
            "status": constraint_status,
            "requested": request.extra_constraints.unwrap_or_default(),
        },
        "feature_outcomes": {
            "requested": request.enable_features.unwrap_or_default(),
            "enabled": enabled_features,
        },
        "metadata_outcome": {
            "required": metadata_required,
            "present": metadata_present,
        },
    });
    json_to_py(py, &response)
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_delete_request(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaDeleteRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_delete_request")?;
    let _ = request.extra_constraints;
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_delete_native(DeltaDeleteRequest {
            session_ctx: &extract_session_ctx(ctx)?,
            table_uri: &request.table_uri,
            storage_options: request.storage_options,
            table_version,
            predicate: request.predicate,
            gate: request.gate,
            commit_options: request.commit_options,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta delete failed: {err}")))?;
    mutation_report_to_pydict(py, &report)
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_update_request(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaUpdateRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_update_request")?;
    if request.updates.is_empty() {
        return Err(PyValueError::new_err(
            "Delta update requires at least one column assignment.",
        ));
    }
    let _ = request.extra_constraints;
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_update_native(DeltaUpdateRequest {
            session_ctx: &extract_session_ctx(ctx)?,
            table_uri: &request.table_uri,
            storage_options: request.storage_options,
            table_version,
            predicate: request.predicate,
            updates: request.updates,
            gate: request.gate,
            commit_options: request.commit_options,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta update failed: {err}")))?;
    mutation_report_to_pydict(py, &report)
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_merge_request(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaMergeRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_merge_request")?;
    if request.matched_updates.is_empty()
        && request.not_matched_inserts.is_empty()
        && !request.delete_not_matched_by_source
    {
        return Err(PyValueError::new_err(
            "Delta merge requires a matched update, not-matched insert, or not-matched-by-source delete.",
        ));
    }
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_merge_native(DeltaMergeRequest {
            session_ctx: &extract_session_ctx(ctx)?,
            table_uri: &request.table_uri,
            storage_options: request.storage_options,
            table_version,
            source_table: &request.source_table,
            predicate: request.predicate,
            source_alias: request.source_alias,
            target_alias: request.target_alias,
            matched_predicate: request.matched_predicate,
            matched_updates: request.matched_updates,
            not_matched_predicate: request.not_matched_predicate,
            not_matched_inserts: request.not_matched_inserts,
            not_matched_by_source_predicate: request.not_matched_by_source_predicate,
            delete_not_matched_by_source: request.delete_not_matched_by_source,
            gate: request.gate,
            commit_options: request.commit_options,
            extra_constraints: request.extra_constraints,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta merge failed: {err}")))?;
    mutation_report_to_pydict(py, &report)
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(delta_write_ipc_request, module)?)?;
    module.add_function(wrap_pyfunction!(delta_delete_request, module)?)?;
    module.add_function(wrap_pyfunction!(delta_update_request, module)?)?;
    module.add_function(wrap_pyfunction!(delta_merge_request, module)?)?;
    Ok(())
}
