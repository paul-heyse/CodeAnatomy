//! Delta maintenance bridge surface.

use std::collections::HashMap;

use crate::delta_maintenance::{
    delta_add_constraints_request as delta_add_constraints_native,
    delta_add_features_request as delta_add_features_native,
    delta_cleanup_metadata as delta_cleanup_metadata_native,
    delta_create_checkpoint as delta_create_checkpoint_native,
    delta_drop_constraints_request as delta_drop_constraints_native,
    delta_optimize_compact_request as delta_optimize_compact_native,
    delta_restore_request as delta_restore_native,
    delta_set_properties_request as delta_set_properties_native,
    delta_vacuum_request as delta_vacuum_native, DeltaAddConstraintsRequest,
    DeltaAddFeaturesRequest, DeltaDropConstraintsRequest, DeltaOptimizeCompactRequest,
    DeltaRestoreRequest, DeltaSetPropertiesRequest, DeltaVacuumRequest,
};
use datafusion_ext::{DeltaCommitOptions, DeltaFeatureGate};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde::Deserialize;
use tracing::instrument;

use super::helpers::{
    extract_session_ctx, maintenance_report_to_pydict, parse_msgpack_payload, runtime,
    table_version_from_options,
};

#[derive(Debug, Deserialize)]
struct DeltaOptimizeRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    target_size: Option<i64>,
    z_order_cols: Option<Vec<String>>,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
}

#[derive(Debug, Deserialize)]
struct DeltaVacuumRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    retention_hours: Option<i64>,
    dry_run: bool,
    enforce_retention_duration: bool,
    require_vacuum_protocol_check: bool,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
}

#[derive(Debug, Deserialize)]
struct DeltaRestoreRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    restore_version: Option<i64>,
    restore_timestamp: Option<String>,
    allow_unsafe_restore: bool,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
}

#[derive(Debug, Deserialize)]
struct DeltaSetPropertiesRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    properties: HashMap<String, String>,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
}

#[derive(Debug, Deserialize)]
struct DeltaAddFeaturesRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    features: Vec<String>,
    allow_protocol_versions_increase: bool,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
}

#[derive(Debug, Deserialize)]
struct DeltaAddConstraintsRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    constraints: HashMap<String, String>,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
}

#[derive(Debug, Deserialize)]
struct DeltaDropConstraintsRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    constraints: Vec<String>,
    raise_if_not_exists: bool,
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
}

#[derive(Debug, Deserialize)]
struct DeltaCheckpointRequestPayload {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    gate: Option<DeltaFeatureGate>,
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_optimize_compact_request_payload(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaOptimizeRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_optimize_compact_request")?;
    let _ = request.z_order_cols;
    let target_size = match request.target_size {
        Some(size) => Some(u64::try_from(size).map_err(|err| {
            PyValueError::new_err(format!("Delta optimize target_size is too large: {err}"))
        })?),
        None => None,
    };
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_optimize_compact_native(DeltaOptimizeCompactRequest {
            session_ctx: &extract_session_ctx(ctx)?,
            table_uri: &request.table_uri,
            storage_options: request.storage_options,
            table_version,
            target_size,
            gate: request.gate,
            commit_options: request.commit_options,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta optimize failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_vacuum_request_payload(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaVacuumRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_vacuum_request")?;
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_vacuum_native(DeltaVacuumRequest {
            session_ctx: &extract_session_ctx(ctx)?,
            table_uri: &request.table_uri,
            storage_options: request.storage_options,
            table_version,
            retention_hours: request.retention_hours,
            dry_run: request.dry_run,
            enforce_retention_duration: request.enforce_retention_duration,
            require_vacuum_protocol_check: request.require_vacuum_protocol_check,
            gate: request.gate,
            commit_options: request.commit_options,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta vacuum failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_restore_request_payload(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaRestoreRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_restore_request")?;
    let _ = request.allow_unsafe_restore;
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let restore_target =
        table_version_from_options(request.restore_version, request.restore_timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_restore_native(DeltaRestoreRequest {
            session_ctx: &extract_session_ctx(ctx)?,
            table_uri: &request.table_uri,
            storage_options: request.storage_options,
            table_version,
            restore_target,
            gate: request.gate,
            commit_options: request.commit_options,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta restore failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_set_properties_request_payload(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaSetPropertiesRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_set_properties_request")?;
    if request.properties.is_empty() {
        return Err(PyValueError::new_err(
            "Delta property update requires at least one key/value pair.",
        ));
    }
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_set_properties_native(DeltaSetPropertiesRequest {
            session_ctx: &extract_session_ctx(ctx)?,
            table_uri: &request.table_uri,
            storage_options: request.storage_options,
            table_version,
            properties: request.properties,
            gate: request.gate,
            commit_options: request.commit_options,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta set-properties failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_add_features_request_payload(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaAddFeaturesRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_add_features_request")?;
    if request.features.is_empty() {
        return Err(PyValueError::new_err(
            "Delta add-features requires at least one feature name.",
        ));
    }
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_add_features_native(DeltaAddFeaturesRequest {
            session_ctx: &extract_session_ctx(ctx)?,
            table_uri: &request.table_uri,
            storage_options: request.storage_options,
            table_version,
            features: request.features,
            allow_protocol_versions_increase: request.allow_protocol_versions_increase,
            gate: request.gate,
            commit_options: request.commit_options,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta add-features failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_add_constraints_request_payload(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaAddConstraintsRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_add_constraints_request")?;
    if request.constraints.is_empty() {
        return Err(PyValueError::new_err(
            "Delta add-constraints requires at least one constraint.",
        ));
    }
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_add_constraints_native(DeltaAddConstraintsRequest {
            session_ctx: &extract_session_ctx(ctx)?,
            table_uri: &request.table_uri,
            storage_options: request.storage_options,
            table_version,
            constraints: request.constraints.into_iter().collect(),
            gate: request.gate,
            commit_options: request.commit_options,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta add-constraints failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_drop_constraints_request_payload(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaDropConstraintsRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_drop_constraints_request")?;
    if request.constraints.is_empty() {
        return Err(PyValueError::new_err(
            "Delta drop-constraints requires at least one constraint name.",
        ));
    }
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_drop_constraints_native(DeltaDropConstraintsRequest {
            session_ctx: &extract_session_ctx(ctx)?,
            table_uri: &request.table_uri,
            storage_options: request.storage_options,
            table_version,
            constraints: request.constraints,
            raise_if_not_exists: request.raise_if_not_exists,
            gate: request.gate,
            commit_options: request.commit_options,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta drop-constraints failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_create_checkpoint_request_payload(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaCheckpointRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_create_checkpoint_request")?;
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_create_checkpoint_native(
            &extract_session_ctx(ctx)?,
            &request.table_uri,
            request.storage_options,
            table_version,
            request.gate,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta checkpoint failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
#[instrument(skip(py, ctx, request_msgpack))]
pub(crate) fn delta_cleanup_metadata_request_payload(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_msgpack: Vec<u8>,
) -> PyResult<Py<PyAny>> {
    let request: DeltaCheckpointRequestPayload =
        parse_msgpack_payload(&request_msgpack, "delta_cleanup_metadata_request")?;
    let table_version = table_version_from_options(request.version, request.timestamp)?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_cleanup_metadata_native(
            &extract_session_ctx(ctx)?,
            &request.table_uri,
            request.storage_options,
            table_version,
            request.gate,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta metadata cleanup failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(
        delta_optimize_compact_request_payload,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(delta_vacuum_request_payload, module)?)?;
    module.add_function(wrap_pyfunction!(delta_restore_request_payload, module)?)?;
    module.add_function(wrap_pyfunction!(
        delta_set_properties_request_payload,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        delta_add_features_request_payload,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        delta_add_constraints_request_payload,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        delta_drop_constraints_request_payload,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        delta_create_checkpoint_request_payload,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        delta_cleanup_metadata_request_payload,
        module
    )?)?;
    Ok(())
}
