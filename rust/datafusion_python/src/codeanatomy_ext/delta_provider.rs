//! Delta provider and scan-config bridge surface.

use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tracing::instrument;

use crate::delta_control_plane::{
    delta_add_actions as delta_add_actions_native, delta_cdf_provider as delta_cdf_provider_native,
    delta_provider_from_session_request as delta_provider_from_session_native,
    delta_provider_with_files_request as delta_provider_with_files_native,
    scan_config_from_session as delta_scan_config_from_session_native,
    snapshot_info_with_gate as delta_snapshot_info_native, DeltaAddActionsRequest,
    DeltaCdfProviderRequest, DeltaCdfScanOptions, DeltaProviderFromSessionRequest,
    DeltaProviderWithFilesRequest,
};
use crate::delta_mutations::{
    delta_data_check_request as delta_data_check_native, DeltaDataCheckRequest,
};

use super::helpers::{
    delta_gate_from_params, extract_session_ctx, json_to_py, provider_capsule, runtime,
    scan_config_to_pydict, scan_overrides_from_params, snapshot_to_pydict, storage_options_map,
    table_version_from_options,
};

#[pyclass]
#[derive(Clone)]
pub(crate) struct DeltaCdfOptions {
    #[pyo3(get, set)]
    pub(crate) starting_version: Option<i64>,
    #[pyo3(get, set)]
    pub(crate) ending_version: Option<i64>,
    #[pyo3(get, set)]
    pub(crate) starting_timestamp: Option<String>,
    #[pyo3(get, set)]
    pub(crate) ending_timestamp: Option<String>,
    #[pyo3(get, set)]
    pub(crate) allow_out_of_range: bool,
}

#[pymethods]
impl DeltaCdfOptions {
    #[new]
    fn new() -> Self {
        Self {
            starting_version: None,
            ending_version: None,
            starting_timestamp: None,
            ending_timestamp: None,
            allow_out_of_range: false,
        }
    }
}

#[pyfunction]
#[instrument(skip(py, table_uri, storage_options, options))]
pub(crate) fn delta_cdf_table_provider(
    py: Python<'_>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    options: DeltaCdfOptions,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let cdf_options = DeltaCdfScanOptions {
        starting_version: options.starting_version,
        ending_version: options.ending_version,
        starting_timestamp: options.starting_timestamp,
        ending_timestamp: options.ending_timestamp,
        allow_out_of_range: options.allow_out_of_range,
    };
    let runtime = runtime()?;
    let table_version = table_version_from_options(version, timestamp)?;
    let (provider, snapshot) = runtime
        .block_on(delta_cdf_provider_native(DeltaCdfProviderRequest {
            table_uri: &table_uri,
            storage_options: storage,
            table_version,
            options: cdf_options,
            gate,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta CDF provider failed: {err}")))?;
    let payload = PyDict::new(py);
    payload.set_item("provider", provider_capsule(py, Arc::new(provider))?)?;
    payload.set_item("snapshot", snapshot_to_pydict(py, &snapshot)?)?;
    Ok(payload.into())
}

#[pyfunction]
#[instrument(skip(py, ctx, table_uri, storage_options, predicate, schema_ipc))]
pub(crate) fn delta_table_provider_from_session(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    predicate: Option<String>,
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,
    wrap_partition_values: Option<bool>,
    schema_ipc: Option<Vec<u8>>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let overrides = scan_overrides_from_params(
        file_column_name,
        enable_parquet_pushdown,
        schema_force_view_types,
        wrap_partition_values,
        schema_ipc,
    )?;
    let runtime = runtime()?;
    let table_version = table_version_from_options(version, timestamp)?;
    let (provider, snapshot, scan_config, add_actions, predicate_error) = runtime
        .block_on(delta_provider_from_session_native(
            DeltaProviderFromSessionRequest {
                session_ctx: &extract_session_ctx(ctx)?,
                table_uri: &table_uri,
                storage_options: storage,
                table_version,
                predicate,
                overrides,
                gate,
            },
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to build Delta provider: {err}")))?;
    let payload = PyDict::new(py);
    payload.set_item("provider", provider_capsule(py, provider)?)?;
    payload.set_item("snapshot", snapshot_to_pydict(py, &snapshot)?)?;
    payload.set_item("scan_config", scan_config_to_pydict(py, &scan_config)?)?;
    if let Some(add_actions) = add_actions {
        let add_payload = crate::delta_observability::add_action_payloads(&add_actions);
        payload.set_item("add_actions", json_to_py(py, &add_payload)?)?;
    }
    if let Some(error) = predicate_error {
        payload.set_item("predicate_error", error)?;
    }
    Ok(payload.into())
}

#[pyfunction]
#[instrument(skip(py, ctx, table_uri, storage_options, schema_ipc))]
pub(crate) fn delta_table_provider_with_files(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    files: Vec<String>,
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,
    wrap_partition_values: Option<bool>,
    schema_ipc: Option<Vec<u8>>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let overrides = scan_overrides_from_params(
        file_column_name,
        enable_parquet_pushdown,
        schema_force_view_types,
        wrap_partition_values,
        schema_ipc,
    )?;
    let runtime = runtime()?;
    let table_version = table_version_from_options(version, timestamp)?;
    let (provider, snapshot, scan_config, add_actions) = runtime
        .block_on(delta_provider_with_files_native(
            DeltaProviderWithFilesRequest {
                session_ctx: &extract_session_ctx(ctx)?,
                table_uri: &table_uri,
                storage_options: storage,
                table_version,
                overrides,
                files,
                gate,
            },
        ))
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to build Delta provider with file pruning: {err}"
            ))
        })?;
    let payload = PyDict::new(py);
    payload.set_item("provider", provider_capsule(py, provider)?)?;
    payload.set_item("snapshot", snapshot_to_pydict(py, &snapshot)?)?;
    payload.set_item("scan_config", scan_config_to_pydict(py, &scan_config)?)?;
    let add_payload = crate::delta_observability::add_action_payloads(&add_actions);
    payload.set_item("add_actions", json_to_py(py, &add_payload)?)?;
    Ok(payload.into())
}

#[pyfunction]
#[instrument(skip(py, ctx, schema_ipc))]
pub(crate) fn delta_scan_config_from_session(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,
    wrap_partition_values: Option<bool>,
    schema_ipc: Option<Vec<u8>>,
) -> PyResult<Py<PyAny>> {
    let overrides = scan_overrides_from_params(
        file_column_name,
        enable_parquet_pushdown,
        schema_force_view_types,
        wrap_partition_values,
        schema_ipc,
    )?;
    let session_state = extract_session_ctx(ctx)?.state();
    let scan_config = delta_scan_config_from_session_native(&session_state, None, overrides)
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to resolve Delta scan config from session: {err}"
            ))
        })?;
    scan_config_to_pydict(py, &scan_config)
}

#[pyfunction]
#[instrument(skip(py, table_uri, storage_options))]
pub(crate) fn delta_snapshot_info(
    py: Python<'_>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let runtime = runtime()?;
    let table_version = table_version_from_options(version, timestamp)?;
    let snapshot = runtime
        .block_on(delta_snapshot_info_native(
            &table_uri,
            storage,
            table_version,
            gate,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to load Delta snapshot: {err}")))?;
    snapshot_to_pydict(py, &snapshot)
}

#[pyfunction]
#[instrument(skip(snapshot_msgpack, gate_msgpack))]
pub(crate) fn validate_protocol_gate(
    snapshot_msgpack: Vec<u8>,
    gate_msgpack: Vec<u8>,
) -> PyResult<()> {
    crate::delta_protocol::validate_protocol_gate_payload(&snapshot_msgpack, &gate_msgpack).map_err(
        |err| PyRuntimeError::new_err(format!("Delta protocol gate validation failed: {err}")),
    )
}

#[pyfunction]
#[instrument(skip(py, table_uri, storage_options))]
pub(crate) fn delta_add_actions(
    py: Python<'_>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let runtime = runtime()?;
    let table_version = table_version_from_options(version, timestamp)?;
    let (snapshot, add_actions) = runtime
        .block_on(delta_add_actions_native(DeltaAddActionsRequest {
            table_uri: &table_uri,
            storage_options: storage,
            table_version,
            gate,
        }))
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to list Delta add actions: {err}"))
        })?;
    let payload = PyDict::new(py);
    payload.set_item("snapshot", snapshot_to_pydict(py, &snapshot)?)?;
    let add_payload = crate::delta_observability::add_action_payloads(&add_actions);
    payload.set_item("add_actions", json_to_py(py, &add_payload)?)?;
    Ok(payload.into())
}

#[pyfunction]
#[instrument(skip(ctx, table_uri, storage_options, data_ipc, extra_constraints))]
pub(crate) fn delta_data_checker(
    ctx: &Bound<'_, PyAny>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    data_ipc: Vec<u8>,
    extra_constraints: Option<Vec<String>>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
) -> PyResult<Vec<String>> {
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let runtime = runtime()?;
    let table_version = table_version_from_options(version, timestamp)?;
    runtime
        .block_on(delta_data_check_native(DeltaDataCheckRequest {
            session_ctx: &extract_session_ctx(ctx)?,
            table_uri: &table_uri,
            storage_options: storage,
            table_version,
            gate,
            data_ipc: data_ipc.as_slice(),
            extra_constraints,
        }))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta data check failed: {err}")))
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(delta_cdf_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(delta_table_provider_from_session, module)?)?;
    module.add_function(wrap_pyfunction!(delta_table_provider_with_files, module)?)?;
    module.add_function(wrap_pyfunction!(delta_scan_config_from_session, module)?)?;
    module.add_function(wrap_pyfunction!(delta_snapshot_info, module)?)?;
    module.add_function(wrap_pyfunction!(validate_protocol_gate, module)?)?;
    module.add_function(wrap_pyfunction!(delta_add_actions, module)?)?;
    module.add_function(wrap_pyfunction!(delta_data_checker, module)?)?;
    Ok(())
}

pub(crate) fn register_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<DeltaCdfOptions>()?;
    Ok(())
}
