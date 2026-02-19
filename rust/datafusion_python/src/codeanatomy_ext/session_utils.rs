//! Session/runtime utility bridge surface.

use std::collections::HashMap;

use codeanatomy_engine::session::extraction::{
    build_extraction_session as build_extraction_session_native, ExtractionConfig,
};
use datafusion::optimizer::OptimizerConfig;
use datafusion_expr::lit;
use datafusion_expr::Expr;
use datafusion_ext::async_runtime;
use datafusion_ext::udf_expr as udf_expr_mod;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyTuple};
use serde::Deserialize;
use tracing::instrument;

use crate::context::PySessionContext;
use crate::delta_control_plane::{
    delta_provider_from_session_request as delta_provider_from_session_native,
    DeltaProviderFromSessionRequest,
};
use crate::delta_observability::add_action_payloads;
use crate::expr::PyExpr;
use crate::utils::py_obj_to_scalar_value;

use super::helpers::{
    delta_gate_from_params, extract_session_ctx, json_to_py, parse_python_payload,
    scan_config_to_pydict, scan_overrides_from_params, snapshot_to_pydict,
    table_version_from_options,
};

#[derive(Debug, Clone, Deserialize)]
struct ExtractionSessionPayload {
    parallelism: Option<usize>,
    memory_limit_bytes: Option<u64>,
    batch_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct DatasetProviderRequestPayload {
    table_name: String,
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    predicate: Option<String>,
    overwrite: Option<bool>,
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,
    wrap_partition_values: Option<bool>,
    schema_ipc: Option<Vec<u8>>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
}

#[pyfunction]
#[instrument(level = "info", skip_all)]
pub(crate) fn build_extraction_session(
    py: Python<'_>,
    config_payload: &Bound<'_, PyAny>,
) -> PyResult<PySessionContext> {
    let payload: ExtractionSessionPayload =
        parse_python_payload(py, config_payload, "extraction session config")?;
    let mut config = ExtractionConfig::default();
    if let Some(parallelism) = payload.parallelism {
        if parallelism == 0 {
            return Err(PyValueError::new_err(
                "extraction session config parallelism must be >= 1",
            ));
        }
        config.parallelism = parallelism;
    }
    if let Some(batch_size) = payload.batch_size {
        if batch_size == 0 {
            return Err(PyValueError::new_err(
                "extraction session config batch_size must be >= 1",
            ));
        }
        config.batch_size = batch_size;
    }
    if let Some(memory_limit_bytes) = payload.memory_limit_bytes {
        config.memory_limit_bytes = Some(usize::try_from(memory_limit_bytes).map_err(|_| {
            PyValueError::new_err(format!(
                "extraction session config memory_limit_bytes exceeds usize: {memory_limit_bytes}"
            ))
        })?);
    }
    let ctx = build_extraction_session_native(&config).map_err(|err| {
        PyRuntimeError::new_err(format!("Extraction session build failed: {err}"))
    })?;
    Ok(PySessionContext { ctx })
}

#[pyfunction]
#[instrument(
    level = "info",
    skip(py, ctx, request_payload),
    fields(table_name, table_uri)
)]
pub(crate) fn register_dataset_provider(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_payload: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let request: DatasetProviderRequestPayload =
        parse_python_payload(py, request_payload, "dataset provider registration")?;
    tracing::Span::current().record(
        "table_name",
        tracing::field::display(request.table_name.as_str()),
    );
    tracing::Span::current().record(
        "table_uri",
        tracing::field::display(request.table_uri.as_str()),
    );
    let gate = delta_gate_from_params(
        request.min_reader_version,
        request.min_writer_version,
        request.required_reader_features,
        request.required_writer_features,
    );
    let overrides = scan_overrides_from_params(
        request.file_column_name,
        request.enable_parquet_pushdown,
        request.schema_force_view_types,
        request.wrap_partition_values,
        request.schema_ipc,
    )?;
    let runtime = async_runtime::shared_runtime().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to acquire shared Tokio runtime: {err}"))
    })?;
    let table_version = table_version_from_options(request.version, request.timestamp.clone())?;
    let session_ctx = extract_session_ctx(ctx)?;
    let (provider, snapshot, scan_config, add_actions, predicate_error) = runtime
        .block_on(delta_provider_from_session_native(
            DeltaProviderFromSessionRequest {
                session_ctx: &session_ctx,
                table_uri: &request.table_uri,
                storage_options: request.storage_options,
                table_version,
                predicate: request.predicate.clone(),
                overrides,
                gate,
            },
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Dataset provider build failed: {err}")))?;

    let table_name = request.table_name;
    if request.overwrite.unwrap_or(true) {
        let _ = session_ctx.deregister_table(table_name.as_str());
    }
    session_ctx
        .register_table(table_name.as_str(), provider)
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to register dataset provider for table {table_name:?}: {err}"
            ))
        })?;

    let payload = PyDict::new(py);
    payload.set_item("table_name", table_name)?;
    payload.set_item("registered", true)?;
    payload.set_item("snapshot", snapshot_to_pydict(py, &snapshot)?)?;
    payload.set_item("scan_config", scan_config_to_pydict(py, &scan_config)?)?;
    if let Some(add_actions) = add_actions {
        let add_payload = add_action_payloads(&add_actions);
        payload.set_item("add_actions", json_to_py(py, &add_payload)?)?;
    }
    if let Some(error) = predicate_error {
        payload.set_item("predicate_error", error)?;
    }
    Ok(payload.into())
}

#[pyfunction]
#[pyo3(signature = (name, *args, ctx=None))]
pub(crate) fn udf_expr(
    py: Python<'_>,
    name: String,
    args: &Bound<'_, PyTuple>,
    ctx: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyExpr> {
    let mut expr_args: Vec<Expr> = Vec::with_capacity(args.len());
    for item in args.iter() {
        if let Ok(expr) = item.extract::<PyExpr>() {
            expr_args.push(expr.into());
            continue;
        }
        let scalar = py_obj_to_scalar_value(py, item.unbind())?;
        expr_args.push(lit(scalar));
    }
    let session_ctx = ctx.map(extract_session_ctx).transpose()?;
    let expr = if let Some(session_ctx) = session_ctx {
        let state = session_ctx.state();
        let config_options = state.config_options();
        if let Some(registry) = state.function_registry() {
            udf_expr_mod::expr_from_registry_or_specs(
                registry,
                name.as_str(),
                expr_args,
                Some(config_options),
            )
        } else {
            udf_expr_mod::expr_from_name(name.as_str(), expr_args, Some(config_options))
        }
    } else {
        udf_expr_mod::expr_from_name(name.as_str(), expr_args, None)
    }
    .map_err(|err| PyValueError::new_err(format!("UDF expression failed: {err}")))?;
    Ok(expr.into())
}

#[pyfunction]
pub(crate) fn table_logical_plan(ctx: &Bound<'_, PyAny>, table_name: String) -> PyResult<String> {
    let runtime = async_runtime::shared_runtime().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to acquire shared Tokio runtime: {err}"))
    })?;
    let df = runtime
        .block_on(extract_session_ctx(ctx)?.table(table_name.as_str()))
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to resolve table {table_name:?}: {err}"))
        })?;
    Ok(format!("{:?}", df.logical_plan()))
}

#[pyfunction]
pub(crate) fn table_dfschema_tree(ctx: &Bound<'_, PyAny>, table_name: String) -> PyResult<String> {
    let runtime = async_runtime::shared_runtime().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to acquire shared Tokio runtime: {err}"))
    })?;
    let df = runtime
        .block_on(extract_session_ctx(ctx)?.table(table_name.as_str()))
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to resolve table {table_name:?}: {err}"))
        })?;
    Ok(df.schema().to_string())
}

#[pyfunction]
pub(crate) fn session_context_contract_probe(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let metrics_payload =
        super::cache_tables::runtime_execution_metrics_payload(&extract_session_ctx(ctx)?);
    let runtime_summary = metrics_payload
        .get("runtime")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let mut payload_obj = serde_json::Map::new();
    payload_obj.insert("ok".to_string(), serde_json::Value::Bool(true));
    payload_obj.insert("runtime".to_string(), runtime_summary.clone());
    if let Some(schema_version) = metrics_payload.get("schema_version") {
        payload_obj.insert("schema_version".to_string(), schema_version.clone());
    }
    if let Some(rows) = metrics_payload.get("rows") {
        payload_obj.insert("rows".to_string(), rows.clone());
    }
    if let Some(runtime_obj) = runtime_summary.as_object() {
        for (key, value) in runtime_obj {
            payload_obj.insert(key.clone(), value.clone());
        }
    }
    let payload = serde_json::Value::Object(payload_obj);
    json_to_py(py, &payload)
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(build_extraction_session, module)?)?;
    module.add_function(wrap_pyfunction!(register_dataset_provider, module)?)?;
    module.add_function(wrap_pyfunction!(session_context_contract_probe, module)?)?;
    module.add_function(wrap_pyfunction!(udf_expr, module)?)?;
    module.add_function(wrap_pyfunction!(table_logical_plan, module)?)?;
    module.add_function(wrap_pyfunction!(table_dfschema_tree, module)?)?;
    Ok(())
}

pub(crate) fn register_internal_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(build_extraction_session, module)?)?;
    module.add_function(wrap_pyfunction!(register_dataset_provider, module)?)?;
    module.add_function(wrap_pyfunction!(session_context_contract_probe, module)?)?;
    module.add_function(wrap_pyfunction!(udf_expr, module)?)?;
    Ok(())
}

pub(crate) fn register_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = module;
    Ok(())
}
