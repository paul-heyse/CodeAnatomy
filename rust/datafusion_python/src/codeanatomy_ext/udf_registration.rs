//! UDF registration and runtime capability bridge surface.

use std::sync::Arc;

use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use datafusion_ext::udf_config::CodeAnatomyUdfConfig;
use df_plugin_host::{DF_PLUGIN_ABI_MAJOR, DF_PLUGIN_ABI_MINOR};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use rmp_serde::to_vec_named;
use tracing::instrument;

use crate::registry_snapshot;

use super::helpers::{extract_session_ctx, json_to_py};

pub(crate) const RUNTIME_INSTALL_CONTRACT_VERSION: u32 = 4;

fn registry_snapshot_hash(snapshot: &registry_snapshot::RegistrySnapshot) -> PyResult<String> {
    let mut hasher = Blake2bVar::new(16).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to initialize registry hash: {err}"))
    })?;
    for name in snapshot
        .scalar()
        .iter()
        .chain(snapshot.aggregate().iter())
        .chain(snapshot.window().iter())
        .chain(snapshot.table().iter())
    {
        hasher.update(name.as_bytes());
        hasher.update(&[0]);
    }
    let mut out = vec![0_u8; 16];
    hasher.finalize_variable(&mut out).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to finalize registry hash: {err}"))
    })?;
    Ok(hex::encode(out))
}

fn registry_snapshot_msgpack(snapshot: &registry_snapshot::RegistrySnapshot) -> PyResult<Vec<u8>> {
    to_vec_named(snapshot).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to serialize registry snapshot: {err}"))
    })
}

#[pyfunction]
pub(crate) fn install_codeanatomy_udf_config(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    let state_ref = extract_session_ctx(ctx)?.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    if config.get_extension::<CodeAnatomyUdfConfig>().is_none() {
        config.set_extension(Arc::new(CodeAnatomyUdfConfig::default()));
    }
    Ok(())
}

#[pyfunction]
pub(crate) fn install_function_factory(
    ctx: &Bound<'_, PyAny>,
    policy_ipc: &Bound<'_, pyo3::types::PyBytes>,
) -> PyResult<()> {
    datafusion_ext::udf::install_function_factory_native(
        &extract_session_ctx(ctx)?,
        policy_ipc.as_bytes(),
    )
    .map_err(|err| PyRuntimeError::new_err(format!("FunctionFactory install failed: {err}")))
}

#[pyfunction(name = "registry_snapshot")]
pub(crate) fn registry_snapshot_py(py: Python<'_>, ctx: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let snapshot = registry_snapshot::registry_snapshot(&extract_session_ctx(ctx)?.state());
    let payload = PyDict::new(py);
    payload.set_item("scalar", PyList::new(py, snapshot.scalar())?)?;
    payload.set_item("aggregate", PyList::new(py, snapshot.aggregate())?)?;
    payload.set_item("window", PyList::new(py, snapshot.window())?)?;
    payload.set_item("table", PyList::new(py, snapshot.table())?)?;
    let alias_payload = PyDict::new(py);
    for (name, aliases) in snapshot.aliases() {
        alias_payload.set_item(name, PyList::new(py, aliases)?)?;
    }
    payload.set_item("aliases", alias_payload)?;
    let param_payload = PyDict::new(py);
    for (name, params) in snapshot.parameter_names() {
        param_payload.set_item(name, PyList::new(py, params)?)?;
    }
    payload.set_item("parameter_names", param_payload)?;
    let volatility_payload = PyDict::new(py);
    for (name, volatility) in snapshot.volatility() {
        volatility_payload.set_item(name, volatility)?;
    }
    payload.set_item("volatility", volatility_payload)?;
    let rewrite_payload = PyDict::new(py);
    for (name, tags) in snapshot.rewrite_tags() {
        rewrite_payload.set_item(name, PyList::new(py, tags)?)?;
    }
    payload.set_item("rewrite_tags", rewrite_payload)?;
    let simplify_payload = PyDict::new(py);
    for (name, enabled) in snapshot.simplify() {
        simplify_payload.set_item(name, enabled)?;
    }
    payload.set_item("simplify", simplify_payload)?;
    let coerce_payload = PyDict::new(py);
    for (name, enabled) in snapshot.coerce_types() {
        coerce_payload.set_item(name, enabled)?;
    }
    payload.set_item("coerce_types", coerce_payload)?;
    let short_payload = PyDict::new(py);
    for (name, enabled) in snapshot.short_circuits() {
        short_payload.set_item(name, enabled)?;
    }
    payload.set_item("short_circuits", short_payload)?;
    let signature_payload = PyDict::new(py);
    for (name, signatures) in snapshot.signature_inputs() {
        let mut rows: Vec<Py<PyAny>> = Vec::with_capacity(signatures.len());
        for row in signatures {
            rows.push(PyList::new(py, row.as_slice())?.into());
        }
        signature_payload.set_item(name, PyList::new(py, rows)?)?;
    }
    payload.set_item("signature_inputs", signature_payload)?;
    let return_payload = PyDict::new(py);
    for (name, return_types) in snapshot.return_types() {
        return_payload.set_item(name, PyList::new(py, return_types)?)?;
    }
    payload.set_item("return_types", return_payload)?;
    let config_payload = PyDict::new(py);
    for (name, defaults) in snapshot.config_defaults() {
        let entry = PyDict::new(py);
        for (key, value) in defaults {
            match value {
                datafusion_ext::udf_config::UdfConfigValue::Bool(flag) => {
                    entry.set_item(key, flag)?;
                }
                datafusion_ext::udf_config::UdfConfigValue::Int(value) => {
                    entry.set_item(key, value)?;
                }
                datafusion_ext::udf_config::UdfConfigValue::String(text) => {
                    entry.set_item(key, text)?;
                }
            }
        }
        config_payload.set_item(name, entry)?;
    }
    payload.set_item("config_defaults", config_payload)?;
    payload.set_item("custom_udfs", PyList::new(py, snapshot.custom_udfs())?)?;
    payload.set_item("pycapsule_udfs", PyList::empty(py))?;
    Ok(payload.into())
}

#[pyfunction(name = "registry_snapshot_msgpack")]
pub(crate) fn registry_snapshot_msgpack_py(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let snapshot = registry_snapshot::registry_snapshot(&extract_session_ctx(ctx)?.state());
    let payload = registry_snapshot_msgpack(&snapshot)?;
    Ok(PyBytes::new(py, &payload).into())
}

#[pyfunction]
#[instrument(level = "info", skip_all, fields(enable_async))]
#[pyo3(signature = (
    ctx,
    enable_async = false,
    async_udf_timeout_ms = None,
    async_udf_batch_size = None
))]
pub(crate) fn register_codeanatomy_udfs(
    ctx: &Bound<'_, PyAny>,
    enable_async: bool,
    async_udf_timeout_ms: Option<u64>,
    async_udf_batch_size: Option<usize>,
) -> PyResult<()> {
    datafusion_ext::udf_registry::register_all_with_policy(
        &extract_session_ctx(ctx)?,
        enable_async,
        async_udf_timeout_ms,
        async_udf_batch_size,
    )
    .map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to register CodeAnatomy UDFs: {err}"))
    })?;
    Ok(())
}

#[pyfunction]
#[instrument(level = "info", skip_all)]
pub(crate) fn capabilities_snapshot(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let ctx = datafusion::execution::context::SessionContext::new();
    datafusion_ext::udf_registry::register_all(&ctx).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build registry snapshot: {err}"))
    })?;
    let snapshot = registry_snapshot::registry_snapshot(&ctx.state());
    let hash = registry_snapshot_hash(&snapshot)?;
    let payload = serde_json::json!({
        "datafusion_version": datafusion::DATAFUSION_VERSION,
        "arrow_version": arrow::ARROW_VERSION,
        "plugin_abi": {
            "major": DF_PLUGIN_ABI_MAJOR,
            "minor": DF_PLUGIN_ABI_MINOR,
        },
        "runtime_install_contract": {
            "version": RUNTIME_INSTALL_CONTRACT_VERSION,
            "supports_unified_entrypoint": true,
            "supports_modular_entrypoints": true,
            "required_modular_entrypoints": [
                "register_codeanatomy_udfs",
                "install_function_factory",
                "install_expr_planners",
                "install_relation_planner",
                "install_type_planner",
                "registry_snapshot",
                "registry_snapshot_msgpack",
            ],
        },
        "function_factory": {
            "available": true,
        },
        "expr_planners": {
            "available": true,
        },
        "cache_registrar": {
            "available": true,
            "entrypoint": "register_cache_tables",
        },
        "delta_control_plane": {
            "available": true,
            "entrypoints": [
                "delta_table_provider_from_session",
                "delta_table_provider_with_files",
                "delta_scan_config_from_session",
                "delta_write_ipc_request",
                "delta_delete_request",
                "delta_update_request",
                "delta_merge_request",
                "delta_vacuum_request_payload",
            ],
        },
        "substrait": {
            "available": cfg!(feature = "substrait"),
            "entrypoints": [
                "replay_substrait_plan",
                "lineage_from_substrait",
                "extract_lineage_json",
            ],
        },
        "extraction_session": {
            "available": true,
            "entrypoint": "build_extraction_session",
        },
        "dataset_provider_registration": {
            "available": true,
            "entrypoint": "register_dataset_provider",
        },
        "async_udf": {
            "available": cfg!(feature = "async-udf"),
        },
        "udf_registry": {
            "scalar": snapshot.scalar().len(),
            "aggregate": snapshot.aggregate().len(),
            "window": snapshot.window().len(),
            "table": snapshot.table().len(),
            "custom": snapshot.custom_udfs().len(),
            "hash": hash,
        },
    });
    json_to_py(py, &payload)
}

#[pyfunction]
#[instrument(level = "info", skip_all, fields(enable_async_udfs))]
#[pyo3(signature = (
    ctx,
    enable_async_udfs = false,
    async_udf_timeout_ms = None,
    async_udf_batch_size = None
))]
pub(crate) fn install_codeanatomy_runtime(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    enable_async_udfs: bool,
    async_udf_timeout_ms: Option<u64>,
    async_udf_batch_size: Option<usize>,
) -> PyResult<Py<PyAny>> {
    datafusion_ext::udf_registry::register_all_with_policy(
        &extract_session_ctx(ctx)?,
        enable_async_udfs,
        async_udf_timeout_ms,
        async_udf_batch_size,
    )
    .map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to install CodeAnatomy runtime UDFs: {err}"))
    })?;
    datafusion_ext::install_sql_macro_factory_native(&extract_session_ctx(ctx)?).map_err(
        |err| {
            PyRuntimeError::new_err(format!(
                "Failed to install CodeAnatomy FunctionFactory runtime: {err}"
            ))
        },
    )?;
    let planner_names = ["codeanatomy_domain"];
    datafusion_ext::install_expr_planners_native(&extract_session_ctx(ctx)?, &planner_names)
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to install CodeAnatomy ExprPlanner runtime: {err}"
            ))
        })?;
    datafusion_ext::install_relation_planner_native(&extract_session_ctx(ctx)?).map_err(|err| {
        PyRuntimeError::new_err(format!(
            "Failed to install CodeAnatomy RelationPlanner runtime: {err}"
        ))
    })?;
    datafusion_ext::install_type_planner_native(&extract_session_ctx(ctx)?).map_err(|err| {
        PyRuntimeError::new_err(format!(
            "Failed to install CodeAnatomy TypePlanner runtime: {err}"
        ))
    })?;

    let snapshot = registry_snapshot::registry_snapshot(&extract_session_ctx(ctx)?.state());
    let snapshot_json = serde_json::to_value(&snapshot).map_err(|err| {
        PyRuntimeError::new_err(format!(
            "Failed to serialize runtime snapshot payload: {err}"
        ))
    })?;
    let snapshot_msgpack = registry_snapshot_msgpack(&snapshot)?;
    let payload = PyDict::new(py);
    payload.set_item("contract_version", RUNTIME_INSTALL_CONTRACT_VERSION)?;
    payload.set_item("runtime_install_mode", "unified")?;
    payload.set_item("snapshot", json_to_py(py, &snapshot_json)?)?;
    payload.set_item("snapshot_msgpack", PyBytes::new(py, &snapshot_msgpack))?;
    payload.set_item("udf_installed", true)?;
    payload.set_item("function_factory_installed", true)?;
    payload.set_item("expr_planners_installed", true)?;
    payload.set_item("relation_planner_installed", true)?;
    payload.set_item("type_planner_installed", true)?;
    payload.set_item("expr_planner_names", planner_names)?;
    payload.set_item("cache_registrar_available", true)?;
    payload.set_item(
        "async",
        json_to_py(
            py,
            &serde_json::json!({
                "enabled": enable_async_udfs,
                "timeout_ms": async_udf_timeout_ms,
                "batch_size": async_udf_batch_size,
            }),
        )?,
    )?;
    Ok(payload.into())
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(install_function_factory, module)?)?;
    module.add_function(wrap_pyfunction!(install_codeanatomy_udf_config, module)?)?;
    module.add_function(wrap_pyfunction!(register_codeanatomy_udfs, module)?)?;
    module.add_function(wrap_pyfunction!(registry_snapshot_py, module)?)?;
    module.add_function(wrap_pyfunction!(registry_snapshot_msgpack_py, module)?)?;
    module.add_function(wrap_pyfunction!(capabilities_snapshot, module)?)?;
    module.add_function(wrap_pyfunction!(install_codeanatomy_runtime, module)?)?;
    Ok(())
}
