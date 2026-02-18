//! UDF registration and config bridge surface.

use std::sync::Arc;

use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use datafusion_ext::physical_rules::{
    ensure_physical_config, install_physical_rules as install_physical_rules_native,
};
use datafusion_ext::planner_rules::{
    ensure_policy_config, install_policy_rules as install_policy_rules_native,
};
use datafusion_ext::udf_config::{CodeAnatomyUdfConfig, UdfConfigValue};
use df_plugin_host::{DF_PLUGIN_ABI_MAJOR, DF_PLUGIN_ABI_MINOR};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList};
use rmp_serde::to_vec_named;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

use crate::{registry_snapshot, udf_docs};

use super::helpers::{extract_session_ctx, json_to_py};

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
    to_vec_named(snapshot)
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to serialize registry snapshot: {err}")))
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

fn json_string_list(value: Option<&JsonValue>) -> Vec<String> {
    let Some(JsonValue::Array(values)) = value else {
        return Vec::new();
    };
    values
        .iter()
        .filter_map(JsonValue::as_str)
        .map(ToString::to_string)
        .collect()
}

fn json_object(value: Option<&JsonValue>) -> Option<&JsonMap<String, JsonValue>> {
    match value {
        Some(JsonValue::Object(entries)) => Some(entries),
        _ => None,
    }
}

fn dtype_for_param(
    signature_inputs: Option<&JsonMap<String, JsonValue>>,
    name: &str,
    index: usize,
) -> String {
    let Some(entries) = signature_inputs else {
        return "Utf8".to_string();
    };
    let Some(JsonValue::Array(signatures)) = entries.get(name) else {
        return "Utf8".to_string();
    };
    let Some(JsonValue::Array(first_signature)) = signatures.first() else {
        return "Utf8".to_string();
    };
    first_signature
        .get(index)
        .and_then(JsonValue::as_str)
        .map_or_else(|| "Utf8".to_string(), ToString::to_string)
}

#[pyfunction]
pub(crate) fn derive_function_factory_policy(
    py: Python<'_>,
    snapshot: &Bound<'_, PyAny>,
    allow_async: bool,
) -> PyResult<Py<PyAny>> {
    let json_module = py.import("json")?;
    let dumped = json_module.call_method1("dumps", (snapshot,))?;
    let snapshot_json: String = dumped.extract()?;
    let parsed: JsonValue = serde_json::from_str(&snapshot_json).map_err(|err| {
        PyValueError::new_err(format!(
            "Invalid snapshot payload for FunctionFactory policy: {err}"
        ))
    })?;
    let snapshot_obj = parsed.as_object().ok_or_else(|| {
        PyValueError::new_err("FunctionFactory policy derivation requires a mapping snapshot.")
    })?;

    let scalar_names = json_string_list(snapshot_obj.get("scalar"));
    let parameter_names = json_object(snapshot_obj.get("parameter_names"));
    let signature_inputs = json_object(snapshot_obj.get("signature_inputs"));
    let return_types = json_object(snapshot_obj.get("return_types"));
    let volatility = json_object(snapshot_obj.get("volatility"));

    let primitives = scalar_names
        .iter()
        .map(|name| {
            let params = json_string_list(parameter_names.and_then(|entries| entries.get(name)));
            let params_payload = params
                .iter()
                .enumerate()
                .map(|(index, param_name)| {
                    json!({
                        "name": param_name,
                        "dtype": dtype_for_param(signature_inputs, name, index),
                    })
                })
                .collect::<Vec<_>>();
            let return_type = return_types
                .and_then(|entries| entries.get(name))
                .and_then(|value| value.as_array())
                .and_then(|rows| rows.first())
                .and_then(JsonValue::as_str)
                .map_or_else(|| "Utf8".to_string(), ToString::to_string);
            let volatility_value = volatility
                .and_then(|entries| entries.get(name))
                .and_then(JsonValue::as_str)
                .map_or_else(|| "stable".to_string(), ToString::to_string);
            json!({
                "name": name,
                "params": params_payload,
                "return_type": return_type,
                "volatility": volatility_value,
                "description": JsonValue::Null,
                "supports_named_args": !params.is_empty(),
            })
        })
        .collect::<Vec<_>>();

    let domain_operator_hooks = json_string_list(snapshot_obj.get("domain_operator_hooks"));
    let payload = json!({
        "primitives": primitives,
        "prefer_named_arguments": parameter_names.map_or(false, |entries| !entries.is_empty()),
        "allow_async": allow_async,
        "domain_operator_hooks": domain_operator_hooks,
    });
    json_to_py(py, &payload)
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
                UdfConfigValue::Bool(flag) => {
                    entry.set_item(key, flag)?;
                }
                UdfConfigValue::Int(value) => {
                    entry.set_item(key, value)?;
                }
                UdfConfigValue::String(text) => {
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
pub(crate) fn capabilities_snapshot(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let ctx = datafusion::execution::context::SessionContext::new();
    datafusion_ext::udf_registry::register_all(&ctx).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build registry snapshot: {err}"))
    })?;
    let snapshot = registry_snapshot::registry_snapshot(&ctx.state());
    let hash = registry_snapshot_hash(&snapshot)?;
    let payload = json!({
        "datafusion_version": datafusion::DATAFUSION_VERSION,
        "arrow_version": arrow::ARROW_VERSION,
        "plugin_abi": {
            "major": DF_PLUGIN_ABI_MAJOR,
            "minor": DF_PLUGIN_ABI_MINOR,
        },
        "runtime_install_contract": {
            "version": 3,
            "supports_unified_entrypoint": true,
            "supports_modular_entrypoints": true,
            "required_modular_entrypoints": [
                "register_codeanatomy_udfs",
                "install_function_factory",
                "install_expr_planners",
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
    let snapshot = registry_snapshot::registry_snapshot(&extract_session_ctx(ctx)?.state());
    let snapshot_json = serde_json::to_value(&snapshot).map_err(|err| {
        PyRuntimeError::new_err(format!(
            "Failed to serialize runtime snapshot payload: {err}"
        ))
    })?;
    let snapshot_msgpack = registry_snapshot_msgpack(&snapshot)?;
    let payload = PyDict::new(py);
    payload.set_item("contract_version", 3)?;
    payload.set_item("runtime_install_mode", "unified")?;
    payload.set_item("snapshot", json_to_py(py, &snapshot_json)?)?;
    payload.set_item("snapshot_msgpack", PyBytes::new(py, &snapshot_msgpack))?;
    payload.set_item("udf_installed", true)?;
    payload.set_item("function_factory_installed", true)?;
    payload.set_item("expr_planners_installed", true)?;
    payload.set_item("expr_planner_names", planner_names)?;
    payload.set_item("cache_registrar_available", true)?;
    payload.set_item(
        "async",
        json_to_py(
            py,
            &json!({
                "enabled": enable_async_udfs,
                "timeout_ms": async_udf_timeout_ms,
                "batch_size": async_udf_batch_size,
            }),
        )?,
    )?;
    Ok(payload.into())
}

#[pyfunction]
pub(crate) fn udf_docs_snapshot(py: Python<'_>, ctx: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let payload = PyDict::new(py);
    let add_doc = |name: &str, doc: &datafusion_expr::Documentation| -> PyResult<()> {
        let entry = PyDict::new(py);
        entry.set_item("description", doc.description.clone())?;
        entry.set_item("syntax", doc.syntax_example.clone())?;
        entry.set_item("section", doc.doc_section.label)?;
        if let Some(example) = &doc.sql_example {
            entry.set_item("sql_example", example.clone())?;
        } else {
            entry.set_item("sql_example", py.None())?;
        }
        if let Some(arguments) = &doc.arguments {
            entry.set_item("arguments", PyList::new(py, arguments.clone())?)?;
        } else {
            entry.set_item("arguments", PyList::empty(py))?;
        }
        if let Some(alternatives) = &doc.alternative_syntax {
            entry.set_item("alternative_syntax", PyList::new(py, alternatives.clone())?)?;
        } else {
            entry.set_item("alternative_syntax", PyList::empty(py))?;
        }
        if let Some(related) = &doc.related_udfs {
            entry.set_item("related_udfs", PyList::new(py, related.clone())?)?;
        } else {
            entry.set_item("related_udfs", PyList::empty(py))?;
        }
        payload.set_item(name, entry)?;
        Ok(())
    };

    let state = extract_session_ctx(ctx)?.state();
    let docs = udf_docs::registry_docs(&state);
    for (name, doc) in docs {
        add_doc(name.as_str(), doc)?;
    }
    Ok(payload.into())
}

#[pyfunction]
#[pyo3(signature = (ctx, allow_ddl = None, allow_dml = None, allow_statements = None))]
pub(crate) fn install_codeanatomy_policy_config(
    ctx: &Bound<'_, PyAny>,
    allow_ddl: Option<bool>,
    allow_dml: Option<bool>,
    allow_statements: Option<bool>,
) -> PyResult<()> {
    let state_ref = extract_session_ctx(ctx)?.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    let policy = ensure_policy_config(config.options_mut()).map_err(|err| {
        PyRuntimeError::new_err(format!("Planner policy configuration failed: {err}"))
    })?;
    if let Some(value) = allow_ddl {
        policy.allow_ddl = value;
    }
    if let Some(value) = allow_dml {
        policy.allow_dml = value;
    }
    if let Some(value) = allow_statements {
        policy.allow_statements = value;
    }
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (ctx, enabled = None))]
pub(crate) fn install_codeanatomy_physical_config(
    ctx: &Bound<'_, PyAny>,
    enabled: Option<bool>,
) -> PyResult<()> {
    let state_ref = extract_session_ctx(ctx)?.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    let physical = ensure_physical_config(config.options_mut()).map_err(|err| {
        PyRuntimeError::new_err(format!("Physical policy configuration failed: {err}"))
    })?;
    if let Some(value) = enabled {
        physical.enabled = value;
    }
    Ok(())
}

#[pyfunction]
pub(crate) fn install_planner_rules(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    install_policy_rules_native(&extract_session_ctx(ctx)?)
        .map_err(|err| PyRuntimeError::new_err(format!("Planner rule install failed: {err}")))
}

#[pyfunction]
pub(crate) fn install_physical_rules(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    install_physical_rules_native(&extract_session_ctx(ctx)?)
        .map_err(|err| PyRuntimeError::new_err(format!("Physical rule install failed: {err}")))
}

#[pyfunction]
pub(crate) fn install_expr_planners(
    ctx: &Bound<'_, PyAny>,
    planner_names: Vec<String>,
) -> PyResult<()> {
    let names: Vec<&str> = planner_names.iter().map(String::as_str).collect();
    datafusion_ext::install_expr_planners_native(&extract_session_ctx(ctx)?, &names)
        .map_err(|err| PyRuntimeError::new_err(format!("ExprPlanner install failed: {err}")))
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(install_function_factory, module)?)?;
    module.add_function(wrap_pyfunction!(derive_function_factory_policy, module)?)?;
    module.add_function(wrap_pyfunction!(install_codeanatomy_udf_config, module)?)?;
    module.add_function(wrap_pyfunction!(register_codeanatomy_udfs, module)?)?;
    module.add_function(wrap_pyfunction!(registry_snapshot_py, module)?)?;
    module.add_function(wrap_pyfunction!(registry_snapshot_msgpack_py, module)?)?;
    module.add_function(wrap_pyfunction!(udf_docs_snapshot, module)?)?;
    module.add_function(wrap_pyfunction!(capabilities_snapshot, module)?)?;
    module.add_function(wrap_pyfunction!(install_codeanatomy_runtime, module)?)?;
    module.add_function(wrap_pyfunction!(install_codeanatomy_policy_config, module)?)?;
    module.add_function(wrap_pyfunction!(
        install_codeanatomy_physical_config,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(install_planner_rules, module)?)?;
    module.add_function(wrap_pyfunction!(install_physical_rules, module)?)?;
    module.add_function(wrap_pyfunction!(install_expr_planners, module)?)?;
    Ok(())
}
