//! Plugin bridge and manifest validation surface.

use std::collections::HashMap;
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion_ext::udf_config::CodeAnatomyUdfConfig;
use df_plugin_common::{parse_major, DELTA_SCAN_CONFIG_VERSION};
use df_plugin_host::{load_plugin, PluginHandle};
use df_plugin_host::{DF_PLUGIN_ABI_MAJOR, DF_PLUGIN_ABI_MINOR};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods, PyDict};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tracing::instrument;

use crate::delta_control_plane::scan_config_from_session as delta_scan_config_from_session_native;
use crate::delta_observability::{scan_config_payload, scan_config_schema_ipc};

use super::delta_session_bridge::install_delta_plan_codecs_inner;
use super::helpers::extract_session_ctx;

const PLUGIN_HANDLE_CAPSULE_NAME: &str = "datafusion_ext.DfPluginHandle";

fn plugin_capsule_name() -> PyResult<CString> {
    CString::new(PLUGIN_HANDLE_CAPSULE_NAME)
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))
}

fn ensure_plugin_manifest_compat(handle: &PluginHandle) -> PyResult<()> {
    let manifest = handle.manifest();
    if manifest.plugin_abi_major != DF_PLUGIN_ABI_MAJOR {
        return Err(PyRuntimeError::new_err(format!(
            "Plugin ABI mismatch: expected {expected} got {actual}",
            expected = DF_PLUGIN_ABI_MAJOR,
            actual = manifest.plugin_abi_major,
        )));
    }
    if manifest.plugin_abi_minor > DF_PLUGIN_ABI_MINOR {
        return Err(PyRuntimeError::new_err(format!(
            "Plugin ABI minor version too new: expected <= {expected} got {actual}",
            expected = DF_PLUGIN_ABI_MINOR,
            actual = manifest.plugin_abi_minor,
        )));
    }
    let ffi_major = datafusion_ffi::version();
    if manifest.df_ffi_major != ffi_major {
        return Err(PyRuntimeError::new_err(format!(
            "Plugin FFI major mismatch: expected {expected} got {actual}",
            expected = ffi_major,
            actual = manifest.df_ffi_major,
        )));
    }
    let datafusion_major =
        parse_major(datafusion::DATAFUSION_VERSION).map_err(PyValueError::new_err)?;
    if manifest.datafusion_major != datafusion_major {
        return Err(PyRuntimeError::new_err(format!(
            "Plugin DataFusion major mismatch: expected {expected} got {actual}",
            expected = datafusion_major,
            actual = manifest.datafusion_major,
        )));
    }
    let arrow_major = parse_major(arrow::ARROW_VERSION).map_err(PyValueError::new_err)?;
    if manifest.arrow_major != arrow_major {
        return Err(PyRuntimeError::new_err(format!(
            "Plugin Arrow major mismatch: expected {expected} got {actual}",
            expected = arrow_major,
            actual = manifest.arrow_major,
        )));
    }
    Ok(())
}

fn extract_plugin_handle(py: Python<'_>, plugin: &Py<PyAny>) -> PyResult<Arc<PluginHandle>> {
    let capsule: Bound<'_, PyCapsule> = plugin.bind(py).extract()?;
    let name = capsule
        .name()
        .map_err(|err| PyValueError::new_err(format!("Invalid plugin capsule: {err}")))?;
    let Some(name) = name else {
        return Err(PyValueError::new_err("Plugin capsule is missing a name."));
    };
    let name = name
        .to_str()
        .map_err(|err| PyValueError::new_err(format!("Invalid plugin capsule name: {err}")))?;
    if name != PLUGIN_HANDLE_CAPSULE_NAME {
        return Err(PyValueError::new_err(format!(
            "Unexpected plugin capsule name: {name}"
        )));
    }
    let handle: &Arc<PluginHandle> = unsafe { capsule.reference() };
    ensure_plugin_manifest_compat(handle)?;
    Ok(handle.clone())
}

fn scan_config_payload_from_ctx(ctx: &SessionContext) -> Result<JsonValue, String> {
    let session_state = ctx.state();
    let scan_config = delta_scan_config_from_session_native(
        &session_state,
        None,
        crate::delta_control_plane::DeltaScanOverrides::default(),
    )
    .map_err(|err| format!("Failed to resolve Delta scan config: {err}"))?;
    let mut payload = scan_config_payload(&scan_config)
        .map_err(|err| format!("Failed to build Delta scan config payload: {err}"))?;
    let schema_ipc = scan_config_schema_ipc(&scan_config)
        .map_err(|err| format!("Failed to encode Delta scan schema IPC: {err}"))?;
    let schema_value = match schema_ipc {
        Some(bytes) => JsonValue::Array(bytes.into_iter().map(JsonValue::from).collect()),
        None => JsonValue::Null,
    };
    payload.insert(
        "scan_config_version".to_string(),
        JsonValue::from(DELTA_SCAN_CONFIG_VERSION),
    );
    payload.insert("schema_ipc".to_string(), schema_value);
    Ok(JsonValue::Object(payload.into_iter().collect()))
}

fn inject_delta_scan_defaults(
    ctx: &SessionContext,
    provider_name: &str,
    options_json: Option<&str>,
) -> Result<Option<String>, String> {
    if provider_name != "delta" {
        return Ok(options_json.map(ToString::to_string));
    }
    let mut payload: JsonValue = if let Some(options_json) = options_json {
        serde_json::from_str(options_json)
            .map_err(|err| format!("Invalid options JSON for {provider_name}: {err}"))?
    } else {
        JsonValue::Object(JsonMap::new())
    };
    let JsonValue::Object(map) = &mut payload else {
        return Ok(options_json.map(ToString::to_string));
    };
    map.entry("scan_config".to_string())
        .or_insert(scan_config_payload_from_ctx(ctx)?);
    serde_json::to_string(&payload)
        .map(Some)
        .map_err(|err| format!("Failed to serialize options JSON for {provider_name}: {err}"))
}

fn udf_config_payload_from_ctx(ctx: &SessionContext) -> JsonValue {
    let config = CodeAnatomyUdfConfig::from_config(ctx.state().config_options());
    serde_json::json!({
        "utf8_normalize_form": config.utf8_normalize_form,
        "utf8_normalize_casefold": config.utf8_normalize_casefold,
        "utf8_normalize_collapse_ws": config.utf8_normalize_collapse_ws,
        "span_default_line_base": config.span_default_line_base,
        "span_default_col_unit": config.span_default_col_unit,
        "span_default_end_exclusive": config.span_default_end_exclusive,
        "map_normalize_key_case": config.map_normalize_key_case,
        "map_normalize_sort_keys": config.map_normalize_sort_keys,
    })
}

#[pyfunction]
#[instrument(level = "info", skip_all, fields(path = %path))]
pub(crate) fn load_df_plugin(py: Python<'_>, path: String) -> PyResult<Py<PyAny>> {
    let handle = load_plugin(Path::new(&path)).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to load DataFusion plugin {path:?}: {err}"))
    })?;
    let name = plugin_capsule_name()?;
    let capsule = PyCapsule::new(py, Arc::new(handle), Some(name))?;
    Ok(capsule.unbind().into())
}

#[pyfunction]
#[instrument(level = "info", skip_all)]
#[pyo3(signature = (ctx, plugin, options_json = None))]
pub(crate) fn register_df_plugin_udfs(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    plugin: Py<PyAny>,
    options_json: Option<String>,
) -> PyResult<()> {
    let session_ctx = extract_session_ctx(ctx)?;
    let handle = extract_plugin_handle(py, &plugin)?;
    let config_payload = udf_config_payload_from_ctx(&session_ctx);
    let mut options_value = if let Some(raw) = options_json.as_deref() {
        serde_json::from_str::<JsonValue>(raw)
            .map_err(|err| PyValueError::new_err(format!("Invalid UDF options JSON: {err}")))?
    } else {
        JsonValue::Object(JsonMap::new())
    };
    options_value = match options_value {
        JsonValue::Object(mut map) => {
            map.entry("udf_config".to_string())
                .or_insert(config_payload);
            JsonValue::Object(map)
        }
        _ => {
            let mut map = JsonMap::new();
            map.insert("udf_config".to_string(), config_payload);
            JsonValue::Object(map)
        }
    };
    let options_json =
        Some(serde_json::to_string(&options_value).map_err(|err| {
            PyValueError::new_err(format!("Failed to encode UDF options: {err}"))
        })?);
    handle
        .register_udfs(&session_ctx, options_json.as_deref())
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to register plugin UDFs: {err}")))?;
    Ok(())
}

#[pyfunction]
pub(crate) fn register_df_plugin_table_functions(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    plugin: Py<PyAny>,
) -> PyResult<()> {
    let session_ctx = extract_session_ctx(ctx)?;
    let handle = extract_plugin_handle(py, &plugin)?;
    handle
        .register_table_functions(&session_ctx)
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to register plugin table functions: {err}"))
        })?;
    Ok(())
}

#[pyfunction]
pub(crate) fn create_df_plugin_table_provider(
    py: Python<'_>,
    plugin: Py<PyAny>,
    provider_name: String,
    options_json: Option<String>,
) -> PyResult<Py<PyAny>> {
    let handle = extract_plugin_handle(py, &plugin)?;
    let provider = handle
        .create_table_provider(provider_name.as_str(), options_json.as_deref())
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to create plugin table provider {provider_name:?}: {err}"
            ))
        })?;
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, provider, Some(name))?;
    Ok(capsule.unbind().into())
}

#[pyfunction]
#[instrument(skip(py, ctx, plugin, table_names, options_json))]
pub(crate) fn register_df_plugin_table_providers(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    plugin: Py<PyAny>,
    table_names: Option<Vec<String>>,
    options_json: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let session_ctx = extract_session_ctx(ctx)?;
    install_delta_plan_codecs_inner(&session_ctx)?;
    let handle = extract_plugin_handle(py, &plugin)?;
    let mut resolved = HashMap::new();
    if let Some(options) = options_json {
        for (name, value) in options {
            let injected = inject_delta_scan_defaults(&session_ctx, name.as_str(), Some(&value))
                .map_err(PyValueError::new_err)?;
            if let Some(injected) = injected {
                resolved.insert(name, injected);
            }
        }
    }
    let wants_delta = table_names
        .as_ref()
        .map_or(true, |names| names.iter().any(|name| name == "delta"));
    if wants_delta && !resolved.contains_key("delta") {
        if let Some(injected) = inject_delta_scan_defaults(&session_ctx, "delta", None)
            .map_err(PyValueError::new_err)?
        {
            resolved.insert("delta".to_string(), injected);
        }
    }
    let resolved_options = if resolved.is_empty() {
        None
    } else {
        Some(resolved)
    };
    handle
        .register_table_providers(
            &session_ctx,
            table_names.as_deref(),
            resolved_options.as_ref(),
        )
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to register plugin table providers: {err}"))
        })?;
    Ok(())
}

#[pyfunction]
#[instrument(level = "info", skip_all)]
pub(crate) fn register_df_plugin(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    plugin: Py<PyAny>,
    table_names: Option<Vec<String>>,
    options_json: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let session_ctx = extract_session_ctx(ctx)?;
    let handle = extract_plugin_handle(py, &plugin)?;
    handle
        .register_udfs(&session_ctx, None)
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to register plugin UDFs: {err}")))?;
    handle
        .register_table_functions(&session_ctx)
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to register plugin table functions: {err}"))
        })?;
    install_delta_plan_codecs_inner(&session_ctx)?;
    let resolved_options = if let Some(options) = options_json {
        let mut resolved = HashMap::with_capacity(options.len());
        for (name, value) in options {
            let injected = inject_delta_scan_defaults(&session_ctx, name.as_str(), Some(&value))
                .map_err(PyValueError::new_err)?;
            if let Some(injected) = injected {
                resolved.insert(name, injected);
            }
        }
        Some(resolved)
    } else {
        None
    };
    handle
        .register_table_providers(
            &session_ctx,
            table_names.as_deref(),
            resolved_options.as_ref(),
        )
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to register plugin table providers: {err}"))
        })?;
    Ok(())
}

fn plugin_library_filename(crate_name: &str) -> String {
    if cfg!(target_os = "windows") {
        format!("{crate_name}.dll")
    } else if cfg!(target_os = "macos") {
        format!("lib{crate_name}.dylib")
    } else {
        format!("lib{crate_name}.so")
    }
}

#[pyfunction]
pub(crate) fn plugin_library_path(py: Python<'_>) -> PyResult<String> {
    let module = py.import("datafusion_ext")?;
    let module_file: String = module.getattr("__file__")?.extract()?;
    let module_path = PathBuf::from(module_file);
    let lib_name = plugin_library_filename("df_plugin_codeanatomy");
    let base = module_path.parent().ok_or_else(|| {
        PyRuntimeError::new_err("datafusion_ext.__file__ has no parent directory")
    })?;
    let plugin_path = base.join("plugin").join(&lib_name);
    if plugin_path.exists() {
        return Ok(plugin_path.display().to_string());
    }
    Ok(base.join(lib_name).display().to_string())
}

#[pyfunction]
#[pyo3(signature = (path = None))]
pub(crate) fn plugin_manifest(py: Python<'_>, path: Option<String>) -> PyResult<Py<PyAny>> {
    let resolved = if let Some(value) = path {
        value
    } else {
        plugin_library_path(py)?
    };
    let handle = load_plugin(Path::new(&resolved)).map_err(|err| {
        PyRuntimeError::new_err(format!(
            "Failed to load plugin manifest for {resolved:?}: {err}"
        ))
    })?;
    let manifest = handle.manifest();
    let payload = PyDict::new(py);
    payload.set_item("plugin_path", resolved)?;
    payload.set_item("struct_size", manifest.struct_size)?;
    payload.set_item("plugin_abi_major", manifest.plugin_abi_major)?;
    payload.set_item("plugin_abi_minor", manifest.plugin_abi_minor)?;
    payload.set_item("df_ffi_major", manifest.df_ffi_major)?;
    payload.set_item("datafusion_major", manifest.datafusion_major)?;
    payload.set_item("arrow_major", manifest.arrow_major)?;
    payload.set_item("plugin_name", manifest.plugin_name.to_string())?;
    payload.set_item("plugin_version", manifest.plugin_version.to_string())?;
    payload.set_item("build_id", manifest.build_id.to_string())?;
    payload.set_item("capabilities", manifest.capabilities)?;
    let relation_planners = handle.relation_planner_names().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to read relation planners: {err}"))
    })?;
    payload.set_item("relation_planners", relation_planners)?;
    let features: Vec<String> = manifest
        .features
        .iter()
        .map(|value| value.to_string())
        .collect();
    payload.set_item("features", features)?;
    Ok(payload.into())
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(load_df_plugin, module)?)?;
    module.add_function(wrap_pyfunction!(register_df_plugin_udfs, module)?)?;
    module.add_function(wrap_pyfunction!(
        register_df_plugin_table_functions,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(create_df_plugin_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(
        register_df_plugin_table_providers,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(register_df_plugin, module)?)?;
    module.add_function(wrap_pyfunction!(plugin_library_path, module)?)?;
    module.add_function(wrap_pyfunction!(plugin_manifest, module)?)?;
    Ok(())
}

pub(crate) fn register_internal_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(create_df_plugin_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(plugin_library_path, module)?)?;
    module.add_function(wrap_pyfunction!(plugin_manifest, module)?)?;
    Ok(())
}
