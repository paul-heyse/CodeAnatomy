//! Shared helpers for the CodeAnatomy Python extension bridge.

use std::collections::HashMap;
use std::ffi::CString;
use std::sync::{Arc, OnceLock};

use arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::execution::TaskContextProvider;
use datafusion_ext::DeltaFeatureGate;
use datafusion_ffi::table_provider::FFI_TableProvider;
use deltalake::delta_datafusion::DeltaScanConfig;
use df_plugin_common::{schema_from_ipc, DELTA_SCAN_CONFIG_VERSION};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyCapsule, PyDict, PyFloat, PyInt, PyList, PyString};
use rmp_serde::from_slice;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use tokio::runtime::Runtime;

use crate::context::PySessionContext;
use crate::delta_control_plane::DeltaScanOverrides;
use crate::delta_maintenance::DeltaMaintenanceReport;
use crate::delta_mutations::DeltaMutationReport;
use crate::delta_observability::{
    maintenance_report_payload, mutation_report_payload, scan_config_payload,
    scan_config_schema_ipc, snapshot_info_as_values,
};
use crate::delta_protocol::{gate_from_parts, DeltaSnapshotInfo, TableVersion};
use crate::utils::get_global_ctx;

/// Extract the underlying DataFusion `SessionContext` from a Python object.
pub(crate) fn extract_session_ctx(ctx: &Bound<'_, PyAny>) -> PyResult<SessionContext> {
    if let Ok(session) = ctx.extract::<Bound<'_, PySessionContext>>() {
        return Ok(session.borrow().ctx().clone());
    }
    let inner = ctx.getattr("ctx")?;
    let session: Bound<'_, PySessionContext> = inner.extract()?;
    Ok(session.borrow().ctx().clone())
}

pub(crate) fn parse_msgpack_payload<T: for<'de> serde::Deserialize<'de>>(
    payload: &[u8],
    label: &str,
) -> PyResult<T> {
    from_slice(payload)
        .map_err(|err| PyValueError::new_err(format!("Invalid {label} payload: {err}")))
}

pub(crate) fn parse_python_payload<T: DeserializeOwned>(
    py: Python<'_>,
    payload: &Bound<'_, PyAny>,
    label: &str,
) -> PyResult<T> {
    let json_module = py.import("json")?;
    let payload_json: String = json_module
        .call_method1("dumps", (payload,))?
        .extract()
        .map_err(|err| PyValueError::new_err(format!("Invalid {label} payload: {err}")))?;
    serde_json::from_str::<T>(&payload_json)
        .map_err(|err| PyValueError::new_err(format!("Invalid {label} payload: {err}")))
}

pub(crate) fn storage_options_map(
    storage_options: Option<Vec<(String, String)>>,
) -> Option<HashMap<String, String>> {
    storage_options.map(|options| options.into_iter().collect())
}

pub(crate) fn delta_gate_from_params(
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
) -> Option<DeltaFeatureGate> {
    let reader_empty = required_reader_features
        .as_ref()
        .map_or(true, |features| features.is_empty());
    let writer_empty = required_writer_features
        .as_ref()
        .map_or(true, |features| features.is_empty());
    if min_reader_version.is_none() && min_writer_version.is_none() && reader_empty && writer_empty
    {
        return None;
    }
    Some(gate_from_parts(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    ))
}

pub(crate) fn table_version_from_options(
    version: Option<i64>,
    timestamp: Option<String>,
) -> PyResult<TableVersion> {
    TableVersion::from_options(version, timestamp)
        .map_err(|err| PyValueError::new_err(format!("Invalid Delta table version options: {err}")))
}

fn decode_schema_ipc(schema_ipc: &[u8]) -> PyResult<SchemaRef> {
    schema_from_ipc(schema_ipc)
        .map_err(|err| PyValueError::new_err(format!("Failed to decode schema IPC: {err}")))
}

pub(crate) fn scan_overrides_from_params(
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,
    wrap_partition_values: Option<bool>,
    schema_ipc: Option<Vec<u8>>,
) -> PyResult<DeltaScanOverrides> {
    let schema = match schema_ipc {
        Some(schema_ipc) => Some(decode_schema_ipc(&schema_ipc)?),
        None => None,
    };
    Ok(DeltaScanOverrides {
        file_column_name,
        enable_parquet_pushdown,
        schema_force_view_types,
        wrap_partition_values,
        schema,
    })
}

pub(crate) fn runtime() -> PyResult<Runtime> {
    Runtime::new()
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}")))
}

pub(crate) fn global_task_ctx_provider() -> Arc<dyn TaskContextProvider> {
    static TASK_CTX_PROVIDER: OnceLock<Arc<SessionContext>> = OnceLock::new();
    let provider = TASK_CTX_PROVIDER.get_or_init(|| Arc::new(get_global_ctx().clone()));
    Arc::clone(provider) as Arc<dyn TaskContextProvider>
}

pub(crate) fn provider_capsule(
    py: Python<'_>,
    provider: Arc<dyn TableProvider>,
) -> PyResult<Py<PyAny>> {
    let task_ctx_provider = global_task_ctx_provider();
    let ffi_provider = FFI_TableProvider::new(
        provider,
        true,
        None,
        &task_ctx_provider,
        None,
    );
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, ffi_provider, Some(name))?;
    Ok(capsule.unbind().into())
}

pub(crate) fn json_to_py(py: Python<'_>, value: &JsonValue) -> PyResult<Py<PyAny>> {
    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(flag) => Ok(PyBool::new(py, *flag).to_owned().unbind().into()),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                return Ok(PyInt::new(py, value).unbind().into());
            }
            if let Some(value) = number.as_u64() {
                return Ok(PyInt::new(py, value).unbind().into());
            }
            if let Some(value) = number.as_f64() {
                return Ok(PyFloat::new(py, value).unbind().into());
            }
            Ok(py.None())
        }
        JsonValue::String(text) => Ok(PyString::new(py, text.as_str()).unbind().into()),
        JsonValue::Array(items) => {
            let mut converted: Vec<Py<PyAny>> = Vec::with_capacity(items.len());
            for item in items {
                converted.push(json_to_py(py, item)?);
            }
            Ok(PyList::new(py, converted)?.into())
        }
        JsonValue::Object(entries) => {
            let payload = PyDict::new(py);
            for (key, entry) in entries {
                payload.set_item(key, json_to_py(py, entry)?)?;
            }
            Ok(payload.into())
        }
    }
}

fn payload_to_pydict(py: Python<'_>, payload: HashMap<String, JsonValue>) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (key, value) in payload {
        dict.set_item(key, json_to_py(py, &value)?)?;
    }
    Ok(dict.into())
}

pub(crate) fn snapshot_to_pydict(
    py: Python<'_>,
    snapshot: &DeltaSnapshotInfo,
) -> PyResult<Py<PyAny>> {
    payload_to_pydict(py, snapshot_info_as_values(snapshot))
}

pub(crate) fn scan_config_to_pydict(
    py: Python<'_>,
    scan_config: &DeltaScanConfig,
) -> PyResult<Py<PyAny>> {
    let payload = scan_config_payload(scan_config).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta scan config payload: {err}"))
    })?;
    let schema_ipc = scan_config_schema_ipc(scan_config).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to encode Delta scan schema IPC: {err}"))
    })?;
    let dict = PyDict::new(py);
    for (key, value) in payload {
        dict.set_item(key, json_to_py(py, &value)?)?;
    }
    dict.set_item("scan_config_version", DELTA_SCAN_CONFIG_VERSION)?;
    if let Some(schema_ipc) = schema_ipc {
        dict.set_item("schema_ipc", PyBytes::new(py, schema_ipc.as_slice()))?;
    } else {
        dict.set_item("schema_ipc", py.None())?;
    }
    Ok(dict.into())
}

pub(crate) fn mutation_report_to_pydict(
    py: Python<'_>,
    report: &DeltaMutationReport,
) -> PyResult<Py<PyAny>> {
    payload_to_pydict(py, mutation_report_payload(report))
}

pub(crate) fn maintenance_report_to_pydict(
    py: Python<'_>,
    report: &DeltaMaintenanceReport,
) -> PyResult<Py<PyAny>> {
    payload_to_pydict(py, maintenance_report_payload(report))
}
