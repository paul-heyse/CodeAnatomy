//! DataFusion extension for native function registration.

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::CString;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Int64Array, MapBuilder, RecordBatchReader, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, Session,
    TableFunctionImpl, TableProvider,
};
use datafusion::config::ConfigOptions;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::{
    Constraint, Constraints, DFSchema, DataFusionError, Result, ScalarValue, Statistics,
};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_parquet::file_format::ParquetFormat;
use datafusion_expr::{
    lit, CreateExternalTable, DdlStatement, Expr, LogicalPlan, SortExpr,
    TableProviderFilterPushDown, TableType,
};
use datafusion_ext::install_expr_planners_native;
use datafusion_ext::planner_rules::{
    ensure_policy_config, install_policy_rules as install_policy_rules_native,
};
use datafusion_ext::physical_rules::{
    ensure_physical_config,
    install_physical_rules as install_physical_rules_native,
};
use datafusion_ext::udf_config::{CodeAnatomyUdfConfig, UdfConfigValue};
use datafusion_ext::udf_expr as udf_expr_mod;
use datafusion_ffi;
use datafusion_ffi::table_provider::FFI_TableProvider;
use df_plugin_host::{DF_PLUGIN_ABI_MAJOR, DF_PLUGIN_ABI_MINOR};
use crate::delta_control_plane::{
    delta_add_actions as delta_add_actions_native,
    delta_cdf_provider as delta_cdf_provider_native,
    delta_provider_from_session as delta_provider_from_session_native,
    delta_provider_with_files as delta_provider_with_files_native,
    scan_config_from_session as delta_scan_config_from_session_native,
    snapshot_info_with_gate as delta_snapshot_info_native, DeltaCdfScanOptions,
    DeltaScanOverrides,
};
use crate::delta_maintenance::{
    delta_add_features as delta_add_features_native,
    delta_add_constraints as delta_add_constraints_native,
    delta_cleanup_metadata as delta_cleanup_metadata_native,
    delta_create_checkpoint as delta_create_checkpoint_native,
    delta_drop_constraints as delta_drop_constraints_native,
    delta_optimize_compact as delta_optimize_compact_native, delta_restore as delta_restore_native,
    delta_set_properties as delta_set_properties_native, delta_vacuum as delta_vacuum_native,
    DeltaMaintenanceReport,
};
use crate::delta_mutations::{
    delta_data_check as delta_data_check_native, delta_delete as delta_delete_native,
    delta_merge as delta_merge_native, delta_update as delta_update_native,
    delta_write_ipc as delta_write_ipc_native, DeltaMutationReport,
};
use crate::delta_observability::{
    add_action_payloads, maintenance_report_payload, mutation_report_payload, scan_config_payload,
    scan_config_schema_ipc, snapshot_payload,
};
use crate::delta_protocol::{gate_from_parts, DeltaSnapshotInfo};
use datafusion_ext::{DeltaAppTransaction, DeltaCommitOptions, DeltaFeatureGate};
use df_plugin_host::{load_plugin, PluginHandle};
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use crate::{registry_snapshot, udf_docs};
use datafusion_ext::udf_registry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::expr_fn::col;
use crate::context::{PyRuntimeEnvBuilder, PySessionContext};
use crate::expr::PyExpr;
use crate::utils::py_obj_to_scalar_value;
use deltalake::delta_datafusion::{
    DeltaLogicalCodec, DeltaPhysicalCodec, DeltaRuntimeEnvBuilder, DeltaScanConfig,
    DeltaSessionConfig, DeltaTableFactory, DeltaTableProvider,
};
use deltalake::delta_datafusion::planner::DeltaPlanner;
use deltalake::protocol::SaveMode;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyBytes, PyCapsule, PyCapsuleMethods, PyDict, PyFloat, PyInt, PyList, PyString,
    PyTuple,
};
use tokio::runtime::Runtime;
use datafusion::optimizer::OptimizerConfig;

const DELTA_SCAN_CONFIG_VERSION: u32 = 1;

macro_rules! register_pyfunctions {
    ($module:expr, [$($func:path),* $(,)?]) => {
        $(
            $module.add_function(pyo3::wrap_pyfunction!($func, $module)?)?;
        )*
    };
}

macro_rules! register_pyclasses {
    ($module:expr, [$($class:ty),* $(,)?]) => {
        $(
            $module.add_class::<$class>()?;
        )*
    };
}

fn schema_from_ipc(schema_ipc: Vec<u8>) -> PyResult<SchemaRef> {
    let reader = StreamReader::try_new(Cursor::new(schema_ipc), None)
        .map_err(|err| PyValueError::new_err(format!("Failed to decode schema IPC: {err}")))?;
    Ok(reader.schema())
}

fn storage_options_map(
    storage_options: Option<Vec<(String, String)>>,
) -> Option<HashMap<String, String>> {
    storage_options.map(|options| options.into_iter().collect())
}

fn delta_gate_from_params(
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

fn scan_overrides_from_params(
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,
    wrap_partition_values: Option<bool>,
    schema_ipc: Option<Vec<u8>>,
) -> PyResult<DeltaScanOverrides> {
    let schema = match schema_ipc {
        Some(schema_ipc) => Some(schema_from_ipc(schema_ipc)?),
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

fn scan_config_payload_from_ctx(ctx: &SessionContext) -> Result<JsonValue, String> {
    let session_state = ctx.state();
    let scan_config = delta_scan_config_from_session_native(
        &session_state,
        None,
        DeltaScanOverrides::default(),
    )
    .map_err(|err| format!("Failed to resolve Delta scan config: {err}"))?;
    let mut payload = scan_config_payload(&scan_config)
        .map_err(|err| format!("Failed to build Delta scan config payload: {err}"))?;
    let schema_ipc = scan_config_schema_ipc(&scan_config)
        .map_err(|err| format!("Failed to encode Delta scan schema IPC: {err}"))?;
    let schema_value = match schema_ipc {
        Some(bytes) => JsonValue::Array(
            bytes
                .into_iter()
                .map(JsonValue::from)
                .collect(),
        ),
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
        return Ok(options_json.map(|value| value.to_string()));
    }
    let mut payload: JsonValue = if let Some(options_json) = options_json {
        serde_json::from_str(options_json)
            .map_err(|err| format!("Invalid options JSON for {provider_name}: {err}"))?
    } else {
        JsonValue::Object(JsonMap::new())
    };
    let JsonValue::Object(map) = &mut payload else {
        return Ok(options_json.map(|value| value.to_string()));
    };
    map.entry("scan_config".to_string())
        .or_insert(scan_config_payload_from_ctx(ctx)?);
    serde_json::to_string(&payload)
        .map(Some)
        .map_err(|err| format!("Failed to serialize options JSON for {provider_name}: {err}"))
}

fn runtime() -> PyResult<Runtime> {
    Runtime::new()
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}")))
}

fn provider_capsule(py: Python<'_>, provider: DeltaTableProvider) -> PyResult<Py<PyAny>> {
    let ffi_provider = FFI_TableProvider::new(Arc::new(provider), true, None);
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, ffi_provider, Some(name))?;
    Ok(capsule.unbind().into())
}

fn json_to_py(py: Python<'_>, value: &JsonValue) -> PyResult<Py<PyAny>> {
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

fn registry_snapshot_hash(snapshot: &registry_snapshot::RegistrySnapshot) -> PyResult<String> {
    let mut hasher = Blake2bVar::new(16).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to initialize registry hash: {err}"))
    })?;
    for name in snapshot
        .scalar
        .iter()
        .chain(snapshot.aggregate.iter())
        .chain(snapshot.window.iter())
        .chain(snapshot.table.iter())
    {
        hasher.update(name.as_bytes());
        hasher.update(&[0]);
    }
    let mut out = vec![0_u8; 16];
    hasher
        .finalize_variable(&mut out)
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to finalize registry hash: {err}")))?;
    Ok(hex::encode(out))
}

fn payload_to_pydict(py: Python<'_>, payload: HashMap<String, JsonValue>) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (key, value) in payload {
        dict.set_item(key, json_to_py(py, &value)?)?;
    }
    Ok(dict.into())
}

fn snapshot_to_pydict(py: Python<'_>, snapshot: &DeltaSnapshotInfo) -> PyResult<Py<PyAny>> {
    payload_to_pydict(py, snapshot_payload(snapshot))
}

fn scan_config_to_pydict(py: Python<'_>, scan_config: &DeltaScanConfig) -> PyResult<Py<PyAny>> {
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

fn mutation_report_to_pydict(py: Python<'_>, report: &DeltaMutationReport) -> PyResult<Py<PyAny>> {
    payload_to_pydict(py, mutation_report_payload(report))
}

fn maintenance_report_to_pydict(
    py: Python<'_>,
    report: &DeltaMaintenanceReport,
) -> PyResult<Py<PyAny>> {
    payload_to_pydict(py, maintenance_report_payload(report))
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

fn commit_options_from_params(
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<DeltaCommitOptions> {
    let metadata = commit_metadata
        .map(|pairs| pairs.into_iter().collect::<HashMap<String, String>>())
        .unwrap_or_default();
    let app_transaction = match (app_id, app_version) {
        (Some(app_id), Some(version)) => Some(DeltaAppTransaction {
            app_id,
            version,
            last_updated: app_last_updated,
        }),
        (None, None) => None,
        _ => {
            return Err(PyValueError::new_err(
                "Delta app transaction requires both app_id and app_version.",
            ));
        }
    };
    Ok(DeltaCommitOptions {
        metadata,
        app_transaction,
        max_retries,
        create_checkpoint,
    })
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

const PLUGIN_HANDLE_CAPSULE_NAME: &str = "datafusion_ext.DfPluginHandle";

fn plugin_capsule_name() -> PyResult<CString> {
    CString::new(PLUGIN_HANDLE_CAPSULE_NAME)
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))
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

fn parse_major(version: &str) -> PyResult<u16> {
    let Some((major, _)) = version.split_once('.') else {
        return Err(PyValueError::new_err(format!(
            "Invalid version string {version:?}"
        )));
    };
    major.parse::<u16>().map_err(|err| {
        PyValueError::new_err(format!("Invalid version string {version:?}: {err}"))
    })
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
    let datafusion_major = parse_major(datafusion::DATAFUSION_VERSION)?;
    if manifest.datafusion_major != datafusion_major {
        return Err(PyRuntimeError::new_err(format!(
            "Plugin DataFusion major mismatch: expected {expected} got {actual}",
            expected = datafusion_major,
            actual = manifest.datafusion_major,
        )));
    }
    let arrow_major = parse_major(arrow::ARROW_VERSION)?;
    if manifest.arrow_major != arrow_major {
        return Err(PyRuntimeError::new_err(format!(
            "Plugin Arrow major mismatch: expected {expected} got {actual}",
            expected = arrow_major,
            actual = manifest.arrow_major,
        )));
    }
    Ok(())
}

fn udf_config_payload_from_ctx(ctx: &SessionContext) -> JsonValue {
    let config = CodeAnatomyUdfConfig::from_config(ctx.state().config_options());
    json!({
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
fn install_codeanatomy_udf_config(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    let state_ref = ctx.ctx.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    if config.get_extension::<CodeAnatomyUdfConfig>().is_none() {
        config.set_extension(Arc::new(CodeAnatomyUdfConfig::default()));
    }
    Ok(())
}

#[pyfunction]
fn install_function_factory(
    ctx: PyRef<PySessionContext>,
    policy_ipc: &Bound<'_, PyBytes>,
) -> PyResult<()> {
    datafusion_ext::udf::install_function_factory_native(&ctx.ctx, policy_ipc.as_bytes())
        .map_err(|err| PyRuntimeError::new_err(format!("FunctionFactory install failed: {err}")))
}

#[pyfunction]
fn capabilities_snapshot(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to build registry snapshot: {err}")))?;
    let snapshot = registry_snapshot::registry_snapshot(&ctx.state());
    let hash = registry_snapshot_hash(&snapshot)?;
    let payload = json!({
        "datafusion_version": datafusion::DATAFUSION_VERSION,
        "arrow_version": arrow::ARROW_VERSION,
        "plugin_abi": {
            "major": DF_PLUGIN_ABI_MAJOR,
            "minor": DF_PLUGIN_ABI_MINOR,
        },
        "delta_control_plane": {
            "available": true,
            "entrypoints": [
                "delta_table_provider_from_session",
                "delta_table_provider_with_files",
                "delta_scan_config_from_session",
                "delta_write_ipc",
                "delta_merge",
                "delta_vacuum",
            ],
        },
        "substrait": {
            "available": cfg!(feature = "substrait"),
        },
        "async_udf": {
            "available": cfg!(feature = "async-udf"),
        },
        "udf_registry": {
            "scalar": snapshot.scalar.len(),
            "aggregate": snapshot.aggregate.len(),
            "window": snapshot.window.len(),
            "table": snapshot.table.len(),
            "custom": snapshot.custom_udfs.len(),
            "hash": hash,
        },
    });
    json_to_py(py, &payload)
}

#[pyfunction]
fn arrow_stream_to_batches(py: Python<'_>, obj: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let bound = obj.bind(py);
    let mut reader = ArrowArrayStreamReader::from_pyarrow_bound(&bound).map_err(|err| {
        PyValueError::new_err(format!(
            "Expected an object supporting __arrow_c_stream__: {err}"
        ))
    })?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch
            .map_err(|err| PyRuntimeError::new_err(format!("Arrow stream batch error: {err}")))?;
        batches.push(batch);
    }
    let mut py_batches = Vec::with_capacity(batches.len());
    for batch in batches {
        py_batches.push(batch.to_pyarrow(py)?);
    }
    let py_batches = PyList::new(py, py_batches)?;
    let schema_py = schema.to_pyarrow(py)?;
    let reader = py
        .import("pyarrow")?
        .getattr("RecordBatchReader")?
        .call_method1("from_batches", (schema_py, py_batches))?;
    Ok(reader.into())
}

#[pyfunction]
#[pyo3(signature = (name, *args, ctx=None))]
fn udf_expr(
    py: Python<'_>,
    name: String,
    args: &Bound<'_, PyTuple>,
    ctx: Option<PyRef<PySessionContext>>,
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
    let expr = if let Some(ctx_ref) = ctx.as_ref() {
        let state = ctx_ref.ctx.state();
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
#[pyo3(signature = (ctx, allow_ddl = None, allow_dml = None, allow_statements = None))]
fn install_codeanatomy_policy_config(
    ctx: PyRef<PySessionContext>,
    allow_ddl: Option<bool>,
    allow_dml: Option<bool>,
    allow_statements: Option<bool>,
) -> PyResult<()> {
    let state_ref = ctx.ctx.state_ref();
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
fn install_codeanatomy_physical_config(
    ctx: PyRef<PySessionContext>,
    enabled: Option<bool>,
) -> PyResult<()> {
    let state_ref = ctx.ctx.state_ref();
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
fn install_planner_rules(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    install_policy_rules_native(&ctx.ctx)
        .map_err(|err| PyRuntimeError::new_err(format!("Planner rule install failed: {err}")))
}

#[pyfunction]
fn install_physical_rules(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    install_physical_rules_native(&ctx.ctx)
        .map_err(|err| PyRuntimeError::new_err(format!("Physical rule install failed: {err}")))
}

#[pyfunction(name = "registry_snapshot")]
fn registry_snapshot_py(py: Python<'_>, ctx: PyRef<PySessionContext>) -> PyResult<Py<PyAny>> {
    let snapshot = registry_snapshot::registry_snapshot(&ctx.ctx.state());
    let payload = PyDict::new(py);
    payload.set_item("scalar", PyList::new(py, snapshot.scalar)?)?;
    payload.set_item("aggregate", PyList::new(py, snapshot.aggregate)?)?;
    payload.set_item("window", PyList::new(py, snapshot.window)?)?;
    payload.set_item("table", PyList::new(py, snapshot.table)?)?;
    let alias_payload = PyDict::new(py);
    for (name, aliases) in snapshot.aliases {
        alias_payload.set_item(name, PyList::new(py, aliases)?)?;
    }
    payload.set_item("aliases", alias_payload)?;
    let param_payload = PyDict::new(py);
    for (name, params) in snapshot.parameter_names {
        param_payload.set_item(name, PyList::new(py, params)?)?;
    }
    payload.set_item("parameter_names", param_payload)?;
    let volatility_payload = PyDict::new(py);
    for (name, volatility) in snapshot.volatility {
        volatility_payload.set_item(name, volatility)?;
    }
    payload.set_item("volatility", volatility_payload)?;
    let rewrite_payload = PyDict::new(py);
    for (name, tags) in snapshot.rewrite_tags {
        rewrite_payload.set_item(name, PyList::new(py, tags)?)?;
    }
    payload.set_item("rewrite_tags", rewrite_payload)?;
    let simplify_payload = PyDict::new(py);
    for (name, enabled) in snapshot.simplify {
        simplify_payload.set_item(name, enabled)?;
    }
    payload.set_item("simplify", simplify_payload)?;
    let coerce_payload = PyDict::new(py);
    for (name, enabled) in snapshot.coerce_types {
        coerce_payload.set_item(name, enabled)?;
    }
    payload.set_item("coerce_types", coerce_payload)?;
    let short_payload = PyDict::new(py);
    for (name, enabled) in snapshot.short_circuits {
        short_payload.set_item(name, enabled)?;
    }
    payload.set_item("short_circuits", short_payload)?;
    let signature_payload = PyDict::new(py);
    for (name, signatures) in snapshot.signature_inputs {
        let mut rows: Vec<Py<PyAny>> = Vec::with_capacity(signatures.len());
        for row in signatures {
            rows.push(PyList::new(py, row)?.into());
        }
        signature_payload.set_item(name, PyList::new(py, rows)?)?;
    }
    payload.set_item("signature_inputs", signature_payload)?;
    let return_payload = PyDict::new(py);
    for (name, return_types) in snapshot.return_types {
        return_payload.set_item(name, PyList::new(py, return_types)?)?;
    }
    payload.set_item("return_types", return_payload)?;
    let config_payload = PyDict::new(py);
    for (name, defaults) in snapshot.config_defaults {
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
    payload.set_item("custom_udfs", PyList::new(py, snapshot.custom_udfs)?)?;
    payload.set_item("pycapsule_udfs", PyList::empty(py))?;
    Ok(payload.into())
}

#[pyfunction]
#[pyo3(signature = (
    ctx,
    enable_async = false,
    async_udf_timeout_ms = None,
    async_udf_batch_size = None
))]
fn register_codeanatomy_udfs(
    ctx: PyRef<PySessionContext>,
    enable_async: bool,
    async_udf_timeout_ms: Option<u64>,
    async_udf_batch_size: Option<usize>,
) -> PyResult<()> {
    udf_registry::register_all_with_policy(
        &ctx.ctx,
        enable_async,
        async_udf_timeout_ms,
        async_udf_batch_size,
    )
    .map_err(|err| PyRuntimeError::new_err(format!("Failed to register CodeAnatomy UDFs: {err}")))?;
    Ok(())
}

#[pyfunction]
fn udf_docs_snapshot(py: Python<'_>, ctx: PyRef<PySessionContext>) -> PyResult<Py<PyAny>> {
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

    let state = ctx.ctx.state();
    let docs = udf_docs::registry_docs(&state);
    for (name, doc) in docs {
        add_doc(name.as_str(), doc)?;
    }
    Ok(payload.into())
}

#[pyfunction]
fn load_df_plugin(py: Python<'_>, path: String) -> PyResult<Py<PyAny>> {
    let handle = load_plugin(Path::new(&path)).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to load DataFusion plugin {path:?}: {err}"))
    })?;
    let name = plugin_capsule_name()?;
    let capsule = PyCapsule::new(py, Arc::new(handle), Some(name))?;
    Ok(capsule.unbind().into())
}

#[pyfunction]
#[pyo3(signature = (ctx, plugin, options_json = None))]
fn register_df_plugin_udfs(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    plugin: Py<PyAny>,
    options_json: Option<String>,
) -> PyResult<()> {
    let handle = extract_plugin_handle(py, &plugin)?;
    let config_payload = udf_config_payload_from_ctx(&ctx.ctx);
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
    let options_json = Some(
        serde_json::to_string(&options_value)
            .map_err(|err| PyValueError::new_err(format!("Failed to encode UDF options: {err}")))?,
    );
    handle
        .register_udfs(&ctx.ctx, options_json.as_deref())
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to register plugin UDFs: {err}")))?;
    Ok(())
}

#[pyfunction]
fn register_df_plugin_table_functions(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    plugin: Py<PyAny>,
) -> PyResult<()> {
    let handle = extract_plugin_handle(py, &plugin)?;
    handle.register_table_functions(&ctx.ctx).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to register plugin table functions: {err}"))
    })?;
    Ok(())
}

#[pyfunction]
fn create_df_plugin_table_provider(
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
fn register_df_plugin_table_providers(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    plugin: Py<PyAny>,
    table_names: Option<Vec<String>>,
    options_json: Option<HashMap<String, String>>,
) -> PyResult<()> {
    install_delta_plan_codecs_inner(&ctx)?;
    let handle = extract_plugin_handle(py, &plugin)?;
    let mut resolved = HashMap::new();
    if let Some(options) = options_json {
        for (name, value) in options {
            let injected = inject_delta_scan_defaults(&ctx.ctx, name.as_str(), Some(&value))
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
        if let Some(injected) = inject_delta_scan_defaults(&ctx.ctx, "delta", None)
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
        .register_table_providers(&ctx.ctx, table_names.as_deref(), resolved_options.as_ref())
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to register plugin table providers: {err}"))
        })?;
    Ok(())
}

#[pyfunction]
fn register_df_plugin(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    plugin: Py<PyAny>,
    table_names: Option<Vec<String>>,
    options_json: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let handle = extract_plugin_handle(py, &plugin)?;
    handle
        .register_udfs(&ctx.ctx, None)
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to register plugin UDFs: {err}")))?;
    handle.register_table_functions(&ctx.ctx).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to register plugin table functions: {err}"))
    })?;
    install_delta_plan_codecs_inner(&ctx)?;
    let resolved_options = if let Some(options) = options_json {
        let mut resolved = HashMap::with_capacity(options.len());
        for (name, value) in options {
            let injected = inject_delta_scan_defaults(&ctx.ctx, name.as_str(), Some(&value))
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
        .register_table_providers(&ctx.ctx, table_names.as_deref(), resolved_options.as_ref())
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
fn plugin_library_path(py: Python<'_>) -> PyResult<String> {
    let module = py.import("datafusion_ext")?;
    let module_file: String = module.getattr("__file__")?.extract()?;
    let module_path = PathBuf::from(module_file);
    let lib_name = plugin_library_filename("df_plugin_codeanatomy");
    let base = module_path
        .parent()
        .ok_or_else(|| PyRuntimeError::new_err("datafusion_ext.__file__ has no parent directory"))?;
    let plugin_path = base.join("plugin").join(&lib_name);
    if plugin_path.exists() {
        return Ok(plugin_path.display().to_string());
    }
    Ok(base.join(lib_name).display().to_string())
}

#[pyfunction]
#[pyo3(signature = (path = None))]
fn plugin_manifest(py: Python<'_>, path: Option<String>) -> PyResult<Py<PyAny>> {
    let resolved = if let Some(value) = path {
        value
    } else {
        plugin_library_path(py)?
    };
    let handle = load_plugin(Path::new(&resolved)).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to load plugin manifest for {resolved:?}: {err}"))
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
    let features: Vec<String> = manifest
        .features
        .iter()
        .map(|value| value.to_string())
        .collect();
    let features_list = PyList::new(py, features)?;
    payload.set_item("features", features_list)?;
    Ok(payload.into())
}

#[derive(Debug, Default)]
struct SchemaEvolutionAdapterFactory;

impl PhysicalExprAdapterFactory for SchemaEvolutionAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        DefaultPhysicalExprAdapterFactory.create(logical_file_schema, physical_file_schema)
    }
}

#[derive(Debug)]
struct CpgTableProvider {
    inner: Arc<dyn TableProvider>,
    ddl: Option<String>,
    logical_plan: Option<LogicalPlan>,
    column_defaults: HashMap<String, Expr>,
    constraints: Option<Constraints>,
}

impl CpgTableProvider {
    fn new(
        inner: Arc<dyn TableProvider>,
        ddl: Option<String>,
        logical_plan: Option<LogicalPlan>,
        column_defaults: HashMap<String, Expr>,
        constraints: Option<Constraints>,
    ) -> Self {
        Self {
            inner,
            ddl,
            logical_plan,
            column_defaults,
            constraints,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for CpgTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.constraints
            .as_ref()
            .or_else(|| self.inner.constraints())
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.ddl
            .as_deref()
            .or_else(|| self.inner.get_table_definition())
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.logical_plan
            .as_ref()
            .map(Cow::Borrowed)
            .or_else(|| self.inner.get_logical_plan())
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults
            .get(column)
            .or_else(|| self.inner.get_column_default(column))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}

fn map_default_expr(field: &Field, default_value: &str) -> Option<Expr> {
    if default_value != "{}" {
        return None;
    }
    let DataType::Map(entry_field, _) = field.data_type() else {
        return None;
    };
    let DataType::Struct(fields) = entry_field.data_type() else {
        return None;
    };
    let mut iter = fields.iter();
    let key_field = iter.next()?;
    let value_field = iter.next()?;
    if key_field.data_type() != &DataType::Utf8 || value_field.data_type() != &DataType::Utf8 {
        return None;
    }
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new())
        .with_keys_field(key_field.clone())
        .with_values_field(value_field.clone());
    if builder.append(true).is_err() {
        return None;
    }
    let array = builder.finish();
    Some(Expr::Literal(ScalarValue::Map(Arc::new(array)), None))
}

fn column_default_exprs(schema: &SchemaRef) -> HashMap<String, Expr> {
    let mut defaults = HashMap::new();
    for field in schema.fields() {
        let meta = field.metadata();
        let Some(default_value) = meta.get("default_value") else {
            continue;
        };
        let expr = if let Some(expr) = map_default_expr(field, default_value) {
            Some(expr)
        } else if field.data_type() == &DataType::Utf8 {
            Some(Expr::Literal(
                ScalarValue::Utf8(Some(default_value.clone())),
                None,
            ))
        } else {
            None
        };
        if let Some(expr) = expr {
            defaults.insert(field.name().to_string(), expr);
        }
    }
    defaults
}

fn constraints_from_key_fields(
    schema: &SchemaRef,
    key_fields: &[String],
) -> Result<Option<Constraints>> {
    if key_fields.is_empty() {
        return Ok(None);
    }
    let mut indices = Vec::with_capacity(key_fields.len());
    for name in key_fields {
        let index = schema.index_of(name).map_err(|_| {
            DataFusionError::Plan(format!("Unknown key field in listing provider: {name}"))
        })?;
        indices.push(index);
    }
    Ok(Some(Constraints::new_unverified(vec![
        Constraint::PrimaryKey(indices),
    ])))
}

fn file_sort_order_from_columns(columns: &[String]) -> Vec<Vec<SortExpr>> {
    if columns.is_empty() {
        return Vec::new();
    }
    let sorts = columns
        .iter()
        .map(|name| SortExpr::new(col(name.as_str()), true, true))
        .collect();
    vec![sorts]
}

fn listing_table_plan(
    schema: SchemaRef,
    table_name: &str,
    location: &str,
    table_partition_cols: Vec<String>,
    definition: Option<String>,
    column_defaults: HashMap<String, Expr>,
    constraints: Constraints,
    file_sort_order: Vec<Vec<SortExpr>>,
) -> Result<LogicalPlan> {
    let dfschema = DFSchema::try_from(schema)?;
    let create = CreateExternalTable {
        schema: Arc::new(dfschema),
        name: table_name.into(),
        location: location.to_string(),
        file_type: "PARQUET".to_string(),
        table_partition_cols,
        if_not_exists: false,
        or_replace: false,
        temporary: false,
        definition,
        order_exprs: file_sort_order,
        unbounded: false,
        options: HashMap::new(),
        constraints,
        column_defaults,
    };
    Ok(LogicalPlan::Ddl(DdlStatement::CreateExternalTable(create)))
}

#[pyfunction]
fn schema_evolution_adapter_factory(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let factory: Arc<dyn PhysicalExprAdapterFactory> =
        Arc::new(SchemaEvolutionAdapterFactory::default());
    let name = CString::new("datafusion_ext.SchemaEvolutionAdapterFactory")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, factory, Some(name))?;
    Ok(capsule.unbind().into())
}

#[pyfunction]
fn parquet_listing_table_provider(
    py: Python<'_>,
    path: String,
    file_extension: String,
    table_name: String,
    table_definition: Option<String>,
    table_partition_cols: Option<Vec<(String, String)>>,
    schema_ipc: Option<Vec<u8>>,
    partition_schema_ipc: Option<Vec<u8>>,
    file_sort_order: Option<Vec<String>>,
    key_fields: Option<Vec<String>>,
    expr_adapter_factory: Option<Py<PyAny>>,
    parquet_pruning: Option<bool>,
    skip_metadata: Option<bool>,
    collect_statistics: Option<bool>,
) -> PyResult<Py<PyAny>> {
    let schema_ipc = schema_ipc.ok_or_else(|| {
        PyValueError::new_err("Parquet listing table provider requires schema_ipc.")
    })?;
    let schema = schema_from_ipc(schema_ipc)?;
    let mut format = ParquetFormat::default();
    if let Some(enable_pruning) = parquet_pruning {
        format = format.with_enable_pruning(enable_pruning);
    }
    if let Some(skip) = skip_metadata {
        format = format.with_skip_metadata(skip);
    }
    let mut options = ListingOptions::new(Arc::new(format)).with_file_extension(file_extension);
    if let Some(collect) = collect_statistics {
        options = options.with_collect_stat(collect);
    }
    if let Some(schema_ipc) = partition_schema_ipc {
        let schema = schema_from_ipc(schema_ipc)?;
        let mut cols = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            cols.push((field.name().to_string(), field.data_type().clone()));
        }
        options = options.with_table_partition_cols(cols);
    } else if let Some(table_partition_cols) = table_partition_cols {
        let mut cols = Vec::with_capacity(table_partition_cols.len());
        for (name, dtype) in table_partition_cols {
            let parsed: DataType = DataType::from_str(&dtype).map_err(|err| {
                PyValueError::new_err(format!(
                    "Unsupported partition column type {dtype:?}: {err}"
                ))
            })?;
            cols.push((name, parsed));
        }
        options = options.with_table_partition_cols(cols);
    }
    let sort_columns = file_sort_order.unwrap_or_default();
    let sort_exprs = file_sort_order_from_columns(&sort_columns);
    if !sort_exprs.is_empty() {
        options = options.with_file_sort_order(sort_exprs.clone());
    }
    let table_path = ListingTableUrl::parse(path.as_str())
        .or_else(|_| ListingTableUrl::parse(format!("file://{path}").as_str()));
    let table_path = table_path.map_err(|err| {
        PyValueError::new_err(format!("Invalid listing table path {path:?}: {err}"))
    })?;
    let table_partition_names: Vec<String> = options
        .table_partition_cols
        .iter()
        .map(|col| col.0.clone())
        .collect();
    let adapter_factory = if let Some(factory_obj) = expr_adapter_factory {
        let capsule: Bound<'_, PyCapsule> = factory_obj.extract(py)?;
        let factory: &Arc<dyn PhysicalExprAdapterFactory> = unsafe { capsule.reference() };
        factory.clone()
    } else {
        Arc::new(SchemaEvolutionAdapterFactory::default())
    };
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(schema.clone())
        .with_expr_adapter_factory(adapter_factory);
    let provider = ListingTable::try_new(config).map_err(|err| {
        PyRuntimeError::new_err(format!("ListingTable provider build failed: {err}"))
    })?;
    let defaults = column_default_exprs(&provider.schema());
    let constraints = match key_fields {
        Some(keys) => constraints_from_key_fields(&schema, &keys)
            .map_err(|err| PyRuntimeError::new_err(format!("Invalid key fields: {err}")))?,
        None => None,
    };
    let plan_constraints = constraints.clone().unwrap_or_default();
    let plan = listing_table_plan(
        provider.schema(),
        &table_name,
        &path,
        table_partition_names,
        table_definition.clone(),
        defaults.clone(),
        plan_constraints,
        sort_exprs,
    )
    .map_err(|err| PyRuntimeError::new_err(format!("ListingTable plan build failed: {err}")))?;
    let wrapped = CpgTableProvider::new(
        Arc::new(provider),
        table_definition,
        Some(plan),
        defaults,
        constraints,
    );
    let ffi_provider = FFI_TableProvider::new(Arc::new(wrapped), true, None);
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, ffi_provider, Some(name))?;
    Ok(capsule.unbind().into())
}

#[pyfunction]
fn delta_table_provider_with_files(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
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
    let (provider, snapshot, scan_config, add_actions) = runtime
        .block_on(delta_provider_with_files_native(
            &ctx.ctx, &table_uri, storage, version, timestamp, overrides, files, gate,
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
    let add_payload = add_action_payloads(&add_actions);
    payload.set_item("add_actions", json_to_py(py, &add_payload)?)?;
    Ok(payload.into())
}

#[pyfunction]
fn install_expr_planners(ctx: PyRef<PySessionContext>, planner_names: Vec<String>) -> PyResult<()> {
    let names: Vec<&str> = planner_names.iter().map(String::as_str).collect();
    install_expr_planners_native(&ctx.ctx, &names)
        .map_err(|err| PyRuntimeError::new_err(format!("ExprPlanner install failed: {err}")))
}

#[pyfunction]
fn install_tracing(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    let state_ref = ctx.ctx.state_ref();
    let mut state = state_ref.write();
    let new_state = SessionStateBuilder::new_from_existing(state.clone())
        .with_physical_optimizer_rule(Arc::new(TracingMarkerRule))
        .build();
    *state = new_state;
    Ok(())
}

#[pyfunction]
fn register_cache_tables(
    ctx: PyRef<PySessionContext>,
    config: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let snapshot_config = CacheSnapshotConfig::from_map(config);
    register_cache_table_functions(&ctx.ctx, snapshot_config).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to register cache table functions: {err}"))
    })?;
    Ok(())
}

#[pyfunction]
fn table_logical_plan(ctx: PyRef<PySessionContext>, table_name: String) -> PyResult<String> {
    let runtime = Runtime::new()
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}")))?;
    let df = runtime
        .block_on(ctx.ctx.table(table_name.as_str()))
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to resolve table {table_name:?}: {err}"))
        })?;
    Ok(format!("{:?}", df.logical_plan()))
}

#[pyfunction]
fn table_dfschema_tree(ctx: PyRef<PySessionContext>, table_name: String) -> PyResult<String> {
    let runtime = Runtime::new()
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}")))?;
    let df = runtime
        .block_on(ctx.ctx.table(table_name.as_str()))
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to resolve table {table_name:?}: {err}"))
        })?;
    Ok(df.schema().to_string())
}

#[pyfunction]
fn install_schema_evolution_adapter_factory(_ctx: PyRef<PySessionContext>) -> PyResult<()> {
    Ok(())
}

#[pyfunction]
fn registry_catalog_provider_factory(
    py: Python<'_>,
    schema_name: Option<String>,
) -> PyResult<Py<PyAny>> {
    let schema_name = schema_name.unwrap_or_else(|| "public".to_string());
    let schema_provider: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema(&schema_name, schema_provider)
        .map_err(|err| PyRuntimeError::new_err(format!("Catalog registration failed: {err}")))?;
    let provider: Arc<dyn CatalogProvider> = catalog;
    let name = CString::new("datafusion_ext.RegistryCatalogProviderFactory")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, provider, Some(name))?;
    Ok(capsule.unbind().into())
}

#[derive(Debug)]
struct TracingMarkerRule;

impl PhysicalOptimizerRule for TracingMarkerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "codeanatomy_tracing_marker"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct CacheSnapshotConfig {
    list_files_cache_ttl: Option<String>,
    list_files_cache_limit: Option<String>,
    metadata_cache_limit: Option<String>,
    predicate_cache_size: Option<String>,
}

impl CacheSnapshotConfig {
    fn from_map(config: Option<HashMap<String, String>>) -> Self {
        let mut config_map = config.unwrap_or_default();
        Self {
            list_files_cache_ttl: config_map.remove("list_files_cache_ttl"),
            list_files_cache_limit: config_map.remove("list_files_cache_limit"),
            metadata_cache_limit: config_map.remove("metadata_cache_limit"),
            predicate_cache_size: config_map.remove("predicate_cache_size"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum CacheTableKind {
    ListFiles,
    Metadata,
    Predicate,
    Statistics,
}

#[derive(Debug)]
struct CacheTableFunction {
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    config: CacheSnapshotConfig,
    kind: CacheTableKind,
}

impl CacheTableFunction {
    fn cache_name(&self) -> &'static str {
        match self.kind {
            CacheTableKind::ListFiles => "list_files",
            CacheTableKind::Metadata => "metadata",
            CacheTableKind::Predicate => "predicate",
            CacheTableKind::Statistics => "statistics",
        }
    }

    fn config_ttl(&self) -> Option<&str> {
        match self.kind {
            CacheTableKind::ListFiles => self.config.list_files_cache_ttl.as_deref(),
            CacheTableKind::Metadata => None,
            CacheTableKind::Predicate => None,
            CacheTableKind::Statistics => None,
        }
    }

    fn config_limit(&self) -> Option<&str> {
        match self.kind {
            CacheTableKind::ListFiles => self.config.list_files_cache_limit.as_deref(),
            CacheTableKind::Metadata => self.config.metadata_cache_limit.as_deref(),
            CacheTableKind::Predicate => self.config.predicate_cache_size.as_deref(),
            CacheTableKind::Statistics => self.config.predicate_cache_size.as_deref(),
        }
    }

    fn entry_count(&self) -> Option<i64> {
        match self.kind {
            CacheTableKind::ListFiles => self
                .runtime_env
                .cache_manager
                .get_list_files_cache()
                .map(|cache| cache.len() as i64),
            CacheTableKind::Metadata => {
                let cache = self.runtime_env.cache_manager.get_file_metadata_cache();
                Some(cache.list_entries().len() as i64)
            }
            CacheTableKind::Predicate => self
                .runtime_env
                .cache_manager
                .get_file_statistic_cache()
                .map(|cache| cache.len() as i64),
            CacheTableKind::Statistics => self
                .runtime_env
                .cache_manager
                .get_file_statistic_cache()
                .map(|cache| cache.len() as i64),
        }
    }

    fn hit_count(&self) -> Option<i64> {
        if matches!(self.kind, CacheTableKind::Metadata) {
            let cache = self.runtime_env.cache_manager.get_file_metadata_cache();
            let hits: i64 = cache
                .list_entries()
                .values()
                .map(|entry| entry.hits as i64)
                .sum();
            return Some(hits);
        }
        None
    }

    fn make_table(&self) -> Result<Arc<dyn TableProvider>> {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("cache_name", DataType::Utf8, false),
            Field::new("event_time_unix_ms", DataType::Int64, false),
            Field::new("entry_count", DataType::Int64, true),
            Field::new("hit_count", DataType::Int64, true),
            Field::new("miss_count", DataType::Int64, true),
            Field::new("eviction_count", DataType::Int64, true),
            Field::new("config_ttl", DataType::Utf8, true),
            Field::new("config_limit", DataType::Utf8, true),
        ]));
        let cache_name = StringArray::from(vec![Some(self.cache_name())]);
        let event_time = Int64Array::from(vec![Some(now_unix_ms())]);
        let entry_count = Int64Array::from(vec![self.entry_count()]);
        let hit_count = Int64Array::from(vec![self.hit_count()]);
        let miss_count = Int64Array::from(vec![None]);
        let eviction_count = Int64Array::from(vec![None]);
        let config_ttl = StringArray::from(vec![self.config_ttl()]);
        let config_limit = StringArray::from(vec![self.config_limit()]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(cache_name),
                Arc::new(event_time),
                Arc::new(entry_count),
                Arc::new(hit_count),
                Arc::new(miss_count),
                Arc::new(eviction_count),
                Arc::new(config_ttl),
                Arc::new(config_limit),
            ],
        )
        .map_err(|err| DataFusionError::Plan(format!("Failed to build cache table: {err}")))?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Arc::new(table))
    }
}

impl TableFunctionImpl for CacheTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return Err(DataFusionError::Plan(
                "Cache table functions do not accept arguments.".into(),
            ));
        }
        self.make_table()
    }
}

fn register_cache_table_functions(ctx: &SessionContext, config: CacheSnapshotConfig) -> Result<()> {
    let runtime_env = Arc::clone(ctx.state().runtime_env());
    let list_files = CacheTableFunction {
        runtime_env: Arc::clone(&runtime_env),
        config: config.clone(),
        kind: CacheTableKind::ListFiles,
    };
    let metadata = CacheTableFunction {
        runtime_env: Arc::clone(&runtime_env),
        config: config.clone(),
        kind: CacheTableKind::Metadata,
    };
    let predicate = CacheTableFunction {
        runtime_env: Arc::clone(&runtime_env),
        config: config.clone(),
        kind: CacheTableKind::Predicate,
    };
    let statistics = CacheTableFunction {
        runtime_env,
        config,
        kind: CacheTableKind::Statistics,
    };
    ctx.register_udtf("list_files_cache", Arc::new(list_files));
    ctx.register_udtf("metadata_cache", Arc::new(metadata));
    ctx.register_udtf("predicate_cache", Arc::new(predicate));
    ctx.register_udtf("statistics_cache", Arc::new(statistics));
    Ok(())
}

// Scope 1: Delta SQL DDL registration via DeltaTableFactory
#[pyfunction]
fn install_delta_table_factory(ctx: PyRef<PySessionContext>, alias: String) -> PyResult<()> {
    let state_ref = ctx.ctx.state_ref();
    let mut state = state_ref.write();
    let factories = state.table_factories_mut();
    factories.insert(alias, Arc::new(DeltaTableFactory {}));
    Ok(())
}

#[pyclass]
#[derive(Clone)]
struct DeltaRuntimeEnvOptions {
    #[pyo3(get, set)]
    max_spill_size: Option<usize>,
    #[pyo3(get, set)]
    max_temp_directory_size: Option<u64>,
}

#[pymethods]
impl DeltaRuntimeEnvOptions {
    #[new]
    fn new() -> Self {
        Self {
            max_spill_size: None,
            max_temp_directory_size: None,
        }
    }
}

#[pyfunction]
#[pyo3(signature = (settings = None, runtime = None, delta_runtime = None))]
fn delta_session_context(
    settings: Option<Vec<(String, String)>>,
    runtime: Option<PyRef<PyRuntimeEnvBuilder>>,
    delta_runtime: Option<PyRef<DeltaRuntimeEnvOptions>>,
) -> PyResult<PySessionContext> {
    let mut config: SessionConfig = DeltaSessionConfig::default().into();
    if let Some(entries) = settings {
        for (key, value) in entries {
            config = config.set_str(&key, &value);
        }
    }
    let runtime_env = if let Some(options) = delta_runtime {
        let mut builder = DeltaRuntimeEnvBuilder::new();
        if let Some(size) = options.max_spill_size {
            builder = builder.with_max_spill_size(size);
        }
        if let Some(size) = options.max_temp_directory_size {
            builder = builder.with_max_temp_directory_size(size);
        }
        builder.build()
    } else {
        let runtime_builder = runtime
            .map(|builder| builder.builder.clone())
            .unwrap_or_else(RuntimeEnvBuilder::new);
        Arc::new(
            runtime_builder
                .build()
                .map_err(|err| PyRuntimeError::new_err(format!("RuntimeEnv build failed: {err}")))?,
        )
    };
    let planner = DeltaPlanner::new();
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_runtime_env(runtime_env)
        .with_query_planner(planner)
        .build();
    Ok(PySessionContext {
        ctx: SessionContext::new_with_state(state),
    })
}

// Scope 3: Install Delta logical/physical plan codecs via SessionConfig extensions
fn install_delta_plan_codecs_inner(ctx: &PySessionContext) -> PyResult<()> {
    let state_ref = ctx.ctx.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    config.set_extension(Arc::new(DeltaLogicalCodec {}));
    config.set_extension(Arc::new(DeltaPhysicalCodec {}));
    Ok(())
}

// Scope 3: Install Delta logical/physical plan codecs via SessionConfig extensions
#[pyfunction]
fn install_delta_plan_codecs(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    install_delta_plan_codecs_inner(&ctx)
}

// Scope 8: Native Delta CDF TableProvider integration
#[pyclass]
#[derive(Clone)]
struct DeltaCdfOptions {
    #[pyo3(get, set)]
    starting_version: Option<i64>,
    #[pyo3(get, set)]
    ending_version: Option<i64>,
    #[pyo3(get, set)]
    starting_timestamp: Option<String>,
    #[pyo3(get, set)]
    ending_timestamp: Option<String>,
    #[pyo3(get, set)]
    allow_out_of_range: bool,
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
fn delta_cdf_table_provider(
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
    let (provider, snapshot) = runtime
        .block_on(delta_cdf_provider_native(
            &table_uri,
            storage,
            version,
            timestamp,
            cdf_options,
            gate,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta CDF provider failed: {err}")))?;
    let ffi_provider = FFI_TableProvider::new(Arc::new(provider), true, None);
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, ffi_provider, Some(name))?;
    let payload = PyDict::new(py);
    payload.set_item("provider", capsule)?;
    payload.set_item("snapshot", snapshot_to_pydict(py, &snapshot)?)?;
    Ok(payload.into())
}


// Scope 9: DeltaScanConfig derived from session settings
#[pyfunction]
fn delta_table_provider_from_session(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
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
    let (provider, snapshot, scan_config, add_actions, predicate_error) = runtime
        .block_on(delta_provider_from_session_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            predicate,
            overrides,
            gate,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to build Delta provider: {err}")))?;
    let payload = PyDict::new(py);
    payload.set_item("provider", provider_capsule(py, provider)?)?;
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
fn delta_scan_config_from_session(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
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
    let session_state = ctx.ctx.state();
    let scan_config =
        delta_scan_config_from_session_native(&session_state, None, overrides).map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to resolve Delta scan config from session: {err}"
            ))
        })?;
    scan_config_to_pydict(py, &scan_config)
}

#[pyfunction]
fn delta_snapshot_info(
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
    let snapshot = runtime
        .block_on(delta_snapshot_info_native(
            &table_uri, storage, version, timestamp, gate,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to load Delta snapshot: {err}")))?;
    snapshot_to_pydict(py, &snapshot)
}

#[pyfunction]
fn validate_protocol_gate(snapshot_msgpack: Vec<u8>, gate_msgpack: Vec<u8>) -> PyResult<()> {
    crate::delta_protocol::validate_protocol_gate_payload(&snapshot_msgpack, &gate_msgpack)
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Delta protocol gate validation failed: {err}"))
        })
}

#[pyfunction]
fn delta_add_actions(
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
    let (snapshot, add_actions) = runtime
        .block_on(delta_add_actions_native(
            &table_uri, storage, version, timestamp, gate,
        ))
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to list Delta add actions: {err}"))
        })?;
    let payload = PyDict::new(py);
    payload.set_item("snapshot", snapshot_to_pydict(py, &snapshot)?)?;
    let add_payload = add_action_payloads(&add_actions);
    payload.set_item("add_actions", json_to_py(py, &add_payload)?)?;
    Ok(payload.into())
}

#[pyfunction]
fn delta_write_ipc(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    data_ipc: Vec<u8>,
    mode: String,
    schema_mode: Option<String>,
    partition_columns: Option<Vec<String>>,
    target_file_size: Option<usize>,
    extra_constraints: Option<Vec<String>>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<Py<PyAny>> {
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let save_mode = save_mode_from_str(mode.as_str())?;
    let commit_options = commit_options_from_params(
        commit_metadata,
        app_id,
        app_version,
        app_last_updated,
        max_retries,
        create_checkpoint,
    )?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_write_ipc_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            data_ipc.as_slice(),
            save_mode,
            schema_mode,
            partition_columns,
            target_file_size,
            gate,
            Some(commit_options),
            extra_constraints,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta write failed: {err}")))?;
    mutation_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_delete(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    predicate: Option<String>,
    _extra_constraints: Option<Vec<String>>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<Py<PyAny>> {
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let commit_options = commit_options_from_params(
        commit_metadata,
        app_id,
        app_version,
        app_last_updated,
        max_retries,
        create_checkpoint,
    )?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_delete_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            predicate,
            gate,
            Some(commit_options),
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta delete failed: {err}")))?;
    mutation_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_update(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    predicate: Option<String>,
    updates: Vec<(String, String)>,
    _extra_constraints: Option<Vec<String>>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<Py<PyAny>> {
    if updates.is_empty() {
        return Err(PyValueError::new_err(
            "Delta update requires at least one column assignment.",
        ));
    }
    let updates = updates.into_iter().collect::<HashMap<String, String>>();
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let commit_options = commit_options_from_params(
        commit_metadata,
        app_id,
        app_version,
        app_last_updated,
        max_retries,
        create_checkpoint,
    )?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_update_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            predicate,
            updates,
            gate,
            Some(commit_options),
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta update failed: {err}")))?;
    mutation_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_merge(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    source_table: String,
    predicate: String,
    source_alias: Option<String>,
    target_alias: Option<String>,
    matched_predicate: Option<String>,
    matched_updates: Option<Vec<(String, String)>>,
    not_matched_predicate: Option<String>,
    not_matched_inserts: Option<Vec<(String, String)>>,
    not_matched_by_source_predicate: Option<String>,
    delete_not_matched_by_source: Option<bool>,
    extra_constraints: Option<Vec<String>>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<Py<PyAny>> {
    let matched_updates = matched_updates
        .map(|entries| entries.into_iter().collect::<HashMap<String, String>>())
        .unwrap_or_default();
    let not_matched_inserts = not_matched_inserts
        .map(|entries| entries.into_iter().collect::<HashMap<String, String>>())
        .unwrap_or_default();
    let delete_not_matched_by_source = delete_not_matched_by_source.unwrap_or(false);
    if matched_updates.is_empty() && not_matched_inserts.is_empty() && !delete_not_matched_by_source
    {
        return Err(PyValueError::new_err(
            "Delta merge requires a matched update, not-matched insert, or not-matched-by-source delete.",
        ));
    }
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let commit_options = commit_options_from_params(
        commit_metadata,
        app_id,
        app_version,
        app_last_updated,
        max_retries,
        create_checkpoint,
    )?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_merge_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            &source_table,
            predicate,
            source_alias,
            target_alias,
            matched_predicate,
            matched_updates,
            not_matched_predicate,
            not_matched_inserts,
            not_matched_by_source_predicate,
            delete_not_matched_by_source,
            gate,
            Some(commit_options),
            extra_constraints,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta merge failed: {err}")))?;
    mutation_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_optimize_compact(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    target_size: Option<i64>,
    _max_concurrent_tasks: Option<i64>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<Py<PyAny>> {
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let commit_options = commit_options_from_params(
        commit_metadata,
        app_id,
        app_version,
        app_last_updated,
        max_retries,
        create_checkpoint,
    )?;
    let target_size = match target_size {
        Some(size) => Some(u64::try_from(size).map_err(|err| {
            PyValueError::new_err(format!("Delta optimize target_size is too large: {err}"))
        })?),
        None => None,
    };
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_optimize_compact_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            target_size,
            gate,
            Some(commit_options),
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta optimize failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_vacuum(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    retention_hours: Option<i64>,
    dry_run: Option<bool>,
    enforce_retention_duration: Option<bool>,
    require_vacuum_protocol_check: Option<bool>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<Py<PyAny>> {
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let commit_options = commit_options_from_params(
        commit_metadata,
        app_id,
        app_version,
        app_last_updated,
        max_retries,
        create_checkpoint,
    )?;
    let dry_run = dry_run.unwrap_or(false);
    let enforce_retention_duration = enforce_retention_duration.unwrap_or(true);
    let require_vacuum_protocol_check = require_vacuum_protocol_check.unwrap_or(false);
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_vacuum_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            retention_hours,
            dry_run,
            enforce_retention_duration,
            require_vacuum_protocol_check,
            gate,
            Some(commit_options),
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta vacuum failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_restore(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    restore_version: Option<i64>,
    restore_timestamp: Option<String>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<Py<PyAny>> {
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let commit_options = commit_options_from_params(
        commit_metadata,
        app_id,
        app_version,
        app_last_updated,
        max_retries,
        create_checkpoint,
    )?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_restore_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            restore_version,
            restore_timestamp,
            gate,
            Some(commit_options),
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta restore failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_set_properties(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    properties: Vec<(String, String)>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<Py<PyAny>> {
    if properties.is_empty() {
        return Err(PyValueError::new_err(
            "Delta property update requires at least one key/value pair.",
        ));
    }
    let properties = properties.into_iter().collect::<HashMap<String, String>>();
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let commit_options = commit_options_from_params(
        commit_metadata,
        app_id,
        app_version,
        app_last_updated,
        max_retries,
        create_checkpoint,
    )?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_set_properties_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            properties,
            gate,
            Some(commit_options),
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta set-properties failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_add_features(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    features: Vec<String>,
    allow_protocol_versions_increase: Option<bool>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<Py<PyAny>> {
    if features.is_empty() {
        return Err(PyValueError::new_err(
            "Delta add-features requires at least one feature name.",
        ));
    }
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let commit_options = commit_options_from_params(
        commit_metadata,
        app_id,
        app_version,
        app_last_updated,
        max_retries,
        create_checkpoint,
    )?;
    let allow_protocol_versions_increase = allow_protocol_versions_increase.unwrap_or(true);
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_add_features_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            features,
            allow_protocol_versions_increase,
            gate,
            Some(commit_options),
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta add-features failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_add_constraints(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    constraints: Vec<(String, String)>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<Py<PyAny>> {
    if constraints.is_empty() {
        return Err(PyValueError::new_err(
            "Delta add-constraints requires at least one constraint.",
        ));
    }
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let commit_options = commit_options_from_params(
        commit_metadata,
        app_id,
        app_version,
        app_last_updated,
        max_retries,
        create_checkpoint,
    )?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_add_constraints_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            constraints,
            gate,
            Some(commit_options),
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta add-constraints failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_drop_constraints(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    constraints: Vec<String>,
    raise_if_not_exists: Option<bool>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<i64>,
    create_checkpoint: Option<bool>,
) -> PyResult<Py<PyAny>> {
    if constraints.is_empty() {
        return Err(PyValueError::new_err(
            "Delta drop-constraints requires at least one constraint name.",
        ));
    }
    let storage = storage_options_map(storage_options);
    let gate = delta_gate_from_params(
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    );
    let commit_options = commit_options_from_params(
        commit_metadata,
        app_id,
        app_version,
        app_last_updated,
        max_retries,
        create_checkpoint,
    )?;
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_drop_constraints_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            constraints,
            raise_if_not_exists.unwrap_or(true),
            gate,
            Some(commit_options),
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta drop-constraints failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_create_checkpoint(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
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
    let report = runtime
        .block_on(delta_create_checkpoint_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            gate,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta checkpoint failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_cleanup_metadata(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
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
    let report = runtime
        .block_on(delta_cleanup_metadata_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            gate,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta metadata cleanup failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

// Scope 2: DeltaDataChecker integration
#[pyfunction]
fn delta_data_checker(
    ctx: PyRef<PySessionContext>,
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
    runtime
        .block_on(delta_data_check_native(
            &ctx.ctx,
            &table_uri,
            storage,
            version,
            timestamp,
            gate,
            data_ipc.as_slice(),
            extra_constraints,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta data check failed: {err}")))
}

pub fn init_module(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_pyfunctions!(module, [
        crate::codeanatomy_ext::capabilities_snapshot,
        crate::codeanatomy_ext::install_function_factory,
        crate::codeanatomy_ext::udf_expr,
    ]);
    register_pyfunctions!(module, [
        crate::codeanatomy_ext::install_codeanatomy_udf_config,
        crate::codeanatomy_ext::install_codeanatomy_policy_config,
        crate::codeanatomy_ext::install_codeanatomy_physical_config,
        crate::codeanatomy_ext::install_planner_rules,
        crate::codeanatomy_ext::install_physical_rules,
        crate::codeanatomy_ext::register_codeanatomy_udfs,
        crate::codeanatomy_ext::registry_snapshot_py,
        crate::codeanatomy_ext::udf_docs_snapshot,
        crate::codeanatomy_ext::load_df_plugin,
        crate::codeanatomy_ext::register_df_plugin_udfs,
        crate::codeanatomy_ext::register_df_plugin_table_functions,
        crate::codeanatomy_ext::create_df_plugin_table_provider,
        crate::codeanatomy_ext::register_df_plugin_table_providers,
        crate::codeanatomy_ext::register_df_plugin,
        crate::codeanatomy_ext::plugin_library_path,
        crate::codeanatomy_ext::plugin_manifest,
        crate::codeanatomy_ext::schema_evolution_adapter_factory,
        crate::codeanatomy_ext::parquet_listing_table_provider,
        crate::codeanatomy_ext::delta_table_provider_with_files,
        crate::codeanatomy_ext::install_expr_planners,
        crate::codeanatomy_ext::install_tracing,
        crate::codeanatomy_ext::register_cache_tables,
        crate::codeanatomy_ext::table_logical_plan,
        crate::codeanatomy_ext::table_dfschema_tree,
        crate::codeanatomy_ext::install_schema_evolution_adapter_factory,
        crate::codeanatomy_ext::registry_catalog_provider_factory,
        crate::codeanatomy_ext::install_delta_table_factory,
        crate::codeanatomy_ext::delta_session_context,
        crate::codeanatomy_ext::install_delta_plan_codecs,
        crate::codeanatomy_ext::delta_cdf_table_provider,
        crate::codeanatomy_ext::delta_snapshot_info,
        crate::codeanatomy_ext::validate_protocol_gate,
        crate::codeanatomy_ext::delta_add_actions,
        crate::codeanatomy_ext::delta_table_provider_from_session,
        crate::codeanatomy_ext::delta_scan_config_from_session,
        crate::codeanatomy_ext::delta_data_checker,
        crate::codeanatomy_ext::delta_write_ipc,
        crate::codeanatomy_ext::delta_delete,
        crate::codeanatomy_ext::delta_update,
        crate::codeanatomy_ext::delta_merge,
        crate::codeanatomy_ext::delta_optimize_compact,
        crate::codeanatomy_ext::delta_vacuum,
        crate::codeanatomy_ext::delta_restore,
        crate::codeanatomy_ext::delta_set_properties,
        crate::codeanatomy_ext::delta_add_features,
        crate::codeanatomy_ext::delta_add_constraints,
        crate::codeanatomy_ext::delta_drop_constraints,
        crate::codeanatomy_ext::delta_create_checkpoint,
        crate::codeanatomy_ext::delta_cleanup_metadata,
    ]);
    register_pyclasses!(module, [
        DeltaCdfOptions,
        DeltaRuntimeEnvOptions,
    ]);
    Ok(())
}

pub fn init_internal_module(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_pyfunctions!(module, [
        crate::codeanatomy_ext::install_function_factory,
        crate::codeanatomy_ext::capabilities_snapshot,
        crate::codeanatomy_ext::arrow_stream_to_batches,
        crate::codeanatomy_ext::udf_expr,
        crate::codeanatomy_ext::install_codeanatomy_udf_config,
        crate::codeanatomy_ext::install_codeanatomy_policy_config,
        crate::codeanatomy_ext::install_codeanatomy_physical_config,
        crate::codeanatomy_ext::install_expr_planners,
        crate::codeanatomy_ext::install_delta_table_factory,
        crate::codeanatomy_ext::delta_session_context,
        crate::codeanatomy_ext::install_delta_plan_codecs,
        crate::codeanatomy_ext::delta_cdf_table_provider,
        crate::codeanatomy_ext::delta_snapshot_info,
        crate::codeanatomy_ext::validate_protocol_gate,
        crate::codeanatomy_ext::delta_add_actions,
        crate::codeanatomy_ext::delta_table_provider_from_session,
        crate::codeanatomy_ext::delta_table_provider_with_files,
        crate::codeanatomy_ext::delta_scan_config_from_session,
        crate::codeanatomy_ext::delta_data_checker,
        crate::codeanatomy_ext::delta_write_ipc,
        crate::codeanatomy_ext::delta_delete,
        crate::codeanatomy_ext::delta_update,
        crate::codeanatomy_ext::delta_merge,
        crate::codeanatomy_ext::delta_optimize_compact,
        crate::codeanatomy_ext::delta_vacuum,
        crate::codeanatomy_ext::delta_restore,
        crate::codeanatomy_ext::delta_set_properties,
        crate::codeanatomy_ext::delta_add_features,
        crate::codeanatomy_ext::delta_add_constraints,
        crate::codeanatomy_ext::delta_drop_constraints,
        crate::codeanatomy_ext::delta_create_checkpoint,
        crate::codeanatomy_ext::delta_cleanup_metadata,
        crate::codeanatomy_ext::create_df_plugin_table_provider,
        crate::codeanatomy_ext::plugin_library_path,
        crate::codeanatomy_ext::plugin_manifest,
    ]);
    register_pyclasses!(module, [
        DeltaCdfOptions,
        DeltaRuntimeEnvOptions,
    ]);
    Ok(())
}
