//! DataFusion extension for native function registration.

mod delta_control_plane;
mod delta_maintenance;
mod delta_mutations;
mod delta_observability;
mod delta_protocol;
mod expr_planner;
mod function_factory;
mod function_rewrite;
pub mod registry_snapshot;
mod udaf_builtin;
#[cfg(feature = "async-udf")]
mod udf_async;
mod udf_builtin;
mod udf_custom;
mod udf_docs;
pub mod udf_registry;
mod udtf_builtin;
mod udtf_external;
mod udwf_builtin;

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::CString;
use std::io::Cursor;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Int64Array, MapBuilder, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, Session,
    TableFunctionImpl, TableProvider,
};
use datafusion::config::ConfigOptions;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
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
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{
    CreateExternalTable, DdlStatement, Expr, LogicalPlan, SortExpr, TableProviderFilterPushDown,
    TableType,
};
use datafusion_ffi::table_provider::FFI_TableProvider;
use delta_control_plane::{
    delta_add_actions as delta_add_actions_native,
    delta_cdf_provider as delta_cdf_provider_native,
    delta_provider_from_session as delta_provider_from_session_native,
    delta_provider_with_files as delta_provider_with_files_native,
    scan_config_from_session as delta_scan_config_from_session_native,
    snapshot_info_with_gate as delta_snapshot_info_native, DeltaCdfScanOptions,
    DeltaScanOverrides,
};
use delta_maintenance::{
    delta_add_features as delta_add_features_native,
    delta_optimize_compact as delta_optimize_compact_native, delta_restore as delta_restore_native,
    delta_set_properties as delta_set_properties_native, delta_vacuum as delta_vacuum_native,
    DeltaMaintenanceReport,
};
use delta_mutations::{
    delta_data_check as delta_data_check_native, delta_delete as delta_delete_native,
    delta_merge as delta_merge_native, delta_update as delta_update_native,
    delta_write_ipc as delta_write_ipc_native, DeltaAppTransaction, DeltaCommitOptions,
    DeltaMutationReport,
};
use delta_observability::{
    add_action_payloads, maintenance_report_payload, mutation_report_payload, scan_config_payload,
    scan_config_schema_ipc, snapshot_payload,
};
use delta_protocol::{gate_from_parts, DeltaFeatureGate, DeltaSnapshotInfo};
use df_plugin_host::{load_plugin, PluginHandle};
use serde_json::Value as JsonValue;

pub fn install_sql_macro_factory_native(ctx: &SessionContext) -> Result<()> {
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    let new_state = function_factory::with_sql_macro_factory(&state);
    *state = new_state;
    Ok(())
}

pub fn install_expr_planners_native(ctx: &SessionContext, planner_names: &[&str]) -> Result<()> {
    if planner_names.is_empty() {
        return Err(DataFusionError::Plan(
            "ExprPlanner installation requires at least one planner name.".into(),
        ));
    }
    let mut unknown: Vec<String> = Vec::new();
    let mut install_domain = false;
    for name in planner_names {
        match *name {
            "codeanatomy_domain" => {
                install_domain = true;
            }
            _ => unknown.push((*name).to_string()),
        }
    }
    if !unknown.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Unsupported ExprPlanner names: {}",
            unknown.join(", ")
        )));
    }
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    state.register_expr_planner(Arc::new(
        datafusion_functions_nested::planner::NestedFunctionPlanner,
    ))?;
    state.register_expr_planner(Arc::new(
        datafusion_functions_nested::planner::FieldAccessPlanner,
    ))?;
    if install_domain {
        state.register_expr_planner(Arc::new(expr_planner::CodeAnatomyDomainPlanner::default()))?;
        state.register_function_rewrite(Arc::new(
            function_rewrite::CodeAnatomyOperatorRewrite::default(),
        ))?;
    }
    Ok(())
}
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::expr_fn::col;
use datafusion_python::context::PySessionContext;
use deltalake::delta_datafusion::{
    DeltaLogicalCodec, DeltaPhysicalCodec, DeltaScanConfig, DeltaTableFactory, DeltaTableProvider,
};
use deltalake::protocol::SaveMode;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyBytes, PyCapsule, PyCapsuleMethods, PyDict, PyFloat, PyInt, PyList, PyString,
};
use tokio::runtime::Runtime;

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
    max_retries: Option<usize>,
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
    Ok(handle.clone())
}

#[pyfunction]
#[pyo3(signature = (ctx, enable_async = false, async_udf_timeout_ms = None, async_udf_batch_size = None))]
fn register_udfs(
    ctx: PyRef<PySessionContext>,
    enable_async: bool,
    async_udf_timeout_ms: Option<u64>,
    async_udf_batch_size: Option<usize>,
) -> PyResult<()> {
    udf_registry::register_all(
        &ctx.ctx,
        enable_async,
        async_udf_timeout_ms,
        async_udf_batch_size,
    )
    .map_err(|err| PyRuntimeError::new_err(format!("Failed to register UDFs: {err}")))?;
    Ok(())
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
    payload.set_item("custom_udfs", PyList::new(py, snapshot.custom_udfs)?)?;
    payload.set_item("pycapsule_udfs", PyList::empty(py))?;
    Ok(payload.into())
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
fn register_df_plugin_udfs(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    plugin: Py<PyAny>,
) -> PyResult<()> {
    let handle = extract_plugin_handle(py, &plugin)?;
    handle
        .register_udfs(&ctx.ctx)
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
fn register_df_plugin_table_providers(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    plugin: Py<PyAny>,
    table_names: Option<Vec<String>>,
    options_json: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let handle = extract_plugin_handle(py, &plugin)?;
    handle
        .register_table_providers(&ctx.ctx, table_names.as_deref(), options_json.as_ref())
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
        .register_udfs(&ctx.ctx)
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to register plugin UDFs: {err}")))?;
    handle.register_table_functions(&ctx.ctx).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to register plugin table functions: {err}"))
    })?;
    handle
        .register_table_providers(&ctx.ctx, table_names.as_deref(), options_json.as_ref())
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to register plugin table providers: {err}"))
        })?;
    Ok(())
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

// Scope 4: Apply Delta session defaults to existing SessionContext
#[pyfunction]
fn apply_delta_session_defaults(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    let state_ref = ctx.ctx.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    config.options_mut().sql_parser.enable_ident_normalization = false;
    Ok(())
}

// Scope 3: Install Delta logical/physical plan codecs via SessionConfig extensions
#[pyfunction]
fn install_delta_plan_codecs(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    let state_ref = ctx.ctx.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    config.set_extension(Arc::new(DeltaLogicalCodec {}));
    config.set_extension(Arc::new(DeltaPhysicalCodec {}));
    Ok(())
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
    let (provider, snapshot, scan_config) = runtime
        .block_on(delta_provider_from_session_native(
            &ctx.ctx, &table_uri, storage, version, timestamp, overrides, gate,
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to build Delta provider: {err}")))?;
    let payload = PyDict::new(py);
    payload.set_item("provider", provider_capsule(py, provider)?)?;
    payload.set_item("snapshot", snapshot_to_pydict(py, &snapshot)?)?;
    payload.set_item("scan_config", scan_config_to_pydict(py, &scan_config)?)?;
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
    let scan_config = delta_scan_config_from_session_native(&session_state, overrides);
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
    max_retries: Option<usize>,
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
    _ctx: PyRef<PySessionContext>,
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
    max_retries: Option<usize>,
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
    max_retries: Option<usize>,
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
    max_retries: Option<usize>,
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
    _ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    target_size: Option<usize>,
    _max_concurrent_tasks: Option<usize>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<usize>,
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
    _ctx: PyRef<PySessionContext>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    retention_hours: Option<i64>,
    dry_run: Option<bool>,
    enforce_retention_duration: Option<bool>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    commit_metadata: Option<Vec<(String, String)>>,
    app_id: Option<String>,
    app_version: Option<i64>,
    app_last_updated: Option<i64>,
    max_retries: Option<usize>,
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
    let runtime = runtime()?;
    let report = runtime
        .block_on(delta_vacuum_native(
            &table_uri,
            storage,
            version,
            timestamp,
            retention_hours,
            dry_run,
            enforce_retention_duration,
            gate,
            Some(commit_options),
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Delta vacuum failed: {err}")))?;
    maintenance_report_to_pydict(py, &report)
}

#[pyfunction]
fn delta_restore(
    py: Python<'_>,
    _ctx: PyRef<PySessionContext>,
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
    max_retries: Option<usize>,
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
    _ctx: PyRef<PySessionContext>,
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
    max_retries: Option<usize>,
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
    _ctx: PyRef<PySessionContext>,
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
    max_retries: Option<usize>,
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

#[pymodule]
fn datafusion_ext(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(
        udf_custom::install_function_factory,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(register_udfs, module)?)?;
    module.add_function(wrap_pyfunction!(registry_snapshot_py, module)?)?;
    module.add_function(wrap_pyfunction!(udf_docs_snapshot, module)?)?;
    module.add_function(wrap_pyfunction!(load_df_plugin, module)?)?;
    module.add_function(wrap_pyfunction!(register_df_plugin_udfs, module)?)?;
    module.add_function(wrap_pyfunction!(
        register_df_plugin_table_functions,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        register_df_plugin_table_providers,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(register_df_plugin, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::map_entries, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::map_keys, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::map_values, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::map_extract, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::list_extract, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::list_unique, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::first_value_agg, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::last_value_agg, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::count_distinct_agg, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::string_agg, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::row_number_window, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::lag_window, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::lead_window, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::arrow_metadata, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::union_tag, module)?)?;
    module.add_function(wrap_pyfunction!(udf_builtin::union_extract, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::stable_hash64, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::stable_hash128, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::prefixed_hash64, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::stable_id, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::stable_id_parts, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::prefixed_hash_parts64, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::stable_hash_any, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::span_make, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::span_len, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::span_overlaps, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::span_contains, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::span_id, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::utf8_normalize, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::utf8_null_if_blank, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::qname_normalize, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::map_get_default, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::map_normalize, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::list_compact, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::list_unique_sorted, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::struct_pick, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::cdf_change_rank, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::cdf_is_upsert, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::cdf_is_delete, module)?)?;
    module.add_function(wrap_pyfunction!(udf_custom::col_to_byte, module)?)?;
    module.add_function(wrap_pyfunction!(schema_evolution_adapter_factory, module)?)?;
    module.add_function(wrap_pyfunction!(parquet_listing_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(delta_table_provider_with_files, module)?)?;
    module.add_function(wrap_pyfunction!(install_expr_planners, module)?)?;
    module.add_function(wrap_pyfunction!(install_tracing, module)?)?;
    module.add_function(wrap_pyfunction!(register_cache_tables, module)?)?;
    module.add_function(wrap_pyfunction!(table_logical_plan, module)?)?;
    module.add_function(wrap_pyfunction!(table_dfschema_tree, module)?)?;
    module.add_function(wrap_pyfunction!(
        install_schema_evolution_adapter_factory,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(registry_catalog_provider_factory, module)?)?;
    module.add_class::<DeltaCdfOptions>()?;
    module.add_function(wrap_pyfunction!(install_delta_table_factory, module)?)?;
    module.add_function(wrap_pyfunction!(apply_delta_session_defaults, module)?)?;
    module.add_function(wrap_pyfunction!(install_delta_plan_codecs, module)?)?;
    module.add_function(wrap_pyfunction!(delta_cdf_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(delta_snapshot_info, module)?)?;
    module.add_function(wrap_pyfunction!(delta_add_actions, module)?)?;
    module.add_function(wrap_pyfunction!(delta_table_provider_from_session, module)?)?;
    module.add_function(wrap_pyfunction!(delta_scan_config_from_session, module)?)?;
    module.add_function(wrap_pyfunction!(delta_data_checker, module)?)?;
    module.add_function(wrap_pyfunction!(delta_write_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(delta_delete, module)?)?;
    module.add_function(wrap_pyfunction!(delta_update, module)?)?;
    module.add_function(wrap_pyfunction!(delta_merge, module)?)?;
    module.add_function(wrap_pyfunction!(delta_optimize_compact, module)?)?;
    module.add_function(wrap_pyfunction!(delta_vacuum, module)?)?;
    module.add_function(wrap_pyfunction!(delta_restore, module)?)?;
    module.add_function(wrap_pyfunction!(delta_set_properties, module)?)?;
    module.add_function(wrap_pyfunction!(delta_add_features, module)?)?;
    Ok(())
}
