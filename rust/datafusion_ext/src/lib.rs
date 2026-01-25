//! DataFusion extension for native function registration.

mod udf_builtin;
mod udf_custom;
mod udf_docs;
mod expr_planner;
mod function_rewrite;
mod function_factory;
mod udaf_builtin;
mod udwf_builtin;
mod udtf_builtin;
pub mod udf_registry;
mod registry_snapshot;

use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
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
    CatalogProvider,
    MemoryCatalogProvider,
    MemorySchemaProvider,
    SchemaProvider,
    Session,
    TableFunctionImpl,
    TableProvider,
};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::config::ConfigOptions;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory,
    PhysicalExprAdapter,
    PhysicalExprAdapterFactory,
};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_common::{
    Constraint,
    Constraints,
    DFSchema,
    DataFusionError,
    Result,
    ScalarValue,
    Statistics,
};
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_parquet::file_format::ParquetFormat;
use datafusion_expr::{
    CreateExternalTable,
    DdlStatement,
    Expr,
    LogicalPlan,
    SortExpr,
    TableProviderFilterPushDown,
    TableType,
};
use datafusion_expr::registry::FunctionRegistry;
use df_plugin_host::{load_plugin, PluginHandle};

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
        state.register_expr_planner(Arc::new(
            expr_planner::CodeAnatomyDomainPlanner::default(),
        ))?;
        state.register_function_rewrite(Arc::new(
            function_rewrite::CodeAnatomyOperatorRewrite::default(),
        ))?;
    }
    Ok(())
}
use datafusion_expr::dml::InsertOp;
use datafusion_expr::expr_fn::col;
use datafusion::physical_plan::ExecutionPlan;
use deltalake::delta_datafusion::{
    DeltaCdfTableProvider,
    DeltaDataChecker,
    DeltaLogicalCodec,
    DeltaPhysicalCodec,
    DeltaScanConfig,
    DeltaScanConfigBuilder,
    DeltaTableFactory,
    DeltaTableProvider,
};
use deltalake::kernel::models::Add;
use deltalake::kernel::scalars::ScalarExt;
use deltalake::table::Constraint as DeltaConstraint;
use deltalake::{ensure_table_uri, DeltaTableBuilder};
use deltalake::errors::DeltaTableError;
use tokio::runtime::Runtime;
use datafusion_python::context::PySessionContext;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyCapsuleMethods, PyDict, PyList};
use chrono::{DateTime, Utc};

fn schema_from_ipc(schema_ipc: Vec<u8>) -> PyResult<SchemaRef> {
    let reader = StreamReader::try_new(Cursor::new(schema_ipc), None)
        .map_err(|err| PyValueError::new_err(format!("Failed to decode schema IPC: {err}")))?;
    Ok(reader.schema())
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

fn extract_plugin_handle(
    py: Python<'_>,
    plugin: &Py<PyAny>,
) -> PyResult<Arc<PluginHandle>> {
    let capsule: Bound<'_, PyCapsule> = plugin.bind(py).extract()?;
    let name = capsule
        .name()
        .map_err(|err| PyValueError::new_err(format!("Invalid plugin capsule: {err}")))?;
    let Some(name) = name else {
        return Err(PyValueError::new_err("Plugin capsule is missing a name."));
    };
    let name = name.to_str().map_err(|err| {
        PyValueError::new_err(format!("Invalid plugin capsule name: {err}"))
    })?;
    if name != PLUGIN_HANDLE_CAPSULE_NAME {
        return Err(PyValueError::new_err(format!(
            "Unexpected plugin capsule name: {name}"
        )));
    }
    let handle: &Arc<PluginHandle> = unsafe { capsule.reference() };
    Ok(handle.clone())
}

#[pyfunction]
fn register_udfs(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    udf_registry::register_all(&ctx.ctx)
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
    for (name, udf) in state.scalar_functions() {
        if let Some(doc) = udf.documentation() {
            add_doc(name, doc)?;
        }
    }
    for (name, udaf) in state.aggregate_functions() {
        if let Some(doc) = udaf.documentation() {
            add_doc(name, doc)?;
        }
    }
    for (name, udwf) in state.window_functions() {
        if let Some(doc) = udwf.documentation() {
            add_doc(name, doc)?;
        }
    }

    for (name, doc) in udf_docs::docs_snapshot() {
        add_doc(name, doc)?;
    }
    Ok(payload.into())
}

#[pyfunction]
fn load_df_plugin(py: Python<'_>, path: String) -> PyResult<Py<PyAny>> {
    let handle = load_plugin(Path::new(&path)).map_err(|err| {
        PyRuntimeError::new_err(format!(
            "Failed to load DataFusion plugin {path:?}: {err}"
        ))
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
    handle.register_udfs(&ctx.ctx).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to register plugin UDFs: {err}"))
    })?;
    Ok(())
}

#[pyfunction]
fn register_df_plugin_table_functions(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    plugin: Py<PyAny>,
) -> PyResult<()> {
    let handle = extract_plugin_handle(py, &plugin)?;
    handle
        .register_table_functions(&ctx.ctx)
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to register plugin table functions: {err}"
            ))
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
        .register_table_providers(
            &ctx.ctx,
            table_names.as_deref(),
            options_json.as_ref(),
        )
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to register plugin table providers: {err}"
            ))
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
    handle.register_udfs(&ctx.ctx).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to register plugin UDFs: {err}"))
    })?;
    handle
        .register_table_functions(&ctx.ctx)
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to register plugin table functions: {err}"
            ))
        })?;
    handle
        .register_table_providers(
            &ctx.ctx,
            table_names.as_deref(),
            options_json.as_ref(),
        )
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to register plugin table providers: {err}"
            ))
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
        DefaultPhysicalExprAdapterFactory
            .create(logical_file_schema, physical_file_schema)
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
        self.constraints.as_ref().or_else(|| self.inner.constraints())
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.ddl.as_deref().or_else(|| self.inner.get_table_definition())
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
    let table_path = ListingTableUrl::parse(path.as_str()).or_else(|_| {
        ListingTableUrl::parse(format!("file://{path}").as_str())
    });
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
fn delta_table_provider(
    py: Python<'_>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,
    wrap_partition_values: Option<bool>,
    schema_ipc: Option<Vec<u8>>,
) -> PyResult<Py<PyAny>> {
    let table_url = ensure_table_uri(table_uri.as_str()).map_err(|err| {
        PyValueError::new_err(format!("Invalid Delta table URI {table_uri:?}: {err}"))
    })?;
    let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta table: {err}"))
    })?;
    if let Some(options) = storage_options {
        let options: HashMap<String, String> = options.into_iter().collect();
        builder = builder.with_storage_options(options);
    }
    if let Some(version) = version {
        builder = builder.with_version(version);
    }
    if let Some(timestamp) = timestamp {
        builder = builder.with_datestring(timestamp).map_err(|err| {
            PyValueError::new_err(format!("Invalid Delta timestamp: {err}"))
        })?;
    }
    let runtime = Runtime::new().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}"))
    })?;
    let table = runtime.block_on(builder.load()).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to load Delta table: {err}"))
    })?;
    let snapshot = table.snapshot().map_err(|err| {
        PyRuntimeError::new_err(format!("Delta table snapshot unavailable: {err}"))
    })?;
    let snapshot = snapshot.snapshot().clone();
    let log_store = table.log_store();
    let mut scan_builder = DeltaScanConfigBuilder::new();
    if let Some(name) = file_column_name {
        scan_builder = scan_builder.with_file_column_name(&name);
    }
    if let Some(pushdown) = enable_parquet_pushdown {
        scan_builder = scan_builder.with_parquet_pushdown(pushdown);
    }
    if let Some(wrap_partition_values) = wrap_partition_values {
        scan_builder = scan_builder.wrap_partition_values(wrap_partition_values);
    }
    if let Some(schema_ipc) = schema_ipc {
        let schema = schema_from_ipc(schema_ipc)?;
        scan_builder = scan_builder.with_schema(schema);
    }
    let mut scan_config = scan_builder.build(&snapshot).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta scan config: {err}"))
    })?;
    if let Some(force_view) = schema_force_view_types {
        scan_config.schema_force_view_types = force_view;
    }
    let provider = DeltaTableProvider::try_new(snapshot, log_store, scan_config).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta table provider: {err}"))
    })?;
    let ffi_provider = FFI_TableProvider::new(Arc::new(provider), true, None);
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, ffi_provider, Some(name))?;
    Ok(capsule.unbind().into())
}

#[pyfunction]
fn delta_table_provider_with_files(
    py: Python<'_>,
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
) -> PyResult<Py<PyAny>> {
    let table_url = ensure_table_uri(table_uri.as_str()).map_err(|err| {
        PyValueError::new_err(format!("Invalid Delta table URI {table_uri:?}: {err}"))
    })?;
    let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta table: {err}"))
    })?;
    if let Some(options) = storage_options {
        let options: HashMap<String, String> = options.into_iter().collect();
        builder = builder.with_storage_options(options);
    }
    if let Some(version) = version {
        builder = builder.with_version(version);
    }
    if let Some(timestamp) = timestamp {
        builder = builder.with_datestring(timestamp).map_err(|err| {
            PyValueError::new_err(format!("Invalid Delta timestamp: {err}"))
        })?;
    }
    let runtime = Runtime::new().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}"))
    })?;
    let table = runtime.block_on(builder.load()).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to load Delta table: {err}"))
    })?;
    let snapshot = table.snapshot().map_err(|err| {
        PyRuntimeError::new_err(format!("Delta table snapshot unavailable: {err}"))
    })?;
    let snapshot = snapshot.snapshot().clone();
    let log_store = table.log_store();
    let mut scan_builder = DeltaScanConfigBuilder::new();
    if let Some(name) = file_column_name {
        scan_builder = scan_builder.with_file_column_name(&name);
    }
    if let Some(pushdown) = enable_parquet_pushdown {
        scan_builder = scan_builder.with_parquet_pushdown(pushdown);
    }
    if let Some(wrap_partition_values) = wrap_partition_values {
        scan_builder = scan_builder.wrap_partition_values(wrap_partition_values);
    }
    if let Some(schema_ipc) = schema_ipc {
        let schema = schema_from_ipc(schema_ipc)?;
        scan_builder = scan_builder.with_schema(schema);
    }
    let mut scan_config = scan_builder.build(&snapshot).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta scan config: {err}"))
    })?;
    if let Some(force_view) = schema_force_view_types {
        scan_config.schema_force_view_types = force_view;
    }
    let add_actions = add_actions_for_paths(&table, &files)
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to build file list: {err}")))?;
    let provider = DeltaTableProvider::try_new(snapshot, log_store, scan_config)
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to build Delta provider: {err}")))?;
    let provider = provider.with_files(add_actions);
    let ffi_provider = FFI_TableProvider::new(Arc::new(provider), true, None);
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, ffi_provider, Some(name))?;
    Ok(capsule.unbind().into())
}

#[pyfunction]
fn install_expr_planners(ctx: PyRef<PySessionContext>, planner_names: Vec<String>) -> PyResult<()> {
    let names: Vec<&str> = planner_names.iter().map(String::as_str).collect();
    install_expr_planners_native(&ctx.ctx, &names).map_err(|err| {
        PyRuntimeError::new_err(format!("ExprPlanner install failed: {err}"))
    })
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
    let runtime = Runtime::new().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}"))
    })?;
    let df = runtime
        .block_on(ctx.ctx.table(table_name.as_str()))
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to resolve table {table_name:?}: {err}"
            ))
        })?;
    Ok(format!("{:?}", df.logical_plan()))
}

#[pyfunction]
fn table_dfschema_tree(ctx: PyRef<PySessionContext>, table_name: String) -> PyResult<String> {
    let runtime = Runtime::new().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}"))
    })?;
    let df = runtime
        .block_on(ctx.ctx.table(table_name.as_str()))
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to resolve table {table_name:?}: {err}"
            ))
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

fn register_cache_table_functions(
    ctx: &SessionContext,
    config: CacheSnapshotConfig,
) -> Result<()> {
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

fn add_actions_for_paths(
    table: &deltalake::DeltaTable,
    files: &[String],
) -> Result<Vec<Add>> {
    let snapshot = table
        .snapshot()
        .map_err(|err| DataFusionError::Plan(format!("Delta snapshot unavailable: {err}")))?;
    let mut remaining: HashSet<String> = files.iter().cloned().collect();
    let mut adds: Vec<Add> = Vec::new();
    for file in snapshot.log_data().iter() {
        let path = file.path().to_string();
        if !remaining.remove(&path) {
            continue;
        }
        let partition_values = file
            .partition_values()
            .map(|data| {
                data.fields()
                    .iter()
                    .zip(data.values().iter())
                    .map(|(field, value)| {
                        let serialized = if value.is_null() {
                            None
                        } else {
                            Some(value.serialize())
                        };
                        (field.name().to_string(), serialized)
                    })
                    .collect::<HashMap<String, Option<String>>>()
            })
            .unwrap_or_default();
        adds.push(Add {
            path,
            partition_values,
            size: file.size(),
            modification_time: file.modification_time(),
            data_change: true,
            stats: file.stats(),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        });
        if remaining.is_empty() {
            break;
        }
    }
    if !remaining.is_empty() {
        let missing: Vec<String> = remaining.into_iter().collect();
        return Err(DataFusionError::Plan(format!(
            "Delta pruning file list not found in table: {missing:?}"
        )));
    }
    Ok(adds)
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
    options: DeltaCdfOptions,
) -> PyResult<Py<PyAny>> {
    let table_url = ensure_table_uri(table_uri.as_str()).map_err(|err| {
        PyValueError::new_err(format!("Invalid Delta table URI {table_uri:?}: {err}"))
    })?;

    let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta table: {err}"))
    })?;

    if let Some(opts) = storage_options {
        let opts: HashMap<String, String> = opts.into_iter().collect();
        builder = builder.with_storage_options(opts);
    }

    let runtime = Runtime::new().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}"))
    })?;

    let table = runtime.block_on(builder.load()).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to load Delta table: {err}"))
    })?;

    let mut cdf_builder = table.scan_cdf();

    if let Some(version) = options.starting_version {
        cdf_builder = cdf_builder.with_starting_version(version);
    }

    if let Some(version) = options.ending_version {
        cdf_builder = cdf_builder.with_ending_version(version);
    }

    if let Some(timestamp) = options.starting_timestamp {
        let dt = DateTime::parse_from_rfc3339(&timestamp)
            .map_err(|err| PyValueError::new_err(format!("Invalid starting timestamp: {err}")))?
            .with_timezone(&Utc);
        cdf_builder = cdf_builder.with_starting_timestamp(dt);
    }

    if let Some(timestamp) = options.ending_timestamp {
        let dt = DateTime::parse_from_rfc3339(&timestamp)
            .map_err(|err| PyValueError::new_err(format!("Invalid ending timestamp: {err}")))?
            .with_timezone(&Utc);
        cdf_builder = cdf_builder.with_ending_timestamp(dt);
    }

    if options.allow_out_of_range {
        cdf_builder = cdf_builder.with_allow_out_of_range();
    }

    // Create DeltaCdfTableProvider
    let provider = DeltaCdfTableProvider::try_new(cdf_builder).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta CDF table provider: {err}"))
    })?;

    let ffi_provider = FFI_TableProvider::new(Arc::new(provider), true, None);
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, ffi_provider, Some(name))?;
    Ok(capsule.unbind().into())
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
    // Override options applied after session defaults
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,
    wrap_partition_values: Option<bool>,
    schema_ipc: Option<Vec<u8>>,
) -> PyResult<Py<PyAny>> {
    let table_url = ensure_table_uri(table_uri.as_str()).map_err(|err| {
        PyValueError::new_err(format!("Invalid Delta table URI {table_uri:?}: {err}"))
    })?;

    let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta table: {err}"))
    })?;

    if let Some(options) = storage_options {
        let options: HashMap<String, String> = options.into_iter().collect();
        builder = builder.with_storage_options(options);
    }

    if let Some(version) = version {
        builder = builder.with_version(version);
    }

    if let Some(timestamp) = timestamp {
        builder = builder.with_datestring(timestamp).map_err(|err| {
            PyValueError::new_err(format!("Invalid Delta timestamp: {err}"))
        })?;
    }

    let runtime = Runtime::new().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}"))
    })?;

    let table = runtime.block_on(builder.load()).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to load Delta table: {err}"))
    })?;

    let snapshot = table.snapshot().map_err(|err| {
        PyRuntimeError::new_err(format!("Delta table snapshot unavailable: {err}"))
    })?;
    let snapshot = snapshot.snapshot().clone();
    let log_store = table.log_store();

    // 1. Use DeltaScanConfig::new_from_session(&ctx.ctx.state()) as base config
    let state_ref = ctx.ctx.state();
    let mut scan_config = DeltaScanConfig::new_from_session(&state_ref);

    // 2. Apply overrides on top
    if let Some(name) = file_column_name {
        scan_config.file_column_name = Some(name);
    }

    if let Some(pushdown) = enable_parquet_pushdown {
        scan_config.enable_parquet_pushdown = pushdown;
    }

    if let Some(force_view) = schema_force_view_types {
        scan_config.schema_force_view_types = force_view;
    }

    if let Some(wrap) = wrap_partition_values {
        scan_config.wrap_partition_values = wrap;
    }

    if let Some(schema_ipc) = schema_ipc {
        let schema = schema_from_ipc(schema_ipc)?;
        scan_config.schema = Some(schema);
    }

    let provider = DeltaTableProvider::try_new(snapshot, log_store, scan_config).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta table provider: {err}"))
    })?;

    let ffi_provider = FFI_TableProvider::new(Arc::new(provider), true, None);
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, ffi_provider, Some(name))?;
    Ok(capsule.unbind().into())
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
    let state_ref = ctx.ctx.state();
    let mut scan_config = DeltaScanConfig::new_from_session(&state_ref);

    if let Some(name) = file_column_name {
        scan_config.file_column_name = Some(name);
    }

    if let Some(pushdown) = enable_parquet_pushdown {
        scan_config.enable_parquet_pushdown = pushdown;
    }

    if let Some(force_view) = schema_force_view_types {
        scan_config.schema_force_view_types = force_view;
    }

    if let Some(wrap) = wrap_partition_values {
        scan_config.wrap_partition_values = wrap;
    }

    let schema_bytes = schema_ipc.clone();
    if let Some(schema_ipc) = schema_ipc {
        let schema = schema_from_ipc(schema_ipc)?;
        scan_config.schema = Some(schema);
    }

    let payload = PyDict::new(py);
    payload.set_item("file_column_name", scan_config.file_column_name)?;
    payload.set_item("enable_parquet_pushdown", scan_config.enable_parquet_pushdown)?;
    payload.set_item("schema_force_view_types", scan_config.schema_force_view_types)?;
    payload.set_item("wrap_partition_values", scan_config.wrap_partition_values)?;
    if let Some(schema_bytes) = schema_bytes {
        payload.set_item("schema_ipc", PyBytes::new(py, &schema_bytes))?;
    } else {
        payload.set_item("schema_ipc", py.None())?;
    }
    Ok(payload.into())
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
) -> PyResult<Vec<String>> {
    if version.is_some() && timestamp.is_some() {
        return Err(PyValueError::new_err(
            "Specify either version or timestamp, not both.",
        ));
    }
    let table_url = ensure_table_uri(table_uri.as_str()).map_err(|err| {
        PyValueError::new_err(format!("Invalid Delta table URI {table_uri:?}: {err}"))
    })?;
    let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta table: {err}"))
    })?;
    if let Some(options) = storage_options {
        let options: HashMap<String, String> = options.into_iter().collect();
        builder = builder.with_storage_options(options);
    }
    if let Some(version) = version {
        builder = builder.with_version(version);
    }
    if let Some(timestamp) = timestamp {
        builder = builder.with_datestring(timestamp).map_err(|err| {
            PyValueError::new_err(format!("Invalid Delta timestamp: {err}"))
        })?;
    }
    let runtime = Runtime::new().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}"))
    })?;
    let table = runtime.block_on(builder.load()).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to load Delta table: {err}"))
    })?;
    let snapshot = table.snapshot().map_err(|err| {
        PyRuntimeError::new_err(format!("Delta table snapshot unavailable: {err}"))
    })?;
    let snapshot = snapshot.snapshot().clone();
    let mut checker = DeltaDataChecker::new(&snapshot).with_session_context(ctx.ctx.clone());
    if let Some(extra_constraints) = extra_constraints {
        let mut constraints: Vec<DeltaConstraint> = Vec::new();
        for (idx, expr) in extra_constraints.into_iter().enumerate() {
            let trimmed = expr.trim();
            if trimmed.is_empty() {
                continue;
            }
            let name = format!("extra_{idx}");
            constraints.push(DeltaConstraint::new(&name, trimmed));
        }
        if !constraints.is_empty() {
            checker = checker.with_extra_constraints(constraints);
        }
    }
    let reader = StreamReader::try_new(Cursor::new(data_ipc), None)
        .map_err(|err| PyValueError::new_err(format!("Failed to decode data IPC: {err}")))?;
    let mut violations: Vec<String> = Vec::new();
    for batch in reader {
        let batch =
            batch.map_err(|err| PyValueError::new_err(format!("Invalid IPC batch: {err}")))?;
        match runtime.block_on(checker.check_batch(&batch)) {
            Ok(()) => {}
            Err(DeltaTableError::InvalidData { violations: batch_violations }) => {
                violations.extend(batch_violations);
            }
            Err(err) => {
                return Err(PyRuntimeError::new_err(format!(
                    "Delta data check failed: {err}"
                )));
            }
        }
    }
    Ok(violations)
}

#[pymodule]
fn datafusion_ext(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(udf_custom::install_function_factory, module)?)?;
    module.add_function(wrap_pyfunction!(register_udfs, module)?)?;
    module.add_function(wrap_pyfunction!(registry_snapshot_py, module)?)?;
    module.add_function(wrap_pyfunction!(udf_docs_snapshot, module)?)?;
    module.add_function(wrap_pyfunction!(load_df_plugin, module)?)?;
    module.add_function(wrap_pyfunction!(register_df_plugin_udfs, module)?)?;
    module.add_function(wrap_pyfunction!(register_df_plugin_table_functions, module)?)?;
    module.add_function(wrap_pyfunction!(register_df_plugin_table_providers, module)?)?;
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
    module.add_function(wrap_pyfunction!(udf_custom::col_to_byte, module)?)?;
    module.add_function(wrap_pyfunction!(schema_evolution_adapter_factory, module)?)?;
    module.add_function(wrap_pyfunction!(parquet_listing_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(delta_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(delta_table_provider_with_files, module)?)?;
    module.add_function(wrap_pyfunction!(install_expr_planners, module)?)?;
    module.add_function(wrap_pyfunction!(install_tracing, module)?)?;
    module.add_function(wrap_pyfunction!(register_cache_tables, module)?)?;
    module.add_function(wrap_pyfunction!(table_logical_plan, module)?)?;
    module.add_function(wrap_pyfunction!(table_dfschema_tree, module)?)?;
    module.add_function(wrap_pyfunction!(install_schema_evolution_adapter_factory, module)?)?;
    module.add_function(wrap_pyfunction!(registry_catalog_provider_factory, module)?)?;
    module.add_class::<DeltaCdfOptions>()?;
    module.add_function(wrap_pyfunction!(install_delta_table_factory, module)?)?;
    module.add_function(wrap_pyfunction!(apply_delta_session_defaults, module)?)?;
    module.add_function(wrap_pyfunction!(install_delta_plan_codecs, module)?)?;
    module.add_function(wrap_pyfunction!(delta_cdf_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(delta_table_provider_from_session, module)?)?;
    module.add_function(wrap_pyfunction!(delta_scan_config_from_session, module)?)?;
    module.add_function(wrap_pyfunction!(delta_data_checker, module)?)?;
    Ok(())
}
