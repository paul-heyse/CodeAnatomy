//! DataFusion extension for native function registration.

use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::io::Cursor;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    ArrayRef,
    Int32Builder,
    Int32Array,
    Int64Array,
    Int64Builder,
    ListArray,
    MapBuilder,
    StringArray,
    StringBuilder,
    StructArray,
};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
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
use datafusion::execution::cache::{CacheAccessor, FileMetadataCache};
use datafusion::execution::SessionStateDefaults;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory,
    PhysicalExprAdapter,
    PhysicalExprAdapterFactory,
};
use datafusion::physical_optimizer::{OptimizerConfig, PhysicalOptimizerRule};
use datafusion_common::{
    Constraint, Constraints, DFSchema, DataFusionError, Result, ScalarValue, Statistics,
};
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_parquet::file_format::ParquetFormat;
use datafusion_expr::{
    ColumnarValue,
    CreateExternalTable,
    DdlStatement,
    Expr,
    InsertOp,
    LogicalPlan,
    SortExpr,
    ScalarFunctionArgs,
    ScalarUDF,
    ScalarUDFImpl,
    Signature,
    TableProviderFilterPushDown,
    TableType,
    Volatility,
};
use datafusion_expr::expr_fn::col;
use datafusion_physical_plan::ExecutionPlan;
use deltalake::delta_datafusion::{DeltaScanConfigBuilder, DeltaTableProvider};
use deltalake::kernel::models::actions::Add;
use deltalake::kernel::scalars::ScalarExt;
use deltalake::{ensure_table_uri, DeltaTableBuilder};
use tokio::runtime::Runtime;
use datafusion_python::context::PySessionContext;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyDict};
use deltalake::delta_datafusion::{DeltaTableFactory, DeltaCdfTableProvider, DeltaScanConfig};
use deltalake::operations::DeltaOps;
use chrono::{DateTime, Utc};

const ENC_UTF8: i32 = 1;
const ENC_UTF16: i32 = 2;
const ENC_UTF32: i32 = 3;

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

#[derive(Debug)]
struct FunctionParameter {
    name: String,
    dtype: String,
}

#[derive(Debug)]
struct RulePrimitive {
    name: String,
    params: Vec<FunctionParameter>,
    return_type: String,
    volatility: String,
    description: Option<String>,
    supports_named_args: bool,
}

#[derive(Debug)]
struct FunctionFactoryPolicy {
    primitives: Vec<RulePrimitive>,
    prefer_named_arguments: bool,
    allow_async: bool,
    domain_operator_hooks: Vec<String>,
}

const POLICY_SCHEMA_VERSION: i32 = 1;

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
        *,
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
    Some(Expr::Literal(ScalarValue::Map(Arc::new(array))))
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
            Some(Expr::Literal(ScalarValue::Utf8(Some(default_value.clone()))))
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
    *,
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

fn policy_from_ipc(payload: &[u8]) -> Result<FunctionFactoryPolicy> {
    let mut reader = StreamReader::try_new(Cursor::new(payload), None).map_err(|err| {
        DataFusionError::Plan(format!("Failed to open policy IPC stream: {err}"))
    })?;
    let batch = reader
        .next()
        .ok_or_else(|| DataFusionError::Plan("Policy IPC stream is empty.".into()))?
        .map_err(|err| DataFusionError::Plan(format!("Failed to read policy IPC batch: {err}")))?;
    if batch.num_rows() != 1 {
        return Err(DataFusionError::Plan(
            "Policy IPC payload must contain exactly one row.".into(),
        ));
    }
    let version = read_int32(&batch, "version")?;
    if version != POLICY_SCHEMA_VERSION {
        return Err(DataFusionError::Plan(format!(
            "Unsupported policy payload version: {version}."
        )));
    }
    let prefer_named_arguments = read_bool(&batch, "prefer_named_arguments")?;
    let allow_async = read_bool(&batch, "allow_async")?;
    let domain_operator_hooks = read_string_list(&batch, "domain_operator_hooks")?;
    let primitives = read_primitives(&batch, "primitives")?;
    Ok(FunctionFactoryPolicy {
        primitives,
        prefer_named_arguments,
        allow_async,
        domain_operator_hooks,
    })
}

fn read_int32(batch: &arrow::record_batch::RecordBatch, name: &str) -> Result<i32> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} column.")))?;
    let array = array
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} column type.")))?;
    if array.is_null(0) {
        return Err(DataFusionError::Plan(format!("{name} cannot be null.")));
    }
    Ok(array.value(0))
}

fn read_bool(batch: &arrow::record_batch::RecordBatch, name: &str) -> Result<bool> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} column.")))?;
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} column type.")))?;
    if array.is_null(0) {
        return Err(DataFusionError::Plan(format!("{name} cannot be null.")));
    }
    Ok(array.value(0))
}

fn read_string_list(batch: &arrow::record_batch::RecordBatch, name: &str) -> Result<Vec<String>> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} column.")))?;
    let array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} column type.")))?;
    if array.is_null(0) {
        return Ok(Vec::new());
    }
    let values = array.value(0);
    let strings = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} list type.")))?;
    let mut output = Vec::with_capacity(strings.len());
    for index in 0..strings.len() {
        if strings.is_null(index) {
            continue;
        }
        output.push(strings.value(index).to_string());
    }
    Ok(output)
}

fn read_primitives(
    batch: &arrow::record_batch::RecordBatch,
    name: &str,
) -> Result<Vec<RulePrimitive>> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} column.")))?;
    let array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} column type.")))?;
    if array.is_null(0) {
        return Ok(Vec::new());
    }
    let values = array.value(0);
    let structs = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Plan("Invalid primitives list type.".into()))?;
    let names = string_field(structs, "name")?;
    let params = list_field(structs, "params")?;
    let return_types = string_field(structs, "return_type")?;
    let volatilities = string_field(structs, "volatility")?;
    let descriptions = string_field(structs, "description")?;
    let supports_named = bool_field(structs, "supports_named_args")?;
    let mut output = Vec::with_capacity(structs.len());
    for index in 0..structs.len() {
        if structs.is_null(index) {
            continue;
        }
        let primitive = RulePrimitive {
            name: read_string_value(&names, index)?,
            params: read_params(&params, index)?,
            return_type: read_string_value(&return_types, index)?,
            volatility: read_string_value(&volatilities, index)?,
            description: read_optional_string(&descriptions, index),
            supports_named_args: read_bool_value(&supports_named, index)?,
        };
        output.push(primitive);
    }
    Ok(output)
}

fn read_params(array: &ListArray, index: usize) -> Result<Vec<FunctionParameter>> {
    if array.is_null(index) {
        return Ok(Vec::new());
    }
    let values = array.value(index);
    let structs = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Plan("Invalid params list type.".into()))?;
    let names = string_field(structs, "name")?;
    let dtypes = string_field(structs, "dtype")?;
    let mut output = Vec::with_capacity(structs.len());
    for row in 0..structs.len() {
        if structs.is_null(row) {
            continue;
        }
        output.push(FunctionParameter {
            name: read_string_value(&names, row)?,
            dtype: read_string_value(&dtypes, row)?,
        });
    }
    Ok(output)
}

fn string_field<'a>(array: &'a StructArray, name: &str) -> Result<&'a StringArray> {
    let column = array
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} field.")))?;
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} field type.")))
}

fn list_field<'a>(array: &'a StructArray, name: &str) -> Result<&'a ListArray> {
    let column = array
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} field.")))?;
    column
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} field type.")))
}

fn bool_field<'a>(
    array: &'a StructArray,
    name: &str,
) -> Result<&'a arrow::array::BooleanArray> {
    let column = array
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} field.")))?;
    column
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} field type.")))
}

fn read_string_value(array: &StringArray, index: usize) -> Result<String> {
    if array.is_null(index) {
        return Err(DataFusionError::Plan("String value cannot be null.".into()));
    }
    Ok(array.value(index).to_string())
}

fn read_optional_string(array: &StringArray, index: usize) -> Option<String> {
    if array.is_null(index) {
        None
    } else {
        Some(array.value(index).to_string())
    }
}

fn read_bool_value(array: &arrow::array::BooleanArray, index: usize) -> Result<bool> {
    if array.is_null(index) {
        return Err(DataFusionError::Plan("Boolean value cannot be null.".into()));
    }
    Ok(array.value(index))
}

#[pyfunction]
fn install_function_factory(ctx: PyRef<PySessionContext>, policy_ipc: &PyBytes) -> PyResult<()> {
    let policy = policy_from_ipc(policy_ipc.as_bytes())
        .map_err(|err| PyValueError::new_err(format!("Invalid policy payload: {err}")))?;
    register_primitives(&ctx.ctx, &policy)
        .map_err(|err| PyRuntimeError::new_err(format!("FunctionFactory install failed: {err}")))?;
    Ok(())
}

#[pyfunction]
fn schema_evolution_adapter_factory(py: Python<'_>) -> PyResult<PyObject> {
    let factory: Arc<dyn PhysicalExprAdapterFactory> =
        Arc::new(SchemaEvolutionAdapterFactory::default());
    let name = CString::new("datafusion_ext.SchemaEvolutionAdapterFactory")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, factory, Some(name))?;
    Ok(capsule.into_py(py))
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
    expr_adapter_factory: Option<PyObject>,
    parquet_pruning: Option<bool>,
    skip_metadata: Option<bool>,
    collect_statistics: Option<bool>,
) -> PyResult<PyObject> {
    let _ = expr_adapter_factory;
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
        let capsule: &PyCapsule = factory_obj.extract(py)?;
        let factory: &Arc<dyn PhysicalExprAdapterFactory> = capsule.reference()?;
        factory.clone()
    } else {
        Arc::new(SchemaEvolutionAdapterFactory::default())
    };
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(schema)
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
        table_name: &table_name,
        location: &path,
        table_partition_cols: table_partition_names,
        definition: table_definition.clone(),
        column_defaults: defaults.clone(),
        constraints: plan_constraints,
        file_sort_order: sort_exprs,
    )
    .map_err(|err| PyRuntimeError::new_err(format!("ListingTable plan build failed: {err}")))?;
    let wrapped = CpgTableProvider::new(
        Arc::new(provider),
        ddl: table_definition,
        logical_plan: Some(plan),
        column_defaults: defaults,
        constraints,
    );
    let ffi_provider = FFI_TableProvider::new(Arc::new(wrapped), true, None);
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, ffi_provider, Some(name))?;
    Ok(capsule.into_py(py))
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
) -> PyResult<PyObject> {
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
    Ok(capsule.into_py(py))
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
) -> PyResult<PyObject> {
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
    Ok(capsule.into_py(py))
}

#[pyfunction]
fn install_expr_planners(ctx: PyRef<PySessionContext>, planner_names: Vec<String>) -> PyResult<()> {
    if planner_names.is_empty() {
        return Err(PyValueError::new_err(
            "ExprPlanner installation requires at least one planner name.",
        ));
    }
    let planners = SessionStateDefaults::default_expr_planners();
    let mut state = ctx.ctx.state_ref().write();
    let slot = state.expr_planners();
    *slot = Some(planners);
    Ok(())
}

#[pyfunction]
fn install_tracing(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    let mut state = ctx.ctx.state_ref().write();
    let rules = state.physical_optimizer_rules();
    let mut existing = rules.take().unwrap_or_default();
    existing.push(Arc::new(TracingMarkerRule));
    *rules = Some(existing);
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
fn install_schema_evolution_adapter_factory(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    let factory: Arc<dyn PhysicalExprAdapterFactory> =
        Arc::new(SchemaEvolutionAdapterFactory::default());
    ctx.ctx
        .register_physical_expr_adapter_factory(factory)
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Schema evolution adapter factory registration failed: {err}"
            ))
        })?;
    Ok(())
}

#[pyfunction]
fn registry_catalog_provider_factory(py: Python<'_>, schema_name: Option<String>) -> PyResult<PyObject> {
    let schema_name = schema_name.unwrap_or_else(|| "public".to_string());
    let schema_provider: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema(schema_name, schema_provider)
        .map_err(|err| PyRuntimeError::new_err(format!("Catalog registration failed: {err}")))?;
    let provider: Arc<dyn CatalogProvider> = catalog;
    let name = CString::new("datafusion_ext.RegistryCatalogProviderFactory")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, provider, Some(name))?;
    Ok(capsule.into_py(py))
}

fn register_primitives(ctx: &SessionContext, policy: &FunctionFactoryPolicy) -> Result<()> {
    for primitive in &policy.primitives {
        let udf = build_udf(primitive, policy.prefer_named_arguments)?;
        ctx.register_udf(udf);
    }
    Ok(())
}

fn build_udf(primitive: &RulePrimitive, prefer_named: bool) -> Result<ScalarUDF> {
    let signature = primitive_signature(primitive, prefer_named)?;
    match primitive.name.as_str() {
        "cpg_score" => Ok(ScalarUDF::from(Arc::new(CpgScoreUdf { signature }))),
        "stable_hash64" => Ok(ScalarUDF::from(Arc::new(StableHash64Udf { signature }))),
        "stable_hash128" => Ok(ScalarUDF::from(Arc::new(StableHash128Udf { signature }))),
        "prefixed_hash64" => Ok(ScalarUDF::from(Arc::new(PrefixedHash64Udf { signature }))),
        "stable_id" => Ok(ScalarUDF::from(Arc::new(StableIdUdf { signature }))),
        "position_encoding_norm" => {
            Ok(ScalarUDF::from(Arc::new(PositionEncodingUdf { signature })))
        }
        "col_to_byte" => Ok(ScalarUDF::from(Arc::new(ColToByteUdf { signature }))),
        name => Err(DataFusionError::Plan(format!(
            "Unsupported rule primitive: {name}"
        ))),
    }
}

fn primitive_signature(primitive: &RulePrimitive, prefer_named: bool) -> Result<Signature> {
    let arg_types: Result<Vec<DataType>> = primitive
        .params
        .iter()
        .map(|param| dtype_from_str(param.dtype.as_str()))
        .collect();
    let signature = Signature::exact(arg_types?, volatility_from_str(primitive.volatility.as_str())?);
    if prefer_named && primitive.supports_named_args {
        let names = primitive.params.iter().map(|param| param.name.clone()).collect();
        return signature.with_parameter_names(names).map_err(|err| {
            DataFusionError::Plan(format!("Invalid parameter names for {}: {err}", primitive.name))
        });
    }
    Ok(signature)
}

fn dtype_from_str(value: &str) -> Result<DataType> {
    match value {
        "string" => Ok(DataType::Utf8),
        "int64" => Ok(DataType::Int64),
        "int32" => Ok(DataType::Int32),
        "float64" => Ok(DataType::Float64),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported dtype in primitive policy: {value}"
        ))),
    }
}

fn volatility_from_str(value: &str) -> Result<Volatility> {
    match value {
        "immutable" => Ok(Volatility::Immutable),
        "stable" => Ok(Volatility::Stable),
        "volatile" => Ok(Volatility::Volatile),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported volatility in primitive policy: {value}"
        ))),
    }
}

fn prefixed_hash64_value(prefix: &str, value: &str) -> String {
    format!("{prefix}:{}", hash64_value(value))
}

fn stable_id_value(prefix: &str, value: &str) -> String {
    format!("{prefix}:{}", hash128_value(value))
}

fn hash64_value(value: &str) -> i64 {
    let mut hasher = Blake2bVar::new(8).expect("blake2b supports 8-byte output");
    hasher.update(value.as_bytes());
    let mut out = [0u8; 8];
    hasher.finalize_variable(&mut out).expect("hash output");
    let unsigned = u64::from_be_bytes(out);
    let masked = unsigned & ((1_u64 << 63) - 1);
    masked as i64
}

fn hash128_value(value: &str) -> String {
    let mut hasher = Blake2bVar::new(16).expect("blake2b supports 16-byte output");
    hasher.update(value.as_bytes());
    let mut out = [0u8; 16];
    hasher.finalize_variable(&mut out).expect("hash output");
    hex::encode(out)
}

#[derive(Debug)]
struct TracingMarkerRule;

impl PhysicalOptimizerRule for TracingMarkerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &dyn OptimizerConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "codeanatomy_tracing_marker"
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
    let mut state = ctx.ctx.state_ref().write();
    let factories = state.table_factories_mut();
    factories.insert(alias, Arc::new(DeltaTableFactory {}));
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
) -> PyResult<PyObject> {
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

    // Build CdfLoadBuilder with version/timestamp options using DeltaOps
    let ops = DeltaOps::from(table);
    let mut cdf_builder = ops.load_cdf();

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
    Ok(capsule.into_py(py))
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
) -> PyResult<PyObject> {
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
    let mut scan_config = DeltaScanConfig::new_from_session(state_ref.as_ref());

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
    Ok(capsule.into_py(py))
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
) -> PyResult<PyObject> {
    let state_ref = ctx.ctx.state();
    let mut scan_config = DeltaScanConfig::new_from_session(state_ref.as_ref());

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

fn normalize_position_encoding(value: &str) -> i32 {
    let upper = value.trim().to_uppercase();
    if upper.is_empty() {
        return ENC_UTF32;
    }
    if upper.chars().all(|ch| ch.is_ascii_digit()) {
        if let Ok(parsed) = upper.parse::<i32>() {
            return match parsed {
                ENC_UTF8 | ENC_UTF16 | ENC_UTF32 => parsed,
                _ => ENC_UTF32,
            };
        }
    }
    if upper.contains("UTF8") {
        return ENC_UTF8;
    }
    if upper.contains("UTF16") {
        return ENC_UTF16;
    }
    ENC_UTF32
}

fn col_unit_from_text(value: &str) -> ColUnit {
    let upper = value.trim().to_uppercase();
    if upper.chars().all(|ch| ch.is_ascii_digit()) {
        if let Ok(parsed) = upper.parse::<i32>() {
            return ColUnit::from_encoding(parsed);
        }
    }
    if upper.contains("BYTE") {
        return ColUnit::Byte;
    }
    if upper.contains("UTF8") {
        return ColUnit::Utf8;
    }
    if upper.contains("UTF16") {
        return ColUnit::Utf16;
    }
    ColUnit::Utf32
}

#[derive(Clone, Copy)]
enum ColUnit {
    Byte,
    Utf8,
    Utf16,
    Utf32,
}

impl ColUnit {
    fn from_encoding(value: i32) -> Self {
        match value {
            ENC_UTF8 => ColUnit::Utf8,
            ENC_UTF16 => ColUnit::Utf16,
            ENC_UTF32 => ColUnit::Utf32,
            _ => ColUnit::Utf32,
        }
    }
}

fn clamp_offset(value: i64, limit: usize) -> usize {
    if value <= 0 {
        return 0;
    }
    let candidate = value as usize;
    candidate.min(limit)
}

fn normalize_offset(value: i64) -> usize {
    if value <= 0 {
        return 0;
    }
    value as usize
}

fn code_unit_offset_to_py_index(line: &str, offset: usize, unit: ColUnit) -> Result<usize> {
    match unit {
        ColUnit::Utf32 => Ok(offset.min(line.chars().count())),
        ColUnit::Utf8 => {
            let bytes = line.as_bytes();
            let prefix = &bytes[..offset.min(bytes.len())];
            let prefix_str = std::str::from_utf8(prefix)
                .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Ok(prefix_str.chars().count())
        }
        ColUnit::Utf16 => {
            let units: Vec<u16> = line.encode_utf16().collect();
            let prefix = &units[..offset.min(units.len())];
            let prefix_str =
                String::from_utf16(prefix).map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Ok(prefix_str.chars().count())
        }
        ColUnit::Byte => Ok(offset.min(line.as_bytes().len())),
    }
}

fn byte_offset_from_py_index(line: &str, py_index: usize) -> usize {
    let prefix: String = line.chars().take(py_index).collect();
    prefix.as_bytes().len()
}

struct CpgScoreUdf {
    signature: Signature,
}

impl ScalarUDFImpl for CpgScoreUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cpg_score"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        Ok(ColumnarValue::Array(arrays[0].clone()))
    }
}

struct StableHash64Udf {
    signature: Signature,
}

impl ScalarUDFImpl for StableHash64Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stable_hash64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("stable_hash64 expects string input".into()))?;
        let mut builder = Int64Builder::with_capacity(input.len());
        for index in 0..input.len() {
            if input.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(hash64_value(input.value(index)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

struct StableHash128Udf {
    signature: Signature,
}

impl ScalarUDFImpl for StableHash128Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stable_hash128"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("stable_hash128 expects string input".into()))?;
        let mut builder = StringBuilder::with_capacity(input.len(), input.len() * 16);
        for index in 0..input.len() {
            if input.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(hash128_value(input.value(index)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

struct PrefixedHash64Udf {
    signature: Signature,
}

impl ScalarUDFImpl for PrefixedHash64Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "prefixed_hash64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let prefixes = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("prefixed_hash64 expects string prefix input".into())
            })?;
        let values = arrays[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("prefixed_hash64 expects string value input".into())
            })?;
        let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 16);
        for index in 0..values.len() {
            if prefixes.is_null(index) || values.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(prefixed_hash64_value(
                    prefixes.value(index),
                    values.value(index),
                ));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

struct StableIdUdf {
    signature: Signature,
}

impl ScalarUDFImpl for StableIdUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stable_id"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let prefixes = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("stable_id expects string prefix input".into()))?;
        let values = arrays[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("stable_id expects string value input".into()))?;
        let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 32);
        for index in 0..values.len() {
            if prefixes.is_null(index) || values.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(stable_id_value(
                    prefixes.value(index),
                    values.value(index),
                ));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

struct PositionEncodingUdf {
    signature: Signature,
}

impl ScalarUDFImpl for PositionEncodingUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "position_encoding_norm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("position_encoding_norm expects string input".into())
            })?;
        let mut builder = Int32Builder::with_capacity(input.len());
        for index in 0..input.len() {
            if input.is_null(index) {
                builder.append_value(ENC_UTF32);
            } else {
                builder.append_value(normalize_position_encoding(input.value(index)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

struct ColToByteUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ColToByteUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "col_to_byte"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let lines = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("col_to_byte expects string input".into()))?;
        let offsets = arrays[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Plan("col_to_byte expects int64 input".into()))?;
        let encodings = arrays[2]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("col_to_byte expects string encoding".into()))?;
        let mut builder = Int64Builder::with_capacity(lines.len());
        for index in 0..lines.len() {
            if lines.is_null(index) || offsets.is_null(index) {
                builder.append_null();
                continue;
            }
            let line = lines.value(index);
            let offset = offsets.value(index);
            let unit = if encodings.is_null(index) {
                ColUnit::Utf32
            } else {
                col_unit_from_text(encodings.value(index))
            };
            if matches!(unit, ColUnit::Byte) {
                let max_len = line.as_bytes().len();
                let bytes = clamp_offset(offset, max_len);
                builder.append_value(bytes as i64);
                continue;
            }
            let py_index = code_unit_offset_to_py_index(line, normalize_offset(offset), unit)?;
            let byte_offset = byte_offset_from_py_index(line, py_index);
            builder.append_value(byte_offset as i64);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[pymodule]
fn datafusion_ext(_py: Python<'_>, module: &PyModule) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(install_function_factory, module)?)?;
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
    module.add_function(wrap_pyfunction!(delta_cdf_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(delta_table_provider_from_session, module)?)?;
    module.add_function(wrap_pyfunction!(delta_scan_config_from_session, module)?)?;
    Ok(())
}
