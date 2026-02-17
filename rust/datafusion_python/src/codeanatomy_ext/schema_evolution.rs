//! Schema evolution and listing-table provider bridge surface.

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::CString;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{MapBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::{
    Constraint, Constraints, DFSchema, DataFusionError, Result, ScalarValue, Statistics,
};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_parquet::file_format::ParquetFormat;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::expr_fn::col;
use datafusion_expr::{
    CreateExternalTable, DdlStatement, Expr, LogicalPlan, SortExpr, TableProviderFilterPushDown,
    TableType,
};
use datafusion_ext::physical_rules::ensure_physical_config;
use datafusion_ffi::table_provider::FFI_TableProvider;
use df_plugin_common::schema_from_ipc;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use super::helpers::extract_session_ctx;

fn decode_schema_ipc(schema_ipc: &[u8]) -> PyResult<SchemaRef> {
    schema_from_ipc(schema_ipc)
        .map_err(|err| PyValueError::new_err(format!("Failed to decode schema IPC: {err}")))
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
pub(crate) fn schema_evolution_adapter_factory(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let factory: Arc<dyn PhysicalExprAdapterFactory> = Arc::new(SchemaEvolutionAdapterFactory);
    let name = CString::new("datafusion_ext.SchemaEvolutionAdapterFactory")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, factory, Some(name))?;
    Ok(capsule.unbind().into())
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(crate) fn parquet_listing_table_provider(
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
    let schema = decode_schema_ipc(&schema_ipc)?;
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
        let schema = decode_schema_ipc(&schema_ipc)?;
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
        Arc::new(SchemaEvolutionAdapterFactory)
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
pub(crate) fn install_schema_evolution_adapter_factory(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    let state_ref = extract_session_ctx(ctx)?.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    ensure_physical_config(config.options_mut()).map_err(|err| {
        PyRuntimeError::new_err(format!(
            "Schema evolution adapter install failed while updating runtime config: {err}"
        ))
    })?;
    Ok(())
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(schema_evolution_adapter_factory, module)?)?;
    module.add_function(wrap_pyfunction!(parquet_listing_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(
        install_schema_evolution_adapter_factory,
        module
    )?)?;
    Ok(())
}
