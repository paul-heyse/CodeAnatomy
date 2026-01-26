use std::fmt;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::execution::context::SessionContext;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::{DataFusionError, DFSchema, Result, ScalarValue, Statistics};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_csv::CsvFormat;
use datafusion_datasource_parquet::file_format::ParquetFormat;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use tokio::runtime::{Handle, Runtime};
use tokio::task::block_in_place;
use arrow::datatypes::DataType;

pub fn register_external_udtfs(ctx: &SessionContext) -> Result<()> {
    let read_parquet: Arc<dyn TableFunctionImpl> =
        Arc::new(ReadParquetTableFunction::new(ctx.clone()));
    ctx.register_udtf("read_parquet", Arc::clone(&read_parquet));
    let read_csv: Arc<dyn TableFunctionImpl> =
        Arc::new(ReadCsvTableFunction::new(ctx.clone()));
    ctx.register_udtf("read_csv", Arc::clone(&read_csv));
    Ok(())
}

#[derive(Clone)]
struct ReadParquetTableFunction {
    ctx: SessionContext,
}

impl ReadParquetTableFunction {
    fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl fmt::Debug for ReadParquetTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadParquetTableFunction").finish()
    }
}

impl TableFunctionImpl for ReadParquetTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = extract_required_path(args, "read_parquet")?;
        let limit = extract_optional_limit(args, "read_parquet")?;
        let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat::default());
        let provider = listing_table_provider(&self.ctx, format, &path)?;
        Ok(wrap_with_limit(provider, limit))
    }
}

#[derive(Clone)]
struct ReadCsvTableFunction {
    ctx: SessionContext,
}

impl ReadCsvTableFunction {
    fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl fmt::Debug for ReadCsvTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadCsvTableFunction").finish()
    }
}

impl TableFunctionImpl for ReadCsvTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = extract_required_path(args, "read_csv")?;
        let limit = extract_optional_limit(args, "read_csv")?;
        let format: Arc<dyn FileFormat> = Arc::new(CsvFormat::default());
        let provider = listing_table_provider(&self.ctx, format, &path)?;
        Ok(wrap_with_limit(provider, limit))
    }
}

fn extract_required_path(args: &[Expr], func_name: &str) -> Result<String> {
    let expr = args.first().ok_or_else(|| {
        DataFusionError::Plan(format!("{func_name} expects at least one argument"))
    })?;
    let simplified = simplify_expr(expr.clone())?;
    let path = extract_literal_string(&simplified).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{func_name} expects a literal string path argument"
        ))
    })?;
    Ok(path)
}

fn extract_optional_limit(args: &[Expr], func_name: &str) -> Result<Option<usize>> {
    if args.len() <= 1 {
        return Ok(None);
    }
    if args.len() > 2 {
        return Err(DataFusionError::Plan(format!(
            "{func_name} expects at most two arguments"
        )));
    }
    let simplified = simplify_expr(args[1].clone())?;
    let limit = extract_literal_usize(&simplified).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{func_name} expects an integer literal limit as the second argument"
        ))
    })?;
    Ok(Some(limit))
}

fn simplify_expr(expr: Expr) -> Result<Expr> {
    let props = ExecutionProps::new();
    let schema = Arc::new(DFSchema::empty());
    let context = SimplifyContext::new(&props).with_schema(schema);
    let simplifier = ExprSimplifier::new(context);
    simplifier.simplify(expr)
}

fn extract_literal_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _) => Some(value.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _) => Some(value.clone()),
        Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => Some(value.clone()),
        _ => None,
    }
}

fn extract_literal_usize(expr: &Expr) -> Option<usize> {
    let value = match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => (*value).try_into().ok(),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => (*value as i64).try_into().ok(),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => (*value).try_into().ok(),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Some(*value as usize),
        _ => None,
    };
    value
}

fn listing_table_provider(
    ctx: &SessionContext,
    format: Arc<dyn FileFormat>,
    path: &str,
) -> Result<Arc<dyn TableProvider>> {
    let state = ctx.state();
    let mut options =
        ListingOptions::new(format).with_session_config_options(state.config());
    let table_path = ListingTableUrl::parse(path).or_else(|_| {
        ListingTableUrl::parse(format!("file://{path}").as_str())
    })?;
    let inferred_extension = infer_file_extension(&table_path, &options, path)?;
    options = options.with_file_extension(inferred_extension);

    if state
        .config_options()
        .execution
        .listing_table_factory_infer_partitions
    {
        let partitions = block_on(options.infer_partitions(&state, &table_path))?;
        let partition_cols = partitions
            .into_iter()
            .map(|name| {
                (
                    name,
                    DataType::Dictionary(
                        Box::new(DataType::UInt16),
                        Box::new(DataType::Utf8),
                    ),
                )
            })
            .collect::<Vec<_>>();
        options = options.with_table_partition_cols(partition_cols);
    }

    block_on(options.validate_partitions(&state, &table_path))?;
    let schema = block_on(options.infer_schema(&state, &table_path))?;

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(schema);
    let provider = ListingTable::try_new(config)?
        .with_cache(state.runtime_env().cache_manager.get_file_statistic_cache());
    Ok(Arc::new(provider))
}

fn block_on<F, T>(future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    if let Ok(handle) = Handle::try_current() {
        return block_in_place(|| handle.block_on(future));
    }
    let runtime = Runtime::new().map_err(|err| {
        DataFusionError::Plan(format!("Failed to create Tokio runtime: {err}"))
    })?;
    runtime.block_on(future)
}

fn infer_file_extension(
    table_path: &ListingTableUrl,
    options: &ListingOptions,
    path: &str,
) -> Result<String> {
    if table_path.is_collection() {
        return Ok(options.file_extension.clone());
    }
    let (extension, compression) =
        ListingTableConfig::infer_file_extension_and_compression_type(path)
            .unwrap_or((options.file_extension.clone(), None));
    if extension.is_empty() {
        return Ok(options.file_extension.clone());
    }
    if let Some(compression) = compression {
        let compression_type = FileCompressionType::from_str(&compression)?;
        return options.format.get_ext_with_compression(&compression_type);
    }
    Ok(extension)
}

fn wrap_with_limit(
    provider: Arc<dyn TableProvider>,
    limit: Option<usize>,
) -> Arc<dyn TableProvider> {
    if let Some(limit) = limit {
        Arc::new(LimitedTableProvider::new(provider, limit))
    } else {
        provider
    }
}

#[derive(Debug)]
struct LimitedTableProvider {
    inner: Arc<dyn TableProvider>,
    max_rows: usize,
}

impl LimitedTableProvider {
    fn new(inner: Arc<dyn TableProvider>, max_rows: usize) -> Self {
        Self { inner, max_rows }
    }
}

#[async_trait]
impl TableProvider for LimitedTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&datafusion_common::Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<std::borrow::Cow<'_, datafusion_expr::LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let effective_limit = limit.map_or(self.max_rows, |value| value.min(self.max_rows));
        self.inner
            .scan(state, projection, filters, Some(effective_limit))
            .await
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
        insert_op: datafusion_expr::dml::InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}
