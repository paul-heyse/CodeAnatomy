**deltalake_core > delta_datafusion > table_provider**

# Module: delta_datafusion::table_provider

## Contents

**Structs**

- [`DeltaScan`](#deltascan) - A wrapper for parquet scans
- [`DeltaScanConfig`](#deltascanconfig) - Include additional metadata columns during a [`DeltaScan`]
- [`DeltaScanConfigBuilder`](#deltascanconfigbuilder) - Used to specify if additional metadata columns are exposed to the user
- [`DeltaTableProvider`](#deltatableprovider) - A Delta table provider that enables additional metadata columns to be included during the scan
- [`TableProviderBuilder`](#tableproviderbuilder) - Builder for a datafusion [TableProvider] for a Delta table

---

## deltalake_core::delta_datafusion::table_provider::DeltaScan

*Struct*

A wrapper for parquet scans

**Trait Implementations:**

- **DisplayAs**
  - `fn fmt_as(self: &Self, _t: DisplayFormatType, f: & mut fmt::Formatter) -> std::fmt::Result`
- **ExecutionPlan**
  - `fn name(self: &Self) -> &str`
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> SchemaRef`
  - `fn properties(self: &Self) -> &PlanProperties`
  - `fn children(self: &Self) -> Vec<&Arc<dyn ExecutionPlan>>`
  - `fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>>`
  - `fn repartitioned(self: &Self, target_partitions: usize, config: &ConfigOptions) -> Result<Option<Arc<dyn ExecutionPlan>>>`
  - `fn execute(self: &Self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream>`
  - `fn metrics(self: &Self) -> Option<MetricsSet>`
  - `fn partition_statistics(self: &Self, partition: Option<usize>) -> Result<Statistics>`
  - `fn gather_filters_for_pushdown(self: &Self, _phase: FilterPushdownPhase, parent_filters: Vec<Arc<dyn PhysicalExpr>>, _config: &ConfigOptions) -> Result<FilterDescription>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::delta_datafusion::table_provider::DeltaScanConfig

*Struct*

Include additional metadata columns during a [`DeltaScan`]

**Fields:**
- `file_column_name: Option<String>` - Include the source path for each record
- `wrap_partition_values: bool` - Wrap partition values in a dictionary encoding, defaults to true
- `enable_parquet_pushdown: bool` - Allow pushdown of the scan filter, defaults to true
- `schema_force_view_types: bool` - If true, parquet reader will read columns of `Utf8`/`Utf8Large`
- `schema: Option<arrow::datatypes::SchemaRef>` - Schema to read as

**Methods:**

- `fn new() -> Self` - Create a new default [`DeltaScanConfig`]
- `fn new_from_session(session: &dyn Session) -> Self`
- `fn with_file_column_name<S>(self: Self, name: S) -> Self`
- `fn with_wrap_partition_values(self: Self, wrap: bool) -> Self` - Whether to wrap partition values in a dictionary encoding
- `fn with_parquet_pushdown(self: Self, pushdown: bool) -> Self` - Allow pushdown of the scan filter
- `fn with_schema(self: Self, schema: SchemaRef) -> Self` - Use the provided [SchemaRef] for the [DeltaScan]

**Trait Implementations:**

- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> DeltaScanConfig`
- **Default**
  - `fn default() -> Self`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::delta_datafusion::table_provider::DeltaScanConfigBuilder

*Struct*

Used to specify if additional metadata columns are exposed to the user

**Methods:**

- `fn new() -> Self` - Construct a new instance of `DeltaScanConfigBuilder`
- `fn with_file_column(self: Self, include: bool) -> Self` - Indicate that a column containing a records file path is included.
- `fn with_file_column_name<S>(self: Self, name: &S) -> Self` - Indicate that a column containing a records file path is included and column name is user defined.
- `fn wrap_partition_values(self: Self, wrap: bool) -> Self` - Whether to wrap partition values in a dictionary encoding
- `fn with_parquet_pushdown(self: Self, pushdown: bool) -> Self` - Allow pushdown of the scan filter
- `fn with_schema(self: Self, schema: SchemaRef) -> Self` - Use the provided [SchemaRef] for the [DeltaScan]
- `fn build(self: &Self, snapshot: &EagerSnapshot) -> DeltaResult<DeltaScanConfig>` - Build a DeltaScanConfig and ensure no column name conflicts occur during downstream processing

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DeltaScanConfigBuilder`
- **Default**
  - `fn default() -> Self`



## deltalake_core::delta_datafusion::table_provider::DeltaTableProvider

*Struct*

A Delta table provider that enables additional metadata columns to be included during the scan

**Methods:**

- `fn try_new(snapshot: EagerSnapshot, log_store: LogStoreRef, config: DeltaScanConfig) -> DeltaResult<Self>` - Build a DeltaTableProvider
- `fn with_files(self: Self, files: Vec<Add>) -> DeltaTableProvider` - Define which files to consider while building a scan, for advanced usecases

**Trait Implementations:**

- **TableProvider**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> Arc<Schema>`
  - `fn table_type(self: &Self) -> TableType`
  - `fn get_table_definition(self: &Self) -> Option<&str>`
  - `fn get_logical_plan(self: &Self) -> Option<Cow<LogicalPlan>>`
  - `fn scan(self: &'life0 Self, session: &'life1 dyn Session, projection: Option<&'life2 Vec<usize>>, filters: &'life3 [Expr], limit: Option<usize>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn supports_filters_pushdown(self: &Self, filter: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>>`
  - `fn insert_into(self: &'life0 Self, state: &'life1 dyn Session, input: Arc<dyn ExecutionPlan>, insert_op: InsertOp) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - Insert the data into the delta table
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::delta_datafusion::table_provider::TableProviderBuilder

*Struct*

Builder for a datafusion [TableProvider] for a Delta table

A table provider can be built by providing either a log store, a Snapshot,
or an eager snapshot. If some Snapshot is provided, that will be used directly,
and no IO will be performed when building the provider.

**Methods:**

- `fn with_log_store<impl Into<Arc<dyn LogStore>>>(self: Self, log_store: impl Trait) -> Self` - Provide the log store to use for the table provider
- `fn with_eager_snapshot<impl Into<Arc<EagerSnapshot>>>(self: Self, snapshot: impl Trait) -> Self` - Provide an eager snapshot to use for the table provider
- `fn with_snapshot<impl Into<Arc<Snapshot>>>(self: Self, snapshot: impl Trait) -> Self` - Provide a snapshot to use for the table provider
- `fn with_session<S>(self: Self, session: Arc<S>) -> Self` - Provide a DataFusion session for scan config defaults.
- `fn with_table_version<impl Into<Option<Version>>>(self: Self, version: impl Trait) -> Self` - Specify the version of the table to provide
- `fn with_file_column<impl ToString>(self: Self, file_column: impl Trait) -> Self` - Specify the name of the file column to include in the scan
- `fn build(self: Self) -> Result<next::DeltaScan>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`
- **Default**
  - `fn default() -> Self`



