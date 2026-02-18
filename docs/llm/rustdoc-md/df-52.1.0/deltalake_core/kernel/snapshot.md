**deltalake_core > kernel > snapshot**

# Module: kernel::snapshot

## Contents

**Structs**

- [`EagerSnapshot`](#eagersnapshot) - A snapshot of a Delta table that has been eagerly loaded into memory.
- [`Snapshot`](#snapshot) - A snapshot of a Delta table

---

## deltalake_core::kernel::snapshot::EagerSnapshot

*Struct*

A snapshot of a Delta table that has been eagerly loaded into memory.

**Methods:**

- `fn try_new(log_store: &dyn LogStore, config: DeltaTableConfig, version: Option<i64>) -> DeltaResult<Self>` - Create a new [`EagerSnapshot`] instance
- `fn version(self: &Self) -> i64` - Get the table version of the snapshot
- `fn version_timestamp(self: &Self, version: i64) -> Option<i64>` - Get the timestamp of the given version
- `fn schema(self: &Self) -> KernelSchemaRef` - Get the table schema of the snapshot
- `fn arrow_schema(self: &Self) -> SchemaRef` - Get the table arrow schema of the snapshot
- `fn metadata(self: &Self) -> &Metadata` - Get the table metadata of the snapshot
- `fn protocol(self: &Self) -> &Protocol` - Get the table protocol of the snapshot
- `fn load_config(self: &Self) -> &DeltaTableConfig` - Get the table config which is loaded with of the snapshot
- `fn table_properties(self: &Self) -> &TableProperties` - Well known table configuration
- `fn table_configuration(self: &Self) -> &TableConfiguration`
- `fn log_data(self: &Self) -> LogDataHandler` - Get a [`LogDataHandler`] for the snapshot to inspect the currently loaded state of the log.
- `fn file_views(self: &Self, log_store: &dyn LogStore, predicate: Option<PredicateRef>) -> BoxStream<DeltaResult<LogicalFileView>>` - Stream the active files in the snapshot
- `fn file_views_by_partitions(self: &Self, log_store: &dyn LogStore, filters: &[PartitionFilter]) -> BoxStream<DeltaResult<LogicalFileView>>`
- `fn transaction_version<impl ToString>(self: &Self, log_store: &dyn LogStore, app_id: impl Trait) -> DeltaResult<Option<i64>>` - Iterate over all latest app transactions
- `fn domain_metadata<impl ToString>(self: &Self, log_store: &dyn LogStore, domain: impl Trait) -> DeltaResult<Option<String>>`
- `fn add_actions_table(self: &Self, flatten: bool) -> Result<arrow::record_batch::RecordBatch, DeltaTableError>` - Get an [arrow::record_batch::RecordBatch] containing add action data.
- `fn add_actions_batches(self: &Self, flatten: bool) -> Result<Vec<arrow::record_batch::RecordBatch>, DeltaTableError>` - Get add action data as a list of [arrow::record_batch::RecordBatch] without

**Trait Implementations:**

- **Deserialize**
  - `fn deserialize<D>(deserializer: D) -> Result<EagerSnapshot, <D as >::Error>`
- **TableReference**
  - `fn protocol(self: &Self) -> &Protocol`
  - `fn metadata(self: &Self) -> &Metadata`
  - `fn config(self: &Self) -> &TableProperties`
  - `fn eager_snapshot(self: &Self) -> &EagerSnapshot`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **DataFusionMixins**
  - `fn read_schema(self: &Self) -> ArrowSchemaRef`
  - `fn input_schema(self: &Self) -> ArrowSchemaRef`
  - `fn parse_predicate_expression<impl AsRef<str>>(self: &Self, expr: impl Trait, session: &dyn Session) -> DeltaResult<Expr>`
- **PruningStatistics**
  - `fn min_values(self: &Self, column: &Column) -> Option<ArrayRef>` - return the minimum values for the named column, if known.
  - `fn max_values(self: &Self, column: &Column) -> Option<ArrayRef>` - return the maximum values for the named column, if known.
  - `fn num_containers(self: &Self) -> usize` - return the number of containers (e.g. row groups) being
  - `fn null_counts(self: &Self, column: &Column) -> Option<ArrayRef>` - return the number of null values for the named column as an
  - `fn row_counts(self: &Self, column: &Column) -> Option<ArrayRef>` - return the number of rows for the named column in each container
  - `fn contained(self: &Self, column: &Column, value: &HashSet<ScalarValue>) -> Option<BooleanArray>`
- **Serialize**
  - `fn serialize<S>(self: &Self, serializer: S) -> Result<<S as >::Ok, <S as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> EagerSnapshot`
- **PartialEq**
  - `fn eq(self: &Self, other: &EagerSnapshot) -> bool`



## deltalake_core::kernel::snapshot::Snapshot

*Struct*

A snapshot of a Delta table

**Methods:**

- `fn try_new_with_engine(engine: Arc<dyn Engine>, table_root: Url, config: DeltaTableConfig, version: Option<Version>) -> DeltaResult<Self>`
- `fn try_new(log_store: &dyn LogStore, config: DeltaTableConfig, version: Option<i64>) -> DeltaResult<Self>` - Create a new [`Snapshot`] instance
- `fn scan_builder(self: &Self) -> ScanBuilder`
- `fn into_scan_builder(self: Self) -> ScanBuilder`
- `fn update(self: Arc<Self>, engine: Arc<dyn Engine>, target_version: Option<u64>) -> DeltaResult<Arc<Self>>` - Update the snapshot to the given version
- `fn version(self: &Self) -> i64` - Get the table version of the snapshot
- `fn schema(self: &Self) -> KernelSchemaRef` - Get the table schema of the snapshot
- `fn arrow_schema(self: &Self) -> SchemaRef`
- `fn metadata(self: &Self) -> &Metadata` - Get the table metadata of the snapshot
- `fn protocol(self: &Self) -> &Protocol` - Get the table protocol of the snapshot
- `fn load_config(self: &Self) -> &DeltaTableConfig` - Get the table config which is loaded with of the snapshot
- `fn table_properties(self: &Self) -> &TableProperties` - Well known properties of the table
- `fn table_configuration(self: &Self) -> &TableConfiguration`
- `fn files(self: &Self, log_store: &dyn LogStore, predicate: Option<PredicateRef>) -> SendableRBStream` - Get the active files for the current snapshot.
- `fn file_views(self: &Self, log_store: &dyn LogStore, predicate: Option<PredicateRef>) -> BoxStream<DeltaResult<LogicalFileView>>` - Stream the active files in the snapshot
- `fn domain_metadata<impl ToString>(self: &Self, log_store: &dyn LogStore, domain: impl Trait) -> DeltaResult<Option<String>>` - Fetch the [domainMetadata] for a specific domain in this snapshot.

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Snapshot`
- **PartialEq**
  - `fn eq(self: &Self, other: &Snapshot) -> bool`
- **Deserialize**
  - `fn deserialize<D>(deserializer: D) -> Result<Self, <D as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **DataFusionMixins**
  - `fn read_schema(self: &Self) -> ArrowSchemaRef`
  - `fn input_schema(self: &Self) -> ArrowSchemaRef`
  - `fn parse_predicate_expression<impl AsRef<str>>(self: &Self, expr: impl Trait, session: &dyn Session) -> DeltaResult<Expr>`
- **Serialize**
  - `fn serialize<S>(self: &Self, serializer: S) -> Result<<S as >::Ok, <S as >::Error>`



