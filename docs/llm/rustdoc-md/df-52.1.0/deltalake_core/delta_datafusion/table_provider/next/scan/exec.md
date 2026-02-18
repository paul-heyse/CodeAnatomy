**deltalake_core > delta_datafusion > table_provider > next > scan > exec**

# Module: delta_datafusion::table_provider::next::scan::exec

## Contents

**Structs**

- [`DeltaScanExec`](#deltascanexec) - Physical execution plan for scanning Delta tables.

---

## deltalake_core::delta_datafusion::table_provider::next::scan::exec::DeltaScanExec

*Struct*

Physical execution plan for scanning Delta tables.

Wraps a Parquet reader execution plan and applies Delta Lake protocol transformations
to produce the logical table data. This includes:

- **Column mapping**: Translates physical column names to logical names
- **Partition values**: Materializes partition column values from file paths
- **Deletion vectors**: Filters out deleted rows using per-file selection vectors
- **Schema evolution**: Handles missing columns and type coercion

# Data Flow

1. Inner [`input`](Self::input) plan reads raw Parquet data
2. Per-file [`transforms`](Self::transforms) convert physical to logical schema
3. [`selection_vectors`](Self::selection_vectors) filter deleted rows
4. Result is cast to [`result_schema`](KernelScanPlan::result_schema)

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **DisplayAs**
  - `fn fmt_as(self: &Self, t: DisplayFormatType, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **ExecutionPlan**
  - `fn name(self: &Self) -> &'static str`
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn properties(self: &Self) -> &PlanProperties`
  - `fn children(self: &Self) -> Vec<&Arc<dyn ExecutionPlan>>`
  - `fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>>`
  - `fn repartitioned(self: &Self, target_partitions: usize, config: &ConfigOptions) -> Result<Option<Arc<dyn ExecutionPlan>>>`
  - `fn execute(self: &Self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream>`
  - `fn metrics(self: &Self) -> Option<MetricsSet>`
  - `fn supports_limit_pushdown(self: &Self) -> bool`
  - `fn cardinality_effect(self: &Self) -> CardinalityEffect`
  - `fn fetch(self: &Self) -> Option<usize>`
  - `fn with_fetch(self: &Self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>>`
  - `fn statistics(self: &Self) -> Result<Statistics>`
  - `fn partition_statistics(self: &Self, partition: Option<usize>) -> Result<Statistics>`
  - `fn gather_filters_for_pushdown(self: &Self, _phase: FilterPushdownPhase, parent_filters: Vec<Arc<dyn PhysicalExpr>>, _config: &ConfigOptions) -> Result<FilterDescription>`
- **Clone**
  - `fn clone(self: &Self) -> DeltaScanExec`



