**deltalake_core > delta_datafusion**

# Module: delta_datafusion

## Contents

**Modules**

- [`cdf`](#cdf) - Logical operators and physical executions for CDF
- [`engine`](#engine)
- [`expr`](#expr) - Utility functions for Datafusion's Expressions
- [`logical`](#logical) - Logical Operations for DataFusion
- [`physical`](#physical) - Physical Operations for DataFusion
- [`planner`](#planner) - Custom planners for datafusion so that you can convert custom nodes, can be used

**Structs**

- [`DeltaColumn`](#deltacolumn) - A wrapper for Deltafusion's Column to preserve case-sensitivity during string conversion
- [`DeltaLogicalCodec`](#deltalogicalcodec) - Does serde on DeltaTables
- [`DeltaPhysicalCodec`](#deltaphysicalcodec) - A codec for deltalake physical plans
- [`DeltaTableFactory`](#deltatablefactory) - Responsible for creating deltatables

**Traits**

- [`DataFusionMixins`](#datafusionmixins) - Convenience trait for calling common methods on snapshot hierarchies

---

## deltalake_core::delta_datafusion::DataFusionMixins

*Trait*

Convenience trait for calling common methods on snapshot hierarchies

**Methods:**

- `read_schema`: The physical datafusion schema of a table
- `input_schema`: Get the table schema as an [`ArrowSchemaRef`]
- `parse_predicate_expression`: Parse an expression string into a datafusion [`Expr`]



## deltalake_core::delta_datafusion::DeltaColumn

*Struct*

A wrapper for Deltafusion's Column to preserve case-sensitivity during string conversion

**Trait Implementations:**

- **From**
  - `fn from(c: &String) -> Self`
- **From**
  - `fn from(c: Column) -> Self`
- **From**
  - `fn from(c: String) -> Self`
- **From**
  - `fn from(c: &str) -> Self`



## deltalake_core::delta_datafusion::DeltaLogicalCodec

*Struct*

Does serde on DeltaTables

**Trait Implementations:**

- **LogicalExtensionCodec**
  - `fn try_decode(self: &Self, _buf: &[u8], _inputs: &[LogicalPlan], _ctx: &TaskContext) -> Result<Extension, DataFusionError>`
  - `fn try_encode(self: &Self, _node: &Extension, _buf: & mut Vec<u8>) -> Result<(), DataFusionError>`
  - `fn try_decode_table_provider(self: &Self, buf: &[u8], _table_ref: &TableReference, _schema: SchemaRef, _ctx: &TaskContext) -> Result<Arc<dyn TableProvider>, DataFusionError>`
  - `fn try_encode_table_provider(self: &Self, _table_ref: &TableReference, node: Arc<dyn TableProvider>, buf: & mut Vec<u8>) -> Result<(), DataFusionError>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::delta_datafusion::DeltaPhysicalCodec

*Struct*

A codec for deltalake physical plans

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PhysicalExtensionCodec**
  - `fn try_decode(self: &Self, buf: &[u8], inputs: &[Arc<dyn ExecutionPlan>], _registry: &TaskContext) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>`
  - `fn try_encode(self: &Self, node: Arc<dyn ExecutionPlan>, buf: & mut Vec<u8>) -> Result<(), DataFusionError>`



## deltalake_core::delta_datafusion::DeltaTableFactory

*Struct*

Responsible for creating deltatables

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TableProviderFactory**
  - `fn create(self: &'life0 Self, ctx: &'life1 dyn Session, cmd: &'life2 CreateExternalTable) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`



## Module: cdf

Logical operators and physical executions for CDF



## Module: engine



## Module: expr

Utility functions for Datafusion's Expressions



## Module: logical

Logical Operations for DataFusion



## Module: physical

Physical Operations for DataFusion



## Module: planner

Custom planners for datafusion so that you can convert custom nodes, can be used
to trace custom metrics in an operation

# Example

#[derive(Clone)]
struct MergeMetricExtensionPlanner {}

#[macro@async_trait]
impl ExtensionPlanner for MergeMetricExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {}

let merge_planner = DeltaPlanner::<MergeMetricExtensionPlanner> {
    extension_planner: MergeMetricExtensionPlanner {}
};

let state = state.with_query_planner(Arc::new(merge_planner));



