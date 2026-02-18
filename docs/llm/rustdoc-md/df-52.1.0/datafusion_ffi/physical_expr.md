**datafusion_ffi > physical_expr**

# Module: physical_expr

## Contents

**Structs**

- [`FFI_PhysicalExpr`](#ffi_physicalexpr)
- [`ForeignPhysicalExpr`](#foreignphysicalexpr) - This wrapper struct exists on the receiver side of the FFI interface, so it has

---

## datafusion_ffi::physical_expr::FFI_PhysicalExpr

*Struct*

**Fields:**
- `data_type: fn(...)`
- `nullable: fn(...)`
- `evaluate: fn(...)`
- `return_field: fn(...)`
- `evaluate_selection: fn(...)`
- `children: fn(...)`
- `new_with_children: fn(...)`
- `evaluate_bounds: fn(...)`
- `propagate_constraints: fn(...)`
- `evaluate_statistics: fn(...)`
- `propagate_statistics: fn(...)`
- `get_properties: fn(...)`
- `fmt_sql: fn(...)`
- `snapshot: fn(...)`
- `snapshot_generation: fn(...)`
- `is_volatile_node: fn(...)`
- `display: fn(...)`
- `hash: fn(...)`
- `clone: fn(...)` - Used to create a clone on the provider of the execution plan. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `version: fn(...)` - Return the major DataFusion version number of this provider.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Traits:** Sync, Send, GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Self`
- **Drop**
  - `fn drop(self: & mut Self)`
- **From**
  - `fn from(expr: Arc<dyn PhysicalExpr>) -> Self` - Creates a new [`FFI_PhysicalExpr`].
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_ffi::physical_expr::ForeignPhysicalExpr

*Struct*

This wrapper struct exists on the receiver side of the FFI interface, so it has
no guarantees about being able to access the data in `private_data`. Any functions
defined on this struct must only use the stable functions provided in
FFI_PhysicalExpr to interact with the expression.

**Traits:** Eq, Sync, Send

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PhysicalExpr**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn data_type(self: &Self, input_schema: &Schema) -> Result<DataType>`
  - `fn nullable(self: &Self, input_schema: &Schema) -> Result<bool>`
  - `fn evaluate(self: &Self, batch: &RecordBatch) -> Result<ColumnarValue>`
  - `fn return_field(self: &Self, input_schema: &Schema) -> Result<FieldRef>`
  - `fn evaluate_selection(self: &Self, batch: &RecordBatch, selection: &BooleanArray) -> Result<ColumnarValue>`
  - `fn children(self: &Self) -> Vec<&Arc<dyn PhysicalExpr>>`
  - `fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn PhysicalExpr>>`
  - `fn evaluate_bounds(self: &Self, children: &[&Interval]) -> Result<Interval>`
  - `fn propagate_constraints(self: &Self, interval: &Interval, children: &[&Interval]) -> Result<Option<Vec<Interval>>>`
  - `fn evaluate_statistics(self: &Self, children: &[&Distribution]) -> Result<Distribution>`
  - `fn propagate_statistics(self: &Self, parent: &Distribution, children: &[&Distribution]) -> Result<Option<Vec<Distribution>>>`
  - `fn get_properties(self: &Self, children: &[ExprProperties]) -> Result<ExprProperties>`
  - `fn fmt_sql(self: &Self, f: & mut Formatter) -> std::fmt::Result`
  - `fn snapshot(self: &Self) -> Result<Option<Arc<dyn PhysicalExpr>>>`
  - `fn snapshot_generation(self: &Self) -> u64`
  - `fn is_volatile_node(self: &Self) -> bool`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`



