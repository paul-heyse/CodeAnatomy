**datafusion_ffi > execution_plan**

# Module: execution_plan

## Contents

**Structs**

- [`ExecutionPlanPrivateData`](#executionplanprivatedata)
- [`FFI_ExecutionPlan`](#ffi_executionplan) - A stable struct for sharing a [`ExecutionPlan`] across FFI boundaries.
- [`ForeignExecutionPlan`](#foreignexecutionplan) - This struct is used to access an execution plan provided by a foreign

---

## datafusion_ffi::execution_plan::ExecutionPlanPrivateData

*Struct*

**Fields:**
- `plan: std::sync::Arc<dyn ExecutionPlan>`
- `runtime: Option<tokio::runtime::Handle>`



## datafusion_ffi::execution_plan::FFI_ExecutionPlan

*Struct*

A stable struct for sharing a [`ExecutionPlan`] across FFI boundaries.

**Fields:**
- `properties: fn(...)` - Return the plan properties
- `children: fn(...)` - Return a vector of children plans
- `name: fn(...)` - Return the plan name.
- `execute: fn(...)` - Execute the plan and return a record batch stream. Errors
- `clone: fn(...)` - Used to create a clone on the provider of the execution plan. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Methods:**

- `fn new(plan: Arc<dyn ExecutionPlan>, runtime: Option<Handle>) -> Self` - This function is called on the provider's side.

**Traits:** Sync, Send, GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Drop**
  - `fn drop(self: & mut Self)`



## datafusion_ffi::execution_plan::ForeignExecutionPlan

*Struct*

This struct is used to access an execution plan provided by a foreign
library across a FFI boundary.

The ForeignExecutionPlan is to be used by the caller of the plan, so it has
no knowledge or access to the private data. All interaction with the plan
must occur through the functions defined in FFI_ExecutionPlan.

**Traits:** Sync, Send

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ExecutionPlan**
  - `fn name(self: &Self) -> &str`
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn properties(self: &Self) -> &PlanProperties`
  - `fn children(self: &Self) -> Vec<&Arc<dyn ExecutionPlan>>`
  - `fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>>`
  - `fn execute(self: &Self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream>`
- **DisplayAs**
  - `fn fmt_as(self: &Self, t: DisplayFormatType, f: & mut std::fmt::Formatter) -> std::fmt::Result`



