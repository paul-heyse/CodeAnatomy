**datafusion_ffi > session**

# Module: session

## Contents

**Modules**

- [`config`](#config)

**Structs**

- [`ForeignSession`](#foreignsession) - This wrapper struct exists on the receiver side of the FFI interface, so it has

---

## datafusion_ffi::session::ForeignSession

*Struct*

This wrapper struct exists on the receiver side of the FFI interface, so it has
no guarantees about being able to access the data in `private_data`. Any functions
defined on this struct must only use the stable functions provided in
FFI_Session to interact with the foreign table provider.

**Traits:** Sync, Send

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Session**
  - `fn session_id(self: &Self) -> &str`
  - `fn config(self: &Self) -> &SessionConfig`
  - `fn config_options(self: &Self) -> &ConfigOptions`
  - `fn create_physical_plan(self: &'life0 Self, logical_plan: &'life1 LogicalPlan) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn create_physical_expr(self: &Self, expr: Expr, df_schema: &DFSchema) -> datafusion_common::Result<Arc<dyn PhysicalExpr>>`
  - `fn scalar_functions(self: &Self) -> &HashMap<String, Arc<ScalarUDF>>`
  - `fn aggregate_functions(self: &Self) -> &HashMap<String, Arc<AggregateUDF>>`
  - `fn window_functions(self: &Self) -> &HashMap<String, Arc<WindowUDF>>`
  - `fn runtime_env(self: &Self) -> &Arc<RuntimeEnv>`
  - `fn execution_props(self: &Self) -> &ExecutionProps`
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn table_options(self: &Self) -> &TableOptions`
  - `fn default_table_options(self: &Self) -> TableOptions`
  - `fn table_options_mut(self: & mut Self) -> & mut TableOptions`
  - `fn task_ctx(self: &Self) -> Arc<TaskContext>`



## Module: config



