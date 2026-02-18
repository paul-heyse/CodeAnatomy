**datafusion_expr > execution_props**

# Module: execution_props

## Contents

**Structs**

- [`ExecutionProps`](#executionprops) - Holds per-query execution properties and data (such as statement

---

## datafusion_expr::execution_props::ExecutionProps

*Struct*

Holds per-query execution properties and data (such as statement
starting timestamps).

An [`ExecutionProps`] is created each time a `LogicalPlan` is
prepared for execution (optimized). If the same plan is optimized
multiple times, a new `ExecutionProps` is created each time.

It is important that this structure be cheap to create as it is
done so during predicate pruning and expression simplification

**Fields:**
- `query_execution_start_time: chrono::DateTime<chrono::Utc>`
- `alias_generator: std::sync::Arc<datafusion_common::alias::AliasGenerator>` - Alias generator used by subquery optimizer rules
- `config_options: Option<std::sync::Arc<datafusion_common::config::ConfigOptions>>` - Snapshot of config options when the query started
- `var_providers: Option<datafusion_common::HashMap<crate::var_provider::VarType, std::sync::Arc<dyn VarProvider>>>` - Providers for scalar variables

**Methods:**

- `fn new() -> Self` - Creates a new execution props
- `fn with_query_execution_start_time(self: Self, query_execution_start_time: DateTime<Utc>) -> Self` - Set the query execution start time to use
- `fn start_execution(self: & mut Self) -> &Self`
- `fn mark_start_execution(self: & mut Self, config_options: Arc<ConfigOptions>) -> &Self` - Marks the execution of query started timestamp.
- `fn add_var_provider(self: & mut Self, var_type: VarType, provider: Arc<dyn VarProvider>) -> Option<Arc<dyn VarProvider>>` - Registers a variable provider, returning the existing
- `fn get_var_provider(self: &Self, var_type: VarType) -> Option<Arc<dyn VarProvider>>` - Returns the provider for the `var_type`, if any
- `fn config_options(self: &Self) -> Option<&Arc<ConfigOptions>>` - Returns the configuration properties for this execution

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> ExecutionProps`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self`



