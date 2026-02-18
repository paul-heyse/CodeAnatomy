**datafusion > execution > session_state_defaults**

# Module: execution::session_state_defaults

## Contents

**Structs**

- [`SessionStateDefaults`](#sessionstatedefaults) - Defaults that are used as part of creating a SessionState such as table providers,

---

## datafusion::execution::session_state_defaults::SessionStateDefaults

*Struct*

Defaults that are used as part of creating a SessionState such as table providers,
file formats, registering of builtin functions, etc.

**Methods:**

- `fn default_table_factories() -> HashMap<String, Arc<dyn TableProviderFactory>>` - returns a map of the default [`TableProviderFactory`]s
- `fn default_catalog(config: &SessionConfig, table_factories: &HashMap<String, Arc<dyn TableProviderFactory>>, runtime: &Arc<RuntimeEnv>) -> MemoryCatalogProvider` - returns the default MemoryCatalogProvider
- `fn default_expr_planners() -> Vec<Arc<dyn ExprPlanner>>` - returns the list of default [`ExprPlanner`]s
- `fn default_scalar_functions() -> Vec<Arc<ScalarUDF>>` - returns the list of default [`ScalarUDF`]s
- `fn default_aggregate_functions() -> Vec<Arc<AggregateUDF>>` - returns the list of default [`AggregateUDF`]s
- `fn default_window_functions() -> Vec<Arc<WindowUDF>>` - returns the list of default [`WindowUDF`]s
- `fn default_table_functions() -> Vec<Arc<TableFunction>>` - returns the list of default [`TableFunction`]s
- `fn default_file_formats() -> Vec<Arc<dyn FileFormatFactory>>` - returns the list of default [`FileFormatFactory`]s
- `fn register_builtin_functions(state: & mut SessionState)` - registers all builtin functions - scalar, array and aggregate
- `fn register_scalar_functions(state: & mut SessionState)` - registers all the builtin scalar functions
- `fn register_array_functions(state: & mut SessionState)` - registers all the builtin array functions
- `fn register_aggregate_functions(state: & mut SessionState)` - registers all the builtin aggregate functions
- `fn register_default_schema(config: &SessionConfig, table_factories: &HashMap<String, Arc<dyn TableProviderFactory>>, runtime: &Arc<RuntimeEnv>, default_catalog: &MemoryCatalogProvider)` - registers the default schema
- `fn register_default_file_formats(state: & mut SessionState)` - registers the default [`FileFormatFactory`]s



