**datafusion > execution > session_state**

# Module: execution::session_state

## Contents

**Structs**

- [`SessionState`](#sessionstate) - `SessionState` contains all the necessary state to plan and execute queries,
- [`SessionStateBuilder`](#sessionstatebuilder) - A builder to be used for building [`SessionState`]'s. Defaults will

**Traits**

- [`CacheFactory`](#cachefactory) - A [`CacheFactory`] can be registered via [`SessionState`]

---

## datafusion::execution::session_state::CacheFactory

*Trait*

A [`CacheFactory`] can be registered via [`SessionState`]
to create a custom logical plan for [`crate::dataframe::DataFrame::cache`].
Additionally, a custom [`crate::physical_planner::ExtensionPlanner`]/[`QueryPlanner`]
may need to be implemented to handle such plans.

**Methods:**

- `create`: Create a logical plan for caching



## datafusion::execution::session_state::SessionState

*Struct*

`SessionState` contains all the necessary state to plan and execute queries,
such as configuration, functions, and runtime environment. Please see the
documentation on [`SessionContext`] for more information.


# Example: `SessionState` from a [`SessionContext`]

```
use datafusion::prelude::*;
let ctx = SessionContext::new();
let state = ctx.state();
```

# Example: `SessionState` via [`SessionStateBuilder`]

You can also use [`SessionStateBuilder`] to build a `SessionState` object
directly:

```
use datafusion::prelude::*;
# use datafusion::{error::Result, assert_batches_eq};
# use datafusion::execution::session_state::SessionStateBuilder;
# use datafusion_execution::runtime_env::RuntimeEnv;
# use std::sync::Arc;
# #[tokio::main]
# async fn main() -> Result<()> {
let state = SessionStateBuilder::new()
    .with_config(SessionConfig::new())
    .with_runtime_env(Arc::new(RuntimeEnv::default()))
    .with_default_features()
    .build();
Ok(())
# }
```

Note that there is no `Default` or `new()` for SessionState,
to avoid accidentally running queries or other operations without passing through
the [`SessionConfig`] or [`RuntimeEnv`]. See [`SessionStateBuilder`] and
[`SessionContext`].

[`SessionContext`]: crate::execution::context::SessionContext

**Methods:**

- `fn schema_for_ref<impl Into<TableReference>>(self: &Self, table_ref: impl Trait) -> datafusion_common::Result<Arc<dyn SchemaProvider>>` - Retrieve the [`SchemaProvider`] for a specific [`TableReference`], if it
- `fn add_analyzer_rule(self: & mut Self, analyzer_rule: Arc<dyn AnalyzerRule>) -> &Self` - Add `analyzer_rule` to the end of the list of
- `fn set_function_factory(self: & mut Self, function_factory: Arc<dyn FunctionFactory>)` - Registers a [`FunctionFactory`] to handle `CREATE FUNCTION` statements
- `fn function_factory(self: &Self) -> Option<&Arc<dyn FunctionFactory>>` - Get the function factory
- `fn set_cache_factory(self: & mut Self, cache_factory: Arc<dyn CacheFactory>)` - Register a [`CacheFactory`] for custom caching strategy
- `fn cache_factory(self: &Self) -> Option<&Arc<dyn CacheFactory>>` - Get the cache factory
- `fn table_factories(self: &Self) -> &HashMap<String, Arc<dyn TableProviderFactory>>` - Get the table factories
- `fn table_factories_mut(self: & mut Self) -> & mut HashMap<String, Arc<dyn TableProviderFactory>>` - Get the table factories
- `fn sql_to_statement(self: &Self, sql: &str, dialect: &Dialect) -> datafusion_common::Result<Statement>` - Parse an SQL string into an DataFusion specific AST
- `fn sql_to_expr(self: &Self, sql: &str, dialect: &Dialect) -> datafusion_common::Result<SQLExpr>` - parse a sql string into a sqlparser-rs AST [`SQLExpr`].
- `fn sql_to_expr_with_alias(self: &Self, sql: &str, dialect: &Dialect) -> datafusion_common::Result<SQLExprWithAlias>` - parse a sql string into a sqlparser-rs AST [`SQLExprWithAlias`].
- `fn resolve_table_references(self: &Self, statement: &Statement) -> datafusion_common::Result<Vec<TableReference>>` - Resolve all table references in the SQL statement. Does not include CTE references.
- `fn statement_to_plan(self: &Self, statement: Statement) -> datafusion_common::Result<LogicalPlan>` - Convert an AST Statement into a LogicalPlan
- `fn create_logical_plan(self: &Self, sql: &str) -> datafusion_common::Result<LogicalPlan>` - Creates a [`LogicalPlan`] from the provided SQL string. This
- `fn create_logical_expr(self: &Self, sql: &str, df_schema: &DFSchema) -> datafusion_common::Result<Expr>` - Creates a datafusion style AST [`Expr`] from a SQL string.
- `fn create_logical_expr_from_sql_expr(self: &Self, sql_expr: SQLExprWithAlias, df_schema: &DFSchema) -> datafusion_common::Result<Expr>` - Creates a datafusion style AST [`Expr`] from a SQL expression.
- `fn analyzer(self: &Self) -> &Analyzer` - Returns the [`Analyzer`] for this session
- `fn optimizer(self: &Self) -> &Optimizer` - Returns the [`Optimizer`] for this session
- `fn expr_planners(self: &Self) -> &[Arc<dyn ExprPlanner>]` - Returns the [`ExprPlanner`]s for this session
- `fn relation_planners(self: &Self) -> &[Arc<dyn RelationPlanner>]` - Returns the registered relation planners in priority order.
- `fn register_relation_planner(self: & mut Self, planner: Arc<dyn RelationPlanner>) -> datafusion_common::Result<()>` - Registers a [`RelationPlanner`] to customize SQL relation planning.
- `fn query_planner(self: &Self) -> &Arc<dyn QueryPlanner>` - Returns the [`QueryPlanner`] for this session
- `fn optimize(self: &Self, plan: &LogicalPlan) -> datafusion_common::Result<LogicalPlan>` - Optimizes the logical plan by applying optimizer rules.
- `fn create_physical_plan(self: &Self, logical_plan: &LogicalPlan) -> datafusion_common::Result<Arc<dyn ExecutionPlan>>` - Creates a physical [`ExecutionPlan`] plan from a [`LogicalPlan`].
- `fn create_physical_expr(self: &Self, expr: Expr, df_schema: &DFSchema) -> datafusion_common::Result<Arc<dyn PhysicalExpr>>` - Create a [`PhysicalExpr`] from an [`Expr`] after applying type
- `fn session_id(self: &Self) -> &str` - Return the session ID
- `fn runtime_env(self: &Self) -> &Arc<RuntimeEnv>` - Return the runtime env
- `fn execution_props(self: &Self) -> &ExecutionProps` - Return the execution properties
- `fn execution_props_mut(self: & mut Self) -> & mut ExecutionProps` - Return mutable execution properties
- `fn config(self: &Self) -> &SessionConfig` - Return the [`SessionConfig`]
- `fn config_mut(self: & mut Self) -> & mut SessionConfig` - Return the mutable [`SessionConfig`].
- `fn optimizers(self: &Self) -> &[Arc<dyn OptimizerRule>]` - Return the logical optimizers
- `fn physical_optimizers(self: &Self) -> &[Arc<dyn PhysicalOptimizerRule>]` - Return the physical optimizers
- `fn config_options(self: &Self) -> &Arc<ConfigOptions>` - return the configuration options
- `fn mark_start_execution(self: & mut Self)` - Mark the start of the execution
- `fn table_options(self: &Self) -> &TableOptions` - Return the table options
- `fn default_table_options(self: &Self) -> TableOptions` - return the TableOptions options with its extensions
- `fn table_options_mut(self: & mut Self) -> & mut TableOptions` - Returns a mutable reference to [`TableOptions`]
- `fn register_table_options_extension<T>(self: & mut Self, extension: T)` - Registers a [`ConfigExtension`] as a table option extension that can be
- `fn register_file_format(self: & mut Self, file_format: Arc<dyn FileFormatFactory>, overwrite: bool) -> Result<(), DataFusionError>` - Adds or updates a [FileFormatFactory] which can be used with COPY TO or
- `fn get_file_format_factory(self: &Self, ext: &str) -> Option<Arc<dyn FileFormatFactory>>` - Retrieves a [FileFormatFactory] based on file extension which has been registered
- `fn task_ctx(self: &Self) -> Arc<TaskContext>` - Get a new TaskContext to run in this session
- `fn catalog_list(self: &Self) -> &Arc<dyn CatalogProviderList>` - Return catalog list
- `fn scalar_functions(self: &Self) -> &HashMap<String, Arc<ScalarUDF>>` - Return reference to scalar_functions
- `fn aggregate_functions(self: &Self) -> &HashMap<String, Arc<AggregateUDF>>` - Return reference to aggregate_functions
- `fn window_functions(self: &Self) -> &HashMap<String, Arc<WindowUDF>>` - Return reference to window functions
- `fn table_functions(self: &Self) -> &HashMap<String, Arc<TableFunction>>` - Return reference to table_functions
- `fn serializer_registry(self: &Self) -> &Arc<dyn SerializerRegistry>` - Return [SerializerRegistry] for extensions
- `fn version(self: &Self) -> &str` - Return version of the cargo package that produced this query
- `fn register_udtf(self: & mut Self, name: &str, fun: Arc<dyn TableFunctionImpl>)` - Register a user defined table function
- `fn deregister_udtf(self: & mut Self, name: &str) -> datafusion_common::Result<Option<Arc<dyn TableFunctionImpl>>>` - Deregister a user defined table function

**Trait Implementations:**

- **OptimizerConfig**
  - `fn query_execution_start_time(self: &Self) -> DateTime<Utc>`
  - `fn alias_generator(self: &Self) -> &Arc<AliasGenerator>`
  - `fn options(self: &Self) -> Arc<ConfigOptions>`
  - `fn function_registry(self: &Self) -> Option<&dyn FunctionRegistry>`
- **FunctionRegistry**
  - `fn udfs(self: &Self) -> HashSet<String>`
  - `fn udf(self: &Self, name: &str) -> datafusion_common::Result<Arc<ScalarUDF>>`
  - `fn udaf(self: &Self, name: &str) -> datafusion_common::Result<Arc<AggregateUDF>>`
  - `fn udwf(self: &Self, name: &str) -> datafusion_common::Result<Arc<WindowUDF>>`
  - `fn register_udf(self: & mut Self, udf: Arc<ScalarUDF>) -> datafusion_common::Result<Option<Arc<ScalarUDF>>>`
  - `fn register_udaf(self: & mut Self, udaf: Arc<AggregateUDF>) -> datafusion_common::Result<Option<Arc<AggregateUDF>>>`
  - `fn register_udwf(self: & mut Self, udwf: Arc<WindowUDF>) -> datafusion_common::Result<Option<Arc<WindowUDF>>>`
  - `fn deregister_udf(self: & mut Self, name: &str) -> datafusion_common::Result<Option<Arc<ScalarUDF>>>`
  - `fn deregister_udaf(self: & mut Self, name: &str) -> datafusion_common::Result<Option<Arc<AggregateUDF>>>`
  - `fn deregister_udwf(self: & mut Self, name: &str) -> datafusion_common::Result<Option<Arc<WindowUDF>>>`
  - `fn register_function_rewrite(self: & mut Self, rewrite: Arc<dyn FunctionRewrite>) -> datafusion_common::Result<()>`
  - `fn expr_planners(self: &Self) -> Vec<Arc<dyn ExprPlanner>>`
  - `fn register_expr_planner(self: & mut Self, expr_planner: Arc<dyn ExprPlanner>) -> datafusion_common::Result<()>`
  - `fn udafs(self: &Self) -> HashSet<String>`
  - `fn udwfs(self: &Self) -> HashSet<String>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result` - Prefer having short fields at the top and long vector fields near the end
- **TaskContextProvider**
  - `fn task_ctx(self: &Self) -> Arc<TaskContext>`
- **Session**
  - `fn session_id(self: &Self) -> &str`
  - `fn config(self: &Self) -> &SessionConfig`
  - `fn create_physical_plan(self: &'life0 Self, logical_plan: &'life1 LogicalPlan) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn create_physical_expr(self: &Self, expr: Expr, df_schema: &DFSchema) -> datafusion_common::Result<Arc<dyn PhysicalExpr>>`
  - `fn scalar_functions(self: &Self) -> &HashMap<String, Arc<ScalarUDF>>`
  - `fn aggregate_functions(self: &Self) -> &HashMap<String, Arc<AggregateUDF>>`
  - `fn window_functions(self: &Self) -> &HashMap<String, Arc<WindowUDF>>`
  - `fn runtime_env(self: &Self) -> &Arc<RuntimeEnv>`
  - `fn execution_props(self: &Self) -> &ExecutionProps`
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn table_options(self: &Self) -> &TableOptions`
  - `fn table_options_mut(self: & mut Self) -> & mut TableOptions`
  - `fn task_ctx(self: &Self) -> Arc<TaskContext>`
- **Clone**
  - `fn clone(self: &Self) -> SessionState`



## datafusion::execution::session_state::SessionStateBuilder

*Struct*

A builder to be used for building [`SessionState`]'s. Defaults will
be used for all values unless explicitly provided.

See example on [`SessionState`]

**Methods:**

- `fn new() -> Self` - Returns a new empty [`SessionStateBuilder`].
- `fn new_from_existing(existing: SessionState) -> Self` - Returns a new [SessionStateBuilder] based on an existing [SessionState].
- `fn with_default_features(self: Self) -> Self` - Adds defaults for table_factories, file formats, expr_planners and builtin
- `fn new_with_default_features() -> Self` - Returns a new [`SessionStateBuilder`] with default features.
- `fn with_session_id(self: Self, session_id: String) -> Self` - Set the session id.
- `fn with_analyzer_rules(self: Self, rules: Vec<Arc<dyn AnalyzerRule>>) -> Self` - Set the [`AnalyzerRule`]s optimizer plan rules.
- `fn with_analyzer_rule(self: Self, analyzer_rule: Arc<dyn AnalyzerRule>) -> Self` - Add `analyzer_rule` to the end of the list of
- `fn with_optimizer_rules(self: Self, rules: Vec<Arc<dyn OptimizerRule>>) -> Self` - Set the [`OptimizerRule`]s used to optimize plans.
- `fn with_optimizer_rule(self: Self, optimizer_rule: Arc<dyn OptimizerRule>) -> Self` - Add `optimizer_rule` to the end of the list of
- `fn with_expr_planners(self: Self, expr_planners: Vec<Arc<dyn ExprPlanner>>) -> Self` - Set the [`ExprPlanner`]s used to customize the behavior of the SQL planner.
- `fn with_relation_planners(self: Self, relation_planners: Vec<Arc<dyn RelationPlanner>>) -> Self` - Sets the [`RelationPlanner`]s used to customize SQL relation planning.
- `fn with_type_planner(self: Self, type_planner: Arc<dyn TypePlanner>) -> Self` - Set the [`TypePlanner`] used to customize the behavior of the SQL planner.
- `fn with_physical_optimizer_rules(self: Self, physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule>>) -> Self` - Set the [`PhysicalOptimizerRule`]s used to optimize plans.
- `fn with_physical_optimizer_rule(self: Self, physical_optimizer_rule: Arc<dyn PhysicalOptimizerRule>) -> Self` - Add `physical_optimizer_rule` to the end of the list of
- `fn with_query_planner(self: Self, query_planner: Arc<dyn QueryPlanner>) -> Self` - Set the [`QueryPlanner`]
- `fn with_catalog_list(self: Self, catalog_list: Arc<dyn CatalogProviderList>) -> Self` - Set the [`CatalogProviderList`]
- `fn with_table_functions(self: Self, table_functions: HashMap<String, Arc<TableFunction>>) -> Self` - Set the map of [`TableFunction`]s
- `fn with_table_function_list(self: Self, table_functions: Vec<Arc<TableFunction>>) -> Self` - Set the list of [`TableFunction`]s
- `fn with_scalar_functions(self: Self, scalar_functions: Vec<Arc<ScalarUDF>>) -> Self` - Set the map of [`ScalarUDF`]s
- `fn with_aggregate_functions(self: Self, aggregate_functions: Vec<Arc<AggregateUDF>>) -> Self` - Set the map of [`AggregateUDF`]s
- `fn with_window_functions(self: Self, window_functions: Vec<Arc<WindowUDF>>) -> Self` - Set the map of [`WindowUDF`]s
- `fn with_serializer_registry(self: Self, serializer_registry: Arc<dyn SerializerRegistry>) -> Self` - Set the [`SerializerRegistry`]
- `fn with_file_formats(self: Self, file_formats: Vec<Arc<dyn FileFormatFactory>>) -> Self` - Set the map of [`FileFormatFactory`]s
- `fn with_config(self: Self, config: SessionConfig) -> Self` - Set the [`SessionConfig`]
- `fn with_table_options(self: Self, table_options: TableOptions) -> Self` - Set the [`TableOptions`]
- `fn with_execution_props(self: Self, execution_props: ExecutionProps) -> Self` - Set the [`ExecutionProps`]
- `fn with_table_factory(self: Self, key: String, table_factory: Arc<dyn TableProviderFactory>) -> Self` - Add a [`TableProviderFactory`] to the map of factories
- `fn with_table_factories(self: Self, table_factories: HashMap<String, Arc<dyn TableProviderFactory>>) -> Self` - Set the map of [`TableProviderFactory`]s
- `fn with_runtime_env(self: Self, runtime_env: Arc<RuntimeEnv>) -> Self` - Set the [`RuntimeEnv`]
- `fn with_function_factory(self: Self, function_factory: Option<Arc<dyn FunctionFactory>>) -> Self` - Set a [`FunctionFactory`] to handle `CREATE FUNCTION` statements
- `fn with_cache_factory(self: Self, cache_factory: Option<Arc<dyn CacheFactory>>) -> Self` - Set a [`CacheFactory`] for custom caching strategy
- `fn with_object_store(self: Self, url: &Url, object_store: Arc<dyn ObjectStore>) -> Self` - Register an `ObjectStore` to the [`RuntimeEnv`]. See [`RuntimeEnv::register_object_store`]
- `fn build(self: Self) -> SessionState` - Builds a [`SessionState`] with the current configuration.
- `fn session_id(self: &Self) -> &Option<String>` - Returns the current session_id value
- `fn analyzer(self: & mut Self) -> & mut Option<Analyzer>` - Returns the current analyzer value
- `fn expr_planners(self: & mut Self) -> & mut Option<Vec<Arc<dyn ExprPlanner>>>` - Returns the current expr_planners value
- `fn relation_planners(self: & mut Self) -> & mut Option<Vec<Arc<dyn RelationPlanner>>>` - Returns a mutable reference to the current [`RelationPlanner`] list.
- `fn type_planner(self: & mut Self) -> & mut Option<Arc<dyn TypePlanner>>` - Returns the current type_planner value
- `fn optimizer(self: & mut Self) -> & mut Option<Optimizer>` - Returns the current optimizer value
- `fn physical_optimizers(self: & mut Self) -> & mut Option<PhysicalOptimizer>` - Returns the current physical_optimizers value
- `fn query_planner(self: & mut Self) -> & mut Option<Arc<dyn QueryPlanner>>` - Returns the current query_planner value
- `fn catalog_list(self: & mut Self) -> & mut Option<Arc<dyn CatalogProviderList>>` - Returns the current catalog_list value
- `fn table_functions(self: & mut Self) -> & mut Option<HashMap<String, Arc<TableFunction>>>` - Returns the current table_functions value
- `fn scalar_functions(self: & mut Self) -> & mut Option<Vec<Arc<ScalarUDF>>>` - Returns the current scalar_functions value
- `fn aggregate_functions(self: & mut Self) -> & mut Option<Vec<Arc<AggregateUDF>>>` - Returns the current aggregate_functions value
- `fn window_functions(self: & mut Self) -> & mut Option<Vec<Arc<WindowUDF>>>` - Returns the current window_functions value
- `fn serializer_registry(self: & mut Self) -> & mut Option<Arc<dyn SerializerRegistry>>` - Returns the current serializer_registry value
- `fn file_formats(self: & mut Self) -> & mut Option<Vec<Arc<dyn FileFormatFactory>>>` - Returns the current file_formats value
- `fn config(self: & mut Self) -> & mut Option<SessionConfig>` - Returns the current session_config value
- `fn table_options(self: & mut Self) -> & mut Option<TableOptions>` - Returns the current table_options value
- `fn execution_props(self: & mut Self) -> & mut Option<ExecutionProps>` - Returns the current execution_props value
- `fn table_factories(self: & mut Self) -> & mut Option<HashMap<String, Arc<dyn TableProviderFactory>>>` - Returns the current table_factories value
- `fn runtime_env(self: & mut Self) -> & mut Option<Arc<RuntimeEnv>>` - Returns the current runtime_env value
- `fn function_factory(self: & mut Self) -> & mut Option<Arc<dyn FunctionFactory>>` - Returns the current function_factory value
- `fn cache_factory(self: & mut Self) -> & mut Option<Arc<dyn CacheFactory>>` - Returns the cache factory
- `fn analyzer_rules(self: & mut Self) -> & mut Option<Vec<Arc<dyn AnalyzerRule>>>` - Returns the current analyzer_rules value
- `fn optimizer_rules(self: & mut Self) -> & mut Option<Vec<Arc<dyn OptimizerRule>>>` - Returns the current optimizer_rules value
- `fn physical_optimizer_rules(self: & mut Self) -> & mut Option<Vec<Arc<dyn PhysicalOptimizerRule>>>` - Returns the current physical_optimizer_rules value

**Trait Implementations:**

- **From**
  - `fn from(state: SessionState) -> Self`
- **Default**
  - `fn default() -> Self`
- **From**
  - `fn from(session: SessionContext) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result` - Prefer having short fields at the top and long vector fields near the end



