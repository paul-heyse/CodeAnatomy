**datafusion > physical_planner**

# Module: physical_planner

## Contents

**Structs**

- [`DefaultPhysicalPlanner`](#defaultphysicalplanner) - Default single node physical query planner that converts a

**Functions**

- [`create_aggregate_expr_and_maybe_filter`](#create_aggregate_expr_and_maybe_filter) - Create an aggregate expression from a logical expression or an alias
- [`create_aggregate_expr_with_name_and_maybe_filter`](#create_aggregate_expr_with_name_and_maybe_filter) - Create an aggregate expression with a name from a logical expression
- [`create_window_expr`](#create_window_expr) - Create a window expression from a logical expression or an alias
- [`create_window_expr_with_name`](#create_window_expr_with_name) - Create a window expression with a name from a logical expression
- [`is_window_frame_bound_valid`](#is_window_frame_bound_valid) - Check if window bounds are valid after schema information is available, and

**Traits**

- [`ExtensionPlanner`](#extensionplanner) - This trait exposes the ability to plan an [`ExecutionPlan`] out of a [`LogicalPlan`].
- [`PhysicalPlanner`](#physicalplanner) - Physical query planner that converts a `LogicalPlan` to an

---

## datafusion::physical_planner::DefaultPhysicalPlanner

*Struct*

Default single node physical query planner that converts a
`LogicalPlan` to an `ExecutionPlan` suitable for execution.

This planner will first flatten the `LogicalPlan` tree via a
depth first approach, which allows it to identify the leaves
of the tree.

Tasks are spawned from these leaves and traverse back up the
tree towards the root, converting each `LogicalPlan` node it
reaches into their equivalent `ExecutionPlan` node. When these
tasks reach a common node, they will terminate until the last
task reaches the node which will then continue building up the
tree.

Up to [`planning_concurrency`] tasks are buffered at once to
execute concurrently.

[`planning_concurrency`]: crate::config::ExecutionOptions::planning_concurrency

**Methods:**

- `fn with_extension_planners(extension_planners: Vec<Arc<dyn ExtensionPlanner>>) -> Self` - Create a physical planner that uses `extension_planners` to
- `fn optimize_physical_plan<F>(self: &Self, plan: Arc<dyn ExecutionPlan>, session_state: &SessionState, observer: F) -> Result<Arc<dyn ExecutionPlan>>` - Optimize a physical plan by applying each physical optimizer,

**Trait Implementations:**

- **PhysicalPlanner**
  - `fn create_physical_plan(self: &'life0 Self, logical_plan: &'life1 LogicalPlan, session_state: &'life2 SessionState) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - Create a physical plan from a logical plan
  - `fn create_physical_expr(self: &Self, expr: &Expr, input_dfschema: &DFSchema, session_state: &SessionState) -> Result<Arc<dyn PhysicalExpr>>` - Create a physical expression from a logical expression
- **Default**
  - `fn default() -> DefaultPhysicalPlanner`



## datafusion::physical_planner::ExtensionPlanner

*Trait*

This trait exposes the ability to plan an [`ExecutionPlan`] out of a [`LogicalPlan`].

**Methods:**

- `plan_extension`: Create a physical plan for a [`UserDefinedLogicalNode`].



## datafusion::physical_planner::PhysicalPlanner

*Trait*

Physical query planner that converts a `LogicalPlan` to an
`ExecutionPlan` suitable for execution.

**Methods:**

- `create_physical_plan`: Create a physical plan from a logical plan
- `create_physical_expr`: Create a physical expression from a logical expression



## datafusion::physical_planner::create_aggregate_expr_and_maybe_filter

*Function*

Create an aggregate expression from a logical expression or an alias

```rust
fn create_aggregate_expr_and_maybe_filter(e: &crate::logical_expr::Expr, logical_input_schema: &datafusion_common::DFSchema, physical_input_schema: &arrow::datatypes::Schema, execution_props: &crate::execution::context::ExecutionProps) -> crate::error::Result<(std::sync::Arc<datafusion_physical_expr::aggregate::AggregateFunctionExpr>, Option<std::sync::Arc<dyn PhysicalExpr>>, Vec<datafusion_physical_expr::PhysicalSortExpr>)>
```



## datafusion::physical_planner::create_aggregate_expr_with_name_and_maybe_filter

*Function*

Create an aggregate expression with a name from a logical expression

```rust
fn create_aggregate_expr_with_name_and_maybe_filter(e: &crate::logical_expr::Expr, name: Option<String>, human_displan: String, logical_input_schema: &datafusion_common::DFSchema, physical_input_schema: &arrow::datatypes::Schema, execution_props: &crate::execution::context::ExecutionProps) -> crate::error::Result<(std::sync::Arc<datafusion_physical_expr::aggregate::AggregateFunctionExpr>, Option<std::sync::Arc<dyn PhysicalExpr>>, Vec<datafusion_physical_expr::PhysicalSortExpr>)>
```



## datafusion::physical_planner::create_window_expr

*Function*

Create a window expression from a logical expression or an alias

```rust
fn create_window_expr(e: &crate::logical_expr::Expr, logical_schema: &datafusion_common::DFSchema, execution_props: &crate::execution::context::ExecutionProps) -> crate::error::Result<std::sync::Arc<dyn WindowExpr>>
```



## datafusion::physical_planner::create_window_expr_with_name

*Function*

Create a window expression with a name from a logical expression

```rust
fn create_window_expr_with_name<impl Into<String>>(e: &crate::logical_expr::Expr, name: impl Trait, logical_schema: &datafusion_common::DFSchema, execution_props: &crate::execution::context::ExecutionProps) -> crate::error::Result<std::sync::Arc<dyn WindowExpr>>
```



## datafusion::physical_planner::is_window_frame_bound_valid

*Function*

Check if window bounds are valid after schema information is available, and
window_frame bounds are casted to the corresponding column type.
queries like:
OVER (ORDER BY a RANGES BETWEEN 3 PRECEDING AND 5 PRECEDING)
OVER (ORDER BY a RANGES BETWEEN INTERVAL '3 DAY' PRECEDING AND '5 DAY' PRECEDING)  are rejected

```rust
fn is_window_frame_bound_valid(window_frame: &datafusion_expr::WindowFrame) -> bool
```



