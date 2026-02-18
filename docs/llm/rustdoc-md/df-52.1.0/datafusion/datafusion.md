**datafusion**

# Module: datafusion

## Contents

**Modules**

- [`catalog`](#catalog) - re-export of [`datafusion_catalog`] crate
- [`common`](#common) - re-export of [`datafusion_common`] crate
- [`dataframe`](#dataframe) - [`DataFrame`] API for building and executing query plans.
- [`datasource`](#datasource) - DataFusion data sources: [`TableProvider`] and [`ListingTable`]
- [`error`](#error) - DataFusion error type [`DataFusionError`] and [`Result`].
- [`execution`](#execution) - Shared state for query planning and execution.
- [`functions`](#functions) - re-export of [`datafusion_functions`] crate
- [`functions_aggregate`](#functions_aggregate) - re-export of [`datafusion_functions_aggregate`] crate
- [`functions_nested`](#functions_nested) - re-export of [`datafusion_functions_nested`] crate, if "nested_expressions" feature is enabled
- [`functions_table`](#functions_table) - re-export of [`datafusion_functions_table`] crate
- [`functions_window`](#functions_window) - re-export of [`datafusion_functions_window`] crate
- [`logical_expr`](#logical_expr) - re-export of [`datafusion_expr`] crate
- [`logical_expr_common`](#logical_expr_common) - re-export of [`datafusion_expr_common`] crate
- [`optimizer`](#optimizer) - re-export of [`datafusion_optimizer`] crate
- [`physical_expr`](#physical_expr) - re-export of [`datafusion_physical_expr`] crate
- [`physical_expr_adapter`](#physical_expr_adapter) - re-export of [`datafusion_physical_expr_adapter`] crate
- [`physical_expr_common`](#physical_expr_common) - re-export of [`datafusion_physical_expr`] crate
- [`physical_optimizer`](#physical_optimizer) - re-export of [`datafusion_physical_optimizer`] crate
- [`physical_plan`](#physical_plan) - re-export of [`datafusion_physical_plan`] crate
- [`physical_planner`](#physical_planner) - Planner for [`LogicalPlan`] to [`ExecutionPlan`]
- [`prelude`](#prelude) - DataFusion "prelude" to simplify importing common types.
- [`scalar`](#scalar) - [`ScalarValue`] single value representation.
- [`sql`](#sql) - re-export of [`datafusion_sql`] crate
- [`test`](#test) - Common unit test utility methods
- [`test_util`](#test_util) - Utility functions to make testing DataFusion based crates easier
- [`variable`](#variable) - re-export of variable provider for `@name` and `@@name` style runtime values.

**Macros**

- [`dataframe`](#dataframe) - Macro for creating DataFrame.

**Constants**

- [`DATAFUSION_VERSION`](#datafusion_version) - DataFusion crate version

---

## datafusion::DATAFUSION_VERSION

*Constant*: `&str`

DataFusion crate version



## Module: catalog

re-export of [`datafusion_catalog`] crate



## Module: common

re-export of [`datafusion_common`] crate



## datafusion::dataframe

*Declarative Macro*

Macro for creating DataFrame.
# Example
```
use datafusion::prelude::dataframe;
# use datafusion::error::Result;
# #[tokio::main]
# async fn main() -> Result<()> {
let df = dataframe!(
   "id" => [1, 2, 3],
   "name" => ["foo", "bar", "baz"]
 )?;
df.show().await?;
// +----+------+,
// | id | name |,
// +----+------+,
// | 1  | foo  |,
// | 2  | bar  |,
// | 3  | baz  |,
// +----+------+,
let df_empty = dataframe!()?; // empty DataFrame
assert_eq!(df_empty.schema().fields().len(), 0);
assert_eq!(df_empty.count().await?, 0);
# Ok(())
# }
```

```rust
macro_rules! dataframe {
    () => { ... };
    ($($name:expr => $data:expr),+ $(,)?) => { ... };
}
```



## Module: dataframe

[`DataFrame`] API for building and executing query plans.



## Module: datasource

DataFusion data sources: [`TableProvider`] and [`ListingTable`]

[`ListingTable`]: crate::datasource::listing::ListingTable



## Module: error

DataFusion error type [`DataFusionError`] and [`Result`].



## Module: execution

Shared state for query planning and execution.



## Module: functions

re-export of [`datafusion_functions`] crate



## Module: functions_aggregate

re-export of [`datafusion_functions_aggregate`] crate



## Module: functions_nested

re-export of [`datafusion_functions_nested`] crate, if "nested_expressions" feature is enabled



## Module: functions_table

re-export of [`datafusion_functions_table`] crate



## Module: functions_window

re-export of [`datafusion_functions_window`] crate



## Module: logical_expr

re-export of [`datafusion_expr`] crate



## Module: logical_expr_common

re-export of [`datafusion_expr_common`] crate



## Module: optimizer

re-export of [`datafusion_optimizer`] crate



## Module: physical_expr

re-export of [`datafusion_physical_expr`] crate



## Module: physical_expr_adapter

re-export of [`datafusion_physical_expr_adapter`] crate



## Module: physical_expr_common

re-export of [`datafusion_physical_expr`] crate



## Module: physical_optimizer

re-export of [`datafusion_physical_optimizer`] crate



## Module: physical_plan

re-export of [`datafusion_physical_plan`] crate



## Module: physical_planner

Planner for [`LogicalPlan`] to [`ExecutionPlan`]



## Module: prelude

DataFusion "prelude" to simplify importing common types.

Like the standard library's prelude, this module simplifies importing of
common items. Unlike the standard prelude, the contents of this module must
be imported manually:

```
use datafusion::prelude::*;
```



## Module: scalar

[`ScalarValue`] single value representation.

Note this is reimported from the datafusion-common crate for easy
migration when datafusion was split into several different crates



## Module: sql

re-export of [`datafusion_sql`] crate



## Module: test

Common unit test utility methods



## Module: test_util

Utility functions to make testing DataFusion based crates easier



## Module: variable

re-export of variable provider for `@name` and `@@name` style runtime values.



