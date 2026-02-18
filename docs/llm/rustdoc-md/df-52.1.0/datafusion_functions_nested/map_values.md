**datafusion_functions_nested > map_values**

# Module: map_values

## Contents

**Functions**

- [`map_values`](#map_values) - Return a list of all values in the map.
- [`map_values_udf`](#map_values_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::map_values::map_values

*Function*

Return a list of all values in the map.

```rust
fn map_values(map: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::map_values::map_values_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
MapValuesFunc

```rust
fn map_values_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



