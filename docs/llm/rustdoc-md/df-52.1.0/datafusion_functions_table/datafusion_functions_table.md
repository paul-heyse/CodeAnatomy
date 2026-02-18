**datafusion_functions_table**

# Module: datafusion_functions_table

## Contents

**Modules**

- [`generate_series`](#generate_series)

**Macros**

- [`create_udtf_function`](#create_udtf_function) - Creates a singleton instance of a table function

**Functions**

- [`all_default_table_functions`](#all_default_table_functions) - Returns all default table functions
- [`generate_series`](#generate_series)
- [`range`](#range)

---

## datafusion_functions_table::all_default_table_functions

*Function*

Returns all default table functions

```rust
fn all_default_table_functions() -> Vec<std::sync::Arc<datafusion_catalog::TableFunction>>
```



## datafusion_functions_table::create_udtf_function

*Declarative Macro*

Creates a singleton instance of a table function
- `$module`: A struct implementing `TableFunctionImpl` to create the function from
- `$name`: The name to give to the created function

This is used to ensure creating the list of `TableFunction` only happens once.

```rust
macro_rules! create_udtf_function {
    ($module:path, $name:expr) => { ... };
}
```



## datafusion_functions_table::generate_series

*Function*

```rust
fn generate_series() -> Arc<TableFunction>
```



## Module: generate_series



## datafusion_functions_table::range

*Function*

```rust
fn range() -> Arc<TableFunction>
```



