**datafusion_functions_nested**

# Module: datafusion_functions_nested

## Contents

**Modules**

- [`array_has`](#array_has) - [`ScalarUDFImpl`] definitions for array_has, array_has_all and array_has_any functions.
- [`cardinality`](#cardinality) - [`ScalarUDFImpl`] definitions for cardinality function.
- [`concat`](#concat) - [`ScalarUDFImpl`] definitions for `array_append`, `array_prepend` and `array_concat` functions.
- [`dimension`](#dimension) - [`ScalarUDFImpl`] definitions for array_dims and array_ndims functions.
- [`distance`](#distance) - [ScalarUDFImpl] definitions for array_distance function.
- [`empty`](#empty) - [`ScalarUDFImpl`] definitions for array_empty function.
- [`except`](#except) - [`ScalarUDFImpl`] definitions for array_except function.
- [`expr_ext`](#expr_ext) - Extension methods for Expr.
- [`expr_fn`](#expr_fn) - Fluent-style API for creating `Expr`s
- [`extract`](#extract) - [`ScalarUDFImpl`] definitions for array_element, array_slice, array_pop_front, array_pop_back, and array_any_value functions.
- [`flatten`](#flatten) - [`ScalarUDFImpl`] definitions for flatten function.
- [`length`](#length) - [`ScalarUDFImpl`] definitions for array_length function.
- [`macros`](#macros)
- [`make_array`](#make_array) - [`ScalarUDFImpl`] definitions for `make_array` function.
- [`map`](#map)
- [`map_entries`](#map_entries) - [`ScalarUDFImpl`] definitions for map_entries function.
- [`map_extract`](#map_extract) - [`ScalarUDFImpl`] definitions for map_extract functions.
- [`map_keys`](#map_keys) - [`ScalarUDFImpl`] definitions for map_keys function.
- [`map_values`](#map_values) - [`ScalarUDFImpl`] definitions for map_values function.
- [`min_max`](#min_max) - [`ScalarUDFImpl`] definitions for array_max function.
- [`planner`](#planner) - SQL planning extensions like [`NestedFunctionPlanner`] and [`FieldAccessPlanner`]
- [`position`](#position) - [`ScalarUDFImpl`] definitions for array_position and array_positions functions.
- [`range`](#range) - [`ScalarUDFImpl`] definitions for range and gen_series functions.
- [`remove`](#remove) - [`ScalarUDFImpl`] definitions for array_remove, array_remove_n, array_remove_all functions.
- [`repeat`](#repeat) - [`ScalarUDFImpl`] definitions for array_repeat function.
- [`replace`](#replace) - [`ScalarUDFImpl`] definitions for array_replace, array_replace_n and array_replace_all functions.
- [`resize`](#resize) - [`ScalarUDFImpl`] definitions for array_resize function.
- [`reverse`](#reverse) - [`ScalarUDFImpl`] definitions for array_reverse function.
- [`set_ops`](#set_ops) - [`ScalarUDFImpl`] definitions for array_union, array_intersect and array_distinct functions.
- [`sort`](#sort) - [`ScalarUDFImpl`] definitions for array_sort function.
- [`string`](#string) - [`ScalarUDFImpl`] definitions for array_to_string and string_to_array functions.
- [`utils`](#utils) - array function utils

**Functions**

- [`all_default_nested_functions`](#all_default_nested_functions) - Return all default nested type functions
- [`register_all`](#register_all) - Registers all enabled packages with a [`FunctionRegistry`]

---

## datafusion_functions_nested::all_default_nested_functions

*Function*

Return all default nested type functions

```rust
fn all_default_nested_functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>>
```



## Module: array_has

[`ScalarUDFImpl`] definitions for array_has, array_has_all and array_has_any functions.



## Module: cardinality

[`ScalarUDFImpl`] definitions for cardinality function.



## Module: concat

[`ScalarUDFImpl`] definitions for `array_append`, `array_prepend` and `array_concat` functions.



## Module: dimension

[`ScalarUDFImpl`] definitions for array_dims and array_ndims functions.



## Module: distance

[ScalarUDFImpl] definitions for array_distance function.



## Module: empty

[`ScalarUDFImpl`] definitions for array_empty function.



## Module: except

[`ScalarUDFImpl`] definitions for array_except function.



## Module: expr_ext

Extension methods for Expr.



## Module: expr_fn

Fluent-style API for creating `Expr`s



## Module: extract

[`ScalarUDFImpl`] definitions for array_element, array_slice, array_pop_front, array_pop_back, and array_any_value functions.



## Module: flatten

[`ScalarUDFImpl`] definitions for flatten function.



## Module: length

[`ScalarUDFImpl`] definitions for array_length function.



## Module: macros



## Module: make_array

[`ScalarUDFImpl`] definitions for `make_array` function.



## Module: map



## Module: map_entries

[`ScalarUDFImpl`] definitions for map_entries function.



## Module: map_extract

[`ScalarUDFImpl`] definitions for map_extract functions.



## Module: map_keys

[`ScalarUDFImpl`] definitions for map_keys function.



## Module: map_values

[`ScalarUDFImpl`] definitions for map_values function.



## Module: min_max

[`ScalarUDFImpl`] definitions for array_max function.



## Module: planner

SQL planning extensions like [`NestedFunctionPlanner`] and [`FieldAccessPlanner`]



## Module: position

[`ScalarUDFImpl`] definitions for array_position and array_positions functions.



## Module: range

[`ScalarUDFImpl`] definitions for range and gen_series functions.



## datafusion_functions_nested::register_all

*Function*

Registers all enabled packages with a [`FunctionRegistry`]

```rust
fn register_all(registry: & mut dyn FunctionRegistry) -> datafusion_common::Result<()>
```



## Module: remove

[`ScalarUDFImpl`] definitions for array_remove, array_remove_n, array_remove_all functions.



## Module: repeat

[`ScalarUDFImpl`] definitions for array_repeat function.



## Module: replace

[`ScalarUDFImpl`] definitions for array_replace, array_replace_n and array_replace_all functions.



## Module: resize

[`ScalarUDFImpl`] definitions for array_resize function.



## Module: reverse

[`ScalarUDFImpl`] definitions for array_reverse function.



## Module: set_ops

[`ScalarUDFImpl`] definitions for array_union, array_intersect and array_distinct functions.



## Module: sort

[`ScalarUDFImpl`] definitions for array_sort function.



## Module: string

[`ScalarUDFImpl`] definitions for array_to_string and string_to_array functions.



## Module: utils

array function utils



