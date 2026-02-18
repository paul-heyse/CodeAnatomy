**datafusion_functions_nested > set_ops**

# Module: set_ops

## Contents

**Structs**

- [`ArrayUnion`](#arrayunion)

**Functions**

- [`array_distinct`](#array_distinct) - returns distinct values from the array after removing duplicates.
- [`array_distinct_udf`](#array_distinct_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_intersect`](#array_intersect) - returns an array of the elements in the intersection of array1 and array2.
- [`array_intersect_udf`](#array_intersect_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_union`](#array_union) - returns an array of the elements in the union of array1 and array2 without duplicates.
- [`array_union_udf`](#array_union_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::set_ops::ArrayUnion

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayUnion) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::set_ops::array_distinct

*Function*

returns distinct values from the array after removing duplicates.

```rust
fn array_distinct(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::set_ops::array_distinct_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayDistinct

```rust
fn array_distinct_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::set_ops::array_intersect

*Function*

returns an array of the elements in the intersection of array1 and array2.

```rust
fn array_intersect(first_array: datafusion_expr::Expr, second_array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::set_ops::array_intersect_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayIntersect

```rust
fn array_intersect_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::set_ops::array_union

*Function*

returns an array of the elements in the union of array1 and array2 without duplicates.

```rust
fn array_union(array1: datafusion_expr::Expr, array2: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::set_ops::array_union_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayUnion

```rust
fn array_union_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



