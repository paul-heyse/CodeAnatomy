**datafusion_functions_nested > make_array**

# Module: make_array

## Contents

**Structs**

- [`MakeArray`](#makearray)

**Functions**

- [`array_array`](#array_array) - Convert one or more [`ArrayRef`] of the same type into a
- [`coerce_types_inner`](#coerce_types_inner)
- [`make_array`](#make_array) - Returns an Arrow array using the specified input expressions.
- [`make_array_udf`](#make_array_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::make_array::MakeArray

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &MakeArray) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



## datafusion_functions_nested::make_array::array_array

*Function*

Convert one or more [`ArrayRef`] of the same type into a
`ListArray` or 'LargeListArray' depending on the offset size.

# Example (non nested)

Calling `array(col1, col2)` where col1 and col2 are non nested
would return a single new `ListArray`, where each row was a list
of 2 elements:

```text
┌─────────┐   ┌─────────┐           ┌──────────────┐
│ ┌─────┐ │   │ ┌─────┐ │           │ ┌──────────┐ │
│ │  A  │ │   │ │  X  │ │           │ │  [A, X]  │ │
│ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
│ │NULL │ │   │ │  Y  │ │──────────▶│ │[NULL, Y] │ │
│ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
│ │  C  │ │   │ │  Z  │ │           │ │  [C, Z]  │ │
│ └─────┘ │   │ └─────┘ │           │ └──────────┘ │
└─────────┘   └─────────┘           └──────────────┘
  col1           col2                    output
```

# Example (nested)

Calling `array(col1, col2)` where col1 and col2 are lists
would return a single new `ListArray`, where each row was a list
of the corresponding elements of col1 and col2.

``` text
┌──────────────┐   ┌──────────────┐        ┌─────────────────────────────┐
│ ┌──────────┐ │   │ ┌──────────┐ │        │ ┌────────────────────────┐  │
│ │  [A, X]  │ │   │ │    []    │ │        │ │    [[A, X], []]        │  │
│ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────┤  │
│ │[NULL, Y] │ │   │ │[Q, R, S] │ │───────▶│ │ [[NULL, Y], [Q, R, S]] │  │
│ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────│  │
│ │  [C, Z]  │ │   │ │   NULL   │ │        │ │    [[C, Z], NULL]      │  │
│ └──────────┘ │   │ └──────────┘ │        │ └────────────────────────┘  │
└──────────────┘   └──────────────┘        └─────────────────────────────┘
     col1               col2                         output
```

```rust
fn array_array<O>(args: &[arrow::array::ArrayRef], data_type: arrow::datatypes::DataType, field_name: &str) -> datafusion_common::Result<arrow::array::ArrayRef>
```



## datafusion_functions_nested::make_array::coerce_types_inner

*Function*

```rust
fn coerce_types_inner(arg_types: &[arrow::datatypes::DataType], name: &str) -> datafusion_common::Result<Vec<arrow::datatypes::DataType>>
```



## datafusion_functions_nested::make_array::make_array

*Function*

Returns an Arrow array using the specified input expressions.

```rust
fn make_array(arg: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions_nested::make_array::make_array_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
MakeArray

```rust
fn make_array_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



