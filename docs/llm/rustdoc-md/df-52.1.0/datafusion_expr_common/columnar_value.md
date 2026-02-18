**datafusion_expr_common > columnar_value**

# Module: columnar_value

## Contents

**Enums**

- [`ColumnarValue`](#columnarvalue) - The result of evaluating an expression.

---

## datafusion_expr_common::columnar_value::ColumnarValue

*Enum*

The result of evaluating an expression.

[`ColumnarValue::Scalar`] represents a single value repeated any number of
times. This is an important performance optimization for handling values
that do not change across rows.

[`ColumnarValue::Array`] represents a column of data, stored as an  Arrow
[`ArrayRef`]

A slice of `ColumnarValue`s logically represents a table, with each column
having the same number of rows. This means that all `Array`s are the same
length.

# Example

A `ColumnarValue::Array` with an array of 5 elements and a
`ColumnarValue::Scalar` with the value 100

```text
┌──────────────┐
│ ┌──────────┐ │
│ │   "A"    │ │
│ ├──────────┤ │
│ │   "B"    │ │
│ ├──────────┤ │
│ │   "C"    │ │
│ ├──────────┤ │
│ │   "D"    │ │        ┌──────────────┐
│ ├──────────┤ │        │ ┌──────────┐ │
│ │   "E"    │ │        │ │   100    │ │
│ └──────────┘ │        │ └──────────┘ │
└──────────────┘        └──────────────┘

 ColumnarValue::        ColumnarValue::
      Array                 Scalar
```

Logically represents the following table:

| Column 1| Column 2 |
| ------- | -------- |
| A | 100 |
| B | 100 |
| C | 100 |
| D | 100 |
| E | 100 |

# Performance Notes

When implementing functions or operators, it is important to consider the
performance implications of handling scalar values.

Because all functions must handle [`ArrayRef`], it is
convenient to convert [`ColumnarValue::Scalar`]s using
[`Self::into_array`]. For example,  [`ColumnarValue::values_to_arrays`]
converts multiple columnar values into arrays of the same length.

However, it is often much more performant to provide a different,
implementation that handles scalar values differently

**Variants:**
- `Array(arrow::array::ArrayRef)` - Array of values
- `Scalar(datafusion_common::ScalarValue)` - A single value

**Methods:**

- `fn data_type(self: &Self) -> DataType`
- `fn into_array(self: Self, num_rows: usize) -> Result<ArrayRef>` - Convert any [`Self::Scalar`] into an Arrow [`ArrayRef`] with the specified
- `fn into_array_of_size(self: Self, num_rows: usize) -> Result<ArrayRef>` - Convert a columnar value into an Arrow [`ArrayRef`] with the specified
- `fn to_array(self: &Self, num_rows: usize) -> Result<ArrayRef>` - Convert any [`Self::Scalar`] into an Arrow [`ArrayRef`] with the specified
- `fn to_array_of_size(self: &Self, num_rows: usize) -> Result<ArrayRef>` - Convert a columnar value into an Arrow [`ArrayRef`] with the specified
- `fn create_null_array(num_rows: usize) -> Self` - Null columnar values are implemented as a null array in order to pass batch
- `fn values_to_arrays(args: &[ColumnarValue]) -> Result<Vec<ArrayRef>>` - Converts  [`ColumnarValue`]s to [`ArrayRef`]s with the same length.
- `fn cast_to(self: &Self, cast_type: &DataType, cast_options: Option<&CastOptions<'static>>) -> Result<ColumnarValue>` - Cast's this [ColumnarValue] to the specified `DataType`

**Trait Implementations:**

- **From**
  - `fn from(value: ScalarValue) -> Self`
- **From**
  - `fn from(value: ArrayRef) -> Self`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ColumnarValue`



