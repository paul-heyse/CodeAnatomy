**datafusion_expr_common > type_coercion > binary**

# Module: type_coercion::binary

## Contents

**Structs**

- [`BinaryTypeCoercer`](#binarytypecoercer) - Provides type information about a binary expression, coercing different

**Functions**

- [`binary_numeric_coercion`](#binary_numeric_coercion) - Coerce `lhs_type` and `rhs_type` to a common type where both are numeric
- [`binary_to_string_coercion`](#binary_to_string_coercion) - Coercion rules for binary (Binary/LargeBinary) to string (Utf8/LargeUtf8):
- [`comparison_coercion`](#comparison_coercion) - Coerce `lhs_type` and `rhs_type` to a common type for the purposes of a
- [`comparison_coercion_numeric`](#comparison_coercion_numeric) - Similar to [`comparison_coercion`] but prefers numeric if compares with
- [`decimal_coercion`](#decimal_coercion) - Decimal coercion rules.
- [`like_coercion`](#like_coercion) - Coercion rules for like operations.
- [`regex_coercion`](#regex_coercion) - Coercion rules for regular expression comparison operations.
- [`string_coercion`](#string_coercion) - Coercion rules for string view types (Utf8/LargeUtf8/Utf8View):
- [`try_type_union_resolution`](#try_type_union_resolution) - Handle type union resolution including struct type and others.
- [`try_type_union_resolution_with_struct`](#try_type_union_resolution_with_struct)
- [`type_union_resolution`](#type_union_resolution) - Coerce dissimilar data types to a single data type.

---

## datafusion_expr_common::type_coercion::binary::BinaryTypeCoercer

*Struct*

Provides type information about a binary expression, coercing different
input types into a sensible output type.

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(lhs: &'a DataType, op: &'a Operator, rhs: &'a DataType) -> Self` - Creates a new [`BinaryTypeCoercer`], for reasoning about the input
- `fn set_lhs_spans(self: & mut Self, spans: Spans)` - Sets the spans information for the left side of the binary expression,
- `fn set_op_spans(self: & mut Self, spans: Spans)` - Sets the spans information for the operator of the binary expression, so
- `fn set_rhs_spans(self: & mut Self, spans: Spans)` - Sets the spans information for the right side of the binary expression,
- `fn get_result_type(self: &'a Self) -> Result<DataType>` - Returns the resulting type of a binary expression evaluating the `op` with the left and right hand types
- `fn get_input_types(self: &'a Self) -> Result<(DataType, DataType)>` - Returns the coerced input types for a binary expression evaluating the `op` with the left and right hand types



## datafusion_expr_common::type_coercion::binary::binary_numeric_coercion

*Function*

Coerce `lhs_type` and `rhs_type` to a common type where both are numeric

```rust
fn binary_numeric_coercion(lhs_type: &arrow::datatypes::DataType, rhs_type: &arrow::datatypes::DataType) -> Option<arrow::datatypes::DataType>
```



## datafusion_expr_common::type_coercion::binary::binary_to_string_coercion

*Function*

Coercion rules for binary (Binary/LargeBinary) to string (Utf8/LargeUtf8):
If one argument is binary and the other is a string then coerce to string
(e.g. for `like`)

```rust
fn binary_to_string_coercion(lhs_type: &arrow::datatypes::DataType, rhs_type: &arrow::datatypes::DataType) -> Option<arrow::datatypes::DataType>
```



## datafusion_expr_common::type_coercion::binary::comparison_coercion

*Function*

Coerce `lhs_type` and `rhs_type` to a common type for the purposes of a
comparison operation

Example comparison operations are `lhs = rhs` and `lhs > rhs`

Binary comparison kernels require the two arguments to be the (exact) same
data type. However, users can write queries where the two arguments are
different data types. In such cases, the data types are automatically cast
(coerced) to a single data type to pass to the kernels.

# Numeric comparisons

When comparing numeric values, the lower precision type is coerced to the
higher precision type to avoid losing data. For example when comparing
`Int32` to `Int64` the coerced type is `Int64` so the `Int32` argument will
be cast.

# Numeric / String comparisons

When comparing numeric values and strings, both values will be coerced to
strings.  For example when comparing `'2' > 1`,  the arguments will be
coerced to `Utf8` for comparison

```rust
fn comparison_coercion(lhs_type: &arrow::datatypes::DataType, rhs_type: &arrow::datatypes::DataType) -> Option<arrow::datatypes::DataType>
```



## datafusion_expr_common::type_coercion::binary::comparison_coercion_numeric

*Function*

Similar to [`comparison_coercion`] but prefers numeric if compares with
numeric and string

# Numeric comparisons

When comparing numeric values and strings, the values will be coerced to the
numeric type.  For example, `'2' > 1` if `1` is an `Int32`, the arguments
will be coerced to `Int32`.

```rust
fn comparison_coercion_numeric(lhs_type: &arrow::datatypes::DataType, rhs_type: &arrow::datatypes::DataType) -> Option<arrow::datatypes::DataType>
```



## datafusion_expr_common::type_coercion::binary::decimal_coercion

*Function*

Decimal coercion rules.

```rust
fn decimal_coercion(lhs_type: &arrow::datatypes::DataType, rhs_type: &arrow::datatypes::DataType) -> Option<arrow::datatypes::DataType>
```



## datafusion_expr_common::type_coercion::binary::like_coercion

*Function*

Coercion rules for like operations.
This is a union of string coercion rules, dictionary coercion rules, and REE coercion rules

```rust
fn like_coercion(lhs_type: &arrow::datatypes::DataType, rhs_type: &arrow::datatypes::DataType) -> Option<arrow::datatypes::DataType>
```



## datafusion_expr_common::type_coercion::binary::regex_coercion

*Function*

Coercion rules for regular expression comparison operations.
This is a union of string coercion rules and dictionary coercion rules

```rust
fn regex_coercion(lhs_type: &arrow::datatypes::DataType, rhs_type: &arrow::datatypes::DataType) -> Option<arrow::datatypes::DataType>
```



## datafusion_expr_common::type_coercion::binary::string_coercion

*Function*

Coercion rules for string view types (Utf8/LargeUtf8/Utf8View):
If at least one argument is a string view, we coerce to string view
based on the observation that StringArray to StringViewArray is cheap but not vice versa.

Between Utf8 and LargeUtf8, we coerce to LargeUtf8.

```rust
fn string_coercion(lhs_type: &arrow::datatypes::DataType, rhs_type: &arrow::datatypes::DataType) -> Option<arrow::datatypes::DataType>
```



## datafusion_expr_common::type_coercion::binary::try_type_union_resolution

*Function*

Handle type union resolution including struct type and others.

```rust
fn try_type_union_resolution(data_types: &[arrow::datatypes::DataType]) -> datafusion_common::Result<Vec<arrow::datatypes::DataType>>
```



## datafusion_expr_common::type_coercion::binary::try_type_union_resolution_with_struct

*Function*

```rust
fn try_type_union_resolution_with_struct(data_types: &[arrow::datatypes::DataType]) -> datafusion_common::Result<Vec<arrow::datatypes::DataType>>
```



## datafusion_expr_common::type_coercion::binary::type_union_resolution

*Function*

Coerce dissimilar data types to a single data type.
UNION, INTERSECT, EXCEPT, CASE, ARRAY, VALUES, and the GREATEST and LEAST functions are
examples that has the similar resolution rules.
See <https://www.postgresql.org/docs/current/typeconv-union-case.html> for more information.
The rules in the document provide a clue, but adhering strictly to them doesn't precisely
align with the behavior of Postgres. Therefore, we've made slight adjustments to the rules
to better match the behavior of both Postgres and DuckDB. For example, we expect adjusted
decimal precision and scale when coercing decimal types.

This function doesn't preserve correct field name and nullability for the struct type, we only care about data type.

Returns Option because we might want to continue on the code even if the data types are not coercible to the common type

```rust
fn type_union_resolution(data_types: &[arrow::datatypes::DataType]) -> Option<arrow::datatypes::DataType>
```



