**datafusion_expr > type_coercion > functions**

# Module: type_coercion::functions

## Contents

**Functions**

- [`can_coerce_from`](#can_coerce_from) - Return true if a value of type `type_from` can be coerced
- [`data_types`](#data_types) - Performs type coercion for function arguments.
- [`data_types_with_scalar_udf`](#data_types_with_scalar_udf) - Performs type coercion for scalar function arguments.
- [`fields_with_aggregate_udf`](#fields_with_aggregate_udf) - Performs type coercion for aggregate function arguments.
- [`fields_with_udf`](#fields_with_udf) - Performs type coercion for UDF arguments.
- [`fields_with_window_udf`](#fields_with_window_udf) - Performs type coercion for window function arguments.

**Traits**

- [`UDFCoercionExt`](#udfcoercionext) - Extension trait to unify common functionality between [`ScalarUDF`], [`AggregateUDF`]

---

## datafusion_expr::type_coercion::functions::UDFCoercionExt

*Trait*

Extension trait to unify common functionality between [`ScalarUDF`], [`AggregateUDF`]
and [`WindowUDF`] for use by signature coercion functions.

**Methods:**

- `name`: Should delegate to [`ScalarUDF::name`], [`AggregateUDF::name`] or [`WindowUDF::name`].
- `signature`: Should delegate to [`ScalarUDF::signature`], [`AggregateUDF::signature`]
- `coerce_types`: Should delegate to [`ScalarUDF::coerce_types`], [`AggregateUDF::coerce_types`]



## datafusion_expr::type_coercion::functions::can_coerce_from

*Function*

Return true if a value of type `type_from` can be coerced
(losslessly converted) into a value of `type_to`

See the module level documentation for more detail on coercion.

```rust
fn can_coerce_from(type_into: &arrow::datatypes::DataType, type_from: &arrow::datatypes::DataType) -> bool
```



## datafusion_expr::type_coercion::functions::data_types

*Function*

Performs type coercion for function arguments.

Returns the data types to which each argument must be coerced to
match `signature`.

For more details on coercion in general, please see the
[`type_coercion`](crate::type_coercion) module.

```rust
fn data_types<impl AsRef<str>>(function_name: impl Trait, current_types: &[arrow::datatypes::DataType], signature: &crate::Signature) -> datafusion_common::Result<Vec<arrow::datatypes::DataType>>
```



## datafusion_expr::type_coercion::functions::data_types_with_scalar_udf

*Function*

Performs type coercion for scalar function arguments.

Returns the data types to which each argument must be coerced to
match `signature`.

For more details on coercion in general, please see the
[`type_coercion`](crate::type_coercion) module.

```rust
fn data_types_with_scalar_udf(current_types: &[arrow::datatypes::DataType], func: &crate::ScalarUDF) -> datafusion_common::Result<Vec<arrow::datatypes::DataType>>
```



## datafusion_expr::type_coercion::functions::fields_with_aggregate_udf

*Function*

Performs type coercion for aggregate function arguments.

Returns the fields to which each argument must be coerced to
match `signature`.

For more details on coercion in general, please see the
[`type_coercion`](crate::type_coercion) module.

```rust
fn fields_with_aggregate_udf(current_fields: &[arrow::datatypes::FieldRef], func: &crate::AggregateUDF) -> datafusion_common::Result<Vec<arrow::datatypes::FieldRef>>
```



## datafusion_expr::type_coercion::functions::fields_with_udf

*Function*

Performs type coercion for UDF arguments.

Returns the data types to which each argument must be coerced to
match `signature`.

For more details on coercion in general, please see the
[`type_coercion`](crate::type_coercion) module.

```rust
fn fields_with_udf<F>(current_fields: &[arrow::datatypes::FieldRef], func: &F) -> datafusion_common::Result<Vec<arrow::datatypes::FieldRef>>
```



## datafusion_expr::type_coercion::functions::fields_with_window_udf

*Function*

Performs type coercion for window function arguments.

Returns the data types to which each argument must be coerced to
match `signature`.

For more details on coercion in general, please see the
[`type_coercion`](crate::type_coercion) module.

```rust
fn fields_with_window_udf(current_fields: &[arrow::datatypes::FieldRef], func: &crate::WindowUDF) -> datafusion_common::Result<Vec<arrow::datatypes::FieldRef>>
```



