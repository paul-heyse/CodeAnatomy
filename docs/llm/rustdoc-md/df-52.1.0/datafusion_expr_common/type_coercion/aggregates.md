**datafusion_expr_common > type_coercion > aggregates**

# Module: type_coercion::aggregates

## Contents

**Functions**

- [`check_arg_count`](#check_arg_count) - Validate the length of `input_fields` matches the `signature` for `agg_fun`.

**Statics**

- [`INTEGERS`](#integers)
- [`NUMERICS`](#numerics)

---

## datafusion_expr_common::type_coercion::aggregates::INTEGERS

*Static*

```rust
static INTEGERS: &[arrow::datatypes::DataType]
```



## datafusion_expr_common::type_coercion::aggregates::NUMERICS

*Static*

```rust
static NUMERICS: &[arrow::datatypes::DataType]
```



## datafusion_expr_common::type_coercion::aggregates::check_arg_count

*Function*

Validate the length of `input_fields` matches the `signature` for `agg_fun`.

This method DOES NOT validate the argument fields - only that (at least one,
in the case of [`TypeSignature::OneOf`]) signature matches the desired
number of input types.

```rust
fn check_arg_count(func_name: &str, input_fields: &[arrow::datatypes::FieldRef], signature: &crate::signature::TypeSignature) -> datafusion_common::Result<()>
```



