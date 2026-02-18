**datafusion_expr > type_coercion > other**

# Module: type_coercion::other

## Contents

**Functions**

- [`get_coerce_type_for_case_expression`](#get_coerce_type_for_case_expression) - Find a common coerceable type for all `when_or_then_types` as well
- [`get_coerce_type_for_list`](#get_coerce_type_for_list) - Attempts to coerce the types of `list_types` to be comparable with the

---

## datafusion_expr::type_coercion::other::get_coerce_type_for_case_expression

*Function*

Find a common coerceable type for all `when_or_then_types` as well
and the `case_or_else_type`, if specified.
Returns the common data type for `when_or_then_types` and `case_or_else_type`

```rust
fn get_coerce_type_for_case_expression(when_or_then_types: &[arrow::datatypes::DataType], case_or_else_type: Option<&arrow::datatypes::DataType>) -> Option<arrow::datatypes::DataType>
```



## datafusion_expr::type_coercion::other::get_coerce_type_for_list

*Function*

Attempts to coerce the types of `list_types` to be comparable with the
`expr_type`.
Returns the common data type for `expr_type` and `list_types`

```rust
fn get_coerce_type_for_list(expr_type: &arrow::datatypes::DataType, list_types: &[arrow::datatypes::DataType]) -> Option<arrow::datatypes::DataType>
```



