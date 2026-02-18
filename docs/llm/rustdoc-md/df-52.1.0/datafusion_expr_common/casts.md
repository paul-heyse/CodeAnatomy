**datafusion_expr_common > casts**

# Module: casts

## Contents

**Functions**

- [`is_supported_type`](#is_supported_type) - Returns true if unwrap_cast_in_comparison supports this data type
- [`try_cast_literal_to_type`](#try_cast_literal_to_type) - Convert a literal value from one data type to another

---

## datafusion_expr_common::casts::is_supported_type

*Function*

Returns true if unwrap_cast_in_comparison supports this data type

```rust
fn is_supported_type(data_type: &arrow::datatypes::DataType) -> bool
```



## datafusion_expr_common::casts::try_cast_literal_to_type

*Function*

Convert a literal value from one data type to another

```rust
fn try_cast_literal_to_type(lit_value: &datafusion_common::ScalarValue, target_type: &arrow::datatypes::DataType) -> Option<datafusion_common::ScalarValue>
```



