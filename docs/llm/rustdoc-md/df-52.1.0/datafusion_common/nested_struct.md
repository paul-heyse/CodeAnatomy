**datafusion_common > nested_struct**

# Module: nested_struct

## Contents

**Functions**

- [`cast_column`](#cast_column) - Cast a column to match the target field type, with special handling for nested structs.
- [`validate_struct_compatibility`](#validate_struct_compatibility) - Validates compatibility between source and target struct fields for casting operations.

---

## datafusion_common::nested_struct::cast_column

*Function*

Cast a column to match the target field type, with special handling for nested structs.

This function serves as the main entry point for column casting operations. For struct
types, it enforces that **only struct columns can be cast to struct types**.

## Casting Behavior
- **Struct Types**: Delegates to `cast_struct_column` for struct-to-struct casting only
- **Non-Struct Types**: Uses Arrow's standard `cast` function for primitive type conversions

## Cast Options
The `cast_options` argument controls how Arrow handles values that cannot be represented
in the target type. When `safe` is `false` (DataFusion's default) the cast will return an
error if such a value is encountered. Setting `safe` to `true` instead produces `NULL`
for out-of-range or otherwise invalid values. The options also allow customizing how
temporal values are formatted when cast to strings.

```
use arrow::array::{ArrayRef, Int64Array};
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field};
use datafusion_common::nested_struct::cast_column;
use std::sync::Arc;

let source: ArrayRef = Arc::new(Int64Array::from(vec![1, i64::MAX]));
let target = Field::new("ints", DataType::Int32, true);
// Permit lossy conversions by producing NULL on overflow instead of erroring
let options = CastOptions {
    safe: true,
    ..Default::default()
};
let result = cast_column(&source, &target, &options).unwrap();
assert!(result.is_null(1));
```

## Struct Casting Requirements
The struct casting logic requires that the source column must already be a struct type.
This makes the function useful for:
- Schema evolution scenarios where struct layouts change over time
- Data migration between different struct schemas
- Type-safe data processing pipelines that maintain struct type integrity

# Arguments
* `source_col` - The source array to cast
* `target_field` - The target field definition (including type and metadata)
* `cast_options` - Options that govern strictness and formatting of the cast

# Returns
A `Result<ArrayRef>` containing the cast array

# Errors
Returns an error if:
- Attempting to cast a non-struct column to a struct type
- Arrow's cast function fails for non-struct types
- Memory allocation fails during struct construction
- Invalid data type combinations are encountered

```rust
fn cast_column(source_col: &arrow::array::ArrayRef, target_field: &arrow::datatypes::Field, cast_options: &arrow::compute::CastOptions) -> crate::error::Result<arrow::array::ArrayRef>
```



## datafusion_common::nested_struct::validate_struct_compatibility

*Function*

Validates compatibility between source and target struct fields for casting operations.

This function implements comprehensive struct compatibility checking by examining:
- Field name matching between source and target structs
- Type castability for each matching field (including recursive struct validation)
- Proper handling of missing fields (target fields not in source are allowed - filled with nulls)
- Proper handling of extra fields (source fields not in target are allowed - ignored)

# Compatibility Rules
- **Field Matching**: Fields are matched by name (case-sensitive)
- **Missing Target Fields**: Allowed - will be filled with null values during casting
- **Extra Source Fields**: Allowed - will be ignored during casting
- **Type Compatibility**: Each matching field must be castable using Arrow's type system
- **Nested Structs**: Recursively validates nested struct compatibility

# Arguments
* `source_fields` - Fields from the source struct type
* `target_fields` - Fields from the target struct type

# Returns
* `Ok(())` if the structs are compatible for casting
* `Err(DataFusionError)` with detailed error message if incompatible

# Examples
```text
// Compatible: source has extra field, target has missing field
// Source: {a: i32, b: string, c: f64}
// Target: {a: i64, d: bool}
// Result: Ok(()) - 'a' can cast i32->i64, 'b','c' ignored, 'd' filled with nulls

// Incompatible: matching field has incompatible types
// Source: {a: string}
// Target: {a: binary}
// Result: Err(...) - string cannot cast to binary
```

```rust
fn validate_struct_compatibility(source_fields: &[arrow::datatypes::FieldRef], target_fields: &[arrow::datatypes::FieldRef]) -> crate::error::Result<()>
```



