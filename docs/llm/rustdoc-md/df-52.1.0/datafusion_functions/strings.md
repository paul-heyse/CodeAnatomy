**datafusion_functions > strings**

# Module: strings

## Contents

**Structs**

- [`LargeStringArrayBuilder`](#largestringarraybuilder)
- [`StringArrayBuilder`](#stringarraybuilder) - Optimized version of the StringBuilder in Arrow that:
- [`StringViewArrayBuilder`](#stringviewarraybuilder)

**Enums**

- [`ColumnarValueRef`](#columnarvalueref)

**Functions**

- [`make_and_append_view`](#make_and_append_view) - Append a new view to the views buffer with the given substr

---

## datafusion_functions::strings::ColumnarValueRef

*Enum*

**Generic Parameters:**
- 'a

**Variants:**
- `Scalar(&'a [u8])`
- `NullableArray(&'a arrow::array::StringArray)`
- `NonNullableArray(&'a arrow::array::StringArray)`
- `NullableLargeStringArray(&'a arrow::array::LargeStringArray)`
- `NonNullableLargeStringArray(&'a arrow::array::LargeStringArray)`
- `NullableStringViewArray(&'a arrow::array::StringViewArray)`
- `NonNullableStringViewArray(&'a arrow::array::StringViewArray)`

**Methods:**

- `fn is_valid(self: &Self, i: usize) -> bool`
- `fn nulls(self: &Self) -> Option<NullBuffer>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions::strings::LargeStringArrayBuilder

*Struct*

**Methods:**

- `fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self`
- `fn write<const CHECK_VALID>(self: & mut Self, column: &ColumnarValueRef, i: usize)`
- `fn append_offset(self: & mut Self)`
- `fn finish(self: Self, null_buffer: Option<NullBuffer>) -> LargeStringArray` - Finalize the builder into a concrete [`LargeStringArray`].



## datafusion_functions::strings::StringArrayBuilder

*Struct*

Optimized version of the StringBuilder in Arrow that:
1. Precalculating the expected length of the result, avoiding reallocations.
2. Avoids creating / incrementally creating a `NullBufferBuilder`

**Methods:**

- `fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self`
- `fn write<const CHECK_VALID>(self: & mut Self, column: &ColumnarValueRef, i: usize)`
- `fn append_offset(self: & mut Self)`
- `fn finish(self: Self, null_buffer: Option<NullBuffer>) -> StringArray` - Finalize the builder into a concrete [`StringArray`].



## datafusion_functions::strings::StringViewArrayBuilder

*Struct*

**Methods:**

- `fn with_capacity(_item_capacity: usize, data_capacity: usize) -> Self`
- `fn write<const CHECK_VALID>(self: & mut Self, column: &ColumnarValueRef, i: usize)`
- `fn append_offset(self: & mut Self)`
- `fn finish(self: Self) -> StringViewArray`



## datafusion_functions::strings::make_and_append_view

*Function*

Append a new view to the views buffer with the given substr

# Safety

original_view must be a valid view (the format described on
[`GenericByteViewArray`](arrow::array::GenericByteViewArray).

# Arguments
- views_buffer: The buffer to append the new view to
- null_builder: The buffer to append the null value to
- original_view: The original view value
- substr: The substring to append. Must be a valid substring of the original view
- start_offset: The start offset of the substring in the view

```rust
fn make_and_append_view(views_buffer: & mut Vec<u128>, null_builder: & mut arrow::array::NullBufferBuilder, original_view: &u128, substr: &str, start_offset: u32)
```



