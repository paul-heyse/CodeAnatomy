**datafusion_common > cast**

# Module: cast

## Contents

**Functions**

- [`as_binary_array`](#as_binary_array)
- [`as_binary_view_array`](#as_binary_view_array)
- [`as_boolean_array`](#as_boolean_array)
- [`as_date32_array`](#as_date32_array)
- [`as_date64_array`](#as_date64_array)
- [`as_decimal128_array`](#as_decimal128_array)
- [`as_decimal256_array`](#as_decimal256_array)
- [`as_decimal32_array`](#as_decimal32_array)
- [`as_decimal64_array`](#as_decimal64_array)
- [`as_dictionary_array`](#as_dictionary_array)
- [`as_duration_microsecond_array`](#as_duration_microsecond_array)
- [`as_duration_millisecond_array`](#as_duration_millisecond_array)
- [`as_duration_nanosecond_array`](#as_duration_nanosecond_array)
- [`as_duration_second_array`](#as_duration_second_array)
- [`as_fixed_size_binary_array`](#as_fixed_size_binary_array)
- [`as_fixed_size_list_array`](#as_fixed_size_list_array)
- [`as_float16_array`](#as_float16_array)
- [`as_float32_array`](#as_float32_array)
- [`as_float64_array`](#as_float64_array)
- [`as_generic_binary_array`](#as_generic_binary_array)
- [`as_generic_list_array`](#as_generic_list_array)
- [`as_generic_string_array`](#as_generic_string_array)
- [`as_int16_array`](#as_int16_array)
- [`as_int32_array`](#as_int32_array)
- [`as_int64_array`](#as_int64_array)
- [`as_int8_array`](#as_int8_array)
- [`as_interval_dt_array`](#as_interval_dt_array)
- [`as_interval_mdn_array`](#as_interval_mdn_array)
- [`as_interval_ym_array`](#as_interval_ym_array)
- [`as_large_binary_array`](#as_large_binary_array)
- [`as_large_list_array`](#as_large_list_array)
- [`as_large_list_view_array`](#as_large_list_view_array)
- [`as_large_string_array`](#as_large_string_array)
- [`as_list_array`](#as_list_array)
- [`as_list_view_array`](#as_list_view_array)
- [`as_map_array`](#as_map_array)
- [`as_null_array`](#as_null_array)
- [`as_primitive_array`](#as_primitive_array)
- [`as_string_array`](#as_string_array)
- [`as_string_view_array`](#as_string_view_array)
- [`as_struct_array`](#as_struct_array)
- [`as_time32_millisecond_array`](#as_time32_millisecond_array)
- [`as_time32_second_array`](#as_time32_second_array)
- [`as_time64_microsecond_array`](#as_time64_microsecond_array)
- [`as_time64_nanosecond_array`](#as_time64_nanosecond_array)
- [`as_timestamp_microsecond_array`](#as_timestamp_microsecond_array)
- [`as_timestamp_millisecond_array`](#as_timestamp_millisecond_array)
- [`as_timestamp_nanosecond_array`](#as_timestamp_nanosecond_array)
- [`as_timestamp_second_array`](#as_timestamp_second_array)
- [`as_uint16_array`](#as_uint16_array)
- [`as_uint32_array`](#as_uint32_array)
- [`as_uint64_array`](#as_uint64_array)
- [`as_uint8_array`](#as_uint8_array)
- [`as_union_array`](#as_union_array)

---

## datafusion_common::cast::as_binary_array

*Function*

```rust
fn as_binary_array(array: &dyn Array) -> crate::Result<&arrow::array::BinaryArray>
```



## datafusion_common::cast::as_binary_view_array

*Function*

```rust
fn as_binary_view_array(array: &dyn Array) -> crate::Result<&arrow::array::BinaryViewArray>
```



## datafusion_common::cast::as_boolean_array

*Function*

```rust
fn as_boolean_array(array: &dyn Array) -> crate::Result<&arrow::array::BooleanArray>
```



## datafusion_common::cast::as_date32_array

*Function*

```rust
fn as_date32_array(array: &dyn Array) -> crate::Result<&arrow::array::Date32Array>
```



## datafusion_common::cast::as_date64_array

*Function*

```rust
fn as_date64_array(array: &dyn Array) -> crate::Result<&arrow::array::Date64Array>
```



## datafusion_common::cast::as_decimal128_array

*Function*

```rust
fn as_decimal128_array(array: &dyn Array) -> crate::Result<&arrow::array::Decimal128Array>
```



## datafusion_common::cast::as_decimal256_array

*Function*

```rust
fn as_decimal256_array(array: &dyn Array) -> crate::Result<&arrow::array::Decimal256Array>
```



## datafusion_common::cast::as_decimal32_array

*Function*

```rust
fn as_decimal32_array(array: &dyn Array) -> crate::Result<&arrow::array::Decimal32Array>
```



## datafusion_common::cast::as_decimal64_array

*Function*

```rust
fn as_decimal64_array(array: &dyn Array) -> crate::Result<&arrow::array::Decimal64Array>
```



## datafusion_common::cast::as_dictionary_array

*Function*

```rust
fn as_dictionary_array<T>(array: &dyn Array) -> crate::Result<&arrow::array::DictionaryArray<T>>
```



## datafusion_common::cast::as_duration_microsecond_array

*Function*

```rust
fn as_duration_microsecond_array(array: &dyn Array) -> crate::Result<&arrow::array::DurationMicrosecondArray>
```



## datafusion_common::cast::as_duration_millisecond_array

*Function*

```rust
fn as_duration_millisecond_array(array: &dyn Array) -> crate::Result<&arrow::array::DurationMillisecondArray>
```



## datafusion_common::cast::as_duration_nanosecond_array

*Function*

```rust
fn as_duration_nanosecond_array(array: &dyn Array) -> crate::Result<&arrow::array::DurationNanosecondArray>
```



## datafusion_common::cast::as_duration_second_array

*Function*

```rust
fn as_duration_second_array(array: &dyn Array) -> crate::Result<&arrow::array::DurationSecondArray>
```



## datafusion_common::cast::as_fixed_size_binary_array

*Function*

```rust
fn as_fixed_size_binary_array(array: &dyn Array) -> crate::Result<&arrow::array::FixedSizeBinaryArray>
```



## datafusion_common::cast::as_fixed_size_list_array

*Function*

```rust
fn as_fixed_size_list_array(array: &dyn Array) -> crate::Result<&arrow::array::FixedSizeListArray>
```



## datafusion_common::cast::as_float16_array

*Function*

```rust
fn as_float16_array(array: &dyn Array) -> crate::Result<&arrow::array::Float16Array>
```



## datafusion_common::cast::as_float32_array

*Function*

```rust
fn as_float32_array(array: &dyn Array) -> crate::Result<&arrow::array::Float32Array>
```



## datafusion_common::cast::as_float64_array

*Function*

```rust
fn as_float64_array(array: &dyn Array) -> crate::Result<&arrow::array::Float64Array>
```



## datafusion_common::cast::as_generic_binary_array

*Function*

```rust
fn as_generic_binary_array<T>(array: &dyn Array) -> crate::Result<&arrow::array::GenericBinaryArray<T>>
```



## datafusion_common::cast::as_generic_list_array

*Function*

```rust
fn as_generic_list_array<T>(array: &dyn Array) -> crate::Result<&arrow::array::GenericListArray<T>>
```



## datafusion_common::cast::as_generic_string_array

*Function*

```rust
fn as_generic_string_array<T>(array: &dyn Array) -> crate::Result<&arrow::array::GenericStringArray<T>>
```



## datafusion_common::cast::as_int16_array

*Function*

```rust
fn as_int16_array(array: &dyn Array) -> crate::Result<&arrow::array::Int16Array>
```



## datafusion_common::cast::as_int32_array

*Function*

```rust
fn as_int32_array(array: &dyn Array) -> crate::Result<&arrow::array::Int32Array>
```



## datafusion_common::cast::as_int64_array

*Function*

```rust
fn as_int64_array(array: &dyn Array) -> crate::Result<&arrow::array::Int64Array>
```



## datafusion_common::cast::as_int8_array

*Function*

```rust
fn as_int8_array(array: &dyn Array) -> crate::Result<&arrow::array::Int8Array>
```



## datafusion_common::cast::as_interval_dt_array

*Function*

```rust
fn as_interval_dt_array(array: &dyn Array) -> crate::Result<&arrow::array::IntervalDayTimeArray>
```



## datafusion_common::cast::as_interval_mdn_array

*Function*

```rust
fn as_interval_mdn_array(array: &dyn Array) -> crate::Result<&arrow::array::IntervalMonthDayNanoArray>
```



## datafusion_common::cast::as_interval_ym_array

*Function*

```rust
fn as_interval_ym_array(array: &dyn Array) -> crate::Result<&arrow::array::IntervalYearMonthArray>
```



## datafusion_common::cast::as_large_binary_array

*Function*

```rust
fn as_large_binary_array(array: &dyn Array) -> crate::Result<&arrow::array::LargeBinaryArray>
```



## datafusion_common::cast::as_large_list_array

*Function*

```rust
fn as_large_list_array(array: &dyn Array) -> crate::Result<&arrow::array::LargeListArray>
```



## datafusion_common::cast::as_large_list_view_array

*Function*

```rust
fn as_large_list_view_array(array: &dyn Array) -> crate::Result<&arrow::array::LargeListViewArray>
```



## datafusion_common::cast::as_large_string_array

*Function*

```rust
fn as_large_string_array(array: &dyn Array) -> crate::Result<&arrow::array::LargeStringArray>
```



## datafusion_common::cast::as_list_array

*Function*

```rust
fn as_list_array(array: &dyn Array) -> crate::Result<&arrow::array::ListArray>
```



## datafusion_common::cast::as_list_view_array

*Function*

```rust
fn as_list_view_array(array: &dyn Array) -> crate::Result<&arrow::array::ListViewArray>
```



## datafusion_common::cast::as_map_array

*Function*

```rust
fn as_map_array(array: &dyn Array) -> crate::Result<&arrow::array::MapArray>
```



## datafusion_common::cast::as_null_array

*Function*

```rust
fn as_null_array(array: &dyn Array) -> crate::Result<&arrow::array::NullArray>
```



## datafusion_common::cast::as_primitive_array

*Function*

```rust
fn as_primitive_array<T>(array: &dyn Array) -> crate::Result<&arrow::array::PrimitiveArray<T>>
```



## datafusion_common::cast::as_string_array

*Function*

```rust
fn as_string_array(array: &dyn Array) -> crate::Result<&arrow::array::StringArray>
```



## datafusion_common::cast::as_string_view_array

*Function*

```rust
fn as_string_view_array(array: &dyn Array) -> crate::Result<&arrow::array::StringViewArray>
```



## datafusion_common::cast::as_struct_array

*Function*

```rust
fn as_struct_array(array: &dyn Array) -> crate::Result<&arrow::array::StructArray>
```



## datafusion_common::cast::as_time32_millisecond_array

*Function*

```rust
fn as_time32_millisecond_array(array: &dyn Array) -> crate::Result<&arrow::array::Time32MillisecondArray>
```



## datafusion_common::cast::as_time32_second_array

*Function*

```rust
fn as_time32_second_array(array: &dyn Array) -> crate::Result<&arrow::array::Time32SecondArray>
```



## datafusion_common::cast::as_time64_microsecond_array

*Function*

```rust
fn as_time64_microsecond_array(array: &dyn Array) -> crate::Result<&arrow::array::Time64MicrosecondArray>
```



## datafusion_common::cast::as_time64_nanosecond_array

*Function*

```rust
fn as_time64_nanosecond_array(array: &dyn Array) -> crate::Result<&arrow::array::Time64NanosecondArray>
```



## datafusion_common::cast::as_timestamp_microsecond_array

*Function*

```rust
fn as_timestamp_microsecond_array(array: &dyn Array) -> crate::Result<&arrow::array::TimestampMicrosecondArray>
```



## datafusion_common::cast::as_timestamp_millisecond_array

*Function*

```rust
fn as_timestamp_millisecond_array(array: &dyn Array) -> crate::Result<&arrow::array::TimestampMillisecondArray>
```



## datafusion_common::cast::as_timestamp_nanosecond_array

*Function*

```rust
fn as_timestamp_nanosecond_array(array: &dyn Array) -> crate::Result<&arrow::array::TimestampNanosecondArray>
```



## datafusion_common::cast::as_timestamp_second_array

*Function*

```rust
fn as_timestamp_second_array(array: &dyn Array) -> crate::Result<&arrow::array::TimestampSecondArray>
```



## datafusion_common::cast::as_uint16_array

*Function*

```rust
fn as_uint16_array(array: &dyn Array) -> crate::Result<&arrow::array::UInt16Array>
```



## datafusion_common::cast::as_uint32_array

*Function*

```rust
fn as_uint32_array(array: &dyn Array) -> crate::Result<&arrow::array::UInt32Array>
```



## datafusion_common::cast::as_uint64_array

*Function*

```rust
fn as_uint64_array(array: &dyn Array) -> crate::Result<&arrow::array::UInt64Array>
```



## datafusion_common::cast::as_uint8_array

*Function*

```rust
fn as_uint8_array(array: &dyn Array) -> crate::Result<&arrow::array::UInt8Array>
```



## datafusion_common::cast::as_union_array

*Function*

```rust
fn as_union_array(array: &dyn Array) -> crate::Result<&arrow::array::UnionArray>
```



