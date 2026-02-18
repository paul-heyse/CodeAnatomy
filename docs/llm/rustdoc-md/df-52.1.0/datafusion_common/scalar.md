**datafusion_common > scalar**

# Module: scalar

## Contents

**Enums**

- [`ScalarValue`](#scalarvalue) - A dynamically typed, nullable single value.

**Functions**

- [`copy_array_data`](#copy_array_data) - Compacts the data of an `ArrayData` into a new `ArrayData`.
- [`date_to_timestamp_multiplier`](#date_to_timestamp_multiplier) - Returns the multiplier that converts the input date representation into the
- [`dict_from_values`](#dict_from_values) - Create a `DictionaryArray` from the provided values array.
- [`ensure_timestamp_in_bounds`](#ensure_timestamp_in_bounds) - Ensures the provided value can be represented as a timestamp with the given
- [`get_dict_value`](#get_dict_value) - Return a reference to the values array and the index into it for a
- [`partial_cmp_struct`](#partial_cmp_struct)

**Traits**

- [`ScalarType`](#scalartype) - Trait used to map a NativeType to a ScalarValue

---

## datafusion_common::scalar::ScalarType

*Trait*

Trait used to map a NativeType to a ScalarValue

**Methods:**

- `scalar`: returns a scalar from an optional T



## datafusion_common::scalar::ScalarValue

*Enum*

A dynamically typed, nullable single value.

While an arrow  [`Array`]) stores one or more values of the same type, in a
single column, a `ScalarValue` stores a single value of a single type, the
equivalent of 1 row and one column.

```text
 ┌────────┐
 │ value1 │
 │ value2 │                  ┌────────┐
 │ value3 │                  │ value2 │
 │  ...   │                  └────────┘
 │ valueN │
 └────────┘

   Array                     ScalarValue

stores multiple,             stores a single,
possibly null, values of     possible null, value
the same type
```

# Performance

In general, performance will be better using arrow [`Array`]s rather than
[`ScalarValue`], as it is far more efficient to process multiple values at
once (vectorized processing).

# Example
```
# use datafusion_common::ScalarValue;
// Create single scalar value for an Int32 value
let s1 = ScalarValue::Int32(Some(10));

// You can also create values using the From impl:
let s2 = ScalarValue::from(10i32);
assert_eq!(s1, s2);
```

# Null Handling

`ScalarValue` represents null values in the same way as Arrow. Nulls are
"typed" in the sense that a null value in an [`Int32Array`] is different
from a null value in a [`Float64Array`], and is different from the values in
a [`NullArray`].

```
# fn main() -> datafusion_common::Result<()> {
# use std::collections::hash_set::Difference;
# use datafusion_common::ScalarValue;
# use arrow::datatypes::DataType;
// You can create a 'null' Int32 value directly:
let s1 = ScalarValue::Int32(None);

// You can also create a null value for a given datatype:
let s2 = ScalarValue::try_from(&DataType::Int32)?;
assert_eq!(s1, s2);

// Note that this is DIFFERENT than a `ScalarValue::Null`
let s3 = ScalarValue::Null;
assert_ne!(s1, s3);
# Ok(())
# }
```

# Nested Types

`List` / `LargeList` / `FixedSizeList` / `Struct` / `Map` are represented as a
single element array of the corresponding type.

## Example: Creating [`ScalarValue::Struct`] using [`ScalarStructBuilder`]
```
# use std::sync::Arc;
# use arrow::datatypes::{DataType, Field};
# use datafusion_common::{ScalarValue, scalar::ScalarStructBuilder};
// Build a struct like: {a: 1, b: "foo"}
let field_a = Field::new("a", DataType::Int32, false);
let field_b = Field::new("b", DataType::Utf8, false);

let s1 = ScalarStructBuilder::new()
    .with_scalar(field_a, ScalarValue::from(1i32))
    .with_scalar(field_b, ScalarValue::from("foo"))
    .build();
```

## Example: Creating a null [`ScalarValue::Struct`] using [`ScalarStructBuilder`]
```
# use std::sync::Arc;
# use arrow::datatypes::{DataType, Field};
# use datafusion_common::{ScalarValue, scalar::ScalarStructBuilder};
// Build a struct representing a NULL value
let fields = vec![
    Field::new("a", DataType::Int32, false),
    Field::new("b", DataType::Utf8, false),
];

let s1 = ScalarStructBuilder::new_null(fields);
```

## Example: Creating [`ScalarValue::Struct`] directly
```
# use std::sync::Arc;
# use arrow::datatypes::{DataType, Field, Fields};
# use arrow::array::{ArrayRef, Int32Array, StructArray, StringArray};
# use datafusion_common::ScalarValue;
// Build a struct like: {a: 1, b: "foo"}
// Field description
let fields = Fields::from(vec![
    Field::new("a", DataType::Int32, false),
    Field::new("b", DataType::Utf8, false),
]);
// one row arrays for each field
let arrays: Vec<ArrayRef> = vec![
    Arc::new(Int32Array::from(vec![1])),
    Arc::new(StringArray::from(vec!["foo"])),
];
// no nulls for this array
let nulls = None;
let arr = StructArray::new(fields, arrays, nulls);

// Create a ScalarValue::Struct directly
let s1 = ScalarValue::Struct(Arc::new(arr));
```


# Further Reading
See [datatypes](https://arrow.apache.org/docs/python/api/datatypes.html) for
details on datatypes and the [format](https://github.com/apache/arrow/blob/master/format/Schema.fbs#L354-L375)
for the definitive reference.

[`NullArray`]: arrow::array::NullArray

**Variants:**
- `Null` - represents `DataType::Null` (castable to/from any other type)
- `Boolean(Option<bool>)` - true or false value
- `Float16(Option<half::f16>)` - 16bit float
- `Float32(Option<f32>)` - 32bit float
- `Float64(Option<f64>)` - 64bit float
- `Decimal32(Option<i32>, u8, i8)` - 32bit decimal, using the i32 to represent the decimal, precision scale
- `Decimal64(Option<i64>, u8, i8)` - 64bit decimal, using the i64 to represent the decimal, precision scale
- `Decimal128(Option<i128>, u8, i8)` - 128bit decimal, using the i128 to represent the decimal, precision scale
- `Decimal256(Option<arrow::datatypes::i256>, u8, i8)` - 256bit decimal, using the i256 to represent the decimal, precision scale
- `Int8(Option<i8>)` - signed 8bit int
- `Int16(Option<i16>)` - signed 16bit int
- `Int32(Option<i32>)` - signed 32bit int
- `Int64(Option<i64>)` - signed 64bit int
- `UInt8(Option<u8>)` - unsigned 8bit int
- `UInt16(Option<u16>)` - unsigned 16bit int
- `UInt32(Option<u32>)` - unsigned 32bit int
- `UInt64(Option<u64>)` - unsigned 64bit int
- `Utf8(Option<String>)` - utf-8 encoded string.
- `Utf8View(Option<String>)` - utf-8 encoded string but from view types.
- `LargeUtf8(Option<String>)` - utf-8 encoded string representing a LargeString's arrow type.
- `Binary(Option<Vec<u8>>)` - binary
- `BinaryView(Option<Vec<u8>>)` - binary but from view types.
- `FixedSizeBinary(i32, Option<Vec<u8>>)` - fixed size binary
- `LargeBinary(Option<Vec<u8>>)` - large binary
- `FixedSizeList(std::sync::Arc<arrow::array::FixedSizeListArray>)` - Fixed size list scalar.
- `List(std::sync::Arc<arrow::array::ListArray>)` - Represents a single element of a [`ListArray`] as an [`ArrayRef`]
- `LargeList(std::sync::Arc<arrow::array::LargeListArray>)` - The array must be a LargeListArray with length 1.
- `Struct(std::sync::Arc<arrow::array::StructArray>)` - Represents a single element [`StructArray`] as an [`ArrayRef`]. See
- `Map(std::sync::Arc<arrow::array::MapArray>)` - Represents a single element [`MapArray`] as an [`ArrayRef`].
- `Date32(Option<i32>)` - Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
- `Date64(Option<i64>)` - Date stored as a signed 64bit int milliseconds since UNIX epoch 1970-01-01
- `Time32Second(Option<i32>)` - Time stored as a signed 32bit int as seconds since midnight
- `Time32Millisecond(Option<i32>)` - Time stored as a signed 32bit int as milliseconds since midnight
- `Time64Microsecond(Option<i64>)` - Time stored as a signed 64bit int as microseconds since midnight
- `Time64Nanosecond(Option<i64>)` - Time stored as a signed 64bit int as nanoseconds since midnight
- `TimestampSecond(Option<i64>, Option<std::sync::Arc<str>>)` - Timestamp Second
- `TimestampMillisecond(Option<i64>, Option<std::sync::Arc<str>>)` - Timestamp Milliseconds
- `TimestampMicrosecond(Option<i64>, Option<std::sync::Arc<str>>)` - Timestamp Microseconds
- `TimestampNanosecond(Option<i64>, Option<std::sync::Arc<str>>)` - Timestamp Nanoseconds
- `IntervalYearMonth(Option<i32>)` - Number of elapsed whole months
- `IntervalDayTime(Option<arrow::datatypes::IntervalDayTime>)` - Number of elapsed days and milliseconds (no leap seconds)
- `IntervalMonthDayNano(Option<arrow::datatypes::IntervalMonthDayNano>)` - A triple of the number of elapsed months, days, and nanoseconds.
- `DurationSecond(Option<i64>)` - Duration in seconds
- `DurationMillisecond(Option<i64>)` - Duration in milliseconds
- `DurationMicrosecond(Option<i64>)` - Duration in microseconds
- `DurationNanosecond(Option<i64>)` - Duration in nanoseconds
- `Union(Option<(i8, Box<ScalarValue>)>, arrow::datatypes::UnionFields, arrow::datatypes::UnionMode)` - A nested datatype that can represent slots of differing types. Components:
- `Dictionary(Box<arrow::datatypes::DataType>, Box<ScalarValue>)` - Dictionary type: index type and value

**Methods:**

- `fn new_primitive<T>(a: Option<<T as >::Native>, d: &DataType) -> Result<Self>` - Create a [`Result<ScalarValue>`] with the provided value and datatype
- `fn try_new_decimal128(value: i128, precision: u8, scale: i8) -> Result<Self>` - Create a decimal Scalar from value/precision and scale.
- `fn try_new_null(data_type: &DataType) -> Result<Self>` - Create a Null instance of ScalarValue for this datatype
- `fn new_utf8<impl Into<String>>(val: impl Trait) -> Self` - Returns a [`ScalarValue::Utf8`] representing `val`
- `fn new_utf8view<impl Into<String>>(val: impl Trait) -> Self` - Returns a [`ScalarValue::Utf8View`] representing `val`
- `fn new_interval_ym(years: i32, months: i32) -> Self` - Returns a [`ScalarValue::IntervalYearMonth`] representing
- `fn new_interval_dt(days: i32, millis: i32) -> Self` - Returns a [`ScalarValue::IntervalDayTime`] representing
- `fn new_interval_mdn(months: i32, days: i32, nanos: i64) -> Self` - Returns a [`ScalarValue::IntervalMonthDayNano`] representing
- `fn new_timestamp<T>(value: Option<i64>, tz_opt: Option<Arc<str>>) -> Self` - Returns a [`ScalarValue`] representing
- `fn new_pi(datatype: &DataType) -> Result<ScalarValue>` - Returns a [`ScalarValue`] representing PI
- `fn new_pi_upper(datatype: &DataType) -> Result<ScalarValue>` - Returns a [`ScalarValue`] representing PI's upper bound
- `fn new_negative_pi_lower(datatype: &DataType) -> Result<ScalarValue>` - Returns a [`ScalarValue`] representing -PI's lower bound
- `fn new_frac_pi_2_upper(datatype: &DataType) -> Result<ScalarValue>` - Returns a [`ScalarValue`] representing FRAC_PI_2's upper bound
- `fn new_neg_frac_pi_2_lower(datatype: &DataType) -> Result<ScalarValue>`
- `fn new_negative_pi(datatype: &DataType) -> Result<ScalarValue>` - Returns a [`ScalarValue`] representing -PI
- `fn new_frac_pi_2(datatype: &DataType) -> Result<ScalarValue>` - Returns a [`ScalarValue`] representing PI/2
- `fn new_neg_frac_pi_2(datatype: &DataType) -> Result<ScalarValue>` - Returns a [`ScalarValue`] representing -PI/2
- `fn new_infinity(datatype: &DataType) -> Result<ScalarValue>` - Returns a [`ScalarValue`] representing infinity
- `fn new_neg_infinity(datatype: &DataType) -> Result<ScalarValue>` - Returns a [`ScalarValue`] representing negative infinity
- `fn new_zero(datatype: &DataType) -> Result<ScalarValue>` - Create a zero value in the given type.
- `fn new_default(datatype: &DataType) -> Result<ScalarValue>` - Returns a default value for the given `DataType`.
- `fn new_one(datatype: &DataType) -> Result<ScalarValue>` - Create an one value in the given type.
- `fn new_negative_one(datatype: &DataType) -> Result<ScalarValue>` - Create a negative one value in the given type.
- `fn new_ten(datatype: &DataType) -> Result<ScalarValue>`
- `fn data_type(self: &Self) -> DataType` - return the [`DataType`] of this `ScalarValue`
- `fn arithmetic_negate(self: &Self) -> Result<Self>` - Calculate arithmetic negation for a scalar value
- `fn add<T>(self: &Self, other: T) -> Result<ScalarValue>` - Wrapping addition of `ScalarValue`
- `fn add_checked<T>(self: &Self, other: T) -> Result<ScalarValue>` - Checked addition of `ScalarValue`
- `fn sub<T>(self: &Self, other: T) -> Result<ScalarValue>` - Wrapping subtraction of `ScalarValue`
- `fn sub_checked<T>(self: &Self, other: T) -> Result<ScalarValue>` - Checked subtraction of `ScalarValue`
- `fn mul<T>(self: &Self, other: T) -> Result<ScalarValue>` - Wrapping multiplication of `ScalarValue`
- `fn mul_checked<T>(self: &Self, other: T) -> Result<ScalarValue>` - Checked multiplication of `ScalarValue`
- `fn div<T>(self: &Self, other: T) -> Result<ScalarValue>` - Performs `lhs / rhs`
- `fn rem<T>(self: &Self, other: T) -> Result<ScalarValue>` - Performs `lhs % rhs`
- `fn is_unsigned(self: &Self) -> bool`
- `fn is_null(self: &Self) -> bool` - whether this value is null or not.
- `fn distance(self: &Self, other: &ScalarValue) -> Option<usize>` - Absolute distance between two numeric values (of the same type). This method will return
- `fn to_array(self: &Self) -> Result<ArrayRef>` - Converts a scalar value into an 1-row array.
- `fn to_scalar(self: &Self) -> Result<Scalar<ArrayRef>>` - Converts a scalar into an arrow [`Scalar`] (which implements
- `fn iter_to_array<impl IntoIterator<Item = ScalarValue>>(scalars: impl Trait) -> Result<ArrayRef>` - Converts an iterator of references [`ScalarValue`] into an [`ArrayRef`]
- `fn new_list(values: &[ScalarValue], data_type: &DataType, nullable: bool) -> Arc<ListArray>` - Converts `Vec<ScalarValue>` where each element has type corresponding to
- `fn new_list_nullable(values: &[ScalarValue], data_type: &DataType) -> Arc<ListArray>` - Same as [`ScalarValue::new_list`] but with nullable set to true.
- `fn new_null_list(data_type: DataType, nullable: bool, null_len: usize) -> Self` - Create ListArray with Null with specific data type
- `fn new_list_from_iter<impl IntoIterator<Item = ScalarValue> + ExactSizeIterator>(values: impl Trait, data_type: &DataType, nullable: bool) -> Arc<ListArray>` - Converts `IntoIterator<Item = ScalarValue>` where each element has type corresponding to
- `fn new_large_list(values: &[ScalarValue], data_type: &DataType) -> Arc<LargeListArray>` - Converts `Vec<ScalarValue>` where each element has type corresponding to
- `fn to_array_of_size(self: &Self, size: usize) -> Result<ArrayRef>` - Converts a scalar value into an array of `size` rows.
- `fn convert_array_to_scalar_vec(array: &dyn Array) -> Result<Vec<Option<Vec<Self>>>>` - Retrieve ScalarValue for each row in `array`
- `fn raw_data(self: &Self) -> Result<ArrayRef>`
- `fn try_from_array(array: &dyn Array, index: usize) -> Result<Self>` - Converts a value in `array` at `index` into a ScalarValue
- `fn try_from_string(value: String, target_type: &DataType) -> Result<Self>` - Try to parse `value` into a ScalarValue of type `target_type`
- `fn try_as_str(self: &Self) -> Option<Option<&str>>` - Returns the Some(`&str`) representation of `ScalarValue` of logical string type
- `fn cast_to(self: &Self, target_type: &DataType) -> Result<Self>` - Try to cast this value to a ScalarValue of type `data_type`
- `fn cast_to_with_options(self: &Self, target_type: &DataType, cast_options: &CastOptions<'static>) -> Result<Self>` - Try to cast this value to a ScalarValue of type `data_type` with [`CastOptions`]
- `fn eq_array(self: &Self, array: &ArrayRef, index: usize) -> Result<bool>` - Compares a single row of array @ index for equality with self,
- `fn try_cmp(self: &Self, other: &Self) -> Result<Ordering>` - Compare `self` with `other` and return an `Ordering`.
- `fn size(self: &Self) -> usize` - Estimate size if bytes including `Self`. For values with internal containers such as `String`
- `fn size_of_vec(vec: &Vec<Self>) -> usize` - Estimates [size](Self::size) of [`Vec`] in bytes.
- `fn size_of_vec_deque(vec_deque: &VecDeque<Self>) -> usize` - Estimates [size](Self::size) of [`VecDeque`] in bytes.
- `fn size_of_hashset<S>(set: &HashSet<Self, S>) -> usize` - Estimates [size](Self::size) of [`HashSet`] in bytes.
- `fn compact(self: & mut Self)` - Compacts the allocation referenced by `self` to the minimum, copying the data if
- `fn compacted(self: Self) -> Self` - Compacts ([ScalarValue::compact]) the current [ScalarValue] and returns it.
- `fn min(datatype: &DataType) -> Option<ScalarValue>` - Returns the minimum value for the given numeric `DataType`.
- `fn max(datatype: &DataType) -> Option<ScalarValue>` - Returns the maximum value for the given numeric `DataType`.

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **From**
  - `fn from(value: Option<i8>) -> Self`
- **From**
  - `fn from(value: Vec<(&str, ScalarValue)>) -> Self`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **From**
  - `fn from(value: u16) -> Self`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **From**
  - `fn from(value: i32) -> Self`
- **From**
  - `fn from(value: f32) -> Self`
- **From**
  - `fn from(value: Option<u32>) -> Self`
- **From**
  - `fn from(value: Option<i64>) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> ScalarValue`
- **From**
  - `fn from(value: Option<f16>) -> Self`
- **From**
  - `fn from(value: &str) -> Self`
- **From**
  - `fn from(value: u8) -> Self`
- **From**
  - `fn from(value: i16) -> Self`
- **From**
  - `fn from(value: f64) -> Self`
- **From**
  - `fn from(value: Option<u16>) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **From**
  - `fn from(value: String) -> Self`
- **From**
  - `fn from(value: Option<i32>) -> Self`
- **From**
  - `fn from(value: Option<f32>) -> Self`
- **From**
  - `fn from(value: u64) -> Self`
- **From**
  - `fn from(value: bool) -> Self`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **From**
  - `fn from(value: i8) -> Self`
- **TryFrom**
  - `fn try_from(datatype: DataType) -> Result<Self>` - Create a Null instance of ScalarValue for this datatype
- **From**
  - `fn from(value: Option<&str>) -> Self`
- **From**
  - `fn from(value: Option<u8>) -> Self`
- **TryFrom**
  - `fn try_from(data_type: &DataType) -> Result<Self>` - Create a Null instance of ScalarValue for this datatype
- **From**
  - `fn from(value: Option<i16>) -> Self`
- **From**
  - `fn from(value: Option<f64>) -> Self`
- **From**
  - `fn from(value: u32) -> Self`
- **From**
  - `fn from(value: Option<String>) -> Self`
- **From**
  - `fn from(value: i64) -> Self`
- **From**
  - `fn from(value: f16) -> Self`
- **From**
  - `fn from(value: Option<u64>) -> Self`
- **From**
  - `fn from(value: Option<bool>) -> Self`



## datafusion_common::scalar::copy_array_data

*Function*

Compacts the data of an `ArrayData` into a new `ArrayData`.

This is useful when you want to minimize the memory footprint of an
`ArrayData`. For example, the value returned by [`Array::slice`] still
points at the same underlying data buffers as the original array, which may
hold many more values. Calling `copy_array_data` on the sliced array will
create a new, smaller, `ArrayData` that only contains the data for the
sliced array.

# Example
```
# use arrow::array::{make_array, Array, Int32Array};
use datafusion_common::scalar::copy_array_data;
let array = Int32Array::from_iter_values(0..8192);
// Take only the first 2 elements
let sliced_array = array.slice(0, 2);
// The memory footprint of `sliced_array` is close to 8192 * 4 bytes
assert_eq!(32864, sliced_array.get_array_memory_size());
// however, we can copy the data to a new `ArrayData`
let new_array = make_array(copy_array_data(&sliced_array.into_data()));
// The memory footprint of `new_array` is now only 2 * 4 bytes
// and overhead:
assert_eq!(160, new_array.get_array_memory_size());
```

See also [`ScalarValue::compact`] which applies to `ScalarValue` instances
as necessary.

```rust
fn copy_array_data(src_data: &arrow::array::ArrayData) -> arrow::array::ArrayData
```



## datafusion_common::scalar::date_to_timestamp_multiplier

*Function*

Returns the multiplier that converts the input date representation into the
desired timestamp unit, if the conversion requires a multiplication that can
overflow an `i64`.

```rust
fn date_to_timestamp_multiplier(source_type: &arrow::datatypes::DataType, target_type: &arrow::datatypes::DataType) -> Option<i64>
```



## datafusion_common::scalar::dict_from_values

*Function*

Create a `DictionaryArray` from the provided values array.

Each element gets a unique key (`0..N-1`), without deduplication.
Useful for wrapping arrays in dictionary form.

# Input
["alice", "bob", "alice", null, "carol"]

# Output
`DictionaryArray<Int32>`
{
  keys:   [0, 1, 2, 3, 4],
  values: ["alice", "bob", "alice", null, "carol"]
}

```rust
fn dict_from_values<K>(values_array: arrow::array::ArrayRef) -> crate::error::Result<arrow::array::ArrayRef>
```



## datafusion_common::scalar::ensure_timestamp_in_bounds

*Function*

Ensures the provided value can be represented as a timestamp with the given
multiplier. Returns an [`DataFusionError::Execution`] when the converted
value would overflow the timestamp range.

```rust
fn ensure_timestamp_in_bounds(value: i64, multiplier: i64, source_type: &arrow::datatypes::DataType, target_type: &arrow::datatypes::DataType) -> crate::error::Result<()>
```



## datafusion_common::scalar::get_dict_value

*Function*

Return a reference to the values array and the index into it for a
dictionary array

# Errors

Errors if the array cannot be downcasted to DictionaryArray

```rust
fn get_dict_value<K>(array: &dyn Array, index: usize) -> crate::error::Result<(&arrow::array::ArrayRef, Option<usize>)>
```



## datafusion_common::scalar::partial_cmp_struct

*Function*

```rust
fn partial_cmp_struct(s1: &arrow::array::StructArray, s2: &arrow::array::StructArray) -> Option<std::cmp::Ordering>
```



