**datafusion_common > types > native**

# Module: types::native

## Contents

**Enums**

- [`NativeType`](#nativetype) - Representation of a type that DataFusion can handle natively. It is a subset

---

## datafusion_common::types::native::NativeType

*Enum*

Representation of a type that DataFusion can handle natively. It is a subset
of the physical variants in Arrow's native [`DataType`].

**Variants:**
- `Null` - Null type
- `Boolean` - A boolean type representing the values `true` and `false`.
- `Int8` - A signed 8-bit integer.
- `Int16` - A signed 16-bit integer.
- `Int32` - A signed 32-bit integer.
- `Int64` - A signed 64-bit integer.
- `UInt8` - An unsigned 8-bit integer.
- `UInt16` - An unsigned 16-bit integer.
- `UInt32` - An unsigned 32-bit integer.
- `UInt64` - An unsigned 64-bit integer.
- `Float16` - A 16-bit floating point number.
- `Float32` - A 32-bit floating point number.
- `Float64` - A 64-bit floating point number.
- `Timestamp(arrow::datatypes::TimeUnit, Option<std::sync::Arc<str>>)` - A timestamp with an optional timezone.
- `Date` - A signed date representing the elapsed time since UNIX epoch (1970-01-01)
- `Time(arrow::datatypes::TimeUnit)` - A signed time representing the elapsed time since midnight in the unit of `TimeUnit`.
- `Duration(arrow::datatypes::TimeUnit)` - Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
- `Interval(arrow::datatypes::IntervalUnit)` - A "calendar" interval which models types that don't necessarily
- `Binary` - Opaque binary data of variable length.
- `FixedSizeBinary(i32)` - Opaque binary data of fixed size.
- `String` - A variable-length string in Unicode with UTF-8 encoding.
- `List(super::LogicalFieldRef)` - A list of some logical data type with variable length.
- `FixedSizeList(super::LogicalFieldRef, i32)` - A list of some logical data type with fixed length.
- `Struct(super::LogicalFields)` - A nested type that contains a number of sub-fields.
- `Union(super::LogicalUnionFields)` - A nested type that can represent slots of differing types.
- `Decimal(u8, i8)` - Decimal value with precision and scale
- `Map(super::LogicalFieldRef)` - A Map is a type that an association between a key and a value.

**Methods:**

- `fn is_numeric(self: &Self) -> bool`
- `fn is_integer(self: &Self) -> bool`
- `fn is_timestamp(self: &Self) -> bool`
- `fn is_date(self: &Self) -> bool`
- `fn is_time(self: &Self) -> bool`
- `fn is_interval(self: &Self) -> bool`
- `fn is_duration(self: &Self) -> bool`
- `fn is_binary(self: &Self) -> bool`
- `fn is_null(self: &Self) -> bool`
- `fn is_decimal(self: &Self) -> bool`
- `fn is_float(self: &Self) -> bool`

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &NativeType) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **LogicalType**
  - `fn native(self: &Self) -> &NativeType`
  - `fn signature(self: &Self) -> TypeSignature`
  - `fn default_cast_for(self: &Self, origin: &DataType) -> Result<DataType>` - Returns the default casted type for the given arrow type
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &NativeType) -> $crate::option::Option<$crate::cmp::Ordering>`
- **From**
  - `fn from(value: &DataType) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Ord**
  - `fn cmp(self: &Self, other: &NativeType) -> $crate::cmp::Ordering`
- **From**
  - `fn from(value: DataType) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> NativeType`



