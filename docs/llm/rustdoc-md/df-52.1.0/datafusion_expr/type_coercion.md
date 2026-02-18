**datafusion_expr > type_coercion**

# Module: type_coercion

## Contents

**Modules**

- [`aggregates`](#aggregates)
- [`functions`](#functions)
- [`other`](#other)

**Functions**

- [`is_datetime`](#is_datetime) - Determine whether the given data type `dt` is a `Date` or `Timestamp`.
- [`is_decimal`](#is_decimal) - Determine whether the given data type `dt` is a `Decimal`.
- [`is_interval`](#is_interval) - Determine whether the given data type 'dt' is a `Interval`.
- [`is_null`](#is_null) - Determine whether the given data type `dt` is `Null`.
- [`is_signed_numeric`](#is_signed_numeric) - Determine whether the given data type `dt` represents signed numeric values.
- [`is_timestamp`](#is_timestamp) - Determine whether the given data type `dt` is a `Timestamp`.
- [`is_utf8_or_utf8view_or_large_utf8`](#is_utf8_or_utf8view_or_large_utf8) - Determine whether the given data type `dt` is a `Utf8` or `Utf8View` or `LargeUtf8`.

---

## Module: aggregates



## Module: functions



## datafusion_expr::type_coercion::is_datetime

*Function*

Determine whether the given data type `dt` is a `Date` or `Timestamp`.

```rust
fn is_datetime(dt: &arrow::datatypes::DataType) -> bool
```



## datafusion_expr::type_coercion::is_decimal

*Function*

Determine whether the given data type `dt` is a `Decimal`.

```rust
fn is_decimal(dt: &arrow::datatypes::DataType) -> bool
```



## datafusion_expr::type_coercion::is_interval

*Function*

Determine whether the given data type 'dt' is a `Interval`.

```rust
fn is_interval(dt: &arrow::datatypes::DataType) -> bool
```



## datafusion_expr::type_coercion::is_null

*Function*

Determine whether the given data type `dt` is `Null`.

```rust
fn is_null(dt: &arrow::datatypes::DataType) -> bool
```



## datafusion_expr::type_coercion::is_signed_numeric

*Function*

Determine whether the given data type `dt` represents signed numeric values.

```rust
fn is_signed_numeric(dt: &arrow::datatypes::DataType) -> bool
```



## datafusion_expr::type_coercion::is_timestamp

*Function*

Determine whether the given data type `dt` is a `Timestamp`.

```rust
fn is_timestamp(dt: &arrow::datatypes::DataType) -> bool
```



## datafusion_expr::type_coercion::is_utf8_or_utf8view_or_large_utf8

*Function*

Determine whether the given data type `dt` is a `Utf8` or `Utf8View` or `LargeUtf8`.

```rust
fn is_utf8_or_utf8view_or_large_utf8(dt: &arrow::datatypes::DataType) -> bool
```



## Module: other



