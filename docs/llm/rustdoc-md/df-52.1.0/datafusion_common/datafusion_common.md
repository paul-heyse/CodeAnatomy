**datafusion_common**

# Module: datafusion_common

## Contents

**Modules**

- [`alias`](#alias)
- [`cast`](#cast) - This module provides DataFusion specific casting functions
- [`config`](#config) - Runtime configuration, via [`ConfigOptions`]
- [`cse`](#cse) - Common Subexpression Elimination logic implemented in [`CSE`] can be controlled with
- [`datatype`](#datatype) - [`DataTypeExt`] and [`FieldExt`] extension trait for working with Arrow [`DataType`] and [`Field`]s
- [`diagnostic`](#diagnostic)
- [`display`](#display) - Types for plan display
- [`encryption`](#encryption) - This module provides types and functions related to encryption in Parquet files.
- [`error`](#error) - # Error Handling in DataFusion
- [`file_options`](#file_options) - Options related to how files should be written
- [`format`](#format)
- [`hash_map`](#hash_map)
- [`hash_set`](#hash_set)
- [`hash_utils`](#hash_utils) - Functionality used both on logical and physical plans
- [`instant`](#instant) - WASM-compatible `Instant` wrapper.
- [`metadata`](#metadata)
- [`nested_struct`](#nested_struct)
- [`parquet_config`](#parquet_config)
- [`parsers`](#parsers) - Interval parsing logic
- [`pruning`](#pruning)
- [`rounding`](#rounding) - Floating point rounding mode utility library
- [`scalar`](#scalar) - [`ScalarValue`]: stores single  values
- [`spans`](#spans)
- [`stats`](#stats) - This module provides data structures to represent statistics
- [`test_util`](#test_util) - Utility functions to make testing DataFusion based crates easier
- [`tree_node`](#tree_node) - [`TreeNode`] for visiting and rewriting expression and plan trees
- [`types`](#types)
- [`utils`](#utils) - This module provides the bisect function, which implements binary search.

**Macros**

- [`arrow_datafusion_err`](#arrow_datafusion_err)
- [`arrow_err`](#arrow_err)
- [`assert_batches_eq`](#assert_batches_eq) - Compares formatted output of a record batch with an expected
- [`assert_batches_sorted_eq`](#assert_batches_sorted_eq) - Compares formatted output of a record batch with an expected
- [`assert_contains`](#assert_contains) - A macro to assert that one string is contained within another with
- [`assert_eq_or_internal_err`](#assert_eq_or_internal_err) - Assert equality, returning `DataFusionError::Internal` on failure.
- [`assert_ne_or_internal_err`](#assert_ne_or_internal_err) - Assert inequality, returning `DataFusionError::Internal` on failure.
- [`assert_not_contains`](#assert_not_contains) - A macro to assert that one string is NOT contained within another with
- [`assert_or_internal_err`](#assert_or_internal_err) - Assert a condition, returning `DataFusionError::Internal` on failure.
- [`config_datafusion_err`](#config_datafusion_err) - Macro wraps `$ERR` to add backtrace feature
- [`config_err`](#config_err) - Macro wraps Err(`$ERR`) to add backtrace feature
- [`config_field`](#config_field) - Macro that generates [`ConfigField`] for a given type.
- [`config_namespace`](#config_namespace) - A macro that wraps a configuration struct and automatically derives
- [`context`](#context)
- [`create_array`](#create_array)
- [`downcast_value`](#downcast_value) - Downcast an Arrow Array to a concrete type, return an `DataFusionError::Internal` if the cast is
- [`exec_datafusion_err`](#exec_datafusion_err) - Macro wraps `$ERR` to add backtrace feature
- [`exec_err`](#exec_err) - Macro wraps Err(`$ERR`) to add backtrace feature
- [`extensions_options`](#extensions_options) - Convenience macro to create [`ExtensionsOptions`].
- [`ffi_datafusion_err`](#ffi_datafusion_err) - Macro wraps `$ERR` to add backtrace feature
- [`ffi_err`](#ffi_err) - Macro wraps Err(`$ERR`) to add backtrace feature
- [`internal_datafusion_err`](#internal_datafusion_err) - Macro wraps `$ERR` to add backtrace feature
- [`internal_err`](#internal_err) - Macro wraps Err(`$ERR`) to add backtrace feature
- [`not_impl_datafusion_err`](#not_impl_datafusion_err) - Macro wraps `$ERR` to add backtrace feature
- [`not_impl_err`](#not_impl_err) - Macro wraps Err(`$ERR`) to add backtrace feature
- [`plan_datafusion_err`](#plan_datafusion_err) - Macro wraps `$ERR` to add backtrace feature
- [`plan_err`](#plan_err) - Macro wraps Err(`$ERR`) to add backtrace feature
- [`record_batch`](#record_batch) - Creates a record batch from literal slice of values, suitable for rapid
- [`resources_datafusion_err`](#resources_datafusion_err) - Macro wraps `$ERR` to add backtrace feature
- [`resources_err`](#resources_err) - Macro wraps Err(`$ERR`) to add backtrace feature
- [`schema_datafusion_err`](#schema_datafusion_err)
- [`schema_err`](#schema_err)
- [`sql_datafusion_err`](#sql_datafusion_err)
- [`sql_err`](#sql_err)
- [`substrait_datafusion_err`](#substrait_datafusion_err) - Macro wraps `$ERR` to add backtrace feature
- [`substrait_err`](#substrait_err) - Macro wraps Err(`$ERR`) to add backtrace feature
- [`unwrap_or_internal_err`](#unwrap_or_internal_err) - Unwrap an `Option` if possible. Otherwise return an `DataFusionError::Internal`.

**Type Aliases**

- [`HashMap`](#hashmap)
- [`HashSet`](#hashset)

---

## datafusion_common::HashMap

*Type Alias*: `hashbrown::HashMap<K, V, S>`



## datafusion_common::HashSet

*Type Alias*: `hashbrown::HashSet<T, S>`



## Module: alias



## datafusion_common::arrow_datafusion_err

*Declarative Macro*

```rust
macro_rules! arrow_datafusion_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::arrow_err

*Declarative Macro*

```rust
macro_rules! arrow_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::assert_batches_eq

*Declarative Macro*

Compares formatted output of a record batch with an expected
vector of strings, with the result of pretty formatting record
batches. This is a macro so errors appear on the correct line

Designed so that failure output can be directly copy/pasted
into the test code as expected results.

Expects to be called about like this:

`assert_batches_eq!(expected_lines: &[&str], batches: &[RecordBatch])`

# Example
```
# use std::sync::Arc;
# use arrow::record_batch::RecordBatch;
# use arrow::array::{ArrayRef, Int32Array};
# use datafusion_common::assert_batches_eq;
let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
let batch = RecordBatch::try_from_iter([("column", col)]).unwrap();
// Expected output is a vec of strings
let expected = vec![
    "+--------+",
    "| column |",
    "+--------+",
    "| 1      |",
    "| 2      |",
    "+--------+",
];
// compare the formatted output of the record batch with the expected output
assert_batches_eq!(expected, &[batch]);
```

```rust
macro_rules! assert_batches_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => { ... };
}
```



## datafusion_common::assert_batches_sorted_eq

*Declarative Macro*

Compares formatted output of a record batch with an expected
vector of strings in a way that order does not matter.
This is a macro so errors appear on the correct line

See [`assert_batches_eq`] for more details and example.

Expects to be called about like this:

`assert_batch_sorted_eq!(expected_lines: &[&str], batches: &[RecordBatch])`

```rust
macro_rules! assert_batches_sorted_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => { ... };
}
```



## datafusion_common::assert_contains

*Declarative Macro*

A macro to assert that one string is contained within another with
a nice error message if they are not.

Usage: `assert_contains!(actual, expected)`

Is a macro so test error
messages are on the same line as the failure;

Both arguments must be convertible into Strings ([`Into`]<[`String`]>)

```rust
macro_rules! assert_contains {
    ($ACTUAL: expr, $EXPECTED: expr) => { ... };
}
```



## datafusion_common::assert_eq_or_internal_err

*Declarative Macro*

Assert equality, returning `DataFusionError::Internal` on failure.

# Examples

```text
assert_eq_or_internal_err!(actual, expected);
assert_eq_or_internal_err!(left_expr, right_expr, "values must match");
assert_eq_or_internal_err!(lhs, rhs, "metadata: {}", extra);
```

```rust
macro_rules! assert_eq_or_internal_err {
    ($left:expr, $right:expr $(,)?) => { ... };
    ($left:expr, $right:expr, $($arg:tt)+) => { ... };
}
```



## datafusion_common::assert_ne_or_internal_err

*Declarative Macro*

Assert inequality, returning `DataFusionError::Internal` on failure.

# Examples

```text
assert_ne_or_internal_err!(left, right);
assert_ne_or_internal_err!(lhs_expr, rhs_expr, "values must differ");
assert_ne_or_internal_err!(a, b, "context {}", info);
```

```rust
macro_rules! assert_ne_or_internal_err {
    ($left:expr, $right:expr $(,)?) => { ... };
    ($left:expr, $right:expr, $($arg:tt)+) => { ... };
}
```



## datafusion_common::assert_not_contains

*Declarative Macro*

A macro to assert that one string is NOT contained within another with
a nice error message if they are are.

Usage: `assert_not_contains!(actual, unexpected)`

Is a macro so test error
messages are on the same line as the failure;

Both arguments must be convertible into Strings ([`Into`]<[`String`]>)

```rust
macro_rules! assert_not_contains {
    ($ACTUAL: expr, $UNEXPECTED: expr) => { ... };
}
```



## datafusion_common::assert_or_internal_err

*Declarative Macro*

Assert a condition, returning `DataFusionError::Internal` on failure.

# Examples

```text
assert_or_internal_err!(predicate);
assert_or_internal_err!(predicate, "human readable message");
assert_or_internal_err!(predicate, format!("details: {}", value));
```

```rust
macro_rules! assert_or_internal_err {
    ($cond:expr) => { ... };
    ($cond:expr, $($arg:tt)+) => { ... };
}
```



## Module: cast

This module provides DataFusion specific casting functions
that provide error handling. They are intended to "never fail"
but provide an error message rather than a panic, as the corresponding
kernels in arrow-rs such as `as_boolean_array` do.



## Module: config

Runtime configuration, via [`ConfigOptions`]



## datafusion_common::config_datafusion_err

*Declarative Macro*

Macro wraps `$ERR` to add backtrace feature

```rust
macro_rules! config_datafusion_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::config_err

*Declarative Macro*

Macro wraps Err(`$ERR`) to add backtrace feature

```rust
macro_rules! config_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::config_field

*Declarative Macro*

Macro that generates [`ConfigField`] for a given type.

# Usage
This always requires [`Display`] to be implemented for the given type.

There are two ways to invoke this macro. The first one uses
[`default_config_transform`]/[`FromStr`] to parse the data:

```ignore
config_field(MyType);
```

Note that the parsing error MUST implement [`std::error::Error`]!

Or you can specify how you want to parse an [`str`] into the type:

```ignore
fn parse_it(s: &str) -> Result<MyType> {
    ...
}

config_field(
    MyType,
    value => parse_it(value)
);
```

```rust
macro_rules! config_field {
    ($t:ty) => { ... };
    ($t:ty, $arg:ident => $transform:expr) => { ... };
}
```



## datafusion_common::config_namespace

*Declarative Macro*

A macro that wraps a configuration struct and automatically derives
[`Default`] and [`ConfigField`] for it, allowing it to be used
in the [`ConfigOptions`] configuration tree.

`transform` is used to normalize values before parsing.

For example,

```ignore
config_namespace! {
   /// Amazing config
   pub struct MyConfig {
       /// Field 1 doc
       field1: String, transform = str::to_lowercase, default = "".to_string()

       /// Field 2 doc
       field2: usize, default = 232

       /// Field 3 doc
       field3: Option<usize>, default = None
   }
}
```

Will generate

```ignore
/// Amazing config
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct MyConfig {
    /// Field 1 doc
    field1: String,
    /// Field 2 doc
    field2: usize,
    /// Field 3 doc
    field3: Option<usize>,
}
impl ConfigField for MyConfig {
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "field1" => {
                let value = str::to_lowercase(value);
                self.field1.set(rem, value.as_ref())
            },
            "field2" => self.field2.set(rem, value.as_ref()),
            "field3" => self.field3.set(rem, value.as_ref()),
            _ => _internal_err!(
                "Config value \"{}\" not found on MyConfig",
                key
            ),
        }
    }

    fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
        let key = format!("{}.field1", key_prefix);
        let desc = "Field 1 doc";
        self.field1.visit(v, key.as_str(), desc);
        let key = format!("{}.field2", key_prefix);
        let desc = "Field 2 doc";
        self.field2.visit(v, key.as_str(), desc);
        let key = format!("{}.field3", key_prefix);
        let desc = "Field 3 doc";
        self.field3.visit(v, key.as_str(), desc);
    }
}

impl Default for MyConfig {
    fn default() -> Self {
        Self {
            field1: "".to_string(),
            field2: 232,
            field3: None,
        }
    }
}
```

NB: Misplaced commas may result in nonsensical errors

```rust
macro_rules! config_namespace {
    (
        $(#[doc = $struct_d:tt])* // Struct-level documentation attributes
        $(#[deprecated($($struct_depr:tt)*)])? // Optional struct-level deprecated attribute
        $(#[allow($($struct_de:tt)*)])?
        $vis:vis struct $struct_name:ident {
            $(
                $(#[doc = $d:tt])* // Field-level documentation attributes
                $(#[deprecated($($field_depr:tt)*)])? // Optional field-level deprecated attribute
                $(#[allow($($field_de:tt)*)])?
                $field_vis:vis $field_name:ident : $field_type:ty,
                $(warn = $warn:expr,)?
                $(transform = $transform:expr,)?
                default = $default:expr
            )*$(,)*
        }
    ) => { ... };
}
```



## datafusion_common::context

*Declarative Macro*

```rust
macro_rules! context {
    ($desc:expr, $err:expr) => { ... };
}
```



## datafusion_common::create_array

*Declarative Macro*

```rust
macro_rules! create_array {
    (Boolean, $values: expr) => { ... };
    (Int8, $values: expr) => { ... };
    (Int16, $values: expr) => { ... };
    (Int32, $values: expr) => { ... };
    (Int64, $values: expr) => { ... };
    (UInt8, $values: expr) => { ... };
    (UInt16, $values: expr) => { ... };
    (UInt32, $values: expr) => { ... };
    (UInt64, $values: expr) => { ... };
    (Float16, $values: expr) => { ... };
    (Float32, $values: expr) => { ... };
    (Float64, $values: expr) => { ... };
    (Utf8, $values: expr) => { ... };
}
```



## Module: cse

Common Subexpression Elimination logic implemented in [`CSE`] can be controlled with
a [`CSEController`], that defines how to eliminate common subtrees from a particular
[`TreeNode`] tree.



## Module: datatype

[`DataTypeExt`] and [`FieldExt`] extension trait for working with Arrow [`DataType`] and [`Field`]s



## Module: diagnostic



## Module: display

Types for plan display



## datafusion_common::downcast_value

*Declarative Macro*

Downcast an Arrow Array to a concrete type, return an `DataFusionError::Internal` if the cast is
not possible. In normal usage of DataFusion the downcast should always succeed.

Example: `let array = downcast_value!(values, Int32Array)`

```rust
macro_rules! downcast_value {
    ($Value: expr, $Type: ident) => { ... };
    ($Value: expr, $Type: ident, $T: tt) => { ... };
}
```



## Module: encryption

This module provides types and functions related to encryption in Parquet files.



## Module: error

# Error Handling in DataFusion

In DataFusion, there are two types of errors that can be raised:

1. Expected errors – These indicate invalid operations performed by the caller,
   such as attempting to open a non-existent file. Different categories exist to
   distinguish their sources (e.g., [`DataFusionError::ArrowError`],
   [`DataFusionError::IoError`], etc.).

2. Unexpected errors – Represented by [`DataFusionError::Internal`], these
   indicate that an internal invariant has been broken, suggesting a potential
   bug in the system.

There are several convenient macros for throwing errors. For example, use
`exec_err!` for expected errors.
For invariant checks, you can use `assert_or_internal_err!`,
`assert_eq_or_internal_err!`, `assert_ne_or_internal_err!` for easier assertions.
On the performance-critical path, use `debug_assert!` instead to reduce overhead.



## datafusion_common::exec_datafusion_err

*Declarative Macro*

Macro wraps `$ERR` to add backtrace feature

```rust
macro_rules! exec_datafusion_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::exec_err

*Declarative Macro*

Macro wraps Err(`$ERR`) to add backtrace feature

```rust
macro_rules! exec_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::extensions_options

*Declarative Macro*

Convenience macro to create [`ExtensionsOptions`].

The created structure implements the following traits:

- [`Clone`]
- [`Debug`]
- [`Default`]
- [`ExtensionOptions`]

# Usage
The syntax is:

```text
extensions_options! {
     /// Struct docs (optional).
    [<vis>] struct <StructName> {
        /// Field docs (optional)
        [<vis>] <field_name>: <field_type>, default = <default_value>

        ... more fields
    }
}
```

The placeholders are:
- `[<vis>]`: Optional visibility modifier like `pub` or `pub(crate)`.
- `<StructName>`: Struct name like `MyStruct`.
- `<field_name>`: Field name like `my_field`.
- `<field_type>`: Field type like `u8`.
- `<default_value>`: Default value matching the field type like `42`.

# Example
See also a full example on the [`ConfigExtension`] documentation

```
use datafusion_common::extensions_options;

extensions_options! {
    /// My own config options.
    pub struct MyConfig {
        /// Should "foo" be replaced by "bar"?
        pub foo_to_bar: bool, default = true

        /// How many "baz" should be created?
        pub baz_count: usize, default = 1337
    }
}
```


[`Debug`]: std::fmt::Debug
[`ExtensionsOptions`]: crate::config::ExtensionOptions

```rust
macro_rules! extensions_options {
    (
     $(#[doc = $struct_d:tt])*
     $vis:vis struct $struct_name:ident {
        $(
        $(#[doc = $d:tt])*
        $field_vis:vis $field_name:ident : $field_type:ty, default = $default:expr
        )*$(,)*
    }
    ) => { ... };
}
```



## datafusion_common::ffi_datafusion_err

*Declarative Macro*

Macro wraps `$ERR` to add backtrace feature

```rust
macro_rules! ffi_datafusion_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::ffi_err

*Declarative Macro*

Macro wraps Err(`$ERR`) to add backtrace feature

```rust
macro_rules! ffi_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## Module: file_options

Options related to how files should be written



## Module: format



## Module: hash_map



## Module: hash_set



## Module: hash_utils

Functionality used both on logical and physical plans



## Module: instant

WASM-compatible `Instant` wrapper.



## datafusion_common::internal_datafusion_err

*Declarative Macro*

Macro wraps `$ERR` to add backtrace feature

```rust
macro_rules! internal_datafusion_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::internal_err

*Declarative Macro*

Macro wraps Err(`$ERR`) to add backtrace feature

```rust
macro_rules! internal_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## Module: metadata



## Module: nested_struct



## datafusion_common::not_impl_datafusion_err

*Declarative Macro*

Macro wraps `$ERR` to add backtrace feature

```rust
macro_rules! not_impl_datafusion_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::not_impl_err

*Declarative Macro*

Macro wraps Err(`$ERR`) to add backtrace feature

```rust
macro_rules! not_impl_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## Module: parquet_config



## Module: parsers

Interval parsing logic



## datafusion_common::plan_datafusion_err

*Declarative Macro*

Macro wraps `$ERR` to add backtrace feature

```rust
macro_rules! plan_datafusion_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::plan_err

*Declarative Macro*

Macro wraps Err(`$ERR`) to add backtrace feature

```rust
macro_rules! plan_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## Module: pruning



## datafusion_common::record_batch

*Declarative Macro*

Creates a record batch from literal slice of values, suitable for rapid
testing and development.

Example:
```
use datafusion_common::record_batch;
let batch = record_batch!(
    ("a", Int32, vec![1, 2, 3]),
    ("b", Float64, vec![Some(4.0), None, Some(5.0)]),
    ("c", Utf8, vec!["alpha", "beta", "gamma"])
);
```

```rust
macro_rules! record_batch {
    ($(($name: expr, $type: ident, $values: expr)),*) => { ... };
}
```



## datafusion_common::resources_datafusion_err

*Declarative Macro*

Macro wraps `$ERR` to add backtrace feature

```rust
macro_rules! resources_datafusion_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::resources_err

*Declarative Macro*

Macro wraps Err(`$ERR`) to add backtrace feature

```rust
macro_rules! resources_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## Module: rounding

Floating point rounding mode utility library
TODO: Remove this custom implementation and the "libc" dependency when
      floating-point rounding mode manipulation functions become available
      in Rust.



## Module: scalar

[`ScalarValue`]: stores single  values



## datafusion_common::schema_datafusion_err

*Declarative Macro*

```rust
macro_rules! schema_datafusion_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::schema_err

*Declarative Macro*

```rust
macro_rules! schema_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## Module: spans



## datafusion_common::sql_datafusion_err

*Declarative Macro*

```rust
macro_rules! sql_datafusion_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::sql_err

*Declarative Macro*

```rust
macro_rules! sql_err {
    ($ERR:expr $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## Module: stats

This module provides data structures to represent statistics



## datafusion_common::substrait_datafusion_err

*Declarative Macro*

Macro wraps `$ERR` to add backtrace feature

```rust
macro_rules! substrait_datafusion_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## datafusion_common::substrait_err

*Declarative Macro*

Macro wraps Err(`$ERR`) to add backtrace feature

```rust
macro_rules! substrait_err {
    ($($args:expr),* $(; diagnostic = $DIAG:expr)?) => { ... };
}
```



## Module: test_util

Utility functions to make testing DataFusion based crates easier



## Module: tree_node

[`TreeNode`] for visiting and rewriting expression and plan trees



## Module: types



## datafusion_common::unwrap_or_internal_err

*Declarative Macro*

Unwrap an `Option` if possible. Otherwise return an `DataFusionError::Internal`.
In normal usage of DataFusion the unwrap should always succeed.

Example: `let values = unwrap_or_internal_err!(values)`

```rust
macro_rules! unwrap_or_internal_err {
    ($Value: ident) => { ... };
}
```



## Module: utils

This module provides the bisect function, which implements binary search.



