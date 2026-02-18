**datafusion_functions**

# Module: datafusion_functions

## Contents

**Modules**

- [`core`](#core) - Core datafusion expressions
- [`crypto`](#crypto) - "crypto" DataFusion functions
- [`datetime`](#datetime) - Date and time expressions.
- [`encoding`](#encoding) - Encoding expressions.
- [`expr_fn`](#expr_fn) - Fluent-style API for creating `Expr`s
- [`macros`](#macros)
- [`math`](#math) - Mathematical functions.
- [`planner`](#planner) - SQL planning extensions like [`UserDefinedFunctionPlanner`]
- [`regex`](#regex) - Regular expression functions.
- [`string`](#string) - "string" DataFusion functions
- [`strings`](#strings)
- [`unicode`](#unicode) - "unicode" DataFusion functions
- [`utils`](#utils)

**Macros**

- [`downcast_arg`](#downcast_arg) - Downcast an argument to a specific array type, returning an internal error
- [`downcast_named_arg`](#downcast_named_arg) - Downcast a named argument to a specific array type, returning an internal error
- [`export_functions`](#export_functions) - macro that exports a list of function names as:
- [`make_abs_function`](#make_abs_function)
- [`make_udf_function`](#make_udf_function) - Creates a singleton `ScalarUDF` of the `$UDF` function and a function
- [`make_udf_function_with_config`](#make_udf_function_with_config) - Creates a singleton `ScalarUDF` of the `$UDF` function and a function
- [`make_wrapping_abs_function`](#make_wrapping_abs_function)

**Functions**

- [`all_default_functions`](#all_default_functions) - Return all default functions
- [`register_all`](#register_all) - Registers all enabled packages with a [`FunctionRegistry`]

---

## datafusion_functions::all_default_functions

*Function*

Return all default functions

```rust
fn all_default_functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>>
```



## Module: core

Core datafusion expressions
These are always available and not controlled by a feature flag
"core" DataFusion functions



## Module: crypto

"crypto" DataFusion functions



## Module: datetime

Date and time expressions.
Contains functions such as to_timestamp
Enabled via feature flag `datetime_expressions`
date & time DataFusion functions



## datafusion_functions::downcast_arg

*Declarative Macro*

Downcast an argument to a specific array type, returning an internal error
if the cast fails

$ARG: ArrayRef
$ARRAY_TYPE: the type of array to cast the argument to

```rust
macro_rules! downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => { ... };
}
```



## datafusion_functions::downcast_named_arg

*Declarative Macro*

Downcast a named argument to a specific array type, returning an internal error
if the cast fails

$ARG: ArrayRef
$NAME: name of the argument (for error messages)
$ARRAY_TYPE: the type of array to cast the argument to

```rust
macro_rules! downcast_named_arg {
    ($ARG:expr, $NAME:expr, $ARRAY_TYPE:ident) => { ... };
}
```



## Module: encoding

Encoding expressions.
Contains Hex and binary `encode` and `decode` functions.
Enabled via feature flag `encoding_expressions`



## datafusion_functions::export_functions

*Declarative Macro*

macro that exports a list of function names as:
1. individual functions in an `expr_fn` module
2. a single function that returns a list of all functions

Equivalent to
```text
pub mod expr_fn {
    use super::*;
    /// Return encode(arg)
    pub fn encode(args: Vec<Expr>) -> Expr {
        super::encode().call(args)
    }
 ...
/// Return a list of all functions in this package
pub(crate) fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
      encode(),
      decode()
   ]
}
```

Exported functions accept:
- `Vec<Expr>` argument (single argument followed by a comma)
- Variable number of `Expr` arguments (zero or more arguments, must be without commas)
- Functions that require config (marked with `@config` prefix)

Note on configuration construction paths:
- The convenience wrappers generated for `@config` functions call the inner
  constructor with `ConfigOptions::default()`. These wrappers are intended
  primarily for programmatic `Expr` construction and convenience usage.
- When functions are registered in a session, DataFusion will call
  `with_updated_config()` to create a `ScalarUDF` instance using the session's
  actual `ConfigOptions`. This also happens when configuration changes at runtime
  (e.g., via `SET` statements). In short: the macro uses the default config for
  convenience constructors; the session config is applied when functions are
  registered or when configuration is updated.

```rust
macro_rules! export_functions {
    ($(($FUNC:ident, $DOC:expr, $($arg:tt)*)),*) => { ... };
    (single $FUNC:ident, $DOC:expr, @config) => { ... };
    (single $FUNC:ident, $DOC:expr, @config $arg:ident,) => { ... };
    (single $FUNC:ident, $DOC:expr, @config $($arg:ident)*) => { ... };
    (single $FUNC:ident, $DOC:expr, $arg:ident,) => { ... };
    (single $FUNC:ident, $DOC:expr, $($arg:ident)*) => { ... };
}
```



## Module: expr_fn

Fluent-style API for creating `Expr`s



## Module: macros



## datafusion_functions::make_abs_function

*Declarative Macro*

```rust
macro_rules! make_abs_function {
    ($ARRAY_TYPE:ident) => { ... };
}
```



## datafusion_functions::make_udf_function

*Declarative Macro*

Creates a singleton `ScalarUDF` of the `$UDF` function and a function
named `$NAME` which returns that singleton. Optionally use a custom constructor
`$CTOR` which defaults to `$UDF::new()` if not specified.

This is used to ensure creating the list of `ScalarUDF` only happens once.

```rust
macro_rules! make_udf_function {
    ($UDF:ty, $NAME:ident, $CTOR:expr) => { ... };
    ($UDF:ty, $NAME:ident) => { ... };
}
```



## datafusion_functions::make_udf_function_with_config

*Declarative Macro*

Creates a singleton `ScalarUDF` of the `$UDF` function and a function
named `$NAME` which returns that singleton. The function takes a
configuration argument of type `$CONFIG_TYPE` to create the UDF.

```rust
macro_rules! make_udf_function_with_config {
    ($UDF:ty, $NAME:ident) => { ... };
}
```



## datafusion_functions::make_wrapping_abs_function

*Declarative Macro*

```rust
macro_rules! make_wrapping_abs_function {
    ($ARRAY_TYPE:ident) => { ... };
}
```



## Module: math

Mathematical functions.
Enabled via feature flag `math_expressions`
"math" DataFusion functions



## Module: planner

SQL planning extensions like [`UserDefinedFunctionPlanner`]



## Module: regex

Regular expression functions.
Enabled via feature flag `regex_expressions`
"regex" DataFusion functions



## datafusion_functions::register_all

*Function*

Registers all enabled packages with a [`FunctionRegistry`]

```rust
fn register_all(registry: & mut dyn FunctionRegistry) -> datafusion_common::Result<()>
```



## Module: string

"string" DataFusion functions



## Module: strings



## Module: unicode

"unicode" DataFusion functions



## Module: utils



