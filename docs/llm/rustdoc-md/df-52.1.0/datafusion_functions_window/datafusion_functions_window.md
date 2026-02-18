**datafusion_functions_window**

# Module: datafusion_functions_window

## Contents

**Modules**

- [`cume_dist`](#cume_dist) - `cume_dist` window function implementation
- [`expr_fn`](#expr_fn) - Fluent-style API for creating `Expr`s
- [`lead_lag`](#lead_lag) - `lead` and `lag` window function implementations
- [`macros`](#macros) - Convenience macros for defining a user-defined window function
- [`nth_value`](#nth_value) - `nth_value` window function implementation
- [`ntile`](#ntile) - `ntile` window function implementation
- [`planner`](#planner) - SQL planning extensions like [`WindowFunctionPlanner`]
- [`rank`](#rank) - Implementation of `rank`, `dense_rank`, and `percent_rank` window functions,
- [`row_number`](#row_number) - `row_number` window function implementation

**Macros**

- [`create_udwf_expr`](#create_udwf_expr) - Create a [`WindowFunction`] expression that exposes a fluent API
- [`define_udwf_and_expr`](#define_udwf_and_expr) - Defines a user-defined window function.
- [`get_or_init_udwf`](#get_or_init_udwf) - Lazily initializes a user-defined window function exactly once

**Functions**

- [`all_default_window_functions`](#all_default_window_functions) - Returns all default window functions
- [`register_all`](#register_all) - Registers all enabled packages with a [`FunctionRegistry`]

---

## datafusion_functions_window::all_default_window_functions

*Function*

Returns all default window functions

```rust
fn all_default_window_functions() -> Vec<std::sync::Arc<datafusion_expr::WindowUDF>>
```



## datafusion_functions_window::create_udwf_expr

*Declarative Macro*

Create a [`WindowFunction`] expression that exposes a fluent API
which you can use to build more complex expressions.

[`WindowFunction`]: datafusion_expr::Expr::WindowFunction

# Parameters

* `$UDWF`: The struct which defines the [`Signature`] of the
  user-defined window function.
* `$OUT_FN_NAME`: The basename to generate a unique function name like
  `$OUT_FN_NAME_udwf`.
* `$DOC`: Doc comments for UDWF.
* (optional) `[$($PARAM:ident),+]`: An array of 1 or more parameters
  for the generated function. The type of parameters is [`Expr`].
  When omitted this creates a function with zero parameters.

[`Signature`]: datafusion_expr::Signature
[`Expr`]: datafusion_expr::Expr

# Example

1. With Zero Parameters
```
# use std::any::Any;
use arrow::datatypes::FieldRef;
# use datafusion_common::arrow::datatypes::{DataType, Field};
# use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
# use datafusion_functions_window::{create_udwf_expr, get_or_init_udwf};
# use datafusion_functions_window_common::field::WindowUDFFieldArgs;
# use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;

# get_or_init_udwf!(
#     RowNumber,
#     row_number,
#     "Returns a unique row number for each row in window partition beginning at 1."
# );
/// Creates `row_number()` API which has zero parameters:
///
///     ```
///     /// Returns a unique row number for each row in window partition
///     /// beginning at 1.
///     pub fn row_number() -> datafusion_expr::Expr {
///        row_number_udwf().call(vec![])
///     }
///     ```
create_udwf_expr!(
    RowNumber,
    row_number,
    "Returns a unique row number for each row in window partition beginning at 1."
);
#
# assert_eq!(
#     row_number().name_for_alias().unwrap(),
#     "row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
# );
#
# #[derive(Debug, PartialEq, Eq, Hash)]
# struct RowNumber {
#     signature: Signature,
# }
# impl Default for RowNumber {
#     fn default() -> Self {
#         Self {
#             signature: Signature::any(0, Volatility::Immutable),
#         }
#     }
# }
# impl WindowUDFImpl for RowNumber {
#     fn as_any(&self) -> &dyn Any {
#         self
#     }
#     fn name(&self) -> &str {
#         "row_number"
#     }
#     fn signature(&self) -> &Signature {
#         &self.signature
#     }
#     fn partition_evaluator(
#         &self,
#         _partition_evaluator_args: PartitionEvaluatorArgs,
#     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
#         unimplemented!()
#     }
#     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<FieldRef> {
#         Ok(Field::new(field_args.name(), DataType::UInt64, false).into())
#     }
# }
```

2. With Multiple Parameters
```
# use std::any::Any;
use arrow::datatypes::FieldRef;
#
# use datafusion_expr::{
#     PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
# };
#
# use datafusion_functions_window::{create_udwf_expr, get_or_init_udwf};
# use datafusion_functions_window_common::field::WindowUDFFieldArgs;
#
# use datafusion_common::arrow::datatypes::Field;
# use datafusion_common::ScalarValue;
# use datafusion_expr::{col, lit};
# use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
#
# get_or_init_udwf!(Lead, lead, "user-defined window function");
#
/// Creates `lead(expr, offset, default)` with 3 parameters:
///
///     ```
///     /// Returns a value evaluated at the row that is offset rows
///     /// after the current row within the partition.
///     pub fn lead(
///         expr: datafusion_expr::Expr,
///         offset: datafusion_expr::Expr,
///         default: datafusion_expr::Expr,
///     ) -> datafusion_expr::Expr {
///         lead_udwf().call(vec![expr, offset, default])
///     }
///     ```
create_udwf_expr!(
    Lead,
    lead,
    [expr, offset, default],
    "Returns a value evaluated at the row that is offset rows after the current row within the partition."
);
#
# assert_eq!(
#     lead(col("a"), lit(1i64), lit(ScalarValue::Null))
#         .name_for_alias()
#         .unwrap(),
#     "lead(a,Int64(1),NULL) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
# );
#
# #[derive(Debug, PartialEq, Eq, Hash)]
# struct Lead {
#     signature: Signature,
# }
#
# impl Default for Lead {
#     fn default() -> Self {
#         Self {
#             signature: Signature::one_of(
#                 vec![
#                     TypeSignature::Any(1),
#                     TypeSignature::Any(2),
#                     TypeSignature::Any(3),
#                 ],
#                 Volatility::Immutable,
#             ),
#         }
#     }
# }
#
# impl WindowUDFImpl for Lead {
#     fn as_any(&self) -> &dyn Any {
#         self
#     }
#     fn name(&self) -> &str {
#         "lead"
#     }
#     fn signature(&self) -> &Signature {
#         &self.signature
#     }
#     fn partition_evaluator(
#         &self,
#         partition_evaluator_args: PartitionEvaluatorArgs,
#     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
#         unimplemented!()
#     }
#     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<FieldRef> {
#         Ok(Field::new(
#             field_args.name(),
#             field_args.get_input_field(0).unwrap().data_type().clone(),
#             false,
#         ).into())
#     }
# }
```

```rust
macro_rules! create_udwf_expr {
    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr) => { ... };
    ($UDWF:ident, $OUT_FN_NAME:ident, [$($PARAM:ident),+], $DOC:expr) => { ... };
}
```



## Module: cume_dist

`cume_dist` window function implementation



## datafusion_functions_window::define_udwf_and_expr

*Declarative Macro*

Defines a user-defined window function.

Combines [`get_or_init_udwf!`] and [`create_udwf_expr!`] into a
single macro for convenience.

# Arguments

* `$UDWF`: The struct which defines the [`Signature`] of the
  user-defined window function.
* `$OUT_FN_NAME`: The basename to generate a unique function name like
  `$OUT_FN_NAME_udwf`.
* (optional) `[$($PARAM:ident),+]`: An array of 1 or more parameters
  for the generated function. The type of parameters is [`Expr`].
  When omitted this creates a function with zero parameters.
* `$DOC`: Doc comments for UDWF.
* (optional) `$CTOR`: Pass a custom constructor. When omitted it
  automatically resolves to `$UDWF::default()`.

[`Signature`]: datafusion_expr::Signature
[`Expr`]: datafusion_expr::Expr

# Usage

## Expression API With Zero parameters
1. Uses default constructor for UDWF.

```
# use std::any::Any;
use arrow::datatypes::FieldRef;
# use datafusion_common::arrow::datatypes::{DataType, Field};
# use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
#
# use datafusion_functions_window_common::field::WindowUDFFieldArgs;
# use datafusion_functions_window::{define_udwf_and_expr, get_or_init_udwf, create_udwf_expr};
# use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
#
/// 1. Defines the `simple_udwf()` user-defined window function.
///
/// 2. Defines the expression API:
///     ```
///     pub fn simple() -> datafusion_expr::Expr {
///         simple_udwf().call(vec![])
///     }
///     ```
define_udwf_and_expr!(
    SimpleUDWF,
    simple,
    "a simple user-defined window function"
);
#
# assert_eq!(simple_udwf().name(), "simple_user_defined_window_function");
#
#  #[derive(Debug, PartialEq, Eq, Hash)]
#  struct SimpleUDWF {
#      signature: Signature,
#  }
#
#  impl Default for SimpleUDWF {
#      fn default() -> Self {
#          Self {
#             signature: Signature::any(0, Volatility::Immutable),
#          }
#      }
#  }
#
#  impl WindowUDFImpl for SimpleUDWF {
#      fn as_any(&self) -> &dyn Any {
#          self
#      }
#      fn name(&self) -> &str {
#          "simple_user_defined_window_function"
#      }
#      fn signature(&self) -> &Signature {
#          &self.signature
#      }
#      fn partition_evaluator(
#          &self,
#          partition_evaluator_args: PartitionEvaluatorArgs,
#      ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
#          unimplemented!()
#      }
#      fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<FieldRef> {
#          Ok(Field::new(field_args.name(), DataType::Int64, false).into())
#      }
#  }
#
```

2. Uses a custom constructor for UDWF.

```
# use std::any::Any;
use arrow::datatypes::FieldRef;
# use datafusion_common::arrow::datatypes::{DataType, Field};
# use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
# use datafusion_functions_window::{create_udwf_expr, define_udwf_and_expr, get_or_init_udwf};
# use datafusion_functions_window_common::field::WindowUDFFieldArgs;
# use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
#
/// 1. Defines the `row_number_udwf()` user-defined window function.
///
/// 2. Defines the expression API:
///     ```
///     pub fn row_number() -> datafusion_expr::Expr {
///         row_number_udwf().call(vec![])
///     }
///     ```
define_udwf_and_expr!(
    RowNumber,
    row_number,
    "Returns a unique row number for each row in window partition beginning at 1.",
    RowNumber::new // <-- custom constructor
);
#
# assert_eq!(
#     row_number().name_for_alias().unwrap(),
#     "row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
# );
#
# #[derive(Debug, PartialEq, Eq, Hash)]
# struct RowNumber {
#     signature: Signature,
# }
# impl RowNumber {
#     fn new() -> Self {
#         Self {
#             signature: Signature::any(0, Volatility::Immutable),
#         }
#     }
# }
# impl WindowUDFImpl for RowNumber {
#     fn as_any(&self) -> &dyn Any {
#         self
#     }
#     fn name(&self) -> &str {
#         "row_number"
#     }
#     fn signature(&self) -> &Signature {
#         &self.signature
#     }
#     fn partition_evaluator(
#         &self,
#         _partition_evaluator_args: PartitionEvaluatorArgs,
#     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
#         unimplemented!()
#     }
#     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<FieldRef> {
#         Ok(Field::new(field_args.name(), DataType::UInt64, false).into())
#     }
# }
```

## Expression API With Multiple Parameters
3. Uses default constructor for UDWF

```
# use std::any::Any;
use arrow::datatypes::FieldRef;
#
# use datafusion_expr::{
#     PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
# };
#
# use datafusion_functions_window::{create_udwf_expr, define_udwf_and_expr, get_or_init_udwf};
# use datafusion_functions_window_common::field::WindowUDFFieldArgs;
#
# use datafusion_common::arrow::datatypes::Field;
# use datafusion_common::ScalarValue;
# use datafusion_expr::{col, lit};
# use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
#
/// 1. Defines the `lead_udwf()` user-defined window function.
///
/// 2. Defines the expression API:
///     ```
///     pub fn lead(
///         expr: datafusion_expr::Expr,
///         offset: datafusion_expr::Expr,
///         default: datafusion_expr::Expr,
///     ) -> datafusion_expr::Expr {
///         lead_udwf().call(vec![expr, offset, default])
///     }
///     ```
define_udwf_and_expr!(
    Lead,
    lead,
    [expr, offset, default],        // <- 3 parameters
    "user-defined window function"
);
#
# assert_eq!(
#     lead(col("a"), lit(1i64), lit(ScalarValue::Null))
#         .name_for_alias()
#         .unwrap(),
#     "lead(a,Int64(1),NULL) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
# );
#
# #[derive(Debug, PartialEq, Eq, Hash)]
# struct Lead {
#     signature: Signature,
# }
#
# impl Default for Lead {
#     fn default() -> Self {
#         Self {
#             signature: Signature::one_of(
#                 vec![
#                     TypeSignature::Any(1),
#                     TypeSignature::Any(2),
#                     TypeSignature::Any(3),
#                 ],
#                 Volatility::Immutable,
#             ),
#         }
#     }
# }
#
# impl WindowUDFImpl for Lead {
#     fn as_any(&self) -> &dyn Any {
#         self
#     }
#     fn name(&self) -> &str {
#         "lead"
#     }
#     fn signature(&self) -> &Signature {
#         &self.signature
#     }
#     fn partition_evaluator(
#         &self,
#         _partition_evaluator_args: PartitionEvaluatorArgs,
#     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
#         unimplemented!()
#     }
#     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<FieldRef> {
#         Ok(Field::new(
#             field_args.name(),
#             field_args.get_input_field(0).unwrap().data_type().clone(),
#             false,
#         ).into())
#     }
# }
```
4. Uses custom constructor for UDWF

```
# use std::any::Any;
use arrow::datatypes::FieldRef;
#
# use datafusion_expr::{
#     PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
# };
#
# use datafusion_functions_window::{create_udwf_expr, define_udwf_and_expr, get_or_init_udwf};
# use datafusion_functions_window_common::field::WindowUDFFieldArgs;
#
# use datafusion_common::arrow::datatypes::Field;
# use datafusion_common::ScalarValue;
# use datafusion_expr::{col, lit};
# use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
#
/// 1. Defines the `lead_udwf()` user-defined window function.
///
/// 2. Defines the expression API:
///     ```
///     pub fn lead(
///         expr: datafusion_expr::Expr,
///         offset: datafusion_expr::Expr,
///         default: datafusion_expr::Expr,
///     ) -> datafusion_expr::Expr {
///         lead_udwf().call(vec![expr, offset, default])
///     }
///     ```
define_udwf_and_expr!(
    Lead,
    lead,
    [expr, offset, default],        // <- 3 parameters
    "user-defined window function",
    Lead::new                       // <- Custom constructor
);
#
# assert_eq!(
#     lead(col("a"), lit(1i64), lit(ScalarValue::Null))
#         .name_for_alias()
#         .unwrap(),
#     "lead(a,Int64(1),NULL) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
# );
#
# #[derive(Debug, PartialEq, Eq, Hash)]
# struct Lead {
#     signature: Signature,
# }
#
# impl Lead {
#     fn new() -> Self {
#         Self {
#             signature: Signature::one_of(
#                 vec![
#                     TypeSignature::Any(1),
#                     TypeSignature::Any(2),
#                     TypeSignature::Any(3),
#                 ],
#                 Volatility::Immutable,
#             ),
#         }
#     }
# }
#
# impl WindowUDFImpl for Lead {
#     fn as_any(&self) -> &dyn Any {
#         self
#     }
#     fn name(&self) -> &str {
#         "lead"
#     }
#     fn signature(&self) -> &Signature {
#         &self.signature
#     }
#     fn partition_evaluator(
#         &self,
#         _partition_evaluator_args: PartitionEvaluatorArgs,
#     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
#         unimplemented!()
#     }
#     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<FieldRef> {
#         Ok(Field::new(
#             field_args.name(),
#             field_args.get_input_field(0).unwrap().data_type().clone(),
#             false,
#         ).into())
#     }
# }
```

```rust
macro_rules! define_udwf_and_expr {
    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr) => { ... };
    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr, $CTOR:path) => { ... };
    ($UDWF:ident, $OUT_FN_NAME:ident, [$($PARAM:ident),+], $DOC:expr) => { ... };
    ($UDWF:ident, $OUT_FN_NAME:ident, [$($PARAM:ident),+], $DOC:expr, $CTOR:path) => { ... };
}
```



## Module: expr_fn

Fluent-style API for creating `Expr`s



## datafusion_functions_window::get_or_init_udwf

*Declarative Macro*

Lazily initializes a user-defined window function exactly once
when called concurrently. Repeated calls return a reference to the
same instance.

# Parameters

* `$UDWF`: The struct which defines the [`Signature`](datafusion_expr::Signature)
  of the user-defined window function.
* `$OUT_FN_NAME`: The basename to generate a unique function name like
  `$OUT_FN_NAME_udwf`.
* `$DOC`: Doc comments for UDWF.
* (optional) `$CTOR`: Pass a custom constructor. When omitted it
  automatically resolves to `$UDWF::default()`.

# Example

```
# use std::any::Any;
use arrow::datatypes::FieldRef;
# use datafusion_common::arrow::datatypes::{DataType, Field};
# use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
#
# use datafusion_functions_window_common::field::WindowUDFFieldArgs;
# use datafusion_functions_window::get_or_init_udwf;
# use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
#
/// Defines the `simple_udwf()` user-defined window function.
get_or_init_udwf!(
    SimpleUDWF,
    simple,
    "Simple user-defined window function doc comment."
);
#
# assert_eq!(simple_udwf().name(), "simple_user_defined_window_function");
#
#  #[derive(Debug, PartialEq, Eq, Hash)]
#  struct SimpleUDWF {
#      signature: Signature,
#  }
#
#  impl Default for SimpleUDWF {
#      fn default() -> Self {
#          Self {
#             signature: Signature::any(0, Volatility::Immutable),
#          }
#      }
#  }
#
#  impl WindowUDFImpl for SimpleUDWF {
#      fn as_any(&self) -> &dyn Any {
#          self
#      }
#      fn name(&self) -> &str {
#          "simple_user_defined_window_function"
#      }
#      fn signature(&self) -> &Signature {
#          &self.signature
#      }
#      fn partition_evaluator(
#          &self,
#         _partition_evaluator_args: PartitionEvaluatorArgs,
#      ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
#          unimplemented!()
#      }
#      fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<FieldRef> {
#          Ok(Field::new(field_args.name(), DataType::Int64, false).into())
#      }
#  }
#
```

```rust
macro_rules! get_or_init_udwf {
    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr) => { ... };
    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr, $CTOR:path) => { ... };
}
```



## Module: lead_lag

`lead` and `lag` window function implementations



## Module: macros

Convenience macros for defining a user-defined window function
and associated expression API (fluent style).

See [`define_udwf_and_expr!`] for usage examples.

[`define_udwf_and_expr!`]: crate::define_udwf_and_expr!



## Module: nth_value

`nth_value` window function implementation



## Module: ntile

`ntile` window function implementation



## Module: planner

SQL planning extensions like [`WindowFunctionPlanner`]



## Module: rank

Implementation of `rank`, `dense_rank`, and `percent_rank` window functions,
which can be evaluated at runtime during query execution.



## datafusion_functions_window::register_all

*Function*

Registers all enabled packages with a [`FunctionRegistry`]

```rust
fn register_all(registry: & mut dyn FunctionRegistry) -> datafusion_common::Result<()>
```



## Module: row_number

`row_number` window function implementation



