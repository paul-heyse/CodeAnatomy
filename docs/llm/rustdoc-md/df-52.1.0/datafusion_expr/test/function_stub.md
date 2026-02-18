**datafusion_expr > test > function_stub**

# Module: test::function_stub

## Contents

**Structs**

- [`Avg`](#avg) - Testing stub implementation of avg aggregate
- [`Count`](#count) - Testing stub implementation of COUNT aggregate
- [`Max`](#max) - Testing stub implementation of MAX aggregate
- [`Min`](#min) - Testing stub implementation of Min aggregate
- [`Sum`](#sum) - Stub `sum` used for optimizer testing

**Functions**

- [`avg`](#avg)
- [`avg_udaf`](#avg_udaf) - AggregateFunction that returns a [AggregateUDF](crate::AggregateUDF) for [`Avg`]
- [`count`](#count)
- [`count_udaf`](#count_udaf) - AggregateFunction that returns a [AggregateUDF](crate::AggregateUDF) for [`Count`]
- [`max`](#max)
- [`max_udaf`](#max_udaf) - AggregateFunction that returns a [AggregateUDF](crate::AggregateUDF) for [`Max`]
- [`min`](#min)
- [`min_udaf`](#min_udaf) - AggregateFunction that returns a [AggregateUDF](crate::AggregateUDF) for [`Min`]
- [`sum`](#sum)
- [`sum_udaf`](#sum_udaf) - AggregateFunction that returns a [AggregateUDF](crate::AggregateUDF) for [`Sum`]

---

## datafusion_expr::test::function_stub::Avg

*Struct*

Testing stub implementation of avg aggregate

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Avg) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn aliases(self: &Self) -> &[String]`
- **Default**
  - `fn default() -> Self`



## datafusion_expr::test::function_stub::Count

*Struct*

Testing stub implementation of COUNT aggregate

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn is_nullable(self: &Self) -> bool`
  - `fn state_fields(self: &Self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn create_groups_accumulator(self: &Self, _args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Default**
  - `fn default() -> Self`
- **PartialEq**
  - `fn eq(self: &Self, other: &Count) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::test::function_stub::Max

*Struct*

Testing stub implementation of MAX aggregate

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn create_groups_accumulator(self: &Self, _args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
  - `fn is_descending(self: &Self) -> Option<bool>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Default**
  - `fn default() -> Self`
- **PartialEq**
  - `fn eq(self: &Self, other: &Max) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::test::function_stub::Min

*Struct*

Testing stub implementation of Min aggregate

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn create_groups_accumulator(self: &Self, _args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
  - `fn is_descending(self: &Self) -> Option<bool>`
- **Default**
  - `fn default() -> Self`
- **PartialEq**
  - `fn eq(self: &Self, other: &Min) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::test::function_stub::Sum

*Struct*

Stub `sum` used for optimizer testing

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Sum) -> bool`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn groups_accumulator_supported(self: &Self, _args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, _args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
  - `fn order_sensitivity(self: &Self) -> AggregateOrderSensitivity`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::test::function_stub::avg

*Function*

```rust
fn avg(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::test::function_stub::avg_udaf

*Function*

AggregateFunction that returns a [AggregateUDF](crate::AggregateUDF) for [`Avg`]

```rust
fn avg_udaf() -> std::sync::Arc<crate::AggregateUDF>
```



## datafusion_expr::test::function_stub::count

*Function*

```rust
fn count(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::test::function_stub::count_udaf

*Function*

AggregateFunction that returns a [AggregateUDF](crate::AggregateUDF) for [`Count`]

```rust
fn count_udaf() -> std::sync::Arc<crate::AggregateUDF>
```



## datafusion_expr::test::function_stub::max

*Function*

```rust
fn max(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::test::function_stub::max_udaf

*Function*

AggregateFunction that returns a [AggregateUDF](crate::AggregateUDF) for [`Max`]

```rust
fn max_udaf() -> std::sync::Arc<crate::AggregateUDF>
```



## datafusion_expr::test::function_stub::min

*Function*

```rust
fn min(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::test::function_stub::min_udaf

*Function*

AggregateFunction that returns a [AggregateUDF](crate::AggregateUDF) for [`Min`]

```rust
fn min_udaf() -> std::sync::Arc<crate::AggregateUDF>
```



## datafusion_expr::test::function_stub::sum

*Function*

```rust
fn sum(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::test::function_stub::sum_udaf

*Function*

AggregateFunction that returns a [AggregateUDF](crate::AggregateUDF) for [`Sum`]

```rust
fn sum_udaf() -> std::sync::Arc<crate::AggregateUDF>
```



