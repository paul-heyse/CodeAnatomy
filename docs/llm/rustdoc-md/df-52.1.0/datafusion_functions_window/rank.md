**datafusion_functions_window > rank**

# Module: rank

## Contents

**Structs**

- [`Rank`](#rank) - Rank calculates the rank in the window function with order by
- [`RankState`](#rankstate) - State for the RANK(rank) built-in window function.

**Enums**

- [`RankType`](#ranktype)

**Functions**

- [`dense_rank`](#dense_rank) - Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
- [`dense_rank_udwf`](#dense_rank_udwf) - Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`dense_rank`].
- [`percent_rank`](#percent_rank) - Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
- [`percent_rank_udwf`](#percent_rank_udwf) - Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`percent_rank`].
- [`rank`](#rank) - Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
- [`rank_udwf`](#rank_udwf) - Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`rank`].

---

## datafusion_functions_window::rank::Rank

*Struct*

Rank calculates the rank in the window function with order by

**Methods:**

- `fn new(name: String, rank_type: RankType) -> Self` - Create a new `rank` function with the specified name and rank type
- `fn basic() -> Self` - Create a `rank` window function
- `fn dense_rank() -> Self` - Create a `dense_rank` window function
- `fn percent_rank() -> Self` - Create a `percent_rank` window function

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Rank) -> bool`
- **WindowUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn partition_evaluator(self: &Self, _partition_evaluator_args: PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>>`
  - `fn field(self: &Self, field_args: WindowUDFFieldArgs) -> Result<FieldRef>`
  - `fn sort_options(self: &Self) -> Option<SortOptions>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn limit_effect(self: &Self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_window::rank::RankState

*Struct*

State for the RANK(rank) built-in window function.

**Fields:**
- `last_rank_data: Option<Vec<datafusion_common::ScalarValue>>` - The last values for rank as these values change, we increase n_rank
- `last_rank_boundary: usize` - The index where last_rank_boundary is started
- `current_group_count: usize` - Keep the number of entries in current rank
- `n_rank: usize` - Rank number kept from the start

**Trait Implementations:**

- **Default**
  - `fn default() -> RankState`
- **Clone**
  - `fn clone(self: &Self) -> RankState`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_window::rank::RankType

*Enum*

**Variants:**
- `Basic`
- `Dense`
- `Percent`

**Traits:** Eq, Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> RankType`
- **PartialEq**
  - `fn eq(self: &Self, other: &RankType) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_window::rank::dense_rank

*Function*

Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
`DenseRank` user-defined window function.

Returns rank of the current row without gaps. This function counts peer groups

```rust
fn dense_rank() -> datafusion_expr::Expr
```



## datafusion_functions_window::rank::dense_rank_udwf

*Function*

Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`dense_rank`].

Returns rank of the current row without gaps. This function counts peer groups

```rust
fn dense_rank_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF>
```



## datafusion_functions_window::rank::percent_rank

*Function*

Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
`PercentRank` user-defined window function.

Returns the relative rank of the current row: (rank - 1) / (total rows - 1)

```rust
fn percent_rank() -> datafusion_expr::Expr
```



## datafusion_functions_window::rank::percent_rank_udwf

*Function*

Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`percent_rank`].

Returns the relative rank of the current row: (rank - 1) / (total rows - 1)

```rust
fn percent_rank_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF>
```



## datafusion_functions_window::rank::rank

*Function*

Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
`Rank` user-defined window function.

Returns rank of the current row with gaps. Same as `row_number` of its first peer

```rust
fn rank() -> datafusion_expr::Expr
```



## datafusion_functions_window::rank::rank_udwf

*Function*

Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`rank`].

Returns rank of the current row with gaps. Same as `row_number` of its first peer

```rust
fn rank_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF>
```



