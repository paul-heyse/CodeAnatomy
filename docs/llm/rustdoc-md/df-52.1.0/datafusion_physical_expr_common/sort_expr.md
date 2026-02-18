**datafusion_physical_expr_common > sort_expr**

# Module: sort_expr

## Contents

**Structs**

- [`LexOrdering`](#lexordering) - This object represents a lexicographical ordering and contains a vector
- [`LexRequirement`](#lexrequirement) - This object represents a lexicographical ordering requirement and contains
- [`PhysicalSortExpr`](#physicalsortexpr) - Represents Sort operation for a column in a RecordBatch
- [`PhysicalSortRequirement`](#physicalsortrequirement) - Represents sort requirement associated with a plan

**Enums**

- [`OrderingRequirements`](#orderingrequirements) - Represents a plan's input ordering requirements. Vector elements represent

**Functions**

- [`format_physical_sort_requirement_list`](#format_physical_sort_requirement_list) - Writes a list of [`PhysicalSortRequirement`]s to a `std::fmt::Formatter`.
- [`is_reversed_sort_options`](#is_reversed_sort_options) - Check if two SortOptions represent reversed orderings.
- [`options_compatible`](#options_compatible) - Returns whether the given two [`SortOptions`] are compatible. Here,

---

## datafusion_physical_expr_common::sort_expr::LexOrdering

*Struct*

This object represents a lexicographical ordering and contains a vector
of `PhysicalSortExpr` objects.

For example, a `vec![a ASC, b DESC]` represents a lexicographical ordering
that first sorts by column `a` in ascending order, then by column `b` in
descending order.

# Invariants

The following always hold true for a `LexOrdering`:

1. It is non-degenerate, meaning it contains at least one element.
2. It is duplicate-free, meaning it does not contain multiple entries for
   the same column.

**Methods:**

- `fn new<impl IntoIterator<Item = PhysicalSortExpr>>(exprs: impl Trait) -> Option<Self>` - Creates a new [`LexOrdering`] from the given vector of sort expressions.
- `fn push(self: & mut Self, sort_expr: PhysicalSortExpr)` - Appends an element to the back of the `LexOrdering`.
- `fn extend<impl IntoIterator<Item = PhysicalSortExpr>>(self: & mut Self, sort_exprs: impl Trait)` - Add all elements from `iter` to the `LexOrdering`.
- `fn first(self: &Self) -> &PhysicalSortExpr` - Returns the leading `PhysicalSortExpr` of the `LexOrdering`. Note that
- `fn capacity(self: &Self) -> usize` - Returns the number of elements that can be stored in the `LexOrdering`
- `fn truncate(self: & mut Self, len: usize) -> bool` - Truncates the `LexOrdering`, keeping only the first `len` elements.
- `fn is_reverse(self: &Self, other: &LexOrdering) -> bool` - Check if reversing this ordering would satisfy another ordering requirement.
- `fn get_sort_options(self: &Self, expr: &dyn PhysicalExpr) -> Option<SortOptions>` - Returns the sort options for the given expression if one is defined in this `LexOrdering`.

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> LexOrdering`
- **From**
  - `fn from(value: [PhysicalSortExpr; N]) -> Self`
- **Deref**
  - `fn deref(self: &Self) -> &<Self as >::Target`
- **From**
  - `fn from(value: LexRequirement) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **IntoIterator**
  - `fn into_iter(self: Self) -> <Self as >::IntoIter`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>` - There is a partial ordering among `LexOrdering` objects. For example, the



## datafusion_physical_expr_common::sort_expr::LexRequirement

*Struct*

This object represents a lexicographical ordering requirement and contains
a vector of `PhysicalSortRequirement` objects.

For example, a `vec![a Some(ASC), b None]` represents a lexicographical
requirement that firsts imposes an ordering by column `a` in ascending
order, then by column `b` in *any* (ascending or descending) order. The
ordering is non-degenerate, meaning it contains at least one element, and
it is duplicate-free, meaning it does not contain multiple entries for the
same column.

Note that a `LexRequirement` need not enforce the uniqueness of its sort
expressions after construction like a `LexOrdering` does, because it provides
no mutation methods. If such methods become necessary, we will need to
enforce uniqueness like the latter object.

**Methods:**

- `fn new<impl IntoIterator<Item = PhysicalSortRequirement>>(reqs: impl Trait) -> Option<Self>` - Creates a new [`LexRequirement`] from the given vector of sort expressions.
- `fn first(self: &Self) -> &PhysicalSortRequirement` - Returns the leading `PhysicalSortRequirement` of the `LexRequirement`.

**Trait Implementations:**

- **From**
  - `fn from(value: [PhysicalSortRequirement; N]) -> Self`
- **From**
  - `fn from(value: LexOrdering) -> Self`
- **IntoIterator**
  - `fn into_iter(self: Self) -> <Self as >::IntoIter`
- **Deref**
  - `fn deref(self: &Self) -> &<Self as >::Target`
- **Clone**
  - `fn clone(self: &Self) -> LexRequirement`
- **PartialEq**
  - `fn eq(self: &Self, other: &LexRequirement) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_common::sort_expr::OrderingRequirements

*Enum*

Represents a plan's input ordering requirements. Vector elements represent
alternative ordering requirements in the order of preference. The list of
alternatives can be either hard or soft, depending on whether the operator
can work without an input ordering.

# Invariants

The following always hold true for a `OrderingRequirements`:

1. It is non-degenerate, meaning it contains at least one ordering. The
   absence of an input ordering requirement is represented by a `None` value
   in `ExecutionPlan` APIs, which return an `Option<OrderingRequirements>`.

**Variants:**
- `Hard(Vec<LexRequirement>)` - The operator is not able to work without one of these requirements.
- `Soft(Vec<LexRequirement>)` - The operator can benefit from these input orderings when available,

**Methods:**

- `fn new_alternatives<impl IntoIterator<Item = LexRequirement>>(alternatives: impl Trait, soft: bool) -> Option<Self>` - Creates a new instance from the given alternatives. If an empty list of
- `fn new(requirement: LexRequirement) -> Self` - Creates a new instance with a single hard requirement.
- `fn new_soft(requirement: LexRequirement) -> Self` - Creates a new instance with a single soft requirement.
- `fn add_alternative(self: & mut Self, requirement: LexRequirement)` - Adds an alternative requirement to the list of alternatives.
- `fn into_single(self: Self) -> LexRequirement` - Returns the first (i.e. most preferred) `LexRequirement` among
- `fn first(self: &Self) -> &LexRequirement` - Returns a reference to the first (i.e. most preferred) `LexRequirement`
- `fn into_alternatives(self: Self) -> (Vec<LexRequirement>, bool)` - Returns all alternatives as a vector of `LexRequirement` objects and a

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> OrderingRequirements`
- **From**
  - `fn from(requirement: LexRequirement) -> Self`
- **PartialEq**
  - `fn eq(self: &Self, other: &OrderingRequirements) -> bool`
- **Deref**
  - `fn deref(self: &Self) -> &<Self as >::Target`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(ordering: LexOrdering) -> Self`
- **DerefMut**
  - `fn deref_mut(self: & mut Self) -> & mut <Self as >::Target`



## datafusion_physical_expr_common::sort_expr::PhysicalSortExpr

*Struct*

Represents Sort operation for a column in a RecordBatch

Example:
```
# use std::any::Any;
# use std::collections::HashMap;
# use std::fmt::{Display, Formatter};
# use std::hash::Hasher;
# use std::sync::Arc;
# use arrow::array::RecordBatch;
# use datafusion_common::Result;
# use arrow::compute::SortOptions;
# use arrow::datatypes::{DataType, Field, FieldRef, Schema};
# use datafusion_expr_common::columnar_value::ColumnarValue;
# use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
# use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
# // this crate doesn't have a physical expression implementation
# // so make a really simple one
# #[derive(Clone, Debug, PartialEq, Eq, Hash)]
# struct MyPhysicalExpr;
# impl PhysicalExpr for MyPhysicalExpr {
#  fn as_any(&self) -> &dyn Any {todo!() }
#  fn data_type(&self, input_schema: &Schema) -> Result<DataType> {todo!()}
#  fn nullable(&self, input_schema: &Schema) -> Result<bool> {todo!() }
#  fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {todo!() }
#  fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> { unimplemented!() }
#  fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {todo!()}
#  fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn PhysicalExpr>> {todo!()}
# fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result { todo!() }
# }
# impl Display for MyPhysicalExpr {
#    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "a") }
# }
# fn col(name: &str) -> Arc<dyn PhysicalExpr> { Arc::new(MyPhysicalExpr) }
// Sort by a ASC
let options = SortOptions::default();
let sort_expr = PhysicalSortExpr::new(col("a"), options);
assert_eq!(sort_expr.to_string(), "a ASC");

// Sort by a DESC NULLS LAST
let sort_expr = PhysicalSortExpr::new_default(col("a"))
  .desc()
  .nulls_last();
assert_eq!(sort_expr.to_string(), "a DESC NULLS LAST");
```

**Fields:**
- `expr: std::sync::Arc<dyn PhysicalExpr>` - Physical expression representing the column to sort
- `options: arrow::compute::kernels::sort::SortOptions` - Option to specify how the given column should be sorted

**Methods:**

- `fn new(expr: Arc<dyn PhysicalExpr>, options: SortOptions) -> Self` - Create a new PhysicalSortExpr
- `fn new_default(expr: Arc<dyn PhysicalExpr>) -> Self` - Create a new PhysicalSortExpr with default [`SortOptions`]
- `fn reverse(self: &Self) -> Self` - Reverses the sort expression. For instance, `[a ASC NULLS LAST]` turns
- `fn asc(self: Self) -> Self` - Set the sort sort options to ASC
- `fn desc(self: Self) -> Self` - Set the sort sort options to DESC
- `fn nulls_first(self: Self) -> Self` - Set the sort sort options to NULLS FIRST
- `fn nulls_last(self: Self) -> Self` - Set the sort sort options to NULLS LAST
- `fn fmt_sql(self: &Self, f: & mut Formatter) -> fmt::Result` - Like [`PhysicalExpr::fmt_sql`] prints a [`PhysicalSortExpr`] in a SQL-like format.
- `fn evaluate_to_sort_column(self: &Self, batch: &RecordBatch) -> Result<SortColumn>` - Evaluates the sort expression into a `SortColumn` that can be passed
- `fn satisfy(self: &Self, requirement: &PhysicalSortRequirement, schema: &Schema) -> bool` - Checks whether this sort expression satisfies the given `requirement`.
- `fn satisfy_expr(self: &Self, sort_expr: &Self, schema: &Schema) -> bool` - Checks whether this sort expression satisfies the given `sort_expr`.

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(value: PhysicalSortRequirement) -> Self` - The default sort options `ASC, NULLS LAST` when the requirement does
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> PhysicalSortExpr`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`



## datafusion_physical_expr_common::sort_expr::PhysicalSortRequirement

*Struct*

Represents sort requirement associated with a plan

If the requirement includes [`SortOptions`] then both the
expression *and* the sort options must match.

If the requirement does not include [`SortOptions`]) then only the
expressions must match.

# Examples

With sort options (`A`, `DESC NULLS FIRST`):
* `ORDER BY A DESC NULLS FIRST` matches
* `ORDER BY A ASC  NULLS FIRST` does not match (`ASC` vs `DESC`)
* `ORDER BY B DESC NULLS FIRST` does not match (different expr)

Without sort options (`A`, None):
* `ORDER BY A DESC NULLS FIRST` matches
* `ORDER BY A ASC  NULLS FIRST` matches (`ASC` and `NULL` options ignored)
* `ORDER BY B DESC NULLS FIRST` does not match  (different expr)

**Fields:**
- `expr: std::sync::Arc<dyn PhysicalExpr>` - Physical expression representing the column to sort
- `options: Option<arrow::compute::kernels::sort::SortOptions>` - Option to specify how the given column should be sorted.

**Methods:**

- `fn new(expr: Arc<dyn PhysicalExpr>, options: Option<SortOptions>) -> Self` - Creates a new requirement.
- `fn compatible(self: &Self, other: &Self) -> bool` - Returns whether this requirement is equal or more specific than `other`.

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> PhysicalSortRequirement`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(value: PhysicalSortExpr) -> Self`



## datafusion_physical_expr_common::sort_expr::format_physical_sort_requirement_list

*Function*

Writes a list of [`PhysicalSortRequirement`]s to a `std::fmt::Formatter`.

Example output: `[a + 1, b]`

```rust
fn format_physical_sort_requirement_list(exprs: &[PhysicalSortRequirement]) -> impl Trait
```



## datafusion_physical_expr_common::sort_expr::is_reversed_sort_options

*Function*

Check if two SortOptions represent reversed orderings.

Returns `true` if both `descending` and `nulls_first` are opposite.

# Example
```
use arrow::compute::SortOptions;
# use datafusion_physical_expr_common::sort_expr::is_reversed_sort_options;

let asc_nulls_last = SortOptions {
    descending: false,
    nulls_first: false,
};
let desc_nulls_first = SortOptions {
    descending: true,
    nulls_first: true,
};

assert!(is_reversed_sort_options(&asc_nulls_last, &desc_nulls_first));
assert!(is_reversed_sort_options(&desc_nulls_first, &asc_nulls_last));
```

```rust
fn is_reversed_sort_options(lhs: &arrow::compute::kernels::sort::SortOptions, rhs: &arrow::compute::kernels::sort::SortOptions) -> bool
```



## datafusion_physical_expr_common::sort_expr::options_compatible

*Function*

Returns whether the given two [`SortOptions`] are compatible. Here,
compatibility means that they are either exactly equal, or they differ only
in whether NULL values come in first/last, which is immaterial because the
column in question is not nullable (specified by the `nullable` parameter).

```rust
fn options_compatible(options_lhs: &arrow::compute::kernels::sort::SortOptions, options_rhs: &arrow::compute::kernels::sort::SortOptions, nullable: bool) -> bool
```



