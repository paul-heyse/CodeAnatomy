**datafusion_expr_common > sort_properties**

# Module: sort_properties

## Contents

**Structs**

- [`ExprProperties`](#exprproperties) - Represents the properties of a `PhysicalExpr`, including its sorting,

**Enums**

- [`SortProperties`](#sortproperties) - To propagate [`SortOptions`] across the `PhysicalExpr`, it is insufficient

---

## datafusion_expr_common::sort_properties::ExprProperties

*Struct*

Represents the properties of a `PhysicalExpr`, including its sorting,
range, and whether it preserves lexicographical ordering.

**Fields:**
- `sort_properties: SortProperties` - Properties that describe the sorting behavior of the expression,
- `range: crate::interval_arithmetic::Interval` - A closed interval representing the range of possible values for
- `preserves_lex_ordering: bool` - Indicates whether the expression preserves lexicographical ordering

**Methods:**

- `fn new_unknown() -> Self` - Creates a new `ExprProperties` instance with unknown sort properties,
- `fn with_order(self: Self, order: SortProperties) -> Self` - Sets the sorting properties of the expression and returns the modified instance.
- `fn with_range(self: Self, range: Interval) -> Self` - Sets the range of the expression and returns the modified instance.
- `fn with_preserves_lex_ordering(self: Self, preserves_lex_ordering: bool) -> Self` - Sets whether the expression maintains lexicographical ordering and returns the modified instance.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ExprProperties`



## datafusion_expr_common::sort_properties::SortProperties

*Enum*

To propagate [`SortOptions`] across the `PhysicalExpr`, it is insufficient
to simply use `Option<SortOptions>`: There must be a differentiation between
unordered columns and literal values, since literals may not break the ordering
when they are used as a child of some binary expression when the other child has
some ordering. On the other hand, unordered columns cannot maintain ordering when
they take part in such operations.

Example: ((a_ordered + b_unordered) + c_ordered) expression cannot end up with
sorted data; however the ((a_ordered + 999) + c_ordered) expression can. Therefore,
we need two different variants for literals and unordered columns as literals are
often more ordering-friendly under most mathematical operations.

**Variants:**
- `Ordered(arrow::compute::SortOptions)` - Use the ordinary [`SortOptions`] struct to represent ordered data:
- `Unordered`
- `Singleton`

**Methods:**

- `fn add(self: &Self, rhs: &Self) -> Self`
- `fn sub(self: &Self, rhs: &Self) -> Self`
- `fn gt_or_gteq(self: &Self, rhs: &Self) -> Self`
- `fn and_or(self: &Self, rhs: &Self) -> Self`

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> SortProperties`
- **PartialEq**
  - `fn eq(self: &Self, other: &SortProperties) -> bool`
- **Default**
  - `fn default() -> SortProperties`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Neg**
  - `fn neg(self: Self) -> <Self as >::Output`



