**datafusion_expr_common > interval_arithmetic**

# Module: interval_arithmetic

## Contents

**Structs**

- [`Interval`](#interval) - The `Interval` type represents a closed interval used for computing

**Enums**

- [`NullableInterval`](#nullableinterval) - An [Interval] that also tracks null status using a boolean interval.

**Functions**

- [`apply_operator`](#apply_operator) - Applies the given binary operator the `lhs` and `rhs` arguments.
- [`cardinality_ratio`](#cardinality_ratio) - This function computes the selectivity of an operation by computing the
- [`satisfy_greater`](#satisfy_greater) - This function updates the given intervals by enforcing (i.e. propagating)

---

## datafusion_expr_common::interval_arithmetic::Interval

*Struct*

The `Interval` type represents a closed interval used for computing
reliable bounds for mathematical expressions.

Conventions:

1. **Closed bounds**: The interval always encompasses its endpoints. We
   accommodate operations resulting in open intervals by incrementing or
   decrementing the interval endpoint value to its successor/predecessor.

2. **Unbounded endpoints**: If the `lower` or `upper` bounds are indeterminate,
   they are labeled as *unbounded*. This is represented using a `NULL`.

3. **Overflow handling**: If the `lower` or `upper` endpoints exceed their
   limits after any operation, they either become unbounded or they are fixed
   to the maximum/minimum value of the datatype, depending on the direction
   of the overflowing endpoint, opting for the safer choice.

4. **Floating-point special cases**:
   - `INF` values are converted to `NULL`s while constructing an interval to
     ensure consistency, with other data types.
   - `NaN` (Not a Number) results are conservatively result in unbounded
     endpoints.

**Methods:**

- `fn try_new(lower: ScalarValue, upper: ScalarValue) -> Result<Self>` - Attempts to create a new `Interval` from the given lower and upper bounds.
- `fn make<T>(lower: Option<T>, upper: Option<T>) -> Result<Self>` - Convenience function to create a new `Interval` from the given (optional)
- `fn make_zero(data_type: &DataType) -> Result<Self>` - Creates a singleton zero interval if the datatype supported.
- `fn make_unbounded(data_type: &DataType) -> Result<Self>` - Creates an unbounded interval from both sides if the datatype supported.
- `fn make_symmetric_unit_interval(data_type: &DataType) -> Result<Self>` - Creates an interval between -1 to 1.
- `fn make_symmetric_pi_interval(data_type: &DataType) -> Result<Self>` - Create an interval from -π to π.
- `fn make_symmetric_half_pi_interval(data_type: &DataType) -> Result<Self>` - Create an interval from -π/2 to π/2.
- `fn make_non_negative_infinity_interval(data_type: &DataType) -> Result<Self>` - Create an interval from 0 to infinity.
- `fn lower(self: &Self) -> &ScalarValue` - Returns a reference to the lower bound.
- `fn upper(self: &Self) -> &ScalarValue` - Returns a reference to the upper bound.
- `fn into_bounds(self: Self) -> (ScalarValue, ScalarValue)` - Converts this `Interval` into its boundary scalar values. It's useful
- `fn data_type(self: &Self) -> DataType` - This function returns the data type of this interval.
- `fn is_unbounded(self: &Self) -> bool` - Checks if the interval is unbounded (on either side).
- `fn cast_to(self: &Self, data_type: &DataType, cast_options: &CastOptions) -> Result<Self>` - Casts this interval to `data_type` using `cast_options`.
- `fn gt<T>(self: &Self, other: T) -> Result<Self>` - Decide if this interval is certainly greater than, possibly greater than,
- `fn gt_eq<T>(self: &Self, other: T) -> Result<Self>` - Decide if this interval is certainly greater than or equal to, possibly
- `fn lt<T>(self: &Self, other: T) -> Result<Self>` - Decide if this interval is certainly less than, possibly less than, or
- `fn lt_eq<T>(self: &Self, other: T) -> Result<Self>` - Decide if this interval is certainly less than or equal to, possibly
- `fn equal<T>(self: &Self, other: T) -> Result<Self>` - Decide if this interval is certainly equal to, possibly equal to, or
- `fn and<T>(self: &Self, other: T) -> Result<Self>` - Compute the logical conjunction of this (boolean) interval with the
- `fn or<T>(self: &Self, other: T) -> Result<Self>` - Compute the logical disjunction of this boolean interval with the
- `fn not(self: &Self) -> Result<Self>` - Compute the logical negation of this (boolean) interval.
- `fn intersect<T>(self: &Self, other: T) -> Result<Option<Self>>` - Compute the intersection of this interval with the given interval.
- `fn union<T>(self: &Self, other: T) -> Result<Self>` - Compute the union of this interval with the given interval.
- `fn contains_value<T>(self: &Self, other: T) -> Result<bool>` - Decide if this interval contains a [`ScalarValue`] (`other`) by returning `true` or `false`.
- `fn contains<T>(self: &Self, other: T) -> Result<Self>` - Decide if this interval is a superset of, overlaps with, or
- `fn is_superset(self: &Self, other: &Interval, strict: bool) -> Result<bool>` - Decide if this interval is a superset of `other`. If argument `strict`
- `fn add<T>(self: &Self, other: T) -> Result<Self>` - Add the given interval (`other`) to this interval. Say we have intervals
- `fn sub<T>(self: &Self, other: T) -> Result<Self>` - Subtract the given interval (`other`) from this interval. Say we have
- `fn mul<T>(self: &Self, other: T) -> Result<Self>` - Multiply the given interval (`other`) with this interval. Say we have
- `fn div<T>(self: &Self, other: T) -> Result<Self>` - Divide this interval by the given interval (`other`). Say we have intervals
- `fn width(self: &Self) -> Result<ScalarValue>` - Computes the width of this interval; i.e. the difference between its
- `fn cardinality(self: &Self) -> Option<u64>` - Returns the cardinality of this interval, which is the number of all
- `fn arithmetic_negate(self: &Self) -> Result<Self>` - Reflects an [`Interval`] around the point zero.

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from(value: &ScalarValue) -> Self`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(value: ScalarValue) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> Interval`
- **PartialEq**
  - `fn eq(self: &Self, other: &Interval) -> bool`



## datafusion_expr_common::interval_arithmetic::NullableInterval

*Enum*

An [Interval] that also tracks null status using a boolean interval.

This represents values that may be in a particular range or be null.

# Examples

```
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::interval_arithmetic::NullableInterval;

// [1, 2) U {NULL}
let maybe_null = NullableInterval::MaybeNull {
    values: Interval::try_new(
        ScalarValue::Int32(Some(1)),
        ScalarValue::Int32(Some(2)),
    )
    .unwrap(),
};

// (0, ∞)
let not_null = NullableInterval::NotNull {
    values: Interval::try_new(ScalarValue::Int32(Some(0)), ScalarValue::Int32(None))
        .unwrap(),
};

// {NULL}
let null_interval = NullableInterval::Null {
    datatype: DataType::Int32,
};

// {4}
let single_value = NullableInterval::from(ScalarValue::Int32(Some(4)));
```

**Variants:**
- `Null{ datatype: arrow::datatypes::DataType }` - The value is always null. This is typed so it can be used in physical
- `MaybeNull{ values: Interval }` - The value may or may not be null. If it is non-null, its is within the
- `NotNull{ values: Interval }` - The value is definitely not null, and is within the specified range.

**Methods:**

- `fn values(self: &Self) -> Option<&Interval>` - Get the values interval, or None if this interval is definitely null.
- `fn data_type(self: &Self) -> DataType` - Get the data type
- `fn is_certainly_true(self: &Self) -> bool` - Return true if the value is definitely true (and not null).
- `fn is_true(self: &Self) -> Result<Self>` - Returns the set of possible values after applying the `is true` test on all
- `fn is_certainly_false(self: &Self) -> bool` - Return true if the value is definitely false (and not null).
- `fn is_false(self: &Self) -> Result<Self>` - Returns the set of possible values after applying the `is false` test on all
- `fn is_certainly_unknown(self: &Self) -> bool` - Return true if the value is definitely null (and not true or false).
- `fn is_unknown(self: &Self) -> Result<Self>` - Returns the set of possible values after applying the `is unknown` test on all
- `fn not(self: &Self) -> Result<Self>` - Returns an interval representing the set of possible values after applying
- `fn and<T>(self: &Self, rhs: T) -> Result<Self>` - Returns an interval representing the set of possible values after applying SQL
- `fn or<T>(self: &Self, rhs: T) -> Result<Self>` - Returns an interval representing the set of possible values after applying SQL three-valued
- `fn apply_operator(self: &Self, op: &Operator, rhs: &Self) -> Result<Self>` - Apply the given operator to this interval and the given interval.
- `fn contains<T>(self: &Self, other: T) -> Result<Self>` - Decide if this interval is a superset of, overlaps with, or
- `fn contains_value<T>(self: &Self, value: T) -> Result<bool>` - Determines if this interval contains a [`ScalarValue`] or not.
- `fn single_value(self: &Self) -> Option<ScalarValue>` - If the interval has collapsed to a single value, return that value.

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(value: ScalarValue) -> Self` - Create an interval that represents a single value.
- **Clone**
  - `fn clone(self: &Self) -> NullableInterval`
- **PartialEq**
  - `fn eq(self: &Self, other: &NullableInterval) -> bool`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`



## datafusion_expr_common::interval_arithmetic::apply_operator

*Function*

Applies the given binary operator the `lhs` and `rhs` arguments.

```rust
fn apply_operator(op: &crate::operator::Operator, lhs: &Interval, rhs: &Interval) -> datafusion_common::Result<Interval>
```



## datafusion_expr_common::interval_arithmetic::cardinality_ratio

*Function*

This function computes the selectivity of an operation by computing the
cardinality ratio of the given input/output intervals. If this can not be
calculated for some reason, it returns `1.0` meaning fully selective (no
filtering).

```rust
fn cardinality_ratio(initial_interval: &Interval, final_interval: &Interval) -> f64
```



## datafusion_expr_common::interval_arithmetic::satisfy_greater

*Function*

This function updates the given intervals by enforcing (i.e. propagating)
the inequality `left > right` (or the `left >= right` inequality, if `strict`
is `true`).

Returns a `Result` wrapping an `Option` containing the tuple of resulting
intervals. If the comparison is infeasible, returns `None`.

Example usage:
```
use datafusion_common::DataFusionError;
use datafusion_expr_common::interval_arithmetic::{satisfy_greater, Interval};

let left = Interval::make(Some(-1000.0_f32), Some(1000.0_f32))?;
let right = Interval::make(Some(500.0_f32), Some(2000.0_f32))?;
let strict = false;
assert_eq!(
    satisfy_greater(&left, &right, strict)?,
    Some((
        Interval::make(Some(500.0_f32), Some(1000.0_f32))?,
        Interval::make(Some(500.0_f32), Some(1000.0_f32))?
    ))
);
Ok::<(), DataFusionError>(())
```

NOTE: This function only works with intervals of the same data type.
      Attempting to compare intervals of different data types will lead
      to an error.

```rust
fn satisfy_greater(left: &Interval, right: &Interval, strict: bool) -> datafusion_common::Result<Option<(Interval, Interval)>>
```



