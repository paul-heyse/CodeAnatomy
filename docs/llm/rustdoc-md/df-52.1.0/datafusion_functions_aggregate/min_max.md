**datafusion_functions_aggregate > min_max**

# Module: min_max

## Contents

**Structs**

- [`Max`](#max)
- [`Min`](#min)
- [`MovingMax`](#movingmax) - Keep track of the maximum value in a sliding window.
- [`MovingMin`](#movingmin) - Keep track of the minimum value in a sliding window.
- [`SlidingMaxAccumulator`](#slidingmaxaccumulator)
- [`SlidingMinAccumulator`](#slidingminaccumulator)

**Functions**

- [`max`](#max) - Returns the maximum of a group of values.
- [`max_udaf`](#max_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Max`]
- [`min`](#min) - Returns the minimum of a group of values.
- [`min_udaf`](#min_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Min`]

---

## datafusion_functions_aggregate::min_max::Max

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn groups_accumulator_supported(self: &Self, args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn create_sliding_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn is_descending(self: &Self) -> Option<bool>`
  - `fn order_sensitivity(self: &Self) -> datafusion_expr::utils::AggregateOrderSensitivity`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn reverse_expr(self: &Self) -> datafusion_expr::ReversedUDAF`
  - `fn value_from_stats(self: &Self, statistics_args: &StatisticsArgs) -> Option<ScalarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn set_monotonicity(self: &Self, _data_type: &DataType) -> SetMonotonicity`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Max) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::min_max::Min

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn groups_accumulator_supported(self: &Self, args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn create_sliding_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn is_descending(self: &Self) -> Option<bool>`
  - `fn value_from_stats(self: &Self, statistics_args: &StatisticsArgs) -> Option<ScalarValue>`
  - `fn order_sensitivity(self: &Self) -> datafusion_expr::utils::AggregateOrderSensitivity`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn reverse_expr(self: &Self) -> datafusion_expr::ReversedUDAF`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn set_monotonicity(self: &Self, _data_type: &DataType) -> SetMonotonicity`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Min) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::min_max::MovingMax

*Struct*

Keep track of the maximum value in a sliding window.

See [`MovingMin`] for more details.

```
# use datafusion_functions_aggregate::min_max::MovingMax;
let mut moving_max = MovingMax::<i32>::new();
moving_max.push(2);
moving_max.push(3);
moving_max.push(1);

assert_eq!(moving_max.max(), Some(&3));
assert_eq!(moving_max.pop(), Some(2));

assert_eq!(moving_max.max(), Some(&3));
assert_eq!(moving_max.pop(), Some(3));

assert_eq!(moving_max.max(), Some(&1));
assert_eq!(moving_max.pop(), Some(1));

assert_eq!(moving_max.max(), None);
assert_eq!(moving_max.pop(), None);
```

**Generic Parameters:**
- T

**Methods:**

- `fn new() -> Self` - Creates a new `MovingMax` to keep track of the maximum in a sliding window.
- `fn with_capacity(capacity: usize) -> Self` - Creates a new `MovingMax` to keep track of the maximum in a sliding window with
- `fn max(self: &Self) -> Option<&T>` - Returns the maximum of the sliding window or `None` if the window is empty.
- `fn push(self: & mut Self, val: T)` - Pushes a new element into the sliding window.
- `fn pop(self: & mut Self) -> Option<T>` - Removes and returns the last value of the sliding window.
- `fn len(self: &Self) -> usize` - Returns the number of elements stored in the sliding window.
- `fn is_empty(self: &Self) -> bool` - Returns `true` if the moving window contains no elements.

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::min_max::MovingMin

*Struct*

Keep track of the minimum value in a sliding window.

The implementation is taken from <https://github.com/spebern/moving_min_max/blob/master/src/lib.rs>

`moving min max` provides one data structure for keeping track of the
minimum value and one for keeping track of the maximum value in a sliding
window.

Each element is stored with the current min/max. One stack to push and another one for pop. If pop stack is empty,
push to this stack all elements popped from first stack while updating their current min/max. Now pop from
the second stack (MovingMin/Max struct works as a queue). To find the minimum element of the queue,
look at the smallest/largest two elements of the individual stacks, then take the minimum of those two values.

The complexity of the operations are
- O(1) for getting the minimum/maximum
- O(1) for push
- amortized O(1) for pop

```
# use datafusion_functions_aggregate::min_max::MovingMin;
let mut moving_min = MovingMin::<i32>::new();
moving_min.push(2);
moving_min.push(1);
moving_min.push(3);

assert_eq!(moving_min.min(), Some(&1));
assert_eq!(moving_min.pop(), Some(2));

assert_eq!(moving_min.min(), Some(&1));
assert_eq!(moving_min.pop(), Some(1));

assert_eq!(moving_min.min(), Some(&3));
assert_eq!(moving_min.pop(), Some(3));

assert_eq!(moving_min.min(), None);
assert_eq!(moving_min.pop(), None);
```

**Generic Parameters:**
- T

**Methods:**

- `fn new() -> Self` - Creates a new `MovingMin` to keep track of the minimum in a sliding
- `fn with_capacity(capacity: usize) -> Self` - Creates a new `MovingMin` to keep track of the minimum in a sliding
- `fn min(self: &Self) -> Option<&T>` - Returns the minimum of the sliding window or `None` if the window is
- `fn push(self: & mut Self, val: T)` - Pushes a new element into the sliding window.
- `fn pop(self: & mut Self) -> Option<T>` - Removes and returns the last value of the sliding window.
- `fn len(self: &Self) -> usize` - Returns the number of elements stored in the sliding window.
- `fn is_empty(self: &Self) -> bool` - Returns `true` if the moving window contains no elements.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self`



## datafusion_functions_aggregate::min_max::SlidingMaxAccumulator

*Struct*

**Methods:**

- `fn try_new(datatype: &DataType) -> Result<Self>` - new max accumulator

**Trait Implementations:**

- **Accumulator**
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn retract_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn supports_retract_batch(self: &Self) -> bool`
  - `fn size(self: &Self) -> usize`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::min_max::SlidingMinAccumulator

*Struct*

**Methods:**

- `fn try_new(datatype: &DataType) -> Result<Self>`

**Trait Implementations:**

- **Accumulator**
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn retract_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn supports_retract_batch(self: &Self) -> bool`
  - `fn size(self: &Self) -> usize`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::min_max::max

*Function*

Returns the maximum of a group of values.

```rust
fn max(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::min_max::max_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Max`]

```rust
fn max_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::min_max::min

*Function*

Returns the minimum of a group of values.

```rust
fn min(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::min_max::min_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Min`]

```rust
fn min_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



