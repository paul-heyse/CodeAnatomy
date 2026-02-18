**datafusion_expr_common > accumulator**

# Module: accumulator

## Contents

**Traits**

- [`Accumulator`](#accumulator) - Tracks an aggregate function's state.

---

## datafusion_expr_common::accumulator::Accumulator

*Trait*

Tracks an aggregate function's state.

`Accumulator`s are stateful objects that implement a single group. They
aggregate values from multiple rows together into a final output aggregate.

[`GroupsAccumulator]` is an additional more performant (but also complex) API
that manages state for multiple groups at once.

An accumulator knows how to:
* update its state from inputs via [`update_batch`]

* compute the final value from its internal state via [`evaluate`]

* retract an update to its state from given inputs via
  [`retract_batch`] (when used as a window aggregate [window
  function])

* convert its internal state to a vector of aggregate values via
  [`state`] and combine the state from multiple accumulators
  via [`merge_batch`], as part of efficient multi-phase grouping.

[`update_batch`]: Self::update_batch
[`retract_batch`]: Self::retract_batch
[`state`]: Self::state
[`evaluate`]: Self::evaluate
[`merge_batch`]: Self::merge_batch
[window function]: https://en.wikipedia.org/wiki/Window_function_(SQL)

**Methods:**

- `update_batch`: Updates the accumulator's state from its input.
- `evaluate`: Returns the final aggregate value, consuming the internal state.
- `size`: Returns the allocated size required for this accumulator, in
- `state`: Returns the intermediate state of the accumulator, consuming the
- `merge_batch`: Updates the accumulator's state from an `Array` containing one
- `retract_batch`: Retracts (removed) an update (caused by the given inputs) to
- `supports_retract_batch`: Does the accumulator support incrementally updating its value



