**datafusion_expr_common > groups_accumulator**

# Module: groups_accumulator

## Contents

**Enums**

- [`EmitTo`](#emitto) - Describes how many rows should be emitted during grouping.

**Traits**

- [`GroupsAccumulator`](#groupsaccumulator) - `GroupsAccumulator` implements a single aggregate (e.g. AVG) and

---

## datafusion_expr_common::groups_accumulator::EmitTo

*Enum*

Describes how many rows should be emitted during grouping.

**Variants:**
- `All` - Emit all groups
- `First(usize)` - Emit only the first `n` groups and shift all existing group

**Methods:**

- `fn take_needed<T>(self: &Self, v: & mut Vec<T>) -> Vec<T>` - Removes the number of rows from `v` required to emit the right

**Traits:** Copy, Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> EmitTo`
- **PartialEq**
  - `fn eq(self: &Self, other: &EmitTo) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr_common::groups_accumulator::GroupsAccumulator

*Trait*

`GroupsAccumulator` implements a single aggregate (e.g. AVG) and
stores the state for *all* groups internally.

Logically, a [`GroupsAccumulator`] stores a mapping from each group index to
the state of the aggregate for that group. For example an implementation for
`min` might look like

```text
   ┌─────┐
   │  0  │───────────▶   100
   ├─────┤
   │  1  │───────────▶   200
   └─────┘
     ...                 ...
   ┌─────┐
   │ N-2 │───────────▶    50
   ├─────┤
   │ N-1 │───────────▶   200
   └─────┘


 Logical group      Current Min
    number          value for that
                    group
```

# Notes on Implementing `GroupsAccumulator`

All aggregates must first implement the simpler [`Accumulator`] trait, which
handles state for a single group. Implementing `GroupsAccumulator` is
optional and is harder to implement than `Accumulator`, but can be much
faster for queries with many group values.  See the [Aggregating Millions of
Groups Fast blog] for more background.

[`NullState`] can help keep the state for groups that have not seen any
values and produce the correct output for those groups.

[`NullState`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/struct.NullState.html

# Details
Each group is assigned a `group_index` by the hash table and each
accumulator manages the specific state, one per `group_index`.

`group_index`es are contiguous (there aren't gaps), and thus it is
expected that each `GroupsAccumulator` will use something like `Vec<..>`
to store the group states.

[`Accumulator`]: crate::accumulator::Accumulator
[Aggregating Millions of Groups Fast blog]: https://arrow.apache.org/blog/2023/08/05/datafusion_fast_grouping/

**Methods:**

- `update_batch`: Updates the accumulator's state from its arguments, encoded as
- `evaluate`: Returns the final aggregate value for each group as a single
- `state`: Returns the intermediate aggregate state for this accumulator,
- `merge_batch`: Merges intermediate state (the output from [`Self::state`])
- `convert_to_state`: Converts an input batch directly to the intermediate aggregate state.
- `supports_convert_to_state`: Returns `true` if [`Self::convert_to_state`] is implemented to support
- `size`: Amount of memory used to store the state of this accumulator,



