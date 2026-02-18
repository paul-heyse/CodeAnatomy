**datafusion_expr > window_state**

# Module: window_state

## Contents

**Structs**

- [`PartitionBatchState`](#partitionbatchstate) - State for each unique partition determined according to PARTITION BY column(s)
- [`WindowAggState`](#windowaggstate) - Holds the state of evaluating a window function
- [`WindowFrameStateGroups`](#windowframestategroups) - This structure encapsulates all the state information we require as we
- [`WindowFrameStateRange`](#windowframestaterange) - This structure encapsulates all the state information we require as we scan

**Enums**

- [`WindowFrameContext`](#windowframecontext) - This object stores the window frame state for use in incremental calculations.

---

## datafusion_expr::window_state::PartitionBatchState

*Struct*

State for each unique partition determined according to PARTITION BY column(s)

**Fields:**
- `record_batch: arrow::record_batch::RecordBatch` - The record batch belonging to current partition
- `most_recent_row: Option<arrow::record_batch::RecordBatch>` - The record batch that contains the most recent row at the input.
- `is_end: bool` - Flag indicating whether we have received all data for this partition
- `n_out_row: usize` - Number of rows emitted for each partition

**Methods:**

- `fn new(schema: SchemaRef) -> Self`
- `fn new_with_batch(batch: RecordBatch) -> Self`
- `fn extend(self: & mut Self, batch: &RecordBatch) -> Result<()>`
- `fn set_most_recent_row(self: & mut Self, batch: RecordBatch)`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> PartitionBatchState`
- **PartialEq**
  - `fn eq(self: &Self, other: &PartitionBatchState) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::window_state::WindowAggState

*Struct*

Holds the state of evaluating a window function

**Fields:**
- `window_frame_range: std::ops::Range<usize>` - The range that we calculate the window function
- `window_frame_ctx: Option<WindowFrameContext>`
- `last_calculated_index: usize` - The index of the last row that its result is calculated inside the partition record batch buffer.
- `offset_pruned_rows: usize` - The offset of the deleted row number
- `out_col: arrow::array::ArrayRef` - Stores the results calculated by window frame
- `n_row_result_missing: usize` - Keeps track of how many rows should be generated to be in sync with input record_batch.
- `is_end: bool` - Flag indicating whether we have received all data for this partition

**Methods:**

- `fn prune_state(self: & mut Self, n_prune: usize)`
- `fn update(self: & mut Self, out_col: &ArrayRef, partition_batch_state: &PartitionBatchState) -> Result<()>`
- `fn new(out_type: &DataType) -> Result<Self>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> WindowAggState`



## datafusion_expr::window_state::WindowFrameContext

*Enum*

This object stores the window frame state for use in incremental calculations.

**Variants:**
- `Rows(std::sync::Arc<crate::WindowFrame>)` - ROWS frames are inherently stateless.
- `Range{ window_frame: std::sync::Arc<crate::WindowFrame>, state: WindowFrameStateRange }` - RANGE frames are stateful, they store indices specifying where the
- `Groups{ window_frame: std::sync::Arc<crate::WindowFrame>, state: WindowFrameStateGroups }` - GROUPS frames are stateful, they store group boundaries and indices

**Methods:**

- `fn new(window_frame: Arc<WindowFrame>, sort_options: Vec<SortOptions>) -> Self` - Create a new state object for the given window frame.
- `fn calculate_range(self: & mut Self, range_columns: &[ArrayRef], last_range: &Range<usize>, length: usize, idx: usize) -> Result<Range<usize>>` - This function calculates beginning/ending indices for the frame of the current row.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> WindowFrameContext`



## datafusion_expr::window_state::WindowFrameStateGroups

*Struct*

This structure encapsulates all the state information we require as we
scan groups of data while processing window frames.

**Fields:**
- `group_end_indices: std::collections::VecDeque<(Vec<datafusion_common::ScalarValue>, usize)>` - A tuple containing group values and the row index where the group ends.
- `current_group_idx: usize` - The group index to which the row index belongs.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> WindowFrameStateGroups`
- **Clone**
  - `fn clone(self: &Self) -> WindowFrameStateGroups`



## datafusion_expr::window_state::WindowFrameStateRange

*Struct*

This structure encapsulates all the state information we require as we scan
ranges of data while processing RANGE frames.
Attribute `sort_options` stores the column ordering specified by the ORDER
BY clause. This information is used to calculate the range.

**Trait Implementations:**

- **Default**
  - `fn default() -> WindowFrameStateRange`
- **Clone**
  - `fn clone(self: &Self) -> WindowFrameStateRange`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



