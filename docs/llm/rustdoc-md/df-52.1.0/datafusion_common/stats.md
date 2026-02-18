**datafusion_common > stats**

# Module: stats

## Contents

**Structs**

- [`ColumnStatistics`](#columnstatistics) - Statistics for a column within a relation
- [`Statistics`](#statistics) - Statistics for a relation

**Enums**

- [`Precision`](#precision) - Represents a value with a degree of certainty. `Precision` is used to

---

## datafusion_common::stats::ColumnStatistics

*Struct*

Statistics for a column within a relation

**Fields:**
- `null_count: Precision<usize>` - Number of null values on column
- `max_value: Precision<crate::ScalarValue>` - Maximum value of column
- `min_value: Precision<crate::ScalarValue>` - Minimum value of column
- `sum_value: Precision<crate::ScalarValue>` - Sum value of a column
- `distinct_count: Precision<usize>` - Number of distinct values
- `byte_size: Precision<usize>` - Estimated size of this column's data in bytes for the output.

**Methods:**

- `fn is_singleton(self: &Self) -> bool` - Column contains a single non null value (e.g constant).
- `fn new_unknown() -> Self` - Returns a [`ColumnStatistics`] instance having all [`Precision::Absent`] parameters.
- `fn with_null_count(self: Self, null_count: Precision<usize>) -> Self` - Set the null count
- `fn with_max_value(self: Self, max_value: Precision<ScalarValue>) -> Self` - Set the max value
- `fn with_min_value(self: Self, min_value: Precision<ScalarValue>) -> Self` - Set the min value
- `fn with_sum_value(self: Self, sum_value: Precision<ScalarValue>) -> Self` - Set the sum value
- `fn with_distinct_count(self: Self, distinct_count: Precision<usize>) -> Self` - Set the distinct count
- `fn with_byte_size(self: Self, byte_size: Precision<usize>) -> Self` - Set the scan byte size
- `fn to_inexact(self: Self) -> Self` - If the exactness of a [`ColumnStatistics`] instance is lost, this

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ColumnStatistics) -> bool`
- **Default**
  - `fn default() -> ColumnStatistics`
- **Clone**
  - `fn clone(self: &Self) -> ColumnStatistics`



## datafusion_common::stats::Precision

*Enum*

Represents a value with a degree of certainty. `Precision` is used to
propagate information the precision of statistical values.

**Generic Parameters:**
- T

**Variants:**
- `Exact(T)` - The exact value is known
- `Inexact(T)` - The value is not known exactly, but is likely close to this value
- `Absent` - Nothing is known about the value

**Methods:**

- `fn add(self: &Self, other: &Precision<ScalarValue>) -> Precision<ScalarValue>` - Calculates the sum of two (possibly inexact) [`ScalarValue`] values,
- `fn sub(self: &Self, other: &Precision<ScalarValue>) -> Precision<ScalarValue>` - Calculates the difference of two (possibly inexact) [`ScalarValue`] values,
- `fn multiply(self: &Self, other: &Precision<ScalarValue>) -> Precision<ScalarValue>` - Calculates the multiplication of two (possibly inexact) [`ScalarValue`] values,
- `fn cast_to(self: &Self, data_type: &DataType) -> Result<Precision<ScalarValue>>` - Casts the value to the given data type, propagating exactness information.
- `fn get_value(self: &Self) -> Option<&T>` - If we have some value (exact or inexact), it returns that value.
- `fn map<U, F>(self: Self, f: F) -> Precision<U>` - Transform the value in this [`Precision`] object, if one exists, using
- `fn is_exact(self: &Self) -> Option<bool>` - Returns `Some(true)` if we have an exact value, `Some(false)` if we
- `fn max(self: &Self, other: &Precision<T>) -> Precision<T>` - Returns the maximum of two (possibly inexact) values, conservatively
- `fn min(self: &Self, other: &Precision<T>) -> Precision<T>` - Returns the minimum of two (possibly inexact) values, conservatively
- `fn to_inexact(self: Self) -> Self` - Demotes the precision state from exact to inexact (if present).
- `fn add(self: &Self, other: &Precision<usize>) -> Precision<usize>` - Calculates the sum of two (possibly inexact) [`usize`] values,
- `fn sub(self: &Self, other: &Precision<usize>) -> Precision<usize>` - Calculates the difference of two (possibly inexact) [`usize`] values,
- `fn multiply(self: &Self, other: &Precision<usize>) -> Precision<usize>` - Calculates the multiplication of two (possibly inexact) [`usize`] values,
- `fn with_estimated_selectivity(self: Self, selectivity: f64) -> Self` - Return the estimate of applying a filter with estimated selectivity

**Traits:** Eq, Copy

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **From**
  - `fn from(value: Precision<usize>) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> Precision<T>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Precision<T>) -> bool`
- **Default**
  - `fn default() -> Precision<T>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`



## datafusion_common::stats::Statistics

*Struct*

Statistics for a relation
Fields are optional and can be inexact because the sources
sometimes provide approximate estimates for performance reasons
and the transformations output are not always predictable.

**Fields:**
- `num_rows: Precision<usize>` - The number of rows estimated to be scanned.
- `total_byte_size: Precision<usize>` - The total bytes of the output data.
- `column_statistics: Vec<ColumnStatistics>` - Statistics on a column level.

**Methods:**

- `fn new_unknown(schema: &Schema) -> Self` - Returns a [`Statistics`] instance for the given schema by assigning
- `fn calculate_total_byte_size(self: & mut Self, schema: &Schema)` - Calculates `total_byte_size` based on the schema and `num_rows`.
- `fn unknown_column(schema: &Schema) -> Vec<ColumnStatistics>` - Returns an unbounded `ColumnStatistics` for each field in the schema.
- `fn with_num_rows(self: Self, num_rows: Precision<usize>) -> Self` - Set the number of rows
- `fn with_total_byte_size(self: Self, total_byte_size: Precision<usize>) -> Self` - Set the total size, in bytes
- `fn add_column_statistics(self: Self, column_stats: ColumnStatistics) -> Self` - Add a column to the column statistics
- `fn to_inexact(self: Self) -> Self` - If the exactness of a [`Statistics`] instance is lost, this function relaxes
- `fn project(self: Self, projection: Option<&Vec<usize>>) -> Self` - Project the statistics to the given column indices.
- `fn with_fetch(self: Self, fetch: Option<usize>, skip: usize, n_partitions: usize) -> Result<Self>` - Calculates the statistics after applying `fetch` and `skip` operations.
- `fn try_merge_iter<'a, I>(items: I, schema: &Schema) -> Result<Statistics>` - Summarize zero or more statistics into a single `Statistics` instance.
- `fn try_merge(self: Self, other: &Statistics) -> Result<Self>` - Merge this Statistics value with another Statistics value.

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Statistics`
- **PartialEq**
  - `fn eq(self: &Self, other: &Statistics) -> bool`
- **Default**
  - `fn default() -> Self` - Returns a new [`Statistics`] instance with all fields set to unknown
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



