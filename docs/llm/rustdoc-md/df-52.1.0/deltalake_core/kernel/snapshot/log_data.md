**deltalake_core > kernel > snapshot > log_data**

# Module: kernel::snapshot::log_data

## Contents

**Structs**

- [`LogDataHandler`](#logdatahandler) - Provides semanitc access to the log data.

---

## deltalake_core::kernel::snapshot::log_data::LogDataHandler

*Struct*

Provides semanitc access to the log data.

This is a helper struct that provides access to the log data in a more semantic way
to avid the necessiity of knowing the exact layout of the underlying log data.

**Generic Parameters:**
- 'a

**Methods:**

- `fn num_files(self: &Self) -> usize` - The number of files in the log data.
- `fn iter(self: &Self) -> impl Trait`

**Trait Implementations:**

- **IntoIterator**
  - `fn into_iter(self: Self) -> <Self as >::IntoIter`
- **DataFusionMixins**
  - `fn read_schema(self: &Self) -> ArrowSchemaRef`
  - `fn input_schema(self: &Self) -> ArrowSchemaRef`
  - `fn parse_predicate_expression<impl AsRef<str>>(self: &Self, expr: impl Trait, session: &dyn Session) -> DeltaResult<Expr>`
- **PruningStatistics**
  - `fn min_values(self: &Self, column: &Column) -> Option<ArrayRef>` - return the minimum values for the named column, if known.
  - `fn max_values(self: &Self, column: &Column) -> Option<ArrayRef>` - return the maximum values for the named column, if known.
  - `fn num_containers(self: &Self) -> usize` - return the number of containers (e.g. row groups) being
  - `fn null_counts(self: &Self, column: &Column) -> Option<ArrayRef>` - return the number of null values for the named column as an
  - `fn row_counts(self: &Self, _column: &Column) -> Option<ArrayRef>` - return the number of rows for the named column in each container
  - `fn contained(self: &Self, column: &Column, value: &HashSet<ScalarValue>) -> Option<BooleanArray>`
- **Clone**
  - `fn clone(self: &Self) -> LogDataHandler<'a>`



