**datafusion_functions_table > generate_series**

# Module: generate_series

## Contents

**Structs**

- [`Empty`](#empty) - Empty generator that produces no rows - used when series arguments contain null values
- [`GenerateSeriesFunc`](#generateseriesfunc)
- [`GenerateSeriesTable`](#generateseriestable) - Table that generates a series of integers/timestamps from `start`(inclusive) to `end`, incrementing by step
- [`GenericSeriesState`](#genericseriesstate)
- [`RangeFunc`](#rangefunc)
- [`TimestampValue`](#timestampvalue)

**Enums**

- [`GenSeriesArgs`](#genseriesargs) - Indicates the arguments used for generating a series.

**Traits**

- [`SeriesValue`](#seriesvalue) - Trait for values that can be generated in a series

---

## datafusion_functions_table::generate_series::Empty

*Struct*

Empty generator that produces no rows - used when series arguments contain null values

**Methods:**

- `fn name(self: &Self) -> &'static str`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Empty`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **LazyBatchGenerator**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn generate_next_batch(self: & mut Self) -> Result<Option<RecordBatch>>`
  - `fn reset_state(self: &Self) -> Arc<RwLock<dyn LazyBatchGenerator>>`



## datafusion_functions_table::generate_series::GenSeriesArgs

*Enum*

Indicates the arguments used for generating a series.

**Variants:**
- `ContainsNull{ name: &'static str }` - ContainsNull signifies that at least one argument(start, end, step) was null, thus no series will be generated.
- `Int64Args{ start: i64, end: i64, step: i64, include_end: bool, name: &'static str }` - Int64Args holds the start, end, and step values for generating integer series when all arguments are not null.
- `TimestampArgs{ start: i64, end: i64, step: arrow::datatypes::IntervalMonthDayNano, tz: Option<std::sync::Arc<str>>, include_end: bool, name: &'static str }` - TimestampArgs holds the start, end, and step values for generating timestamp series when all arguments are not null.
- `DateArgs{ start: i64, end: i64, step: arrow::datatypes::IntervalMonthDayNano, include_end: bool, name: &'static str }` - DateArgs holds the start, end, and step values for generating date series when all arguments are not null.

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> GenSeriesArgs`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_table::generate_series::GenerateSeriesFunc

*Struct*

**Trait Implementations:**

- **TableFunctionImpl**
  - `fn call(self: &Self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_table::generate_series::GenerateSeriesTable

*Struct*

Table that generates a series of integers/timestamps from `start`(inclusive) to `end`, incrementing by step

**Methods:**

- `fn new(schema: SchemaRef, args: GenSeriesArgs) -> Self`
- `fn as_generator(self: &Self, batch_size: usize) -> Result<Arc<RwLock<dyn LazyBatchGenerator>>>`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> GenerateSeriesTable`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TableProvider**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> SchemaRef`
  - `fn table_type(self: &Self) -> TableType`
  - `fn scan(self: &'life0 Self, state: &'life1 dyn Session, projection: Option<&'life2 Vec<usize>>, _filters: &'life3 [Expr], _limit: Option<usize>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`



## datafusion_functions_table::generate_series::GenericSeriesState

*Struct*

**Generic Parameters:**
- T

**Methods:**

- `fn name(self: &Self) -> &'static str`
- `fn batch_size(self: &Self) -> usize`
- `fn include_end(self: &Self) -> bool`
- `fn start(self: &Self) -> &T`
- `fn end(self: &Self) -> &T`
- `fn step(self: &Self) -> &<T as >::StepType`
- `fn current(self: &Self) -> &T`

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **LazyBatchGenerator**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn generate_next_batch(self: & mut Self) -> Result<Option<RecordBatch>>`
  - `fn reset_state(self: &Self) -> Arc<RwLock<dyn LazyBatchGenerator>>`
- **Clone**
  - `fn clone(self: &Self) -> GenericSeriesState<T>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_table::generate_series::RangeFunc

*Struct*

**Trait Implementations:**

- **TableFunctionImpl**
  - `fn call(self: &Self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_table::generate_series::SeriesValue

*Trait*

Trait for values that can be generated in a series

**Methods:**

- `StepType`
- `ValueType`
- `should_stop`: Check if we've reached the end of the series
- `advance`: Advance to the next value in the series
- `create_array`: Create an Arrow array from a vector of values
- `to_value_type`: Convert self to ValueType for array creation
- `display_value`: Display the value for debugging



## datafusion_functions_table::generate_series::TimestampValue

*Struct*

**Methods:**

- `fn value(self: &Self) -> i64`
- `fn tz_str(self: &Self) -> Option<&Arc<str>>`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> TimestampValue`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **SeriesValue**
  - `fn should_stop(self: &Self, end: Self, step: &<Self as >::StepType, include_end: bool) -> bool`
  - `fn advance(self: & mut Self, step: &<Self as >::StepType) -> Result<()>`
  - `fn create_array(self: &Self, values: Vec<<Self as >::ValueType>) -> Result<ArrayRef>`
  - `fn to_value_type(self: &Self) -> <Self as >::ValueType`
  - `fn display_value(self: &Self) -> String`



