**datafusion_common > utils**

# Module: utils

## Contents

**Modules**

- [`datafusion_strsim`](#datafusion_strsim) - Adopted from strsim-rs for string similarity metrics
- [`expr`](#expr) - Expression utilities
- [`memory`](#memory) - This module provides a function to estimate the memory size of a HashTable prior to allocation
- [`proxy`](#proxy) - [`VecAllocExt`] to help tracking of memory allocations
- [`string_utils`](#string_utils) - Utilities for working with strings

**Structs**

- [`SingleRowListArrayBuilder`](#singlerowlistarraybuilder) - Creates single element [`ListArray`], [`LargeListArray`] and

**Enums**

- [`ListCoercion`](#listcoercion) - Information about how to coerce lists.

**Functions**

- [`arrays_into_list_array`](#arrays_into_list_array) - Wrap arrays into a single element `ListArray`.
- [`base_type`](#base_type) - Get the base type of a data type.
- [`bisect`](#bisect) - This function searches for a tuple of given values (`target`) among the given
- [`coerced_fixed_size_list_to_list`](#coerced_fixed_size_list_to_list) - Recursively coerce and `FixedSizeList` elements to `List`
- [`coerced_type_with_base_type_only`](#coerced_type_with_base_type_only) - A helper function to coerce base type in List.
- [`combine_limit`](#combine_limit) - Computes the `skip` and `fetch` parameters of a single limit that would be
- [`compare_rows`](#compare_rows) - This function compares two tuples depending on the given sort options.
- [`evaluate_partition_ranges`](#evaluate_partition_ranges) - Given a list of 0 or more already sorted columns, finds the
- [`extract_row_at_idx_to_buf`](#extract_row_at_idx_to_buf) - Extracts a row at the specified index from a set of columns and stores it in the provided buffer.
- [`find_bisect_point`](#find_bisect_point) - This function searches for a tuple of given values (`target`) among a slice of
- [`find_indices`](#find_indices) - Find indices of each element in `targets` inside `items`. If one of the
- [`fixed_size_list_to_arrays`](#fixed_size_list_to_arrays) - Helper function to convert a FixedSizeListArray into a vector of ArrayRefs.
- [`get_at_indices`](#get_at_indices) - This function "takes" the elements at `indices` from the slice `items`.
- [`get_available_parallelism`](#get_available_parallelism) - Returns the estimated number of threads available for parallel execution.
- [`get_row_at_idx`](#get_row_at_idx) - Given column vectors, returns row at `idx`.
- [`linear_search`](#linear_search) - This function searches for a tuple of given values (`target`) among the given
- [`list_ndims`](#list_ndims) - Compute the number of dimensions in a list data type.
- [`list_to_arrays`](#list_to_arrays) - Helper function to convert a ListArray into a vector of ArrayRefs.
- [`longest_consecutive_prefix`](#longest_consecutive_prefix) - This function finds the longest prefix of the form 0, 1, 2, ... within the
- [`merge_and_order_indices`](#merge_and_order_indices) - Merges collections `first` and `second`, removes duplicates and sorts the
- [`project_schema`](#project_schema) - Applies an optional projection to a [`SchemaRef`], returning the
- [`quote_identifier`](#quote_identifier) - Wraps identifier string in double quotes, escaping any double quotes in
- [`search_in_slice`](#search_in_slice) - This function searches for a tuple of given values (`target`) among a slice of
- [`set_difference`](#set_difference) - Calculates the set difference between sequences `first` and `second`,
- [`take_function_args`](#take_function_args) - Converts a collection of function arguments into a fixed-size array of length N
- [`transpose`](#transpose) - Transposes the given vector of vectors.

---

## datafusion_common::utils::ListCoercion

*Enum*

Information about how to coerce lists.

**Variants:**
- `FixedSizedListToList` - [`DataType::FixedSizeList`] should be coerced to [`DataType::List`].

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ListCoercion`
- **PartialEq**
  - `fn eq(self: &Self, other: &ListCoercion) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &ListCoercion) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_common::utils::SingleRowListArrayBuilder

*Struct*

Creates single element [`ListArray`], [`LargeListArray`] and
[`FixedSizeListArray`] from other arrays

For example this builder can convert `[1, 2, 3]` into `[[1, 2, 3]]`

# Example
```
# use std::sync::Arc;
# use arrow::array::{Array, ListArray};
# use arrow::array::types::Int64Type;
# use datafusion_common::utils::SingleRowListArrayBuilder;
// Array is [1, 2, 3]
let arr = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
    Some(1),
    Some(2),
    Some(3),
])]);
// Wrap as a list array: [[1, 2, 3]]
let list_arr = SingleRowListArrayBuilder::new(Arc::new(arr)).build_list_array();
assert_eq!(list_arr.len(), 1);
```

**Methods:**

- `fn new(arr: ArrayRef) -> Self` - Create a new instance of [`SingleRowListArrayBuilder`]
- `fn with_nullable(self: Self, nullable: bool) -> Self` - Set the nullable flag
- `fn with_field_name(self: Self, field_name: Option<String>) -> Self` - sets the field name for the resulting array
- `fn with_field(self: Self, field: &Field) -> Self` - Copies field name and nullable from the specified field
- `fn build_list_array(self: Self) -> ListArray` - Build a single element [`ListArray`]
- `fn build_list_scalar(self: Self) -> ScalarValue` - Build a single element [`ListArray`] and wrap as [`ScalarValue::List`]
- `fn build_large_list_array(self: Self) -> LargeListArray` - Build a single element [`LargeListArray`]
- `fn build_large_list_scalar(self: Self) -> ScalarValue` - Build a single element [`LargeListArray`] and wrap as [`ScalarValue::LargeList`]
- `fn build_fixed_size_list_array(self: Self, list_size: usize) -> FixedSizeListArray` - Build a single element [`FixedSizeListArray`]
- `fn build_fixed_size_list_scalar(self: Self, list_size: usize) -> ScalarValue` - Build a single element [`FixedSizeListArray`] and wrap as [`ScalarValue::FixedSizeList`]

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> SingleRowListArrayBuilder`



## datafusion_common::utils::arrays_into_list_array

*Function*

Wrap arrays into a single element `ListArray`.

Example:
```
use arrow::array::{Int32Array, ListArray, ArrayRef};
use arrow::datatypes::{Int32Type, Field};
use std::sync::Arc;

let arr1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
let arr2 = Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef;

let list_arr = datafusion_common::utils::arrays_into_list_array([arr1, arr2]).unwrap();

let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(
   vec![
    Some(vec![Some(1), Some(2), Some(3)]),
    Some(vec![Some(4), Some(5), Some(6)]),
   ]
);

assert_eq!(list_arr, expected);

```rust
fn arrays_into_list_array<impl IntoIterator<Item = ArrayRef>>(arr: impl Trait) -> crate::Result<arrow::array::ListArray>
```



## datafusion_common::utils::base_type

*Function*

Get the base type of a data type.

Example
```
use arrow::datatypes::{DataType, Field};
use datafusion_common::utils::base_type;
use std::sync::Arc;

let data_type =
    DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
assert_eq!(base_type(&data_type), DataType::Int32);

let data_type = DataType::Int32;
assert_eq!(base_type(&data_type), DataType::Int32);
```

```rust
fn base_type(data_type: &arrow::datatypes::DataType) -> arrow::datatypes::DataType
```



## datafusion_common::utils::bisect

*Function*

This function searches for a tuple of given values (`target`) among the given
rows (`item_columns`) using the bisection algorithm. It assumes that `item_columns`
is sorted according to `sort_options` and returns the insertion index of `target`.
Template argument `SIDE` being `true`/`false` means left/right insertion.

```rust
fn bisect<const SIDE>(item_columns: &[arrow::array::ArrayRef], target: &[crate::ScalarValue], sort_options: &[arrow::compute::SortOptions]) -> crate::Result<usize>
```



## datafusion_common::utils::coerced_fixed_size_list_to_list

*Function*

Recursively coerce and `FixedSizeList` elements to `List`

```rust
fn coerced_fixed_size_list_to_list(data_type: &arrow::datatypes::DataType) -> arrow::datatypes::DataType
```



## datafusion_common::utils::coerced_type_with_base_type_only

*Function*

A helper function to coerce base type in List.

Example
```
use arrow::datatypes::{DataType, Field};
use datafusion_common::utils::coerced_type_with_base_type_only;
use std::sync::Arc;

let data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
let base_type = DataType::Float64;
let coerced_type = coerced_type_with_base_type_only(&data_type, &base_type, None);
assert_eq!(coerced_type, DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))));

```rust
fn coerced_type_with_base_type_only(data_type: &arrow::datatypes::DataType, base_type: &arrow::datatypes::DataType, array_coercion: Option<&ListCoercion>) -> arrow::datatypes::DataType
```



## datafusion_common::utils::combine_limit

*Function*

Computes the `skip` and `fetch` parameters of a single limit that would be
equivalent to two consecutive limits with the given `skip`/`fetch` parameters.

There are multiple cases to consider:

# Case 0: Parent and child are disjoint (`child_fetch <= skip`).

```text
  Before merging:
                    |........skip........|---fetch-->|     Parent limit
   |...child_skip...|---child_fetch-->|                    Child limit
```

  After merging:
```text
   |.........(child_skip + skip).........|
```

# Case 1: Parent is beyond child's range (`skip < child_fetch <= skip + fetch`).

  Before merging:
```text
                    |...skip...|------------fetch------------>|   Parent limit
   |...child_skip...|-------------child_fetch------------>|       Child limit
```

  After merging:
```text
   |....(child_skip + skip)....|---(child_fetch - skip)-->|
```

 # Case 2: Parent is within child's range (`skip + fetch < child_fetch`).

  Before merging:
```text
                    |...skip...|---fetch-->|                   Parent limit
   |...child_skip...|-------------child_fetch------------>|    Child limit
```

  After merging:
```text
   |....(child_skip + skip)....|---fetch-->|
```

```rust
fn combine_limit(parent_skip: usize, parent_fetch: Option<usize>, child_skip: usize, child_fetch: Option<usize>) -> (usize, Option<usize>)
```



## datafusion_common::utils::compare_rows

*Function*

This function compares two tuples depending on the given sort options.

```rust
fn compare_rows(x: &[crate::ScalarValue], y: &[crate::ScalarValue], sort_options: &[arrow::compute::SortOptions]) -> crate::Result<std::cmp::Ordering>
```



## Module: datafusion_strsim

Adopted from strsim-rs for string similarity metrics



## datafusion_common::utils::evaluate_partition_ranges

*Function*

Given a list of 0 or more already sorted columns, finds the
partition ranges that would partition equally across columns.

See [`partition`] for more details.

```rust
fn evaluate_partition_ranges(num_rows: usize, partition_columns: &[arrow::compute::SortColumn]) -> crate::Result<Vec<std::ops::Range<usize>>>
```



## Module: expr

Expression utilities



## datafusion_common::utils::extract_row_at_idx_to_buf

*Function*

Extracts a row at the specified index from a set of columns and stores it in the provided buffer.

```rust
fn extract_row_at_idx_to_buf(columns: &[arrow::array::ArrayRef], idx: usize, buf: & mut Vec<crate::ScalarValue>) -> crate::Result<()>
```



## datafusion_common::utils::find_bisect_point

*Function*

This function searches for a tuple of given values (`target`) among a slice of
the given rows (`item_columns`) using the bisection algorithm. The slice starts
at the index `low` and ends at the index `high`. The boolean-valued function
`compare_fn` specifies whether we bisect on the left (by returning `false`),
or on the right (by returning `true`) when we compare the target value with
the current value as we iteratively bisect the input.

```rust
fn find_bisect_point<F>(item_columns: &[arrow::array::ArrayRef], target: &[crate::ScalarValue], compare_fn: F, low: usize, high: usize) -> crate::Result<usize>
```



## datafusion_common::utils::find_indices

*Function*

Find indices of each element in `targets` inside `items`. If one of the
elements is absent in `items`, returns an error.

```rust
fn find_indices<T, S, impl IntoIterator<Item = S>>(items: &[T], targets: impl Trait) -> crate::Result<Vec<usize>>
```



## datafusion_common::utils::fixed_size_list_to_arrays

*Function*

Helper function to convert a FixedSizeListArray into a vector of ArrayRefs.

```rust
fn fixed_size_list_to_arrays(a: &arrow::array::ArrayRef) -> Vec<arrow::array::ArrayRef>
```



## datafusion_common::utils::get_at_indices

*Function*

This function "takes" the elements at `indices` from the slice `items`.

```rust
fn get_at_indices<T, I, impl IntoIterator<Item = I>>(items: &[T], indices: impl Trait) -> crate::Result<Vec<T>>
```



## datafusion_common::utils::get_available_parallelism

*Function*

Returns the estimated number of threads available for parallel execution.

This is a wrapper around `std::thread::available_parallelism`, providing a default value
of `1` if the system's parallelism cannot be determined.

```rust
fn get_available_parallelism() -> usize
```



## datafusion_common::utils::get_row_at_idx

*Function*

Given column vectors, returns row at `idx`.

```rust
fn get_row_at_idx(columns: &[arrow::array::ArrayRef], idx: usize) -> crate::Result<Vec<crate::ScalarValue>>
```



## datafusion_common::utils::linear_search

*Function*

This function searches for a tuple of given values (`target`) among the given
rows (`item_columns`) via a linear scan. It assumes that `item_columns` is sorted
according to `sort_options` and returns the insertion index of `target`.
Template argument `SIDE` being `true`/`false` means left/right insertion.

```rust
fn linear_search<const SIDE>(item_columns: &[arrow::array::ArrayRef], target: &[crate::ScalarValue], sort_options: &[arrow::compute::SortOptions]) -> crate::Result<usize>
```



## datafusion_common::utils::list_ndims

*Function*

Compute the number of dimensions in a list data type.

```rust
fn list_ndims(data_type: &arrow::datatypes::DataType) -> u64
```



## datafusion_common::utils::list_to_arrays

*Function*

Helper function to convert a ListArray into a vector of ArrayRefs.

```rust
fn list_to_arrays<O>(a: &arrow::array::ArrayRef) -> Vec<arrow::array::ArrayRef>
```



## datafusion_common::utils::longest_consecutive_prefix

*Function*

This function finds the longest prefix of the form 0, 1, 2, ... within the
collection `sequence`. Examples:
- For 0, 1, 2, 4, 5; we would produce 3, meaning 0, 1, 2 is the longest satisfying
  prefix.
- For 1, 2, 3, 4; we would produce 0, meaning there is no such prefix.

```rust
fn longest_consecutive_prefix<T, impl IntoIterator<Item = T>>(sequence: impl Trait) -> usize
```



## Module: memory

This module provides a function to estimate the memory size of a HashTable prior to allocation



## datafusion_common::utils::merge_and_order_indices

*Function*

Merges collections `first` and `second`, removes duplicates and sorts the
result, returning it as a [`Vec`].

```rust
fn merge_and_order_indices<T, S, impl IntoIterator<Item = T>, impl IntoIterator<Item = S>>(first: impl Trait, second: impl Trait) -> Vec<usize>
```



## datafusion_common::utils::project_schema

*Function*

Applies an optional projection to a [`SchemaRef`], returning the
projected schema

Example:
```
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::project_schema;

// Schema with columns 'a', 'b', and 'c'
let schema = SchemaRef::new(Schema::new(vec![
    Field::new("a", DataType::Int32, true),
    Field::new("b", DataType::Int64, true),
    Field::new("c", DataType::Utf8, true),
]));

// Pick columns 'c' and 'b'
let projection = Some(vec![2, 1]);
let projected_schema = project_schema(&schema, projection.as_ref()).unwrap();

let expected_schema = SchemaRef::new(Schema::new(vec![
    Field::new("c", DataType::Utf8, true),
    Field::new("b", DataType::Int64, true),
]));

assert_eq!(projected_schema, expected_schema);
```

```rust
fn project_schema(schema: &arrow::datatypes::SchemaRef, projection: Option<&Vec<usize>>) -> crate::Result<arrow::datatypes::SchemaRef>
```



## Module: proxy

[`VecAllocExt`] to help tracking of memory allocations



## datafusion_common::utils::quote_identifier

*Function*

Wraps identifier string in double quotes, escaping any double quotes in
the identifier by replacing it with two double quotes

e.g. identifier `tab.le"name` becomes `"tab.le""name"`

```rust
fn quote_identifier(s: &str) -> std::borrow::Cow<str>
```



## datafusion_common::utils::search_in_slice

*Function*

This function searches for a tuple of given values (`target`) among a slice of
the given rows (`item_columns`) via a linear scan. The slice starts at the index
`low` and ends at the index `high`. The boolean-valued function `compare_fn`
specifies the stopping criterion.

```rust
fn search_in_slice<F>(item_columns: &[arrow::array::ArrayRef], target: &[crate::ScalarValue], compare_fn: F, low: usize, high: usize) -> crate::Result<usize>
```



## datafusion_common::utils::set_difference

*Function*

Calculates the set difference between sequences `first` and `second`,
returning the result as a [`Vec`]. Preserves the ordering of `first`.

```rust
fn set_difference<T, S, impl IntoIterator<Item = T>, impl IntoIterator<Item = S>>(first: impl Trait, second: impl Trait) -> Vec<usize>
```



## Module: string_utils

Utilities for working with strings



## datafusion_common::utils::take_function_args

*Function*

Converts a collection of function arguments into a fixed-size array of length N
producing a reasonable error message in case of unexpected number of arguments.

# Example
```
# use datafusion_common::Result;
# use datafusion_common::utils::take_function_args;
# use datafusion_common::ScalarValue;
fn my_function(args: &[ScalarValue]) -> Result<()> {
    // function expects 2 args, so create a 2-element array
    let [arg1, arg2] = take_function_args("my_function", args)?;
    // ... do stuff..
    Ok(())
}

// Calling the function with 1 argument produces an error:
let args = vec![ScalarValue::Int32(Some(10))];
let err = my_function(&args).unwrap_err();
assert_eq!(
    err.to_string(),
    "Execution error: my_function function requires 2 arguments, got 1"
);
// Calling the function with 2 arguments works great
let args = vec![ScalarValue::Int32(Some(10)), ScalarValue::Int32(Some(20))];
my_function(&args).unwrap();
```

```rust
fn take_function_args<const N, T, impl IntoIterator<Item = T>>(function_name: &str, args: impl Trait) -> crate::Result<[T; N]>
```



## datafusion_common::utils::transpose

*Function*

Transposes the given vector of vectors.

```rust
fn transpose<T>(original: Vec<Vec<T>>) -> Vec<Vec<T>>
```



