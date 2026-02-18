**arrow**

# Module: arrow

## Contents

**Modules**

- [`array`](#array) - Statically typed implementations of Arrow Arrays
- [`compute`](#compute) - Computation kernels on Arrow Arrays
- [`datatypes`](#datatypes) - Defines the logical data types of Arrow arrays.
- [`error`](#error) - Defines `ArrowError` for representing failures in various Arrow operations.
- [`record_batch`](#record_batch) - Contains the `RecordBatch` type and associated traits
- [`tensor`](#tensor) - Arrow Tensor Type, defined in
- [`util`](#util) - Utility functions for working with Arrow data

**Constants**

- [`ARROW_VERSION`](#arrow_version) - Arrow crate version

---

## arrow::ARROW_VERSION

*Constant*: `&str`

Arrow crate version



## Module: array

Statically typed implementations of Arrow Arrays

**See [arrow_array] for examples and usage instructions**



## Module: compute

Computation kernels on Arrow Arrays



## Module: datatypes

Defines the logical data types of Arrow arrays.

The most important things you might be looking for are:
 * [`Schema`] to describe a schema.
 * [`Field`] to describe one field within a schema.
 * [`DataType`] to describe the type of a field.



## Module: error

Defines `ArrowError` for representing failures in various Arrow operations.



## Module: record_batch

Contains the `RecordBatch` type and associated traits



## Module: tensor

Arrow Tensor Type, defined in
[`format/Tensor.fbs`](https://github.com/apache/arrow/blob/master/format/Tensor.fbs).



## Module: util

Utility functions for working with Arrow data



