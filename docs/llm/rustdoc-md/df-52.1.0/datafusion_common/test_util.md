**datafusion_common > test_util**

# Module: test_util

## Contents

**Modules**

- [`array_conversion`](#array_conversion)

**Functions**

- [`arrow_test_data`](#arrow_test_data) - Returns the arrow test data directory, which is by default stored
- [`batches_to_sort_string`](#batches_to_sort_string)
- [`batches_to_string`](#batches_to_string)
- [`datafusion_test_data`](#datafusion_test_data) - Returns the datafusion test data directory, which is by default rooted at `datafusion/core/tests/data`.
- [`format_batches`](#format_batches)
- [`get_data_dir`](#get_data_dir) - Returns a directory path for finding test data.
- [`parquet_test_data`](#parquet_test_data) - Returns the parquet test data directory, which is by default

**Traits**

- [`IntoArrayRef`](#intoarrayref) - Converts a vector or array into an ArrayRef.

---

## datafusion_common::test_util::IntoArrayRef

*Trait*

Converts a vector or array into an ArrayRef.

**Methods:**

- `into_array_ref`



## Module: array_conversion



## datafusion_common::test_util::arrow_test_data

*Function*

Returns the arrow test data directory, which is by default stored
in a git submodule rooted at `testing/data`.

The default can be overridden by the optional environment
variable `ARROW_TEST_DATA`

panics when the directory can not be found.

Example:
```
let testdata = datafusion_common::test_util::arrow_test_data();
let csvdata = format!("{}/csv/aggregate_test_100.csv", testdata);
assert!(std::path::PathBuf::from(csvdata).exists());
```

```rust
fn arrow_test_data() -> String
```



## datafusion_common::test_util::batches_to_sort_string

*Function*

```rust
fn batches_to_sort_string(batches: &[arrow::array::RecordBatch]) -> String
```



## datafusion_common::test_util::batches_to_string

*Function*

```rust
fn batches_to_string(batches: &[arrow::array::RecordBatch]) -> String
```



## datafusion_common::test_util::datafusion_test_data

*Function*

Returns the datafusion test data directory, which is by default rooted at `datafusion/core/tests/data`.

The default can be overridden by the optional environment
variable `DATAFUSION_TEST_DATA`

panics when the directory can not be found.

Example:
```
let testdata = datafusion_common::test_util::datafusion_test_data();
let csvdata = format!("{}/window_1.csv", testdata);
assert!(std::path::PathBuf::from(csvdata).exists());
```

```rust
fn datafusion_test_data() -> String
```



## datafusion_common::test_util::format_batches

*Function*

```rust
fn format_batches(results: &[arrow::array::RecordBatch]) -> Result<impl Trait, arrow::error::ArrowError>
```



## datafusion_common::test_util::get_data_dir

*Function*

Returns a directory path for finding test data.

udf_env: name of an environment variable

submodule_dir: fallback path (relative to CARGO_MANIFEST_DIR)

 Returns either:
The path referred to in `udf_env` if that variable is set and refers to a directory
The submodule_data directory relative to CARGO_MANIFEST_PATH

```rust
fn get_data_dir(udf_env: &str, submodule_data: &str) -> Result<std::path::PathBuf, Box<dyn Error>>
```



## datafusion_common::test_util::parquet_test_data

*Function*

Returns the parquet test data directory, which is by default
stored in a git submodule rooted at
`parquet-testing/data`.

The default can be overridden by the optional environment variable
`PARQUET_TEST_DATA`

panics when the directory can not be found.

Example:
```
let testdata = datafusion_common::test_util::parquet_test_data();
let filename = format!("{}/binary.parquet", testdata);
assert!(std::path::PathBuf::from(filename).exists());
```

```rust
fn parquet_test_data() -> String
```



