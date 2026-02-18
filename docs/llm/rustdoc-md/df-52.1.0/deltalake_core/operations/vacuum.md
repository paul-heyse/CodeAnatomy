**deltalake_core > operations > vacuum**

# Module: operations::vacuum

## Contents

**Structs**

- [`VacuumBuilder`](#vacuumbuilder) - Vacuum a Delta table with the given options
- [`VacuumEndOperationMetrics`](#vacuumendoperationmetrics) - Details for the Vacuum End operation for the transaction log
- [`VacuumMetrics`](#vacuummetrics) - Details for the Vacuum operation including which files were
- [`VacuumStartOperationMetrics`](#vacuumstartoperationmetrics) - Details for the Vacuum start operation for the transaction log

**Enums**

- [`VacuumMode`](#vacuummode) - Type of Vacuum operation to perform

**Traits**

- [`Clock`](#clock) - A source of time

---

## deltalake_core::operations::vacuum::Clock

*Trait*

A source of time

**Methods:**

- `current_timestamp_millis`: get the current time in milliseconds since epoch



## deltalake_core::operations::vacuum::VacuumBuilder

*Struct*

Vacuum a Delta table with the given options
See this module's documentation for more information

**Methods:**

- `fn with_retention_period(self: Self, retention_period: Duration) -> Self` - Override the default retention period for which files are deleted.
- `fn with_keep_versions(self: Self, versions: &[i64]) -> Self` - Specify table versions that we want to keep for time travel.
- `fn with_mode(self: Self, mode: VacuumMode) -> Self` - Override the default vacuum mode (lite)
- `fn with_dry_run(self: Self, dry_run: bool) -> Self` - Only determine which files should be deleted
- `fn with_enforce_retention_duration(self: Self, enforce: bool) -> Self` - Check if the specified retention period is less than the table's minimum
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



## deltalake_core::operations::vacuum::VacuumEndOperationMetrics

*Struct*

Details for the Vacuum End operation for the transaction log

**Fields:**
- `num_deleted_files: i64` - The number of actually deleted files
- `num_vacuumed_directories: i64` - The number of actually vacuumed directories

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



## deltalake_core::operations::vacuum::VacuumMetrics

*Struct*

Details for the Vacuum operation including which files were

**Fields:**
- `dry_run: bool` - Was this a dry run
- `files_deleted: Vec<String>` - Files deleted successfully

**Trait Implementations:**

- **Default**
  - `fn default() -> VacuumMetrics`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::operations::vacuum::VacuumMode

*Enum*

Type of Vacuum operation to perform

**Variants:**
- `Lite` - The `lite` mode will only remove files which are referenced in the `_delta_log` associated
- `Full` - A `full` mode vacuum will remove _all_ data files no longer actively referenced in the

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> VacuumMode`
- **Clone**
  - `fn clone(self: &Self) -> VacuumMode`
- **PartialEq**
  - `fn eq(self: &Self, other: &VacuumMode) -> bool`



## deltalake_core::operations::vacuum::VacuumStartOperationMetrics

*Struct*

Details for the Vacuum start operation for the transaction log

**Fields:**
- `num_files_to_delete: i64` - The number of files that will be deleted
- `size_of_data_to_delete: i64` - Size of the data to be deleted in bytes

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



