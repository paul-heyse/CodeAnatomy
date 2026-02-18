**deltalake_core > operations > restore**

# Module: operations::restore

## Contents

**Structs**

- [`RestoreBuilder`](#restorebuilder) - Restore a Delta table with given version
- [`RestoreMetrics`](#restoremetrics) - Metrics from Restore

---

## deltalake_core::operations::restore::RestoreBuilder

*Struct*

Restore a Delta table with given version
See this module's documentation for more information

**Methods:**

- `fn with_version_to_restore(self: Self, version: i64) -> Self` - Set the version to restore
- `fn with_datetime_to_restore(self: Self, datetime: DateTime<Utc>) -> Self` - Set the datetime to restore
- `fn with_ignore_missing_files(self: Self, ignore_missing_files: bool) -> Self` - Set whether to ignore missing files which delete manually or by vacuum.
- `fn with_protocol_downgrade_allowed(self: Self, protocol_downgrade_allowed: bool) -> Self` - Set whether allow to downgrade protocol
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



## deltalake_core::operations::restore::RestoreMetrics

*Struct*

Metrics from Restore

**Fields:**
- `num_removed_file: usize` - Number of files removed
- `num_restored_file: usize` - Number of files restored

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Default**
  - `fn default() -> RestoreMetrics`



