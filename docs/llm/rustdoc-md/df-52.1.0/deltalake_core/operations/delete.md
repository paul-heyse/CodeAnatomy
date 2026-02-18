**deltalake_core > operations > delete**

# Module: operations::delete

## Contents

**Structs**

- [`DeleteBuilder`](#deletebuilder) - Delete Records from the Delta Table.
- [`DeleteMetrics`](#deletemetrics) - Metrics for the Delete Operation

---

## deltalake_core::operations::delete::DeleteBuilder

*Struct*

Delete Records from the Delta Table.
See this module's documentation for more information

**Methods:**

- `fn with_predicate<E>(self: Self, predicate: E) -> Self` - A predicate that determines if a record is deleted
- `fn with_session_state(self: Self, session: Arc<dyn Session>) -> Self` - Set the DataFusion session used for planning and execution.
- `fn with_session_fallback_policy(self: Self, policy: SessionFallbackPolicy) -> Self` - Control how delta-rs resolves the provided session when it is not a concrete `SessionState`.
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional information to write to the commit
- `fn with_writer_properties(self: Self, writer_properties: WriterProperties) -> Self` - Writer properties passed to parquet writer for when files are rewritten
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`
- **Clone**
  - `fn clone(self: &Self) -> DeleteBuilder`



## deltalake_core::operations::delete::DeleteMetrics

*Struct*

Metrics for the Delete Operation

**Fields:**
- `num_added_files: usize` - Number of files added
- `num_removed_files: usize` - Number of files removed
- `num_deleted_rows: usize` - Number of rows removed
- `num_copied_rows: usize` - Number of rows copied in the process of deleting files
- `execution_time_ms: u64` - Time taken to execute the entire operation
- `scan_time_ms: u64` - Time taken to scan the file for matches
- `rewrite_time_ms: u64` - Time taken to rewrite the matched files

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Default**
  - `fn default() -> DeleteMetrics`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



