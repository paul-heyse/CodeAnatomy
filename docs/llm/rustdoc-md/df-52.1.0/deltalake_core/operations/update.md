**deltalake_core > operations > update**

# Module: operations::update

## Contents

**Structs**

- [`UpdateBuilder`](#updatebuilder) - Updates records in the Delta Table.
- [`UpdateMetrics`](#updatemetrics) - Metrics collected during the Update operation

---

## deltalake_core::operations::update::UpdateBuilder

*Struct*

Updates records in the Delta Table.
See this module's documentation for more information

**Methods:**

- `fn with_predicate<E>(self: Self, predicate: E) -> Self` - Which records to update
- `fn with_update<S, E>(self: Self, column: S, expression: E) -> Self` - Perform an additional update expression during the operation
- `fn with_session_state(self: Self, session: Arc<dyn Session>) -> Self` - Set the DataFusion session used for planning and execution.
- `fn with_session_fallback_policy(self: Self, policy: SessionFallbackPolicy) -> Self` - Control how delta-rs resolves the provided session when it is not a concrete `SessionState`.
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_writer_properties(self: Self, writer_properties: WriterProperties) -> Self` - Writer properties passed to parquet writer for when files are rewritten
- `fn with_safe_cast(self: Self, safe_cast: bool) -> Self` - Specify the cast options to use when casting columns that do not match
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



## deltalake_core::operations::update::UpdateMetrics

*Struct*

Metrics collected during the Update operation

**Fields:**
- `num_added_files: usize` - Number of files added.
- `num_removed_files: usize` - Number of files removed.
- `num_updated_rows: usize` - Number of rows updated.
- `num_copied_rows: usize` - Number of rows just copied over in the process of updating files.
- `execution_time_ms: u64` - Time taken to execute the entire operation.
- `scan_time_ms: u64` - Time taken to scan the files for matches.

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> UpdateMetrics`



