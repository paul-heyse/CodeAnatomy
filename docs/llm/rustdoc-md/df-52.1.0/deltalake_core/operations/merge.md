**deltalake_core > operations > merge**

# Module: operations::merge

## Contents

**Structs**

- [`DeleteBuilder`](#deletebuilder) - Builder for delete clauses
- [`InsertBuilder`](#insertbuilder) - Builder for insert clauses
- [`MergeBuilder`](#mergebuilder) - Merge records into a Delta Table.
- [`MergeMetrics`](#mergemetrics) - Metrics for the Merge Operation
- [`UpdateBuilder`](#updatebuilder) - Builder for update clauses

---

## deltalake_core::operations::merge::DeleteBuilder

*Struct*

Builder for delete clauses

**Methods:**

- `fn predicate<E>(self: Self, predicate: E) -> Self` - Delete a record when the predicate is satisfied

**Trait Implementations:**

- **Default**
  - `fn default() -> DeleteBuilder`



## deltalake_core::operations::merge::InsertBuilder

*Struct*

Builder for insert clauses

**Methods:**

- `fn predicate<E>(self: Self, predicate: E) -> Self` - Perform the insert operation when the predicate is satisfied
- `fn set<C, E>(self: Self, column: C, expression: E) -> Self` - Which values to insert into the target tables. If a target column is not

**Trait Implementations:**

- **Default**
  - `fn default() -> InsertBuilder`



## deltalake_core::operations::merge::MergeBuilder

*Struct*

Merge records into a Delta Table.

**Methods:**

- `fn new<E>(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>, predicate: E, source: DataFrame) -> Self` - Create a new [`MergeBuilder`]
- `fn when_matched_update<F>(self: Self, builder: F) -> DeltaResult<MergeBuilder>` - Update a target record when it matches with a source record
- `fn when_matched_delete<F>(self: Self, builder: F) -> DeltaResult<MergeBuilder>` - Delete a target record when it matches with a source record
- `fn when_not_matched_insert<F>(self: Self, builder: F) -> DeltaResult<MergeBuilder>` - Insert a source record when it does not match with a target record
- `fn when_not_matched_by_source_update<F>(self: Self, builder: F) -> DeltaResult<MergeBuilder>` - Update a target record when it does not match with a
- `fn when_not_matched_by_source_delete<F>(self: Self, builder: F) -> DeltaResult<MergeBuilder>` - Delete a target record when it does not match with a source record
- `fn with_source_alias<S>(self: Self, alias: S) -> Self` - Rename columns in the source dataset to have a prefix of `alias`.`original column name`
- `fn with_target_alias<S>(self: Self, alias: S) -> Self` - Rename columns in the target dataset to have a prefix of `alias`.`original column name`
- `fn with_merge_schema(self: Self, merge_schema: bool) -> Self` - Add Schema Write Mode
- `fn with_session_state(self: Self, state: Arc<dyn Session>) -> Self` - Set the DataFusion session used for planning and execution.
- `fn with_session_fallback_policy(self: Self, policy: SessionFallbackPolicy) -> Self` - Control how delta-rs resolves the provided session when it is not a concrete `SessionState`.
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_writer_properties(self: Self, writer_properties: WriterProperties) -> Self` - Writer properties passed to parquet writer for when files are rewritten
- `fn with_safe_cast(self: Self, safe_cast: bool) -> Self` - Specify the cast options to use when casting columns that do not match
- `fn with_streaming(self: Self, streaming: bool) -> Self` - Set streaming mode execution
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



## deltalake_core::operations::merge::MergeMetrics

*Struct*

Metrics for the Merge Operation

**Fields:**
- `num_source_rows: usize` - Number of rows in the source data
- `num_target_rows_inserted: usize` - Number of rows inserted into the target table
- `num_target_rows_updated: usize` - Number of rows updated in the target table
- `num_target_rows_deleted: usize` - Number of rows deleted in the target table
- `num_target_rows_copied: usize` - Number of target rows copied
- `num_output_rows: usize` - Total number of rows written out
- `num_target_files_scanned: usize` - Amount of files considered during table scan
- `num_target_files_skipped_during_scan: usize` - Amount of files not considered (pruned) during table scan
- `num_target_files_added: usize` - Number of files added to the sink(target)
- `num_target_files_removed: usize` - Number of files removed from the sink(target)
- `execution_time_ms: u64` - Time taken to execute the entire operation
- `scan_time_ms: u64` - Time taken to scan the files for matches
- `rewrite_time_ms: u64` - Time taken to rewrite the matched files

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> MergeMetrics`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



## deltalake_core::operations::merge::UpdateBuilder

*Struct*

Builder for update clauses

**Methods:**

- `fn predicate<E>(self: Self, predicate: E) -> Self` - Perform the update operation when the predicate is satisfied
- `fn update<C, E>(self: Self, column: C, expression: E) -> Self` - How a column from the target table should be updated.

**Trait Implementations:**

- **Default**
  - `fn default() -> UpdateBuilder`



