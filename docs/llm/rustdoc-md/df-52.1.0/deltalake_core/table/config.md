**deltalake_core > table > config**

# Module: table::config

## Contents

**Enums**

- [`DeltaConfigError`](#deltaconfigerror) - Delta configuration error
- [`TableProperty`](#tableproperty) - Typed property keys that can be defined on a delta table

**Traits**

- [`TablePropertiesExt`](#tablepropertiesext)

**Constants**

- [`DEFAULT_NUM_INDEX_COLS`](#default_num_index_cols) - Default num index cols
- [`DEFAULT_TARGET_FILE_SIZE`](#default_target_file_size) - Default target file size

---

## deltalake_core::table::config::DEFAULT_NUM_INDEX_COLS

*Constant*: `u64`

Default num index cols



## deltalake_core::table::config::DEFAULT_TARGET_FILE_SIZE

*Constant*: `u64`

Default target file size



## deltalake_core::table::config::DeltaConfigError

*Enum*

Delta configuration error

**Variants:**
- `Validation(String)` - Error returned when configuration validation failed.

**Traits:** Eq, Error

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, __formatter: & mut ::core::fmt::Formatter) -> ::core::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &DeltaConfigError) -> bool`



## deltalake_core::table::config::TablePropertiesExt

*Trait*

**Methods:**

- `append_only`: true for this Delta table to be append-only. If append-only, existing records cannot be
- `log_retention_duration`: How long the history for a Delta table is kept.
- `enable_expired_log_cleanup`: Whether to clean up expired checkpoints/commits in the delta log.
- `checkpoint_interval`: Interval (expressed as number of commits) after which a new checkpoint should be created.
- `num_indexed_cols`: Number of columns to be indexed.
- `target_file_size`
- `enable_change_data_feed`
- `deleted_file_retention_duration`
- `isolation_level`
- `get_constraints`



## deltalake_core::table::config::TableProperty

*Enum*

Typed property keys that can be defined on a delta table

<https://docs.delta.io/latest/table-properties.html#delta-table-properties-reference>
<https://learn.microsoft.com/en-us/azure/databricks/delta/table-properties>

**Variants:**
- `AppendOnly` - true for this Delta table to be append-only. If append-only,
- `AutoOptimizeAutoCompact` - true for Delta Lake to automatically optimize the layout of the files for this Delta table.
- `AutoOptimizeOptimizeWrite` - true for Delta Lake to automatically optimize the layout of the files for this Delta table during writes.
- `CheckpointInterval` - Interval (number of commits) after which a new checkpoint should be created
- `CheckpointWriteStatsAsJson` - true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.
- `CheckpointWriteStatsAsStruct` - true for Delta Lake to write file statistics to checkpoints in struct format for the
- `CheckpointUseRunLengthEncoding` - true for Delta Lake to write checkpoint files using run length encoding (RLE).
- `ColumnMappingMode` - Whether column mapping is enabled for Delta table columns and the corresponding
- `DataSkippingNumIndexedCols` - The number of columns for Delta Lake to collect statistics about for data skipping.
- `DataSkippingStatsColumns` - A comma-separated list of column names on which Delta Lake collects statistics to enhance
- `DeletedFileRetentionDuration` - The shortest duration for Delta Lake to keep logically deleted data files before deleting
- `EnableChangeDataFeed` - true to enable change data feed.
- `EnableDeletionVectors` - true to enable deletion vectors and predictive I/O for updates.
- `IsolationLevel` - The degree to which a transaction must be isolated from modifications made by concurrent transactions.
- `LogRetentionDuration` - How long the history for a Delta table is kept.
- `EnableExpiredLogCleanup` - TODO I could not find this property in the documentation, but was defined here and makes sense..?
- `MinReaderVersion` - The minimum required protocol reader version for a reader that allows to read from this Delta table.
- `MinWriterVersion` - The minimum required protocol writer version for a writer that allows to write to this Delta table.
- `RandomizeFilePrefixes` - true for Delta Lake to generate a random prefix for a file path instead of partition information.
- `RandomPrefixLength` - When delta.randomizeFilePrefixes is set to true, the number of characters that Delta Lake generates for random prefixes.
- `SetTransactionRetentionDuration` - The shortest duration within which new snapshots will retain transaction identifiers (for example, SetTransactions).
- `TargetFileSize` - The target file size in bytes or higher units for file tuning. For example, 104857600 (bytes) or 100mb.
- `TuneFileSizesForRewrites` - The target file size in bytes or higher units for file tuning. For example, 104857600 (bytes) or 100mb.
- `CheckpointPolicy` - 'classic' for classic Delta Lake checkpoints. 'v2' for v2 checkpoints.

**Traits:** Eq

**Trait Implementations:**

- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **PartialEq**
  - `fn eq(self: &Self, other: &TableProperty) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



