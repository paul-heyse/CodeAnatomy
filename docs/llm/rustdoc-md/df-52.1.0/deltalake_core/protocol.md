**deltalake_core > protocol**

# Module: protocol

## Contents

**Modules**

- [`checkpoints`](#checkpoints) - Implementation for writing delta checkpoints.

**Structs**

- [`MergePredicate`](#mergepredicate) - Used to record the operations performed to the Delta Log
- [`Stats`](#stats) - Statistics associated with Add actions contained in the Delta log.
- [`StatsParsed`](#statsparsed) - File stats parsed from raw parquet format.

**Enums**

- [`ColumnCountStat`](#columncountstat) - Struct used to represent nullCount in add action statistics.
- [`ColumnValueStat`](#columnvaluestat) - Struct used to represent minValues and maxValues in add action statistics.
- [`DeltaOperation`](#deltaoperation) - Operation performed when creating a new log entry with one or more actions.
- [`OutputMode`](#outputmode) - The OutputMode used in streaming operations.
- [`SaveMode`](#savemode) - The SaveMode used when performing a DeltaOperation

---

## deltalake_core::protocol::ColumnCountStat

*Enum*

Struct used to represent nullCount in add action statistics.

**Variants:**
- `Column(std::collections::HashMap<String, ColumnCountStat>)` - Composite HashMap representation of statistics.
- `Value(i64)` - Json representation of statistics.

**Methods:**

- `fn as_column(self: &Self) -> Option<&HashMap<String, ColumnCountStat>>` - Returns the HashMap representation of the ColumnCountStat.
- `fn as_value(self: &Self) -> Option<i64>` - Returns the serde_json representation of the ColumnCountStat.

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &ColumnCountStat) -> bool`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::protocol::ColumnValueStat

*Enum*

Struct used to represent minValues and maxValues in add action statistics.

**Variants:**
- `Column(std::collections::HashMap<String, ColumnValueStat>)` - Composite HashMap representation of statistics.
- `Value(serde_json::Value)` - Json representation of statistics.

**Methods:**

- `fn as_column(self: &Self) -> Option<&HashMap<String, ColumnValueStat>>` - Returns the HashMap representation of the ColumnValueStat.
- `fn as_value(self: &Self) -> Option<&Value>` - Returns the serde_json representation of the ColumnValueStat.

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ColumnValueStat) -> bool`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`



## deltalake_core::protocol::DeltaOperation

*Enum*

Operation performed when creating a new log entry with one or more actions.
This is a key element of the `CommitInfo` action.

**Variants:**
- `AddColumn{ fields: Vec<crate::kernel::StructField> }` - Represents a Delta `Add Column` operation.
- `Create{ mode: SaveMode, location: url::Url, protocol: crate::kernel::Protocol, metadata: crate::kernel::Metadata }` - Represents a Delta `Create` operation.
- `Write{ mode: SaveMode, partition_by: Option<Vec<String>>, predicate: Option<String> }` - Represents a Delta `Write` operation.
- `Delete{ predicate: Option<String> }` - Delete data matching predicate from delta table
- `Update{ predicate: Option<String> }` - Update data matching predicate from delta table
- `AddConstraint{ constraints: Vec<crate::table::Constraint> }` - Add constraints to a table
- `AddFeature{ name: Vec<crate::kernel::TableFeatures> }` - Add table features to a table
- `DropConstraint{ name: String }` - Drops constraints from a table
- `Merge{ predicate: Option<String>, merge_predicate: Option<String>, matched_predicates: Vec<MergePredicate>, not_matched_predicates: Vec<MergePredicate>, not_matched_by_source_predicates: Vec<MergePredicate> }` - Merge data with a source data with the following predicate
- `StreamingUpdate{ output_mode: OutputMode, query_id: String, epoch_id: i64 }` - Represents a Delta `StreamingUpdate` operation.
- `SetTableProperties{ properties: std::collections::HashMap<String, String> }` - Set table properties operations
- `Optimize{ predicate: Option<String>, target_size: i64 }` - Represents a `Optimize` operation
- `FileSystemCheck{  }` - Represents a `FileSystemCheck` operation
- `Restore{ version: Option<i64>, datetime: Option<i64> }` - Represents a `Restore` operation
- `VacuumStart{ retention_check_enabled: bool, specified_retention_millis: Option<i64>, default_retention_millis: i64 }` - Represents the start of `Vacuum` operation
- `VacuumEnd{ status: String }` - Represents the end of `Vacuum` operation
- `UpdateFieldMetadata{ fields: Vec<crate::kernel::StructField> }` - Set table field metadata operations
- `UpdateTableMetadata{ metadata_update: crate::operations::update_table_metadata::TableMetadataUpdate }` - Update table metadata operations

**Methods:**

- `fn name(self: &Self) -> &str` - A human readable name for the operation
- `fn operation_parameters(self: &Self) -> DeltaResult<HashMap<String, Value>>` - Parameters configured for operation.
- `fn changes_data(self: &Self) -> bool` - Denotes if the operation changes the data contained in the table
- `fn get_commit_info(self: &Self) -> CommitInfo` - Retrieve basic commit information to be added to Delta commits
- `fn read_predicate(self: &Self) -> Option<String>` - Get predicate expression applied when the operation reads data from the table.
- `fn read_whole_table(self: &Self) -> bool` - Denotes if the operation reads the entire table

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> DeltaOperation`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::protocol::MergePredicate

*Struct*

Used to record the operations performed to the Delta Log

**Fields:**
- `action_type: String` - The type of merge operation performed
- `predicate: Option<String>` - The predicate used for the merge operation

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> MergePredicate`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`



## deltalake_core::protocol::OutputMode

*Enum*

The OutputMode used in streaming operations.

**Variants:**
- `Append` - Only new rows will be written when new data is available.
- `Complete` - The full output (all rows) will be written whenever new data is available.
- `Update` - Only rows with updates will be written when new or changed data is available.

**Traits:** Copy

**Trait Implementations:**

- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> OutputMode`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



## deltalake_core::protocol::SaveMode

*Enum*

The SaveMode used when performing a DeltaOperation

**Variants:**
- `Append` - Files will be appended to the target location.
- `Overwrite` - The target location will be overwritten.
- `ErrorIfExists` - If files exist for the target, the operation must fail.
- `Ignore` - If files exist for the target, the operation must not proceed or change any data.

**Traits:** Copy, Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> SaveMode`
- **PartialEq**
  - `fn eq(self: &Self, other: &SaveMode) -> bool`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **FromStr**
  - `fn from_str(s: &str) -> DeltaResult<Self>`



## deltalake_core::protocol::Stats

*Struct*

Statistics associated with Add actions contained in the Delta log.

**Fields:**
- `num_records: i64` - Number of records in the file associated with the log action.
- `min_values: std::collections::HashMap<String, ColumnValueStat>` - Contains a value smaller than all values present in the file for all columns.
- `max_values: std::collections::HashMap<String, ColumnValueStat>` - Contains a value larger than all values present in the file for all columns.
- `null_count: std::collections::HashMap<String, ColumnCountStat>` - The number of null values for all columns.

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Default**
  - `fn default() -> Stats`
- **PartialEq**
  - `fn eq(self: &Self, other: &Stats) -> bool`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`



## deltalake_core::protocol::StatsParsed

*Struct*

File stats parsed from raw parquet format.

**Fields:**
- `num_records: i64` - Number of records in the file associated with the log action.
- `min_values: std::collections::HashMap<String, parquet::record::Field>` - Contains a value smaller than all values present in the file for all columns.
- `max_values: std::collections::HashMap<String, parquet::record::Field>` - Contains a value larger than all values present in the file for all columns.
- `null_count: std::collections::HashMap<String, i64>` - The number of null values for all columns.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> StatsParsed`



## Module: checkpoints

Implementation for writing delta checkpoints.



