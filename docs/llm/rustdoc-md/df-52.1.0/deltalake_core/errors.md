**deltalake_core > errors**

# Module: errors

## Contents

**Enums**

- [`DeltaTableError`](#deltatableerror) - Delta Table specific error

**Type Aliases**

- [`DeltaResult`](#deltaresult) - A result returned by delta-rs

---

## deltalake_core::errors::DeltaResult

*Type Alias*: `Result<T, E>`

A result returned by delta-rs



## deltalake_core::errors::DeltaTableError

*Enum*

Delta Table specific error

**Variants:**
- `KernelError(delta_kernel::error::Error)`
- `ObjectStore{ source: object_store::Error }` - Error returned when reading the delta log object failed.
- `Parquet{ source: parquet::errors::ParquetError }` - Error returned when parsing checkpoint parquet.
- `Arrow{ source: arrow::error::ArrowError }` - Error returned when converting the schema in Arrow format failed.
- `InvalidJsonLog{ json_err: serde_json::error::Error, line: String, version: i64 }` - Error returned when the log record has an invalid JSON.
- `InvalidStatsJson{ json_err: serde_json::error::Error }` - Error returned when the log contains invalid stats JSON.
- `InvalidVersion(i64)` - Error returned when the DeltaTable has an invalid version.
- `InvalidDateTimeString{ source: chrono::ParseError }` - Error returned when the datetime string is invalid for a conversion.
- `InvalidData{ message: String }` - Error returned when attempting to write bad data to the table
- `NotATable(String)` - Error returned when it is not a DeltaTable.
- `NoSchema` - Error returned when no schema was found in the DeltaTable.
- `SchemaMismatch{ msg: String }` - Error returned when writes are attempted with data that doesn't match the schema of the
- `PartitionError{ partition: String }` - Error returned when a partition is not formatted as a Hive Partition.
- `InvalidPartitionFilter{ partition_filter: String }` - Error returned when a invalid partition filter was found.
- `Io{ source: std::io::Error }` - Error returned when a line from log record is invalid.
- `CommitValidation{ source: crate::kernel::transaction::CommitBuilderError }` - Error raised while preparing a commit
- `Transaction{ source: crate::kernel::transaction::TransactionError }` - Error raised while commititng transaction
- `VersionAlreadyExists(i64)` - Error returned when transaction is failed to be committed because given version already exists.
- `VersionMismatch(i64, i64)` - Error returned when user attempts to commit actions that don't belong to the next version.
- `MissingFeature{ feature: &'static str, url: String }` - A Feature is missing to perform operation
- `InvalidTableLocation(String)` - A Feature is missing to perform operation
- `SerializeLogJson{ json_err: serde_json::error::Error }` - Generic Delta Table error
- `Generic(String)` - Generic Delta Table error
- `GenericError{ source: Box<dyn std::error::Error> }` - Generic Delta Table error
- `Kernel{ source: crate::kernel::Error }`
- `MetadataError(String)`
- `NotInitialized`
- `NotInitializedWithFiles(String)`
- `ChangeDataNotRecorded{ version: i64, start: i64, end: i64 }`
- `ChangeDataNotEnabled{ version: i64 }`
- `ChangeDataInvalidVersionRange{ start: i64, end: i64 }`
- `ChangeDataTimestampGreaterThanCommit{ ending_timestamp: chrono::DateTime<chrono::Utc> }`
- `NoStartingVersionOrTimestamp`

**Methods:**

- `fn not_a_table<impl AsRef<str>>(path: impl Trait) -> Self` - Crate a NotATable Error with message for given path.
- `fn generic<impl ToString>(msg: impl Trait) -> Self` - Create a [Generic](DeltaTableError::Generic) error with the given message.

**Trait Implementations:**

- **From**
  - `fn from(source: arrow::error::ArrowError) -> Self`
- **From**
  - `fn from(err: DataFusionError) -> Self`
- **From**
  - `fn from(source: crate::kernel::Error) -> Self`
- **Error**
  - `fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private18::Error>`
- **From**
  - `fn from(source: ObjectStoreError) -> Self`
- **From**
  - `fn from(err: TransactionError) -> Self`
- **From**
  - `fn from(source: chrono::ParseError) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, __formatter: & mut ::core::fmt::Formatter) -> ::core::fmt::Result`
- **From**
  - `fn from(err: object_store::path::Error) -> Self`
- **From**
  - `fn from(source: parquet::errors::ParquetError) -> Self`
- **From**
  - `fn from(err: CommitBuilderError) -> Self`
- **From**
  - `fn from(source: std::io::Error) -> Self`
- **From**
  - `fn from(source: delta_kernel::error::Error) -> Self`
- **From**
  - `fn from(value: serde_json::Error) -> Self`



