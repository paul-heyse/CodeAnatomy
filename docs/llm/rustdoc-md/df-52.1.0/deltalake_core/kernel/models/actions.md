**deltalake_core > kernel > models > actions**

# Module: kernel::models::actions

## Contents

**Structs**

- [`Add`](#add) - Defines an add action
- [`AddCDCFile`](#addcdcfile) - Delta AddCDCFile action that describes a parquet CDC data file.
- [`CheckpointMetadata`](#checkpointmetadata) - This action is only allowed in checkpoints following V2 spec. It describes the details about the checkpoint.
- [`CommitInfo`](#commitinfo) - The commitInfo is a fairly flexible action within the delta specification, where arbitrary data can be stored.
- [`DeletionVectorDescriptor`](#deletionvectordescriptor) - Defines a deletion vector
- [`DomainMetadata`](#domainmetadata) - The domain metadata action contains a configuration (string) for a named metadata domain
- [`Remove`](#remove) - Represents a tombstone (deleted file) in the Delta log.
- [`Sidecar`](#sidecar) - The sidecar action references a sidecar file which provides some of the checkpoint's file actions.
- [`Transaction`](#transaction) - Action used by streaming systems to track progress using application-specific versions to

**Enums**

- [`IsolationLevel`](#isolationlevel) - The isolation level applied during transaction
- [`StorageType`](#storagetype) - Storage type of deletion vector
- [`TableFeatures`](#tablefeatures) - High level table features

**Functions**

- [`contains_timestampntz`](#contains_timestampntz) - checks if table contains timestamp_ntz in any field including nested fields.
- [`new_metadata`](#new_metadata) - Please don't use, this API will be leaving shortly!

**Traits**

- [`MetadataExt`](#metadataext) - Extension trait for Metadata action

---

## deltalake_core::kernel::models::actions::Add

*Struct*

Defines an add action

**Fields:**
- `path: String` - A relative path to a data file from the root of the table or an absolute path to a file
- `partition_values: std::collections::HashMap<String, Option<String>>` - A map from partition column to value for this logical file.
- `size: i64` - The size of this data file in bytes
- `modification_time: i64` - The time this logical file was created, as milliseconds since the epoch.
- `data_change: bool` - When `false` the logical file must already be present in the table or the records
- `stats: Option<String>` - Contains [statistics] (e.g., count, min/max values for columns) about the data in this logical file.
- `tags: Option<std::collections::HashMap<String, Option<String>>>` - Map containing metadata about this logical file.
- `deletion_vector: Option<DeletionVectorDescriptor>` - Information about deletion vector (DV) associated with this add action
- `base_row_id: Option<i64>` - Default generated Row ID of the first row in the file. The default generated Row IDs
- `default_row_commit_version: Option<i64>` - First commit version in which an add action with the same path was committed to the table.
- `clustering_provider: Option<String>` - The name of the clustering implementation

**Methods:**

- `fn get_stats(self: &Self) -> Result<Option<Stats>, serde_json::error::Error>` - Get whatever stats are available. Uses (parquet struct) parsed_stats if present falling back to json stats.

**Traits:** Eq

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Add`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Default**
  - `fn default() -> Add`
- **FileAction**
  - `fn partition_values(self: &Self) -> DeltaResult<&HashMap<String, Option<String>>>`
  - `fn path(self: &Self) -> String`
  - `fn size(self: &Self) -> DeltaResult<usize>`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::kernel::models::actions::AddCDCFile

*Struct*

Delta AddCDCFile action that describes a parquet CDC data file.

**Fields:**
- `path: String` - A relative path, from the root of the table, or an
- `size: i64` - The size of this file in bytes
- `partition_values: std::collections::HashMap<String, Option<String>>` - A map from partition column to value for this file
- `data_change: bool` - Should always be set to false because they do not change the underlying data of the table
- `tags: Option<std::collections::HashMap<String, Option<String>>>` - Map containing metadata about this file

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> AddCDCFile`
- **PartialEq**
  - `fn eq(self: &Self, other: &AddCDCFile) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> AddCDCFile`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **FileAction**
  - `fn partition_values(self: &Self) -> DeltaResult<&HashMap<String, Option<String>>>`
  - `fn path(self: &Self) -> String`
  - `fn size(self: &Self) -> DeltaResult<usize>`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`



## deltalake_core::kernel::models::actions::CheckpointMetadata

*Struct*

This action is only allowed in checkpoints following V2 spec. It describes the details about the checkpoint.

**Fields:**
- `flavor: String` - The flavor of the V2 checkpoint. Allowed values: "flat".
- `tags: Option<std::collections::HashMap<String, Option<String>>>` - Map containing any additional metadata about the v2 spec checkpoint.

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> CheckpointMetadata`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Default**
  - `fn default() -> CheckpointMetadata`
- **PartialEq**
  - `fn eq(self: &Self, other: &CheckpointMetadata) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



## deltalake_core::kernel::models::actions::CommitInfo

*Struct*

The commitInfo is a fairly flexible action within the delta specification, where arbitrary data can be stored.
However the reference implementation as well as delta-rs store useful information that may for instance
allow us to be more permissive in commit conflict resolution.

**Fields:**
- `timestamp: Option<i64>` - Timestamp in millis when the commit was created
- `user_id: Option<String>` - Id of the user invoking the commit
- `user_name: Option<String>` - Name of the user invoking the commit
- `operation: Option<String>` - The operation performed during the
- `operation_parameters: Option<std::collections::HashMap<String, serde_json::Value>>` - Parameters used for table operation
- `read_version: Option<i64>` - Version of the table when the operation was started
- `isolation_level: Option<IsolationLevel>` - The isolation level of the commit
- `is_blind_append: Option<bool>` - TODO
- `engine_info: Option<String>` - Delta engine which created the commit.
- `info: std::collections::HashMap<String, serde_json::Value>` - Additional provenance information for the commit
- `user_metadata: Option<String>` - User defined metadata

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> CommitInfo`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Default**
  - `fn default() -> CommitInfo`
- **PartialEq**
  - `fn eq(self: &Self, other: &CommitInfo) -> bool`



## deltalake_core::kernel::models::actions::DeletionVectorDescriptor

*Struct*

Defines a deletion vector

**Fields:**
- `storage_type: StorageType` - A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'].
- `path_or_inline_dv: String` - Three format options are currently proposed:
- `offset: Option<i32>` - Start of the data for this DV in number of bytes from the beginning of the file it is stored in.
- `size_in_bytes: i32` - Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
- `cardinality: i64` - Number of rows the given DV logically removes from the file.

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DeletionVectorDescriptor`
- **PartialEq**
  - `fn eq(self: &Self, other: &DeletionVectorDescriptor) -> bool`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



## deltalake_core::kernel::models::actions::DomainMetadata

*Struct*

The domain metadata action contains a configuration (string) for a named metadata domain

**Fields:**
- `domain: String` - Identifier for this domain (system or user-provided)
- `configuration: String` - String containing configuration for the metadata domain
- `removed: bool` - When `true` the action serves as a tombstone

**Traits:** Eq

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> DomainMetadata`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Default**
  - `fn default() -> DomainMetadata`
- **PartialEq**
  - `fn eq(self: &Self, other: &DomainMetadata) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::kernel::models::actions::IsolationLevel

*Enum*

The isolation level applied during transaction

**Variants:**
- `Serializable` - The strongest isolation level. It ensures that committed write operations
- `WriteSerializable` - A weaker isolation level than Serializable. It ensures only that the write
- `SnapshotIsolation` - SnapshotIsolation is a guarantee that all reads made in a transaction will see a consistent

**Traits:** Eq, Copy

**Trait Implementations:**

- **Default**
  - `fn default() -> IsolationLevel`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> IsolationLevel`
- **PartialEq**
  - `fn eq(self: &Self, other: &IsolationLevel) -> bool`
- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`



## deltalake_core::kernel::models::actions::MetadataExt

*Trait*

Extension trait for Metadata action

This trait is a stop-gap to adopt the Metadata action from delta-kernel-rs
while the update / mutation APIs are being implemented. It allows us to implement
additional APIs on the Metadata action and hide specifics of how we do the updates.

**Methods:**

- `with_table_id`
- `with_name`
- `with_description`
- `with_schema`
- `add_config_key`
- `remove_config_key`



## deltalake_core::kernel::models::actions::Remove

*Struct*

Represents a tombstone (deleted file) in the Delta log.

**Fields:**
- `path: String` - A relative path to a data file from the root of the table or an absolute path to a file
- `data_change: bool` - When `false` the logical file must already be present in the table or the records
- `deletion_timestamp: Option<i64>` - The time this logical file was created, as milliseconds since the epoch.
- `extended_file_metadata: Option<bool>` - When true the fields `partition_values`, `size`, and `tags` are present
- `partition_values: Option<std::collections::HashMap<String, Option<String>>>` - A map from partition column to value for this logical file.
- `size: Option<i64>` - The size of this data file in bytes
- `tags: Option<std::collections::HashMap<String, Option<String>>>` - Map containing metadata about this logical file.
- `deletion_vector: Option<DeletionVectorDescriptor>` - Information about deletion vector (DV) associated with this add action
- `base_row_id: Option<i64>` - Default generated Row ID of the first row in the file. The default generated Row IDs
- `default_row_commit_version: Option<i64>` - First commit version in which an add action with the same path was committed to the table.

**Traits:** Eq

**Trait Implementations:**

- **Borrow**
  - `fn borrow(self: &Self) -> &str`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **FileAction**
  - `fn partition_values(self: &Self) -> DeltaResult<&HashMap<String, Option<String>>>`
  - `fn path(self: &Self) -> String`
  - `fn size(self: &Self) -> DeltaResult<usize>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> Remove`
- **Default**
  - `fn default() -> Remove`



## deltalake_core::kernel::models::actions::Sidecar

*Struct*

The sidecar action references a sidecar file which provides some of the checkpoint's file actions.
This action is only allowed in checkpoints following V2 spec.

**Fields:**
- `file_name: String` - The name of the sidecar file (not a path).
- `size_in_bytes: i64` - The size of the sidecar file in bytes
- `modification_time: i64` - The time this sidecar file was created, as milliseconds since the epoch.
- `sidecar_type: String` - Type of sidecar. Valid values are: "fileaction".
- `tags: Option<std::collections::HashMap<String, Option<String>>>` - Map containing any additional metadata about the checkpoint sidecar file.

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Sidecar`
- **PartialEq**
  - `fn eq(self: &Self, other: &Sidecar) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> Sidecar`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`



## deltalake_core::kernel::models::actions::StorageType

*Enum*

Storage type of deletion vector

**Variants:**
- `UuidRelativePath` - Stored at relative path derived from a UUID.
- `Inline` - Stored as inline string.
- `AbsolutePath` - Stored at an absolute path.

**Traits:** Copy, Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &StorageType) -> bool`
- **Default**
  - `fn default() -> StorageType`
- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> StorageType`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::kernel::models::actions::TableFeatures

*Enum*

High level table features

**Variants:**
- `ColumnMapping` - Mapping of one column to another
- `DeletionVectors` - Deletion vectors for merge, update, delete
- `TimestampWithoutTimezone` - timestamps without timezone support
- `V2Checkpoint` - version 2 of checkpointing
- `AppendOnly` - Append Only Tables
- `Invariants` - Table invariants
- `CheckConstraints` - Check constraints on columns
- `ChangeDataFeed` - CDF on a table
- `GeneratedColumns` - Columns with generated values
- `IdentityColumns` - ID Columns
- `RowTracking` - Row tracking on tables
- `DomainMetadata` - domain specific metadata
- `IcebergCompatV1` - Iceberg compatibility support
- `MaterializePartitionColumns`

**Methods:**

- `fn to_reader_writer_features(self: &Self) -> (Option<TableFeature>, Option<TableFeature>)` - Convert table feature to respective reader or/and write feature

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **Clone**
  - `fn clone(self: &Self) -> TableFeatures`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &TableFeatures) -> bool`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **FromStr**
  - `fn from_str(value: &str) -> Result<Self, <Self as >::Err>`



## deltalake_core::kernel::models::actions::Transaction

*Struct*

Action used by streaming systems to track progress using application-specific versions to
enable idempotency.

**Fields:**
- `app_id: String` - A unique identifier for the application performing the transaction.
- `version: i64` - An application-specific numeric identifier for this transaction.
- `last_updated: Option<i64>` - The time when this transaction action was created in milliseconds since the Unix epoch.

**Methods:**

- `fn new<impl ToString>(app_id: impl Trait, version: i64) -> Self` - Create a new application transactions. See [`Txn`] for details.
- `fn new_with_last_update<impl ToString>(app_id: impl Trait, version: i64, last_updated: Option<i64>) -> Self` - Create a new application transactions. See [`Txn`] for details.

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Transaction`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> Transaction`
- **PartialEq**
  - `fn eq(self: &Self, other: &Transaction) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



## deltalake_core::kernel::models::actions::contains_timestampntz

*Function*

checks if table contains timestamp_ntz in any field including nested fields.

```rust
fn contains_timestampntz<'a, impl Iterator<Item = &'a StructField>>(fields: impl Trait) -> bool
```



## deltalake_core::kernel::models::actions::new_metadata

*Function*

Please don't use, this API will be leaving shortly!

Since the adoption of delta-kernel-rs we lost the direct ability to create [Metadata] actions
which is required for some use-cases.

Upstream tracked here: <https://github.com/delta-io/delta-kernel-rs/issues/1055>

```rust
fn new_metadata<impl ToString, impl IntoIterator<Item = impl ToString>, impl ToString, impl ToString, impl IntoIterator<Item = (impl ToString, impl ToString)>>(schema: &crate::kernel::StructType, partition_columns: impl Trait, configuration: impl Trait) -> crate::kernel::DeltaResult<Metadata>
```



