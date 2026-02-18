**deltalake_core > kernel > transaction > conflict_checker**

# Module: kernel::transaction::conflict_checker

## Contents

**Enums**

- [`CommitConflictError`](#commitconflicterror) - Exceptions raised during commit conflict resolution

---

## deltalake_core::kernel::transaction::conflict_checker::CommitConflictError

*Enum*

Exceptions raised during commit conflict resolution

**Variants:**
- `ConcurrentAppend` - This exception occurs when a concurrent operation adds files in the same partition
- `ConcurrentDeleteRead` - This exception occurs when a concurrent operation deleted a file that your operation read.
- `ConcurrentDeleteDelete` - This exception occurs when a concurrent operation deleted a file that your operation also deletes.
- `MetadataChanged` - This exception occurs when a concurrent transaction updates the metadata of a Delta table.
- `ConcurrentTransaction` - If a streaming query using the same checkpoint location is started multiple times concurrently
- `ProtocolChanged(String)` - This exception can occur in the following cases:
- `UnsupportedWriterVersion(i32)` - Error returned when the table requires an unsupported writer version
- `UnsupportedReaderVersion(i32)` - Error returned when the table requires an unsupported writer version
- `CorruptedState{ source: Box<dyn std::error::Error> }` - Error returned when the snapshot has missing or corrupted data
- `Predicate{ source: Box<dyn std::error::Error> }` - Error returned when evaluating predicate
- `NoMetadata` - Error returned when no metadata was found in the DeltaTable.

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, __formatter: & mut ::core::fmt::Formatter) -> ::core::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Error**
  - `fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private18::Error>`



