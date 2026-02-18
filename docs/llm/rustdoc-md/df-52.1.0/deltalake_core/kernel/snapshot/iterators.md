**deltalake_core > kernel > snapshot > iterators**

# Module: kernel::snapshot::iterators

## Contents

**Structs**

- [`LogicalFileView`](#logicalfileview) - Provides semantic, typed access to file metadata from Delta log replay.

---

## deltalake_core::kernel::snapshot::iterators::LogicalFileView

*Struct*

Provides semantic, typed access to file metadata from Delta log replay.

This struct wraps a RecordBatch containing file data and provides zero-copy
access to individual file entries through an index. It serves as a view into
the kernel's log replay results, offering convenient methods to extract
file properties without unnecessary data copies.

**Methods:**

- `fn path(self: &Self) -> Cow<str>` - Returns the file path with URL decoding applied.
- `fn size(self: &Self) -> i64` - Returns the file size in bytes.
- `fn modification_time(self: &Self) -> i64` - Returns the file modification time in milliseconds since Unix epoch.
- `fn modification_datetime(self: &Self) -> DeltaResult<chrono::DateTime<Utc>>` - Returns the file modification time as a UTC DateTime.
- `fn stats(self: &Self) -> Option<String>` - Returns the raw JSON statistics string for this file, if available.
- `fn partition_values(self: &Self) -> Option<StructData>` - Returns the parsed partition values as structured data.
- `fn num_records(self: &Self) -> Option<usize>` - Returns the number of records in this file.
- `fn null_counts(self: &Self) -> Option<Scalar>` - Returns null counts for all columns in this file as structured data.
- `fn min_values(self: &Self) -> Option<Scalar>` - Returns minimum values for all columns with statics in this file as structured data.
- `fn max_values(self: &Self) -> Option<Scalar>` - Returns maximum values for all columns in this file as structured data.
- `fn deletion_vector_descriptor(self: &Self) -> Option<DeletionVectorDescriptor>` - Return the underlying [DeletionVectorDescriptor] if it exists.
- `fn remove_action(self: &Self, data_change: bool) -> Remove` - Converts this file view into a Remove action for log operations.

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> LogicalFileView`



