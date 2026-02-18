**deltalake_core > writer**

# Module: writer

## Contents

**Modules**

- [`json`](#json) - Main writer API to write json messages to delta table
- [`record_batch`](#record_batch) - Main writer API to write record batches to Delta Table
- [`utils`](#utils) - Handle JSON messages when writing to delta tables

**Enums**

- [`WriteMode`](#writemode) - Write mode for the [DeltaWriter]

**Traits**

- [`DeltaWriter`](#deltawriter) - Trait for writing data to Delta tables

---

## deltalake_core::writer::DeltaWriter

*Trait*

Trait for writing data to Delta tables

**Methods:**

- `write`: Write a chunk of values into the internal write buffers with the default write mode
- `write_with_mode`: Wreite a chunk of values into the internal write buffers with the specified [WriteMode]
- `flush`: Flush the internal write buffers to files in the delta table folder structure.
- `flush_and_commit`: Flush the internal write buffers to files in the delta table folder structure.



## deltalake_core::writer::WriteMode

*Enum*

Write mode for the [DeltaWriter]

**Variants:**
- `Default` - Default write mode which will return an error if schemas do not match correctly
- `MergeSchema` - Merge the schema of the table with the newly written data

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &WriteMode) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> WriteMode`



## Module: json

Main writer API to write json messages to delta table



## Module: record_batch

Main writer API to write record batches to Delta Table

Writes Arrow record batches to a Delta Table, handling partitioning and file statistics.
Each Parquet file is buffered in-memory and only written once `flush()` is called on
the writer. Once written, add actions are returned by the writer. It's the users responsibility
to create the transaction using those actions.



## Module: utils

Handle JSON messages when writing to delta tables




