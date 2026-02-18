**datafusion_datasource > table_schema**

# Module: table_schema

## Contents

**Structs**

- [`TableSchema`](#tableschema) - Helper to hold table schema information for partitioned data sources.

---

## datafusion_datasource::table_schema::TableSchema

*Struct*

Helper to hold table schema information for partitioned data sources.

When reading partitioned data (such as Hive-style partitioning), a table's schema
consists of two parts:
1. **File schema**: The schema of the actual data files on disk
2. **Partition columns**: Columns that are encoded in the directory structure,
   not stored in the files themselves

# Example: Partitioned Table

Consider a table with the following directory structure:
```text
/data/date=2025-10-10/region=us-west/data.parquet
/data/date=2025-10-11/region=us-east/data.parquet
```

In this case:
- **File schema**: The schema of `data.parquet` files (e.g., `[user_id, amount]`)
- **Partition columns**: `[date, region]` extracted from the directory path
- **Table schema**: The full schema combining both (e.g., `[user_id, amount, date, region]`)

# When to Use

Use `TableSchema` when:
- Reading partitioned data sources (Parquet, CSV, etc. with Hive-style partitioning)
- You need to efficiently access different schema representations without reconstructing them
- You want to avoid repeatedly concatenating file and partition schemas

For non-partitioned data or when working with a single schema representation,
working directly with Arrow's `Schema` or `SchemaRef` is simpler.

# Performance

This struct pre-computes and caches the full table schema, allowing cheap references
to any representation without repeated allocations or reconstructions.

**Methods:**

- `fn new(file_schema: SchemaRef, table_partition_cols: Vec<FieldRef>) -> Self` - Create a new TableSchema from a file schema and partition columns.
- `fn from_file_schema(file_schema: SchemaRef) -> Self` - Create a new TableSchema with no partition columns.
- `fn with_table_partition_cols(self: Self, partition_cols: Vec<FieldRef>) -> Self` - Add partition columns to an existing TableSchema, returning a new instance.
- `fn file_schema(self: &Self) -> &SchemaRef` - Get the file schema (without partition columns).
- `fn table_partition_cols(self: &Self) -> &Vec<FieldRef>` - Get the table partition columns.
- `fn table_schema(self: &Self) -> &SchemaRef` - Get the full table schema (file schema + partition columns).

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> TableSchema`
- **From**
  - `fn from(schema: SchemaRef) -> Self`



