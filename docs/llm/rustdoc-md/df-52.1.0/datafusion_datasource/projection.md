**datafusion_datasource > projection**

# Module: projection

## Contents

**Structs**

- [`PartitionColumnIndex`](#partitioncolumnindex)
- [`ProjectionOpener`](#projectionopener) - A file opener that handles applying a projection on top of an inner opener.
- [`SplitProjection`](#splitprojection) - At a high level the goal of SplitProjection is to take a ProjectionExprs meant to be applied to the table schema

---

## datafusion_datasource::projection::PartitionColumnIndex

*Struct*

**Fields:**
- `in_remainder_projection: usize` - The index of this partition column in the remainder projection (>= num_file_columns)
- `in_partition_values: usize` - The index of this partition column in the partition_values array

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> PartitionColumnIndex`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_datasource::projection::ProjectionOpener

*Struct*

A file opener that handles applying a projection on top of an inner opener.

This includes handling partition columns.

Any projection pushed down will be split up into:
- Simple column indices / column selection
- A remainder projection that this opener applies on top of it

This is meant to simplify projection pushdown for sources like CSV
that can only handle "simple" column selection.

**Methods:**

- `fn try_new(projection: SplitProjection, inner: Arc<dyn FileOpener>, file_schema: &Schema) -> Result<Arc<dyn FileOpener>>`

**Trait Implementations:**

- **FileOpener**
  - `fn open(self: &Self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture>`



## datafusion_datasource::projection::SplitProjection

*Struct*

At a high level the goal of SplitProjection is to take a ProjectionExprs meant to be applied to the table schema
and split that into:
- The projection indices into the file schema (file_indices)
- The projection indices into the partition values (partition_value_indices), which pre-compute both the index into the table schema
  and the index into the partition values array
- A remapped projection that can be applied after the file projection is applied
  This remapped projection has the following properties:
    - Column indices referring to file columns are remapped to [0..file_indices.len())
    - Column indices referring to partition columns are remapped to [file_indices.len()..)

  This allows the ProjectionOpener to easily identify which columns in the remapped projection
  refer to partition columns and substitute them with literals from the partition values.

**Fields:**
- `source: datafusion_physical_expr::projection::ProjectionExprs` - The original projection this [`SplitProjection`] was derived from
- `file_indices: Vec<usize>` - Column indices to read from file (public for file sources)

**Methods:**

- `fn unprojected(table_schema: &TableSchema) -> Self`
- `fn new(logical_file_schema: &Schema, projection: &ProjectionExprs) -> Self` - Creates a new [`SplitProjection`] by splitting a projection into

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> SplitProjection`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



