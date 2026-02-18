**deltalake_core > writer > stats**

# Module: writer::stats

## Contents

**Functions**

- [`create_add`](#create_add) - Creates an [`Add`] log action struct.

---

## deltalake_core::writer::stats::create_add

*Function*

Creates an [`Add`] log action struct.

```rust
fn create_add<impl AsRef<str>>(partition_values: &indexmap::IndexMap<String, delta_kernel::expressions::Scalar>, path: String, size: i64, file_metadata: &parquet::file::metadata::ParquetMetaData, num_indexed_cols: delta_kernel::table_properties::DataSkippingNumIndexedCols, stats_columns: &Option<Vec<impl Trait>>) -> Result<crate::kernel::Add, crate::errors::DeltaTableError>
```



