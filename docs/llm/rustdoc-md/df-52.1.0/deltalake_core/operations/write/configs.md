**deltalake_core > operations > write > configs**

# Module: operations::write::configs

## Contents

**Structs**

- [`WriterStatsConfig`](#writerstatsconfig) - Configuration for the writer on how to collect stats

---

## deltalake_core::operations::write::configs::WriterStatsConfig

*Struct*

Configuration for the writer on how to collect stats

**Fields:**
- `num_indexed_cols: delta_kernel::table_properties::DataSkippingNumIndexedCols` - Number of columns to collect stats for, idx based
- `stats_columns: Option<Vec<String>>` - Optional list of columns which to collect stats for, takes precedende over num_index_cols

**Methods:**

- `fn new(num_indexed_cols: DataSkippingNumIndexedCols, stats_columns: Option<Vec<String>>) -> Self` - Create new writer stats config
- `fn from_config(config: &TableConfiguration) -> Self`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> WriterStatsConfig`



