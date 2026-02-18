**datafusion_tracing > preview_utils**

# Module: preview_utils

## Contents

**Functions**

- [`pretty_format_compact_batch`](#pretty_format_compact_batch) - Formats a `RecordBatch` as a neatly aligned ASCII table,

---

## datafusion_tracing::preview_utils::pretty_format_compact_batch

*Function*

Formats a `RecordBatch` as a neatly aligned ASCII table,
constraining the total width to `max_width`. Columns are
dynamically resized or truncated, and columns that cannot
fit within the given width may be dropped.

```rust
fn pretty_format_compact_batch(batch: &datafusion::arrow::array::RecordBatch, max_width: usize, max_row_height: usize, min_compacted_col_width: usize) -> Result<impl Trait, datafusion::arrow::error::ArrowError>
```



