**datafusion_datasource_parquet > writer**

# Module: writer

## Contents

**Functions**

- [`plan_to_parquet`](#plan_to_parquet) - Executes a query and writes the results to a partitioned Parquet file.

---

## datafusion_datasource_parquet::writer::plan_to_parquet

*Function*

Executes a query and writes the results to a partitioned Parquet file.

```rust
fn plan_to_parquet<impl AsRef<str>>(task_ctx: std::sync::Arc<datafusion_execution::TaskContext>, plan: std::sync::Arc<dyn ExecutionPlan>, path: impl Trait, writer_properties: Option<parquet::file::properties::WriterProperties>) -> datafusion_common::Result<()>
```



