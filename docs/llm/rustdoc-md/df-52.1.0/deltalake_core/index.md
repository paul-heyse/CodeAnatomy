# deltalake_core

Native Delta Lake implementation in Rust

# Usage

Load a Delta Table by URL:

```rust
# use url::Url;
async {
  let table_url = Url::from_directory_path("/abs/test/tests/data/simple_table").unwrap();
  let table = deltalake_core::open_table(table_url).await.unwrap();
  let version = table.version();
};
```

Load a specific version of Delta Table by URL then filter files by partitions:

```rust
# use url::Url;
async {
  let table_url = Url::from_directory_path("/abs/test/tests/data/simple_table").unwrap();
  let table = deltalake_core::open_table_with_version(table_url, 0).await.unwrap();
  let filter = [deltalake_core::PartitionFilter {
      key: "month".to_string(),
      value: deltalake_core::PartitionValue::Equal("12".to_string()),
  }];
  let files = table.get_files_by_partitions(&filter).await.unwrap();
};
```

Load a specific version of Delta Table by URL and datetime:

```rust
# use url::Url;
async {
  let table_url = Url::from_directory_path("../test/tests/data/simple_table").unwrap();
  let table = deltalake_core::open_table_with_ds(
      table_url,
      "2020-05-02T23:47:31-07:00",
  ).await.unwrap();
  let version = table.version();
};
```

# Optional cargo package features

- `s3`, `gcs`, `azure` - enable the storage backends for AWS S3, Google Cloud Storage (GCS),
  or Azure Blob Storage / Azure Data Lake Storage Gen2 (ADLS2). Use `s3-native-tls` to use native TLS
  instead of Rust TLS implementation.
- `datafusion` - enable the `datafusion::datasource::TableProvider` trait implementation
  for Delta Tables, allowing them to be queried using [DataFusion](https://github.com/apache/arrow-datafusion).
- `datafusion-ext` - DEPRECATED: alias for `datafusion` feature.

# Querying Delta Tables with Datafusion

Querying from local filesystem:
```
use std::sync::Arc;
# use url::Url;

# #[cfg(feature="datafusion")]
async {
  use datafusion::prelude::SessionContext;
  let mut ctx = SessionContext::new();
  let table_url = Url::from_directory_path("/abs/test/tests/data/simple_table").unwrap();
  let table = deltalake_core::open_table(table_url)
      .await
      .unwrap();
  ctx.register_table("demo", table.table_provider().await.unwrap()).unwrap();

  let batches = ctx
      .sql("SELECT * FROM demo").await.unwrap()
      .collect()
      .await.unwrap();
};
```

## Modules

### [`deltalake_core`](deltalake_core.md)

*6 functions, 9 modules*

### [`data_catalog`](data_catalog.md)

*1 enum, 1 module, 1 trait, 1 type alias*

### [`data_catalog::storage`](data_catalog/storage.md)

*1 struct*

### [`delta_datafusion`](delta_datafusion.md)

*1 trait, 4 structs, 6 modules*

### [`delta_datafusion::cdf`](delta_datafusion/cdf.md)

*1 module, 1 trait, 3 constants*

### [`delta_datafusion::cdf::scan`](delta_datafusion/cdf/scan.md)

*1 struct*

### [`delta_datafusion::engine`](delta_datafusion/engine.md)

*1 struct*

### [`delta_datafusion::engine::storage`](delta_datafusion/engine/storage.md)

*1 trait*

### [`delta_datafusion::expr`](delta_datafusion/expr.md)

*1 constant, 2 functions*

### [`delta_datafusion::planner`](delta_datafusion/planner.md)

*2 structs*

### [`delta_datafusion::session`](delta_datafusion/session.md)

*1 enum, 1 function, 4 structs*

### [`delta_datafusion::table_provider`](delta_datafusion/table_provider.md)

*5 structs*

### [`delta_datafusion::table_provider::next`](delta_datafusion/table_provider/next.md)

*1 enum, 2 structs*

### [`delta_datafusion::table_provider::next::scan::exec`](delta_datafusion/table_provider/next/scan/exec.md)

*1 struct*

### [`delta_datafusion::utils`](delta_datafusion/utils.md)

*1 enum*

### [`errors`](errors.md)

*1 enum, 1 type alias*

### [`kernel`](kernel.md)

*6 modules*

### [`kernel::arrow::engine_ext`](kernel/arrow/engine_ext.md)

*1 trait*

### [`kernel::error`](kernel/error.md)

*1 enum, 1 type alias*

### [`kernel::models`](kernel/models.md)

*1 enum*

### [`kernel::models::actions`](kernel/models/actions.md)

*1 trait, 2 functions, 3 enums, 9 structs*

### [`kernel::scalars`](kernel/scalars.md)

*1 trait*

### [`kernel::schema`](kernel/schema.md)

*1 trait, 2 modules*

### [`kernel::schema::cast`](kernel/schema/cast.md)

*1 function*

### [`kernel::schema::partitions`](kernel/schema/partitions.md)

*1 constant, 1 enum, 2 structs*

### [`kernel::schema::schema`](kernel/schema/schema.md)

*1 struct, 1 trait, 2 type aliases*

### [`kernel::snapshot`](kernel/snapshot.md)

*2 structs*

### [`kernel::snapshot::iterators`](kernel/snapshot/iterators.md)

*1 struct*

### [`kernel::snapshot::iterators::tombstones`](kernel/snapshot/iterators/tombstones.md)

*1 struct*

### [`kernel::snapshot::log_data`](kernel/snapshot/log_data.md)

*1 struct*

### [`kernel::snapshot::scan`](kernel/snapshot/scan.md)

*1 type alias, 2 structs*

### [`kernel::snapshot::stream`](kernel/snapshot/stream.md)

*1 trait, 2 type aliases*

### [`kernel::transaction`](kernel/transaction.md)

*1 trait, 11 structs, 2 enums*

### [`kernel::transaction::conflict_checker`](kernel/transaction/conflict_checker.md)

*1 enum*

### [`kernel::transaction::protocol`](kernel/transaction/protocol.md)

*1 static, 1 struct*

### [`logstore`](logstore.md)

*1 enum, 1 module, 1 struct, 1 trait, 1 type alias, 10 functions*

### [`logstore::config`](logstore/config.md)

*1 trait, 2 structs, 5 functions*

### [`logstore::factories`](logstore/factories.md)

*2 traits, 2 type aliases, 3 functions*

### [`logstore::storage`](logstore/storage.md)

*1 trait, 1 type alias, 2 structs*

### [`logstore::storage::retry_ext`](logstore/storage/retry_ext.md)

*1 trait*

### [`logstore::storage::runtime`](logstore/storage/runtime.md)

*1 enum, 2 structs*

### [`logstore::storage::utils`](logstore/storage/utils.md)

*1 function*

### [`operations`](operations.md)

*1 function, 1 struct, 1 trait, 19 modules*

### [`operations::add_column`](operations/add_column.md)

*1 struct*

### [`operations::add_feature`](operations/add_feature.md)

*1 struct*

### [`operations::constraints`](operations/constraints.md)

*1 struct*

### [`operations::convert_to_delta`](operations/convert_to_delta.md)

*1 enum, 1 struct*

### [`operations::create`](operations/create.md)

*1 struct*

### [`operations::delete`](operations/delete.md)

*2 structs*

### [`operations::drop_constraints`](operations/drop_constraints.md)

*1 struct*

### [`operations::filesystem_check`](operations/filesystem_check.md)

*2 structs*

### [`operations::generate`](operations/generate.md)

*1 struct*

### [`operations::load`](operations/load.md)

*1 struct*

### [`operations::load_cdf`](operations/load_cdf.md)

*1 struct*

### [`operations::merge`](operations/merge.md)

*5 structs*

### [`operations::optimize`](operations/optimize.md)

*1 enum, 2 functions, 6 structs*

### [`operations::restore`](operations/restore.md)

*2 structs*

### [`operations::set_tbl_properties`](operations/set_tbl_properties.md)

*1 struct*

### [`operations::update`](operations/update.md)

*2 structs*

### [`operations::update_field_metadata`](operations/update_field_metadata.md)

*1 struct*

### [`operations::update_table_metadata`](operations/update_table_metadata.md)

*2 structs*

### [`operations::vacuum`](operations/vacuum.md)

*1 enum, 1 trait, 4 structs*

### [`operations::write`](operations/write.md)

*1 enum, 2 modules, 2 structs*

### [`operations::write::configs`](operations/write/configs.md)

*1 struct*

### [`operations::write::writer`](operations/write/writer.md)

*4 structs*

### [`protocol`](protocol.md)

*1 module, 3 structs, 5 enums*

### [`protocol::checkpoints`](protocol/checkpoints.md)

*4 functions*

### [`table`](table.md)

*1 function, 1 struct, 3 modules*

### [`table::builder`](table/builder.md)

*2 enums, 2 functions, 2 structs*

### [`table::columns`](table/columns.md)

*2 structs*

### [`table::config`](table/config.md)

*1 trait, 2 constants, 2 enums*

### [`table::state`](table/state.md)

*1 struct*

### [`writer`](writer.md)

*1 enum, 1 trait, 3 modules*

### [`writer::json`](writer/json.md)

*1 struct*

### [`writer::record_batch`](writer/record_batch.md)

*2 structs*

### [`writer::stats`](writer/stats.md)

*1 function*

### [`writer::utils`](writer/utils.md)

*1 function, 1 struct*

