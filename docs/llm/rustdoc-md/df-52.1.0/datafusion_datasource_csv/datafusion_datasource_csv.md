**datafusion_datasource_csv**

# Module: datafusion_datasource_csv

## Contents

**Modules**

- [`file_format`](#file_format) - [`CsvFormat`], Comma Separated Value (CSV) [`FileFormat`] abstractions
- [`source`](#source) - Execution plan for reading CSV files

**Functions**

- [`partitioned_csv_config`](#partitioned_csv_config) - Returns a [`FileScanConfig`] for given `file_groups`

---

## Module: file_format

[`CsvFormat`], Comma Separated Value (CSV) [`FileFormat`] abstractions



## datafusion_datasource_csv::partitioned_csv_config

*Function*

Returns a [`FileScanConfig`] for given `file_groups`

```rust
fn partitioned_csv_config(file_groups: Vec<datafusion_datasource::file_groups::FileGroup>, file_source: std::sync::Arc<dyn FileSource>) -> datafusion_common::Result<datafusion_datasource::file_scan_config::FileScanConfig>
```



## Module: source

Execution plan for reading CSV files



