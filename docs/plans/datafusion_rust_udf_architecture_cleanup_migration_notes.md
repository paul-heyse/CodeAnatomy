# DataFusion Rust UDF Architecture Cleanup — Migration Notes

Status: design-phase breaking changes (implemented)

## Summary of Breaking Changes

1. **Per-UDF Python wrappers removed**
   - `datafusion_ext` no longer exposes per-UDF helpers (e.g., `map_entries`, `list_extract`).
   - Use the generic helper `udf_expr(name, *args, ctx=None)` from
     `datafusion_engine.udf.expr`.

2. **`range_table` alias removed**
   - Use DataFusion built-ins: `range` or `generate_series`.

3. **`read_parquet` / `read_csv` UDTFs now accept a full options payload**
   - Signature: `read_parquet(path [, limit] [, options_json])`
   - Signature: `read_csv(path [, limit] [, options_json])`
   - `options_json` is a JSON object (string literal) supporting schema hints, partitioning,
     file sort order, storage options, and format-specific read options.

4. **Delta plugin options now require `scan_config`**
   - Per-field overrides (`file_column_name`, `enable_parquet_pushdown`, `schema_force_view_types`,
     `wrap_partition_values`, `schema_ipc`) are removed from plugin options.
   - Use host-derived defaults via `delta_plugin_options_from_session()` or
     `delta_plugin_options_json()` in `src/datafusion_engine/delta/plugin_options.py`.

## Migration Steps

- **UDF expressions**: replace direct `datafusion_ext.<udf_name>` calls with:
  - `datafusion_ext.udf_expr("<udf_name>", *args)`
  - or `datafusion_engine.udf.expr.udf_expr("<udf_name>", *args)`

- **Table functions**: update any SQL that used `range_table(...)` to `range(...)` or
  `generate_series(...)`.

- **Plugin options**: replace manual JSON assembly with:
  - `delta_plugin_options_from_session(ctx, options)` (Python dict)
  - `delta_plugin_options_json(ctx, options)` (JSON string)

- **UDTF options**: if you need schema hints or storage options, pass them through
  `options_json` for `read_parquet` / `read_csv`.

## Removed Modules / Files

- `rust/datafusion_python/src/udf_custom_py.rs`
- `rust/datafusion_python/src/udf_builtin.rs`
- `rust/datafusion_ext/src/udf_builtin.rs`
- `rust/datafusion_ext/src/udtf_external.rs`

## New / Updated Entry Points

- `rust/datafusion_ext/src/udf_expr.rs`
- `datafusion_ext.udf_expr(name, *args, ctx=None)`
- `datafusion_engine.delta.delta_plugin_options_from_session()`
- `datafusion_engine.delta.delta_plugin_options_json()`
