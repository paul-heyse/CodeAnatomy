# Delta Plugin Options Assessment

## Context
The DataFusion plugin path exposes Delta table providers through the FFI boundary. This uses Delta as a `TableProvider`, which is the best‑in‑class integration path because it preserves file‑level pruning and DataFusion planning optimizations. This is consistent with the DataFusion and delta‑rs integration guidance documented in the DataFusion stack references.

Key references (from the datafusion-stack skill):
- Delta as a `TableProvider` is the preferred path for performance and correctness (file‑level pruning via the Delta log + DataFusion row‑group pruning).
- DataFusion’s FFI boundary expects provider configuration to be supplied explicitly when the host runtime (Python) is constructing the provider.

## Current Plugin Options (DeltaProviderOptions)
The plugin currently accepts:
- `table_uri`, `storage_options`
- `version` / `timestamp` (mutually exclusive, enforced in the control‑plane load path)
- `file_column_name`
- `enable_parquet_pushdown`
- `schema_force_view_types`
- `wrap_partition_values`
- `schema_ipc` (schema overrides)
- `min_reader_version`, `min_writer_version`, `required_reader_features`, `required_writer_features`
- `files` (file‑set restriction)

These options are mapped onto `DeltaScanConfig` and parity logic that mirrors the control‑plane path (builder usage + schema override + file column normalization).

## Assessment vs Control-Plane Behavior
- **Session‑derived settings**: `enable_parquet_pushdown` and `schema_force_view_types` are session settings in the control‑plane (`DeltaScanConfig::new_from_session`). The plugin path does not have direct access to the session, so parity depends on the host passing these values explicitly.
- **File column normalization**: `file_column_name` is correctly routed through the builder path, ensuring column‑name collision handling is consistent with control‑plane behavior.
- **Schema overrides**: `schema_ipc` is necessary to preserve scan schema overrides and is a required parity knob for plan stability.
- **Partition wrapping**: `wrap_partition_values` remains a valid scan‑level optimization toggle and should remain host‑controlled.
- **Delta protocol gating**: reader/writer version gates and required features are aligned with control‑plane policy enforcement.

## Recommendations
1. **Keep the current option set**, but explicitly treat the following as *session‑derived* and expected to be passed by the host:
   - `enable_parquet_pushdown`
   - `schema_force_view_types`

2. **Do not deprecate schema controls yet**: `schema_ipc`, `wrap_partition_values`, and `file_column_name` remain essential parity knobs, especially for deterministic plan snapshots and partition‑column behavior.

3. **Host helper (future)**: add a small Python helper that derives session settings and injects them into plugin options (to make the plugin path match control‑plane without requiring the caller to know all defaults).

4. **Deprecation candidate (later)**: once the host helper is in place and universally used, consider deprecating *direct* user overrides of `enable_parquet_pushdown` and `schema_force_view_types` in plugin‑level APIs to reduce divergence risk.

## Action Taken
- No code changes required immediately; the plugin options remain necessary for parity and explicit configuration.
- This assessment documents the expected usage contract and future deprecation path once session‑derived values are injected automatically by the host.
