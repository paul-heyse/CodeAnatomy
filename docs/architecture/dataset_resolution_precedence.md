# Dataset Resolution Precedence

This document describes how `DatasetLocation` values are resolved into a
`ResolvedDatasetLocation` for registration, scan configuration, and schema
validation.

## Canonical Resolver
Use `datafusion_engine.dataset.registry.resolve_dataset_location()` as the
single source of truth. All per-field helpers (`resolve_*`) should delegate to
this resolved view.

## Precedence Rules

### Override-bearing fields
For each override-capable field, resolution order is:
1. `DatasetLocation.overrides.<field>`
2. `DatasetSpec.<field>` (when a DatasetSpec is present)
3. Derived defaults (when applicable)

Override-capable fields include:
- `datafusion_scan`
- `delta_scan`
- `delta_cdf_policy`
- `delta_write_policy`
- `delta_schema_policy`
- `delta_maintenance_policy`
- `delta_feature_gate`
- `delta_constraints`
- `table_spec`

### Dataset specification
- If `DatasetLocation.dataset_spec` is provided, it is authoritative.
- If absent and `table_spec` is resolved, build a `DatasetSpec` from it.
- If neither is available, infer schema from the data source when possible.

### Delta log storage options
- Use `DatasetLocation.delta_log_storage_options` when set.
- Otherwise fall back to `DatasetLocation.storage_options`.
- If neither is provided, log options are `None`.

### DataFusion provider selection
Provider resolution is derived as follows:
1. Explicit `DatasetLocation.datafusion_provider` (if set)
2. Delta CDF requirements (policy-required or delta_cdf_options)
3. Non-delta formats default to the listing provider
4. Listing provider is selected when `_prefers_listing_table(...)` returns true

## Testing Guidance
Tests should validate that:
- Overrides take precedence over DatasetSpec values.
- DatasetSpec defaults apply when overrides are absent.
- Schema resolution uses the resolved view and respects table_spec overrides.
