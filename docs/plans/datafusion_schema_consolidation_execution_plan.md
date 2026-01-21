# DataFusion Schema Consolidation Plan

Status: draft

## Goals
- Make DataFusion the single source of truth for schemas via DDL and information_schema.
- Move schema evolution and alignment to scan-time adapters, not Python-side transforms.
- Require Delta table providers for delta datasets to preserve metadata, pruning, and inserts.
- Surface constraints, defaults, and metadata through DataFusion so downstream tooling is uniform.
- Replace static function catalogs with introspection-driven catalogs for version resilience.

## Non-goals
- Rewrite extractor logic or dataset semantics.
- Replace Delta or Parquet storage formats in this phase.
- Introduce new external dependencies beyond DataFusion and delta-rs.

## Scope 1: External Table DDL + Catalog Autoload

Goal: generate authoritative CREATE EXTERNAL TABLE statements from schema specs and register via
DDL (including UNBOUNDED, PARTITIONED BY, WITH ORDER, OPTIONS) while enabling catalog autoload.

Code pattern:
```python
def datafusion_external_table_sql(
    *,
    name: str,
    location: DatasetLocation,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> str | None:
    spec = location.dataset_spec or dataset_spec_from_schema(name, resolve_dataset_schema(location))
    scan = resolve_datafusion_scan_options(location)
    overrides = ExternalTableConfigOverrides(
        table_name=name,
        partitioned_by=[col for col, _dtype in (scan.partition_cols if scan else ())],
        file_sort_order=list(scan.file_sort_order) if scan and scan.file_sort_order else None,
        unbounded=bool(scan.unbounded) if scan else False,
        options=_external_table_options(location, scan),
    )
    config = spec.table_spec.external_table_config(
        location=str(location.path),
        file_format=location.format,
        overrides=overrides,
    )
    return spec.external_table_sql(config)
```

Target files
- `src/datafusion_engine/registry_bridge.py`
- `src/schema_spec/specs.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/runtime.py`

Implementation checklist
- [ ] Implement `datafusion_external_table_sql` to build DDL from `TableSchemaSpec`.
- [ ] Add helpers to translate `DataFusionScanOptions` into DDL OPTIONS and partition/order clauses.
- [ ] Record DDL via `record_table_definition_override` for SHOW CREATE TABLE parity.
- [ ] Enable catalog autoload defaults in `DataFusionRuntimeProfile` and document env vars.
- [ ] Add diagnostics showing DDL fingerprints vs expected schema fingerprints.

## Scope 2: Schema Evolution Adapters at Scan Boundary

Goal: use DataFusion physical expr adapters (schema adapters) instead of Python-side casts.

Code pattern:
```python
from datafusion_ext import schema_evolution_adapter_factory

def build_schema_adapter_factories(registry: IbisDatasetRegistry) -> dict[str, object]:
    factories: dict[str, object] = {}
    for name in registry.catalog.names():
        spec = registry.catalog.get(name).dataset_spec
        if spec is None:
            continue
        evolution = spec.evolution_spec
        if evolution.allow_extra or evolution.allow_missing or evolution.allow_casts:
            factories[name] = schema_evolution_adapter_factory()
    return factories
```

Target files
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/schema_spec/system.py`
- `rust/datafusion_ext/src/lib.rs`
- `src/normalize/schema_infer.py`

Implementation checklist
- [ ] Populate `DataFusionRuntimeProfile.schema_adapter_factories` from dataset specs.
- [ ] Ensure `registry_bridge._resolve_expr_adapter_factory` picks dataset-specific factories.
- [ ] Attach `TableSchemaContract` for file schema + partition columns when scanning.
- [ ] Remove or bypass `SchemaTransform` casts in normalize paths once adapters are active.
- [ ] Add diagnostics comparing logical vs physical schemas after adapter application.

## Scope 3: Delta Provider Enforcement and Fallback Removal

Goal: require Delta table providers (rust or Python) for delta datasets and stop silent fallback
to pyarrow datasets that drop Delta metadata and pruning.

Code pattern:
```python
def _register_delta(context: DataFusionRegistrationContext) -> DataFrame:
    delta_provider = _delta_rust_table_provider(context, delta_scan=context.options.delta_scan)
    if delta_provider is None:
        delta_provider = _delta_table_provider(table, delta_scan=context.options.delta_scan)
    if delta_provider is None and context.runtime_profile and context.runtime_profile.require_delta:
        msg = f"Delta provider required for {context.name!r}."
        raise ValueError(msg)
```

Target files
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/registry.py`
- `src/storage/deltalake/delta.py`

Implementation checklist
- [ ] Add `require_delta` (or allowlist) to runtime profile and registry options.
- [ ] Treat provider absence as error for production datasets, warning for dev tiers.
- [ ] Ensure `DeltaScanOptions.schema` is set from dataset specs to lock schema.
- [ ] Remove delta fallback paths once provider availability is validated in CI.

## Scope 4: Constraints, Defaults, and Metadata Surfacing

Goal: make constraints and column defaults visible in DataFusion and available via
information_schema for contract enforcement and auditing.

Code pattern:
```python
def _table_key_fields(context: DataFusionRegistrationContext) -> tuple[str, ...]:
    schema = context.options.schema
    if schema is None:
        return ()
    _required, key_fields = schema_constraints_from_metadata(schema.metadata)
    return key_fields
```

Target files
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/arrowdsl/schema/metadata.py`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/schema_introspection.py`

Implementation checklist
- [ ] Standardize `default_value` metadata on fields that need defaults.
- [ ] Ensure key fields are passed into listing providers for constraint creation.
- [ ] Validate constraints and defaults via `information_schema.table_constraints`.
- [ ] Add tests to ensure column defaults appear in `information_schema.columns`.

## Scope 5: Dynamic Function Catalog Introspection

Goal: replace static function signature tables with introspection-driven catalogs from
information_schema and SHOW FUNCTIONS.

Code pattern:
```python
def function_catalog(ctx: SessionContext) -> FunctionCatalog:
    introspector = SchemaIntrospector(ctx)
    routines = introspector.routines_snapshot()
    params = introspector.parameters_snapshot()
    return FunctionCatalog.from_information_schema(routines, params)
```

Target files
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/runtime.py`

Implementation checklist
- [ ] Add a `FunctionCatalog` builder from routines/parameters snapshots.
- [ ] Replace static signature maps in AST/CST/TS validations with the catalog.
- [ ] Add fallbacks for deployments with information_schema disabled.
- [ ] Add diagnostics output when required functions are missing.

## Scope 6: Schema Authority Consolidation (remove local inference)

Goal: eliminate schema inference and alignment in Python and rely on DataFusion schemas,
requested schema projection, and information_schema validation.

Code pattern:
```python
schema = ctx.table(name).schema()
resolved = coerce_table_like(table, requested_schema=schema)
df_ctx.register_record_batches(name, [resolved.read_all().to_batches()])
```

Target files
- `src/normalize/schema_infer.py`
- `src/normalize/ibis_plan_builders.py`
- `src/ibis_engine/schema_utils.py`
- `src/arrowdsl/core/interop.py`
- `src/datafusion_engine/nested_tables.py`

Implementation checklist
- [ ] Replace `infer_schema_or_registry` callers with DataFusion-backed schema lookups.
- [ ] Replace `align_table_to_schema` in normalize pipelines with DataFusion scans or views.
- [ ] Enforce schema validation via DESCRIBE and information_schema columns.
- [ ] Remove list-view normalization once schema hardening disables view types.

## Scope 7: Write Path Consolidation via DataFusion INSERT/COPY

Goal: prefer DataFusion-native INSERT/COPY to Delta and external tables when providers support
insert_into to reduce bespoke delta-rs write paths.

Code pattern:
```python
ctx.register_table("target", delta_provider)
ctx.sql("INSERT INTO target SELECT * FROM staging").collect()
ctx.sql("COPY (SELECT * FROM target) TO 'path' STORED AS PARQUET").collect()
```

Target files
- `src/storage/deltalake/delta.py`
- `src/ibis_engine/io_bridge.py`
- `src/engine/materialize.py`

Implementation checklist
- [ ] Prefer `write_datafusion_delta` for append-only writes when available.
- [ ] Add INSERT-based writer for datasets with provider insert support.
- [ ] Keep delta-rs writer only for cases not supported by DataFusion inserts.

## Legacy Decommission List (after scopes above complete)

Delete these modules or methods once replacements are in place and call sites are removed:
- `src/normalize/schema_infer.py` (replace with DataFusion schema lookups and adapters).
- `src/ibis_engine/schema_utils.align_table_to_schema` (replace with DataFusion DDL + projection).
- `src/ibis_engine/schema_utils.normalize_table_for_ibis` (remove once view types are disabled).
- `src/datafusion_engine/schema_authority.py` (merge into `schema_spec.system` or introspector use).
- `src/schema_spec/system.schema_evolution_compatible` (obsolete with scan-time adapters).
- Delta dataset fallbacks in `src/datafusion_engine/registry_bridge.py`:
  `_delta_dataset_from_table` and `_delta_dataset_from_files`.

## Sequencing
- Phase 1: Scopes 1, 4, 5 (DDL + constraints + introspection).
- Phase 2: Scopes 2 and 6 (scan adapters + remove inference/alignment).
- Phase 3: Scopes 3 and 7 (Delta provider enforcement + write path consolidation).
- Phase 4: Remove legacy modules after validation passes.
