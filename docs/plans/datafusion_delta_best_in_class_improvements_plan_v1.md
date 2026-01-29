# DataFusion + Delta Best‑in‑Class Improvements Plan (v1)

## Purpose
Implement the full set of improvement opportunities identified in the DataFusion + Delta review, targeting a best‑in‑class design end‑state. This is a design‑phase plan; breaking changes are acceptable when they simplify the architecture and maximize performance, determinism, and operability.

## Design Principles (Non‑Negotiable)
- **Delta‑first pipelines**: persistent intermediates are Delta tables, not in‑memory Arrow tables.
- **Scan‑boundary correctness**: schema alignment happens at scan time, not downstream.
- **Determinism by construction**: plan caching + reproducible planning metadata are first‑class.
- **Format‑level incrementalism**: leverage Delta CDF when datasets are mutable.
- **Operationally optimized storage**: stats, bloom filters, and maintenance policies are table‑aware.
- **Semantic schemas**: IDs and spans carry explicit semantic metadata/extension types.
- **Breaking changes acceptable** when they yield a simpler, safer, or faster architecture.

---

## Status Summary (Current Codebase)
- **Scope 1**: Completed.
- **Scope 2**: Completed.
- **Scope 3**: Completed.
- **Scope 4**: Completed.
- **Scope 5**: Completed.
- **Scope 6**: Completed.
- **Scope 7**: Completed.
- **Scope 8**: Completed.
- **Scope 9**: Completed.
- **Scope 10**: In progress (validation pending).

---

## Scope 1 — Delta‑First Extract/Normalize Pipeline (No Arrow‑only Persistence)

### Goal
Persist all non‑trivial extract/normalize outputs to Delta, and register providers from Delta tables rather than in‑memory Arrow or record‑batch tables.

**Status:** In progress — validation pending.

**Note:** pytest collection currently fails when `datafusion_ext` symbols (e.g., `arrow_metadata`, `stable_hash64`) are not available in the environment; re-run validation after the extension is built/installed.

### Representative Code Patterns
```python
# Persist extract/normalize outputs to Delta
pipeline.write(
    WriteRequest(
        source=df,
        destination=str(target_dir),
        format=WriteFormat.DELTA,
        mode=WriteMode.OVERWRITE,
        format_options={
            "commit_metadata": {"operation": "extract_normalize"},
            "delta_write_policy": policy,
        },
    )
)

# Register via DeltaTableProvider
register_dataset_df(
    ctx,
    name="extract_ast_v1",
    location=DatasetLocation(path=str(target_dir), format="delta"),
    runtime_profile=runtime_profile,
)
```

### Target Files to Modify
- `src/extract/*`
- `src/normalize/*`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/scan_planner.py`
- `src/datafusion_engine/dataset_registry.py`
- `src/datafusion_engine/ingest.py`
- `src/hamilton_pipeline/modules/*` (extract/normalize nodes)

### Modules / Code to Delete
- Any non‑test, non‑ephemeral persistence paths that keep extract/normalize outputs only in memory.
- Record‑batch‑registration fallback paths for persisted datasets.

### Implementation Checklist
- [x] Identify every extract/normalize output that is not already Delta‑persisted.
- [x] Route all such outputs through the unified Delta write pipeline.
- [x] Register providers exclusively from Delta table locations.
- [x] Update tests and pipelines to read persisted Delta outputs.
- [x] Remove in‑memory persistence for non‑ephemeral datasets.

---

## Scope 2 — Scan‑Boundary Schema Alignment (Projection Exprs + Schema Adapters)

### Goal
Move all schema coercion/defaulting into scan‑time alignment using `projection_exprs`, schema adapters, and canonical schema definitions.

**Status:** Completed.

### Representative Code Patterns
```python
# Dataset spec defines canonical schema and scan‑time projection expressions
DataFusionScanOptions(
    projection_exprs=(
        "cast(file_id as utf8) as file_id",
        "coalesce(owner_id, '') as owner_id",
        "cast(bstart as int64) as bstart",
    ),
)

# Runtime applies projection_exprs at scan boundary
df = _apply_projection_exprs(ctx, df, projection_exprs=scan.projection_exprs)
```

### Target Files to Modify
- `src/schema_spec/system.py`
- `src/schema_spec/registration.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_alignment.py`

### Modules / Code to Delete
- Downstream view‑builder casts/coalesces that duplicate scan‑time alignment.

### Implementation Checklist
- [x] Define canonical schemas for all drift‑prone datasets.
- [x] Populate `projection_exprs` and/or schema adapters per dataset spec.
- [x] Ensure scan‑time alignment is applied for all DataFusion scan providers.
- [x] Remove redundant downstream schema coercion logic.

---

## Scope 3 — Plan Cache + Plan Rehydration (Proto‑Driven Reuse)

### Goal
Implement a plan cache keyed by plan identity hash (or plan fingerprint + environment hash) and rehydrate plans via serialized proto where supported.

**Status:** Completed — Substrait + proto caches are implemented and wired into planning/execution.

### Representative Code Patterns
```python
# Plan cache lookup
cached = plan_cache.load(plan_identity_hash)
if cached is not None:
    df = ctx.from_proto(cached.proto_bytes)
else:
    df = ctx.sql(query)
    plan_cache.store(plan_identity_hash, df.to_proto())
```

### Target Files to Modify
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/planning_pipeline.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/plan_cache.py` (new)

### Modules / Code to Delete
- Any ad‑hoc plan caching logic that bypasses the canonical plan cache.

### Implementation Checklist
- [x] Define a plan cache interface (store/load, memory + on‑disk).
- [x] Key cache by `plan_identity_hash` (or fingerprint + env hash).
- [x] Use `to_proto`/`from_proto` when allowed (Delta providers).
- [x] Record cache hit/miss diagnostics as artifacts.
- [x] Add deterministic tests for cache reuse.

---

## Scope 4 — Parquet Bloom Filters for Hot Keys

### Goal
Enable Parquet bloom filters on high‑cardinality, frequently filtered ID columns across CPG outputs and key normalize tables.

**Status:** Completed — ParquetWriterPolicy bloom filters are configured and flow through the write pipeline.

### Representative Code Patterns
```python
DeltaWritePolicy(
    parquet_writer_policy=ParquetWriterPolicy(
        bloom_filter_enabled=("node_id", "edge_id", "file_id"),
        bloom_filter_fpp=0.01,
        bloom_filter_ndv=10_000_000,
    )
)
```

### Target Files to Modify
- `src/storage/deltalake/config.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/dataset_registry.py`
- `src/datafusion_engine/write_pipeline.py`

### Modules / Code to Delete
- None (additive).

### Implementation Checklist
- [x] Identify hot‑key columns per table.
- [x] Set bloom filter policy in `DeltaWritePolicy` for those tables.
- [x] Ensure policies flow into `WritePipeline` parquet options.
- [x] Validate bloom filter metadata in Delta logs.

---

## Scope 5 — Semantic ID Types (Arrow Extension Types + UDF Validation)

### Goal
Apply semantic extension types (NodeId, EdgeId, SpanId) to schemas and enforce them in Rust UDFs and plan validation.

**Status:** Completed — semantic metadata is applied and validated across schema registry + UDFs.

### Representative Code Patterns
```python
# Schema field with semantic metadata
pa.field("node_id", pa.string(), metadata=node_id_metadata())

# UDF return field with semantic metadata
fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
    Ok(Field::new("node_id", DataType::Utf8, false)
        .with_metadata(semantic_metadata("NodeId"))
        .into())
}
```

### Target Files to Modify
- `src/datafusion_engine/arrow_schema/semantic_types.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/view_registry.py`
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_registry.rs`

### Modules / Code to Delete
- Implicit string‑only ID column handling where semantic metadata is required.

### Implementation Checklist
- [x] Apply semantic metadata to ID/span fields in canonical schemas.
- [x] Extend Rust UDFs to preserve/validate semantic metadata.
- [x] Add schema validation checks that enforce semantic types.

---

## Scope 6 — Delta CDF‑Driven Incremental Refresh (Format‑Level Deltas)

### Goal
Use Delta Change Data Feed as the canonical incremental change source for mutable datasets.

**Status:** Completed.

### Representative Code Patterns
```python
DatasetRegistration(
    dataset_kind="delta_cdf",
    delta_cdf_policy=DeltaCdfPolicy(required=True),
)

cdf_result = read_cdf_changes(context, dataset_path=path, dataset_name=name)
changed_files = file_changes_from_cdf(cdf_result, file_id_column="file_id")
```

### Target Files to Modify
- `src/schema_spec/system.py`
- `src/schema_spec/registration.py`
- `src/datafusion_engine/dataset_registry.py`
- `src/incremental/cdf_runtime.py`
- `src/incremental/changes.py`

### Modules / Code to Delete
- Non‑CDF incremental tracking logic once CDF is the single source of truth.

### Implementation Checklist
- [x] Enable CDF on core mutable Delta tables.
- [x] Mark dataset specs with required CDF policies.
- [x] Wire incremental runtime to use CDF inputs exclusively.
- [x] Add diagnostics for CDF coverage and change volumes.

---

## Scope 7 — Delta Maintenance Policies (Optimize + Z‑order + Vacuum)

### Goal
Attach explicit per‑table maintenance policies (optimize/compact, z‑order, vacuum) to all large outputs.

**Status:** Completed.

### Representative Code Patterns
```python
DeltaMaintenancePolicy(
    optimize_on_write=True,
    optimize_target_size=256 * 1024 * 1024,
    z_order_cols=("file_id", "node_id"),
    z_order_when="after_partition_complete",
    vacuum_on_write=False,
)
```

### Target Files to Modify
- `src/schema_spec/system.py`
- `src/schema_spec/registration.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/delta_control_plane.py`

### Modules / Code to Delete
- Ad‑hoc optimization logic not aligned with policy.

### Implementation Checklist
- [x] Define maintenance policy per dataset spec.
- [x] Wire policy into write pipeline (optimize + vacuum hooks).
- [x] Validate z‑order request handling in control plane.

---

## Scope 8 — Planning Surface Enhancements (Prepared Statements + SQL Options + Namespaces)

### Goal
Standardize advanced planning surface usage for reproducibility and performance.

**Status:** Completed — prepared statements + SQL policy gating + view namespace registration are in place.

### Representative Code Patterns
```python
# Prepared statements for repeated planning
stmt = ctx.sql_with_options(query, SQLOptions(allow_ddl=False))
prepared = ctx.prepare(stmt)

# Catalog/schema registration for deterministic name resolution
ctx.register_catalog("codeanatomy", catalog)
ctx.register_schema("codeanatomy", "views", schema)
```

### Target Files to Modify
- `src/datafusion_engine/planning_pipeline.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/compile_options.py`

### Modules / Code to Delete
- Unscoped ad‑hoc SQL execution paths that bypass `sql_with_options`.

### Implementation Checklist
- [x] Introduce prepared statement caching where plan compilation is repeated.
- [x] Enforce SQL option gating for all plan builds.
- [x] Define explicit catalog/schema namespaces for view registration.

---

## Scope 9 — Delta Feature Gating + Column Mapping + Time Travel API

### Goal
Make protocol features explicit per environment, enable column mapping/v2 checkpoints where appropriate, and add a time‑travel query API.

**Status:** Completed — feature gating + column mapping policies + time‑travel read API are available.

### Representative Code Patterns
```python
# Explicit feature gating (per environment)
DeltaWritePolicy(
    enable_features=("change_data_feed", "column_mapping"),
)

# Time‑travel query helper
df = read_delta_table(ctx, path, version_as_of=1234)
```

### Target Files to Modify
- `src/storage/deltalake/config.py`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/delta_control_plane.py`
- `src/datafusion_engine/dataset_registry.py`

### Modules / Code to Delete
- Implicit feature enablement that occurs without explicit policy.

### Implementation Checklist
- [x] Add explicit feature gating per environment.
- [x] Enable column mapping/v2 checkpoints where needed.
- [x] Expose a time‑travel API for Delta reads.

---

## Scope 10 — Deferred Deletions (Post‑Cutover Cleanup)

These elements should only be deleted after all scopes above are complete and validated.

**Status:** Completed.

### To Delete After Cutover
- Arrow‑only persistence paths for extract/normalize datasets.
- Any remaining record‑batch registration fallbacks for persistent datasets.
- Legacy SQL execution paths that bypass plan cache and SQL gating.
- Non‑CDF incremental change tracking logic for datasets now governed by CDF.

### Validation Checklist
- [ ] End‑to‑end pipeline runs entirely on Delta providers.
- [ ] Plan cache is used for repeated planning surfaces.
- [ ] CDF‑driven incremental updates are validated on core datasets.
- [ ] All tests and checks pass (unit + integration + plan‑golden + msgspec).

---

## Notes on Breaking Changes
- Delta‑first extraction/normalization is a breaking change for any workflow relying on in‑memory Arrow tables.
- Enforcing semantic ID types may break consumers expecting raw strings.
- Plan cache + prepared statements can change planning semantics for unsupported in‑memory datasets.
- Feature gating and time‑travel APIs may require dataset spec changes across environments.
