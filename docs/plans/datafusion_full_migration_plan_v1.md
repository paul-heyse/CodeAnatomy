## DataFusion Full Migration Plan (v2)

### Goal
Migrate the entire codebase to use DataFusion as the compute plane, with Delta Lake as the
transactional storage engine, and Rust UDFs for any remaining compute that cannot be expressed
with DataFusion expressions. This plan is scoped to the full repo (`src/`, `scripts/`, tests as
needed), and standardizes on **delta-rs as the commit engine** for writes/mutations.

### Non-Goals
- Do not change product behavior beyond switching execution paths to DataFusion equivalents.
- Do not introduce new external dependencies.
- Do not delete Arrow/utility modules until all migration scopes are complete.
- Do not allow ad-hoc `SessionContext` creation outside the runtime profile.

---

## Scope Item 0 — Runtime contract, SQL safety, and catalog discipline

**Intent:** Establish a single, enforced SessionContext contract (runtime profile), ensure SQL
guardrails, and standardize on catalog registration through the IO adapter.

**Representative pattern snippet:**
```python
from datafusion import SQLOptions

from datafusion_engine.runtime import DataFusionRuntimeProfile

profile = DataFusionRuntimeProfile(
    enable_information_schema=True,
    enable_ident_normalization=False,
    require_delta=True,
)
ctx = profile.session_context()

read_only = (
    SQLOptions()
    .with_allow_ddl(False)
    .with_allow_dml(False)
    .with_allow_statements(False)
)
df = ctx.sql_with_options(
    "SELECT * FROM information_schema.tables",
    read_only,
)
```

**Target files to modify:**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/sql_options.py`
- `src/datafusion_engine/io_adapter.py`
- `src/datafusion_engine/introspection.py`
- `src/datafusion_engine/schema_introspection.py`

**Implementation checklist:**
- Route all SessionContext creation through `DataFusionRuntimeProfile`.
- Enforce `SQLOptions` usage for untrusted SQL (DDL/DML/SET blocked).
- Standardize registration via `DataFusionIOAdapter`.
- Ensure identifier normalization and URL-table behaviors are explicit and documented.

---

## Scope Item 1 — Catalog + information_schema as source of truth

**Intent:** Make `information_schema` and the DataFusion catalog the authoritative schema surface,
and remove Python-side registries once parity is proven.

**Representative pattern snippet:**
```python
from datafusion import SessionContext

from datafusion_engine.schema_introspection import table_names_snapshot

ctx = SessionContext()
table_names = table_names_snapshot(ctx)
```

**Target files to modify:**
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_validation.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/introspection.py`
- `src/extract/schema_ops.py`
- `src/normalize/registry_runtime.py`
- `src/schema_spec/system.py`

**Planned deletions:** none (deferred until final cleanup).

**Implementation checklist:**
- Enable `information_schema` in runtime config by default.
- Replace direct schema lookups with `information_schema.*` queries and introspection helpers.
- Require `UdfCatalog` parity validation before removing registries.
- Add diagnostics snapshots for catalog tables, columns, routines, and settings.

---

## Scope Item 2 — Expression-first compute (DataFusion functions + Rust UDF gaps)

**Intent:** Move all compute into DataFusion plans so optimization and pushdown apply end-to-end,
and surface any missing kernels as Rust UDFs.

**Representative pattern snippet:**
```python
from datafusion import col, functions as f

mask = f.is_null(col("value")) | f.is_null(col("fallback"))
df = df.filter(mask).select(
    f.coalesce(col("value"), col("fallback")).alias("value"),
    f.arrow_cast(col("ts"), "Timestamp(Microsecond, None)").alias("ts"),
)
```

**Target files to modify (PyArrow compute removal):**
- `src/datafusion_engine/finalize.py`
- `src/obs/metrics.py`
- `src/datafusion_engine/param_tables.py`
- `src/arrow_utils/schema/build.py`
- `src/arrow_utils/schema/dictionary.py`

**Planned deletions:**
- Remove `pyarrow.compute` usage in the files above.
- Keep compute-related helpers until all replacement paths are integrated.

**Implementation checklist:**
- Replace `pc.call_function(...)` with DataFusion expressions.
- Convert compute masks into `filter(...)` expressions.
- Replace `pc.unique` with `distinct` or aggregate-based uniqueness.
- Use `arrow_cast`, `arrow_typeof`, `named_struct`, and `get_field` for schema-precise work.
- Track any missing kernels and implement them as Rust UDFs.

---

## Scope Item 3 — Schema alignment + encoding via DataFusion + Delta contracts

**Intent:** Replace Arrow-centric schema alignment with DataFusion casts and Delta schema contracts
for authoritative types, nullability, and metadata.

**Representative pattern snippet:**
```python
import pyarrow as pa

from datafusion_engine.runtime import align_table_to_schema
from storage.deltalake.delta import DeltaSchemaRequest, delta_table_schema

request = DeltaSchemaRequest(path="/data/my_delta")
target_schema = delta_table_schema(request) or pa.schema([])

aligned = align_table_to_schema(
    table=arrow_table,
    schema=target_schema,
    keep_extra_columns=False,
)
```

**Target files to modify:**
- `src/datafusion_engine/schema_alignment.py`
- `src/datafusion_engine/schema_validation.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`
- `src/storage/deltalake/delta.py`
- `src/storage/deltalake/config.py`

**Planned deletions:**
- Defer removal of Arrow schema alignment helpers until Scope Item 8.

**Implementation checklist:**
- Resolve schema from Delta snapshot and cross-check with information_schema.
- Use DataFusion projections for casting and nullability alignment.
- Apply encoding policy at write time (DataFusion-first, Arrow only at boundary).
- Enforce schema policy rules (merge/overwrite, column mapping mode).

---

## Scope Item 4 — DataFrame-first pipelines + Arrow boundary discipline

**Intent:** Use DataFusion DataFrames throughout extract and incremental pipelines; keep Arrow only
at IPC/serialization boundaries.

**Representative pattern snippet:**
```python
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.runtime import DataFusionRuntimeProfile

profile = DataFusionRuntimeProfile()
ctx = profile.session_context()
adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)

adapter.register_record_batches("tmp_events", arrow_table.to_batches())
df = ctx.table("tmp_events").select("repo", "path")
```

**Target files to modify (representative list):**
- `src/datafusion_engine/arrow_ingest.py`
- `src/datafusion_engine/nested_tables.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/runtime.py`
- `src/extract/helpers.py`
- `src/extract/scip_extract.py`
- `src/hamilton_pipeline/driver_factory.py`
- `src/incremental/impact.py`
- `src/incremental/deltas.py`
- `src/incremental/metadata.py`
- `src/incremental/exports.py`
- `src/incremental/snapshot.py`
- `src/incremental/imports_resolved.py`
- `src/incremental/fingerprint_changes.py`
- `src/incremental/module_index.py`

**Planned deletions:**
- None in this scope; Arrow boundary helpers retained until final cleanup.

**Implementation checklist:**
- Register Arrow inputs via `DataFusionIOAdapter.register_record_batches`.
- Replace `pa.Table` transforms with DataFusion DataFrame/SQL plans.
- Use DataFrame iteration or `__arrow_c_stream__` for large outputs.
- Maintain Arrow conversions only at IO/IPC boundaries.

---

## Scope Item 5 — Delta integration standardization (providers, CDF, commit engine)

**Intent:** Standardize Delta read paths on TableProvider registration, use Delta CDF providers
for incremental flows, and route all mutations through delta-rs control plane.

**Representative pattern snippet:**
```python
from datafusion_engine.delta_control_plane import (
    DeltaProviderRequest,
    DeltaCdfRequest,
    delta_cdf_provider,
    delta_provider_from_session,
)
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.runtime import DataFusionRuntimeProfile
from storage.deltalake import DeltaCdfOptions

profile = DataFusionRuntimeProfile()
ctx = profile.session_context()
adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)

bundle = delta_provider_from_session(
    ctx,
    request=DeltaProviderRequest(
        table_uri="/data/events",
        storage_options=None,
        version=None,
        timestamp=None,
        delta_scan=None,
        gate=None,
    ),
)
adapter.register_delta_table_provider("events", bundle.provider)

cdf_bundle = delta_cdf_provider(
    request=DeltaCdfRequest(
        table_uri="/data/events",
        storage_options=None,
        version=None,
        timestamp=None,
        options=DeltaCdfOptions(starting_version=0),
        gate=None,
    )
)
adapter.register_delta_cdf_provider("events_cdf", cdf_bundle.provider)
```

**Target files to modify:**
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/io_adapter.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/delta_control_plane.py`
- `src/storage/deltalake/delta.py`
- `src/storage/deltalake/query_builder.py`
- `src/storage/deltalake/scan_profile.py`
- `src/storage/deltalake/config.py`
- `src/storage/deltalake/file_pruning.py`
- `src/incremental/write_helpers.py`
- `scripts/delta_export_snapshot.py`
- `scripts/migrate_parquet_to_delta.py`
- `scripts/delta_maintenance.py`
- `scripts/delta_restore_table.py`
- `scripts/e2e_diagnostics_report.py`

**Planned deletions:**
- Remove the Arrow Dataset fallback for Delta registrations except as a guarded escape hatch.

**Implementation checklist:**
- Use `delta_provider_from_session` and `register_delta_table_provider` for all reads.
- Drive scan behavior via `DeltaScanOptions` and `DeltaFeatureGate`.
- Standardize incremental reads on CDF providers and `DeltaCdfOptions`.
- Route all writes/mutations through `delta_write_ipc`, `delta_merge`, `delta_update`,
  and `delta_delete` in the Rust control plane.
- Prefer `query_delta_sql(...)` (QueryBuilder) only when DataFusion Python is unavailable.

---

## Scope Item 6 — Rust UDF platform maturity (registry, docs, parity, optimizer hooks)

**Intent:** Replace remaining custom compute with Rust UDFs and enforce registry parity with
information_schema, including docs and parameter metadata.

**Representative pattern snippet (Rust):**
```rust
#[derive(Debug, PartialEq, Eq, Hash)]
struct MyUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for MyUdf {
    fn name(&self) -> &str { "my_udf" }
    fn signature(&self) -> &Signature { self.signature.signature() }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> { ... }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> { ... }
}
```

**Target files to modify:**
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/udf_docs.rs`
- `rust/datafusion_ext/tests/udf_conformance.rs`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_parity.py`
- `src/datafusion_ext.pyi`
- `src/test_support/datafusion_ext_stub.py`

**Planned deletions:**
- Remove Python-side stand-ins once Rust UDFs exist and are validated.

**Implementation checklist:**
- Implement UDFs with `return_field_from_args` for type + nullability correctness.
- Add aliases and named parameters; ensure they show in information_schema.
- Add conformance tests for signatures, return types, and docs parity.
- Validate registry snapshots before runtime usage.

---

## Scope Item 7 — Observability, diagnostics, and performance gates

**Intent:** Make migration safety measurable via diagnostics, registry snapshots, and plan
metadata, and ensure performance regressions are detectable.

**Representative pattern snippet:**
```python
from datafusion_engine.runtime import DataFusionRuntimeProfile

profile = DataFusionRuntimeProfile(enable_information_schema=True)
profile.record_schema_snapshots()
profile.record_catalog_autoload()
```

**Target files to modify:**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/udf_parity.py`
- `src/datafusion_engine/delta_observability.py`
- `src/datafusion_engine/diagnostics.py`
- `scripts/e2e_diagnostics_report.py`

**Implementation checklist:**
- Record catalog, schema, and UDF snapshots after each major registration step.
- Capture scan configs, Delta snapshot metadata, and commit payloads.
- Establish parity checks for row counts, schema fingerprints, and nullability.
- Add performance baselines for scan pruning, join plans, and data skipping.

---

## Scope Item 8 — Deferred cleanup (only after all scopes complete)

**Intent:** Delete Arrow-centric helpers and compatibility layers only once all above scopes are
complete and tests pass.

**Candidate deletions (conditional):**
- `src/arrow_utils/schema/*`
- `src/arrow_utils/core/interop.py`
- `src/datafusion_engine/arrow_ingest.py`
- `src/storage/ipc.py` (only if IPC-only paths removed)
- `scripts/check_pyarrow_compute_usage.py`

**Implementation checklist:**
- Verify no remaining imports or runtime use of Arrow-only helpers.
- Remove modules and update any exports/docs referencing them.
- Run full lint/type gates and targeted test suites.
