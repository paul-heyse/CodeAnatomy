# Architectural Unification Best-in-Class Integration Addendum Plan v1

Status: Updated (design-phase addendum; implementation audited Feb 2026)
Owner: Codex (proposed)

This plan captures **additional best-in-class improvements** to Rust + Python DataFusion/Delta integration, aligned with:
- `docs/architecture/architectural_unification_deep_dive.md`
- `docs/plans/architectural_unification_scope_alignment_plan_v1.md`

Update notes:
- Audited against current `src/` + `rust/` implementation (Feb 2026).
- Streaming-first Delta read/write migration completed.
- DataFusion cache config (TTL + snapshot-pinned mode) implemented and wired.
- Rust UDF conformance coverage expanded (named params + short-circuit invariants).
- Each scope includes a short status snapshot (implemented vs remaining gaps).

The scope below is intentionally additive and does **not** redesign the existing architecture. It focuses on packaging, streaming, storage/credential unification, Delta concurrency correctness, cache tuning, UDF quality gates, observability, compatibility handshakes, and Delta-specific integration hardening.

---

## Scope 0 — Packaging & wheel pipeline hardening (abi3/manylinux)

### Problem
Best-in-class Rust↔Python integrations fail in production without deterministic, portable wheels and ABI-compatible packaging. The current plan does not codify the packaging contract or CI targets.

### Status
- Complete: CI workflow in `.github/workflows/wheels.yml`, manylinux baseline codified in `scripts/build_datafusion_wheels.sh`, packaging contract in `docs/architecture/packaging.md`.
- Complete: module-name invariants and `capabilities_snapshot()` smoke test in `scripts/build_datafusion_wheels.sh`.

### Architecture and Code Pattern
Define a single “artifact strategy” and enforce it in CI.

```yaml
# .github/workflows/wheels.yml
- uses: PyO3/maturin-action@v1
  with:
    args: --release --locked --compatibility pypi
    manylinux: "2014"
```

```toml
# rust/datafusion_python/Cargo.toml.orig
[dependencies.pyo3]
features = ["extension-module", "abi3-py310"]  # or per-version wheels if ABI not possible
```

### Target Files
- `.github/workflows/*.yml` (new or updated)
- `scripts/build_datafusion_wheels.sh`
- `rust/datafusion_python/Cargo.toml.orig`
- `rust/datafusion_python/Cargo.toml`
- `pyproject.toml`
- `docs/architecture/packaging.md` (new; one-page contract)

### Deprecate/Delete After Completion
- Ad-hoc, undocumented wheel builds outside CI.

### Implementation Checklist
- [x] Confirm artifact strategy: abi3 wheels as default.
- [x] Decide manylinux baseline (2014) and codify in CI + script.
- [x] Add CI workflow using `maturin-action` with deterministic args.
- [x] Validate module name invariants (Cargo `[lib].name` == `#[pymodule]` name).
- [x] Add post-build import + `capabilities_snapshot()` smoke test.

---

## Scope 1 — Streaming-first Arrow C Stream everywhere

### Problem
Eager `to_arrow_table()` breaks streaming and blows memory. Best-in-class integration uses `__arrow_c_stream__` end-to-end.

### Status
- Complete: streaming helpers (`src/arrow_utils/core/streaming.py`), coercion helpers, Rust `arrow_stream_to_batches`.
- Complete: Delta read/write paths are streaming-first with explicit eager fallbacks only when required (IPC/encoding alignment).

### Architecture and Code Pattern
Use stream as the default interchange object; only materialize when explicitly requested.

```python
# preferred write path
reader = pa.RecordBatchReader.from_stream(df)  # df implements __arrow_c_stream__
write_deltalake(uri, reader, storage_options=storage.to_deltalake_options())
```

```rust
#[pyfunction]
fn arrow_stream_to_batches(py: Python<'_>, obj: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let mut reader = ArrowArrayStreamReader::from_pyarrow_bound(&obj.bind(py))?;
    let schema = reader.schema();
    let batches: Vec<_> = reader.collect::<Result<_, _>>()?;
    record_batch_reader_from(schema, batches)
}
```

### Target Files
- `src/arrow_utils/core/streaming.py`
- `src/utils/value_coercion.py`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/io/write.py`
- `src/engine/delta_tools.py`
- `src/storage/ipc_utils.py`
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `src/datafusion_ext.pyi`

### Deprecate/Delete After Completion
- Any internal helper that materializes DataFrames via `to_arrow_table()` by default.

### Implementation Checklist
- [x] Treat Arrow C Stream as primary interchange (Python) across all Delta read/write entrypoints.
- [x] Provide explicit eager conversion helpers only when needed.
- [x] Migrate remaining Delta read/write paths to `RecordBatchReader`/`to_reader`.
- [x] Ensure Delta writes accept `ArrowStreamExportable` without materializing.

---

## Scope 2 — Unified storage policy (DataFusion + delta-rs)

### Problem
DataFusion object-store registration and delta-rs `storage_options` are currently resolved separately. This causes mismatched credentials, endpoints, and schemes.

### Status
- Complete: `DeltaStorePolicy` + policy application in `delta/service.py` and `session/runtime.py`.
- Complete: object-store registry alignment via `register_delta_object_store()` in Delta control-plane.

### Architecture and Code Pattern
Adopt `DeltaStorePolicy` as the single source of truth, and propagate it to both DataFusion and delta-rs.

```python
@dataclass(frozen=True)
class DeltaStorePolicy:
    storage_options: Mapping[str, str]
    log_storage_options: Mapping[str, str]
```

```python
storage, log_storage = resolve_delta_store_policy(
    table_uri=path,
    policy=profile.policies.delta_store_policy,
    storage_options=location.storage_options,
    log_storage_options=location.delta_log_storage_options,
)
```

### Target Files
- `src/datafusion_engine/delta/store_policy.py`
- `src/datafusion_engine/delta/service.py`
- `src/datafusion_engine/delta/plugin_options.py`
- `src/datafusion_engine/session/runtime.py`
- `src/datafusion_engine/io/adapter.py`
- `src/storage/deltalake/*`

### Deprecate/Delete After Completion
- Direct `storage_options` construction at call sites.

### Implementation Checklist
- [x] Replace “StorageProfile” references with `DeltaStorePolicy` in docs/plans.
- [x] Route all storage resolution through `DeltaStorePolicy` and plugin options.
- [x] Register object stores from policy into DataFusion runtime (host/scheme alignment).
- [x] Enforce canonical scheme (`s3://` vs `s3a://`) at policy boundaries.

---

## Scope 3 — Delta concurrency policy + retry classification

### Problem
Delta conflict exceptions have different correctness semantics; a blanket retry policy is unsafe.

### Status
- Complete: `DeltaRetryPolicy`/`DeltaMutationPolicy` implemented and enforced in `storage/deltalake/delta.py` mutations.

### Architecture and Code Pattern
Classify exceptions into retry/abort; enforce append-only and locking rules.

```python
class DeltaRetryPolicy:
    retryable = {"ConcurrentAppendException", "ConcurrentTransactionException"}
    fatal = {"MetadataChangedException", "ProtocolChangedException"}
```

### Target Files
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/delta/service.py`
- `src/engine/delta_tools.py`

### Deprecate/Delete After Completion
- Ad-hoc retry loops in Delta mutation helpers.

### Implementation Checklist
- [x] Introduce `DeltaRetryPolicy` and use it in mutation flows.
- [x] Enforce locking provider when using S3 backends.
- [x] Provide explicit “append-only” safety policy hooks.

---

## Scope 4 — Runtime cache configuration (metadata wins)

### Problem
Metadata IO dominates Delta performance; cache configuration is not a first-class surface.

### Status
- Complete: `CachePolicyConfig` + runtime settings in `session/runtime.py`.
- Complete: TTL/limits surfaced via config + “snapshot pinned” cache mode (per-dataset) implemented.

### Architecture and Code Pattern
Expose cache options via `CachePolicyConfig` and scan policy settings; apply to RuntimeEnv.

```python
class CachePolicyConfig(StructBaseStrict, frozen=True):
    listing_cache_size: int
    metadata_cache_size: int
    stats_cache_size: int | None = None
```

### Target Files
- `src/datafusion_engine/session/cache_policy.py`
- `src/datafusion_engine/session/runtime.py`
- `src/runtime_models/root.py`
- `src/cli/config_models.py`
- `docs/architecture/architectural_unification_deep_dive.md` (cache notes)

### Deprecate/Delete After Completion
- Hard-coded cache defaults in runtime factory paths.

### Implementation Checklist
- [x] Align plan text with `CachePolicyConfig` + scan-policy TTL controls.
- [x] Ensure TTL/limits are configurable via runtime profile or config models.
- [x] Provide safe “snapshot pinned” cache mode.

---

## Scope 5 — UDF correctness/performance gate (Rust)

### Problem
Rust UDFs can violate DataFusion invariants (row count, scalar paths, metadata-aware outputs).

### Status
- Complete: conformance tests in `rust/datafusion_ext/tests/udf_conformance.rs` and `docs/architecture/udf_quality_gate.md`.
- Complete: named parameter + short-circuit invariants covered.

### Architecture and Code Pattern
Define a “UDF quality gate” checklist and enforce in code review/tests.

```rust
// Example: use return_field_from_args for metadata-aware outputs
fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> { ... }
```

### Target Files
- `rust/datafusion_ext/src/udf/*.rs`
- `rust/datafusion_ext/tests/udf_conformance.rs`
- `docs/architecture/udf_quality_gate.md` (new)

### Deprecate/Delete After Completion
- UDF implementations that expand scalars to arrays without fast paths.

### Implementation Checklist
- [x] Enforce row-count invariant via `to_array_of_size`.
- [x] Require `return_field_from_args` when output depends on input metadata.
- [x] Validate `short_circuits` for lazy-eval UDFs.
- [x] Add named-parameter invariants to conformance tests.

---

## Scope 6 — Observability contract for DataFusion + Delta

### Problem
Current observability focuses on storage, not query-level metrics or Delta mutation telemetry.

### Status
- Complete: explain-analyze capture + thresholds in plan bundle/view graph.
- Complete: Delta tracing init + run_id/query_id span correlation.

### Architecture and Code Pattern
Capture `EXPLAIN ANALYZE` summary on slow queries and enable delta-rs tracing once per process.

```python
if duration_ms > threshold:
    df.explain(analyze=True, verbose=False)
```

### Target Files
- `src/obs/diagnostics_report.py`
- `src/datafusion_engine/lineage/diagnostics.py`
- `src/storage/deltalake/delta.py`

### Deprecate/Delete After Completion
- None (additive instrumentation).

### Implementation Checklist
- [x] Keep slow-query explain capture with bounded verbosity.
- [x] Call `deltalake.init_tracing()` once at startup.
- [x] Attach run_id + query_id to all Delta spans.

---

## Scope 7 — Compatibility handshake (Rust↔Python ABI)

### Problem
Runtime failures occur when Rust and Python layers drift (DataFusion/Arrow ABI mismatch).

### Status
- Complete: `capabilities_snapshot()` exposed in Rust + `datafusion_ext.pyi` and enforced in Python runtime.

### Architecture and Code Pattern
Use `capabilities_snapshot()` as a hard gate in Python.

```python
caps = datafusion_ext.capabilities_snapshot()
assert caps["plugin_abi"]["major"] == EXPECTED_ABI
```

### Target Files
- `src/datafusion_engine/udf/runtime.py`
- `src/datafusion_engine/session/runtime.py`
- `src/obs/diagnostics.py`

### Deprecate/Delete After Completion
- Silent fallbacks when Rust extensions are incompatible.

### Implementation Checklist
- [x] Gate runtime initialization on compatibility snapshot.
- [x] Store snapshot in diagnostics artifacts for reproducibility.
- [x] Emit a clear error when ABI mismatches.

---

## Scope 8 — Delta Mixins + session defaults alignment

### Problem
Delta schema and predicate parsing can diverge from delta-rs expectations unless DataFusionMixins are used consistently, and identifier normalization can break mixed-case columns.

### Architecture and Code Pattern
Use DataFusionMixins for predicate parsing and align session defaults to delta-rs guidance.

### Target Files
- `rust/datafusion_ext/src/delta_control_plane.rs`
- `src/datafusion_engine/delta/control_plane.py`
- `src/datafusion_engine/session/runtime.py`

### Deprecate/Delete After Completion
- Ad-hoc predicate parsing paths that bypass DataFusionMixins.

### Implementation Checklist
- [x] Ensure predicate parsing uses `read_schema()` and DataFusionMixins in Rust control-plane.
- [x] Add explicit switch to disable identifier normalization when Delta defaults enabled.
- [x] Record the session default flags in diagnostics payloads.

---

## Scope 9 — FFI TableProvider gating (Delta)

### Problem
Registering Delta tables via PyArrow dataset fallback loses filter pushdown and can regress performance.

### Architecture and Code Pattern
Detect which registration path is in use and emit diagnostics or enforce strict mode.

### Target Files
- `src/datafusion_engine/dataset/registration.py`
- `src/datafusion_engine/delta/service.py`
- `src/storage/deltalake/query_builder.py`

### Deprecate/Delete After Completion
- Silent fallback to Arrow Dataset registration for Delta tables.

### Implementation Checklist
- [x] Detect provider type at registration time (FFI TableProvider vs dataset fallback).
- [x] Emit diagnostics warning on fallback.
- [x] Add strict mode to forbid fallback in production profiles.

---

## Scope 10 — Delta CDF provider exposure

### Problem
CDF provider is available in Rust but not exposed as a first-class table in Python runtime.

### Architecture and Code Pattern
Expose CDF providers via the session registry to allow SQL querying against CDF as a table.

### Target Files
- `src/datafusion_engine/delta/cdf.py`
- `src/datafusion_engine/delta/control_plane.py`
- `src/datafusion_engine/session/facade.py`

### Deprecate/Delete After Completion
- Manual CDF batch extraction + re-registration patterns.

### Implementation Checklist
- [x] Add a CDF provider registration helper in the session facade.
- [x] Ensure storage options and feature gates propagate correctly.
- [x] Add a small conformance test for CDF provider registration.

---

## Scope 11 — QueryBuilder guardrails (ad-hoc only)

### Problem
Delta QueryBuilder uses its own embedded context and can drift from runtime policies and diagnostics.

### Architecture and Code Pattern
Treat QueryBuilder as ad-hoc only; prefer SessionContext + TableProvider for all pipeline work.

### Target Files
- `src/storage/deltalake/query_builder.py`
- `docs/architecture/architectural_unification_deep_dive.md`

### Deprecate/Delete After Completion
- QueryBuilder usage in pipeline or production pathways.

### Implementation Checklist
- [x] Add explicit warnings/diagnostics when QueryBuilder is used.
- [x] Update docs to position QueryBuilder as ad-hoc only.
- [x] Provide a SessionContext-based alternative in the API docs.

---

## Deliverables
- Deterministic Rust wheel pipeline (abi3 + manylinux policy).
- Streaming-first Arrow C Stream across all Rust↔Python data paths.
- Unified DeltaStorePolicy for DataFusion + delta-rs, including object-store registry alignment.
- Correctness-safe Delta concurrency policies.
- Cache policy surfaced and configurable.
- UDF quality gate + conformance tests enforced.
- Observable DataFusion/Delta runtime with diagnostics.
- Explicit compatibility handshake via `capabilities_snapshot()`.
- Delta Mixins + session defaults aligned with delta-rs best practices.
- FFI TableProvider gating with diagnostics and strict mode.
- CDF provider exposed as a first-class table.
- QueryBuilder guardrails and documentation.
