# Architectural Unification Best-in-Class Integration Addendum Plan v1

Status: Proposed (design-phase addendum)
Owner: Codex (proposed)

This plan captures **additional best-in-class improvements** to Rust + Python DataFusion/Delta integration, aligned with:
- `docs/architecture/architectural_unification_deep_dive.md`
- `docs/plans/architectural_unification_scope_alignment_plan_v1.md`

The scope below is intentionally additive and does **not** redesign the existing architecture. It focuses on packaging, streaming, storage/credential unification, Delta concurrency correctness, cache tuning, UDF quality gates, observability, and compatibility handshakes.

---

## Scope 0 — Packaging & wheel pipeline hardening (abi3/manylinux)

### Problem
Best-in-class Rust↔Python integrations fail in production without deterministic, portable wheels and ABI-compatible packaging. The current plan does not codify the packaging contract or CI targets.

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
- `pyproject.toml`
- `docs/architecture/packaging.md` (new; one-page contract)

### Deprecate/Delete After Completion
- Ad-hoc, undocumented wheel builds outside CI.

### Implementation Checklist
- [ ] Choose artifact strategy: abi3 vs per-Python wheels.
- [ ] Enforce manylinux/musllinux policy in CI (maturin-action).
- [ ] Add compatibility smoke test: `capabilities_snapshot()` executed post-build.
- [ ] Document packaging invariants (module name vs lib name).

---

## Scope 1 — Streaming-first Arrow C Stream everywhere

### Problem
Eager `to_arrow_table()` breaks streaming and blows memory. Best-in-class integration uses `__arrow_c_stream__` end-to-end.

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
- `src/datafusion_engine/arrow/build.py`
- `src/datafusion_engine/io/write.py`
- `src/datafusion_engine/delta/service.py`
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `src/datafusion_ext.pyi`

### Deprecate/Delete After Completion
- Any internal helper that materializes DataFrames via `to_arrow_table()` by default.

### Implementation Checklist
- [ ] Treat Arrow C Stream as primary interchange (Python).
- [ ] Provide explicit eager conversion helpers only when needed.
- [ ] Ensure Delta writes accept `ArrowStreamExportable`.

---

## Scope 2 — Unified StorageProfile (DataFusion + delta-rs)

### Problem
DataFusion object-store registration and delta-rs `storage_options` are currently resolved separately. This causes mismatched credentials, endpoints, and schemes.

### Architecture and Code Pattern
Single profile that yields both surfaces.

```python
class StorageProfile:
    def to_datafusion_object_store(self) -> object: ...
    def to_deltalake_options(self) -> dict[str, str]: ...
```

### Target Files
- `src/datafusion_engine/delta/service.py`
- `src/datafusion_engine/delta/plugin_options.py`
- `src/datafusion_engine/session/runtime.py`
- `src/storage/deltalake/*`

### Deprecate/Delete After Completion
- Direct `storage_options` construction at call sites.

### Implementation Checklist
- [ ] Introduce `StorageProfile` and enforce canonical scheme (`s3://` vs `s3a://`).
- [ ] Route all storage resolution through the profile.
- [ ] Add validation for lock/endpoint consistency.

---

## Scope 3 — Delta concurrency policy + retry classification

### Problem
Delta conflict exceptions have different correctness semantics; a blanket retry policy is unsafe.

### Architecture and Code Pattern
Classify exceptions into retry/abort; enforce append-only and locking rules.

```python
class DeltaRetryPolicy:
    retryable = {"ConcurrentAppendException"}
    fatal = {"MetadataChangedException", "ProtocolChangedException"}
```

### Target Files
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/delta/service.py`
- `src/engine/delta_tools.py`

### Deprecate/Delete After Completion
- Ad-hoc retry loops in Delta mutation helpers.

### Implementation Checklist
- [ ] Introduce `DeltaRetryPolicy` and use it in mutation flows.
- [ ] Enforce locking provider when using S3 backends.
- [ ] Provide explicit “append-only” safety policy hooks.

---

## Scope 4 — Runtime cache configuration (metadata wins)

### Problem
Metadata IO dominates Delta performance; cache configuration is not a first-class surface.

### Architecture and Code Pattern
Expose cache options via `DataFusionRuntimeProfile` and apply to RuntimeEnv.

```python
@msgspec.Struct(frozen=True)
class CacheConfig:
    list_files_ttl_s: int | None
    parquet_meta_ttl_s: int | None
```

### Target Files
- `src/datafusion_engine/session/runtime.py`
- `src/datafusion_engine/session/factory.py`
- `docs/architecture/architectural_unification_deep_dive.md` (cache notes)

### Deprecate/Delete After Completion
- Hard-coded cache defaults in runtime factory paths.

### Implementation Checklist
- [ ] Add cache config to `DataFusionRuntimeProfile`.
- [ ] Apply cache config to RuntimeEnvBuilder.
- [ ] Provide safe “snapshot pinned” cache mode.

---

## Scope 5 — UDF correctness/performance gate (Rust)

### Problem
Rust UDFs can violate DataFusion invariants (row count, scalar paths, metadata-aware outputs).

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
- [ ] Enforce row-count invariant via `to_array_of_size`.
- [ ] Ensure `return_field_from_args` used where output depends on inputs.
- [ ] Add UDF tests for scalar fast paths and nullability.

---

## Scope 6 — Observability contract for DataFusion + Delta

### Problem
Current observability focuses on storage, not query-level metrics or Delta mutation telemetry.

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
- [ ] Add slow-query explain capture with bounded verbosity.
- [ ] Call `deltalake.init_tracing()` once at startup.
- [ ] Attach run_id + query_id to all Delta spans.

---

## Scope 7 — Compatibility handshake (Rust↔Python ABI)

### Problem
Runtime failures occur when Rust and Python layers drift (DataFusion/Arrow ABI mismatch).

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
- [ ] Gate runtime initialization on compatibility snapshot.
- [ ] Store snapshot in diagnostics artifacts for reproducibility.
- [ ] Emit a clear error when ABI mismatches.

---

## Deliverables
- Deterministic Rust wheel pipeline (abi3/manylinux policy).
- Streaming-first Arrow C Stream across all Rust↔Python data paths.
- Unified StorageProfile for DataFusion + delta-rs.
- Correctness-safe Delta concurrency policies.
- DataFusion cache surface exposed and configured.
- UDF quality gate + tests.
- Observable DataFusion/Delta runtime with diagnostics.
- Explicit compatibility handshake via `capabilities_snapshot()`.

