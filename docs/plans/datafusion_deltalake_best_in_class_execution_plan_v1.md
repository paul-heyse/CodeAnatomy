# DataFusion + DeltaLake Best-in-Class Execution Plan v1

## Summary

This plan defines the target-state implementation for a best-in-class DeltaLake deployment integrated with DataFusion and Rust extensions.

Primary outcomes:
- Native Delta provider path is the default and enforced execution path.
- Rust control-plane and Python runtime use one compatibility and context-adaptation contract.
- Rust wheel packaging and capability exposure are deterministic, portable, and verifiable.
- Query execution paths are streaming-first and avoid non-native fallbacks in production.
- FFI panic boundaries are hardened to structured error contracts.
- Test profile enforces required capabilities (no skip-based optionality).
- Plan, config, and snapshot artifacts are deterministic and reviewable.

Design-phase policy: breaking changes are allowed where they improve correctness, determinism, performance, and operability.

---

## Scope 1 - Unify Delta extension compatibility and context adaptation

### Goal
Make capability probing and execution use the same context-adaptation rules so "compatible" means executable in production code paths.

### Representative code pattern

```python
# src/datafusion_engine/delta/capabilities.py

def probe_delta_entrypoint(ctx: Any, *, require_non_fallback: bool) -> DeltaProbeResult:
    for candidate in resolve_delta_entrypoints():
        for adapted in adapt_session_context(ctx, allow_fallback=not require_non_fallback):
            ok, error = try_invoke_probe(candidate, adapted)
            if ok:
                return DeltaProbeResult(
                    compatible=True,
                    entrypoint=candidate.name,
                    module=candidate.module,
                    ctx_kind=adapted.kind,
                    probe_result="ok",
                    error=None,
                )
    return DeltaProbeResult(compatible=False, probe_result="failed", error="no compatible entrypoint")
```

```python
# src/datafusion_engine/delta/control_plane.py

def delta_provider_from_session(ctx: Any, request: DeltaProviderRequest) -> TableProvider:
    probe = probe_delta_entrypoint(ctx, require_non_fallback=True)
    if not probe.compatible:
        raise DataFusionEngineError.plugin(
            f"Delta control-plane incompatible: entrypoint={probe.entrypoint} "
            f"ctx_kind={probe.ctx_kind} error={probe.error}"
        )
    return invoke_provider_entrypoint(ctx, request, probe.entrypoint)
```

### Target files
- `src/datafusion_engine/delta/capabilities.py`
- `src/datafusion_engine/delta/control_plane.py`
- `src/datafusion_engine/delta/plugin_options.py`
- `src/datafusion_engine/delta/service.py`
- `tests/unit/test_delta_extension_capabilities.py`

### Deprecate/delete after completion
- Context adaptation logic duplicated across capabilities and control-plane modules.
- Compatibility states that return `compatible=True` with fallback-only context when production path forbids fallback.

### Implementation checklist
- [ ] Add one shared context-adaptation helper and use it in probe + execution paths.
- [ ] Ensure probe payload includes `entrypoint`, `module`, `ctx_kind`, `probe_result`.
- [ ] Enforce non-fallback compatibility where provider entrypoints require it.
- [ ] Add unit tests for probe matrix and diagnostics payload shape.

---

## Scope 2 - Enforce native Delta provider and retire silent fallback

### Goal
Prevent runtime drift and performance regressions by removing silent fallback to `DeltaTable(...).to_pyarrow_dataset()` for production Delta execution.

### Representative code pattern

```python
# src/datafusion_engine/dataset/resolution.py

def _delta_provider_bundle(...):
    provider = delta_service.provider_from_location(location, ctx)
    if provider.kind != "delta_native":
        if profile.policies.enforce_delta_ffi_provider:
            raise DataFusionEngineError.plugin(
                "Native Delta provider required; non-native fallback is disabled"
            )
    return provider
```

```python
# src/datafusion_engine/dataset/registration.py

def register_delta_dataset(...):
    bundle = resolve_dataset_provider(...)
    assert bundle.provider_kind == "delta_native"
    return register_provider(ctx, bundle.provider)
```

### Target files
- `src/datafusion_engine/dataset/resolution.py`
- `src/datafusion_engine/dataset/registration.py`
- `src/datafusion_engine/delta/service.py`
- `src/storage/deltalake/delta.py`
- `tests/integration/test_pycapsule_provider_registry.py`
- `tests/integration/runtime/test_delta_provider_panic_containment.py`

### Deprecate/delete after completion
- `src/datafusion_engine/dataset/resolution.py::_fallback_delta_provider_bundle`
- `src/datafusion_engine/dataset/resolution.py::_fallback_delta_cdf_bundle`
- Silent downgrade paths from Delta provider to Arrow dataset in production profile.

### Implementation checklist
- [ ] Remove silent fallback registration in strict profile.
- [ ] Keep optional degraded mode explicit and opt-in only.
- [ ] Ensure CDF provider resolution follows the same strict policy.
- [ ] Update tests to assert native provider contract explicitly.

---

## Scope 3 - Consolidate production query path away from Delta QueryBuilder

### Goal
Treat QueryBuilder as ad-hoc tooling only and route production SQL/query operations through session runtime + native providers.

### Representative code pattern

```python
# src/storage/deltalake/delta.py

def query_delta_sql(...):
    if runtime_profile.policies.production_mode:
        return query_via_runtime_session(...)
    return query_via_querybuilder_for_debug(...)
```

```python
# src/storage/deltalake/query_builder.py
class DeltaTableQueryBuilder:
    """Deprecated for production execution. Kept for ad-hoc diagnostics only."""
```

### Target files
- `src/storage/deltalake/delta.py`
- `src/storage/deltalake/query_builder.py`
- `src/datafusion_engine/session/factory.py`
- `src/datafusion_engine/session/runtime.py`
- `tests/integration/test_delta_protocol_and_schema_mode.py`

### Deprecate/delete after completion
- Production call sites that invoke `DeltaTableQueryBuilder` directly.
- Ad-hoc embedded-context query paths in operational code.

### Implementation checklist
- [ ] Add explicit production guard routing to runtime session path.
- [ ] Mark QueryBuilder API as diagnostics-only in docs and type hints.
- [ ] Remove/replace production references to QueryBuilder.
- [ ] Validate pushdown/pruning via integration tests after migration.

---

## Scope 4 - Make wheel/plugin packaging deterministic and verifiable

### Goal
Guarantee runtime artifact integrity: Python module exports, plugin shared library presence, loadability checks, and reproducible release builds.

### Representative code pattern

```bash
# scripts/build_datafusion_wheels.sh
FEATURES="substrait,async-udf"

# Dev lane: fast local iteration
uv run maturin build --features "$FEATURES" --compatibility off --locked

# Release lane: portable artifacts
uv run maturin build --features "$FEATURES" --compatibility pypi --locked --sdist

uv run python - <<'PY'
import datafusion_ext
assert hasattr(datafusion_ext, "capabilities_snapshot")
manifest = datafusion_ext.plugin_manifest()
assert manifest["plugin_path"], manifest
PY
```

```rust
// rust/datafusion_python/src/codeanatomy_ext.rs
#[pymodule]
fn datafusion_ext(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(capabilities_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(plugin_manifest, m)?)?;
    Ok(())
}
```

### Target files
- `scripts/build_datafusion_wheels.sh`
- `scripts/rebuild_rust_artifacts.sh`
- `.github/workflows/wheels.yml`
- `rust/datafusion_ext_py/pyproject.toml`
- `rust/datafusion_ext_py/Cargo.toml`
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `tests/unit/test_test_env_capabilities.py`

### Deprecate/delete after completion
- Implicit packaging assumptions without artifact checks.
- Internal-only capability exports not available on public module surface.

### Implementation checklist
- [ ] Keep `abi3` baseline and module-name invariants enforced in CI.
- [ ] Ensure plugin artifact is bundled and discoverable in wheel layout.
- [ ] Export `capabilities_snapshot` from public module initialization.
- [ ] Add build-time post-check for plugin manifest + capability fields.
- [ ] Build with `--locked` in CI and publish both wheel + sdist artifacts.
- [ ] Keep manylinux/musllinux compliance as release-profile gate, not dev-profile blocker.

---

## Scope 5 - Harden Rust FFI panic boundaries to structured errors

### Goal
Eliminate panic/expect/unwrap behavior in FFI-adjacent runtime paths and convert failures to structured error contracts.

### Representative code pattern

```rust
// rust/datafusion_ext/src/async_runtime.rs
fn shared_runtime() -> datafusion_common::Result<&'static Runtime> {
    SHARED_RUNTIME
        .get_or_try_init(|| Runtime::new().map_err(|e| DataFusionError::Execution(e.to_string())))
}
```

```rust
// rust/datafusion_ext/src/planner_rules.rs
let cfg = ctx
    .session_config()
    .get_extension::<CodeanatomyPlannerConfig>()
    .ok_or_else(|| DataFusionError::Execution("missing planner config extension".into()))?;
```

### Target files
- `rust/datafusion_ext/src/async_runtime.rs`
- `rust/datafusion_ext/src/planner_rules.rs`
- `rust/datafusion_ext/src/physical_rules.rs`
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `tests/integration/runtime/test_delta_provider_panic_containment.py`

### Deprecate/delete after completion
- `expect()`/`unwrap()` in non-test FFI-adjacent runtime flow.
- Panic-based error signaling across Python/Rust boundary.

### Implementation checklist
- [ ] Replace panic-prone calls with `Result`-based propagation.
- [ ] Normalize Rust -> Python error mapping with contextual messages.
- [ ] Add panic-containment integration assertions for bad provider paths.
- [ ] Keep only harmless dead-code warnings (`unused`) as acceptable residual warnings.

---

## Scope 6 - Make capability baseline strict in tests (no skip-based optionality)

### Goal
Convert Delta/Substrait/async-UDF capability requirements from skip gates to fail-fast prerequisites in this repository profile.

### Representative code pattern

```python
# tests/test_helpers/optional_deps.py

def require_delta_extension() -> None:
    compat = is_delta_extension_compatible(df_ctx())
    if not compat.compatible:
        pytest.fail(f"Required delta extension capability missing: {compat}")
```

```python
# tests/unit/test_test_env_capabilities.py

def test_required_runtime_capabilities_present() -> None:
    caps = datafusion_ext.capabilities_snapshot()
    assert caps["delta_control_plane"]["available"] is True
    assert caps["substrait"]["available"] is True
    assert caps["async_udf"]["available"] is True
```

### Target files
- `tests/test_helpers/optional_deps.py`
- `tests/unit/test_test_env_capabilities.py`
- `tests/integration/test_delta_cache_alignment.py`
- `tests/unit/test_plan_bundle_artifacts.py`

### Deprecate/delete after completion
- Skip-based dependency gates for required runtime features in active test profile.

### Implementation checklist
- [ ] Replace `pytest.skip` usage for required capabilities with `pytest.fail`.
- [ ] Add one authoritative test-env capability sanity suite.
- [ ] Remove module-level skipif decorators for required feature paths.
- [ ] Validate `uv run pytest -q -rs` reports `0 skipped`.

---

## Scope 7 - Tighten Delta write/snapshot contracts

### Goal
Guarantee committed Delta mutations return deterministic version/snapshot metadata and maintain strict post-write visibility contract.

### Representative code pattern

```python
# src/storage/deltalake/delta.py

def write_delta_table_via_pipeline(...) -> DeltaWriteResult:
    canonical_uri = canonical_table_uri(table_uri)
    result = perform_delta_write(...)
    if result.committed and result.version is None:
        resolved = resolve_post_commit_version(canonical_uri, storage_options)
        if resolved is None:
            raise DataFusionEngineError.runtime("Committed Delta write missing version")
        result = result.with_version(resolved)
    return result.with_snapshot_key(canonical_uri, result.version)
```

### Target files
- `src/storage/deltalake/delta.py`
- `src/semantics/incremental/delta_updates.py`
- `src/semantics/incremental/write_helpers.py`
- `src/datafusion_engine/delta/store_policy.py`
- `tests/unit/test_incremental_delta_snapshots.py`
- `tests/incremental/test_view_artifacts.py`

### Deprecate/delete after completion
- Paths that allow `committed=True` with `version=None` in standard write flow.
- Paths that produce snapshot identity using non-canonical table URI forms.

### Implementation checklist
- [ ] Enforce version population for committed writes.
- [ ] Add fallback version resolve call where engine write API omits version.
- [ ] Canonicalize table URI before building snapshot identity.
- [ ] Update snapshot tests to assert strict committed-version contract.
- [ ] Confirm downstream CDF/snapshot diff logic uses resolved version.

---

## Scope 8 - Observability and diagnostics contract for Delta/DataFusion runtime

### Goal
Standardize diagnostics so capability, provider-kind, fallback-state, plugin metadata, explain settings, and operator metrics are visible in artifacts.

### Representative code pattern

```python
# src/datafusion_engine/session/runtime.py

def emit_runtime_capabilities(...) -> dict[str, Any]:
    caps = datafusion_ext.capabilities_snapshot()
    return {
        "delta_provider_policy": profile.policies.enforce_delta_ffi_provider,
        "capabilities": caps,
        "plugin_manifest": safe_plugin_manifest(),
        "df_settings": ctx.sql("select * from information_schema.df_settings").to_pylist(),
    }
```

```python
# src/datafusion_engine/lineage/diagnostics.py
def explain_summary(df: DataFrame) -> str:
    return df.explain(verbose=True, analyze=True)
```

### Target files
- `src/datafusion_engine/session/runtime.py`
- `src/datafusion_engine/dataset/registration.py`
- `src/datafusion_engine/delta/service.py`
- `src/datafusion_engine/lineage/diagnostics.py`
- `src/obs/diagnostics_report.py`

### Deprecate/delete after completion
- Diagnostics fields that do not include capability/provider metadata.
- Ambiguous "incompatible" messages without entrypoint/context details.

### Implementation checklist
- [ ] Emit capability snapshot and plugin manifest into runtime diagnostics.
- [ ] Include provider-kind/fallback metadata in dataset registration events.
- [ ] Capture `df_settings` in diagnostics artifacts for each run.
- [ ] Capture explain/metrics summary for selected slow queries.
- [ ] Normalize control-plane error messages with capability diagnostics.

---

## Scope 9 - SessionContext lifecycle and isolation contract

### Goal
Standardize session lifecycle, ephemeral registration, and cleanup semantics for deterministic isolation and no state leakage.

### Representative code pattern

```python
# src/datafusion_engine/session/factory.py

class DataFusionContextPool:
    def checkout(self) -> Iterator[SessionContext]:
        ctx = self._queue.get()
        try:
            yield ctx
        finally:
            cleanup_ephemeral_objects(ctx, prefix=self._active_run_prefix)
            self._queue.put(ctx)
```

```python
# src/datafusion_engine/compile/options.py
READ_ONLY_SQL = (
    SQLOptions()
    .with_allow_ddl(False)
    .with_allow_dml(False)
    .with_allow_statements(False)
)
```

### Target files
- `src/datafusion_engine/session/factory.py`
- `src/datafusion_engine/session/runtime.py`
- `src/datafusion_engine/compile/options.py`
- `src/datafusion_engine/semantics_runtime.py`
- `tests/integration/runtime/test_runtime_context_smoke.py`

### Deprecate/delete after completion
- Production use of shared global mutable `SessionContext` without explicit checkout/cleanup discipline.
- Query paths that allow unrestricted SQL statements in read-only services.

### Implementation checklist
- [ ] Implement checkout/cleanup lifecycle contract for pooled contexts.
- [ ] Introduce run-scoped ephemeral naming and cleanup sweep.
- [ ] Set read-only SQL defaults in service-facing query APIs.
- [ ] Add integration tests that verify no per-run table leakage across requests.

---

## Scope 10 - Arrow C Stream wire ABI contract

### Goal
Make Arrow C Stream the primary Rust<->Python and DataFusion<->Delta interchange contract with explicit materialization as opt-in only.

### Representative code pattern

```python
# src/datafusion_engine/arrow/interop.py

def as_reader(obj: ArrowStreamExportable) -> pa.RecordBatchReader:
    return pa.RecordBatchReader.from_stream(obj)
```

```rust
// rust/datafusion_python/src/codeanatomy_ext.rs
#[pyfunction]
fn consume_stream(obj: PyObject) -> PyResult<usize> {
    // extract Arrow stream-compatible object, avoid table materialization
    ...
}
```

### Target files
- `src/datafusion_engine/arrow/interop.py`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/io/write.py`
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `rust/datafusion_ext/src/udtf_sources.rs`
- `tests/unit/test_write_delta_table_streaming.py`

### Deprecate/delete after completion
- Core-path `to_arrow_table()`/`collect()` materialization patterns at engine boundaries.
- Rust bridge functions that require table-only ingestion for stream-capable flows.

### Implementation checklist
- [ ] Define and adopt `ArrowStreamExportable` contract in Python interfaces.
- [ ] Add `as_reader()` helper and route Delta writes through it.
- [ ] Ensure Rust bridge accepts any `__arrow_c_stream__` producer where applicable.
- [ ] Keep materialization helpers as explicit opt-in for small-result/debug paths.

---

## Scope 11 - DataFusion cache profile and staleness policy contract

### Goal
Introduce explicit cache profiles tied to snapshot and freshness policies, and prevent ad-hoc runtime mutation drift.

### Representative code pattern

```python
# src/datafusion_engine/session/runtime.py

CACHE_PROFILES = {
    "snapshot_pinned": {
        "datafusion.runtime.list_files_cache_limit": "64M",
        "datafusion.runtime.list_files_cache_ttl": None,
    },
    "always_latest_ttl30s": {
        "datafusion.runtime.list_files_cache_limit": "64M",
        "datafusion.runtime.list_files_cache_ttl": "30s",
    },
    "multi_tenant_strict": {
        "datafusion.runtime.list_files_cache_limit": "0K",
        "datafusion.runtime.metadata_cache_limit": "0",
    },
}
```

### Target files
- `src/datafusion_engine/session/runtime.py`
- `src/cli/config_models.py`
- `src/runtime_models/root.py`
- `src/serde_schema_registry.py`
- `tests/unit/test_cache_snapshot_pinned_prefix.py`
- `tests/unit/test_diskcache_profile_overrides.py`

### Deprecate/delete after completion
- Unstructured cache key/value writes spread across unrelated modules.
- Service paths that allow user SQL `SET` mutation of pinned runtime policy.

### Implementation checklist
- [ ] Add named cache profiles with explicit staleness semantics.
- [ ] Apply cache profile at context construction time.
- [ ] Prevent mutable SQL settings in read-only service mode.
- [ ] Add tests that verify profile application and cache-prefix behavior.

---

## Scope 12 - StorageProfile and object-store/locking hardening

### Goal
Use one canonical storage profile for DataFusion object stores and delta-rs `storage_options`, with explicit lock-safety policy.

### Representative code pattern

```python
# src/datafusion_engine/delta/store_policy.py

class StorageProfile:
    def to_datafusion_object_store(self) -> ObjectStoreSpec: ...
    def to_deltalake_options(self) -> dict[str, str]: ...
    def canonicalize_uri(self, uri: str) -> str: ...
```

```python
# src/storage/deltalake/delta.py
def validate_write_safety(uri: str, profile: StorageProfile) -> None:
    if profile.backend == "aws_s3" and profile.locking_provider != "dynamodb":
        raise DataFusionEngineError.config("AWS S3 writes require locking provider")
```

### Target files
- `src/datafusion_engine/delta/store_policy.py`
- `src/datafusion_engine/delta/object_store.py`
- `src/datafusion_engine/delta/plugin_options.py`
- `src/datafusion_engine/delta/control_plane.py`
- `src/storage/deltalake/delta.py`
- `tests/integration/runtime/test_object_store_registration_contracts.py`

### Deprecate/delete after completion
- Split credential/endpoint configuration paths for DataFusion vs delta-rs.
- Bucket-specific object-store registration patterns where scheme-root registration is required.

### Implementation checklist
- [ ] Enforce one canonical URI scheme per dataset.
- [ ] Emit both DataFusion and delta-rs configs from one storage profile.
- [ ] Add explicit lock policy validation for AWS S3 concurrent writes.
- [ ] Add endpoint consistency checks for S3-compatible stores.

---

## Scope 13 - Deterministic plan/config/snapshot manifest contract

### Goal
Persist one deterministic manifest per query/run that captures plans, config, and snapshot identity.

### Representative code pattern

```python
# src/datafusion_engine/plan/bundle.py

def capture_plan_manifest(ctx: SessionContext, df: DataFrame, snapshot_keys: dict[str, SnapshotKey]) -> PlanManifest:
    return PlanManifest(
        optimized_logical_plan=df.optimized_logical_plan().display_indent_schema(),
        physical_plan_proto_b64=encode_or_none(df.execution_plan().to_proto()),
        df_settings=ctx.sql("select name, setting from information_schema.df_settings").to_pylist(),
        snapshot_keys=snapshot_keys,
    )
```

### Target files
- `src/datafusion_engine/plan/bundle.py`
- `src/obs/diagnostics_report.py`
- `src/storage/deltalake/delta.py`
- `tests/plan_golden/test_plan_artifacts.py`
- `tests/unit/test_plan_bundle_artifacts.py`

### Deprecate/delete after completion
- Artifact outputs missing snapshot identity or config snapshot.
- Ad-hoc plan dumps that omit optimized plan or physical plan representation.

### Implementation checklist
- [ ] Capture optimized logical + physical plan (proto where supported, text fallback otherwise).
- [ ] Capture full `df_settings` snapshot from `information_schema`.
- [ ] Persist per-table `SnapshotKey = (canonical_uri, resolved_version)`.
- [ ] Capture write/concurrency outcomes in manifest payload.

---

## Scope 14 - Integration conformance harness expansion

### Goal
Establish layered conformance tests for deterministic behavior across local and object-store deployments, including locking and retention semantics.

### Representative code pattern

```python
# tests/harness/snapshot_key.py
def assert_snapshot_pinned(uri: str, version: int, storage_options: dict[str, str]) -> None:
    dt_latest = DeltaTable(uri, storage_options=storage_options)
    dt_pinned = DeltaTable(uri, version=version, storage_options=storage_options)
    assert dt_latest.version() >= dt_pinned.version()
```

```python
# tests/integration/test_concurrent_writes_localstack.py
def test_locking_enabled_concurrent_append(...):
    # two concurrent writers append to the same table; both commits must succeed
    ...
```

### Target files
- `tests/harness/profiles.py`
- `tests/harness/plan_bundle.py`
- `tests/harness/snapshot_key.py`
- `tests/integration/test_df_delta_smoke.py`
- `tests/integration/test_snapshot_identity.py`
- `tests/integration/test_vacuum_retention.py`
- `tests/integration/test_cleanup_metadata.py`
- `tests/integration/test_concurrent_writes_localstack.py`

### Deprecate/delete after completion
- Integration tests that validate only happy-path local filesystem behavior for critical Delta contracts.
- Non-deterministic integration tests without seeded data or plan/snapshot artifact assertions.

### Implementation checklist
- [ ] Add layered suites: local FS, MinIO, LocalStack+DynamoDB.
- [ ] Add deterministic table seeding helpers.
- [ ] Add conflict classifier conformance tests (retryable vs fatal families).
- [ ] Add VACUUM/log retention behavioral tests.
- [ ] Record plan/config/snapshot artifacts for failing conformance runs.

---

## Cross-Scope Decommission Map

Remove only after all dependent call sites are migrated:
- `src/datafusion_engine/dataset/resolution.py::_fallback_delta_provider_bundle`
- `src/datafusion_engine/dataset/resolution.py::_fallback_delta_cdf_bundle`
- Production-path call sites to `DeltaTableQueryBuilder` in `src/storage/deltalake/delta.py`
- Skip-based required-capability helpers in `tests/test_helpers/optional_deps.py`
- Any production boundary helper that materializes by default instead of streaming.

---

## Validation and Acceptance Criteria

### Required commands
```bash
uv run pytest -q -rs
uv run pytest -W error -q
uv run pytest -q
```

### Extended conformance lanes
```bash
uv run pytest -q -m "not localstack and not minio"
uv run pytest -q -m "minio"
uv run pytest -q -m "localstack"
```

### Required outcomes
- `0 failed`
- `0 skipped`
- `0 warnings`
- `build/test-results/junit.xml` reports `skipped="0"` and no `<skipped>` test case nodes.

### Runtime contract verification
- `datafusion_ext.capabilities_snapshot()` is callable from public module import.
- `datafusion_ext.plugin_manifest()` resolves a valid plugin artifact path.
- Delta provider registration path reports `provider_kind="delta_native"` in strict profile.
- Query result APIs return stream-compatible objects by default.
- Committed Delta writes always return a resolved snapshot version.

---

## Sequencing

1. Scope 4 (packaging/capabilities baseline)
2. Scope 1 -> Scope 2 (compatibility/probe correctness before provider enforcement)
3. Scope 9 (session lifecycle isolation)
4. Scope 10 + Scope 3 (wire ABI + query-path migration)
5. Scope 12 (storage/locking hardening)
6. Scope 11 (cache profile policy)
7. Scope 7 (snapshot/write contract strictness)
8. Scope 5 (Rust panic hardening)
9. Scope 13 + Scope 8 (deterministic manifests + observability)
10. Scope 14 (conformance harness)
11. Final decommission pass
12. Full validation matrix

---

## Assumptions and Defaults

- Design-phase breaking changes are acceptable where they improve robustness and determinism.
- Dev/test wheel builds prioritize local reproducibility; manylinux/musllinux compliance is release-profile policy and not required for local iteration.
- Delta/Substrait/async-UDF are required capabilities for this repository test profile.
- Native Delta provider is mandatory in strict profile; degraded fallback paths are explicit and opt-in only.
- Stream-first interchange is the default interface contract; eager materialization is opt-in.
