# Pytest Failure Remediation Plan (Rust-First DataFusion Pivot) v1

**Date:** 2026-02-12  
**Primary evidence source:** `build/test-results/junit.xml` (full-suite baseline parsed before later targeted reruns)  
**Baseline analyzed:** 2726 tests, 85 failures, 22 skipped

## 1) Failure Review (Clustered From `junit.xml`)

### Cluster A: Rust UDF registry snapshot ABI mismatch (47 failures)
- Primary exception origin: `src/datafusion_engine/udf/runtime.py:133`
- Primary message: `TypeError: ... SessionContext ABI mismatch ... cannot be converted ...`
- Fanout path observed across failing tests:
`build_session_runtime()` -> `rust_udf_snapshot()` -> `_build_registry_snapshot()`
- High-impact affected surfaces:
`extract/*`, `semantics/*`, `session/runtime`, plan artifacts, function registry expectations

### Cluster B: Missing Rust `SchemaRuntime` bridge (22 failures)
- Primary exception origin: `src/schema_spec/contracts.py:569`
- Primary message:
`RuntimeError: codeanatomy_engine.SchemaRuntime is required ...`
- Fanout path observed:
`resolve_dataset_provider()` -> `resolve_delta_scan_options()` -> `apply_delta_scan_policy()` -> `_schema_runtime()`
- High-impact affected surfaces:
dataset resolution, zero-row bootstrap, provider registry, scan-policy tests

### Cluster C: Platform returned no snapshot (11 failures)
- Primary exception origin: `src/datafusion_engine/udf/platform.py:416`
- Primary message:
`RuntimeError: Rust UDF platform did not return a registry snapshot.`
- High-impact affected surfaces:
`test_async_udf_policy`, `test_datafusion_schema_registry`, function registry snapshots

### Edge failures (5 total)
- 1: FunctionFactory install contract test expects `installed=True`, receives mismatch error payload.
- 1: `stable_hash64` SQL function unavailable in integration path.
- 1: zero-row internal table bootstrap returns `None`.
- 1: schema contract golden drift (`ValidationPolicySpec` description changed after Pandera cutover).
- 1: seeded zero-row bootstrap assertion cascades from scan-policy/SchemaRuntime failure.

## 2) Rust-Pivot Constraints For Fixes

- Do **not** reintroduce Python fallback execution for UDF/scan-policy authority.
- Fixes must increase Rust authority and reduce Python bridge logic.
- SessionContext compatibility must be a first-class contract, not a best-effort probe.
- Packaging/bootstrap must guarantee Rust extension availability before runtime logic executes.

## 3) Execution Scopes

## Scope 1: Enforce SessionContext ABI Contract At Extension Boundary

**Addresses:** Cluster A, Cluster C, FunctionFactory edge failure.

### Representative code snippet

```rust
// rust/datafusion_python/src/codeanatomy_ext.rs
#[pyfunction]
fn session_context_contract_probe(py: Python<'_>, ctx: PyRef<PySessionContext>) -> PyResult<Py<PyAny>> {
    let state = ctx.ctx.state();
    let snapshot = registry_snapshot::registry_snapshot(&state);
    let payload = serde_json::json!({
        "ok": true,
        "plugin_abi": { "major": DF_PLUGIN_ABI_MAJOR, "minor": DF_PLUGIN_ABI_MINOR },
        "udf_counts": { "scalar": snapshot.scalar.len(), "aggregate": snapshot.aggregate.len() }
    });
    json_to_py(py, &payload)
}
```

```python
# src/datafusion_engine/udf/runtime.py
def validate_extension_capabilities(*, strict: bool = True) -> dict[str, object]:
    report = extension_capabilities_report()
    if not report.get("compatible", False):
        if strict:
            raise RuntimeError(report.get("error") or "Extension ABI compatibility check failed.")
        return report
    probe = getattr(_datafusion_internal(), "session_context_contract_probe", None)
    if callable(probe):
        probe(SessionContext())  # hard fail if conversion is broken
    return report
```

### Target files to edit/create
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `rust/datafusion_python/src/lib.rs` (export new pyfunction)
- `src/datafusion_engine/udf/runtime.py`
- `src/datafusion_engine/udf/platform.py`
- `tests/unit/test_delta_extension_capabilities.py`
- `tests/unit/test_fallback_udf_snapshot.py` (or successor contract tests)

### Decommission/delete after completion
- Remove `_module_ctx_arg` double-dispatch retries for `ctx` vs `ctx.ctx` in UDF snapshot path.
- Remove `_probe_registry_snapshot()` heuristic probing in `src/datafusion_engine/udf/platform.py`.
- Remove duplicated SessionContext conversion fallback branches in:
`src/datafusion_engine/udf/runtime.py` and `src/datafusion_engine/udf/factory.py`.

### Implementation checklist
- [ ] Add Rust probe API and expose through module init.
- [ ] Invoke probe from strict capability validation.
- [ ] Convert ABI mismatch from deep runtime TypeError to early deterministic startup error.
- [ ] Update tests to assert early contract failure semantics.

---

## Scope 2: Unify Rust Platform Bootstrap Into One Authoritative Entrypoint

**Addresses:** Cluster A, Cluster C, `stable_hash64` edge failure.

### Representative code snippet

```rust
// rust/datafusion_python/src/codeanatomy_ext.rs
#[pyfunction]
fn install_codeanatomy_runtime(
    py: Python<'_>,
    ctx: PyRef<PySessionContext>,
    enable_async_udfs: bool,
    timeout_ms: Option<u64>,
    batch_size: Option<usize>,
) -> PyResult<Py<PyAny>> {
    udf_registry::register_all(&ctx.ctx)
        .map_err(|e| PyRuntimeError::new_err(format!("UDF registration failed: {e}")))?;
    let snapshot = registry_snapshot::registry_snapshot(&ctx.ctx.state());
    let payload = serde_json::json!({
        "snapshot": snapshot,
        "function_factory_installed": true,
        "expr_planners_installed": true,
        "async": { "enabled": enable_async_udfs, "timeout_ms": timeout_ms, "batch_size": batch_size }
    });
    json_to_py(py, &payload)
}
```

```python
# src/datafusion_engine/udf/platform.py
payload = internal.install_codeanatomy_runtime(ctx_arg, enable_async, timeout_ms, batch_size)
snapshot = cast(Mapping[str, object], payload["snapshot"])
```

### Target files to edit/create
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `src/datafusion_engine/udf/platform.py`
- `src/datafusion_engine/udf/runtime.py`
- `src/datafusion_engine/udf/factory.py`
- `src/datafusion_engine/session/runtime.py`
- `tests/unit/test_function_registry_snapshot.py`
- `tests/integration/test_pycapsule_udf_registry.py`

### Decommission/delete after completion
- Remove split bootstrap logic:
`register_rust_udfs()` + `_install_function_factory()` + `_install_expr_planners()` orchestration in Python.
- Remove Python-side FunctionFactory policy synthesis fallback path where Rust already returns canonical payload.
- Remove ad hoc retry logic based on message text (`"cannot be converted"` checks).

### Implementation checklist
- [ ] Add consolidated Rust bootstrap API returning typed payload.
- [ ] Replace Python multi-step platform install with single Rust call.
- [ ] Ensure payload includes registry snapshot used for `stable_hash64` and required UDF validation.
- [ ] Update integration tests to validate function availability through returned snapshot.

---

## Scope 3: Make `codeanatomy_engine` Wheel A First-Class Environment Dependency

**Addresses:** Cluster B and all SchemaRuntime-cascade failures.

### Representative code snippet

```toml
# pyproject.toml
[project]
dependencies = [
  "datafusion>=50.1.0",
  "datafusion_ext>=0.1.0",
  "codeanatomy_engine>=0.1.0",
]

[tool.uv.sources]
datafusion = { path = "dist/wheels/datafusion-51.0.0-cp310-abi3-macosx_11_0_arm64.whl" }
datafusion_ext = { path = "dist/wheels/datafusion_ext-0.1.0-cp310-abi3-macosx_11_0_arm64.whl" }
codeanatomy_engine = { path = "dist/wheels/codeanatomy_engine-0.1.0-cp310-abi3-macosx_11_0_arm64.whl" }
```

```bash
# scripts/bootstrap_codex.sh (add)
python - <<'PY'
import importlib
mod = importlib.import_module("codeanatomy_engine")
assert hasattr(mod, "SchemaRuntime")
PY
```

### Target files to edit/create
- `pyproject.toml`
- `uv.lock`
- `scripts/bootstrap_codex.sh`
- `scripts/build_datafusion_wheels.sh` (ensure `codeanatomy_engine` wheel is always staged and verified)
- `src/schema_spec/contracts.py`
- `tests/integration/storage/test_resolve_dataset_provider.py`

### Decommission/delete after completion
- Remove “extension missing” runtime guidance paths that assume `codeanatomy_engine` may be absent in supported dev/test setup.
- Remove redundant import checks scattered in tests once bootstrap guarantees module presence.

### Implementation checklist
- [ ] Add `codeanatomy_engine` to locked dependencies.
- [ ] Enforce import checks in bootstrap script.
- [ ] Re-run `uv sync` and verify `import codeanatomy_engine`.
- [ ] Confirm scan-policy resolution executes without ModuleNotFoundError.

---

## Scope 4: Consolidate Schema Policy Calls Behind Shared Rust Runtime Loader

**Addresses:** Cluster B fanout, zero-row bootstrap cascade failures.

### Representative code snippet

```python
# src/datafusion_engine/extensions/schema_runtime.py (new)
from functools import lru_cache
import importlib

@lru_cache(maxsize=1)
def load_schema_runtime():
    mod = importlib.import_module("codeanatomy_engine")
    runtime_cls = getattr(mod, "SchemaRuntime")
    return runtime_cls()
```

```python
# src/schema_spec/contracts.py
runtime = load_schema_runtime()
merged_json = runtime.apply_delta_scan_policy_json(base_json, defaults_json)
return msgspec.json.decode(merged_json, type=DeltaScanOptions)
```

### Target files to edit/create
- `src/datafusion_engine/extensions/schema_runtime.py` (new)
- `src/datafusion_engine/extensions/__init__.py`
- `src/schema_spec/contracts.py`
- `src/datafusion_engine/delta/scan_config.py`
- `src/datafusion_engine/dataset/policies.py`
- `tests/unit/test_dataset_resolution_precedence.py`
- `tests/unit/datafusion_engine/test_zero_row_bootstrap.py`

### Decommission/delete after completion
- Delete `_schema_runtime()` implementation from `src/schema_spec/contracts.py`.
- Remove duplicated runtime-loading logic in any other module using `SchemaRuntime`.
- Remove legacy direct import probes in policy paths.

### Implementation checklist
- [ ] Introduce shared loader module and swap callsites.
- [ ] Keep strict errors but centralize error text and diagnostics shape.
- [ ] Validate dataset resolution and zero-row bootstrap pass with Rust runtime present.
- [ ] Confirm no remaining duplicate runtime loaders via CQ search.

---

## Scope 5: Normalize Test Harness For Native-Required Paths

**Addresses:** Cluster C and edge failures currently asserting outdated behavior.

### Representative code snippet

```python
# tests/conftest.py
import pytest
from datafusion_engine.udf.runtime import validate_extension_capabilities

@pytest.fixture(scope="session")
def require_native_runtime() -> None:
    try:
        validate_extension_capabilities(strict=True)
    except RuntimeError as exc:
        pytest.skip(f"native runtime contract unavailable: {exc}")
```

```python
# tests/unit/test_async_udf_policy.py
def test_async_udf_policy_requires_enable_flag(require_native_runtime) -> None:
    ...
```

### Target files to edit/create
- `tests/conftest.py`
- `tests/test_helpers/optional_deps.py`
- `tests/unit/test_async_udf_policy.py`
- `tests/unit/test_datafusion_schema_registry.py`
- `tests/integration/test_create_function_factory.py`
- `tests/integration/test_pycapsule_udf_registry.py`

### Decommission/delete after completion
- Remove per-test ad hoc extension availability checks.
- Remove assertions that rely on fallback/soft-fail semantics incompatible with strict rust-first contract.

### Implementation checklist
- [ ] Add shared fixture/marker for native-required tests.
- [ ] Update tests to assert contract outcomes, not incidental fallback behavior.
- [ ] Verify `stable_hash64` test executes only after runtime install succeeds.

---

## Scope 6: Golden Contract Cleanup And Drift Control

**Addresses:** schema registry golden edge failure.

### Representative code snippet

```diff
- "description": "Runtime DataFrame validation policy (Pandera)."
+ "description": "Runtime DataFrame validation policy."
```

### Target files to edit/create
- `tests/msgspec_contract/goldens/schema_registry.json`
- `tests/msgspec_contract/test_contract_schema.py`
- `src/serde_schema_registry.py` (if description source is updated there)

### Decommission/delete after completion
- Remove stale Pandera terminology in schema contract docs and test expectations.

### Implementation checklist
- [ ] Confirm description source of truth.
- [ ] Update golden intentionally.
- [ ] Re-run msgspec contract tests.

---

## Scope 7: Final Python Bridge Decommission (Post-Green)

**Addresses:** residual complexity that caused the current failures to fan out.

### Representative code snippet

```python
# keep thin wrapper only
def rust_udf_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    return install_rust_udf_platform(ctx, options=RustUdfPlatformOptions(strict=True)).snapshot
```

### Target files to edit/create
- `src/datafusion_engine/udf/runtime.py`
- `src/datafusion_engine/udf/platform.py`
- `src/datafusion_engine/udf/factory.py`
- `src/datafusion_engine/session/runtime.py`
- `docs/architecture/cq/08_neighborhood_subsystem.md` (ownership boundary update if relevant)

### Decommission/delete after completion
- Remove dead helper paths for split install/probe logic.
- Remove stale “soft fallback” diagnostics that are no longer reachable.

### Implementation checklist
- [ ] CQ zero-hit checks for deprecated helpers.
- [ ] Delete unreachable helper code and update imports.
- [ ] Keep behavior parity verified by targeted integration tests.

## 4) Recommended Execution Order

1. Scope 3 (packaging + dependency truth)  
2. Scope 1 (ABI contract probe)  
3. Scope 2 (single bootstrap path)  
4. Scope 4 (schema policy loader consolidation)  
5. Scope 5 (test harness normalization)  
6. Scope 6 (golden cleanup)  
7. Scope 7 (final decommission sweep)

## 5) Validation Gates

- Targeted per-scope tests first (affected files only).
- Then full gate:

```bash
uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q
```

- CQ regression checks before merge:

```bash
./cq search "_probe_registry_snapshot" --in src --format summary
./cq search "_schema_runtime" --in src --format summary
./cq search "cannot be converted" --in src/datafusion_engine --format summary
./cq search "FunctionFactory installation failed; native extension is required" --in src --format summary
```

## 6) Expected Outcome

- The 47 + 22 + 11 dominant failures are removed by contract-correct runtime bootstrap and guaranteed Rust module presence.
- Edge failures collapse naturally once contract and packaging are correct; only the schema golden update should remain as an intentional content change.
- Python `src/` surface area shrinks further in favor of Rust-owned DataFusion runtime authority, consistent with the active pivot plan.
