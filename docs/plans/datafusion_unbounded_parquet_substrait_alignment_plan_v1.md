# DataFusion Substrait + Wheel Feature Alignment Plan v1

Status: Proposed  
Owner: Codex  
Scope: Rust + wheel build + Python wiring to guarantee Substrait encoding availability and feature parity

## Objectives
- Ensure Substrait encoding is always available in our wheels (no feature gaps).
- Add build-time feature/compat diagnostics so integration regressions are caught early.
- Align DataFusion Python bindings with required feature flags for planning/audit workflows.
- Align tests with the registry-first semantic architecture used in production.

---

## Scope Item 1 — Substrait Feature Parity in Wheels

### Why
Substrait encoding fails when the wheel build does not enable the `substrait` feature. This is a build-time gap, not a code-level gap.

### Design
Enable `substrait` (and `protoc` if required) during `maturin build` for `datafusion_python`, and record the feature state in a manifest.

### Representative snippet
```bash
# scripts/build_datafusion_wheels.sh
uv run maturin build -m rust/datafusion_python/Cargo.toml \
  --${profile} --features substrait \
  "${manylinux_args[@]}" -o "${wheel_dir}"
```

### Target files
- `scripts/build_datafusion_wheels.sh`
- `rust/datafusion_python/Cargo.toml` (if feature gating needs adjustments)

### Deprecations / deletions
- None.

### Implementation checklist
- Add `--features substrait` to the build script.
- Add a post-build Python probe for `SubstraitProducer` availability.

---

## Scope Item 2 — Build Feature Manifest + Consistency Guard

### Why
We need durable visibility into which Rust features are active in shipped wheels to avoid silent regressions.

### Design
Emit a manifest capturing:
- datafusion/datafusion_ext version
- enabled feature flags (parquet, substrait, etc.)
- plugin path + build profile

### Representative snippet
```json
{
  "datafusion_version": "51.0.0",
  "features": ["parquet", "substrait"],
  "plugin_path": ".../libdf_plugin_codeanatomy.so",
  "build_profile": "release"
}
```

### Target files
- `scripts/build_datafusion_wheels.sh`
- `build/datafusion_plugin_manifest.json` (extend)

### Deprecations / deletions
- None.

### Implementation checklist
- Write a feature manifest after wheel build.
- Add a CI sanity check that fails if required features are missing.

---

## Scope Item 3 — Test Alignment with Registry-First Semantic Architecture

### Why
Several failing tests assume datasets exist in the SessionContext by default. Our production architecture is registry‑first and spec‑driven, so tests should register the semantic registry before calling `dataset_spec_from_context(...)` or plan‑bundle helpers. This is a causal alignment, not a fallback.

### Design
Introduce a single test helper that:
- Builds/loads the semantic registry
- Registers required dataset specs into a SessionContext
- Returns the SessionContext + SessionRuntime for tests that depend on registry‑derived specs

### Representative snippet
```python
# tests/test_helpers/semantic_registry_runtime.py
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from semantics.registry import semantic_registry
from semantics.catalog.dataset_registry import register_semantic_registry


def semantic_registry_runtime():
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    registry = semantic_registry()
    register_semantic_registry(ctx, registry)
    return ctx, profile.session_runtime()
```

### Target files
- `tests/test_helpers/semantic_registry_runtime.py` (new)
- `tests/unit/test_inferred_deps.py`
- `tests/unit/test_plan_bundle_artifacts.py`
- `tests/unit/test_lineage_plan_variants.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Add the registry runtime helper to `tests/test_helpers`.
- Update tests to use the helper before calling `dataset_spec_from_context(...)`.
- Ensure tests no longer assume implicit registry registration.

---

## Acceptance Gates
- Substrait encoding succeeds for LIMIT + EXPLAIN/EXPLAIN ANALYZE plans.
- Build manifest explicitly shows enabled features and is checked in CI.
- Registry‑dependent tests pass without implicit SessionContext state.

---

## Notes / Non-goals
- Not attempting to redesign Delta Lake protocol usage; this plan focuses on Substrait feature availability and wheel feature parity.
- No fallback behavior (skip/disable) is added for Substrait; we implement missing capability instead.
