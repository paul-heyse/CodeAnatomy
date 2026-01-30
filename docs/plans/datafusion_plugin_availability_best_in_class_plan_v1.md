# DataFusion Plugin Availability Best-in-Class Plan (v1)

This plan defines a **best-in-class**, breaking-change–friendly path to make DataFusion plugin discovery
and capability utilization **always available, deterministic, and self-validating**.

## Goals

- Guarantee plugin availability for every runtime and test by default.
- Eliminate silent downgrades when plugin libraries are missing or mismatched.
- Provide a single canonical discovery/validation surface used everywhere.
- Package plugin artifacts with the Python extension to remove path guesswork.
- Record and validate ABI/DataFusion/Arrow compatibility in diagnostics.

## Guiding Principles

- **Fail fast** on missing or incompatible plugins (no implicit optional path).
- **Single source of truth** for plugin discovery, diagnostics, and manifest validation.
- **Packaging-first**: the Python extension ships with the plugin library it expects.
- **Determinism**: runtime + tests must behave identically regardless of machine state.

---

## Scope S1: Canonical Plugin Discovery + Enforcement Layer

**Target State:** All plugin discovery and validation flows through a single canonical module.
Missing or incompatible plugins raise deterministic errors before runtime execution.

**Representative pattern** (canonical discovery API):

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from datafusion_engine.plugin_discovery import assert_plugin_available, resolve_plugin_path


@dataclass(frozen=True)
class PluginResolution:
    path: Path
    manifest_hash: str


def resolve_plugin() -> PluginResolution:
    assert_plugin_available()
    return PluginResolution(
        path=resolve_plugin_path(),
        manifest_hash="sha256:...",
    )
```

**Target files to modify**
- `src/datafusion_engine/plugin_discovery.py` (new)
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/plugin_manager.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/obs/diagnostics.py` (diagnostic payload integration)

**Delete after migration**
- `_default_df_plugin_path()` in `src/datafusion_engine/runtime.py`
- `_allow_missing_df_plugin()` and `CODEANATOMY_DF_PLUGIN_OPTIONAL` path
- Ad-hoc plugin path probing in any runtime code

**Implementation checklist**
- [x] Implement `resolve_plugin_path()` with a strict resolution order.
- [x] Implement `assert_plugin_available()` that raises with actionable guidance.
- [x] Add a `PluginDiscoveryReport` payload for diagnostics.
- [x] Update runtime and plugin manager to use the canonical discovery surface.
- [x] Add unit tests for discovery precedence and error messaging.

---

## Scope S2: Package Plugin Library with the Python Extension

**Target State:** `datafusion_ext` ships the plugin library as package data and exposes a stable
`plugin_library_path()` function, removing guesswork and environment dependency.

**Representative pattern** (extension-provided path):

```python
import datafusion_ext

plugin_path = datafusion_ext.plugin_library_path()
# Used directly by plugin discovery and runtime plugin specs.
```

**Target files to modify**
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `rust/datafusion_ext_py/src/lib.rs`
- `rust/datafusion_ext_py/Cargo.toml`
- `rust/datafusion_python/pyproject.toml` (maturin package data)
- `pyproject.toml` (ensure datafusion_ext wheel is built/installed)
- `src/datafusion_ext.py` (remove or convert into a package loader, see deletions)

**Delete after migration**
- `src/datafusion_ext.py` (remove the Python shim that shadows the compiled extension)

**Implementation checklist**
- [x] Export `plugin_library_path()` from the Rust extension.
- [x] Add packaging rules to include the plugin library in the wheel.
- [x] Ensure `import datafusion_ext` loads the compiled module.
- [x] Update runtime to use `datafusion_ext.plugin_library_path()` as a top-priority source.

---

## Scope S3: Manifest + ABI/Version Compatibility Gate

**Target State:** Runtime validates plugin ABI, DataFusion, and Arrow compatibility at startup.
Diagnostics include plugin manifest data and build metadata.

**Representative pattern** (manifest validation):

```python
from datafusion_engine.plugin_discovery import plugin_manifest

manifest = plugin_manifest()
if manifest["plugin_abi_major"] != manifest["host_abi_major"]:
    raise RuntimeError("Plugin ABI mismatch.")
```

**Target files to modify**
- `rust/df_plugin_api/src/lib.rs`
- `rust/df_plugin_host/src/loader.rs`
- `rust/df_plugin_codeanatomy/src/lib.rs`
- `rust/datafusion_python/src/codeanatomy_ext.rs` (expose manifest to Python)
- `src/datafusion_engine/plugin_discovery.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/diagnostics.py`

**Delete after migration**
- Ad-hoc version checks or runtime warnings that do not enforce compatibility.

**Implementation checklist**
- [x] Expose manifest via `datafusion_ext.plugin_manifest()` returning JSON-serializable data.
- [x] Validate ABI/DataFusion/Arrow versions in Python discovery.
- [x] Record manifest + compatibility status in diagnostics.

---

## Scope S4: Test + CI Hard-Fail Enforcement

**Target State:** Tests and CI fail immediately when plugin capabilities are missing.
No silent fallback to degraded behavior.

**Representative pattern** (pytest preflight):

```python
from datafusion_engine.plugin_discovery import assert_plugin_available


def pytest_sessionstart(session: object) -> None:
    _ = session
    assert_plugin_available()
```

**Target files to modify**
- `tests/conftest.py`
- `scripts/bootstrap.sh`
- `scripts/bootstrap_codex.sh`
- `README.md` or `docs/architecture/utilities_reference.md`

**Delete after migration**
- `CODEANATOMY_DF_PLUGIN_OPTIONAL` default in tests
- Any test-specific logic that hides plugin failures

**Implementation checklist**
- [x] Remove the optional plugin env default in tests.
- [x] Add pytest session preflight for plugin availability.
- [x] Ensure bootstrap scripts build + expose plugin library path.

---

## Scope S5: Explicit Stub Policy (Opt-in Only)

**Target State:** Stubs are allowed only when explicitly requested (e.g., `CODEANATOMY_PLUGIN_STUB=1`).
All other paths require the real plugin.

**Representative pattern** (opt-in stub):

```python
if env_bool("CODEANATOMY_PLUGIN_STUB", default=False):
    sys.modules.setdefault("datafusion_ext", datafusion_ext_stub)
else:
    assert_plugin_available()
```

**Target files to modify**
- `tests/plan_golden/test_plan_artifacts.py`
- `tests/unit/test_delta_write_policies.py`
- `src/test_support/datafusion_ext_stub.py`
- `tests/conftest.py`

**Delete after migration**
- Implicit or default stub injection for `datafusion_ext`

**Implementation checklist**
- [x] Introduce a single explicit stub gate (`CODEANATOMY_PLUGIN_STUB`).
- [x] Update tests that rely on stubs to opt in explicitly.
- [x] Document stub usage for local developer workflows.

---

## Scope S6: Deferred Decommissioning (After All Scopes Complete)

These items **must not be removed** until all prior scopes are fully migrated.

**Deferred deletions**
- `src/datafusion_ext.py` (only after the compiled extension is always imported)
- `_allow_missing_df_plugin()` and any optional plugin flags
- Any fallback plugin discovery code paths in runtime or tests

**Deferred checklist**
- [ ] Confirm plugin discovery and manifest validation pass in CI and local dev.
- [x] Remove deprecated env vars and shim modules.

---

## Final Verification (All Scopes)

- [ ] `uv run ruff check --fix`
- [ ] `uv run pyrefly check`
- [ ] `uv run pyright --warnings --pythonversion=3.13`
- [ ] `uv run pytest tests/unit/`
- [ ] `uv run pytest tests/`
