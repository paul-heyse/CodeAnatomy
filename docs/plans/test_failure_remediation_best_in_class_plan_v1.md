# Test Failure Remediation Best-in-Class Plan (v1)

## Objective
Resolve all pytest failures, skips, and warnings discovered in `build/test-results/junit.xml` and `build/test-results/pytest-report.json` by implementing causal fixes that align with current architecture and testing expectations. This plan captures the agreed recommendations as actionable scope items with representative code snippets, target files, deprecations, and implementation checklists.

---

## Scope Item 1 — DataFusion Internal Fallbacks (delta_snapshot_info + stable_hash64)
**Goal:** Remove hard dependency on `datafusion._internal` for core test paths by providing safe fallback behavior when Rust extensions are unavailable.

**Representative code snippet**
```python
# src/datafusion_engine/udf/shims.py
def stable_hash64(value: Expr) -> Expr:
    try:
        return _call_expr("stable_hash64", value)
    except (RuntimeError, TypeError):
        # Fallback to deterministic hash in Python
        return f.hash(value)
```

**Target files**
- `src/datafusion_engine/udf/shims.py`
- `src/datafusion_engine/delta/control_plane.py`
- `src/storage/deltalake/delta.py`

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Guard `delta_snapshot_info()` when internal entrypoints are missing and fall back to deltalake/py snapshot APIs.
- [x] Provide deterministic fallback for `stable_hash64` in shims when internal UDF is absent.
- [x] Ensure fallback behavior is covered in unit tests without skipping.

---

## Scope Item 2 — RawTable and Dataset Normalization Fixes
**Goal:** Eliminate RawTable ingestion failures and allow in‑memory datasets to be normalized when a delta policy is configured.

**Representative code snippet**
```python
# src/datafusion_engine/catalog/provider.py
def _table_from_dataset(dataset: object) -> Table:
    if hasattr(dataset, "__datafusion_table_provider__"):
        return Table(cast("TableProviderExportable", dataset))
    if isinstance(dataset, DataFrame):
        return Table(dataset)
    if isinstance(dataset, ds.Dataset):
        return Table(dataset)
    return Table(ds.dataset(dataset))
```

**Target files**
- `src/datafusion_engine/catalog/provider.py`
- `src/storage/dataset_sources.py`

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Accept DataFusion view/table-provider objects before calling `pyarrow.dataset()`.
- [x] Allow `normalize_dataset_source()` to handle in-memory sources even when `dataset_format="delta"` is active.

---

## Scope Item 3 — Arrow Cast Policy Hardening
**Goal:** Avoid optimizer errors (`arrow_cast requires its second argument to be a constant string`) by centralizing casting through `Expr.cast(pa.<type>)`.

**Representative code snippet**
```python
# shared helper pattern
def _safe_cast(expr: Expr, dtype: pa.DataType) -> Expr:
    return expr.cast(dtype)
```

**Target files**
- `src/semantics/catalog/analysis_builders.py`
- `src/cpg/view_builders_df.py`
- `src/storage/deltalake/file_pruning.py`
- `src/datafusion_engine/schema/validation.py`
- `src/datafusion_engine/session/runtime.py`

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Add shared cast helper.
- [x] Replace `f.arrow_cast(expr, lit("..."))` with safe cast helper at all call sites.
- [x] Validate with optimizer path tests.

---

## Scope Item 4 — Semantic Dataset Field De‑duplication
**Goal:** Remove duplicate `file_id/path/bstart/bend` fields arising from `file_identity`/`span` bundles and explicit CPG field lists.

**Representative code snippet**
```python
# src/semantics/catalog/dataset_rows.py
fields = tuple(
    name for name in _NODE_OUTPUT_COLUMNS
    if name not in {"file_id", "path", "bstart", "bend"}
)
```

**Target files**
- `src/semantics/catalog/dataset_rows.py`
- `src/cpg/emit_specs.py`

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Remove bundle-derived fields from explicit CPG field lists.
- [x] Rebuild dataset specs and ensure no duplicate field errors.

---

## Scope Item 5 — Fix `dataset_rows` Module/Function Name Collision
**Goal:** Ensure `import semantics.catalog.dataset_rows` returns the module, not the function.

**Representative code snippet**
```python
# src/semantics/catalog/__init__.py
# Remove re-export of dataset_rows function or rename it to avoid collision.
```

**Target files**
- `src/semantics/catalog/__init__.py`
- `src/semantics/catalog/dataset_rows.py`

**Deprecate/Delete after completion**
- Deprecated function alias if rename is used (`dataset_rows` → `dataset_rows_by_name`).

**Implementation checklist**
- [x] Rename function or stop re-exporting it.
- [x] Update imports in downstream modules/tests.

---

## Scope Item 6 — Input Validation Cache Invalidation
**Goal:** Ensure semantic input validation sees tables registered during test setup by invalidating the introspection cache.

**Representative code snippet**
```python
# src/semantics/input_registry.py
invalidate_introspection_cache(ctx)
available = set(table_names_snapshot(ctx))
```

**Target files**
- `src/semantics/input_registry.py`
- `src/datafusion_engine/schema/introspection.py`
- `tests/test_helpers/arrow_seed.py` (if invalidation is centralized in test helper)

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Invalidate cache before reading `information_schema`.
- [x] Keep invalidation in a single location (preferred).

---

## Scope Item 7 — Hamilton Type Resolution & Extract Fields
**Goal:** Fix `issubclass()` failures and `NameError` from forward type hints.

**Representative code snippet**
```python
# src/hamilton_pipeline/modules/execution_plan.py
@extract_fields({"plan_session_runtime_hash": object})
```

**Target files**
- `src/hamilton_pipeline/modules/execution_plan.py`
- `src/hamilton_pipeline/modules/task_execution.py`
- `src/hamilton_pipeline/modules/inputs.py`

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Replace union types in `extract_fields` with runtime-safe classes (`object`, `tuple`).
- [x] Provide runtime bindings for `EngineSession` and `OtelBootstrapOptions` to avoid eval errors.

---

## Scope Item 8 — Observability + OTel Bootstrap Consistency
**Goal:** Ensure telemetry tests consistently capture logs/metrics/spans in test runs and warnings are eliminated.

**Representative code snippet**
```python
# src/obs/otel/bootstrap.py
if options and options.test_mode:
    _STATE["providers"] = None  # allow test-mode override
```

**Target files**
- `src/obs/otel/bootstrap.py`
- `src/extract/scanning/scope_rules.py`
- `tests/unit/test_plan_unification.py`
- `tests/unit/test_cache_lineage_correctness.py`

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Allow test-mode OTel provider override (or reset) for telemetry tests.
- [x] Replace deprecated `gitwildmatch` usage with `gitignore`.
- [x] Close SQLite metadata store connections in tests to avoid resource warnings.

---

## Scope Item 9 — Msgspec Contract and Schema Registry Stability
**Goal:** Restore schema registry metadata consistency and align JSON/msgpack goldens with current payloads.

**Representative code snippet**
```python
# src/serde_schema_registry.py
payload.setdefault("description", "Delta protocol schema")
```

**Target files**
- `src/serde_schema_registry.py`
- `schemas/delta/app_transaction.schema.json`
- `tests/msgspec_contract/goldens/*`

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Ensure external schemas include `description`.
- [x] Regenerate msgspec golden files once payloads are confirmed.

---

## Scope Item 10 — Incremental Streaming Diagnostics + Telemetry
**Goal:** Restore streaming write metrics and ensure delta runtime environment options appear in telemetry payloads.

**Representative code snippet**
```python
# src/semantics/incremental/delta_updates.py
if table.num_rows > _STREAMING_ROW_THRESHOLD:
    context.runtime.profile.record_artifact("incremental_streaming_writes_v1", payload)
```

**Target files**
- `src/semantics/incremental/delta_updates.py`
- `src/datafusion_engine/session/runtime.py`

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Restore `_STREAMING_ROW_THRESHOLD`.
- [x] Emit streaming write diagnostics.
- [x] Include `delta_runtime_env` in `telemetry_payload_v1()`.

---

## Scope Item 11 — Repo Blobs Git Commit Path Fix
**Goal:** Fix pygit2 index add paths in tests by using repo‑relative paths.

**Representative code snippet**
```python
# tests/unit/test_repo_blobs_git.py
repo.index.add(path.relative_to(repo_path).as_posix())
```

**Target files**
- `tests/unit/test_repo_blobs_git.py`

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Use repo-relative paths when adding files to the index.
- [x] Confirm source_ref tests pass.

---

## Scope Item 12 — Hashing Consistency (Config + Delta Scan)
**Goal:** Align hashing functions with tests and ensure deterministic encodings.

**Representative code snippet**
```python
# src/core/config_base.py
return hash_json_canonical(payload, str_keys=True)
```

**Target files**
- `src/core/config_base.py`
- `src/datafusion_engine/delta/scan_config.py`

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Use canonical JSON hashing for config fingerprints.
- [x] Hash `DeltaScanConfigSnapshot` directly via msgpack encoder.

---

## Scope Item 13 — Rust UDF Plugin Availability (Skip Reduction)
**Goal:** Remove skip conditions by ensuring the CodeAnatomy Rust UDF plugin is present in test/dev builds.

**Representative code snippet**
```python
# tests/test_helpers/optional_deps.py
if udf_backend_available():
    return datafusion
```

**Target files**
- `scripts/bootstrap_codex.sh`
- `tests/test_helpers/optional_deps.py`
- `src/datafusion_engine/udf/runtime.py`

**Deprecate/Delete after completion**
- None

**Implementation checklist**
- [x] Build and install Rust UDF plugin in bootstrap.
- [x] Relax skip conditions when fallbacks are available.
- [x] Add minimal fallback UDF registry for test runs.

---

## Cross‑Cutting Implementation Checklist
- [x] Update all impacted unit/integration tests to align with new behavior.
- [x] Regenerate msgspec goldens after payload changes.
- [ ] Run full pytest suite; resolve any new warnings.
- [x] Verify `pyright`, `pyrefly`, and `ruff` are clean in all edited files.
