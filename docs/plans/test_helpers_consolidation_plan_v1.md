# Test Helpers Consolidation Plan v1

## Executive Summary

This plan consolidates repeated test setup patterns into shared helpers under a new `tests/test_helpers/` package. The goal is to standardize DataFusion setup, Delta table seeding, diagnostics scaffolding, and skip logic to improve consistency, reduce maintenance overhead, and make debugging easier.

**Primary outcomes:**
1. Reduce duplicated setup logic across unit/integration tests.
2. Provide centralized, typed helpers for DataFusion/Delta fixtures.
3. Standardize `importorskip` and diagnostics capture.
4. Make future refactors of runtime and diagnostics easier by updating a single helper.

---

## Design Principles

1. **Single Source of Truth** for test setup patterns.
2. **Minimal Fixture Surface** — thin fixtures only; helper logic stays out of conftest.
3. **Test-Local Overrides** — helpers accept parameters to avoid hard-coded assumptions.
4. **Clear Boundaries** — helpers live in `tests/test_helpers/` and do not leak into runtime code.
5. **Compatibility** — avoid behavior changes; only centralize existing logic.

---

## Scope Index

1. DataFusion Runtime/Profile Helpers
2. Arrow Table Registration Helpers
3. Delta Table Write Helpers
4. Diagnostics + Telemetry Helpers
5. `pytest.importorskip` / Optional Dependency Helpers
6. Plan Bundle / Session Runtime Helpers
7. Documentation + Migration Notes
8. Deferred Cleanup and Verification

---

## 1. DataFusion Runtime/Profile Helpers

### Objective
Centralize `DataFusionRuntimeProfile` and context setup used across tests.

### Target Files
- **Create:** `tests/test_helpers/datafusion_runtime.py`
- **Modify:** tests that directly instantiate runtime contexts

### Proposed APIs
```python
def df_profile(*, diagnostics: DiagnosticsCollector | None = None, **kwargs) -> DataFusionRuntimeProfile: ...
def df_ctx(*, diagnostics: DiagnosticsCollector | None = None, **kwargs) -> SessionContext: ...
```

### Checklist
- [ ] Create `df_profile()` helper.
- [ ] Create `df_ctx()` helper.
- [ ] Migrate repeated `DataFusionRuntimeProfile().session_context()` usage.
- [ ] Add thin fixtures in `tests/conftest.py` (optional) that wrap `df_profile()` / `df_ctx()` when tests prefer fixture injection.

---

## 2. Arrow Table Registration Helpers

### Objective
Consolidate `datafusion_from_arrow` + table seed patterns.

### Target Files
- **Create:** `tests/test_helpers/arrow_seed.py`
- **Modify:** tests repeatedly calling `datafusion_from_arrow(...)`

### Proposed APIs
```python
def register_arrow_table(
    ctx: SessionContext,
    *,
    name: str,
    table: pa.Table,
) -> DataFrame: ...
```

### Checklist
- [ ] Add `register_arrow_table()` helper.
- [ ] Replace repeated `datafusion_from_arrow` patterns in plan bundle and lineage tests.
- [ ] Optional: add a fixture in `tests/conftest.py` only if multiple tests need a pre-registered table.

---

## 3. Delta Table Write Helpers

### Objective
Centralize Delta write setup using `WritePipeline` + `WriteRequest`.

### Target Files
- **Create:** `tests/test_helpers/delta_seed.py`
- **Modify:** tests that write Delta tables in tmp paths

### Proposed APIs
```python
def write_delta_table(
    tmp_path: Path,
    *,
    table: pa.Table,
    profile: DataFusionRuntimeProfile | None = None,
    partition_by: tuple[str, ...] | None = None,
    schema_mode: str | None = None,
) -> tuple[DataFusionRuntimeProfile, SessionContext, Path]: ...
```

### Checklist
- [ ] Add `write_delta_table()` helper.
- [ ] Migrate tests in integration + unit suites that seed Delta tables.
- [ ] Do **not** place Delta helpers directly in `tests/conftest.py` (avoid import-time DataFusion/deltalake setup).

---

## 4. Diagnostics + Telemetry Helpers

### Objective
Standardize `DiagnosticsCollector` + profile wiring used in observability tests.

### Target Files
- **Create:** `tests/test_helpers/diagnostics.py`
- **Modify:** tests that manually instantiate `DiagnosticsCollector()`

### Proposed APIs
```python
def diagnostic_profile(**kwargs) -> tuple[DataFusionRuntimeProfile, DiagnosticsCollector]: ...
```

### Checklist
- [ ] Add `diagnostic_profile()` helper.
- [ ] Migrate tests in diagnostics/observability integration scope.
- [ ] Optional: add `diagnostic_profile` fixture in `tests/conftest.py` for tests that consistently use fixture injection.

---

## 5. Optional Dependency Helpers

### Objective
Provide consistent `pytest.importorskip` usage for optional deps.

### Target Files
- **Create:** `tests/test_helpers/optional_deps.py`
- **Modify:** tests that repeat `pytest.importorskip("datafusion")` or `pytest.importorskip("deltalake")`

### Proposed APIs
```python
def require_datafusion() -> None: ...
def require_deltalake() -> None: ...
```

### Checklist
- [ ] Add `require_datafusion()` and `require_deltalake()` helpers.
- [ ] Replace repeated `pytest.importorskip` calls.
- [ ] Keep helper functions in `tests/test_helpers/` and optionally expose pytest fixtures that call them (avoid unconditional imports in `conftest.py`).

---

## 6. Plan Bundle / Session Runtime Helpers

### Objective
Reduce duplication in plan-bundle tests that repeatedly create runtime + plan bundles.

### Target Files
- **Create:** `tests/test_helpers/plan_bundle.py`
- **Modify:** `tests/unit/test_datafusion_unparser_artifacts.py` and other plan-bundle tests

### Proposed APIs
```python
def bundle_for_table(ctx: SessionContext, table: pa.Table, *, name: str) -> DataFusionPlanBundle: ...
```

### Checklist
- [ ] Add `bundle_for_table()` helper.
- [ ] Collapse repeated bundle creation code in plan bundle tests.
- [ ] Avoid putting bundle helpers in `tests/conftest.py` to prevent eager DataFusion work at collection time.

---

## 7. Documentation + Migration Notes

### Objective
Make it clear where helpers live and how to use them.

### Target Files
- **Create:** `tests/test_helpers/README.md`
- **Update:** `tests/README.md` (if present)

### Checklist
- [ ] Add helper usage documentation.
- [ ] Note migration steps for existing tests.

---

## 8. Deferred Cleanup and Verification

### Objective
Validate helper adoption and remove residual duplication.

### Checklist
- [ ] Run ruff/pyright/pyrefly on tests after migration.
- [ ] Ensure no `pytest.importorskip("datafusion")` duplicates remain.
- [ ] Ensure Delta/Arrow seed patterns are consistently routed through helpers.

---

## Expected Outcomes

- 30–50% reduction in boilerplate across tests.
- A consistent path for DataFusion/Delta setup in unit + integration suites.
- Easier test updates when runtime or diagnostics APIs change.
