# Integration Testing Proposal Review

**Document purpose:** Provide a repo-verified review of `docs/plans/integration_testing_proposal.md` with concrete corrections, scope improvements, and scope expansions for the original author to integrate.

**Review date:** 2026-02-05

## How This Review Was Performed

1. Environment setup was executed exactly as requested:
   - `bash scripts/bootstrap_codex.sh`
   - `uv sync`
2. Proposal content was reviewed in full and cross-checked against current source and test code.
3. Existing test inventory, pytest config, and integration baseline were measured from the current branch.
4. Key APIs named in the proposal were validated against implementation using `./cq` and direct source inspection.

## Repo-Verified Baseline (Current State)

- Pytest marker/config source is `pytest.ini`, not `pyproject.toml`.
- Current test file distribution:
  - `tests/unit`: 185 files
  - `tests/integration`: 23 files
  - `tests/incremental`: 1 file
  - `tests/e2e`: 16 files
- Current integration suite runtime baseline:
  - Command: `uv run pytest tests/integration -q`
  - Result: `41 passed, 5 failed, 4 skipped` in `20.14s`

## Corrections Recommended For The Proposal

### 1. Fix pytest marker/config location

**Issue:** Proposal appendix says marker config is in `pyproject.toml`.

**Correction:** Marker definitions currently live in `pytest.ini` (`markers = ...`).

**Why this matters:** If the author updates the wrong file, CI marker behavior will not change.

### 2. Align with existing test ownership boundaries

**Issue:** Proposal puts many incremental tests under `tests/integration/incremental/`.

**Correction:** Keep stateful Delta/CDF lifecycle tests in `tests/incremental/` and reserve `tests/integration/` for multi-subsystem boundaries that are not state-store-centric.

**Why this matters:** Current repo already separates these concerns, and preserving that split improves discoverability and ownership.

### 3. Avoid duplicating established unit coverage as integration work

Several proposal items are already covered at unit level and should not be re-implemented broadly at integration level:

- CDF cursor behavior and `get_start_version()` semantics: `tests/unit/semantics/incremental/test_cdf_cursors.py`
- Merge strategies (`APPEND`, `UPSERT`, `REPLACE`, `DELETE_INSERT`): `tests/unit/semantics/incremental/test_merge_strategies.py`
- Semantic runtime adapter round-trip basics: `tests/unit/datafusion_engine/test_semantics_runtime_bridge.py`
- Streaming-write artifact threshold behavior: `tests/unit/test_incremental_streaming_metrics.py`

**Correction:** Keep only boundary-crossing integration variants; keep algorithmic behavior at unit level.

### 4. Fix brittle scheduling test logic in proposal pseudocode

**Issue:** The proposed `test_schedule_respects_topological_order` compares `TaskNode.inputs` against scheduled task names.

**Correction:** `TaskNode.inputs` are evidence dataset names, not task names. Dependency ordering should be checked via predecessor task nodes in graph topology, not by comparing to `inputs`.

**Why this matters:** Current pseudocode can pass while missing real ordering regressions.

### 5. Do not add an integration-specific `conftest.py` that bypasses shared fixtures

**Issue:** Proposal defines a new `tests/integration/conftest.py` with raw `SessionContext()` fixture setup.

**Correction:** Reuse shared fixtures and helpers in:
- `tests/conftest.py`
- `tests/test_helpers/datafusion_runtime.py`
- `tests/test_helpers/optional_deps.py`
- `tests/test_helpers/delta_seed.py`

**Why this matters:** Shared helpers already encode environment/capability checks and diagnostics conventions.

### 6. Keep performance tests non-gating

**Issue:** Proposal mixes performance tests into core integration scope.

**Correction:** Keep performance tests marked non-gating (`benchmark`/`performance`) and run separately from required integration gate.

**Why this matters:** Preserves signal quality and avoids noisy CI failures from host variance.

## Improvements To The Proposed Scope

### A. Keep and prioritize (high ROI, currently under-tested)

1. **Evidence plan gating and projection behavior**
   - Cover `apply_query_and_project()` + `plan_requires_row()` + `required_columns_for()` interaction.
   - This area currently lacks direct tests.
2. **Scheduling behavior, not just schedule artifacts**
   - Add direct tests for `schedule_tasks()` behavior under missing evidence, reduced graph, and cost context.
   - Current tests mostly instantiate `TaskSchedule` objects instead of exercising scheduler logic.
3. **Incremental impact closure**
   - `merge_impacted_files()` and `impacted_importers_from_changed_exports()` currently have no direct tests.
4. **Extract postprocess resilience**
   - Test `register_view_failed` / `view_artifact_failed` / `schema_contract_failed` event recording paths.

### B. Keep but reduce breadth

1. **Adapter tests**
   - Keep a small integration set proving profile → semantic config → profile behavior inside session construction.
   - Do not duplicate every unit-path adapter behavior in integration.
2. **Immutability tests**
   - Keep mostly unit-level; integration should have only a smoke assertion where immutability affects subsystem behavior.

### C. Defer / narrow in first pass

1. **Large multi-source alignment matrix (CST/AST/SCIP/symtable/bytecode)**
   - Start with one focused integration contract, not full cross-product.
2. **Large performance suite**
   - Add after boundary contracts are stable.

## Recommended Scope Expansions (Not Strongly Captured Yet)

### 1. Delta read-after-write and provider registration contract tests (high priority)

Current integration failures show this boundary is unstable and should be explicitly covered:

- `tests/integration/test_delta_protocol_and_schema_mode.py::test_schema_mode_merge_allows_new_columns`
- `tests/integration/test_incremental_partitioned_updates.py::test_upsert_partitioned_dataset_alignment_and_deletes`
- `tests/integration/test_pycapsule_provider_registry.py::test_table_provider_registry_records_delta_capsule`
- `tests/integration/test_pycapsule_provider_registry.py::test_delta_pruning_predicate_from_dataset_spec`
- `tests/integration/test_semantic_incremental_overwrite.py::test_write_overwrite_dataset_roundtrip`

**Expansion:** Add explicit contract tests that verify schema resolution and provider registration immediately after Delta writes for both plain and dataset-spec-backed locations.

### 2. Capability-negotiation and graceful-fallback boundaries

Recent junit failures indicate fragile behavior around extension capabilities and replay errors.

**Expansion:** Add boundary tests ensuring deterministic failure payloads (not uncaught exceptions) when:
- Substrait payload decode fails.
- Async UDF capability is requested but unsupported in runtime build.

### 3. Evidence-plan-to-extractor-options pipeline tests

Proposal covers EvidencePlan methods, but should also include end-to-end gating from plan to extractor options:
- `plan_feature_flags()`
- `rule_execution_options()`
- `enabled_when` stage gating
- projected column subset behavior

### 4. Plan and artifact determinism boundaries

Add targeted integration tests validating when fingerprints/artifacts should and should not change:
- plan fingerprint stability under unchanged query/runtime
- expected fingerprint changes with meaningful runtime policy toggles

### 5. Diagnostics contract assertions for error-path observability

For resilience tests, assert both:
- event presence
- stable status taxonomy (`register_view_failed`, `view_artifact_failed`, `schema_contract_failed`)

This keeps tests resilient and operationally useful.

## Revised Phased Implementation (Recommended)

### Phase 0: Stabilize existing integration failures (immediate)

1. Resolve and lock down current 5 failing integration boundaries listed above.
2. Add regression assertions around the fixed behavior.

### Phase 1: Core boundary contracts (highest value)

1. `tests/integration/boundaries/test_evidence_plan_gating.py`
2. `tests/integration/scheduling/test_schedule_tasks_boundaries.py`
3. `tests/integration/error_handling/test_extract_postprocess_resilience.py`
4. `tests/incremental/test_impact_closure_strategies.py`

### Phase 2: Delta and runtime capability contracts

1. `tests/incremental/test_delta_read_after_write_contracts.py`
2. `tests/integration/runtime/test_capability_negotiation.py`
3. `tests/integration/adapters/test_semantic_runtime_bridge_integration.py` (small set only)

### Phase 3: Selective expansion

1. One multi-source alignment contract test.
2. Non-gating performance baseline tests.

## Marker and CI Recommendations

1. Reuse existing markers in `pytest.ini` (`integration`, `serial`, `benchmark`, `performance`).
2. Add new markers only when required; avoid marker explosion.
3. Keep default integration gate fast and deterministic; run performance and heavy matrix tests in separate jobs.

## Final Conclusions

1. The proposal direction is strong and correctly prioritizes subsystem boundaries over pure E2E.
2. The biggest needed adjustment is **scope calibration**: keep boundary-crossing tests, remove unit duplication.
3. Immediate value is highest in:
   - evidence-plan gating behavior,
   - scheduler behavior contracts,
   - incremental impact closure,
   - extract postprocess resilience,
   - Delta read-after-write/provider registration boundaries.
4. Integrating the corrections above will reduce churn, improve CI signal, and produce a smaller but higher-value integration suite.
