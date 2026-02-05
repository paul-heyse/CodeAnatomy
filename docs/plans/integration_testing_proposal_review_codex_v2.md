# Integration Testing Proposal Review (Round 2)

**Document purpose:** Provide a second-pass, code-verified review of `docs/plans/integration_testing_proposal.md` after updates, with concrete corrections, scope improvements, and scope expansions the author can integrate directly.

**Review date:** 2026-02-05

**Method note:** This review intentionally does **not** rely on transient pytest pass/fail totals as primary evidence, per current branch churn. Recommendations are based on static API/contract verification against source code and existing test inventory.

## Executive Conclusions

1. The proposal direction is strong and now substantially more accurate than the first draft.
2. Remaining issues are mostly in three areas:
   - internal consistency (path/name mismatches across sections),
   - integration-vs-unit scope boundaries,
   - missing coverage of a few high-risk runtime boundaries (extension compatibility and panic containment).
3. The biggest practical improvement is to tighten the proposal into a smaller, boundary-pure integration set and move method-level assertions to unit tests.

---

## 1. Corrections To Apply Immediately

### 1.1 Resolve internal path/name inconsistencies in the proposal

The proposal currently references conflicting file paths/names for the same suites.

| Proposal Location | Current Text | Correction |
|---|---|---|
| `docs/plans/integration_testing_proposal.md:179` vs `docs/plans/integration_testing_proposal.md:533` | `test_relationship_to_cpg_output.py` vs `test_cpg_to_materialization.py` | Keep one canonical file name (recommend `test_cpg_to_materialization.py`) and update all sections to match. |
| `docs/plans/integration_testing_proposal.md:181` vs `docs/plans/integration_testing_proposal.md:1113` vs `docs/plans/integration_testing_proposal.md:1642` | `test_evidence_plan_gating.py` appears under different directories (`boundaries/` vs `error_handling/`) | Pick one location. Recommend `tests/integration/error_handling/test_evidence_plan_gating.py` and update all references. |
| `docs/plans/integration_testing_proposal.md:192` vs `docs/plans/integration_testing_proposal.md:1171` vs `docs/plans/integration_testing_proposal.md:1646` | `test_schedule_tasks_boundaries.py` vs `test_schedule_generation.py` | Use one canonical name. Recommend `test_schedule_generation.py` (matches suite section). |
| `docs/plans/integration_testing_proposal.md:199` and `docs/plans/integration_testing_proposal.md:202` | `test_capability_negotiation.py` is listed in both `error_handling/` and `runtime/` | Keep only one location. Recommend `tests/integration/runtime/test_capability_negotiation.py`. |

### 1.2 Replace hard-coded inventory/failure counts with generated snapshots

Counts and failure baselines in the proposal are inherently per-branch and already drift-prone.

- `docs/plans/integration_testing_proposal.md:122` through `docs/plans/integration_testing_proposal.md:125` hard-code test counts.
- `docs/plans/integration_testing_proposal.md:1627` hard-codes a specific integration result summary.

**Correction:** Replace with a reproducible command block and a short “captured-on” note, instead of fixed values in narrative text.

### 1.3 Correct schedule topology pseudocode to match actual graph node model

In the proposed schedule pseudocode (`docs/plans/integration_testing_proposal.md:1219` onward), the logic checks `hasattr(node, "name")` on graph nodes.

Actual model:
- Node wrapper is `GraphNode(kind, payload)` in `src/relspec/rustworkx_graph.py:139`.
- Task/evidence names live on `payload.name`, not top-level node attributes.

**Correction:** In topology assertions, branch on `graph_node.kind` and read `graph_node.payload.name`.

### 1.4 Keep source-of-truth API references aligned in test text

The updated proposal is mostly correct, but the suite examples should consistently anchor to the current API points:

- `schedule_tasks(graph, *, evidence, options)` in `src/relspec/rustworkx_schedule.py:60`.
- `TaskSchedule.ordered_tasks`, `.generations`, `.missing_tasks`, `.validation_summary` in `src/relspec/rustworkx_schedule.py:28`.
- Evidence-plan gating through `apply_query_and_project` in `src/extract/coordination/materialization.py:293`.
- Feature-flag and execution-option derivation in `src/extract/coordination/spec_helpers.py:73` and `src/extract/coordination/spec_helpers.py:159`.

---

## 2. Improvements To Test Scope

### 2.1 Enforce boundary-pure integration scope (avoid method-level duplication)

Several proposed tests are currently method-level checks that are better classified as unit tests.

Examples to trim or relocate:
- `CdfCursorStore.get_start_version()` behavior (already unit-covered).
- `CdfFilterPolicy` constructor/factory defaults and `to_sql_predicate()` shape (already unit-covered).
- `bytes_from_file_ctx()` fallback chain as pure utility behavior.

**Improvement:** For integration, keep only cross-component contracts, e.g.:
- cursor advancement across **Delta write + subsequent read window selection**,
- filter policy effects across **CDF read path + downstream merge semantics**,
- FileContext fallback behavior only where it affects extractor orchestration end-to-end.

### 2.2 Consolidate duplicated EvidencePlan scope

Evidence-plan scope currently appears in multiple suites and expansion sections with overlap.

**Improvement:** Split cleanly:
- One suite for **plan gating mechanics** (`requires_dataset`, `required_columns_for`, `plan_feature_flags`, `enabled_when` integration path).
- One suite for **extractor materialization effects** (empty-plan behavior, projected scans, diagnostics on gating outcomes).

Avoid duplicating the same assertions in both Boundary and Error Handling suites.

### 2.3 Tighten schedule/cost tests to integration wiring, not algorithm internals

`bottom_level_costs` and `task_slack_by_task` are pure algorithmic functions in `src/relspec/execution_plan.py:923` and `src/relspec/execution_plan.py:955`.

**Improvement:**
- Unit tests should own formula correctness.
- Integration tests should validate that scheduler output changes appropriately when cost context is injected via `ScheduleOptions.cost_context`.

### 2.4 Use canonical diagnostics taxonomy assertions via helper

Canonical postprocess statuses are emitted in both extract helper paths:
- `register_view_failed`, `view_artifact_failed`, `schema_contract_failed` in `src/extract/helpers.py:540` and `src/extract/coordination/materialization.py:518`.

**Improvement:** Add a shared assertion helper (test-side) for status taxonomy and stage values, then reuse in all resilience tests. This reduces string drift and keeps assertions consistent.

### 2.5 Strengthen fixture governance for integration tests

Proposal guidance is good; tighten it further:
- Prefer root fixtures from `tests/conftest.py` and helpers from `tests/test_helpers/`.
- Require capability checks (`require_datafusion()`, `require_delta_extension()`, etc.) at module setup for capability-sensitive suites.
- Use unique tmp paths and avoid cross-test state reuse for Delta/CDF suites.

---

## 3. Recommended Scope Expansions

These are not sufficiently explicit in the current proposal and should be added as first-class coverage.

### 3.1 Cache introspection registration compatibility boundary (High Priority)

Why this matters:
- Cache table registration crosses Python `SessionContext` and extension entrypoints.
- Integration behavior depends on extension hook compatibility at runtime.

Relevant code boundary:
- `register_cache_introspection_functions` in `src/datafusion_engine/catalog/introspection.py:665`.
- runtime install path in `src/datafusion_engine/session/runtime.py:5983`.

Recommended tests:
- Compatible hook path registers cache introspection functions successfully.
- Missing hook path yields deterministic, actionable error.
- Incompatible context type path yields deterministic failure taxonomy (no opaque panic).

Suggested file:
- `tests/integration/runtime/test_cache_introspection_capabilities.py`

### 3.2 Delta provider panic-containment contract

Why this matters:
- Delta provider construction crosses FFI/runtime boundaries and can surface as panics if unguarded.

Recommended tests:
- Failure in provider construction is surfaced as structured `RuntimeError` with actionable diagnostics.
- No uncaught panic/FFI exception leaks to test boundary for known bad-provider scenarios.

Suggested file:
- `tests/integration/runtime/test_delta_provider_panic_containment.py`

### 3.3 Object store registration idempotency and URI normalization

Why this matters:
- Provider registration depends on URI/scheme normalization and object-store registration order.
- This is a frequent source of environment-specific breakage in storage-backed integration paths.

Recommended tests:
- Registering same Delta location repeatedly is idempotent.
- Equivalent URI forms resolve consistently (`path`, `file://`, dataset-spec-backed forms).
- Re-registration after schema evolution preserves queryability.

Suggested file:
- `tests/integration/runtime/test_object_store_registration_contracts.py`

### 3.4 Evidence plan to projection contract at DataFusion plan level

The proposal references this concept, but add explicit plan-level assertions:
- projected columns actually appear in logical plan, not just in helper return values,
- required join keys and derived fields are retained as expected.

Primary boundary code:
- `apply_query_and_project` in `src/extract/coordination/materialization.py:293`.

### 3.5 Session-context reuse determinism for plan/artifact outputs

Current determinism expansion is good; add one explicit variant:
- same runtime profile + reused session context across runs should yield stable plan signatures/fingerprints unless semantic config changed.

This catches hidden mutable-state coupling in runtime/session caches.

---

## 4. Revised Priority Sequence

### Phase A (Proposal hygiene before implementation)

1. Resolve all file/path/name inconsistencies across sections.
2. Replace hard-coded inventory/failure counts with generated snapshot instructions.
3. Mark unit-level duplicates for relocation/removal from integration scope.

### Phase B (Highest-value boundary contracts)

1. EvidencePlan -> extractor/materialization gating contracts.
2. Scheduler behavior contracts with topology and cost-context wiring.
3. Postprocess diagnostics taxonomy assertions.
4. Incremental impact closure contracts.

### Phase C (Runtime/storage hardening)

1. Cache introspection capability negotiation.
2. Delta provider panic containment.
3. Object store registration idempotency/normalization.
4. Delta read-after-write visibility and provider registration contracts.

### Phase D (Selective expansion)

1. Plan/artifact determinism variants (including session reuse).
2. Single focused multi-source alignment contract.
3. Non-gating performance baselines.

---

## 5. Final Recommendations To The Author

1. Keep the current proposal structure, but fix internal naming/path drift first.
2. Tighten integration scope to boundary-crossing behavior only; move method-level checks to unit tests.
3. Add explicit runtime compatibility boundaries (cache-table registration, delta provider panic containment) because these seams are high risk and currently underrepresented.
4. Use generated snapshots for inventory/baseline metadata instead of static counts in narrative text.
5. Keep diagnostics taxonomy assertions canonical and centralized to reduce brittleness.

