# Holistic Review: Integration Tests Outside Current Expanded-v2 Implementation Scope

Date: 2026-02-06

## Scope

This review intentionally excludes the currently in-progress Expanded v2 implementation directories:
- `tests/integration/extraction/`
- `tests/integration/pipeline/`
- `tests/integration/relspec/`
- `tests/integration/semantics/`
- `tests/integration/storage/`

Reviewed surface: all other integration suites/files under `tests/integration` (40 test files).

## CQ Capability-First Approach

Per request, this review started by confirming CQ capabilities and using CQ as primary discovery tooling.

Primary CQ capabilities used in this review:
- `./cq search` for semantic discovery and marker/pattern inventory
- `./cq q "pattern=..."` for AST-accurate assertion-shape scans
- `./cq calls <function>` for fan-out/choke-point boundary prioritization

Representative commands used:
- `./cq search pytest.mark.integration --in tests/integration`
- `./cq q "pattern='assert $X is not None' in=tests/integration"`
- `./cq calls delta_provider_from_session`
- `./cq calls schedule_tasks`
- `./cq calls build_engine_session`
- `./cq calls normalize_dataset_locations_for_profile`
- `./cq search _normalize_options --in tests/integration`

## Executive Summary

The non-v2 integration surface has useful boundary coverage, but there are structural gaps that reduce CI effectiveness and production realism:

1. `pytest -m integration` currently drops 17 tests due to missing `integration` markers across 7 files.
2. Several "integration" suites are mostly unit/contract checks (object construction, immutability, shape assertions) rather than multi-subsystem runtime boundary validation.
3. Some high-value runtime boundary tests overuse broad exception buckets and weak assertions (`is not None`, `hasattr`), which can hide real regressions.
4. A subset of tests couples directly to private APIs and brittle internals, increasing refactor friction without increasing boundary confidence.

## Findings (Ordered by Severity)

### 1) Integration marker gap causes silent CI deselection (High)

Evidence:
- `uv run pytest tests/integration --collect-only -q` => `223 tests collected`
- `uv run pytest tests/integration -m integration --collect-only -q` => `206/223 tests collected (17 deselected)`

Deselected files (all missing `@pytest.mark.integration`):
- `tests/integration/cq/test_calls_integration.py`
- `tests/integration/cq/test_search_migration.py`
- `tests/integration/test_cli_meta_integration.py`
- `tests/integration/test_evidence_semantic_catalog.py`
- `tests/integration/test_incremental_partitioned_updates.py`
- `tests/integration/test_runtime_semantic_locations.py`
- `tests/integration/test_semantic_incremental_overwrite.py`

Impact:
- Any CI path that relies on `-m integration` is under-testing by design.

### 2) "Integration" classification drift to unit/contract behavior (High)

Examples:
- `tests/integration/contracts/test_immutability_contracts.py` validates frozen-attribute mutation behavior only (contract/unit-level behavior).
- `tests/integration/adapters/test_semantic_runtime_bridge_integration.py` mostly validates object mapping and field propagation without exercising downstream session/runtime boundaries.
- `tests/integration/boundaries/test_plan_determinism.py` and `tests/integration/scheduling/test_schedule_generation.py` rely on synthetic `InferredDeps` graphs and structural assertions rather than full boundary pathways.

CQ support:
- `./cq calls build_task_graph_from_inferred_deps` reports heavy use in these suites with synthetic fixtures.

Impact:
- Misclassification makes integration runtime signal look stronger than it is.

### 3) Weak assertions on runtime boundaries reduce regression detection (High)

Representative examples:
- `tests/integration/runtime/test_capability_negotiation.py:36-39` (`hasattr`, `isinstance(bool)` checks only).
- `tests/integration/runtime/test_capability_negotiation.py:111-112` (`assert profile is not None`).
- `tests/integration/runtime/test_cache_introspection_capabilities.py:106-109` (`assert ctx is not None` as primary assertion for gate behavior).

CQ support:
- `./cq q "pattern='assert $X is not None' in=tests/integration"` highlights broad usage, including critical runtime tests.

Impact:
- Tests pass even if semantics drift, as long as objects still exist.

### 4) Broad exception buckets obscure failure contracts (Medium-High)

Representative examples:
- `tests/integration/runtime/test_cache_introspection_capabilities.py:90`:
  - `except (ImportError, TypeError, RuntimeError): pass`
- `tests/integration/runtime/test_delta_provider_panic_containment.py:65`
- `tests/integration/runtime/test_delta_provider_panic_containment.py:103`
  - both catch `(RuntimeError, OSError, ValueError, DataFusionEngineError)`

CQ support:
- `./cq search "except \(ImportError, TypeError, RuntimeError\):" --regex --in tests/integration`
- `./cq search "except \(RuntimeError, OSError, ValueError, DataFusionEngineError\) as exc:" --regex --in tests/integration`

Impact:
- Error taxonomy regressions can slip through as long as any broad exception is raised.

### 5) Private API coupling in integration suites (Medium)

Representative examples:
- `tests/integration/runtime/test_object_store_registration_contracts.py:75`
- `tests/integration/runtime/test_object_store_registration_contracts.py:84`
- `tests/integration/runtime/test_object_store_registration_contracts.py:93`
  - direct imports of `_normalize_options`
- `tests/integration/runtime/test_object_store_registration_contracts.py:100`
- `tests/integration/runtime/test_object_store_registration_contracts.py:110`
  - direct imports of `_resolve_store_spec`

CQ support:
- `./cq search _normalize_options --in tests/integration`

Impact:
- Refactor-hostile tests with lower production-path confidence than public API-based boundary tests.

### 6) Hard-coded path assumptions (`/tmp/...`) in tests (Medium)

Representative examples:
- `tests/integration/test_engine_session_semantic_config.py:17`
- `tests/integration/test_engine_session_semantic_config.py:27`
- `tests/integration/adapters/test_semantic_runtime_bridge_integration.py:75`
- `tests/integration/adapters/test_semantic_runtime_bridge_integration.py:157`

CQ support:
- `./cq search /tmp/ --in tests/integration`

Impact:
- Less hermetic tests and avoidable cross-environment/path-policy brittleness.

### 7) Capability-gated tests check installation event but not post-install behavior deeply (Medium)

Representative examples:
- `tests/integration/test_create_function_factory.py:29-41`
- `tests/integration/test_expr_planner_hooks.py:33-45`

These tests assert `installed` diagnostic payloads but do limited validation of behavior after successful installation.

Impact:
- Installation telemetry is covered; runtime functional correctness after installation is only partially covered.

## Corrections to Apply

1. Add `@pytest.mark.integration` to all 7 unmarked integration files listed above.
2. Replace broad exception catches with explicit error assertions:
   - assert stable error class where possible
   - assert payload/error message contract fields for expected failure mode
3. Replace weak structural assertions (`is not None`, `hasattr`) in runtime tests with behavior/result invariants.
4. Move private-function assertions (`_normalize_options`, `_resolve_store_spec`) to unit tests or rewrite integration tests around public boundaries (`register_delta_object_store`).
5. Remove hard-coded `/tmp/...` paths in favor of `tmp_path` fixtures and generated per-test locations.

## Realism/Robustness Additions Recommended

1. Add negative-path contract tests for runtime boundaries with precise taxonomy assertions:
   - cache introspection install failures
   - delta provider resolution failures
2. For capability-dependent install tests, validate one representative successful behavior path after installation (not only "installed=true").
3. Add repeatability/idempotency checks under realistic sequence operations:
   - register/unregister/re-register cycles
   - multi-step runtime session reuse with state cleanup assertions
4. Add diagnostics contract checks where boundaries are expected to emit artifacts:
   - required keys
   - value semantics (not just artifact presence)
5. Add environment-variance resilient fixtures:
   - isolate filesystem under `tmp_path`
   - avoid global monkeypatch of broad import surfaces unless no seam exists

## Test Structure Optimizations

1. Reclassify unit-like integration suites where appropriate:
   - move purely immutability/constructor contract checks closer to unit boundaries
   - keep integration directory for multi-subsystem behavior and runtime boundary checks
2. Keep integration suites boundary-oriented with explicit "Boundary Under Test" sections and public entrypoint usage.
3. Adopt a "contract-first assertion template" for integration tests:
   - precondition
   - action via public boundary
   - functional outcome
   - diagnostics/artifact contract

## Prioritized Remediation Plan

### P0
- Fix marker coverage gap (7 files, 17 tests currently deselected).
- Tighten broad exception handling in runtime boundary tests.

### P1
- Upgrade weak runtime assertions to semantic/contract assertions.
- Remove private API coupling from integration suites.

### P2
- Reclassify unit-like integration tests and streamline suite taxonomy.
- Expand realism scenarios for capability-dependent runtime paths.

## Conclusion

The highest immediate value is fixing integration marker consistency and strengthening runtime boundary assertions/error contracts. After those are in place, reclassifying unit-like tests and reducing private API coupling will improve long-term maintainability while raising the production signal quality of `tests/integration`.
