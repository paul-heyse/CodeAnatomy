# Review of Expanded Integration Testing Proposal v2

Date: 2026-02-06

Inputs reviewed:
- `docs/plans/expanded_integration_testing_v2.md`
- `docs/plans/integration_testing_proposal.md`

Environment bootstrap completed before review:
- `bash scripts/bootstrap_codex.sh`
- `uv sync`

## Executive Summary

The v2 proposal is directionally strong and covers real cross-subsystem seams, but several suites need correction before implementation:

1. Some ownership assumptions are incorrect (pipeline vs build orchestration boundaries).
2. A materializer test assumption is incorrect (`TableSummarySaver` does not perform Delta writes).
3. The proposed `tests/integration/e2e/` location conflicts with current test policy.
4. Some planned tests duplicate existing integration/unit coverage and should be merged or tightened.

After CQ-based mapping, the highest-value additions are:
- direct `compile_execution_plan` integration coverage (including Delta pin conflict path);
- direct `materialize_extract_plan` boundary coverage (high fan-out);
- direct `resolve_dataset_provider` boundary coverage (critical provider choke-point);
- explicit `plan_execution_diff_v1` diagnostics assertions.

## Corrections (Must Fix)

### 1. Pipeline ownership: move heartbeat/signal/build-phase assertions to `build_graph_product`

Proposed Suite 3.1 currently anchors several assertions to `execute_pipeline()`, but lifecycle ownership is in `build_graph_product()` / `_execute_build()`.

Evidence:
- `execute_pipeline()` in `src/hamilton_pipeline/execution.py:256` handles Hamilton execute/materialize and emits plan diff artifacts.
- heartbeat start/stop and signal handler install/restore are in `src/graph/product_build.py:159` and `src/graph/product_build.py:190`.
- `build.phase.start` / `build.phase.end` events are in `src/graph/product_build.py:296` and `src/graph/product_build.py:326`.
- CQ `calls execute_pipeline` reports one caller: `_execute_build` in `src/graph/product_build.py:306`.

Correction:
- Keep `execute_pipeline` tests focused on pipeline execution semantics.
- Add a separate build-orchestration suite for heartbeat/signal/build-phase diagnostics around `build_graph_product`.

### 2. Materializer boundary correction: `TableSummarySaver` is metadata capture, not Delta writer

Proposed `test_materializer_writes_delta_table` in Suite 3.2 is not aligned with implementation.

Evidence:
- `TableSummarySaver.save_data()` returns summary metadata fields (`rows`, `columns`, `plan_signature`, etc.) in `src/hamilton_pipeline/materializers.py:53`.
- It does not call Delta write APIs.
- `build_hamilton_materializers()` wires summary materializers over `delta_output_specs()` in `src/hamilton_pipeline/materializers.py:126`.

Correction:
- Replace Delta-write assertion with:
  - summary payload contract checks for `TableSummarySaver`;
  - factory wiring checks against `delta_output_specs()`;
  - optional payload contract check via `validate_delta_output_payload` where applicable (`src/hamilton_pipeline/io_contracts.py:169`).

### 3. E2E directory correction

The proposal’s Phase 6 location `tests/integration/e2e/` conflicts with repo policy.

Evidence:
- `tests/AGENTS.md:11` defines `tests/e2e/` as the e2e directory.

Correction:
- Place new end-to-end pipeline tests under `tests/e2e/`.

### 4. CDF cursor lifecycle scope should avoid unit-coverage duplication

Much of Suite 3.12 duplicates existing unit coverage.

Evidence:
- `get_start_version` behavior already covered in `tests/unit/semantics/incremental/test_cdf_cursors.py:239`.
- path sanitization behaviors already covered in `tests/unit/semantics/incremental/test_cdf_cursors.py:268`.

Correction:
- Keep integration coverage only for cursor <-> Delta/CDF runtime interaction (state progression through real CDF reads/writes), not standalone store mechanics.

## Optimization Recommendations (Reduce Redundancy, Increase Signal)

### 1. Merge or downscope overlap with existing semantic-input integration tests

Existing coverage already includes key semantic input validation paths:
- `validate_semantic_inputs` usage in `tests/integration/test_semantic_pipeline.py:44`, `tests/integration/test_semantic_pipeline.py:58`, `tests/integration/test_semantic_pipeline.py:79`.
- `validate_semantic_input_columns` path in `tests/integration/test_semantic_pipeline.py:198`.

Optimization:
- Keep Suite 3.9 only for cases not already asserted (type mismatch, missing required typed columns, cross-module error payload quality).

### 2. Avoid duplicating current scheduling/cost-context integration checks

Existing scheduling suite already covers:
- topological order and generation waves: `tests/integration/scheduling/test_schedule_generation.py:222`, `tests/integration/scheduling/test_schedule_generation.py:277`;
- cost-context wiring: `tests/integration/scheduling/test_schedule_generation.py:316`;
- determinism: `tests/integration/scheduling/test_schedule_generation.py:350`.

Optimization:
- In new relspec suites, prioritize compile-time behavior and contract surfaces (not basic scheduler output already covered).

### 3. Consolidate evidence-plan gating suites

Current overlap exists between:
- `tests/integration/boundaries/test_evidence_plan_to_extractor.py:1`
- `tests/integration/error_handling/test_evidence_plan_gating.py:1`

Optimization:
- Keep one gating suite for flag/template projection logic.
- Add one direct materialization suite specifically for `apply_query_and_project` + `materialize_extract_plan` effects.

### 4. Prefer boundary assertions over structural smoke assertions

Many current runtime tests validate object existence/`isinstance` only.

Optimization:
- In the new scope, bias toward artifact payload assertions, contract fields, and failure payload semantics at subsystem boundaries.

## Additions Recommended for v2 Scope

### A. Add explicit `compile_execution_plan` boundary tests

Why:
- CQ shows a single choke-point callsite (`src/hamilton_pipeline/driver_factory.py:527`) with no direct integration tests.

Add:
- runtime-profile mismatch raises (`src/relspec/execution_plan.py:633`);
- plan artifact persistence failure records `plan_artifacts_store_failed_v1` without aborting compile (`src/relspec/execution_plan.py:678`);
- requested task without `plan_bundle` validation (`src/relspec/execution_plan.py:535`).

### B. Add Delta pin conflict test (currently absent)

Why:
- Conflict behavior is explicit but untested.

Add:
- conflicting scan-unit pins for one dataset should raise with clear message from `src/datafusion_engine/plan/bundle.py:255`.

### C. Add explicit `plan_execution_diff_v1` diagnostics test

Why:
- Emitted in pipeline execution but currently no tests found.

Add:
- assert artifact emission and payload shape for missing/unexpected tasks from `_emit_plan_execution_diff` in `src/hamilton_pipeline/execution.py:367`.

### D. Add direct `materialize_extract_plan` integration tests

Why:
- CQ call fan-out is high (24 callsites across 13 files).

Add:
- projection/gating behavior through `apply_query_and_project` (`src/extract/coordination/materialization.py:293`);
- streaming vs non-streaming materialization behavior (`src/extract/coordination/materialization.py:560` and `src/extract/coordination/materialization.py:595`);
- compile/execute artifact emission (`extract_plan_compile_v1`, `extract_plan_execute_v1`).

### E. Add direct `resolve_dataset_provider` integration tests for both provider kinds

Why:
- CQ reports 7 callsites across critical read/write/service paths.

Add:
- `provider_kind="delta"` and `provider_kind="delta_cdf"` resolution paths in `src/datafusion_engine/dataset/resolution.py:74`.

### F. Strengthen idempotent-write scope to include multi-operation propagation

Why:
- `idempotent_commit_properties` is reused by write, delete, and snapshot merge flows.

Add:
- verify metadata/transaction propagation from `src/storage/deltalake/delta.py:3281` through:
  - `WritePipeline._delta_write_spec` (`src/datafusion_engine/io/write.py:1430` callsite per CQ);
  - incremental delete/merge flows (callsites in `src/semantics/incremental/delta_updates.py` and `src/semantics/incremental/snapshot.py`).

## Revised Priority (Recommended)

### P0 (highest risk / highest fan-out / currently untested)
- `compile_execution_plan` end-to-end boundary tests (including Delta pin conflict + artifact-store failure path).
- `materialize_extract_plan` end-to-end boundary tests.
- `resolve_dataset_provider` boundary tests.

### P1
- Build orchestration lifecycle tests (`build_graph_product` heartbeat/signal/build-phase events).
- `execute_pipeline` diagnostics tests (`plan_execution_diff_v1`).
- Idempotent commit propagation tests.

### P2
- Multi-source span normalization and file identity suites.
- Additional semantic compiler/UDF availability boundary tests.

### P3
- New full-pipeline tests under `tests/e2e/` (minimal deterministic smoke first).

## CQ Evidence Snapshot

Key fan-out/choke-point findings used to prioritize scope:
- `materialize_extract_plan`: 24 callsites across 13 files (`./cq calls materialize_extract_plan`).
- `resolve_dataset_provider`: 7 callsites across 7 files (`./cq calls resolve_dataset_provider`).
- `validate_required_udfs`: 10 callsites across 8 files (`./cq calls validate_required_udfs`).
- `idempotent_commit_properties`: 3 callsites across 3 files (`./cq calls idempotent_commit_properties`).
- `compile_execution_plan`: 1 callsite (`./cq calls compile_execution_plan`) in `src/hamilton_pipeline/driver_factory.py:527`.
- `execute_pipeline`: 1 callsite (`./cq calls execute_pipeline`) in `src/graph/product_build.py:306`.

No direct test references found for:
- `compile_execution_plan` in integration tests (`./cq search compile_execution_plan --in tests/integration`).
- `plan_execution_diff_v1` in tests (`./cq search plan_execution_diff_v1 --in tests`).

## Proposed Document Update Impact

If adopted, the updated scope will:
- remove incorrect or duplicate tests before implementation effort is spent;
- shift effort to high-risk boundaries with verified blast radius;
- keep suite structure aligned with repo test taxonomy (`integration` vs `e2e`).
