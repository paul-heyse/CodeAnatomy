# DataFusion + DeltaLake Rust-First Semantic Execution Architecture v1

Date: 2026-02-07

## Summary

This proposal defines a best-in-class target architecture for CodeAnatomy that:

- Keeps extraction outside planning and executes extractor adapters in parallel up front.
- Consolidates planning, sequencing, optimization, and execution into a DataFusion + DeltaLake core.
- Moves the execution-critical path to Rust, while preserving a thin Python control surface.
- Prioritizes semantic correctness, deterministic behavior, and Delta-native performance (file pruning, snapshot pinning, protocol-aware reads/writes).
- De-prioritizes run-artifact expansion for now and focuses on robust execution and correctness guarantees.

This plan is intentionally designed for side-by-side implementation and controlled cutover from the current Hamilton + rustworkx orchestration path.

---

## Design Principles

1. One plan graph per workload: combine work as one LogicalPlan DAG, not stitched fragments.
2. Semantic-first planning: relational plans are compiled from semantic intent, not from imperative orchestration steps.
3. Delta-native by default: `DeltaTableProvider` and scan config controls are the primary storage/query path.
4. Rust on the hot path: planner rules, Delta control plane, and execution-critical logic live in Rust crates.
5. Python as API/UX shell: CLI, config, and operational glue remain in Python initially.
6. Deterministic execution: snapshot/version pinning, stable resolution, and policy-bound planner behavior.

---

## Current-State Anchors (Codebase)

The migration plan is anchored to existing seams:

- Entrypoint currently routes through Hamilton:
  - `src/graph/product_build.py`
  - `src/hamilton_pipeline/execution.py`
- Cross-task plan/schedule logic currently in relspec/rustworkx:
  - `src/relspec/execution_plan.py`
  - `src/relspec/rustworkx_graph.py`
  - `src/relspec/rustworkx_schedule.py`
- DataFusion-native plan core already exists:
  - `src/datafusion_engine/plan/pipeline.py` (`plan_with_delta_pins`)
- Rust already has Delta/DataFusion control-plane capabilities:
  - `rust/datafusion_ext/src/delta_control_plane.rs`
  - `rust/datafusion_ext/src/planner_rules.rs`
  - `rust/datafusion_ext/src/physical_rules.rs`
  - `rust/datafusion_python/src/context.rs`

---

## Target Runtime Architecture

### Stage 0: Parallel Extraction (Outside Planner)

Execute extractor adapters first, in parallel, independent of relational planning:

- `repo_scan`
- `scip`
- `python_imports`
- `python_external`
- `ast`
- `cst`
- `tree_sitter`
- `bytecode`
- `symtable`

Materialize extractor outputs directly into Delta tables with a deterministic naming and snapshot convention.

### Stage 1: Semantic Compile to Relational Intent

Compile semantic model intent into a normalized relational spec:

- Canonical dataset references.
- Join contracts and inferred dependency edges.
- Policy constraints (allowed operators/statements, scan policy intent).
- Materialization intent (target Delta outputs and write modes).

This stage outputs a declarative semantic execution spec, not an imperative task graph.

### Stage 2: Global Plan Combination

Use Rust DataFusion planning to combine all relational work into one or a small number of plan DAGs:

- Compose via joins/unions/subqueries/CTEs.
- Register pinned Delta providers and semantic views in one session context.
- Apply analyzer and optimizer rules from a policy-bound rulepack.

### Stage 3: Delta-Aware Optimization + Physical Planning

Generate physical plans under Delta-aware controls:

- Snapshot/version/time-travel pinning.
- Scan config overrides (`file_column_name`, pushdown behavior, partition wrapping).
- File-level pruning and optional explicit `with_files(...)` narrowing.
- Partitioning/repartition strategy from profile and policy.

### Stage 4: Execution + Delta Commit

Execute with Rust-owned runtime path:

- Stream-first execution.
- Delta writes/commits through provider-supported append/overwrite and Delta commit APIs.
- Strict protocol/feature gates where configured.

---

## Rust-First Responsibility Split

### Rust (primary ownership)

1. Delta control plane:
   - Provider construction, snapshot loading, protocol gating, file-action filtering.
   - Build on and extend `rust/datafusion_ext/src/delta_control_plane.rs`.
2. Planner governance:
   - Analyzer/optimizer/physical rule installation and policy enforcement.
   - Extend `rust/datafusion_ext/src/planner_rules.rs` and `rust/datafusion_ext/src/physical_rules.rs`.
3. Session/runtime behavior:
   - Session config defaults, capability checks, planner/execution knobs.
   - Extend `rust/datafusion_python/src/context.rs` and extension exports.
4. Execution path:
   - Plan execution, partition behavior, backpressure-aware streaming.
5. Delta mutation path:
   - Append/overwrite and controlled mutation features via delta-rs APIs.

### Python (thin layer)

1. User-facing API:
   - CLI and request shaping.
2. Semantic input model assembly:
   - Build semantic spec payload consumed by Rust planner.
3. Extraction orchestration:
   - Keep current extract adapter runtime initially, then incrementally migrate.
4. Cutover controls:
   - Runtime flags and profile selection for old/new paths.

---

## Core Interfaces (New/Updated)

### 1) Semantic Execution Spec (new)

Create a single serialized spec passed from Python to Rust with:

- Semantic outputs requested.
- Dataset bindings and Delta locations.
- Join and dependency hints.
- Policy controls (DML/DDL allowance, scan policy preferences).
- Snapshot/version pins and optional timestamp pins.
- Execution mode and materialization directives.

Use msgspec-compatible payloads on Python side and strict Rust deserialization contracts.

### 2) Rust Planner API (new)

Expose a Rust API entrypoint (Python-callable) that:

- Accepts semantic spec + runtime profile.
- Builds/validates session context.
- Registers providers/views/UDF/UDAF/UDTF/UDWF.
- Produces executable plan handles and executes.

### 3) Delta Registration Contract (updated)

Standardize Delta registration semantics:

- Default to native provider path.
- Optional explicit file subset.
- Explicit failure for unsupported fallback in strict mode.

---

## Migration Plan

### Phase A: Foundation (Rust planner lane, no cutover)

1. Add semantic spec contract and Python->Rust bridge.
2. Add Rust planner/executor entrypoint with no behavioral parity guarantee yet.
3. Register Delta providers from Rust with snapshot pinning.

Exit criteria:
- End-to-end execution succeeds on small representative workload through new lane.

### Phase B: Semantic parity and policy parity

1. Port current policy checks into Rust analyzer/optimizer rule stack.
2. Match current semantic outputs for representative repositories.
3. Keep Hamilton/rustworkx as production path; run new lane in shadow mode.

Exit criteria:
- Deterministic output parity on agreed parity suite.

### Phase C: Performance and Delta optimization

1. Enable aggressive Delta-native pruning controls.
2. Tune partitioning/repartition and scan pushdown policy by profile.
3. Validate runtime stability under larger repos.

Exit criteria:
- Throughput/latency wins on benchmark repos without correctness regression.

### Phase D: Controlled cutover

1. Introduce runtime flag defaulting selected environments to Rust-first lane.
2. Keep fallback to legacy path during stabilization window.
3. Remove Hamilton/rustworkx execution ownership after sustained parity.

Exit criteria:
- New lane stable as default with no blocker regressions.

### Phase E: Legacy decommission

1. Remove legacy orchestration paths from `src/hamilton_pipeline/*` and `src/relspec/*` execution ownership.
2. Keep only compatibility shims required by external API boundaries.

Exit criteria:
- Rust-first planner/executor is sole production execution path.

---

## DeltaLake Maximization Checklist

1. Native provider default for all Delta datasets.
2. Snapshot pinning and protocol gate enforcement before planning/execution.
3. File-level pruning using Delta log metadata.
4. Optional `with_files(...)` narrowing for semantically constrained scans.
5. Policy-driven scan config (`enable_parquet_pushdown`, partition/value controls).
6. CDF provider path for incremental/change-driven flows where enabled.
7. Strict identifier normalization policy (quoted identifiers or normalization policy) to avoid mixed-case failures.

---

## Semantically Driven Execution Model

The new control flow should be:

1. Semantic compiler emits desired graph/data outcomes and constraints.
2. Rust planner maps constraints into DataFusion logical plan construction.
3. Optimizer and physical planner execute under explicit policy.
4. Delta commit layer enforces storage correctness and protocol compatibility.

This replaces imperative DAG task scheduling with declarative semantic plan construction and rule-driven optimization.

---

## Non-Goals (for this phase)

1. Expanding run-artifact schema and diagnostics breadth.
2. Introducing broad backward-compat shims for legacy orchestration internals.
3. Rewriting all extractors to Rust in the first cut.

These can be addressed after stable execution cutover.

---

## Risks and Mitigations

1. Risk: Semantic parity gaps vs legacy orchestration.
   - Mitigation: Shadow-mode dual-run parity gates before default cutover.
2. Risk: Version drift across DataFusion/delta-rs/Python bindings.
   - Mitigation: lockstep version matrix and CI compatibility checks in Rust wheel pipeline.
3. Risk: Hidden dependency on Hamilton/rustworkx side effects.
   - Mitigation: explicit dependency inventory and staged route replacement by boundary.
4. Risk: Over-optimistic extraction-always strategy cost.
   - Mitigation: keep initial simplification, add change-based extractor gating once execution path is stable.

---

## Acceptance Criteria

1. New Rust-first lane executes end-to-end from semantic spec to Delta materialization.
2. Delta-native provider path is default and enforced in strict mode.
3. Semantic output parity passes on agreed benchmark/parity suite.
4. Performance improves on representative workloads (planning + execution wall-clock and scan efficiency).
5. Legacy Hamilton/rustworkx execution path can be disabled without functional loss.

---

## Immediate Next Implementation Steps

1. Define `SemanticExecutionSpec` payload in Python and Rust.
2. Add Rust entrypoint for "plan+execute semantic spec" in existing extension crates.
3. Add Python feature flag path in `src/graph/product_build.py` to call new Rust lane.
4. Build shadow-mode parity harness for selected repos.
5. Gate cutover on parity + performance thresholds.
