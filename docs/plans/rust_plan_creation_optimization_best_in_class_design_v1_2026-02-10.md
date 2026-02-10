# Rust Plan Creation + Optimization Best-in-Class Design (Hard Cutover)

Date: 2026-02-10

## 1) Goal

Define the target architecture for plan creation and optimization as a Rust-first system centered on `rust/codeanatomy_engine`, and explicitly displace overlapping planning/optimization capabilities currently implemented in `src/`.

This plan is design-first and allows breaking changes where needed to reach a cleaner long-term architecture.

## 2) Inputs Used

This design uses:

- `dfdl_ref` references:
  - `.claude/skills/dfdl_ref/reference/datafusion_planning.md`
  - `.claude/skills/dfdl_ref/reference/datafusion_plan_combination.md` (including post-1525 addendum)
  - `.claude/skills/dfdl_ref/reference/Datafusion_logicplan_rust.md`
  - `.claude/skills/dfdl_ref/reference/deltalake_datafusion_integration.md`
  - `.claude/skills/dfdl_ref/reference/deltalake_datafusionmixins.md`
- Current Rust implementation:
  - `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`
  - `rust/codeanatomy_engine/src/compiler/graph_validator.rs`
  - `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`
  - `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs`
  - `rust/codeanatomy_engine/src/session/planning_surface.rs`
  - `rust/codeanatomy_engine/src/session/planning_manifest.rs`
  - `rust/codeanatomy_engine/src/session/factory.rs`
  - `rust/codeanatomy_engine/src/rules/optimizer.rs`
  - `rust/codeanatomy_engine/src/rules/physical.rs`
  - `rust/codeanatomy_engine/src/providers/registration.rs`
  - `rust/codeanatomy_engine/src/providers/pushdown_contract.rs`
  - `rust/codeanatomy_engine/src/compliance/capture.rs`
  - `rust/codeanatomy_engine_py/src/materializer.rs`
- Current Python overlaps:
  - `src/relspec/execution_plan.py`
  - `src/relspec/execution_authority.py`
  - `src/relspec/calibration_bounds.py`
  - `src/datafusion_engine/plan/pipeline.py`
  - `src/datafusion_engine/plan/bundle.py`
  - `src/datafusion_engine/plan/execution.py`
  - `src/datafusion_engine/session/factory.py`
  - `src/datafusion_engine/views/graph.py`
  - `src/engine/runtime.py`
  - `src/engine/runtime_profile.py`
  - `src/engine/build_orchestrator.py`
  - `src/engine/spec_builder.py`

## 3) Current-State Assessment

## 3.1 Rust Strengths Already Present

- Deterministic planning identity and manifest hashing are in place:
  - `PlanningSurfaceSpec` + `PlanningSurfaceManifest` (`session/planning_surface.rs`, `session/planning_manifest.rs`)
- Session construction is mostly canonicalized:
  - `SessionFactory::build_session_state_internal` (`session/factory.rs`)
- Core plan artifact contract exists:
  - P0/P1/P2 digests + explain + Substrait/SQL/codec optionals (`compiler/plan_bundle.rs`)
- Pushdown contract model uses `Unsupported|Inexact|Exact`:
  - `providers/pushdown_contract.rs`
- Compliance and warning model are structured:
  - `compliance/capture.rs`, `executor/warnings.rs`
- Shared pre-pipeline orchestration exists and is used by Python bindings:
  - `executor/orchestration.rs`, `codeanatomy_engine_py/src/materializer.rs`

## 3.2 Core Gaps vs Best-in-Class DataFusion/Delta Design

Relative to the DataFusion planning references, key gaps are:

1. Optimizer control plane is narrow:
   - Rust rules are currently minimal in scope (`rules/optimizer.rs`, `rules/physical.rs`)
   - No first-class standalone optimizer harness API for controlled rulepack experiments and per-pass observability as a primary compile interface.
2. Graph validation is mostly structural:
   - `graph_validator.rs` validates names/references but not stronger semantic contracts (transform invariants, expression/type conformance, cross-view schema compatibility classes).
3. Planning-surface governance is present but incomplete:
   - `PlanningSurfaceSpec` has file formats/table options/planners, but table-factory and format-factory strategy is not yet a full SQL DDL control plane with strict extension contracts.
4. Provider pushdown probing is compliance-time:
   - We probe pushdown status, but compile-time plan-correctness contracts around residual filters and optimizer trust are not yet enforced as first-class planner invariants.
5. Plan creation/optimization logic is still duplicated in Python stack:
   - `src/relspec/execution_plan.py`, `src/datafusion_engine/plan/*`, and `src/datafusion_engine/views/graph.py` implement planning/scheduling/bundle behaviors that should become Rust-owned for long-term coherence.

## 4) End-State Design Principles

1. Single planning/optimization authority in Rust:
   - `rust/codeanatomy_engine` owns plan graph compilation, optimization policy, artifact generation, and optimizer observability.
2. Python becomes orchestration + request/response boundary only:
   - `src/` keeps API ergonomics, input normalization, and high-level workflow composition.
3. Determinism-by-construction:
   - Session/planning/provider/runtime surfaces must be captured in canonical hashes and persisted artifacts.
4. Explainable optimization:
   - Every optimizer stage should be introspectable with stable diff artifacts.
5. Provider correctness over heuristics:
   - Pushdown contracts and residual semantics must be machine-validated.
6. Hard cutover, not prolonged dual-stack:
   - duplicate planning engines in Python are retired, not co-maintained.

## 5) Target Architecture

## 5.1 Rust Compiler Stack (Authoritative)

### Layer A: Spec + Validation

- `SemanticExecutionSpec` remains the wire contract (`spec/execution_spec.rs`) but validation is upgraded from structural to semantic.
- Introduce richer validation modules:
  - transform legality checks
  - expression binding/type checks
  - view/output schema compatibility checks
  - join-key and aggregation invariant checks

### Layer B: Planning Surface + Environment

- Keep `PlanningSurfaceSpec` as the canonical environment definition.
- Expand it into a full control plane for:
  - `FileFormatFactory` registration policy
  - `TableProviderFactory` registration policy
  - `TableOptions` policy profiles
  - query/expr/function planner registries
- Manifest hash must include all planning-affecting knobs (including extension registries and relevant config options).

### Layer C: Logical Plan Construction

- Keep transform-to-DataFrame compilation in `plan_compiler.rs`, but split into:
  - IR normalization pass
  - semantic validation pass
  - deterministic build pass
- Add explicit plan-build request/response types (instead of broad method-local state).

### Layer D: Optimization Pipeline

- Add first-class optimizer pipeline component with:
  - configured pass sequence (analyzer/logical/physical)
  - deterministic pass IDs
  - per-pass observer output (before/after digests + optional normalized plans)
  - policy-controlled max passes, skip-failed behavior, and fail-open/fail-closed modes
- Preserve integration with existing runtime config but move “lab mode” into generic optimizer pipeline instrumentation.

### Layer E: Artifact + Diff + Replay

- Continue `PlanBundleArtifact` as canonical persisted format.
- Expand artifact contract to include:
  - pass-level optimization trace summary
  - residual-filter validation summaries
  - stronger source/provider lineage linkages
  - explicit artifact schema version evolution policy

## 5.2 Python Boundary (Non-Authoritative)

- Python responsibilities:
  - Spec assembly (`src/engine/spec_builder.py`) and submission
  - CLI/application orchestration
  - High-level diagnostics presentation
- Python must not:
  - compile dependency graphs
  - perform DataFusion plan construction/optimization logic
  - own plan bundle canonicalization logic

## 6) Duplicate Capability Displacement Plan (`src/` -> `rust/`)

## 6.1 Remove/Retire (Hard Cutover)

- Planning graph + schedule compilation:
  - `src/relspec/execution_plan.py`
- Python-native DataFusion planning pipeline and bundle construction:
  - `src/datafusion_engine/plan/pipeline.py`
  - `src/datafusion_engine/plan/bundle.py`
  - `src/datafusion_engine/plan/execution.py`
- Python-side view graph plan artifact extraction as a planning authority:
  - `src/datafusion_engine/views/graph.py` (retain only optional presentation helpers if needed)
- Python session-factory logic that duplicates Rust planning-surface governance:
  - major portions of `src/datafusion_engine/session/factory.py`

## 6.2 Retain as Boundary/Facade

- `src/engine/spec_builder.py`:
  - retained as the external contract builder, with strict Rust parity tests.
- `src/engine/build_orchestrator.py`, CLI entrypoints:
  - retained as orchestration layer.
- `src/engine/runtime.py`, `src/engine/runtime_profile.py`:
  - retained only for top-level runtime profile selection and handoff; remove planning internals that duplicate Rust behavior.

## 7) High-Impact Improvement Workstreams

## WS1: Semantic Validator Upgrade

Add a new semantic validation subsystem in Rust:

- Files:
  - `rust/codeanatomy_engine/src/compiler/graph_validator.rs` (refactor)
  - new `rust/codeanatomy_engine/src/compiler/semantic_validator.rs`
- Add checks for:
  - unsupported transform compositions
  - unresolved/ambiguous column references in filter/project/aggregate
  - join key compatibility
  - output contract satisfiability

## WS2: Optimizer Pipeline Framework

Create explicit optimizer orchestration module:

- Files:
  - new `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs`
  - `rust/codeanatomy_engine/src/rules/optimizer.rs`
  - `rust/codeanatomy_engine/src/rules/physical.rs`
  - `rust/codeanatomy_engine/src/stability/optimizer_lab.rs`
- Capabilities:
  - pass sequencing + rulepack bundles
  - per-pass observers with stable digests
  - configurable strictness (`skip_failed_rules` strategy)
  - policy-driven experiment harness

## WS3: Planning Surface Governance Expansion

Elevate `PlanningSurfaceSpec` into an explicit extension governance model:

- Files:
  - `rust/codeanatomy_engine/src/session/planning_surface.rs`
  - `rust/codeanatomy_engine/src/session/planning_manifest.rs`
  - `rust/codeanatomy_engine/src/session/factory.rs`
  - `rust/datafusion_ext/src/*` (extension provider/factory modules)
- Capabilities:
  - explicit allowlist + identity capture for file formats/table factories
  - strict table options policy profiles
  - planning manifest coverage for all registries that affect plan shape

## WS4: Pushdown Correctness Contract Enforcement

Promote pushdown from “probe-only diagnostics” to “planner contract”:

- Files:
  - `rust/codeanatomy_engine/src/providers/pushdown_contract.rs`
  - `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs`
  - `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`
  - `rust/codeanatomy_engine/src/compliance/capture.rs`
- Add:
  - residual-filter assertions for `Inexact`
  - compile-time/provider-contract summaries in artifacts
  - deterministic warning or hard-fail modes by runtime profile

## WS5: Plan Artifact Evolution and Replay Readiness

Strengthen persisted plan contract:

- Files:
  - `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`
  - `rust/codeanatomy_engine/src/compiler/plan_codec.rs`
  - `rust/codeanatomy_engine/src/executor/pipeline.rs`
- Add:
  - optimizer pass trace sections
  - stronger schema/version migration strategy
  - explicit replay compatibility flags

## WS6: Cost Model + Scheduling Ownership in Rust

Move task-cost/scheduling authority from Python to Rust:

- Files:
  - new `rust/codeanatomy_engine/src/compiler/scheduling.rs`
  - new `rust/codeanatomy_engine/src/compiler/cost_model.rs`
  - `rust/codeanatomy_engine/src/executor/orchestration.rs`
- Replace Python-side scheduling logic in `src/relspec/execution_plan.py` with Rust-returned schedule metadata.

## WS7: Hard Cutover of Python Planning Stack

Delete or freeze Python planning internals after Rust replacements are in place:

- Primary removals:
  - `src/relspec/execution_plan.py`
  - `src/datafusion_engine/plan/pipeline.py`
  - `src/datafusion_engine/plan/bundle.py`
  - `src/datafusion_engine/plan/execution.py`
- Adapt callers to Rust result payloads and thin adapters.

## WS8: Compatibility Contract and Breaking Change Package

Ship as a coordinated breaking release:

- Remove deprecated Python plan/bundle types
- Version bump spec and artifact schemas
- Enforce strict unknown-field rejection at Rust boundary

## 8) Proposed Public Interface Changes

1. Rust planning compile API:
   - add `CompileRequest` / `CompileResponse` types for deterministic compilation and metadata output.
2. Rust optimizer pipeline API:
   - add `OptimizerPipelineConfig`, `OptimizerPassTrace`, `OptimizerPipelineResult`.
3. Rust result envelope:
   - include scheduling/cost metadata currently only available in Python planning objects.
4. Python bridge API:
   - replace Python-native plan bundle classes with Rust-emitted artifact envelopes only.

## 9) Testing and Acceptance Criteria

## 9.1 Determinism

- same spec + same planning surface + same provider identities => stable:
  - envelope hash
  - planning surface hash
  - plan digests (P0/P1/P2)
  - optimizer pass traces

## 9.2 Pushdown Correctness

- `Unsupported`: filter remains above scan
- `Inexact`: residual filter retained and recorded
- `Exact`: residual eligible for elimination

## 9.3 Cross-Boundary Consistency

- Python facade outputs exactly match Rust artifacts; no Python-only plan logic drift.

## 9.4 Cutover Safety

- No runtime path may invoke deprecated Python planning modules once cutover flag is enabled.

## 10) Rollout Strategy

1. Add Rust capabilities first behind internal feature flags.
2. Introduce dual-read validation in CI (Rust artifacts vs legacy Python outputs) for one short transition window.
3. Flip default to Rust-authoritative planning.
4. Remove legacy Python planning modules in next release.

## 11) Explicit Breaking Changes (Approved by Design Goal)

1. Python internal planning APIs in `src/relspec` and `src/datafusion_engine/plan` become unsupported.
2. Plan artifact schemas evolve (version bump required).
3. Runtime profile fields that drive planning must be Rust-validated; ambiguous Python-only knobs are removed.
4. Any behavior dependent on legacy Python scheduling heuristics is replaced by Rust cost/scheduling outputs.

## 12) Final Conclusion

The best-in-class path is to complete a hard architectural consolidation: Rust owns all DataFusion/Delta planning and optimization semantics, while Python becomes a thin contract/orchestration layer.

The codebase already has strong foundations (planning surface manifests, deterministic envelopes, structured warnings, plan artifacts). The remaining leap is eliminating dual planning authorities and elevating Rust’s optimizer/validation/pushdown enforcement into a comprehensive compiler-style pipeline with first-class observability and strict contracts.
