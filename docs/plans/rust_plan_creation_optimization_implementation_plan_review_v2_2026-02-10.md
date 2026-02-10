# Rust Plan Creation + Optimization Implementation Plan Review (v2)

Date: 2026-02-10
Reviewed Plan: `docs/plans/rust_plan_creation_optimization_implementation_plan_v1_2026-02-10.md`
Reviewed Design: `docs/plans/rust_plan_creation_optimization_best_in_class_design_v1_2026-02-10.md`

## 1) Executive Summary

The implementation plan is directionally strong and aligned with the design intent, but it now has several factual mismatches with the current codebase and misses a few high-leverage architecture scopes needed for a true best-in-class end state.

Key conclusions:

- The plan must be re-baselined to DataFusion 51 and current crate topology before execution.
- Several planned items are already implemented and should be removed from "to-do" scope to avoid churn.
- The remaining core gap is not "features in isolation"; it is enforcing a single, observable, deterministic planning authority with strict environment governance and optimizer observability contracts.
- Additional net-new scope is required for long-term robustness: planner environment identity, portable plan contract policy, statistics-quality governance, and Delta protocol compatibility controls.

## 2) Evidence Snapshot (Current Code)

- Workspace DataFusion is pinned to `51.0.0` in `rust/Cargo.toml:27` and `rust/Cargo.toml:47`.
- `datafusion-tracing` is already a workspace dependency and an optional engine dependency in `rust/codeanatomy_engine/Cargo.toml:22` and enabled in `rust/codeanatomy_engine/Cargo.toml:65`.
- Core crate split has already happened:
  - Rust-only core crate type in `rust/codeanatomy_engine/Cargo.toml:13`.
  - Binding crate present in workspace in `rust/Cargo.toml:5`.
- Structured warnings are already implemented (`RunWarning`, `WarningCode`, `WarningStage`) in `rust/codeanatomy_engine/src/executor/warnings.rs:7`.
- Runtime toggles already include `capture_optimizer_lab`, `capture_delta_codec`, and lineage tags in `rust/codeanatomy_engine/src/spec/runtime.rs:199`, `rust/codeanatomy_engine/src/spec/runtime.rs:203`, and `rust/codeanatomy_engine/src/spec/runtime.rs:227`.
- Session build canonicalization and overrides already exist in `rust/codeanatomy_engine/src/session/factory.rs:53` and `rust/codeanatomy_engine/src/session/factory.rs:95`.
- Shared pre-pipeline orchestration already exists in `rust/codeanatomy_engine/src/executor/orchestration.rs:16`.
- Plan bundle already carries delta codec fields and diff diagnostics in `rust/codeanatomy_engine/src/compiler/plan_bundle.rs:111` and `rust/codeanatomy_engine/src/compiler/plan_bundle.rs:173`.
- Legacy template payloads are already rejected at boundary in `rust/codeanatomy_engine_py/src/compiler.rs:45`.
- Pushdown probe derivation already expanded to multiple transform shapes in `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs:3`.

## 3) Required Corrections to v1 Implementation Plan

## C1. Version baseline is wrong

Issue:
- v1 states `DataFusion version: 50.1+` and recommends pinning `datafusion = "50.1"`.

Correction:
- Rebaseline all plan text, snippets, and risk items to DataFusion `51.0.0`.

Why:
- Current workspace and crates are fully pinned to 51 (`rust/Cargo.toml:27`).

## C2. `datafusion-tracing` dependency step is obsolete

Issue:
- v1 includes a work item to add `datafusion-tracing` dependency.

Correction:
- Replace with "verify feature coverage and instrumentation policy" scope.

Why:
- Dependency and feature wiring already exist (`rust/codeanatomy_engine/Cargo.toml:22`, `rust/codeanatomy_engine/Cargo.toml:65`).

## C3. Some API snippets are outdated for current engine patterns

Issue:
- v1 semantic validator examples use a DataFrame-centric parsing flow (`dummy_df.parse_sql_expr`).

Correction:
- Use `SessionContext::parse_sql_expr(predicate, schema)` as canonical binding path in validator design.

Why:
- Current code already uses this schema-aware parse surface in pushdown extraction (`rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs:174`).

## C4. Plan should not re-propose already completed hard-cutover foundations

Issue:
- v1 and downstream variants still describe core/bindings split and structured warning migration as pending.

Correction:
- Move these to "already completed baseline" and only retain follow-on hardening tasks.

Why:
- Core crate is Rust-only, bindings are separate, warnings are typed (see evidence above).

## C5. Contract sections should reflect current fields and names

Issue:
- Some snippets use provider identity fields not matching current struct shape (`name` vs `table_name`) and do not account for current plan bundle shape.

Correction:
- Normalize all examples to current structs:
  - `ProviderIdentity { table_name, identity_hash }`
  - `PlanBundleArtifact { delta_codec_logical_bytes, delta_codec_physical_bytes, ... }`

Why:
- This avoids implementation ambiguity and prevents wrong-code copy/paste during execution.

## 4) Improvements to Existing Workstreams (WS1-WS8)

## WS1 Semantic Validator

Improvements:

- Add a validator sub-contract for "binding context reproducibility":
  - capture and assert catalog/schema defaults and identifier normalization settings before validation.
- Add explicit transform legality matrix per `ViewTransform` with deterministic error codes.
- Add DFSchema-qualified ambiguity checks for joins and projections.

Target files:
- `rust/codeanatomy_engine/src/compiler/semantic_validator.rs` (new)
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`
- `rust/codeanatomy_engine/src/compiler/graph_validator.rs`

## WS2 Optimizer Pipeline

Improvements:

- Build an explicit optimizer orchestration API that can run in two modes:
  - compile-time pipeline mode (production)
  - observer/lab mode (diagnostics)
- Persist pass metadata with stable IDs and stage boundaries.
- Use runtime-configured `max_passes` and `skip_failed_rules` directly from effective session config.

Target files:
- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs` (new)
- `rust/codeanatomy_engine/src/stability/optimizer_lab.rs`
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`

## WS3 Planning Surface Governance

Improvements:

- Extend planning manifest identity beyond surface registrations to include:
  - planning-affecting config key snapshot
  - catalog/default schema identity
  - function registry identity digest
- Add explicit governance modes (`StrictAllowlist`, `WarnOnUnregistered`, `Permissive`) for extension registries.

Target files:
- `rust/codeanatomy_engine/src/session/planning_surface.rs`
- `rust/codeanatomy_engine/src/session/planning_manifest.rs`
- `rust/codeanatomy_engine/src/session/factory.rs`

## WS4 Pushdown Contract Enforcement

Improvements:

- Promote pushdown validation from compliance-only probe output to compile-time assertion report.
- Add residual-filter contract checks against optimized logical plan:
  - `Inexact` requires residual filter
  - `Unsupported` requires full residual
  - `Exact` may eliminate residual
- Keep strict and warn modes runtime-configurable.

Target files:
- `rust/codeanatomy_engine/src/providers/pushdown_contract.rs`
- `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs`
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`
- `rust/codeanatomy_engine/src/compliance/capture.rs`

## WS5 Artifact Evolution

Improvements:

- Keep P0/P1/P2 digests but make artifact replay/portability policy explicit:
  - P1 logical as primary deterministic audit anchor
  - P2 treated as environment-sensitive diagnostics
- Add schema version migration policy with strict decode behavior at boundaries.
- Add lineage section that maps scan nodes to provider identities deterministically.

Target files:
- `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`
- `rust/codeanatomy_engine/src/compiler/plan_codec.rs`
- `rust/codeanatomy_engine/src/executor/pipeline.rs`

## WS6 Scheduling Ownership

Improvements:

- Introduce explicit compile metadata output contract for scheduling inputs (view deps, scan deps, output deps), not ad-hoc derivation.
- Ensure scheduling/cost outputs are included in Rust result envelope and exposed through bindings.

Target files:
- `rust/codeanatomy_engine/src/compiler/scheduling.rs` (new)
- `rust/codeanatomy_engine/src/compiler/cost_model.rs` (new)
- `rust/codeanatomy_engine/src/executor/pipeline.rs`
- `rust/codeanatomy_engine/src/executor/result.rs`
- `rust/codeanatomy_engine_py/src/result.rs`

## WS7 Hard Cutover

Improvements:

- Keep existing dual-read strategy, but add strict cutoff checkpoints:
  - parity gate must pass on determinism, artifacts, and warning schema
  - once passed, remove runtime references to Python planning modules in production path
- Preserve Python as boundary/orchestration only.

Target files:
- `src/engine/build_orchestrator.py`
- `src/datafusion_engine/session/factory.py`
- `src/relspec/execution_plan.py` (retire)
- `src/datafusion_engine/plan/*` (retire)

## WS8 Breaking Contract Package

Improvements:

- Explicitly enforce unknown-field rejection at Rust boundary for all cross-language payloads.
- Add migration error taxonomy for version mismatches and legacy payload rejection.

Target files:
- `rust/codeanatomy_engine/src/spec/execution_spec.rs`
- `rust/codeanatomy_engine_py/src/compiler.rs`
- `src/engine/spec_builder.py`

## 5) Additional Net-New Scope Needed for Best-in-Class End State

These scopes are not fully covered in v1 but are necessary to complete the architecture at a truly best-in-class level.

## WS9 Planner Environment Identity and Governance Control Plane

Objective:
- Make planning environment identity first-class and hash-stable.

Scope:
- Capture deterministic snapshots of:
  - catalog providers
  - default catalog/schema
  - table factory and file format factory identities
  - planning-affecting `SessionConfig` keys
- Add SQL mutation governance using explicit allowed SQL options during planning-only phases.

Why:
- `dfdl_ref` emphasizes `SessionContext` as the full planning environment boundary, not just rule lists.

## WS10 Standalone Optimizer API Surface

Objective:
- Provide a dedicated optimizer service surface independent of full materialization pipeline.

Scope:
- Add a "compile-only optimizer" API returning:
  - optimized logical plan digest/text
  - per-rule pass trace
  - warning/error report
- Enable offline rulepack experiments without invoking full materialization.

Why:
- This closes the gap between lab tooling and production planner behavior.

## WS11 Statistics-Quality and Cost-Reliability Governance

Objective:
- Prevent cost model drift when statistics quality is poor.

Scope:
- Add quality scoring for planner statistics (exact/estimated/unknown)
- Gate high-sensitivity cost decisions (join strategy/scheduling priority) when stats quality is below threshold
- Surface stats-quality diagnostics in artifacts and warnings

Why:
- Best-in-class optimization must be correctness-first under uncertain stats.

## WS12 Delta Protocol and Column-Mapping Compatibility Contract

Objective:
- Make Delta compatibility checks explicit and deterministic in planning artifacts.

Scope:
- Capture and validate protocol features and table metadata relevant to planning.
- Add explicit checks around schema evolution and column mapping semantics for join/union compatibility.

Why:
- Delta protocol and schema mapping drift are common long-term failure modes in mixed-version environments.

## WS13 Portable Plan Policy (Substrait as Primary Portable Artifact)

Objective:
- Separate portable and non-portable artifacts clearly.

Scope:
- Treat Substrait artifact as primary portable logical representation when enabled.
- Treat physical plan artifacts as non-portable diagnostics only.
- Define fingerprint envelope policy for portability artifacts.

Why:
- Matches dfdl_ref guidance: physical plans are environment-sensitive; portable interchange belongs at logical layer.

## WS14 Custom Source Readiness (FileSource/TableProvider Contract Hardening)

Objective:
- Make future custom storage/scanning extensions optimizer-compatible by design.

Scope:
- Add contract tests for projection/filter pushdown and statistics propagation at provider/source boundaries.
- Ensure future custom sources expose required capabilities without degrading optimizer effectiveness.

Why:
- This prevents future regressions when introducing specialized CPG-native sources.

## 6) Revised Priority and Delivery Order

Phase A: Rebaseline and de-duplicate plan scope
- Apply C1-C5 corrections.
- Mark already completed scope as baseline.

Phase B: Complete core compiler authority
- Execute WS1, WS2, WS3, WS4, WS5 in parallel where feasible.
- Land WS6 once compile metadata contract is stable.

Phase C: Cutover and contract hardening
- Execute WS7 and WS8 with dual-read parity gate.

Phase D: Best-in-class hardening (net-new)
- Execute WS9, WS10, WS11, WS12, WS13, WS14.

## 7) Updated Definition of Done

The architecture is only complete when all conditions hold:

- One authoritative Rust planning and optimization path drives production.
- Planning identity includes full environment surface, not only spec/rule hashes.
- Pushdown correctness is enforced as planner contract, not only observed in compliance.
- Optimizer behavior is observable with stable pass traces and strict failure policy controls.
- Artifact contract separates deterministic logical identity from environment-sensitive physical diagnostics.
- Python planning duplication is removed from production path.
- Delta compatibility and portability contracts are explicit and tested.

## 8) Immediate Actionable Changes to v1 Plan Document

1. Update all DataFusion version references to 51.0.0 and remove 50.x pin guidance.
2. Remove "add datafusion-tracing dependency" and replace with instrumentation policy work.
3. Add an "already completed baseline" section listing completed hard-cutover foundations.
4. Add WS9-WS14 sections from this review.
5. Regenerate workstream dependency graph with new scopes.
6. Update test matrix to include:
- environment identity hash determinism
- portable artifact policy checks
- stats-quality gating behavior
- Delta protocol/column-mapping compatibility checks
