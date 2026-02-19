# Plan Review and Expansion: Rust Crates Cleanup + DataFusion-Native Planning Alignment

**Topic:** rust-crates-cleanup-plan-review-and-df52-alignment  
**Version:** v1  
**Date:** 2026-02-19  
**Author:** Codex (GPT-5)

## Scope and Inputs

This review covers:

1. Plan quality review for `docs/plans/rust_crates_cleanup_implementation_plan_v1_2026-02-18.md`.
2. Additional scope to maximize use of DataFusion integrated planning/runtime surfaces from:
   - `docs/python_library_reference/datafusion_52_vs_51_changes.md`
   - `docs/python_library_reference/datafusion_plan_combination.md`
   - `docs/python_library_reference/datafusion_planning.md`
3. Alignment recommendations against `docs/python_library_reference/design_principles.md`.

Code evidence was validated across both `rust/` and `src/` to satisfy cross-repo alignment, including:

- `rust/codeanatomy_engine/`
- `rust/datafusion_ext/`
- `rust/datafusion_python/`
- `rust/df_plugin_api/`
- `rust/df_plugin_codeanatomy/`
- `src/datafusion_engine/`

## Environment Setup Completed

Executed successfully:

1. `bash scripts/bootstrap_codex.sh`
2. `uv sync`

## Executive Conclusions

1. The existing Rust cleanup plan is strong on local deduplication and contract hygiene inside `rust/`, and most baseline findings are accurate.
2. The current plan does not satisfy full architecture alignment because it explicitly excludes `src/` (`docs/plans/rust_crates_cleanup_implementation_plan_v1_2026-02-18.md:32`), while the target design principles and DataFusion planning model are cross-layer.
3. Four material corrections are needed in the current S-items:
   - S8 implementation direction is under-specified and likely unsafe.
   - S9 logging snippet uses tracing-style field syntax under `log::info!`.
   - S14 should not remove relation planner capability; it should implement it end-to-end.
   - ABI policy (S15) should expand to include planner-extension ABI evolution, not just compatibility checks.
4. The highest-value additional scope is to converge both Rust and Python layers on one DataFusion-native planning surface: Session/State builder policy, planner hooks (Expr/Relation/Type), provider pushdown contracts, and session-aware FFI codec/task-context integration.

## Review of Existing Plan (Errors, Risks, Improvements)

### Strengths in the Current Plan

The following are correct and high value:

1. S1 (silent `extra_constraints` drop) is real and critical.  
Evidence: `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:183`, `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:214`.
2. S2/S3 dedup opportunities are real.  
Evidence: `rust/codeanatomy_engine/src/compiler/compile_phases.rs:131`, `rust/codeanatomy_engine/src/executor/pipeline.rs:389`, and task graph construction in both files.
3. S5/S6/S7 capture concrete duplication and lifecycle issues in `datafusion_python` bridge utilities.
4. S11 idempotency concern is valid for rule registration.  
Evidence: `rust/datafusion_ext/src/planner_rules.rs:100`, `rust/datafusion_ext/src/physical_rules.rs:106`.
5. S12 duplicate determinism contract types are real and drift-prone.  
Evidence: `rust/codeanatomy_engine/src/spec/hashing.rs:69`, `rust/codeanatomy_engine/src/executor/result.rs:82`.
6. S19 error swallowing in UDF bundle bootstrap is real.  
Evidence: `rust/df_plugin_codeanatomy/src/udf_bundle.rs:114`.

### Required Corrections to Current Plan

#### C1. Scope boundary is misaligned with requested system goal

- Current plan marks `src/` out of scope (`docs/plans/rust_crates_cleanup_implementation_plan_v1_2026-02-18.md:32`).
- The requested objective is codebase-wide alignment across `src/` and `rust/`.
- DataFusion planning surfaces in the provided references are explicitly environment-level constructs, not Rust-only constructs:
  - `SessionContext` as planning environment (`docs/python_library_reference/datafusion_planning.md:211`)
  - builder-driven planning surfaces (`docs/python_library_reference/datafusion_plan_combination.md:1634`)
  - extensibility hooks (`docs/python_library_reference/datafusion_planning.md:2933`)

**Correction:** Add a companion `src/datafusion_engine` workstream in the same execution program, not a separate deferred effort.

#### C2. S8 approach needs redesign (do not spawn nested bare contexts)

- The plan correctly flags `SessionContext::new()` usage:
  - `rust/codeanatomy_engine/src/providers/interval_align_provider.rs:108`
  - `rust/codeanatomy_engine/src/providers/interval_align_provider.rs:564`
- But passing `state: &dyn Session` into the current SQL-string `build_dataframe` path is not enough by itself. The current method relies on an internal temporary context and SQL planning side effects.

**Correction:** Replace nested-context SQL assembly with one of:

1. Logical plan assembly via `LogicalPlanBuilder` and provider inputs, then `state.create_physical_plan(...)`.
2. A dedicated execution plan node when semantics exceed normal relational lowering.

Keep `SessionContext::new()` out of provider execution paths entirely.

#### C3. S9 snippet syntax mismatch for `log` crate

- Proposed snippet uses `log::info!(field = %value, "...")` style (`docs/plans/rust_crates_cleanup_implementation_plan_v1_2026-02-18.md:761`), which is tracing-style field syntax.

**Correction:** Either:

1. Use `tracing` structured logs consistently where field syntax is required, or
2. Use `log` with standard formatting arguments and stable key-value patterns.

#### C4. S14 should be replaced, not applied as written

- Existing S14 says remove phantom `RELATION_PLANNER`.
- Evidence confirms capability is currently phantom:
  - declared in `rust/df_plugin_api/src/manifest.rs:13`
  - advertised in `rust/df_plugin_codeanatomy/src/lib.rs:38`
  - no corresponding export in `rust/df_plugin_api/src/lib.rs:36`
- But DataFusion 52 explicitly elevates RelationPlanner as a first-class extension hook (`docs/python_library_reference/datafusion_52_vs_51_changes.md:1991`, `docs/python_library_reference/datafusion_52_vs_51_changes.md:1995`).

**Correction:** Replace S14 with "Implement real relation planner support end-to-end, then keep capability bit truthful."

## Additional Scope: DataFusion-Native Expansion (Cross `src/` + `rust/`)

The following items are additional scope beyond S1-S20 and provide the largest reduction in bespoke architecture.

### X1. Implement DF52 RelationPlanner end-to-end (replace S14)

**Why (DataFusion docs):**

- RelationPlanner is explicit DF52 SQL planning extension surface (`docs/python_library_reference/datafusion_52_vs_51_changes.md:1995`).
- It enables custom FROM-clause planning while preserving DataFusion plan pipeline (`docs/python_library_reference/datafusion_52_vs_51_changes.md:1954`).

**Current repo state:**

- Python protocol exists but is effectively placeholder:
  - `src/datafusion_engine/session/protocols.py:48`
  - `src/datafusion_engine/expr/relation_planner.py:17`
- Runtime path only calls relation planner when a custom hook object is provided:
  - `src/datafusion_engine/session/runtime_extensions.py:979`
- Plugin capability advertises relation planner without export surface:
  - `rust/df_plugin_api/src/manifest.rs:13`
  - `rust/df_plugin_codeanatomy/src/lib.rs:38`
  - missing export in `rust/df_plugin_api/src/lib.rs:36`

**Implementation target:**

1. Add relation-planner registration path in `rust/datafusion_ext`.
2. Add builder/planning surface slot in `rust/codeanatomy_engine/src/session/planning_surface.rs`.
3. Add plugin ABI export field(s) for relation planner factories in `rust/df_plugin_api/src/lib.rs`.
4. Wire Python hook to native registration in `src/datafusion_engine` runtime installation flow.
5. Keep capability bit only when export is present and tested.

**Acceptance criteria:**

1. Custom FROM relation is planned through registered planner.
2. Capability snapshot truthfully reports relation planner support.
3. No placeholder/no-op relation planner remains as the default path.

### X2. Add TypePlanner support to planning surface

**Why (DataFusion docs):**

- DF52 planning extensibility includes ExprPlanner, RelationPlanner, and TypePlanner as first-class peers (`docs/python_library_reference/datafusion_52_vs_51_changes.md:1987`).

**Current repo state:**

- Planning surface currently includes expr planners but no type planner slot:
  - `rust/codeanatomy_engine/src/session/planning_surface.rs:45`

**Implementation target:**

1. Add `type_planner` field(s) to `PlanningSurfaceSpec`.
2. Register through SessionStateBuilder path.
3. Add profile policy fields in `src/datafusion_engine/session/runtime_profile_config.py`.
4. Include in planning manifest/fingerprint capture.

**Acceptance criteria:**

1. Type planner can be injected through policy + runtime path.
2. Planning hash changes when type planner changes.

### X3. Migrate FFI provider contract to session-aware codec/context model and remove global fallback context

**Why (DataFusion docs):**

- DF52 FFI changed provider constructors to require `TaskContextProvider` and optional logical codec (`docs/python_library_reference/datafusion_52_vs_51_changes.md:3407`).
- Python capsule methods now require session for codec extraction (`docs/python_library_reference/datafusion_52_vs_51_changes.md:3495`).

**Current repo state:**

- Global task context provider still used:
  - `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:134`
  - `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:144`
- Context adaptation layer carries complex fallback logic:
  - `src/datafusion_engine/extensions/context_adaptation.py:83`
  - `src/datafusion_engine/extensions/datafusion_ext.py:84`
- Schema evolution/provider capsule still uses global provider pattern:
  - `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs:365`

**Implementation target:**

1. Make all provider capsule creation paths session-bound and codec-aware.
2. Remove global task-context fallback creation from provider exports.
3. Collapse adaptation wrappers once all native entrypoints accept one canonical context type.
4. Treat `session_context_contract_probe` as migration gate, then retire it after convergence.

**Acceptance criteria:**

1. No provider capsule constructor uses global singleton task context.
2. Extension entrypoints no longer require outer/internal/fallback probing for normal operation.

### X4. Replace stringly runtime settings bridge with typed builder policy where possible

**Why (DataFusion docs):**

- Session/State builder is the canonical planning environment constructor (`docs/python_library_reference/datafusion_plan_combination.md:1626`).
- Table options and file format factories are planning control plane, not ad-hoc key maps (`docs/python_library_reference/datafusion_plan_combination.md:1535`).

**Current repo state:**

- Significant runtime settings use string-key transforms:
  - `src/datafusion_engine/session/runtime_config_policies.py:190`
  - `src/datafusion_engine/session/delta_session_builder.py:322`
  - `src/datafusion_engine/session/context_pool.py:355`
- Rust side already has typed planning surface primitives:
  - `rust/codeanatomy_engine/src/session/planning_surface.rs:33`
  - `rust/codeanatomy_engine/src/session/factory.rs:117`

**Implementation target:**

1. Move shared planning-affecting options into typed contracts exchanged between `src` and `rust`.
2. Keep string-key overrides only at external boundary.
3. Ensure one canonical mapping from runtime profile policy to builder options.

**Acceptance criteria:**

1. Policy-to-builder mapping is single-source and test-covered.
2. Planning fingerprint contains typed policy payload, not only raw key/value strings.

### X5. Pushdown-first provider architecture for custom sources (starting with IntervalAlignProvider)

**Why (DataFusion docs):**

- Scan pushdown is a first-class DF52 architecture area (`docs/python_library_reference/datafusion_52_vs_51_changes.md:1239`).
- TableProvider pushdown correctness contracts are explicit (`docs/python_library_reference/datafusion_plan_combination.md:2512`).
- `scan_with_args` evolution path is called out in planning docs (`docs/python_library_reference/datafusion_planning.md:3117`).

**Current repo state:**

- IntervalAlignProvider currently builds internal SQL and reports all filter pushdown as `Inexact`:
  - `rust/codeanatomy_engine/src/providers/interval_align_provider.rs:102`
  - `rust/codeanatomy_engine/src/providers/interval_align_provider.rs:177`

**Implementation target:**

1. Refactor IntervalAlignProvider to direct logical/physical plan assembly under caller session state.
2. Improve pushdown classification per predicate where exact semantics are known.
3. Evaluate adopting `scan_with_args` where ordering/advanced scan parameters matter.

**Acceptance criteria:**

1. No nested bare SessionContext in provider scan/execute path.
2. EXPLAIN plans show expected projection/filter/limit pushdown behavior.

### X6. Expand DML integration to leverage DF52 TableProvider mutation contracts

**Why (DataFusion docs):**

- DF52 adds `delete_from`/`update` provider hooks and DML contract semantics (`docs/python_library_reference/datafusion_52_vs_51_changes.md:1686`).

**Current repo state:**

- Bridge currently validates/executes bespoke mutation payloads in `datafusion_python`.
- `extra_constraints` bug highlights contract drift risk:
  - `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:183`

**Implementation target:**

1. Route mutation semantics through DataFusion DML contract where feasible.
2. Keep bespoke request schema only as transport envelope, not semantic engine.
3. Return consistent count-contract behavior for delete/update.

**Acceptance criteria:**

1. Mutation behavior matches DF52 DML count/result contract.
2. Unsupported constraint cases fail explicitly at boundary parse.

### X7. Consolidate cache control plane on native CacheManagerConfig + introspection APIs

**Why (DataFusion docs):**

- DF52 expands cache manager controls and cache introspection (`docs/python_library_reference/datafusion_52_vs_51_changes.md:843`).

**Current repo state:**

- Cache data and telemetry are already extensive across `src` and `rust`.
- Settings and snapshots are split across multiple policy layers.

**Implementation target:**

1. Use one typed cache policy artifact that compiles into CacheManagerConfig/runtime builder.
2. Standardize metrics and SQL/UDTF cache introspection payloads across layers.
3. Remove duplicated cache-limit parsing logic where possible.

### X8. Adopt standalone optimizer harness as canonical compile-only diagnostics engine

**Why (DataFusion docs):**

- Standalone optimizer APIs support rulepack experimentation and observer hooks (`docs/python_library_reference/datafusion_plan_combination.md:2286`).

**Current repo state:**

- Repo already collects optimizer diagnostics but through multiple pathways.

**Implementation target:**

1. Add a unified offline optimizer harness in Rust for deterministic plan-diff traces.
2. Reuse this harness in compliance/diagnostics flows rather than ad-hoc duplicates.
3. Capture rule-by-rule snapshots for regression goldens.

### X9. Tighten plan serialization contract: Substrait primary for cross-process, DataFusion proto for same-version internal use

**Why (DataFusion docs):**

- DataFusion proto is not version-stable (`docs/python_library_reference/datafusion_planning.md:2691`).
- Substrait is the interoperability layer (`docs/python_library_reference/datafusion_planning.md:2826`).

**Current repo state:**

- Codebase has rich plan capture/serialization surfaces in both `src` and `rust`.

**Implementation target:**

1. Formalize artifact classes and intended lifetimes.
2. Make cross-process interchange use Substrait by default.
3. Retain DataFusion proto as same-version/local optimization artifact only.

### X10. Extend planning-surface manifest parity across `src` and `rust`

**Why (Design principles):**

- Information hiding, DRY, and explicit contracts require one authoritative planning surface model:
  - `docs/python_library_reference/design_principles.md:4`
  - `docs/python_library_reference/design_principles.md:24`
  - `docs/python_library_reference/design_principles.md:75`

**Current repo state:**

- Rust has typed `PlanningSurfaceSpec`.
- Python side has rich policies but broader string-based adaptation and fallback logic.

**Implementation target:**

1. Define a versioned planning-surface contract shared between `src` and `rust`.
2. Include planners, factories, options, and runtime-affecting knobs in one manifest.
3. Use this manifest as the core determinism/fingerprint unit.

## Proposed Revision to S-Items (What to Keep/Modify/Replace)

| Existing Item | Action | Rationale |
|---|---|---|
| S1 | Keep | Correctness-critical, validated. |
| S2 | Keep | Dedup + contract consistency. |
| S3 | Keep | Dedup + maintainability. |
| S4 | Keep | Deterministic ordering consistency. |
| S5 | Keep | Runtime lifecycle fix. |
| S6 | Keep | DRY for session extraction contract. |
| S7 | Keep | DRY for schema decode boundary. |
| S8 | Modify | Rework to eliminate nested SessionContext SQL path, not just parameter threading. |
| S9 | Modify | Fix logging macro usage and choose one structured logging contract. |
| S10 | Keep | Tracing is useful once logging contract is clarified. |
| S11 | Keep | Idempotency is required. |
| S12 | Keep | Eliminate duplicate determinism type. |
| S13 | Keep | Naming cleanup low risk. |
| S14 | Replace with X1 | Implement relation planner, do not remove capability surface. |
| S15 | Expand | Include planner-extension ABI evolution policy. |
| S16 | Keep | Test coverage for pure binding functions. |
| S17 | Keep with S8 coupling | Useful only if S8 architecture is corrected. |
| S18 | Keep | Reduces constructor coupling. |
| S19 | Keep | Fail-fast over silent empty bundle. |
| S20 | Keep | Structural split supports SRP and testability. |

## Revised Delivery Sequence

### Phase 0: Correctness and contract guards

1. S1, S11, S12, S19.
2. Add S9 syntax correction as part of observability baseline.

### Phase 1: Dedup and runtime hygiene

1. S2, S3, S5, S6, S7, S20.
2. Ensure no behavior drift with targeted tests.

### Phase 2: Provider/pushdown architecture

1. S8 (reworked), S17.
2. X5 pushdown semantics and provider contract parity.

### Phase 3: Planner extension modernization

1. X1 relation planner end-to-end (replaces S14).
2. X2 type planner support.
3. S15 ABI policy expansion.

### Phase 4: Cross-layer `src` + `rust` convergence

1. X3 session-aware FFI/context cleanup.
2. X4 typed planning policy unification.
3. X10 manifest parity and deterministic fingerprinting.

### Phase 5: System-level optimization and artifact policy

1. X6 DML alignment.
2. X7 cache control-plane consolidation.
3. X8 standalone optimizer harness.
4. X9 serialization policy hardening.

## Definition of Done (Updated)

1. No critical planning/runtime behavior is split between parallel bespoke abstractions across `src` and `rust` without an explicit contract boundary.
2. Planner extension capabilities reported by plugins are truthful and executable.
3. Provider scan paths avoid nested ad-hoc contexts and respect DataFusion pushdown contracts.
4. Cross-process plan artifact policy is explicit (Substrait primary).
5. Determinism contract is single-source and versioned.
6. All affected layers pass repository quality gates and targeted regression tests.

## High-Priority File Targets for Follow-on Implementation

### Rust

- `rust/codeanatomy_engine/src/session/planning_surface.rs`
- `rust/codeanatomy_engine/src/session/factory.rs`
- `rust/codeanatomy_engine/src/providers/interval_align_provider.rs`
- `rust/datafusion_ext/src/lib.rs`
- `rust/datafusion_ext/src/planner_rules.rs`
- `rust/datafusion_ext/src/physical_rules.rs`
- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs`
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs`
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs`
- `rust/df_plugin_api/src/lib.rs`
- `rust/df_plugin_api/src/manifest.rs`
- `rust/df_plugin_codeanatomy/src/lib.rs`

### Python (`src/`)

- `src/datafusion_engine/session/runtime_extensions.py`
- `src/datafusion_engine/session/protocols.py`
- `src/datafusion_engine/expr/relation_planner.py`
- `src/datafusion_engine/expr/planner.py`
- `src/datafusion_engine/extensions/context_adaptation.py`
- `src/datafusion_engine/extensions/datafusion_ext.py`
- `src/datafusion_engine/session/runtime_profile_config.py`
- `src/datafusion_engine/session/runtime_config_policies.py`
- `src/datafusion_engine/session/delta_session_builder.py`
- `src/datafusion_engine/session/context_pool.py`

## Final Recommendation

Proceed with S1-S20 only after incorporating the corrections above, and explicitly expand program scope to include `src/datafusion_engine` convergence. Without the cross-layer workstreams (X1-X10), the cleanup will reduce local Rust debt but will not maximize DataFusion-native planning integration or fully satisfy design-principle alignment.

