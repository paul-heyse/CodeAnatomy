# Semantic Orchestration + DataFusion Planning Convergence
## Integrated Implementation Plan v2

**Date:** 2026-02-19
**Status:** Approved for implementation
**Supersedes:** This document's prior review-oriented v1 draft
**Primary predecessor plan:** `docs/plans/semantic_orchestration_cleanup_implementation_plan_v1_2026-02-18.md`
**Reference material:**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md`
- `docs/python_library_reference/datafusion_plan_combination.md`
- `docs/python_library_reference/datafusion_planning.md`
- `docs/python_library_reference/design_principles.md`

---

## Scope Summary

This integrated plan keeps the existing semantic/relspec/orchestrator cleanup work (S1-S14) and fully incorporates the approved DataFusion-native expansion scope (DX1-DX12).

Unlike the predecessor plan, this plan explicitly includes:
- `src/semantics/`
- `src/relspec/`
- `src/graph/`
- `src/planning_engine/`
- `src/datafusion_engine/`
- `rust/`

Target outcome:
- Reduce bespoke planning/fingerprinting/orchestration logic.
- Converge on DataFusion-native planning artifacts, environment construction, and extension points.
- Improve determinism, contract clarity, and cross-language consistency.

---

## Design Principles

1. **DataFusion-native first**
   - Prefer DataFusion planning surfaces over bespoke orchestration logic when equivalent capability exists.
2. **Single source of truth for planning identity**
   - One canonical plan fingerprint and plan identity pathway.
3. **No compatibility shims for internal renames**
   - Hard cutovers for S4/S5/S13 renames.
4. **Determinism by construction**
   - Fingerprints and package hashes are stable and environment-scoped only via explicit hashed inputs.
5. **Cross-language contract parity**
   - Python and Rust planning policies/settings must be hash-comparable.
6. **Post-build full gate**
   - Run full repository quality gate after implementation completion (not per report-only document changes).

---

## Baseline Evidence Snapshot

The following baseline findings are treated as implementation drivers:

- Duplicate strategy confidence values:
  - `src/semantics/ir_pipeline.py:1088`
  - `src/semantics/joins/inference.py:49`
- Remaining `RelationshipSpec` migration debt:
  - `src/semantics/pipeline_build.py:48`
  - `src/semantics/pipeline_builders.py:13`
  - `src/semantics/pipeline_builders.py:132`
- Plan fingerprint fallback collision risk:
  - `src/semantics/plans/fingerprints.py:223`
- CQS-violating resolver naming:
  - `src/semantics/table_registry.py:42`
  - `src/semantics/compiler.py:615`
- Timestamp already excluded from composite payload (clarification needed, not behavior fix):
  - `src/relspec/execution_package.py:146`
  - `src/relspec/execution_package.py:217`
- Duplicate scan-setting application logic:
  - `src/datafusion_engine/dataset/registration_validation.py:26`
  - `src/datafusion_engine/tables/registration.py:178`
- Canonical DataFusion plan-fingerprint + identity already exists:
  - `src/datafusion_engine/plan/plan_fingerprint.py:58`
  - `src/datafusion_engine/plan/plan_identity.py:100`
  - `src/datafusion_engine/plan/bundle_assembly.py:565`
- Semantics fingerprint path still bespoke and uses private substrait internals:
  - `src/semantics/plans/fingerprints.py:184`
  - `src/semantics/plans/fingerprints.py:276`
- Rule installer idempotency gaps:
  - `rust/datafusion_ext/src/planner_rules.rs:100`
  - `rust/datafusion_ext/src/physical_rules.rs:106`

---

## Workstream A: Integrate Approved Corrections into S1-S14

## A1. Scope Correction

**Decision:** predecessor scope exclusion of `src/datafusion_engine/` and `rust/` is removed.

**Action:** all sequencing and checklists in this plan include DataFusion Python and Rust work where applicable.

## A2. Risk Reclassification

- `S4` is **medium risk** (global rename + command/query semantics at compiler boundary).
- `S5` is **medium risk** (public surface promotion + cross-module import changes).

All other S-items keep original risk labels unless impacted by DX dependencies.

## A3. S7 Reframing

**Decision:** S7 is a clarification/additive determinism item, not a primary behavior fix.

**Action:**
- Add explicit provenance-only comment for `created_at_unix_ms`.
- Add optional `created_at_unix_ms` override for deterministic replay use-cases.

## A4. S2 Verification Hardening

**Decision:** narrow verification in predecessor plan is expanded.

**Required verification scope before RelationshipSpec decommission steps:**
- `src/`
- `tests/`
- `tools/`

## A5. S3 Positioning

**Decision:** retain immediate fallback collision fix, but treat it as interim.

**Action:**
- Immediate: implement `unknown_plan:{view_name}` guardrail.
- Follow-on (DX1): migrate semantics consumers to shared DataFusion plan fingerprint/identity.

## A6. S1 Import Placement

**Decision:** keep canonical constant imports in module import blocks, not inline constant region.

## A7. Quality Gate Cadence Alignment

**Decision:**
- No full gate for report-only document edits.
- Full gate required after code implementation completion:
  - `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q`

---

## Workstream B: DataFusion-Native Expansion (DX1-DX12)

## DX1. Unify Plan Fingerprinting + Identity

**Goal**
- Remove parallel semantics-local fingerprint logic as authoritative runtime identity.

**Implementation**
1. Introduce a semantics adapter that consumes DataFusion plan bundle identity artifacts.
2. Transition cache-key identity to `plan_identity_hash` where available.
3. Mark semantics-local fingerprint pathway as deprecated for runtime execution identity.

**Primary files**
- `src/semantics/plans/fingerprints.py`
- `src/semantics/plans/__init__.py`
- `src/datafusion_engine/plan/plan_fingerprint.py`
- `src/datafusion_engine/plan/plan_identity.py`
- `src/datafusion_engine/plan/bundle_assembly.py`

**Success criteria**
- No active runtime path depends on semantics-only fallback hash identity.

## DX2. Remove Private Substrait Internal Access

**Goal**
- Eliminate `datafusion._internal` dependency from semantics plan fingerprinting.

**Implementation**
1. Route Substrait production through bundle assembly/runtime-backed helpers.
2. Keep one serialization pathway per execution mode.

**Primary files**
- `src/semantics/plans/fingerprints.py`
- `src/datafusion_engine/plan/bundle_assembly.py`
- `src/datafusion_engine/session/facade.py`

**Success criteria**
- No `datafusion._internal` references remain in semantic planning/fingerprint code.

## DX3. Promote Planning-Environment Hashes to relspec Inputs

**Goal**
- Make planning environment state explicit in relspec execution package/policy lineage.

**Implementation**
1. Extend relspec payloads to include `planning_env_hash`, `rulepack_hash`, `information_schema_hash`, and function registry hash.
2. Ensure these hashes appear in diagnostics and replay artifacts.

**Primary files**
- `src/relspec/execution_package.py`
- `src/relspec/compiled_policy.py`
- `src/relspec/policy_compiler.py`
- `src/datafusion_engine/plan/bundle_assembly.py`

**Success criteria**
- relspec package artifacts include planning-environment identity fields.

## DX4. Deduplicate Scan-Setting Application

**Goal**
- Single canonical scan-setting application pathway.

**Implementation**
1. Extract shared `apply_scan_settings` + `set_runtime_setting` helper.
2. Replace duplicate implementations in registration modules.
3. Preserve current behavior and SQL options handling.

**Primary files**
- `src/datafusion_engine/dataset/registration_validation.py`
- `src/datafusion_engine/tables/registration.py`
- `src/datafusion_engine/session/` (new shared helper module)

**Success criteria**
- Duplicate logic removed; both call sites consume shared helper.

## DX5. Elevate Cache Diagnostics into Planning Control Loop

**Goal**
- Treat cache state as first-class planning/diagnostics input, not passive metadata.

**Implementation**
1. Propagate cache diagnostic snapshots into plan execution artifacts.
2. Add optional relspec policy calibration input surface for cache signals.
3. Include stale-cache diagnostics for mutable listing contexts.

**Primary files**
- `src/datafusion_engine/catalog/introspection.py`
- `src/datafusion_engine/plan/bundle_assembly.py`
- `src/relspec/policy_calibrator.py`

**Success criteria**
- Cache diagnostics present in execution artifacts and visible for policy analysis.

## DX6. Python/Rust Planning Policy Parity Contract

**Goal**
- One policy contract that yields equivalent effective planning settings in both languages.

**Implementation**
1. Define shared planning-policy schema (including feature gates).
2. Generate/apply settings in Python runtime profiles and Rust session factory from the same contract.
3. Add parity check that compares effective settings hash.

**Primary files**
- `src/datafusion_engine/session/runtime_config_policies.py`
- `src/datafusion_engine/session/runtime.py`
- `rust/codeanatomy_engine/src/session/factory.rs`
- `rust/codeanatomy_engine/src/session/planning_surface.rs`

**Success criteria**
- Same named profile yields hash-equivalent settings snapshots across Python and Rust.

## DX7. PlanningSurfaceSpec as Mandatory Builder Path

**Goal**
- Make builder-first planning-surface composition the default for new planning features.

**Implementation**
1. Prohibit ad hoc post-build planner mutation except where no builder API exists.
2. Route new file format/table option/planner/factory registrations through `PlanningSurfaceSpec`.

**Primary files**
- `rust/codeanatomy_engine/src/session/planning_surface.rs`
- `rust/codeanatomy_engine/src/session/factory.rs`

**Success criteria**
- New planning-surface features are wired via builder path.

## DX8. RelationPlanner Adoption Track

**Goal**
- Reduce bespoke FROM-clause orchestration through planner-native extension points.

**Implementation**
1. Identify one bounded semantic SQL construct for pilot conversion.
2. Implement relation-planner extension with explain-based equivalence validation.

**Primary files**
- `rust/datafusion_ext/src/` (new or extended relation-planner module)
- `rust/codeanatomy_engine/src/session/` wiring

**Success criteria**
- At least one domain-specific FROM pattern is planned via extension point instead of bespoke pre-rewrite.

## DX9. Plan-Combination Discipline

**Goal**
- Build one logical DAG for multi-step semantic transformations where feasible.

**Implementation**
1. Identify unnecessary materialization boundaries.
2. Replace with DataFrame/SQL DAG composition (`join`, `union`, CTE/view composition).
3. Validate with `(unoptimized, optimized, physical)` plan artifacts.

**Primary files**
- `src/semantics/`
- `src/datafusion_engine/session/facade.py`
- `src/datafusion_engine/plan/`

**Success criteria**
- Reduced materialization boundaries with preserved outputs and improved plan coherence.

## DX10. Idempotency Guards for Rule Installers

**Goal**
- Ensure repeated registration calls do not append duplicate rules.

**Implementation**
1. Guard analyzer/physical rule install by rule name.
2. Emit installed rule-set diagnostics.

**Primary files**
- `rust/datafusion_ext/src/planner_rules.rs`
- `rust/datafusion_ext/src/physical_rules.rs`
- `rust/codeanatomy_engine/src/session/planning_manifest.rs`

**Success criteria**
- Repeated installer invocation is idempotent.

## DX11. Overlay Sessions as Standard for Rule Experiments

**Goal**
- Standardize rulepack experimentation using overlay sessions, not mutable base state.

**Implementation**
1. Route policy experiments through `new_from_existing` overlay path.
2. Persist explain-based per-rule deltas for comparison.

**Primary files**
- `rust/codeanatomy_engine/src/rules/overlay.rs`
- `rust/codeanatomy_engine/src/session/` integration points

**Success criteria**
- Experiment workflows avoid in-place mutation of baseline session state.

## DX12. Optional: TableProvider-Native DML Migration Evaluation

**Goal**
- Reduce bespoke delta mutation pathways where provider-native DML semantics are available.

**Implementation**
1. Evaluate support matrix for provider-native `DELETE/UPDATE` semantics.
2. Define migration envelope and fallback requirements.
3. Pilot on one low-risk mutation flow.

**Primary files**
- `src/datafusion_engine/delta/`
- `rust/datafusion_ext/src/delta_*`
- `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs`

**Success criteria**
- Documented go/no-go decision with pilot result and contract guarantees.

---

## Dependencies

- `DX1` depends on `S3` guardrail completion.
- `DX2` should run with or immediately after `DX1`.
- `DX3` depends on stable bundle environment artifact surface (`DX1`, `DX2`).
- `DX4` can run in parallel with `DX1/DX2`.
- `DX5` depends on `DX4` for clean settings/control-plane inputs.
- `DX6` should begin early and continue iteratively across phases.
- `DX7` is foundational for `DX8` and future planner extensions.
- `DX10` should be completed before broad overlay experimentation.
- `DX11` depends on `DX10` idempotent installer behavior.
- `DX12` is optional and last.

---

## Phased Execution Sequence

## Phase 0: Plan Corrections + Baseline Hardening

- Apply A1-A7 decisions in implementation execution notes.
- Keep predecessor S1-S14 tasks, but with revised risk and verification rules.

## Phase 1: Immediate Determinism + Contract Guardrails

- `S1`, `S3`, `S9`, `S14`
- `DX10`

## Phase 2: Fingerprint/Substrait Convergence

- `DX1`
- `DX2`
- Interim compatibility handling for semantics callers.

## Phase 3: Settings and Diagnostics Convergence

- `DX4`
- `DX5`
- `DX6` (first parity pass)

## Phase 4: relspec + Orchestration Integration

- `S7` (clarification/additive parameter)
- `S10`, `S12`
- `DX3`

## Phase 5: Planner Surface and DAG Composition

- `S2`, `S4`, `S5`, `S6`, `S8`, `S11`, `S13`
- `DX7`
- `DX9`
- `DX8` pilot

## Phase 6: Optional DML Migration

- `DX12` decision and pilot

## Phase 7: Stabilization

- `DX6` final parity gate
- Decommission old paths
- Final regression and replay validation

---

## Integrated Implementation Checklist

### Track A (S1-S14 with approved corrections)

- [ ] S1: Introduce canonical confidence map and remove duplicate literals.
- [ ] S1: Keep import placement in import section.
- [ ] S2: Remove RelationshipSpec imports/usages from active semantics build paths.
- [ ] S2: Run expanded reference audit across `src/`, `tests/`, `tools/` before final decommission.
- [ ] S3: Implement `unknown_plan:{view_name}` fallback guard.
- [ ] S4: Rename resolver APIs (`resolve` -> `ensure_and_get`, `get_or_register` -> `ensure_registered`) with no shims.
- [ ] S5: Promote selected pipeline builder helpers to public symbols.
- [ ] S6: Replace local normalization dict reconstruction with canonical accessor.
- [ ] S7: Add provenance comment and optional timestamp override parameter.
- [ ] S8: Normalize calibration aliases at function entry.
- [ ] S9: Enforce `ValidationMode` type at field declaration.
- [ ] S10: Add relspec OTel spans for policy compile and lineage inference.
- [ ] S11: Replace inline chained getattr access with helper.
- [ ] S12: Narrow orchestrator stage span scope; move post-processing outside build span.
- [ ] S13: Remove `internal_name`; document identity-by-default behavior.
- [ ] S14: Complete public `__all__` for planning config aliases.

### Track B (DX1-DX12)

- [ ] DX1: Migrate semantics runtime identity to shared plan fingerprint + plan identity artifacts.
- [ ] DX2: Remove private `datafusion._internal` Substrait path usage.
- [ ] DX3: Add planning-environment hashes to relspec package/policy artifacts.
- [ ] DX4: Deduplicate scan-setting runtime application.
- [ ] DX5: Add cache diagnostics to execution and optional policy calibration inputs.
- [ ] DX6: Establish Python/Rust planning policy parity contract + hash check.
- [ ] DX7: Enforce `PlanningSurfaceSpec` as the default builder path for new planning features.
- [ ] DX8: Implement one bounded RelationPlanner pilot.
- [ ] DX9: Remove avoidable materialization boundaries in semantic execution graph assembly.
- [ ] DX10: Add idempotency guards to planner/physical rule installers.
- [ ] DX11: Standardize overlay-session experiment workflow.
- [ ] DX12: Complete provider-native DML feasibility + pilot decision.

### Decommission and Cleanup

- [ ] Retire semantics-local runtime fingerprint authority after DX1 cutover.
- [ ] Remove dead/private compatibility pathways replaced by builder-native or shared adapters.
- [ ] Publish migration notes for any internal contract version increments.

---

## Acceptance Criteria

1. No active runtime path depends on semantics-only fingerprint identity.
2. No private `datafusion._internal` Substrait access remains in planning/fingerprint code.
3. Scan-setting logic is centralized.
4. relspec execution artifacts include planning environment hash inputs.
5. Rule installers are idempotent and observable.
6. Cache diagnostics are included in runtime artifacts and available to policy diagnostics.
7. Python and Rust planning profile settings are parity-checkable via stable hash.
8. S1-S14 cleanup changes are complete with renamed APIs and decommission scope closed.

---

## Risks and Mitigations

- **Risk:** Identity/fingerprint migration causes cache invalidation surprises.
  - **Mitigation:** explicit payload versioning and one-time cache namespace bump.

- **Risk:** Cross-language policy parity drift.
  - **Mitigation:** CI parity assertions against effective settings snapshots.

- **Risk:** Planner extension changes alter semantics unexpectedly.
  - **Mitigation:** explain-based before/after proof and bounded pilots.

- **Risk:** Rename-heavy phases increase refactor blast radius.
  - **Mitigation:** medium-risk classification, staged rollouts, and expanded caller audits.

---

## Final Recommendation

Execute this integrated v2 plan as the canonical implementation path. Keep the predecessor S1-S14 cleanup scope, but do not ship it in isolation. Pair it with DX1-DX12 convergence work so the codebase actually realizes the intended DataFusion-native architecture across Python and Rust.
