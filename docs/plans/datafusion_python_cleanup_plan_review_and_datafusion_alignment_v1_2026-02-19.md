# DataFusion Python Cleanup Plan Review + DataFusion Alignment
## Detailed Findings and Expanded Scope v1

**Date:** 2026-02-19  
**Status:** Review complete (recommended as replacement baseline for execution planning)  
**Reviewed plan:** `docs/plans/datafusion_python_cleanup_implementation_plan_v1_2026-02-18.md`  
**Primary references:**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md`
- `docs/python_library_reference/datafusion_plan_combination.md`
- `docs/python_library_reference/datafusion_planning.md`
- `docs/python_library_reference/design_principles.md`

---

## Environment Setup Confirmation

Requested environment setup was completed before review:

- `scripts/bootstrap_codex.sh`
- `uv sync`

No setup blockers were observed.

---

## Executive Conclusions

The current cleanup plan is strong on local Python hygiene but under-scopes the highest-leverage DataFusion-native opportunities.

1. The current scope boundary is too narrow for the stated objective.
   - The plan excludes `rust/`, `src/semantics/`, and `src/extract` (`docs/plans/datafusion_python_cleanup_implementation_plan_v1_2026-02-18.md:20`), but those areas currently own core planning-surface, manifest, and execution-identity behavior.
2. Several items should be corrected before implementation starts.
   - `S2` includes an incorrect bug statement.
   - `S4`, `S8`, `S10`, and `S13` have contract or behavior-change risks that need adjusted implementation strategy.
3. The largest impact is to expand from “Python cleanup” to “cross-layer planning-surface unification.”
   - Use DataFusion-native builder/planner/cache/provider contracts end-to-end, with Python as orchestration and Rust as canonical planning backend.

---

## Confirmed Plan Corrections (Errors / Risks)

## C1. Scope boundary is misaligned with objective

**Plan statement**
- `docs/plans/datafusion_python_cleanup_implementation_plan_v1_2026-02-18.md:20`

**Problem**
- Excluding `rust/` and upstream planning consumers (`src/semantics`, `src/extract`) conflicts with “maximize DataFusion-integrated planning deployment.”
- Planning-surface ownership is already Rust-first:
  - `rust/codeanatomy_engine/src/session/planning_surface.rs:1`
  - `rust/codeanatomy_engine/src/session/factory.rs:386`
  - `rust/codeanatomy_engine/src/session/planning_manifest.rs:1`

**Required correction**
- Expand plan scope to include:
  - `rust/codeanatomy_engine/src/session/*`
  - `rust/datafusion_python/src/*`
  - `src/semantics/*` plan identity/fingerprint consumers
  - `src/extract/*` plan bundle/execution consumers

---

## C2. `S2` bug statement is inaccurate (case-sensitivity)

**Plan statement**
- `docs/plans/datafusion_python_cleanup_implementation_plan_v1_2026-02-18.md:135`

**Current code**
- `src/storage/deltalake/file_pruning.py:422`
- `src/storage/deltalake/file_pruning.py:424`

`_coerce_bool_value` currently uses `value.lower() == "true"`, so this specific path is not case-sensitive.

**Required correction**
- Keep the DRY cleanup, but reframe rationale as:
  - semantic duplication,
  - inconsistent coercion semantics (`"foo"` becomes `False` vs unknown),
  - maintainability drift risk.

---

## C3. `S10` should not default to deletion of strict API

**Plan statement**
- `docs/plans/datafusion_python_cleanup_implementation_plan_v1_2026-02-18.md:697`

**Current code**
- `src/datafusion_engine/udf/metadata.py:869`
- `src/datafusion_engine/udf/metadata.py:887`
- strict/default both return `UdfCatalog(udf_specs=udf_specs)`.

**Risk**
- Deleting `create_strict_catalog` can remove a public contract surface while callers still request strict mode:
  - `src/datafusion_engine/udf/metadata.py:1173`
  - `src/datafusion_engine/session/runtime_extensions.py:302`

**Required correction**
- Prefer implementing strict semantics (or explicit deprecation shim), not hard deletion in first pass.
- If deprecating:
  - retain symbol with warning,
  - add compatibility window,
  - version contract change per design principle #22.

---

## C4. `S8` ContextVar proposal has semantics mismatch risk

**Plan statement**
- `docs/plans/datafusion_python_cleanup_implementation_plan_v1_2026-02-18.md:578`

**Current code**
- `src/datafusion_engine/session/runtime_extensions.py:75`
- `src/datafusion_engine/session/runtime_extensions.py:226`

**Risk**
- `ContextVar` isolates per logical context, not necessarily per runtime-profile/session lifecycle.
- Existing behavior is process-global warning suppression. Replacing with `ContextVar` may emit duplicates in concurrent task contexts or suppress unexpectedly across the wrong boundary.

**Required correction**
- Move warning state to explicit lifecycle owner:
  - profile-scoped weak map keyed by `SessionContext`, or
  - runtime-profile state object with deterministic reset hooks for tests.

---

## C5. `S13` can introduce broad behavior regression

**Plan statement**
- `docs/plans/datafusion_python_cleanup_implementation_plan_v1_2026-02-18.md:875`

**Current code**
- `src/datafusion_engine/catalog/introspection.py:347`
- `src/datafusion_engine/catalog/introspection.py:357`

**Risk**
- Current `snapshot` getter lazily captures and clears invalidation.
- Changing getter to raise before explicit refresh can break many transitive callsites that currently rely on lazy capture.

**Required correction**
- Keep backward-compatible getter behavior in first pass.
- Add explicit command API (`refresh()`), then gradually migrate callsites.
- Introduce optional strict mode only after callsite audit.

---

## C6. `S4` mixin removal needs compatibility strategy

**Plan statement**
- `docs/plans/datafusion_python_cleanup_implementation_plan_v1_2026-02-18.md:278`

**Current code**
- `src/datafusion_engine/session/runtime.py:224`
- `src/datafusion_engine/session/runtime_ops.py:456`

**Risk**
- Directly removing facade methods can break external/internal callers using existing method names.

**Required correction**
- Stepwise migration:
  - keep facade methods as compatibility wrappers with deprecation warnings,
  - migrate callsites,
  - remove wrappers in later release window.

---

## C7. Quality gate command should align with repository policy

**Plan statement**
- `docs/plans/datafusion_python_cleanup_implementation_plan_v1_2026-02-18.md:38`

**Issue**
- Plan hardcodes `... && uv run pytest -q` into the “single quality gate.”
- Repository guidance defines canonical gate as:
  - `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright`

**Required correction**
- Keep canonical gate as required.
- Treat pytest selection as task-specific validation step, not part of the hard gate contract.

---

## C8. Delta read/write private coupling should be elevated in priority

**Current code**
- `src/storage/deltalake/delta_write.py:23`
- `src/storage/deltalake/delta_write.py:29`
- `src/storage/deltalake/delta_read.py:63`
- `src/storage/deltalake/delta_read.py:315`

`delta_write.py` imports private/internal symbols from `delta_read.py`, creating fragile module coupling.

**Required correction**
- Promote this from medium cleanup to high-priority boundary fix.
- Move shared public contracts/helpers to dedicated module (`delta_shared.py` or equivalent).

---

## Expanded DataFusion-Native Scope (Recommended Additions)

The following additions are the main leverage points to reduce bespoke code and align to DataFusion 52 planning/runtime contracts.

## DX1. Make Rust planning surface the canonical source of truth, then project to Python

**Why**
- Rust already has typed planning-surface composition and manifest hashing:
  - `rust/codeanatomy_engine/src/session/planning_surface.rs:27`
  - `rust/codeanatomy_engine/src/session/factory.rs:438`
  - `rust/codeanatomy_engine/src/session/planning_manifest.rs:32`

**Add scope**
- Define one canonical planning-surface payload schema in Rust.
- Python consumes this payload instead of re-deriving policy identity independently.
- Enforce parity in CI using manifest hash and policy hash.

**Design principles**
- #1 Information hiding, #7 DRY, #12 Dependency inversion, #22 Versioned contracts.

---

## DX2. Collapse runtime policy parsing duplication into DF52-native cache/runtime contracts

**Why**
- Python currently parses runtime strings and bridges them manually:
  - `src/datafusion_engine/session/delta_session_builder.py:169`
  - `src/datafusion_engine/session/delta_session_builder.py:369`
- Rust bridge already supports typed `CacheManagerConfig` application:
  - `rust/datafusion_python/src/codeanatomy_ext/delta_session_bridge.rs:104`
  - `rust/datafusion_python/src/codeanatomy_ext/delta_session_bridge.rs:121`

**Add scope**
- Unify runtime cache policy parsing in one place (typed contract).
- Remove duplicated string parsing paths in Python where Rust bridge already guarantees typed handling.
- Preserve explicit diagnostics payload for consumed/unsupported keys.

**Reference anchors**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:847`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:1121`

---

## DX3. Complete DF52 FFI provider signature alignment (session-aware provider capsules)

**Why**
- Rust side already calls capsule providers with `session` parameter:
  - `rust/datafusion_python/src/utils.rs:171`
  - `rust/datafusion_python/src/context.rs:607`
  - `rust/datafusion_python/src/catalog.rs:117`
- Python Protocols still advertise no-arg signatures:
  - `rust/datafusion_python/python/datafusion/context.py:93`
  - `rust/datafusion_python/python/datafusion/catalog.py:278`

**Add scope**
- Update Python Protocol signatures and docs to include `session`.
- Add compatibility adapter or clear migration note for old no-arg provider implementations.
- Add test coverage for table/catalog/schema provider capsule calls with session argument.

**Reference anchors**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:156`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:159`

---

## DX4. Standardize explain artifact capture on DataFrame rows, not stdout text scraping

**Why**
- Python path still captures explain text via redirected stdout:
  - `src/datafusion_engine/plan/profiler.py:102`
  - `src/datafusion_engine/plan/plan_utils.py:13`
- Rust path already captures structured explain rows from DataFrame result batches:
  - `rust/codeanatomy_engine/src/compiler/plan_bundle.rs:707`
  - `rust/codeanatomy_engine/src/compiler/plan_bundle.rs:738`

**Add scope**
- Replace stdout explain capture with SQL/DataFrame explain row capture (`plan_type`, `plan`).
- Keep text rendering only as a fallback path.
- Ensure `EXPLAIN VERBOSE`/`ANALYZE` handling respects DataFusion format constraints.

**Reference anchors**
- `docs/python_library_reference/datafusion_plan_combination.md:800`
- `docs/python_library_reference/datafusion_planning.md:2500`

---

## DX5. Expand planning-surface governance to include object store/catalog environment identity in Python parity path

**Why**
- Rust manifest tracks rich planning-surface identity:
  - `rust/codeanatomy_engine/src/session/planning_manifest.rs:69`
- Python has object store registration instrumentation:
  - `src/datafusion_engine/io/adapter.py:143`
- But cross-layer parity should include these environment identities in one canonical payload.

**Add scope**
- Include object-store/catalog/schema provider identity snapshots in Python planning-env payload or consume Rust-provided payload directly.
- Hash these identities in plan portability/fingerprint envelope.

**Reference anchors**
- `docs/python_library_reference/datafusion_plan_combination.md:1221`
- `docs/python_library_reference/datafusion_planning.md:207`

---

## DX6. Treat scan pushdown and physical expr adapter behavior as contract tests (not optional behavior)

**Why**
- Python installs physical expr adapter factory conditionally:
  - `src/datafusion_engine/session/runtime_extensions.py:1038`
- Schema evolution adapter path exists in Rust extension:
  - `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs:327`

**Add scope**
- Add explicit conformance tests for:
  - predicate rewrite correctness under schema evolution,
  - projection/predicate interaction,
  - required fallback behavior when adapter registration surface is missing.

**Reference anchors**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:1247`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:1276`

---

## DX7. Unify plan identity/fingerprint computation with existing plan artifact identity to remove bespoke hashing

**Why**
- `src/semantics/plans/fingerprints.py` recomputes logical/substrait/schema hashes with custom fallback behavior:
  - `src/semantics/plans/fingerprints.py:184`
  - `src/semantics/plans/fingerprints.py:265`
- Plan bundle pipeline already computes deterministic fingerprint and identity payload:
  - `src/datafusion_engine/plan/bundle_assembly.py:575`
  - `src/datafusion_engine/plan/plan_identity.py:100`

**Add scope**
- Semantics should consume canonical `plan_identity_hash` / `plan_fingerprint` from plan bundle artifacts.
- Reserve bespoke hash computation only for exceptional contexts where bundle generation is unavailable.

**Design principles**
- #7 DRY, #18 Determinism, #22 Contract versioning.

---

## DX8. Promote listing table inference/cache settings into explicit policy profile + artifact assertion

**Why**
- Listing-table partition inference is already in config and identity payload:
  - `src/datafusion_engine/session/runtime_config_policies.py:529`
  - `src/datafusion_engine/plan/plan_identity.py:110`
- It should be explicitly validated in all plan regression bundles and policy parity checks.

**Add scope**
- Add contract test to assert this key is always present in `df_settings`.
- Fail plan identity assembly when required planning-affecting keys are absent.

**Reference anchors**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:4196`

---

## DX9. Expand DML alignment to DataFusion TableProvider-native semantics where feasible

**Why**
- DF52 expands provider hooks for DELETE/UPDATE.
- Current code still routes through bespoke control-plane wrappers in many paths.

**Add scope**
- Evaluate where provider-native DML hooks can replace custom wrappers while preserving Delta-specific policy controls.
- Keep wrapper layer only for policy/audit concerns that DataFusion does not natively represent.

**Reference anchors**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:81`

---

## DX10. Include `src/extract` and `src/semantics` plan-consumer paths in same implementation plan

**Why**
- Extract and semantics layers are already consuming plan artifacts and identities:
  - `src/extract/infrastructure/worklists.py:270`
  - `src/semantics/pipeline_builders.py:56`
  - `src/semantics/pipeline_diagnostics.py:48`

**Add scope**
- Co-migrate consumer assumptions when plan identity/explain capture/policy payload contracts change.
- Prevent split-brain behavior where producer and consumers evolve independently.

---

## Recommended Revised Workstreams

## Wave 0: Contract and scope reset (must do first)

1. Expand scope to include Rust planning surface + semantics/extract consumers.
2. Correct `S2` rationale and `S10` strategy.
3. Define compatibility policy for `S4` and `S13`.

**Exit criteria**
- Updated plan document with corrected scope and compatibility strategy.
- No implementation starts before this baseline is accepted.

---

## Wave 1: Canonical planning/runtime contracts

1. Rust-first canonical planning-surface payload + hash parity.
2. Runtime cache policy unification and duplicate parser removal.
3. DF52 session-aware provider signature alignment.

**Exit criteria**
- Cross-language parity tests pass for policy hash and provider signatures.

---

## Wave 2: Artifact and explain unification

1. Replace stdout explain capture with row-based explain artifacts.
2. Unify semantics fingerprint consumers with canonical plan identity payloads.
3. Add deterministic artifact fixture checks (`df_settings`, planning payload, explain rows).

**Exit criteria**
- Deterministic plan artifact bundle validated in Python and Rust CI paths.

---

## Wave 3: Targeted cleanup items from original S1-S20

Execute original cleanup items with corrected sequencing:

1. Contract safety first (`S1`, `S3`, `_CACHE_SNAPSHOT_ERRORS` narrowing, delta private coupling).
2. Compatibility-safe API cleanup (`S4`, `S10`, `S13` via staged migration).
3. Mechanical dedup and rename tasks (`S2`, `S5`, `S7`, etc.).

**Exit criteria**
- Cleanup completed without unplanned API breakage.

---

## Acceptance Criteria and Test Matrix Additions

## A. Parity and contract tests

1. Python/Rust planning-surface hash parity test for same runtime profile.
2. FFI capsule provider signature tests with `session` argument (`table`, `catalog`, `schema`).
3. Policy payload versioning test to enforce explicit contract evolution.

## B. Plan artifact determinism tests

1. Assert bundle contains:
   - `df_settings`,
   - planning surface hash/policy hash,
   - structured explain rows (`plan_type`, `plan`).
2. Golden-diff tests for optimized logical plan and explain rows under stable settings.

## C. Pushdown/runtime behavior tests

1. Physical expr adapter contract tests for schema evolution and filter correctness.
2. Cache policy tests for list-files/statistics cache settings application.
3. Listing partition inference policy capture/identity tests.

---

## Final Recommendation

Adopt this review as the new baseline and rewrite the current implementation plan before coding.

The current plan is valuable as a cleanup backlog, but without the scope expansion and contract corrections above it will optimize local Python code while leaving the main source of bespoke planning divergence (cross-layer planning-surface and runtime contract split between Python and Rust) unresolved.

