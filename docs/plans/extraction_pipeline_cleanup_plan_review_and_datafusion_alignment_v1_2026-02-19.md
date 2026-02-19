# Extraction Pipeline Cleanup Plan Review + DataFusion Alignment
## Detailed Findings and Expanded Scope v1

**Date:** 2026-02-19  
**Status:** Review complete (recommended as next planning baseline)  
**Reviewed plan:** `docs/plans/extraction_pipeline_cleanup_implementation_plan_v1_2026-02-18.md`  
**Primary references:**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md`
- `docs/python_library_reference/datafusion_plan_combination.md`
- `docs/python_library_reference/datafusion_planning.md`
- `docs/python_library_reference/design_principles.md`

---

## Executive Conclusions

The current extraction cleanup plan (`S1`-`S8`) is directionally strong, but it has two blocking scope/contract errors and several under-scoped opportunities.

1. The plan should proceed only after correcting the two blocking issues:
   - `S1/D1` omits a real `SpanSpec` usage in test support code.
   - `S4` assumes an outdated `run_extraction(...)` signature.
2. Beyond cleanup, there is substantial additional scope to replace bespoke logic with DataFusion-native planning/runtime surfaces, especially around:
   - runtime cache policy wiring,
   - planning surface composition (`SessionStateBuilder` + file format/table options),
   - projection/pushdown/sort contracts in DF52,
   - plan-artifact identity and environment snapshoting,
   - optimizer rule governance and ordering safety.
3. The largest leverage point is to treat DataFusion as the source of truth for planning objects, environment construction, and execution boundaries, and to confine custom logic to explicit adapters.

---

## Confirmed Plan Corrections (Errors / Gaps)

## C1. `S1/D1` deletion scope is incomplete

**Plan text that is incomplete**
- `docs/plans/extraction_pipeline_cleanup_implementation_plan_v1_2026-02-18.md:94`
- `docs/plans/extraction_pipeline_cleanup_implementation_plan_v1_2026-02-18.md:593`

**Missing in plan scope**
- `src/test_support/datafusion_ext_stub.py:683`
- `src/test_support/datafusion_ext_stub.py:698`

`SpanSpec`/`span_dict` are still imported and instantiated in test-support code, so deleting them per D1 without including this file will break the migration.

**Required correction**
- Add `src/test_support/datafusion_ext_stub.py` to S1 and D1 migration checklist.
- Add regression test updates for test-support tree-sitter row builder behavior.

---

## C2. `S4` is based on a stale public API shape

**Plan text that does not match current code**
- `docs/plans/extraction_pipeline_cleanup_implementation_plan_v1_2026-02-18.md:234`
- `docs/plans/extraction_pipeline_cleanup_implementation_plan_v1_2026-02-18.md:245`
- `docs/plans/extraction_pipeline_cleanup_implementation_plan_v1_2026-02-18.md:259`

**Actual current API**
- `src/extraction/orchestrator.py:83` (`run_extraction(request: RunExtractionRequestV1)`)
- `src/extraction/contracts.py:17` (`RunExtractionRequestV1`)
- `src/graph/build_pipeline.py:243` (request-object callsite)

The plan’s proposed direct function-parameter injection for `run_extraction` is not aligned with the current request-envelope API.

**Required correction**
- Keep `run_extraction` request-envelope shape intact.
- If bundle injection is needed, add it as:
  - a request field in `RunExtractionRequestV1` (explicit contract), or
  - an internal test seam in a lower-level helper, not a mismatched top-level signature change.

---

## C3. `S6` duplication cleanup is under-scoped

The plan focuses on `extract_*` entrypoints, but comparable orchestration duplication is also present in `extract_*_plans` paths.

**Evidence**
- `src/extract/extractors/ast/builders.py:926`
- `src/extract/extractors/ast/builders.py:967`
- `src/extract/extractors/bytecode/builders.py:1563`
- `src/extract/extractors/bytecode/builders.py:1610`
- `src/extract/extractors/cst/builders.py:1517`
- `src/extract/extractors/cst/builders.py:1586`
- `src/extract/extractors/symtable_extract.py:759`
- `src/extract/extractors/symtable_extract.py:809`

**Required correction**
- Expand S6 abstraction target to both materializing and plan-returning entrypoints.
- Preserve extractor-specific row-collection differences while deduplicating session/runtime/evidence wiring.

---

## C4. `S5` pure-core split is valid and should be generalized

`_extract_ast_for_context` currently mixes deterministic parse/walk logic with cache read/write and lock orchestration.

**Evidence**
- `src/extract/extractors/ast/builders.py:857`
- `src/extract/extractors/ast/builders.py:874`
- `src/extract/extractors/ast/builders.py:908`

This is a good first step toward design principle #16 (functional core, imperative shell), and should become the extraction-wide pattern, not AST-only.

---

## C5. `S2` dead reflection removal is valid and low risk

`inspect.signature(...)` checks are unnecessary because all target functions already accept `execution_bundle`.

**Evidence**
- Reflection callsites:
  - `src/extraction/orchestrator.py:230`
  - `src/extraction/orchestrator.py:342`
  - `src/extraction/orchestrator.py:378`
- Target function signatures:
  - `src/extraction/orchestrator.py:457`
  - `src/extraction/orchestrator.py:819`
  - `src/extraction/orchestrator.py:867`

---

## C6. `S8` timing fix is necessary

Stage-1 per-extractor timings are currently measured from shared stage start.

**Evidence**
- `src/extraction/orchestrator.py:302`
- `src/extraction/orchestrator.py:314`
- `src/extraction/orchestrator.py:321`

The plan’s proposed submission-time map is the correct fix.

---

## Expanded DataFusion-Native Scope (Recommended Additions)

The following additions maximize use of integrated DataFusion planning/runtime capabilities and reduce bespoke implementation.

## DX1. Unify runtime cache policy on DF52-native cache surfaces

**Why**
- DF52 exposes typed cache management via `CacheManagerConfig` and runtime cache manager APIs.
- Runtime knobs are first-class and documented (`list_files_cache_limit`, `list_files_cache_ttl`).

**Reference**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:847`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:862`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:1121`

**Current repo state**
- Python side performs bespoke runtime-setting parsing/bridging:
  - `src/datafusion_engine/session/delta_session_builder.py:175`
  - `src/datafusion_engine/session/delta_session_builder.py:375`
  - `src/datafusion_engine/session/delta_session_builder.py:466`
- Session config path skips `datafusion.runtime.*` keys:
  - `src/datafusion_engine/session/context_pool.py:50`
- Rust extension already wires `CacheManagerConfig`:
  - `rust/datafusion_python/src/codeanatomy_ext/delta_session_bridge.rs:104`
  - `rust/datafusion_python/src/codeanatomy_ext/delta_session_bridge.rs:124`

**Add scope**
- Collapse custom runtime-setting parsing to one typed policy contract feeding DF-native cache manager wiring.
- Remove duplicated string-key policy paths where possible.

---

## DX2. Move “reserved” runtime fields to applied where DF52 supports them

**Why**
- Several fields are marked reserved in Rust coverage despite explicit DF52 surfaces and active bridge wiring.

**Reference**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:847`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:1121`

**Current repo state**
- Reserved coverage entries:
  - `rust/codeanatomy_engine/src/session/profile_coverage.rs:67`
  - `rust/codeanatomy_engine/src/session/profile_coverage.rs:141`
  - `rust/codeanatomy_engine/src/session/profile_coverage.rs:146`
  - `rust/codeanatomy_engine/src/session/profile_coverage.rs:151`
- Reserved warnings emitted for these fields:
  - `rust/codeanatomy_engine/src/session/profile_coverage.rs:229`
- Runtime profile carries these fields:
  - `rust/codeanatomy_engine/src/session/runtime_profiles.rs:34`
  - `rust/codeanatomy_engine/src/session/runtime_profiles.rs:70`

**Add scope**
- Reclassify and wire currently-reserved cache-related fields through canonical runtime policy application.
- Update coverage tests/expectations accordingly.

---

## DX3. Promote planning-surface composition as the single builder path (Python + Rust parity)

**Why**
- DataFusion planning docs emphasize builder-time composition (`with_default_features`, `with_file_formats`, `with_table_options`).
- This is directly aligned to design principles #1, #7, #12, #22.

**Reference**
- `docs/python_library_reference/datafusion_plan_combination.md:1539`
- `docs/python_library_reference/datafusion_plan_combination.md:1543`
- `docs/python_library_reference/datafusion_plan_combination.md:1544`
- `docs/python_library_reference/datafusion_plan_combination.md:1545`

**Current repo state**
- Rust has strong `PlanningSurfaceSpec` + apply function:
  - `rust/codeanatomy_engine/src/session/planning_surface.rs:43`
  - `rust/codeanatomy_engine/src/session/planning_surface.rs:99`
  - `rust/codeanatomy_engine/src/session/factory.rs:335`
- Relation planner is installed when domain planner is enabled:
  - `rust/codeanatomy_engine/src/session/factory.rs:363`

**Add scope**
- Define parity contract so Python-side planning/session composition derives from the same policy payload semantics as Rust.
- Make ad-hoc post-build mutation the explicit exception path only.

---

## DX4. Adopt DF52 schema-forward pushdown contracts as mandatory for custom scan paths

**Why**
- DF52 moved projection/sort pushdown contracts into `FileSource` and formalized `PhysicalExprAdapter`.
- This is critical for reducing bespoke scan adaptation logic and preserving optimizer behavior.

**Reference**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:1247`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:1289`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:1294`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:4031`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:4051`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:4083`

**Current repo state**
- Adapter install path exists:
  - `src/datafusion_engine/session/runtime_extensions.py:1039`
  - `src/datafusion_engine/session/runtime_extensions.py:1061`
- Rust schema-evolution adapter wiring exists:
  - `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs:327`

**Add scope**
- Require pushdown contract conformance tests for custom file/table sources.
- Treat `TableSchema` composition and projection fallibility as required invariants, not optional behavior.

---

## DX5. Pin listing-table partition inference behavior explicitly

**Why**
- DF52 docs call out partition inference defaults as a schema drift footgun.

**Reference**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:4196`

**Add scope**
- Set and snapshot explicit policy for `listing_table_factory_infer_partitions`.
- Ensure plan identity/fingerprint payload captures this decision path.

---

## DX6. Expand plan artifacts as the canonical replay/debug envelope

**Why**
- DataFusion planning guidance recommends capturing `df_settings` and physical/logical artifacts.
- Existing bundle tooling already captures much of this.

**Reference**
- `docs/python_library_reference/datafusion_plan_combination.md:628`
- `docs/python_library_reference/datafusion_plan_combination.md:826`
- `docs/python_library_reference/datafusion_planning.md:454`
- `docs/python_library_reference/datafusion_planning.md:756`

**Current repo state**
- Bundle assembly already captures logical/optimized plans, settings, information schema hashes, and identity payload:
  - `src/datafusion_engine/plan/bundle_assembly.py:338`
  - `src/datafusion_engine/plan/bundle_assembly.py:343`
  - `src/datafusion_engine/plan/bundle_assembly.py:571`
  - `src/datafusion_engine/plan/plan_identity.py:100`

**Add scope**
- Make this envelope mandatory for extraction-planning paths that need reproducibility/debug parity.
- Prefer one shared plan-identity pathway; avoid bespoke extraction-local identity mechanisms.

---

## DX7. Rationalize DML/materialization boundaries to reduce bespoke write orchestration

**Why**
- DataFusion explicitly supports DML/COPY and write-table boundaries suitable for plan sinks.

**Reference**
- `docs/python_library_reference/datafusion_plan_combination.md:670`
- `docs/python_library_reference/datafusion_plan_combination.md:685`
- `docs/python_library_reference/datafusion_planning.md:550`
- `docs/python_library_reference/datafusion_planning.md:559`

**Current repo state**
- Extraction writes are performed through custom `_write_delta` + transaction request assembly:
  - `src/extraction/orchestrator.py:407`
  - `src/extraction/orchestrator.py:441`

**Add scope**
- Evaluate replacing parts of bespoke write orchestration with DataFusion materialization pathways where behavior and guarantees align.
- Keep custom path only where Delta-specific guarantees exceed DataFusion DML path requirements.

---

## DX8. Physical optimizer rule governance and ordering safety

**Why**
- In DF52, appended custom physical rules run after defaults, and rule ordering matters with dynamic filters.

**Reference**
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:4875`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:4879`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:4982`
- `docs/python_library_reference/datafusion_52_vs_51_changes.md:4988`

**Current repo state**
- Custom physical rule wraps plans with `CoalescePartitionsExec`:
  - `rust/datafusion_ext/src/physical_rules.rs:105`
  - `rust/datafusion_ext/src/physical_rules.rs:118`

**Add scope**
- Add benchmark- and correctness-based gate for this rule.
- Ensure rule placement does not violate DF52 dynamic-filter safety assumptions.

---

## DX9. Relation planner extension should be meaningful or disabled

**Why**
- A registered extension that always delegates adds maintenance surface without capability.

**Current repo state**
- Relation planner currently returns `Original(relation)` unconditionally:
  - `rust/datafusion_ext/src/relation_planner.rs:13`
  - `rust/datafusion_ext/src/relation_planner.rs:19`
- It is installed through planning surface when enabled:
  - `rust/codeanatomy_engine/src/session/factory.rs:363`

**Add scope**
- Either implement concrete relation-planning behavior with tests, or disable/remove this extension by default until needed.

---

## DX10. Cross-language policy parity and contract versioning

**Why**
- Design principle #22 requires stable, explicit contract versioning for public/stable surfaces.
- Current system has strong primitives; parity should be formalized across Python and Rust.

**Current repo state**
- Rich typed planning surface and manifests already exist in Rust:
  - `rust/codeanatomy_engine/src/session/planning_surface.rs:43`
  - `rust/codeanatomy_engine/src/session/planning_manifest.rs:264`

**Add scope**
- Define and enforce a versioned cross-language policy contract for:
  - runtime/cache knobs,
  - planning-surface features,
  - extension governance,
  - plan identity inputs.

---

## Recommended Revised Implementation Order

1. **Fix blocking plan corrections first** (`C1`, `C2`, `C3` updates to the plan document/checklists).
2. **Implement S2 + S8 + S7** (low-risk cleanup wins with measurable value).
3. **Implement S3 + corrected S4 injection strategy** on request-envelope or internal seam.
4. **Implement S5 + expanded S6** across both materializing and plan-returning extractor entrypoints.
5. **Execute DataFusion-native expansion as a dedicated follow-on stream** (`DX1`-`DX10`) with explicit acceptance criteria per item.

---

## Design-Principles Alignment (What Improves)

- **Information hiding / separation / SRP** (`#1`, `#2`, `#3`): move planning/runtime plumbing into DataFusion-native composition surfaces, keep extraction policy logic separate.
- **DRY / single source of truth** (`#7`): one canonical runtime/policy mapping and one canonical planning identity payload.
- **Design by contract / explicit contracts** (`#8`, `#22`): versioned cross-language policy contract replaces implicit string-key behavior.
- **Functional core / deterministic shell** (`#16`, `#18`): isolate parse/walk pure cores and standardize plan artifact snapshots (`df_settings`, schema hash, plan identity).
- **Testability / observability** (`#23`, `#24`): injection seams tied to request envelopes and standardized plan/runtime telemetry.

---

## Implementation Readiness Checklist (Updated)

- [ ] Plan doc updated for `C1`/`C2`/`C3`.
- [ ] `S1` migration includes `src/test_support/datafusion_ext_stub.py`.
- [ ] `S4` uses request-envelope-compatible seam strategy.
- [ ] `S6` includes `extract_*_plans` dedup path.
- [ ] DataFusion runtime cache policy unification scope accepted (`DX1`/`DX2`).
- [ ] Planning-surface parity contract scope accepted (`DX3`/`DX10`).
- [ ] Pushdown/schema-forward conformance test scope accepted (`DX4`/`DX5`).
- [ ] Optimizer/extension governance scope accepted (`DX8`/`DX9`).
- [ ] Materialization boundary rationalization scope accepted (`DX7`).

