# Design Review: DF Plan Compilation + Lineage + Views

**Date:** 2026-02-17
**Scope:** `src/datafusion_engine/plan/`, `src/datafusion_engine/lineage/`, `src/datafusion_engine/views/`
**Focus:** Correctness (16-18), Knowledge (7-11), Simplicity (19-22)
**Depth:** Deep
**Files reviewed:** 47 (all files in scope)

---

## Executive Summary

The plan/lineage/views subsystem is structurally sound and demonstrates thoughtful
decomposition. The Rust compiler bridge (`rust_bundle_bridge.py`) is the canonical path for
Substrait serialization, and the `registry_specs.py` registration authority contract holds.
However, three systemic issues warrant attention: (1) the Python lineage walker in
`reporting.py` substantially duplicates the Rust lineage extractor in
`compiler/lineage.rs`, with the Python copy carrying fragility around `getattr`-chain
attribute probing; (2) the information-schema introspection logic — including
`information_schema_snapshot`, `canonicalize_snapshot`, `function_registry_hash`, and
`settings_rows_to_mapping` — is **duplicated verbatim** between `bundle_environment.py`
and `plan_introspection.py`, a direct DRY violation; (3) the Python-side plan walker
(`walk.py`) implements a bespoke preorder traversal that DataFusion's `LogicalPlan.inputs()`
already provides, and the walker-plus-`getattr`-chain lineage pattern will fragment as the
DataFusion IR evolves. DF52's dynamic filter pushdown and `CoalesceBatchesExec` removal
are not yet reflected in any Python-side plan-shape assumptions.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 7 | DRY | 0 | small | medium | `bundle_environment.py` and `plan_introspection.py` duplicate identical code verbatim |
| 8 | Design by contract | 2 | small | low | `PlanBundleOptions.compute_substrait` guard inconsistent; `validate_substrait_plan` returns `Mapping` instead of typed result |
| 9 | Parse, don't validate | 2 | small | low | Filter strings parsed by regex post-hoc in `scheduling.py` instead of at plan extraction boundary |
| 10 | Illegal states unrepresentable | 2 | small | low | `DataFusionPlanArtifact.execution_plan` typed `object | None`; `ExplainCapture` carries mixed nulls |
| 11 | CQS | 1 | medium | medium | `_plan_core_components` computes plans and records telemetry timing as a side effect; `validate_substrait_plan` executes the plan and returns match status |
| 16 | Functional core, imperative shell | 2 | medium | low | Lineage extraction is largely pure but `_safe_attr` and `_expr_children` swallow errors rather than propagating typed failures |
| 17 | Idempotency | 3 | — | — | Bundle assembly is idempotent by design (Substrait via Rust) |
| 18 | Determinism | 2 | medium | medium | Python lineage walker probes runtime object identity (`id(node)`) and attribute shape rather than plan structure; fragile under DF API evolution |
| 19 | KISS | 1 | large | medium | Python plan walker + lineage extractor + filter parser duplicates the Rust lineage compiler; two code paths for the same operation |
| 20 | YAGNI | 2 | small | low | `PlanCacheKey` carries ten hash fields; `PlanCacheEntry` duplicates all ten for reconstruction; speculative generality |
| 21 | Least astonishment | 2 | small | low | `profiler.capture_explain` respects `CODEANATOMY_DISABLE_DF_EXPLAIN` env var defaulting to `"1"` (disabled by default), which is surprising |
| 22 | Public contracts | 2 | small | low | `DataFusionPlanArtifact.logical_plan` typed `object` rather than `DataFusionLogicalPlan`; callers must cast |

---

## Detailed Findings

### Category: Knowledge (7-11)

#### P7. DRY — Alignment: 0/3

**Current state:**

`bundle_environment.py` and `plan_introspection.py` contain **identical function
implementations** for the following: `information_schema_sql_options`, `build_introspector`,
`table_definitions_snapshot`, `safe_introspection_rows`, `routine_metadata_snapshot`,
`settings_rows_to_mapping`, `df_settings_snapshot`, `information_schema_snapshot`,
`canonicalize_snapshot`, `canonical_sort_key`, `information_schema_hash`, and
`function_registry_hash`. The docstrings differ but the bodies are byte-for-byte
copies.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/bundle_environment.py:17-219` — complete module
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/plan_introspection.py:19-228` — byte-for-byte duplicates of the same 12 functions
- `planning_env.py:176-199` — `function_registry_artifacts` reimplements logic already in `bundle_environment.py:216` and `plan_introspection.py:231`, differing only in try/except structure

The only consumer difference: `plan_introspection.py` uses a `function_registry_hash_for_context` entry point and `suppress_introspection_errors()` helper not present in `bundle_environment.py`. These small additions do not justify full duplication.

**Suggested improvement:**

Delete `plan_introspection.py` entirely. Move `function_registry_hash_for_context` and
`suppress_introspection_errors` into `bundle_environment.py` as the single canonical
module. Update all callers of `plan_introspection` imports (currently `bundle_assembly.py`
imports from `bundle_environment.py` directly; verify `plan_introspection.py` has no
remaining callers via `/cq calls information_schema_snapshot`). The `planning_env.py`
`function_registry_artifacts` function should delegate to `bundle_environment.function_registry_hash_for_context`.

**Effort:** small
**Risk if unaddressed:** medium — any schema introspection logic change will require two
synchronized edits; drift has likely already started (docstrings differ).

---

#### P8. Design by Contract — Alignment: 2/3

**Current state:**

Most contracts are explicit and enforced. However, two weaknesses exist:

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/bundle_artifact.py:114-116` — `build_plan_artifact` raises `ValueError` if `compute_substrait` is False, but `PlanBundleOptions.compute_substrait` defaults to `True` and the guard in `bundle_assembly.py:249` also raises. The same invariant is checked twice in different stack frames with slightly different messages, fragmenting where the contract is enforced.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/result_types.py:355-397` — `validate_substrait_plan` returns `Mapping[str, object]` with keys `status`, `match`, `diagnostics`. Callers must inspect raw strings like `validation.get("match")`. This is an "anemic return" anti-pattern for a contract-checking function. A typed result struct would make the contract explicit.

**Suggested improvement:**

Introduce a `SubstraitValidationResult(msgspec.Struct)` with fields `status: str`, `match: bool | None`, `diagnostics: dict[str, object]`. Remove the duplicate `compute_substrait` guard — keep only the one in `bundle_assembly.py`. Add an `__post_init__`-style precondition to `PlanBundleOptions` asserting `compute_substrait is True` at construction time rather than at use time.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, Don't Validate — Alignment: 2/3

**Current state:**

Filter strings extracted by the Python lineage walker are stored as raw strings in
`ScanLineage.pushed_filters`. The scan planner in `scheduling.py` then re-parses these
strings using two regular expressions (`_EQUALS_FILTER_PATTERN`, `_COMPARISON_FILTER_PATTERN`) to reconstruct partition and stats filters.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/lineage/scheduling.py:52-57` — two regex patterns for parsing already-rendered filter expressions
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/lineage/scheduling.py:735-777` — `_partition_filters_from_strings` and `_stats_filters_from_strings` parse rendered strings back into structured `PartitionFilter`/`StatsFilter` objects
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/lineage/reporting.py:229-244` — filters are captured as `str(expr)` at plan extraction time, discarding the structured `Expr` representation

The fundamental problem: the boundary where the DataFusion plan is traversed (`reporting.py`) stringifies expressions that the scanner (`scheduling.py`) then re-parses. If the filter expressions were captured in structured form at the lineage-extraction boundary, the regex re-parsing would be unnecessary.

**Suggested improvement:**

Extend `ScanLineage` with optional `partition_filter_hints: tuple[PartitionFilter, ...]`
and `stats_filter_hints: tuple[StatsFilter, ...]` populated at extraction time when the
Rust lineage path is available (the Rust lineage extractor in `compiler/lineage.rs`
has access to `Expr` objects directly). Fall back to regex parsing only when hints are
absent. This removes the speculative parse-back entirely for the Rust-derived path.

**Effort:** medium
**Risk if unaddressed:** low — current approach works; risk is parse failures on complex expressions

---

#### P10. Illegal States Unrepresentable — Alignment: 2/3

**Current state:**

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/bundle_artifact.py:53-54` — `DataFusionPlanArtifact.logical_plan: object` and `optimized_logical_plan: object` are typed as bare `object`. The actual type is `DataFusionLogicalPlan`; the `TYPE_CHECKING` import exists but the field types aren't updated. Callers must cast.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/bundle_assembly.py:239-240` — `_plan_core_components` casts the result of `safe_logical_plan()` back to `DataFusionLogicalPlan`, but `safe_logical_plan` returns `object | None`. The `cast()` call masks a real potential `None` that would propagate.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/bundle_artifact.py:59` — `execution_plan: object | None` — the `object` type here means callers cannot call any plan method without casting.

**Suggested improvement:**

Import `DataFusionLogicalPlan` from `datafusion.plan` inside `TYPE_CHECKING` blocks and
use it as the declared field type on `DataFusionPlanArtifact`. The `safe_logical_plan`
wrapper already guards; assert non-None inside `_plan_core_components` after the cast
to make the invariant explicit rather than masked.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS — Alignment: 1/3

**Current state:**

Two functions violate CQS by both computing state and emitting side effects:

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/bundle_assembly.py:229-267` — `_plan_core_components` extracts plan layers AND records per-phase timing to `logical_ms`, `optimized_ms`, `execution_ms`, `substrait_ms` fields inside the same function. The timing data is a side-effect measurement mixed with the query.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/result_types.py:355-397` — `validate_substrait_plan` both executes a DataFusion DataFrame (command: causes IO/compute) and returns the match status (query). The function name implies query semantics but it is an execution trigger.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/bundle_assembly.py:517-604` — `_collect_bundle_assembly_state` builds state and validates (side-effect: raises on UDF violations) in the same call.

**Suggested improvement:**

Split `_plan_core_components` into `_extract_plan_layers(ctx, df, options)` (pure query returning logical/optimized/physical/substrait) and `_time_plan_extraction(...)` (command that wraps the extraction in timing). For `validate_substrait_plan`, rename to `execute_substrait_validation` to signal its command semantics, or extract the execution into a separate `replay_and_collect(ctx, bytes)` that the caller can control. For `_collect_bundle_assembly_state`, split validation into a separate `_assert_bundle_requirements(state, options)` command.

**Effort:** medium
**Risk if unaddressed:** medium — CQS violations make caching and retry unsafe

---

### Category: Correctness (16-18)

#### P16. Functional Core, Imperative Shell — Alignment: 2/3

**Current state:**

The lineage extractor (`reporting.py`) is functionally designed with clearly pure helper
functions. Plan fingerprinting (`plan_fingerprint.py`) and identity (`plan_identity.py`)
are pure transforms. However, the walker layer introduces brittleness.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/lineage/reporting.py:186-198` — `_safe_attr` silently converts `RuntimeError`, `TypeError`, `ValueError` to `None`. When a DataFusion API changes, failures become silent `None` propagation rather than surfaced errors.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/walk.py:8-21` — `looks_like_plan` uses duck-typing on three attributes (`inputs`, `to_variant`, `display_indent`). If DataFusion renames any of these, plan walking silently produces empty traversals with no diagnostic signal.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/profiler.py:111-116` — `_run_explain_text` catches `BaseException` and checks `exc.__class__.__name__ == "PanicException"` by string comparison. This is a fragile cross-language boundary check; a Rust panic surfaced differently would fall through uncaught.

**Suggested improvement:**

Add a diagnostic counter or structured log emission when `_safe_attr` swallows an error
(at least count `None` returns per attribute name) so silent fallbacks become observable.
Replace the `"PanicException"` string check in `profiler.py` with an
`isinstance` check against the actual pyo3 exception type, or document and freeze the
fallback behavior clearly. Consider a `_WalkError` sentinel that can be detected by
callers versus a `None` that looks like "no data."

**Effort:** medium
**Risk if unaddressed:** low (current degradation is graceful) — medium if DF API evolves

---

#### P17. Idempotency — Alignment: 3/3

Plan bundle construction is idempotent by design: Substrait serialization is delegated to
the Rust bridge, fingerprints are deterministic hashes, and the cache stores by content hash.
Re-running `build_plan_artifact` with identical inputs produces identical results. No action needed.

---

#### P18. Determinism / Reproducibility — Alignment: 2/3

**Current state:**

The fingerprint logic in `plan_fingerprint.py` is well-designed and covers the full
environment surface. However, the Python lineage walker introduces non-determinism risks.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/walk.py:88-89` — `walk_logical_complete` deduplicates by `id(node)` (Python object identity). Object identity is an in-process address; if the same logical plan node is reconstructed across calls, deduplication behaves inconsistently. This is not a correctness risk today (plans are not reconstructed mid-walk) but is a latent fragility.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/lineage/reporting.py:552-557` — `_udf_name_map` constructs a case-folded map. If two UDFs differ only in case, the later one silently wins. This is not deterministic in a registry with case-conflicting names.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/planning_env.py:136-165` — `rulepack_snapshot` probes `ctx.state()` or `ctx.session_state()` by `getattr` and iterates `containers`, taking the last non-`None` source as the `"source"` label. The selection order is not fully deterministic when both `state` and `session_state` exist and return distinct objects.

**Suggested improvement:**

Replace `id(node)` deduplication with a structural identity derived from `to_variant()` type names (which are stable across calls). Enforce a uniqueness assertion on UDF registry names at registration time. Pin `rulepack_snapshot` to a single, prioritized lookup order with explicit fallback documentation.

**Effort:** medium
**Risk if unaddressed:** medium — plan fingerprints could diverge between calls if any of these edge cases trigger

---

### Category: Simplicity (19-22)

#### P19. KISS — Alignment: 1/3

**Current state:**

The most significant complexity issue: the Python lineage extractor (`reporting.py`, 569 LOC) substantially duplicates the Rust lineage extractor (`rust/codeanatomy_engine/src/compiler/lineage.rs`). Both implement plan traversal, scan extraction, pushed-filter capture, UDF name resolution, and column reference gathering.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/lineage/reporting.py:1-569` — full Python plan visitor with 25+ private helpers
- `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine/src/compiler/lineage.rs:1-60+` — parallel Rust implementation using DataFusion's native `TreeNode::apply` visitor
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/walk.py:74-109` — Python plan walker implementing preorder traversal that DataFusion's `LogicalPlan.inputs()` already provides recursively
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/lineage/scheduling.py:735-777` — post-hoc string re-parsing of filter expressions already rendered during lineage extraction

The Python walker operates via duck-typed `getattr` probing of DataFusion Python-binding objects, which creates a maintenance surface that must be kept in sync with every DataFusion Python API release. The Rust extractor uses strongly-typed plan IR directly.

**Suggested improvement:**

Route the `extract_lineage` call path through the Rust bridge (already used for Substrait). The Rust bridge's `build_plan_bundle_artifact_with_warnings` already returns `required_udfs` from `_required_udfs_from_rust_artifact`. Extend the bridge payload to return the full `LineageReport` in structured form. Retire the Python walker entirely, keeping `reporting.py`'s public types (`ScanLineage`, `JoinLineage`, `LineageReport`) as the Python-facing deserialization targets. This removes ~600 LOC of fragile `getattr`-chain code and aligns with the existing Rust delegation pattern.

**Effort:** large
**Risk if unaddressed:** medium — Python walker must be maintained in parallel with every DataFusion API change; probe failures are silent

---

#### P20. YAGNI — Alignment: 2/3

**Current state:**

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/cache.py:147-191` — `PlanCacheKey` carries 10 hash fields and `PlanCacheEntry` carries the same 10 fields plus `plan_bytes`. The `key()` method reconstructs a `PlanCacheKey` from the entry, meaning every field is stored twice. If the cache key composition changes, two structs must be updated.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/bundle_assembly.py:90-167` — six frozen dataclasses (`_BundleComponents`, `_BundleAssemblyState`, `_BundleIdentityResult`, `_PlanCoreComponents`, `_ExplainArtifacts`, `_EnvironmentArtifacts`, `_PlanArtifactsInputs`) are private to `bundle_assembly.py` and used only for the single assembly pipeline. This is well-structured but six distinct step-structs for one function pipeline is borderline for a module that never exposes them externally.

**Suggested improvement:**

Merge `PlanCacheKey` fields into `PlanCacheEntry` (they already overlap completely). Add a `as_cache_key_str(self) -> str` method directly on `PlanCacheEntry` to remove the round-trip. For the assembly dataclasses, consider whether `_BundleIdentityResult` (only two fields) and `_ExplainArtifacts` (only three fields) warrant their own types versus being inlined into `_BundleAssemblyState` directly.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least Astonishment — Alignment: 2/3

**Current state:**

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/profiler.py:64` — `capture_explain` checks `os.environ.get("CODEANATOMY_DISABLE_DF_EXPLAIN", "1") == "1"`. The **default is disabled** (default `"1"` means skip). A reader calling `capture_explain(df, verbose=False)` would be surprised to receive `None` in all environments that have not explicitly set the env var to `"0"`. The function name implies it captures; the default behavior is to not capture.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/walk.py:49-71` — `embedded_plans` iterates `variant.__dict__` to discover plan-like attributes. Iterating `__dict__` of a pyo3-backed object may include internal slots or cached values unrelated to plan structure. This is a latent surprise for contributors extending variant types.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/views/registry_specs.py:146-172` — `_semantic_cache_policy_for_row` uses a mix of dataset-binding lookup, runtime override, and feature-flag checks in a single function with four return paths. The policy precedence is non-obvious without reading the full function body.

**Suggested improvement:**

Flip the `CODEANATOMY_DISABLE_DF_EXPLAIN` default to `"0"` (enabled) and rename it
`CODEANATOMY_ENABLE_DF_EXPLAIN` to match the positive-default convention. For `embedded_plans`,
enumerate only the known variant attribute names (`subquery`, `plan`, `input`) rather
than probing `__dict__`. Document `_semantic_cache_policy_for_row` with an explicit
precedence comment: override > binding location > CDF flags > none.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and Version Public Contracts — Alignment: 2/3

**Current state:**

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/plan_identity.py:108` — `plan_identity_payload` embeds `"version": 4` as a hardcoded literal inside the returned mapping, not declared as a module-level constant or exported symbol. Callers cannot reference the version without parsing the payload.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/plan/bundle_artifact.py:181-188` — `__all__` re-exports `DeltaInputPin` and `PlanArtifacts` which are defined in `serde_artifacts`. Downstream callers may import from `bundle_artifact` and later discover these types moved. The re-export is undocumented.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/lineage/protocols.py` — clean Protocol contracts for `LineageScan`, `LineageJoin`, `LineageExpr`, `LineageQuery`. Well-structured; score uplift here.

**Suggested improvement:**

Declare `PLAN_IDENTITY_PAYLOAD_VERSION: int = 4` as a module-level constant in
`plan_identity.py` and reference it in `plan_identity_payload`. Add `# re-exported from serde_artifacts` comments on the `bundle_artifact.__all__` entries and consider whether the re-export should live in a dedicated compatibility shim.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Python Walker / Rust Lineage Parallelism

**Root cause:** The Rust compiler bridge was added after the Python lineage walker was
already established. The bridge was extended incrementally (first Substrait, then
`required_udfs`) but lineage extraction was never delegated.

**Affected principles:** P7 (DRY — two lineage implementations), P18 (Determinism —
`id(node)` deduplication), P19 (KISS — 600 LOC `getattr`-chain walker), P16 (functional
core — silent error swallowing in `_safe_attr`)

**Suggested approach:** Extend the Rust bridge payload to include the full `LineageReport`
JSON struct (already exists in `compiler/lineage.rs`). The Python types in
`reporting.py` become pure deserialization targets (msgspec decode) rather than live
plan visitors. This is the highest-impact single refactor available.

---

### Theme 2: Information-Schema Duplication

**Root cause:** `plan_introspection.py` was likely created as a copy of
`bundle_environment.py` during a refactor pass and never cleaned up.

**Affected principles:** P7 (DRY — 12 functions duplicated verbatim), P3 (SRP — two
modules with identical purpose)

**Suggested approach:** Delete `plan_introspection.py`; migrate the two unique helpers
(`function_registry_hash_for_context`, `suppress_introspection_errors`) into
`bundle_environment.py`. Single-pass fix with no behavioral change.

---

### Theme 3: Silent Error Swallowing

**Root cause:** The system is designed for graceful degradation (empty outputs, not
exceptions), which is correct. However, the current pattern swallows errors without
emitting any diagnostic signal, making it impossible to distinguish "no data" from
"error suppressed."

**Affected principles:** P16 (functional core — `_safe_attr` absorbs errors), P8 (contract
— callers cannot detect silent failures), P24 (observability — suppressed errors are
unobservable)

**Suggested approach:** Add a `_safe_attr_with_trace` variant that increments a
per-call counter (or emits a structured log at debug level) when it catches and swallows.
Apply this in the lineage walker hot path. The existing `suppress_introspection_errors()`
contextlib helper is a good model — extend it to track suppression counts.

---

## Rust Migration Candidates

| Component | Python Location | Rust Location | Migration Readiness | LOC Reduction | Assessment |
|-----------|----------------|---------------|---------------------|---------------|------------|
| Lineage extraction | `lineage/reporting.py` (569 LOC) | `compiler/lineage.rs` (exists) | High — Rust implementation exists | ~600 Python LOC | Extend bridge payload; retire Python walker |
| Plan walk | `plan/walk.py` (110 LOC) | DataFusion native `TreeNode::apply` | High — no custom Rust needed | ~110 Python LOC | Delegate to Rust extractor; remove need for Python walker |
| Filter string parsing | `lineage/scheduling.py:735-777` | `compiler/lineage.rs` (pushed_filters captured natively) | Medium — requires bridge extension | ~50 Python LOC | Captured as structured filters in Rust at extraction time |
| Plan normalization | `plan/normalization.py` (155 LOC) | Already handled in Rust substrait path | High — Substrait path normalizes before serialization | ~155 Python LOC | Verify Rust bridge handles all wrapper-stripping cases |
| Rulepack snapshot | `plan/planning_env.py:136-165` | `compiler/plan_bundle.rs` (rulepack fingerprint exists) | Medium — bridge returns `rulepack_fingerprint` bytes | ~30 Python LOC | Return rulepack names from bridge instead of probing Python API |

---

## DF52 Migration Impact

### Breaking changes affecting this scope

| DF52 Change | Python Location | Impact | Required Action |
|-------------|----------------|--------|-----------------|
| `CoalesceBatchesExec` removed | `plan/plan_utils.py:plan_display` (reads execution plan display) | Low — display text changes but parse is lenient | Verify `explain_rows_from_text` still matches new plan text format; no hardcoded `CoalesceBatches` strings found |
| Scan pushdown: `FileSource::with_projection` removed | `plan/walk.py:_SUBSTRAIT_WRAPPER_VARIANTS` / `normalization.py` | Medium — if any Python code assumes `FileScanConfig`-shaped projection in plan variants | Audit `_plan_variant_name` calls for `FileScanConfig`; the Rust substrait path is insulated |
| `FileScanConfig` statistics moved | `plan/signals.py:_extract_stats` reads `plan_details["statistics"]` | Low — stats key names may change | `_extract_stats` probes multiple key names (`num_rows`/`row_count`); lenient enough |
| `DFSchema.field()` returns `&FieldRef` | Not directly consumed in Python | None | N/A |
| Hash join dynamic filtering (DF52 B.1) | Python probe logic for join pushdown not present | Opportunity | DF52's built-in dynamic filtering could reduce need for custom `pushdown_probe_extract.rs`; assess after migration |
| `PhysicalOptimizerRule` API change | `plan/planning_env.py:rulepack_snapshot` reads `physical_optimizer_rules` | Low — attribute name unchanged but rule API changed | Verify `extract_rule_names(container, "physical_optimizer_rules")` still resolves; duck-typed probe is lenient |

### DF52 opportunities

- **Dynamic filtering (B.1):** DF52's hash-join build-side filter propagation and MIN/MAX
aggregate dynamic filters could make `pushdown_probe_extract.rs` partially redundant for
common join patterns. Assess `pushdown_probe_extract.rs` against DF52's native dynamic filter
capabilities.
- **Sort pushdown (D.2):** `signals.py:_extract_sort_keys` reads Sort-kind expressions from
lineage. If DF52's sort pushdown moves Sort nodes into scan providers, the lineage walker
may stop seeing them. Document this assumption.

---

## Planning-Object Consolidation

### Audit findings against the six consolidation criteria

| Criteria | Finding | Location | Severity |
|----------|---------|----------|----------|
| (a) Manual plan stitching | NOT found — plan extraction uses `df.logical_plan()`, `df.optimized_logical_plan()`, `df.execution_plan()` correctly | `plan/plan_utils.py:38-88` | None |
| (b) Reimplemented session setup | NOT found — session setup is delegated to `SessionRuntime` | — | None |
| (c) Custom plan introspection | FOUND — `walk.py` implements bespoke preorder traversal; `looks_like_plan` duck-types API shape | `plan/walk.py:8-109` | Medium |
| (d) Custom plan caching | FOUND — `plan/cache.py` wraps `diskcache` directly; DF52 has `statistics_cache()` and `list_files_cache()` for scan-level caching | `plan/cache.py:49-339` | Low — different cache scope |
| (e) Manual view registration | PARTIALLY — `registry_specs.py` uses `adapter.register_view(name, bundle.df, overwrite=True)` which is correct. `views/graph.py` manages topological registration order via BFS. No rogue `ctx.register_view` calls found outside adapter. Contract holds. | `views/registry_specs.py:401,564` | None |
| (f) Bespoke plan walker | FOUND — `walk.py` + `reporting.py` duplicate DataFusion's built-in `LogicalPlan.inputs()` traversal | `plan/walk.py:74-109`, `lineage/reporting.py:123-148` | Medium |
| (g) Custom optimizer rules | NOT found | — | None |

### Key consolidation conclusion

The `registry_specs.py` "sole registration authority" contract **holds**: all view
registrations go through `DataFusionIOAdapter.register_view`. The view graph `graph.py`
correctly uses `register_view_graph` as the single registration path. No rogue callers
found.

The plan-walker consolidation (criteria f) is the highest-impact consolidation candidate:
retiring `walk.py` and `reporting.py`'s visitor logic in favor of the Rust bridge
lineage path would remove the most code while eliminating the DF API surface dependency.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Delete `plan_introspection.py`; merge 2 unique helpers into `bundle_environment.py` | small | Eliminates 12 duplicated functions; single place to evolve introspection logic |
| 2 | P21 (Least astonishment) | Flip `CODEANATOMY_DISABLE_DF_EXPLAIN` default and rename | small | Explain capture works in standard environments without env var ceremony |
| 3 | P22 (Contracts) | Declare `PLAN_IDENTITY_PAYLOAD_VERSION = 4` as module constant | small | Enables callers to compare/detect version without parsing payload |
| 4 | P20 (YAGNI) | Merge `PlanCacheKey` fields into `PlanCacheEntry`; add `as_cache_key_str()` directly | small | Removes 10-field duplication between two cache structs |
| 5 | P8 (Contract) | Remove duplicate `compute_substrait` guard from `bundle_artifact.py:114`; keep only `bundle_assembly.py:249` | small | Single authoritative precondition check |

---

## Recommended Action Sequence

1. **(P7, small)** Delete `plan_introspection.py`. Migrate `function_registry_hash_for_context` and `suppress_introspection_errors` into `bundle_environment.py`. Update `planning_env.py:function_registry_artifacts` to call `bundle_environment.function_registry_hash_for_context`.

2. **(P21, small)** Rename `CODEANATOMY_DISABLE_DF_EXPLAIN` to `CODEANATOMY_ENABLE_DF_EXPLAIN` with a `"0"` default. Update `profiler.capture_explain` check accordingly.

3. **(P22 + P20, small)** Declare `PLAN_IDENTITY_PAYLOAD_VERSION`. Merge `PlanCacheKey` fields into `PlanCacheEntry`.

4. **(P8, small)** Remove the duplicate `compute_substrait` guard in `bundle_artifact.py:114-116`. Introduce `SubstraitValidationResult` struct to replace `Mapping[str, object]` return of `validate_substrait_plan`.

5. **(P11, medium)** Split `_plan_core_components` into pure extraction + timing wrapper. Rename `validate_substrait_plan` to `execute_substrait_validation`.

6. **(P18 + P16, medium)** Replace `id(node)` deduplication in `walk.py` with structural identity. Add diagnostic emission to `_safe_attr` when errors are swallowed.

7. **(P19 + P7, large)** Extend Rust bridge payload to return `LineageReport` as structured data. Retire Python walker (`walk.py`, `reporting.py` visitor logic). Retain Python types (`ScanLineage`, `JoinLineage`, `LineageReport`) as msgspec deserialization targets. This depends on step 6 to ensure error cases are observable during the transition.
