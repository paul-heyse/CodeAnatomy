# Design Review: CQ Language Lanes (tree-sitter, Python, Rust Extraction & Enrichment)

**Date:** 2026-02-16
**Scope:** `tools/cq/search/tree_sitter/` + `tools/cq/search/python/` + `tools/cq/search/rust/` + `tools/cq/search/enrichment/`
**Focus:** Knowledge (7-11), Boundaries (1-6), Quality (23-24)
**Depth:** moderate
**Files reviewed:** 20 of 77

## Executive Summary

The CQ language lane subsystem demonstrates strong architectural discipline in its Rust extraction tier, where a well-designed `RustNodeAccess` protocol successfully unifies ast-grep and tree-sitter backends behind shared extractors. However, the Python lane lacks an equivalent abstraction, leading to parallel but divergent enrichment paths. The most significant finding is the **triplication of `_find_ancestor` logic** across three modules and the **dual-implementation pattern for role classification** between the ast-grep and tree-sitter Rust lanes. The enrichment pipeline composition layer (`enrichment/core.py`) provides solid merge/budget primitives, but the gap-fill merging strategy produces payloads with weak structural contracts -- most payload shapes are `dict[str, object]` rather than typed structs. Overall alignment is good (average 2.1/3) with several medium-effort improvements that would reduce coupling and improve testability.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Private helpers exported in `__all__` (enrichment_extractors.py) |
| 2 | Separation of concerns | 2 | medium | medium | rust_lane/runtime.py mixes parse, query, enrichment, and payload assembly |
| 3 | SRP (one reason to change) | 1 | medium | medium | rust_lane/runtime.py (1081 LOC) has 6+ responsibilities |
| 4 | High cohesion, low coupling | 2 | medium | medium | Rust enrichment imports from 15 siblings in one file |
| 5 | Dependency direction | 2 | small | low | enrichment/ depends on pipeline/classifier for node resolution |
| 6 | Ports & Adapters | 3 | - | - | LanguageEnrichmentPort protocol cleanly separates adapters |
| 7 | DRY (knowledge) | 1 | medium | high | `_find_ancestor` triplicated; role classification duplicated |
| 8 | Design by contract | 2 | small | low | QueryWindowV1 validates invariants; most payloads lack validation |
| 9 | Parse, don't validate | 2 | medium | medium | Enrichment payloads built as `dict[str, object]` rather than typed structs |
| 10 | Make illegal states unrepresentable | 2 | medium | medium | EnrichmentStatus is typed; but payload shapes allow arbitrary keys |
| 11 | CQS | 2 | small | low | `_merge_capture_map` and `_apply_extractors` mutate + return implicitly |
| 23 | Design for testability | 2 | medium | medium | Module-level caches (lru_cache, BoundedCache) hard to reset between tests |
| 24 | Observability | 2 | small | low | Consistent logger.warning for degradation; but no structured telemetry contract |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The module boundaries are generally clean, with `__all__` declarations in every module reviewed. However, some modules export private-prefixed symbols through `__all__`.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/enrichment_extractors.py:76-86`: `__all__` exports 7 names prefixed with `_` (e.g., `_extract_attributes_dict`, `_extract_call_target`). These are consumed by `runtime.py` via direct imports of private names, violating the convention that `_`-prefixed names are internal.
- `tools/cq/search/tree_sitter/rust_lane/role_classification.py:176`: Exports `_classify_item_role` as the sole public entry but uses underscore prefix.
- `tools/cq/search/tree_sitter/rust_lane/fact_extraction.py`: Similarly, the runtime.py file imports 6 private-prefixed functions from this module.

**Suggested improvement:**
Rename the exported functions in `enrichment_extractors.py` and `role_classification.py` to drop the underscore prefix for symbols intended as the module's public surface. For example: `_extract_function_signature` becomes `extract_function_signature`, `_classify_item_role` becomes `classify_item_role`. The current naming signals "do not use" while the module explicitly exports and depends on these symbols.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The Python enrichment pipeline in `extractors.py` demonstrates good separation with distinct stage functions (`_run_ast_grep_stage`, `_run_python_ast_stage`, `_run_import_stage`, `_run_python_resolution_stage`, `_run_tree_sitter_stage`). However, the Rust tree-sitter runtime conflates multiple concerns.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`: This 1081-line file handles: (a) availability checking (lines 200-222), (b) node/scope tree traversal (lines 230-296), (c) enrichment payload building (lines 308-588), (d) query pack execution orchestration (lines 596-678), (e) query window planning (lines 681-727), (f) query collection (lines 730-759), (g) telemetry aggregation (lines 846-873), (h) payload attachment with msgspec serialization (lines 876-917), and (i) two public entry points with similar control flow (lines 924-1070).

**Suggested improvement:**
Extract the node/scope utilities (`_scope_name`, `_scope_chain`, `_find_scope`, `_find_ancestor`) into a shared module (they already exist in `extractors_shared.py` via `RustNodeAccess` -- the tree-sitter variants should use the shared versions via `TreeSitterRustNodeAccess` adapters). Extract query pack orchestration (`_collect_query_pack_captures`, `_collect_query_bundle`) into a dedicated `query_orchestration.py` module. Extract telemetry aggregation and payload attachment into a `payload_assembly.py` module.

**Effort:** medium
**Risk if unaddressed:** medium -- adding new extractors or query packs requires understanding 1000+ lines of interleaved concerns.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`rust_lane/runtime.py` changes when: query execution semantics change, tree-sitter API evolves, enrichment field schema changes, telemetry format changes, injection processing changes, or diagnostic collection changes. This violates single-reason-to-change.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`: Contains 8 internal dataclass types (`_RustPackRunResultV1`, `_RustPackAccumulatorV1`, `_RustQueryPackArtifactsV1`, `_RustQueryCollectionV1`, `_RustQueryExecutionPlanV1`, and 3 implicit via imports) plus 30+ functions. The `_attach_query_pack_payload` function alone (lines 876-917) performs msgspec serialization, tag summary computation, macro expansion request extraction, and injection runtime processing.
- `tools/cq/search/python/extractors.py` at 601 lines is similarly large but already decomposed into stage functions and delegates to sub-modules (`extractors_analysis.py`, `extractors_classification.py`, `extractors_structure.py`), demonstrating a better pattern.

**Suggested improvement:**
Follow the Python lane decomposition pattern for Rust: split `runtime.py` into `rust_lane/enrichment_builder.py` (payload building), `rust_lane/query_orchestration.py` (pack execution and collection), and `rust_lane/payload_assembly.py` (telemetry, attachment, serialization). Keep `runtime.py` as the thin public entry point that composes these.

**Effort:** medium
**Risk if unaddressed:** medium -- maintenance burden grows linearly with new query packs or enrichment fields.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The `RustNodeAccess` protocol in `rust/node_access.py` is a strong cohesion boundary -- it concentrates node-access semantics in one place with two concrete adapters. However, the Rust runtime has excessive import fan-in.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:10-87`: Imports from 18 distinct modules across 5 package subtrees. This high fan-in creates fragile dependency chains where changes in any of these modules may require runtime.py attention.
- `tools/cq/search/enrichment/core.py`: Well-focused at 194 LOC with narrow interface (10 functions). Good cohesion example.

**Suggested improvement:**
Reduce the import footprint of `runtime.py` by extracting intermediate modules that aggregate related imports. For example, a `rust_lane/query_infrastructure.py` could re-export `compile_query`, `has_custom_predicates`, `make_query_predicate`, `QueryPackExecutionContextV1`, `execute_pack_rows_with_matches`, reducing 5 imports to 1.

**Effort:** medium
**Risk if unaddressed:** medium -- import chain breakage from tree-sitter API changes cascades widely.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Core extraction logic generally depends inward (shared extractors -> node access protocol). However, one concerning inward dependency exists.

**Findings:**
- `tools/cq/search/python/extractors.py:1357`: `enrich_python_context_by_byte_range` imports `from tools.cq.search.pipeline.classifier import get_node_index` -- the enrichment tier (lower-level) depends on the pipeline tier (higher-level). This creates a circular dependency potential and means the enrichment layer cannot be used without the pipeline.
- `tools/cq/search/tree_sitter/core/lane_support.py:46`: Contains a deferred import to avoid circular dependency (`from tools.cq.search.tree_sitter.core.change_windows import ...`), suggesting the dependency graph has known pressure points.

**Suggested improvement:**
Inject the `get_node_index` dependency into `enrich_python_context_by_byte_range` rather than importing it. Pass a `node_resolver: Callable[[Path, SgRoot, str], NodeIndex]` parameter, or restructure so the caller (in pipeline) resolves the node before calling the enrichment function.

**Effort:** small
**Risk if unaddressed:** low -- the deferred import works, but it prevents the enrichment module from being independently testable.

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
The `LanguageEnrichmentPort` protocol in `enrichment/contracts.py` is a clean port definition. The `language_registry.py` provides lazy adapter registration. `PythonEnrichmentAdapter` and `RustEnrichmentAdapter` implement the port for their respective languages.

**Findings:**
- `tools/cq/search/enrichment/contracts.py:42-60`: `LanguageEnrichmentPort` defines three methods (`payload_from_match`, `accumulate_telemetry`, `build_diagnostics`) that form a minimal, sufficient interface for cross-language enrichment integration.
- `tools/cq/search/enrichment/language_registry.py:16-23`: `_ensure_defaults()` lazily registers Python and Rust adapters on first access -- clean deferred initialization.
- `tools/cq/search/rust/node_access.py:13-35`: `RustNodeAccess` protocol + `SgRustNodeAccess`/`TreeSitterRustNodeAccess` adapters is a textbook Ports & Adapters implementation for unifying two AST backends.

**Suggested improvement:**
No action needed. This is the strongest design pattern in the reviewed scope.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
This is the most significant alignment gap in the review scope. Three categories of knowledge duplication exist.

**Findings:**

**Finding 1: `_find_ancestor` triplicated.**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:272-296`: `_find_ancestor` for tree-sitter `Node` objects.
- `tools/cq/search/tree_sitter/rust_lane/role_classification.py:51-75`: Identical `_find_ancestor` implementation for tree-sitter `Node` objects.
- `tools/cq/search/rust/extractors_shared.py:96-111`: `find_ancestor` for `RustNodeAccess` protocol objects.
All three encode the same invariant: "walk up from node, match kind, respect max_depth." The tree-sitter versions operate on raw `Node` objects instead of going through the `RustNodeAccess` adapter, bypassing the unification that `node_access.py` was designed to provide.

**Finding 2: Role classification duplicated.**
- `tools/cq/search/tree_sitter/rust_lane/role_classification.py:78-172`: `_classify_function_role`, `_classify_call_role`, `_classify_item_role` for tree-sitter nodes with `_ITEM_ROLE_SIMPLE` mapping.
- `tools/cq/search/rust/enrichment.py:179-235`: `_classify_item_role`, `_classify_function_role`, `_classify_call_like_role` for ast-grep nodes with `_NON_FUNCTION_ROLE_BY_KIND` mapping.
These encode the same semantic truth ("Rust items have roles based on their kind, attributes, and ancestor context") but diverge in details: `role_classification.py` maps `const_item -> "const_item"` and `type_item -> "type_alias"` while `enrichment.py` omits these. The `_ITEM_ROLE_SIMPLE` dict contains 7 entries while `_NON_FUNCTION_ROLE_BY_KIND` contains 3 -- drift risk is high.

**Finding 3: Scope chain construction duplicated.**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:244-258`: `_scope_chain` for tree-sitter `Node`.
- `tools/cq/search/rust/extractors_shared.py:114-132`: `scope_chain` for `RustNodeAccess`.
Same algorithm, same scope kinds, same output format, but operating on different node types.

**Suggested improvement:**
For findings 1 and 3: All tree-sitter lane operations that need ancestor/scope traversal should use `TreeSitterRustNodeAccess` adapters and call the shared `extractors_shared` functions, eliminating the raw `Node` variants. This is the exact pattern `enrichment_extractors.py` already uses (wrapping `Node` in `TreeSitterRustNodeAccess` and delegating to `_shared` functions).

For finding 2: Extract role classification into a single `classify_rust_item_role(node: RustNodeAccess, attributes: list[str], *, max_scope_depth: int) -> str` function in `extractors_shared.py`, merging the two role-mapping dictionaries. Both `enrichment.py` and `role_classification.py` should import and delegate to this single authority.

**Effort:** medium
**Risk if unaddressed:** high -- the role classification tables have already started to drift, and there is no test that cross-checks them.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Some contracts are well-specified. `QueryWindowV1.__post_init__` validates byte ordering. `QueryExecutionSettingsV1` uses `Literal` types for `window_mode`. The `ENRICHMENT_ERRORS` tuple provides a canonical fail-open contract.

**Findings:**
- `tools/cq/search/_shared/error_boundaries.py:5-14`: `ENRICHMENT_ERRORS` is a single-source-of-truth tuple of 8 exception types. Every enrichment function wraps its body in `except ENRICHMENT_ERRORS`. This is clean and consistent.
- `tools/cq/search/tree_sitter/contracts/core_models.py:55-69`: `QueryWindowV1.__post_init__` enforces `start_byte <= end_byte`. Good.
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:924-995`: `enrich_rust_context` accepts `line: int` (1-based) and `col: int` (0-based) but the only precondition check is `line < 1 or col < 0`. The docstring documents the convention but no runtime enforcement converts an accidentally 0-based line to an error.
- Enrichment payloads lack post-condition validation. The `_build_enrichment_payload` function returns `dict[str, object]` with no schema assertion that required keys (`language`, `enrichment_status`, `enrichment_sources`) are present.

**Suggested improvement:**
Add a lightweight post-condition check to `_build_enrichment_payload` that asserts the required metadata keys are present before returning. This could be a simple `assert` in debug mode or a validation function called at the payload boundary.

**Effort:** small
**Risk if unaddressed:** low -- the keys are always set in practice, but silent omission during refactoring would cause downstream failures.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The tree-sitter contract layer (`contracts/core_models.py`) properly defines typed structs using `msgspec.Struct` (via `CqStruct`). However, the enrichment pipeline internally operates on `dict[str, object]` throughout, deferring structural parsing to the very end.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:573-578`: `_build_enrichment_payload` returns `dict[str, object]`. The entire enrichment building chain operates on untyped dicts, with fields added imperatively via `payload.update(...)` and `payload["key"] = value`.
- `tools/cq/search/enrichment/python_facts.py:143-158` and `tools/cq/search/enrichment/rust_facts.py:65-73`: `PythonEnrichmentFacts` and `RustEnrichmentFacts` are well-designed typed structs that model the full enrichment schema. But `enrichment/core.py:57-82` shows that `parse_python_enrichment` and `parse_rust_enrichment` convert `dict -> typed struct` as a terminal operation, meaning most of the pipeline never benefits from the type safety.
- `tools/cq/search/enrichment/core.py:334-360`: `normalize_python_payload` creates a `PythonEnrichmentPayload` wrapper but immediately destructures it back into a dict for output.

**Suggested improvement:**
Push the boundary of typed representation earlier in the pipeline. Instead of building payloads as `dict[str, object]` and converting at the end, define builder types (e.g., `RustEnrichmentPayloadBuilder` with typed fields and `.build() -> RustEnrichmentPayload` method) that enforce the schema throughout the construction process. Start with the Rust lane where the payload shape is simpler.

**Effort:** medium
**Risk if unaddressed:** medium -- schema drift between producers and consumers is only caught at runtime when `msgspec.convert` fails.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Good use of `Literal` types for enumerated states and frozen dataclasses for immutable query structures. Some gaps in payload modeling.

**Findings:**
- `tools/cq/search/enrichment/contracts.py:13`: `EnrichmentStatus = Literal["applied", "degraded", "skipped"]` -- prevents invalid status strings at the type level.
- `tools/cq/search/tree_sitter/contracts/core_models.py:90-94`: `window_mode: Literal["intersection", "containment_preferred", "containment_required"]` -- clean enumeration.
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:106-113`: `_RustPackRunResultV1` uses `query: object` and `capture_telemetry: object` -- these could be properly typed to prevent passing wrong object types.
- `tools/cq/search/enrichment/contracts.py:28-39`: `PythonEnrichmentPayload` and `RustEnrichmentPayload` use `data: dict[str, object]` as a catch-all. The typed fact structs in `python_facts.py` and `rust_facts.py` exist but aren't used as the payload container.

**Suggested improvement:**
Type the `object` fields in `_RustPackRunResultV1` (e.g., `capture_telemetry: QueryExecutionTelemetryV1`). Consider replacing the `data: dict[str, object]` in the payload wrappers with the corresponding facts struct union or mapping.

**Effort:** medium
**Risk if unaddressed:** medium -- weak typing allows payload corruption to propagate silently.

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most functions follow CQS. However, several extractor-composition functions both mutate and return.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:409-416`: `_apply_extractors` mutates `payload` in place and also appends to `reasons` -- this is a command, which is fine. But `_merge_result` at line 387 both mutates `payload` via `payload.update(fields)` and mutates `reasons` via `reasons.append(reason)`. The caller pattern `_merge_result(payload, reasons, _try_extract(...))` reads like a query but performs two mutations.
- `tools/cq/search/python/extractors.py:663-694`: `_collect_extract` mutates `payload` AND `degrade_reasons` AND returns the raw result dict. This three-way mixed operation makes reasoning about state difficult.
- `tools/cq/search/tree_sitter/core/runtime.py:370-388`: `_merge_capture_map` mutates `merged` in place (command). Clean.

**Suggested improvement:**
For `_collect_extract`: separate the mutation from the return. Either return a structured result that the caller merges, or drop the return value since callers only use it in one place (`_enrich_ast_grep_core` checks `role_result.get("item_role")`).

**Effort:** small
**Risk if unaddressed:** low -- the pattern works but adds cognitive load during debugging.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The `RustNodeAccess` protocol enables clean test doubles. The `ENRICHMENT_ERRORS` boundary enables predictable fail-open testing. However, module-level caches create test isolation challenges.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/runtime_cache.py:29-30`: `_TREE_CACHE` is a module-level `BoundedCache` and `_TREE_CACHE_EVICTIONS` is a module-level dict. `clear_tree_sitter_rust_cache()` exists but must be called manually between tests.
- `tools/cq/search/python/extractors.py:257-259`: `_AST_CACHE` is a module-level `BoundedCache`. `clear_python_enrichment_cache()` exists.
- `tools/cq/search/tree_sitter/python_lane/runtime.py:95-101`: `@lru_cache(maxsize=1)` on `_python_language()`. While `clear_tree_sitter_python_cache` calls `.cache_clear()`, this requires knowledge of which caches to clear.
- `tools/cq/search/tree_sitter/python_lane/facts.py:235-257`: `@lru_cache(maxsize=1)` on `_pack_source_rows()` -- cached query compilation results. No corresponding `clear_*` function for the facts module.
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:200-222`: `is_tree_sitter_rust_available()` performs availability checking via module imports. Not injectable, so tests must ensure the actual library is available.

**Suggested improvement:**
Consolidate cache management. Rather than scattering `lru_cache` and `BoundedCache` across modules with per-module clear functions, introduce a `CacheRegistry` that tracks all caches for a given language lane. Provide a single `clear_all_caches("rust")` entry point. For `is_tree_sitter_rust_available`, inject the availability check via a parameter or make it a testable property of a session object.

**Effort:** medium
**Risk if unaddressed:** medium -- cache leakage between tests causes flaky test failures.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Consistent use of `logging.getLogger(__name__)` and `logger.warning(...)` for degradation events. The telemetry structs (`QueryExecutionTelemetryV1`, `ParseSessionStatsV1`) provide structured observability data.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:991-993`: `logger.warning("Rust context enrichment failed: %s", type(exc).__name__)` -- consistent degradation logging.
- `tools/cq/search/python/extractors.py:1079-1083`: Stage timings recorded in `state.stage_timings_ms` -- structured performance tracking.
- `tools/cq/search/tree_sitter/core/runtime.py:510-520`: Telemetry struct captures `windows_total`, `windows_executed`, `capture_count`, `exceeded_match_limit`, `cancelled`, `window_split_count`, `degrade_reason` -- comprehensive.
- Gap: The Rust lane lacks the stage-timing pattern used by the Python enrichment pipeline. There is no equivalent of `stage_timings_ms` in the Rust enrichment path, making it harder to identify slow extractors.
- Gap: Degradation reasons use free-form strings (`f"{label}: {exc}"`) rather than an enumerated set. This makes aggregation in dashboards difficult.

**Suggested improvement:**
Add stage timings to the Rust enrichment path, mirroring the Python pattern. Define a `DegradeReasonKind` enum or `Literal` type to constrain degradation reason strings for dashboard aggregation.

**Effort:** small
**Risk if unaddressed:** low -- operational debugging is harder but not blocked.

---

## Cross-Cutting Themes

### Theme 1: Incomplete Adapter Adoption for Tree-Sitter Nodes

**Root cause:** The `RustNodeAccess` protocol and `TreeSitterRustNodeAccess` adapter were designed to unify ast-grep and tree-sitter node access. The `enrichment_extractors.py` module adopts this pattern (wrapping tree-sitter nodes before delegating to shared extractors). However, `runtime.py` and `role_classification.py` bypass the adapter entirely and operate directly on tree-sitter `Node` objects, duplicating logic that already exists in `extractors_shared.py`.

**Affected principles:** P7 (DRY), P4 (coupling), P2 (separation of concerns).

**Suggested approach:** Complete the adapter adoption by having `runtime.py` and `role_classification.py` use `TreeSitterRustNodeAccess` wrappers for all scope/ancestor traversal and role classification. This eliminates the duplicated `_find_ancestor`, `_scope_chain`, and role classification code.

### Theme 2: Python Lane Lacks Equivalent Abstraction Protocol

**Root cause:** The Rust lane has `RustNodeAccess` to unify ast-grep (`SgRustNodeAccess`) and tree-sitter (`TreeSitterRustNodeAccess`). The Python lane has no equivalent. `extractors.py` operates on `SgNode` via ast-grep, while `python_lane/facts.py` operates on tree-sitter `Node`. There is no `PythonNodeAccess` protocol to unify them, meaning the Python extractors cannot be reused across backends.

**Affected principles:** P7 (DRY), P6 (Ports & Adapters), P13 (composition over inheritance).

**Suggested approach:** This is a future design seam rather than an immediate need. The Python lane's ast-grep and tree-sitter tiers operate at different granularities (statement-level vs. fact-list-level), so a unified protocol may not provide the same benefit as the Rust one. Monitor whether parallel extractors emerge before investing.

### Theme 3: Payload Schemas Exist but Are Consumed Too Late

**Root cause:** The typed enrichment facts (`PythonEnrichmentFacts`, `RustEnrichmentFacts`) and wrapper structs (`PythonEnrichmentPayload`, `RustEnrichmentPayload`) are well-designed but only used as terminal converters. The entire pipeline operates on `dict[str, object]`, losing type safety until the very end.

**Affected principles:** P9 (parse, don't validate), P10 (illegal states), P8 (contracts).

**Suggested approach:** Introduce an intermediate builder pattern that constructs payloads through typed fields. Start with the simpler Rust payload (`RustEnrichmentFacts` has only 3 sub-structs). This would be a medium-effort investment that pays off in catching schema drift during development rather than at runtime.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Eliminate `_find_ancestor` duplication in `role_classification.py` and `runtime.py` by using `TreeSitterRustNodeAccess` + `extractors_shared.find_ancestor` | small | Removes 25 lines of duplicated code and eliminates divergence risk |
| 2 | P1 (Info hiding) | Remove underscore prefix from public exports in `enrichment_extractors.py` and `role_classification.py` | small | Aligns naming with actual usage; reduces confusion |
| 3 | P24 (Observability) | Add stage-timing telemetry to Rust enrichment path matching Python's pattern | small | Enables performance diagnosis across both language lanes |
| 4 | P8 (Contract) | Add post-condition assertion for required metadata keys in enrichment payload builders | small | Catches missing keys during development |
| 5 | P7 (DRY) | Unify role classification into `extractors_shared.py` with single `_ITEM_ROLE_MAP` | medium | Eliminates the highest-risk drift surface (7 vs 3 role mappings) |

## Recommended Action Sequence

1. **[P7, small] Adopt `TreeSitterRustNodeAccess` in `role_classification.py`**: Replace the local `_find_ancestor` with the shared version via adapter wrapping. This is a contained change with no external API impact and eliminates the most obvious duplication. Reference: `tools/cq/search/tree_sitter/rust_lane/role_classification.py:51-75`.

2. **[P7, small] Adopt `TreeSitterRustNodeAccess` in `runtime.py` for scope utilities**: Replace `_find_ancestor`, `_find_scope`, `_scope_chain`, `_scope_name` in `runtime.py` with calls through the adapter to `extractors_shared`. Reference: `tools/cq/search/tree_sitter/rust_lane/runtime.py:230-296`.

3. **[P7, medium] Unify role classification**: Create `classify_rust_item_role` in `extractors_shared.py` that merges the `_ITEM_ROLE_SIMPLE` (from `role_classification.py`) and `_NON_FUNCTION_ROLE_BY_KIND` (from `enrichment.py`) mappings. Both callsites delegate to this single authority. Reference: `tools/cq/search/tree_sitter/rust_lane/role_classification.py:23-31` and `tools/cq/search/rust/enrichment.py:198-202`.

4. **[P1, small] Fix public API naming**: Remove underscore prefixes from exported symbols in `enrichment_extractors.py` and `role_classification.py`. Update imports in `runtime.py` accordingly.

5. **[P3, medium] Decompose `runtime.py`**: Following step 2, `runtime.py` will have fewer internal utility functions. Extract query pack orchestration and payload assembly into separate modules, reducing the file from 1081 to approximately 400 lines of entry-point and enrichment-building logic.

6. **[P9, medium] Push typed boundaries earlier**: After step 5, the payload building functions are isolated in their own module. Introduce a typed builder that constructs `RustEnrichmentFacts` directly rather than building `dict[str, object]` and converting at the end.

7. **[P23, medium] Consolidate cache lifecycle**: Create a per-language `CacheRegistry` that tracks all `lru_cache` and `BoundedCache` instances, providing a single `clear_all()` entry point for test teardown. Reference: scattered caches in `runtime_cache.py`, `runtime.py`, `extractors.py`, `facts.py`.
