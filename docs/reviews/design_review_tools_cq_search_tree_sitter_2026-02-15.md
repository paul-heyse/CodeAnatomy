# Design Review: tools/cq/search/tree_sitter

**Date:** 2026-02-15
**Scope:** `tools/cq/search/tree_sitter/`
**Focus:** All principles (1-24)
**Depth:** moderate
**Files reviewed:** 20 of 46

## Executive Summary

The tree-sitter subsystem exhibits strong architectural layering with a well-defined contracts layer, clear dependency direction from contracts through core into language lanes, and robust fail-open design throughout. The primary weaknesses are **semantic duplication across Python and Rust lanes** (duplicated `_build_query_windows`, `_lift_anchor`, `_make_parser`, `_parse_tree`, `_pack_source_rows`, `_python_field_ids`, `NodeLike` protocol, `_normalize_semantic_version`, and `_ENRICHMENT_ERRORS`), the **Rust lane runtime being a 1,976-line monolith** that handles scope traversal, extraction, classification, injection, tags, and payload assembly, and **five independent `NodeLike` protocol definitions** that could be consolidated. The contracts layer and bounded-execution infrastructure are notably well designed.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Internal helpers well-prefixed with `_`; but `_TREE_CACHE` mutable dict exposed as module-level state in rust_lane/runtime.py |
| 2 | Separation of concerns | 1 | large | medium | rust_lane/runtime.py mixes parsing, scope traversal, extraction, classification, injection, tags, and payload assembly in one file |
| 3 | SRP (one reason to change) | 1 | large | medium | rust_lane/runtime.py changes for any of 7+ reasons; python_lane has better separation (facts.py vs runtime.py) |
| 4 | High cohesion, low coupling | 2 | medium | low | Sub-packages are well-cohesive; but lanes are tightly coupled to many core modules each |
| 5 | Dependency direction | 3 | - | - | Clean contracts -> core -> lanes -> query -> structural layering respected throughout |
| 6 | Ports & Adapters | 2 | medium | low | Language dispatch uses if/elif chains; no formal port/adapter for language-specific behavior |
| 7 | DRY (knowledge) | 0 | medium | high | 6+ duplicated functions across python/rust lanes; 5 independent NodeLike protocols; 3 copies of _normalize_semantic_version |
| 8 | Design by contract | 3 | - | - | All public contracts are frozen msgspec structs with explicit defaults; QueryExecutionSettingsV1 clearly bounds execution |
| 9 | Parse, don't validate | 2 | small | low | Good at boundary: decode_yaml_strict for YAML; but enrichment payloads are untyped dict[str, object] internally |
| 10 | Make illegal states unrepresentable | 2 | small | low | Frozen structs enforce immutability; QueryWindowV1 does not enforce start_byte < end_byte at construction |
| 11 | CQS | 2 | small | low | record_runtime_sample is a clean command; runtime_snapshot is a clean query; _touch_tree_cache mixes mutation with no return |
| 12 | Dependency inversion | 2 | medium | low | query_pack_executor delegates to runtime via lazy imports; but lanes directly construct parsers instead of accepting them |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; all behavior composed via functions and frozen structs |
| 14 | Law of Demeter | 2 | small | low | _touch_tree_cache accesses session._entries internal dict; most code respects collaborator boundaries |
| 15 | Tell, don't ask | 2 | small | low | Good in extractors (_try_extract pattern); but lots of getattr probing on tree-sitter node objects |
| 16 | Functional core, imperative shell | 2 | small | low | Pattern scoring, plan building are pure; adaptive_runtime mixes IO (diskcache) with computation |
| 17 | Idempotency | 3 | - | - | LRU-cached functions idempotent; parse sessions reuse cached trees; re-running produces same results |
| 18 | Determinism | 3 | - | - | Pattern scoring deterministic; parallel execution preserves input order; match dedup via fingerprinting |
| 19 | KISS | 2 | small | low | Core runtime is well-factored; rust_lane/runtime.py complexity is above threshold at ~1,976 LOC |
| 20 | YAGNI | 2 | small | low | _FIELD_GROUPS dict in rust_lane/runtime.py defined but never used for validation at runtime |
| 21 | Least astonishment | 2 | small | low | canonicalize_*_lane_payload mutates input dict via pop(); unexpected side effect for a "canonicalize" function |
| 22 | Public contracts | 3 | - | - | All contracts versioned (V1 suffix), frozen, explicit __all__ exports on every module |
| 23 | Design for testability | 2 | medium | low | Pure extractors testable; but parser factory hardcoded in lanes, no DI seam for parser construction |
| 24 | Observability | 2 | small | low | QueryExecutionTelemetryV1 comprehensive; parse session stats good; but no structured logging in the subsystem |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Internal helpers are consistently prefixed with `_` across all modules. The `contracts/__init__.py` provides a clean re-export surface. However, module-level mutable state is exposed.

**Findings:**
- `rust_lane/runtime.py:118-119`: `_TREE_CACHE: OrderedDict[str, None]` and `_TREE_CACHE_EVICTIONS: dict` are module-level mutable state that could be accessed by any importer. While prefixed with `_`, this is mutable shared state that leaks cache implementation decisions.
- `adaptive_runtime.py:30-31`: `_LAST_DRIFT_REPORTS` in `query/registry.py:30` is a mutable module-level dict used as a side-channel to communicate drift reports, bypassing the function return path.

**Suggested improvement:**
Encapsulate `_TREE_CACHE` and `_TREE_CACHE_EVICTIONS` into a small frozen-friendly cache wrapper class within `rust_lane/runtime.py`. Move `_LAST_DRIFT_REPORTS` into a function-scoped mechanism or return drift reports as part of the `load_query_pack_sources` return type.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
The Python lane splits responsibilities well across `facts.py` (fact extraction), `runtime.py` (parsing/enrichment), `fallback_support.py` (degradation), and `locals_index.py` (scope indexing). The Rust lane concentrates everything in a single 1,976-line `runtime.py`.

**Findings:**
- `rust_lane/runtime.py` handles at least 7 distinct concerns: (1) parser management (lines 276-327), (2) scope traversal (lines 503-569), (3) signature extraction (lines 579-678), (4) visibility/attribute extraction (lines 681-763), (5) impl context extraction (lines 766-803), (6) struct/enum shape extraction (lines 866-935), (7) item role classification (lines 942-1073), (8) query pack orchestration (lines 1413-1504), (9) fact list building (lines 1507-1644), (10) module/import row building (lines 1647-1721), and (11) macro expansion request assembly (lines 1724-1752).
- By contrast, the Python lane separates these into `facts.py` (630 LOC), `runtime.py` (687 LOC), `fallback_support.py` (353 LOC), and `locals_index.py` (130 LOC).

**Suggested improvement:**
Extract the Rust lane into parallel modules mirroring the Python lane structure: `rust_lane/facts.py` (extraction logic), `rust_lane/classification.py` (item role classification), `rust_lane/scope.py` (scope chain traversal). Keep `rust_lane/runtime.py` as the thin orchestration entry point.

**Effort:** large
**Risk if unaddressed:** medium -- the monolithic file makes targeted changes risky and review difficult.

---

#### P3. SRP -- Alignment: 1/3

**Current state:**
Most modules have a single clear reason to change. The exception is `rust_lane/runtime.py`, which changes for parser API changes, scope traversal logic, Rust grammar changes, extraction field additions, classification rules, injection integration, tags integration, or payload format evolution.

**Findings:**
- `rust_lane/runtime.py` would need modification for any of: tree-sitter API changes, Rust grammar updates, new enrichment field groups, classification rule changes, injection profile additions, tag event schema changes, or macro expansion request format changes.
- `python_lane/runtime.py:591-677` (`enrich_python_context_by_byte_range`) is a 86-line orchestrator that assembles parsing, windowing, diagnostics, capture fields, and fallback resolution -- this is acceptable as orchestration, but the file also contains parsing primitives (`_parse_tree`, `_make_parser`) that could live in the shared core.

**Suggested improvement:**
As per P2 above, decompose `rust_lane/runtime.py`. Additionally, move `_make_parser` and `_parse_tree` from both `python_lane/runtime.py` and `rust_lane/runtime.py` into `core/infrastructure.py` where `make_parser` already exists, eliminating the lane-specific duplicates.

**Effort:** large
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Sub-packages are internally cohesive. The contracts package groups all data models. The core package groups runtime execution primitives. However, lane runtimes have high fan-out to many core modules.

**Findings:**
- `python_lane/facts.py` imports from 12 internal modules (lines 12-48). This is acceptable given its role as an orchestrator but indicates the module does significant assembly work.
- `rust_lane/runtime.py` imports from 17 internal modules (lines 23-69), which is notably high and reflects its monolithic nature.
- The `contracts/__init__.py` re-export layer (89 LOC) provides good cohesive access for external consumers.

**Suggested improvement:**
Reduce `rust_lane/runtime.py` import count by extracting sub-concerns into dedicated modules (see P2). Consider a `python_lane/core.py` that provides a shared "lane context" to reduce the fan-out in `facts.py`.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
Dependencies flow cleanly inward: `contracts/` has zero internal dependencies beyond `tools.cq.core.structs`. `core/` depends on contracts and schema. Lanes depend on core and contracts. Query modules depend on contracts. This matches the documented `contracts -> core -> lanes -> query -> structural` hierarchy.

**Findings:**
- No reverse dependencies detected. Core modules never import from lanes. Contracts never import from core.
- Lazy imports are used strategically to break import-time cycles (e.g., `diagnostics.py:149` imports `run_bounded_query_matches` inside the function body, and `query_pack_executor.py:44` lazy-imports from `core.runtime`).

**Suggested improvement:**
No action needed. Well aligned.

**Effort:** -
**Risk if unaddressed:** -

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
Language dispatch uses hardcoded if/elif chains in `language_registry.py:227-234` (`load_tree_sitter_language`). Lane selection is not formalized -- callers know whether to call Python or Rust lane functions directly.

**Findings:**
- `language_registry.py:227-234`: Language loading is a simple if/elif on `"python"` and `"rust"`. Adding a third language requires modifying this function.
- No formal `LanguageLane` protocol exists. The Python lane exposes `build_python_tree_sitter_facts` and `enrich_python_context_by_byte_range`; the Rust lane exposes `enrich_rust_context` and `enrich_rust_context_by_byte_range`. These have different signatures, making generic dispatch impossible.

**Suggested improvement:**
This is acceptable for a two-language system per YAGNI. If a third language lane is added, introduce a `LanguageLaneProtocol` with a common `enrich_by_byte_range(source, byte_start, byte_end, ...)` signature and a registry for lane dispatch. The current design is a reasonable seam that allows extension without mandating it.

**Effort:** medium
**Risk if unaddressed:** low (only relevant if third language added)

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 0/3

**Current state:**
This is the subsystem's most significant weakness. Multiple pieces of knowledge are duplicated across the Python and Rust lanes and across core modules.

**Findings:**
- **`_build_query_windows`** duplicated identically in `python_lane/runtime.py:162-180` and `python_lane/facts.py:226-244`. Both build query windows from changed ranges with identical logic (pad_bytes=96, ensure fallback, check containment). The Rust lane has a near-identical inline version at `rust_lane/runtime.py:1427-1438`.
- **`_lift_anchor`** duplicated in `python_lane/runtime.py:487-504` and `python_lane/facts.py:115-130`. Both walk up the tree looking for the same set of containing node kinds (`call`, `attribute`, `assignment`, `import_statement`, etc.) with slightly different stop conditions.
- **`_normalize_semantic_version`** duplicated three times: `language_registry.py:64-70`, `query/planner.py:21-26`, and `schema/node_schema.py:93` (inferred from grep). All perform the same 3-part tuple validation.
- **`NodeLike` protocol** defined five times independently: `tags.py:30`, `rust_lane/injections.py:18`, `structural/exports.py:131`, `core/node_utils.py:14`, `python_lane/locals_index.py:14`. Each defines the same `start_byte`/`end_byte` property protocol with varying additional properties.
- **`_python_field_ids`** duplicated four times: `python_lane/facts.py:59`, `python_lane/runtime.py:62`, `python_lane/fallback_support.py:19`, `python_lane/locals_index.py:39`. All are `@lru_cache(maxsize=1)` wrappers around `cached_field_ids("python")`.
- **`_ENRICHMENT_ERRORS`** duplicated in `python_lane/runtime.py:57` and `rust_lane/runtime.py:94`. Both define the same tuple of exception types.
- **`_make_parser` / `_parse_tree`** duplicated in `python_lane/runtime.py:98-111` and `rust_lane/runtime.py:285-298`. Both follow the same pattern with language-specific factory.
- **`_pack_source_rows`** exists three times: `python_lane/runtime.py:207`, `python_lane/facts.py:198`, and `rust_lane/runtime.py:356`. While language-specific, they share the same structural pattern.

**Suggested improvement:**
1. Extract `_build_query_windows` to `core/change_windows.py` (it already exists nearby in that module).
2. Extract `_lift_anchor` to `core/node_utils.py` with configurable stop-kinds and anchor-kinds.
3. Consolidate `_normalize_semantic_version` into a single location in `core/infrastructure.py` or `schema/node_schema.py` and import elsewhere.
4. Define a single `NodeLike` protocol in `contracts/core_models.py` and import it everywhere.
5. Move `_python_field_ids` to a single location in `python_lane/__init__.py` or a shared `python_lane/helpers.py`.
6. Move `_ENRICHMENT_ERRORS` to `contracts/core_models.py` as `ENRICHMENT_ERRORS`.

**Effort:** medium (each individual fix is small; the aggregate is medium)
**Risk if unaddressed:** high -- semantic drift between copies is the most likely source of subtle bugs in this subsystem.

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
Excellent use of frozen msgspec structs for all boundary contracts. `QueryExecutionSettingsV1` clearly documents execution bounds. `QueryWindowV1` enforces inclusive-exclusive semantics. V1 suffixes signal versioning intent.

**Findings:**
- All 28+ contract types in `contracts/core_models.py` are frozen with explicit defaults.
- `QueryPackRulesV1` in `contracts/query_models.py:42-48` provides explicit validation rule contracts with sensible defaults.
- `QueryExecutionCallbacksV1` in `core/runtime.py:38-44` uses `@dataclass(frozen=True, slots=True)` for callback bundles.

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Good at external boundaries: `decode_yaml_strict` in `query_models.py:138` converts YAML to typed structs at load time. However, internal enrichment payloads are `dict[str, object]` throughout both lanes, relying on key conventions rather than typed structures.

**Findings:**
- `python_lane/facts.py:516-550` (`_build_payload`): Assembles a `dict[str, object]` with 15+ keys, relying on string key conventions rather than a typed struct.
- `rust_lane/runtime.py:1395-1410` (`_build_enrichment_payload`): Same pattern -- builds an untyped dict.
- Both then pass through `canonicalize_*_lane_payload` which validates via `msgspec.convert` but does not return a typed object.
- `lane_payloads.py:50` runs `msgspec.convert(payload, type=PythonTreeSitterPayloadV1, strict=False)` but discards the result (`_ = ...`), using it only for validation, not for producing a typed value.

**Suggested improvement:**
Have `canonicalize_python_lane_payload` return a `PythonTreeSitterPayloadV1` instead of a `dict`. This would push type safety deeper into the pipeline. The current approach validates but then immediately discards the typed representation.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Frozen structs prevent mutation. Enums and literals constrain `window_mode`. However, some constructs could be tighter.

**Findings:**
- `QueryWindowV1` in `contracts/core_models.py:17-19` does not enforce `end_byte > start_byte` at construction. Invalid windows are filtered out in `_normalized_windows` (`core/runtime.py:72`) rather than being prevented at creation.
- `TreeSitterDiagnosticV1.kind` is `str` rather than a `Literal["ERROR", "MISSING"]`, which is what the only producer (`diagnostics.py:166`) actually generates.

**Suggested improvement:**
Add a `__post_init__` check or use `msgspec` validation hooks to enforce `end_byte > start_byte` on `QueryWindowV1`. Narrow `TreeSitterDiagnosticV1.kind` to `Literal["ERROR", "MISSING"]`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions are either pure queries (scoring, planning) or explicit commands (record_runtime_sample). A few functions mix.

**Findings:**
- `canonicalize_python_lane_payload` in `lane_payloads.py:39-51` mutates its input dict via `payload.pop("tree_sitter_diagnostics", None)` (line 45) while also returning a value. This is a mixed command/query.
- `_touch_tree_cache` in `rust_lane/runtime.py:301-313` mutates `_TREE_CACHE` and `_TREE_CACHE_EVICTIONS` and accesses `session._entries`, but returns None. This is a pure command, which is fine, but it reaches into session internals.

**Suggested improvement:**
Have `canonicalize_python_lane_payload` operate on a copy of the payload dict rather than mutating the input. This makes it a pure query.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion -- Alignment: 2/3

**Current state:**
`query_pack_executor.py` uses lazy imports to delegate to `core/runtime.py`, which is a clean inversion pattern. However, both lanes hardcode their parser construction rather than accepting a parser factory via injection.

**Findings:**
- `python_lane/runtime.py:98-102` (`_make_parser`) and `rust_lane/runtime.py:285-289` (`_make_parser`) both hardcode parser construction. The parse session (`get_parse_session`) does accept a `parser_factory` parameter, which is a good DI seam, but the factory itself is always `_make_parser`.
- `diagnostics.py:149` uses a lazy import for `run_bounded_query_matches`, which is a good pattern for avoiding circular dependencies while keeping the runtime import light.

**Suggested improvement:**
The `parser_factory` parameter on `get_parse_session` is the right seam for testing. No structural change needed, but lane-level functions could expose this seam to callers for testing purposes.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No class hierarchies exist in the subsystem. All behavior is composed via functions, frozen dataclasses, and msgspec structs. Language-specific behavior is achieved through separate module namespaces, not subclassing.

**Findings:**
- Zero use of inheritance. All 28+ contract types extend `CqStruct` (which is a msgspec.Struct base), but this is structural, not behavioral inheritance.
- The `_try_extract` / `_merge_result` pattern in `rust_lane/runtime.py:1094-1191` composes extractors via function references rather than an extractor class hierarchy.

**Suggested improvement:**
No action needed. Well aligned.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. One notable violation exists.

**Findings:**
- `rust_lane/runtime.py:310-312`: `_touch_tree_cache` accesses `session._entries` (a private attribute of the parse session object) to evict stale entries. This couples the Rust lane to the internal data structure of the parse session.
- Tree-sitter node access via `getattr(node, "start_byte", 0)` is pervasive throughout the subsystem. While defensive, this pattern reaches through the node's interface rather than using a typed wrapper. This is acceptable given tree-sitter's dynamic C FFI nature.

**Suggested improvement:**
Add a `remove_entry(key)` method to the parse session API instead of reaching into `_entries` from `_touch_tree_cache`. This isolates the Rust lane from parse session internals.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The `_try_extract` pattern in `rust_lane/runtime.py` is a good "tell" design -- extractors are passed a node and told to extract. However, extensive `getattr` probing on tree-sitter nodes is an "ask" pattern.

**Findings:**
- `getattr(node, "start_byte", 0)` appears dozens of times across the subsystem. This is asking the node for data rather than telling it to produce a result. However, this is necessary because tree-sitter nodes are C FFI objects without a Python-native protocol.
- `core/runtime.py:171-177` (`_build_cursor`): Checks `hasattr(cursor_any, "set_max_start_depth")` and `hasattr(cursor_any, "set_timeout_micros")` before calling. This is capability probing, acceptable for version-compatibility.

**Suggested improvement:**
No structural change needed. The `getattr` probing is a reasonable adaptation to the tree-sitter FFI boundary. The `NodeLike` protocol (once consolidated) would formalize the "tell" interface.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Pattern scoring (`_pattern_score`), plan building (`build_pattern_plan`, `build_pack_plan`), and match row construction (`build_match_rows`) are pure functions. The adaptive runtime mixes computation with diskcache IO.

**Findings:**
- `core/runtime_support.py:34-56` (`build_autotune_plan`): Pure function -- takes a snapshot and returns a plan. Good.
- `core/adaptive_runtime.py:69-93` (`record_runtime_sample`): Mixes latency computation with diskcache writes. The "should I record" decision and the "what to record" computation are tangled with the IO.
- `core/adaptive_runtime.py:128-134` (`adaptive_query_budget_ms`): Mixes cache read IO with budget computation.

**Suggested improvement:**
Separate budget computation from cache access in `adaptive_runtime.py`. Have `adaptive_query_budget_ms` accept a `cached_average: float | None` parameter instead of reading from cache internally, pushing the cache read to the caller.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All LRU-cached functions are idempotent. Parse sessions cache parsed trees, so re-parsing the same source returns the same tree. Query execution with the same inputs produces the same match set (deterministic pattern matching).

**Findings:**
- `record_runtime_sample` uses `incr` on a cache counter, which is not idempotent (calling twice increments twice). However, this is telemetry, not correctness-critical state, so the non-idempotency is acceptable.

**Suggested improvement:**
No action needed. The non-idempotent telemetry recording is appropriate for its purpose.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism -- Alignment: 3/3

**Current state:**
Excellent determinism guarantees throughout.

**Findings:**
- `core/infrastructure.py:55-61` (`run_file_lanes_parallel`): Uses indexed result tracking to preserve input order regardless of completion order.
- `query/planner.py:131`: Pattern plans sorted by score with deterministic tiebreaking.
- `core/runtime.py:260-269` (`_match_fingerprint`): Match deduplication uses sorted capture names for deterministic fingerprinting.
- `injections.py:135-186`: Injection plans deduplicated via `OrderedDict` key tuples, preserving insertion order.

**Suggested improvement:**
No action needed. Well aligned.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The core runtime and infrastructure modules are well-factored with clear responsibilities. The Rust lane runtime exceeds reasonable complexity.

**Findings:**
- `rust_lane/runtime.py` at 1,976 LOC is the largest file in the subsystem and exceeds the project's complexity guidelines. It contains 40+ private functions spanning 7+ concern areas.
- `core/runtime.py` at 633 LOC is at the upper limit but acceptable given its role as the central bounded query execution engine.
- The window splitting logic in `core/runtime.py:207-229` (`_split_window`) is simple and clear.

**Suggested improvement:**
Decompose `rust_lane/runtime.py` as described in P2/P3. Target no file exceeding ~700 LOC.

**Effort:** large (but overlaps with P2/P3 work)
**Risk if unaddressed:** low (complexity, not correctness)

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions are justified by current usage. One piece of unused infrastructure exists.

**Findings:**
- `rust_lane/runtime.py:125-170` (`_FIELD_GROUPS`): A dict mapping group names to field name lists. This dict is defined but never used at runtime for validation, dispatch, or filtering. It serves only as documentation in code.
- `structural/export.py` and `structural/exports.py` appear to be two separate structural export modules with overlapping responsibility. The architecture doc calls `exports.py` an "alternative implementation (possibly for different export formats or legacy compatibility)."

**Suggested improvement:**
Remove `_FIELD_GROUPS` from runtime code and move it to a docstring or architecture doc if needed for reference. Clarify or consolidate `export.py` vs `exports.py`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs are generally predictable. One surprising behavior exists in payload canonicalization.

**Findings:**
- `lane_payloads.py:45`: `canonicalize_python_lane_payload` calls `payload.pop("tree_sitter_diagnostics", None)`, which mutates the input dict. A function named "canonicalize" is expected to normalize, not destructively modify its input. The same pattern appears in `canonicalize_rust_lane_payload` at line 60.
- `query_pack_executor.py:113`: `execute_pack_rows` calls `execute_pack_rows_with_matches` and then discards the matches with `_ = matches`. This is surprising -- why compute matches only to discard them? The comment is absent.

**Suggested improvement:**
Make `canonicalize_*_lane_payload` operate on a shallow copy: `payload = dict(payload)` at the start. Add a comment in `execute_pack_rows` explaining why matches are computed but discarded (likely because `build_match_rows_with_query_hits` needs both captures and matches).

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 3/3

**Current state:**
All contracts use V1 suffixes. Every module has an explicit `__all__` list. The `contracts/__init__.py` provides a centralized re-export surface.

**Findings:**
- 100% of modules in the subsystem define `__all__`.
- All contract types use the `V1` suffix convention.
- `CqStruct` (frozen=True) base ensures serialization compatibility.

**Suggested improvement:**
No action needed. Well aligned.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure functions (scoring, planning, match row building) are easily testable. Parse session accepts a `parser_factory` parameter, which is a DI seam. However, adaptive runtime is tightly coupled to diskcache.

**Findings:**
- `core/adaptive_runtime.py:19-21` (`_cache`): Directly calls `get_cq_cache_backend(root=Path.cwd())`. This makes testing require either a real cache backend or monkeypatching.
- `python_lane/runtime.py:76-86` (`is_tree_sitter_python_available`): Checks runtime availability by loading actual language objects. Tests that want to simulate unavailability must monkeypatch multiple globals.
- `core/runtime_support.py:34-56` (`build_autotune_plan`): Pure function, trivially testable. Good.

**Suggested improvement:**
Accept a `cache_backend` parameter in `adaptive_runtime` functions instead of calling `_cache()` internally. This would allow tests to inject a fake cache without monkeypatching.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`QueryExecutionTelemetryV1` provides comprehensive per-query telemetry (windows total/executed, capture count, match count, exceeded limits, cancelled, split count, degrade reason). Parse session stats track hits/misses. However, there is no structured logging.

**Findings:**
- No `logging.getLogger` calls exist in the subsystem. All observability is through return-value telemetry structs.
- `ParseSessionStatsV1` in `contracts/core_models.py:96-103` tracks parse cache hits/misses/counts.
- `QueryExecutionTelemetryV1` captures detailed per-execution metrics.
- `_TREE_CACHE_EVICTIONS` in `rust_lane/runtime.py:119` uses a mutable dict for eviction counting -- not aligned with the struct-based telemetry pattern used elsewhere.

**Suggested improvement:**
Align `_TREE_CACHE_EVICTIONS` with the telemetry pattern by including it in `get_tree_sitter_rust_cache_stats()` return (which it already is, at line 348). Consider adding structured logging at INFO level for parse failures and query budget exhaustion to aid operational debugging.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Python/Rust Lane Duplication

**Description:** The Python and Rust lanes independently implement many of the same patterns: query window building, anchor lifting, parser construction, pack source loading, field ID caching, and error handling. This is the root cause of P7's score of 0/3.

**Root cause:** The lanes were likely developed sequentially, with Rust lane code copied from the Python lane rather than extracting shared abstractions.

**Affected principles:** P7 (DRY), P3 (SRP -- shared logic duplicated in lane-specific files), P19 (KISS -- duplicated code adds conceptual weight).

**Suggested approach:** Create a `core/lane_support.py` module containing the 6+ shared functions. Both lanes import from this shared module. This is a refactor, not a redesign -- the existing function signatures are stable.

### Theme 2: Rust Lane Monolith

**Description:** `rust_lane/runtime.py` at 1,976 LOC handles 7+ distinct concerns in a single file, making it the highest-risk file in the subsystem for change.

**Root cause:** Incremental feature additions (injection support, tag events, macro expansion, module/import rows) were added to the existing file rather than extracted into purpose-built modules.

**Affected principles:** P2 (Separation of concerns), P3 (SRP), P4 (Cohesion), P19 (KISS).

**Suggested approach:** Extract into `rust_lane/extraction.py` (signature/visibility/attributes/impl/struct/enum), `rust_lane/classification.py` (item roles), `rust_lane/facts.py` (fact list and module/import rows), keeping `rust_lane/runtime.py` as the thin entry point.

### Theme 3: Untyped Internal Payloads

**Description:** Both lanes build enrichment payloads as `dict[str, object]` with string key conventions rather than typed structs. The `canonicalize_*_lane_payload` functions validate but discard the typed representation.

**Root cause:** The enrichment payloads evolved organically as new fields were added, and typing them retroactively was deferred.

**Affected principles:** P9 (Parse, don't validate), P10 (Illegal states), P8 (Contracts).

**Suggested approach:** Define typed enrichment payload structs (separate from the output contracts) and build payloads using struct construction rather than dict assembly. This is a medium-term improvement that would catch field-name typos at construction time.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate 5 `NodeLike` protocols into `contracts/core_models.py` | small | Eliminates 4 redundant protocol definitions; prevents semantic drift |
| 2 | P7 (DRY) | Extract `_normalize_semantic_version` to one location | small | Eliminates 2 redundant copies; single source of truth for version parsing |
| 3 | P21 (Astonishment) | Make `canonicalize_*_lane_payload` non-mutating by copying input | small | Prevents surprising caller-side mutation |
| 4 | P7 (DRY) | Consolidate `_python_field_ids` to one location in python_lane | small | Eliminates 3 redundant LRU-cached wrappers |
| 5 | P7 (DRY) | Extract `_build_query_windows` and `_ENRICHMENT_ERRORS` to shared modules | small | Eliminates cross-lane semantic duplication |

## Recommended Action Sequence

1. **Consolidate `NodeLike` protocol** into `contracts/core_models.py`. Update all 5 import sites. (P7, small, no behavioral change)

2. **Extract `_normalize_semantic_version`** to `core/infrastructure.py` (or `schema/node_schema.py`). Update `language_registry.py` and `query/planner.py` to import. (P7, small)

3. **Fix `canonicalize_*_lane_payload` mutation**: Add `payload = dict(payload)` at the start of both functions in `lane_payloads.py`. (P21, small)

4. **Consolidate `_python_field_ids`** into `python_lane/__init__.py` or a new `python_lane/helpers.py`. Update 4 import sites. (P7, small)

5. **Extract shared lane utilities**: Create `core/lane_support.py` with `_build_query_windows`, `ENRICHMENT_ERRORS` tuple, and `_lift_anchor`. Update both lanes to import. (P7, medium)

6. **Decompose `rust_lane/runtime.py`**: Extract extraction logic, classification logic, and fact building into separate modules. This is the largest single improvement and depends on steps 1-5 being complete for clean extraction. (P2, P3, P19, large)

7. **Add `remove_entry` method to parse session**: Eliminates the `_touch_tree_cache` Demeter violation in `rust_lane/runtime.py`. (P14, small, depends on step 6 for clean integration)
