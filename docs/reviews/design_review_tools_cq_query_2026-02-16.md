# Design Review: tools/cq/query/

**Date:** 2026-02-16
**Scope:** `tools/cq/query/` (entire directory)
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 24 (10,030 LOC)

## Executive Summary

The query subsystem is a well-structured DSL-to-execution pipeline with strong separation between parsing, IR, planning, and execution phases. The IR layer (`ir.py`) is the standout: frozen `msgspec.Struct` contracts, exhaustive type literals, and clean `__post_init__` validation make illegal states nearly unrepresentable. The principal weaknesses are (1) duplicated cache-orchestration ceremony across `executor.py` and `executor_ast_grep.py`, (2) the `executor.py` god-module at 1,198 lines mixing coordination, cache plumbing, summary assembly, and dispatch, and (3) entity-kind classification sets duplicated in at least three modules. Addressing these three themes would improve six or more principle scores simultaneously.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `executor_dispatch.py` imports private `_execute_*` functions |
| 2 | Separation of concerns | 1 | medium | medium | `executor.py` mixes cache plumbing, summary assembly, and dispatch |
| 3 | SRP | 1 | medium | medium | `executor.py` changes for cache, summary, entity, and pattern reasons |
| 4 | High cohesion, low coupling | 2 | medium | low | Entity-kind sets duplicated across 3 modules |
| 5 | Dependency direction | 2 | small | low | Core IR is clean; executor depends outward appropriately |
| 6 | Ports & Adapters | 2 | medium | low | Cache access is via interface but constructed inline |
| 7 | DRY (knowledge) | 1 | medium | medium | Entity-kind sets, cache ceremony, pattern-query finalization duplicated |
| 8 | Design by contract | 2 | small | low | IR post-init validates; parser raises typed errors |
| 9 | Parse, don't validate | 2 | small | low | Parser produces typed IR; some raw dicts leak in match data |
| 10 | Illegal states | 2 | small | low | Query enforces entity XOR pattern; Scope is well-typed |
| 11 | CQS | 2 | small | low | Most functions are queries; `_apply_entity_handlers` mutates result in-place |
| 12 | DI + explicit composition | 1 | medium | medium | `get_cq_cache_backend(root)` called inline in 3+ places |
| 13 | Composition over inheritance | 3 | -- | -- | No inheritance hierarchies; pure composition throughout |
| 14 | Law of Demeter | 2 | small | low | `ctx.plan.sg_rules`, `ctx.query.pattern_spec.pattern` chains |
| 15 | Tell, don't ask | 2 | small | low | `_process_decorator_query` reads source + inspects record fields |
| 16 | Functional core | 2 | small | low | IR/planner are pure; executors mix IO with logic |
| 17 | Idempotency | 3 | -- | -- | All queries are read-only; cache writes are content-addressed |
| 18 | Determinism | 3 | -- | -- | Sort keys are deterministic; output ordering is stable |
| 19 | KISS | 2 | small | low | Cache fragment protocol adds significant ceremony |
| 20 | YAGNI | 2 | small | low | `Query.with_*` builders manually copy all 16 fields |
| 21 | Least astonishment | 2 | small | low | `_execute_entity_query` vs public `execute_entity_query` naming |
| 22 | Public contracts | 2 | small | low | `__all__` declared on all modules; `ExecutePlanRequestV1` versioned |
| 23 | Testability | 1 | medium | medium | Inline cache/service construction makes unit testing hard |
| 24 | Observability | 2 | small | low | Logger present; no structured spans or metrics |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The IR layer (`ir.py`) and language module (`language.py`) expose clean public surfaces. However, `executor_dispatch.py` imports private-prefixed functions from `executor.py`.

**Findings:**
- `executor_dispatch.py:15-17` imports `_execute_entity_query` and `_execute_pattern_query` via deferred import from `executor.py`. These are private by naming convention but serve as the real public dispatch targets.
- `executor.py:158` places `from tools.cq.index.files import ...` outside the `TYPE_CHECKING` block despite `FileTabulationResult` being used only in type positions within several functions.

**Suggested improvement:**
Rename `_execute_entity_query` and `_execute_pattern_query` in `executor.py` to drop the underscore prefix (they are the canonical implementation functions), and have `executor_dispatch.py` import them cleanly.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`executor.py` (1,198 lines) is the primary concern-mixing hotspot. It handles: (a) cache fragment orchestration for entity scans, (b) summary dict assembly, (c) auto-scope language dispatch, (d) entity and pattern query coordination, (e) ripgrep file matching, and (f) front-door insight attachment.

**Findings:**
- `executor.py:330-406` (`_scan_entity_records`) contains ~75 lines of cache fragment orchestration (build context, probe, scan misses, persist writes) that mirrors similar logic in `executor_ast_grep.py:158-274` (`execute_ast_grep_rules`).
- `executor.py:240-277` (`_summary_common_for_query`) builds telemetry dicts with hardcoded keys like `"python_semantic_telemetry"` and `"rust_semantic_telemetry"` -- this is presentation/reporting concern mixed with execution.
- `executor.py:1149-1188` (`rg_files_with_matches`) is a standalone file-matching utility that belongs in the search/rg adapter, not the query executor.

**Suggested improvement:**
Extract cache-fragment orchestration into a shared helper (e.g., `query/cache_orchestration.py`) that both `executor.py` and `executor_ast_grep.py` delegate to. Move summary template construction to a dedicated `query/summary_builders.py`. Move `rg_files_with_matches` to `tools/cq/search/rg/`.

**Effort:** medium
**Risk if unaddressed:** medium -- every cache policy change requires parallel edits in two files.

---

#### P3. SRP -- Alignment: 1/3

**Current state:**
`executor.py` changes for at least four distinct reasons: cache strategy changes, summary format changes, new entity types, and new pattern features.

**Findings:**
- `executor.py` has 7 dataclasses/structs (`EntityExecutionState`, `PatternExecutionState`, `_EntityFragmentContext`, `ExecutePlanRequestV1`, plus sort-key functions), 5 public entry points, and ~20 private functions spanning cache, scan, summary, and dispatch.
- `executor_ast_grep.py` (1,169 lines) similarly mixes AST-grep execution, cache fragment handling, prefiltering, and match-data conversion. It exports 39 symbols in `__all__`.

**Suggested improvement:**
Decompose `executor.py` into: (1) `executor_entity.py` for entity query coordination, (2) `executor_pattern.py` for pattern query coordination, (3) `cache_orchestration.py` for shared cache fragment plumbing. The existing `executor_dispatch.py` facade remains as the clean entry point.

**Effort:** medium
**Risk if unaddressed:** medium -- the module is the most changed file in the subsystem and has high cognitive load.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Most modules have good internal cohesion. The coupling issue is entity-kind classification sets being duplicated.

**Findings:**
- Entity kind sets (`function_kinds`, `class_kinds`, `import_kinds`) are defined independently in `executor_definitions.py:307-329` (`matches_entity`), `finding_builders.py:300-308` (`find_enclosing_class`), `enrichment.py:277-278` (`_enrich_record`), and `section_builders.py` (implicit via calls).
- `executor.py` imports from 27 distinct modules (counted from import block lines 6-161), creating a wide dependency fan-out.

**Suggested improvement:**
Extract canonical entity-kind sets to a single authority (e.g., `ir.py` or a new `entity_kinds.py` constants module). All classification functions should reference this single source.

**Effort:** medium
**Risk if unaddressed:** low -- kind set drift is a latent correctness risk when Rust entity kinds are added.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency direction is mostly correct: `ir.py` depends on `language.py` only. `parser.py` depends on `ir.py`. `planner.py` depends on `ir.py` and `language.py`. Executors depend outward on core, cache, and astgrep modules.

**Findings:**
- `ir.py:12-17` depends on `language.py` for `QueryLanguage`/`QueryLanguageScope` -- this is appropriate.
- `executor.py:15-154` has a massive import block that reaches into `tools.cq.core.cache.*` (13 imports), `tools.cq.core.*` (5 imports), `tools.cq.orchestration.*` (3 imports), and `tools.cq.search.*` (3 imports). The executor knows about too many infrastructure modules.

**Suggested improvement:**
Introduce a cache-facade that bundles the 13 cache imports into a single service interface. The executor would depend on the facade, not on 13 individual cache modules.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`CqCacheBackend` is a proper port (interface in `tools/cq/core/cache/interface.py`). However, the adapter is constructed inline via `get_cq_cache_backend(root)` inside executor functions rather than being injected.

**Findings:**
- `executor.py:440` calls `get_cq_cache_backend(root=resolved_root)` directly inside `_build_entity_fragment_context`.
- `executor_ast_grep.py:323` does the same inside `build_pattern_fragment_context`.
- `sg_parser.py:219` does the same inside `_tabulate_scan_files`.

**Suggested improvement:**
Accept `CqCacheBackend` as a parameter on `QueryExecutionContext` (it already has a `cache_backend: object | None` field at line 28, but it is unused). Thread it through instead of constructing it inline.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
Three distinct knowledge-duplication patterns exist.

**Findings:**
- **Entity kind sets**: `function_kinds = {"function", "async_function", "function_typeparams"}` appears at `executor_definitions.py:307-309`, `enrichment.py:277`, and implicitly at `executor.py:554`. Each is slightly different (e.g., `enrichment.py` uses a union `function_kinds | class_kinds`).
- **Cache fragment ceremony**: The probe-scan-persist pattern appears in `executor.py:340-404` and `executor_ast_grep.py:192-267` with near-identical structure (build entries, partition, scan misses, persist writes). The two differ only in payload types.
- **Pattern query finalization**: `_execute_pattern_query` at `executor.py:922-965` and `execute_pattern_query_with_files` at `executor.py:968-1029` share ~40 lines of identical scope-filter + limit + summary logic.

**Suggested improvement:**
(1) Create a canonical `ENTITY_KIND_SETS` constant in `ir.py` or a shared constants module. (2) Extract a generic `fragment_scan_orchestrate()` helper parameterized by payload type and scan function. (3) Extract shared pattern-query finalization into a `_finalize_pattern_result()` helper.

**Effort:** medium
**Risk if unaddressed:** medium -- kind-set drift between modules will cause silent classification bugs.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The IR layer has strong contracts. `Query.__post_init__` validates entity XOR pattern at `ir.py:613-624`. `RelationalConstraint.__post_init__` validates field/operator compatibility at `ir.py:369-378`. Parser raises typed `QueryParseError` for all invalid inputs.

**Findings:**
- `MetaVarFilter.matches()` at `ir.py:208-224` imports `re` at call time (deferred import inside a method). This is functional but surprising.
- `executor_ast_grep.py:920-972` (`match_to_finding`) returns `(None, None)` tuple for invalid data rather than raising -- this is correct graceful degradation but lacks documentation of the postcondition.

**Suggested improvement:**
Move the `re` import in `MetaVarFilter.matches()` to the module level (it is already imported in `metavar.py` which handles the same concern). Document the `(None, None)` return contract on `match_to_finding`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The parser produces well-typed `Query` objects from raw strings. The `_tokenize` + `_parse_entity_query`/`_parse_pattern_query` pipeline is a textbook parse-don't-validate flow.

**Findings:**
- Match data in `executor_ast_grep.py:717-745` (`build_match_data`) is built as `dict[str, object]` with string keys like `"ruleId"`, `"file"`, `"range"` rather than a typed struct. This raw dict propagates through cache and is later parsed with `isinstance` checks in `match_to_finding`.
- `entity_front_door.py:415-417` (`_detail_int`) uses `isinstance(value, int)` guards to extract values from `Finding.details` -- the details dict is untyped.

**Suggested improvement:**
Define a `MatchData` frozen struct (msgspec) to replace the raw dict in `build_match_data`. This would eliminate the defensive `isinstance` checks in `match_to_finding` and `raw_match_sort_key`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`Query` enforces entity XOR pattern via `__post_init__`. `Expander.depth` uses `msgspec.Meta(ge=1)` to prevent zero-depth. `PatternSpec.strictness` uses `Literal` types.

**Findings:**
- `Query` at `ir.py:553-611` has 16 optional fields, many of which are only meaningful for entity queries (e.g., `decorator_filter`, `joins`) or pattern queries (e.g., `pattern_spec`). The flat struct permits states like a pattern query with `decorator_filter` set, which is meaningless.
- `QueryExecutionContext.cache_backend` and `symtable_enricher` at `execution_context.py:27-28` are typed as `object | None` -- they bypass the type system entirely.

**Suggested improvement:**
For `QueryExecutionContext`, type `cache_backend` as `CqCacheBackend | None` and `symtable_enricher` as `SymtableEnricher | None`. For `Query`, the entity-vs-pattern field overlap is a known trade-off of using a single struct vs tagged union; document which fields are meaningful for each mode.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions are either queries (return data) or commands (mutate result). The pattern is mostly clean.

**Findings:**
- `_apply_entity_handlers` at `executor.py:644-668` mutates `result` in place (appending to `key_findings`, updating `summary`). This is a command, which is fine, but it is called mid-pipeline where the caller also reads `result.summary` afterward.
- `process_import_query` at `executor_definitions.py:39-79` both appends findings to `result.key_findings` AND overwrites `result.key_findings` via `filter_by_scope` return. This is a mixed read-write pattern on the same mutable argument.

**Suggested improvement:**
Have `process_import_query` and `process_def_query` return findings lists rather than mutating the result object directly. The caller would then assign them.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 1/3

**Current state:**
Service construction happens inline at call sites rather than being injected from the composition root.

**Findings:**
- `executor.py:440`: `cache = get_cq_cache_backend(root=resolved_root)` -- cache constructed inline.
- `executor.py:439`: `policy = default_cache_policy(root=resolved_root)` -- policy constructed inline.
- `executor.py:842`: `services = resolve_runtime_services(root)` -- service locator pattern inside `_attach_entity_insight`.
- `enrichment.py:367-376`: `SymtableEnricher.__init__` takes `root` and constructs its own internal cache.

**Suggested improvement:**
Thread `CqCacheBackend` and `CqCachePolicyV1` through `QueryExecutionContext` (which already has unused `cache_backend` field). Have `_attach_entity_insight` receive the entity service as a parameter rather than using a service locator. This makes the dependency graph explicit and enables testing with fake backends.

**Effort:** medium
**Risk if unaddressed:** medium -- unit testing these functions requires monkeypatching module-level constructors.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
The entire query subsystem uses composition exclusively. There are no class hierarchies. Data structures are dataclasses or frozen msgspec Structs. Behavior is composed via function calls and delegation.

**No findings.** This is well aligned.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most access patterns are through direct collaborators. A few chains exist.

**Findings:**
- `executor.py:231`: `query.pattern_spec.pattern` -- two-level chain through `Query` to `PatternSpec` to `pattern`.
- `executor.py:700-708`: `query.pattern_spec.context`, `query.pattern_spec.selector`, `query.pattern_spec.strictness` -- repeated access to `pattern_spec` sub-fields in `_maybe_add_pattern_explain`.
- `section_builders.py:169-178`: `finding.details.get("name")`, `finding.details.get("caller_count")` -- reaching into `Finding.details` dict by string keys.

**Suggested improvement:**
Add a convenience method on `Query` to expose pattern metadata (e.g., `Query.pattern_text` property). For `Finding.details`, this is inherent to the generic details dict design; no change needed unless a typed detail struct is introduced.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Most modules follow tell-don't-ask patterns. The decorator query handler is the primary exception.

**Findings:**
- `executor.py:1032-1116` (`_process_decorator_query`) reads source files, inspects `def_record.kind`, checks `query.name`, calls `enrich_with_decorators`, inspects decorator list, applies count filters -- all in one function. The record doesn't know about its decorators; the function interrogates it.
- `finding_builders.py:232-258` (`extract_call_target`) uses regex to parse the call target from `call.text` rather than the record carrying structured call-target data.

**Suggested improvement:**
Consider enriching `SgRecord` (or a wrapper) with structured call-target and decorator data at scan time, so downstream code can ask the record directly rather than re-parsing text. This would require coordination with the `sgpy_scanner` module.

**Effort:** large
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
`ir.py`, `parser.py`, `planner.py`, `language.py`, `metavar.py`, `cache_converters.py`, and `shared_utils.py` are effectively pure -- no IO, no side effects. The executors (`executor.py`, `executor_ast_grep.py`) are the imperative shell that reads files and writes cache.

**Findings:**
- `enrichment.py:301-361` (`enrich_records`) reads files from disk, compiles code objects, and inspects bytecode -- this is IO-heavy logic mixed with analysis logic in the same function.
- `executor_ast_grep.py:547-570` (`process_ast_grep_file`) reads a file and processes it -- appropriately placed in the imperative shell.

**Suggested improvement:**
Separate `enrich_records` into a file-reading shell and a pure enrichment core that takes `(source: str, records: list[SgRecord])` as input. This would make the enrichment logic testable without filesystem access.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All queries are read-only operations over source files. Cache writes are content-addressed (keyed by file content hash + rules digest), so re-running produces identical results. Cache eviction uses tag-based lifecycle.

**No findings.** This is well aligned.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Sort keys are deterministic across all output paths. `cache_converters.py` provides both detailed and lightweight sort keys. `assemble_pattern_output` at `executor_ast_grep.py:453-478` iterates files in path order and sorts each bucket. UUIDs are used only for run IDs, never for cache keys.

**No findings.** This is well aligned.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The DSL parser and IR are pleasantly simple. The cache fragment protocol adds significant incidental complexity.

**Findings:**
- `executor.py:409-463` (`_build_entity_fragment_context`) constructs `_EntityFragmentContext` through a 10-step process involving scope hash, snapshot fingerprint, policy resolution, cache backend construction, namespace check, TTL resolution, and write-tag resolution. This ceremony is repeated in `executor_ast_grep.py:277-339`.
- `executor.py:466-488` (`_entity_fragment_entries`) builds cache entries with a nested `extras` dict containing `file_content_hash` and `record_types` -- the nesting is required by the generic fragment engine but adds indirection.

**Suggested improvement:**
The cache fragment protocol complexity is imposed by the generic cache engine. A thin query-specific cache facade would hide this ceremony behind a simpler interface like `scan_with_cache(files, scan_fn) -> records`.

**Effort:** medium
**Risk if unaddressed:** low -- the complexity is contained and correct, just verbose.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The codebase is lean overall. One area of speculative generality exists.

**Findings:**
- `Query` at `ir.py:631-716` has four `with_*` builder methods (`with_scope`, `with_expand`, `with_fields`, `with_relational`) that manually copy all 16 fields. Since `Query` is a `msgspec.Struct`, `msgspec.structs.replace()` achieves the same result without maintaining field lists. The `with_*` methods are used by bundle builders but could be replaced by direct `structs.replace()` calls.
- `symbol_resolver.py` defines a full `SymbolTable` with import resolution and `_is_builtin` builtin detection, but this is used only by `resolve_call_target` which appears to have limited callsites.

**Suggested improvement:**
Replace `Query.with_*` methods with `msgspec.structs.replace()` at call sites (already used at `executor.py:824` for `scoped_query`). This eliminates 85 lines of boilerplate in `ir.py` that must be updated whenever a field is added to `Query`.

**Effort:** small
**Risk if unaddressed:** low -- but every new `Query` field requires updating four methods.

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs are generally predictable. Naming is the primary issue.

**Findings:**
- `executor.py` has both `_execute_entity_query` (private, the real implementation at line 851) and `execute_entity_query_from_records` (public, at line 875). The dispatch module `executor_dispatch.py` re-exports the private one as `execute_entity_query`. This three-layer naming is confusing.
- `executor.py:84` imports `SymtableEnricher` and `filter_by_scope` from `enrichment.py` but also imports from `executor_definitions.py` which re-imports from `enrichment.py`. The import graph creates surprising indirection.

**Suggested improvement:**
Consolidate the naming: make `execute_entity_query` and `execute_pattern_query` the canonical public functions in `executor.py` (drop the underscore prefix). Remove `executor_dispatch.py` or make it a true facade that adds value (e.g., logging, error handling).

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
All 24 modules declare `__all__`. `ExecutePlanRequestV1` follows the versioned contract naming convention. IR types use `Literal` type aliases for valid values.

**Findings:**
- `QueryExecutionContext` at `execution_context.py:16-28` uses `object | None` for `cache_backend` and `symtable_enricher`, which erases the contract for consumers.
- `executor_ast_grep.py` exports 39 symbols in `__all__` -- many are internal helpers like `coerce_int`, `is_variadic_separator`, `node_payload` that could be private.

**Suggested improvement:**
Trim `executor_ast_grep.py.__all__` to only the symbols needed by external modules. Type `QueryExecutionContext` fields precisely.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
The pure modules (`ir.py`, `parser.py`, `planner.py`, `metavar.py`, `language.py`) are trivially testable. The executors are not, because they construct their own dependencies inline.

**Findings:**
- `executor.py:440`: `get_cq_cache_backend(root)` is called inline, requiring monkeypatch to test without disk cache.
- `executor.py:842`: `resolve_runtime_services(root)` uses a service locator, requiring monkeypatch.
- `enrichment.py:338-339`: File reads happen inside `enrich_records`, requiring real files or monkeypatch.
- `sg_parser.py:219`: `get_cq_cache_backend(root)` called inline for file inventory caching.

**Suggested improvement:**
Thread dependencies through `QueryExecutionContext`. The context already has `cache_backend` and `symtable_enricher` fields (lines 27-28) -- use them. For `enrich_records`, accept source text as input parameter alongside records. For `sg_parser`, accept cache backend as parameter.

**Effort:** medium
**Risk if unaddressed:** medium -- executor functions cannot be unit-tested without heavyweight mocking or real filesystem.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Logger is used consistently across modules (`logger = logging.getLogger(__name__)`). Debug-level messages cover scan starts, file skips, and dispatch decisions. Cache telemetry functions (`record_cache_get`, `record_cache_set`) are called at appropriate points.

**Findings:**
- No structured spans (OpenTelemetry or similar) in the query execution path, unlike the semantic pipeline which uses `SCOPE_SEMANTICS` spans.
- `executor.py:735-741` logs at DEBUG with `"Executing query plan mode=%s lang=%s"` but does not include run_id, making multi-request correlation difficult.
- Cache hit/miss ratios are recorded but not surfaced in the result summary in a standardized way (only via `snapshot_backend_metrics`).

**Suggested improvement:**
Add run_id to structured log fields. Consider adding a lightweight timing context to `execute_plan` that records parse/scan/filter/finalize phase durations into the result summary.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Cache Fragment Ceremony Duplication

**Description:** The probe-scan-persist cache fragment pattern is implemented twice -- once for entity scans in `executor.py:330-406` and once for pattern scans in `executor_ast_grep.py:158-274`. Both follow the same structure: build fragment context, build entries, partition into hits/misses, scan misses, persist writes. The duplication spans ~150 lines per instance.

**Root cause:** The generic fragment engine (`tools/cq/core/cache/fragment_engine.py`) provides primitives but not a complete orchestration helper. Each executor had to wire the primitives independently.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P7 (DRY), P19 (KISS), P23 (testability).

**Suggested approach:** Extract a `CachedScanOrchestrator` or `scan_with_fragment_cache()` function parameterized by: scan function, payload encoder/decoder, and fragment context builder. Both executors would call this single orchestrator.

### Theme 2: Inline Service Construction

**Description:** `get_cq_cache_backend(root)`, `default_cache_policy(root)`, `resolve_runtime_services(root)`, and `SymtableEnricher(root)` are constructed at their call sites rather than injected. This occurs in at least 6 locations across 4 files.

**Root cause:** `QueryExecutionContext` was designed to carry these dependencies (`cache_backend`, `symtable_enricher` fields exist) but they were never wired.

**Affected principles:** P6 (ports & adapters), P12 (DI), P23 (testability).

**Suggested approach:** Wire `QueryExecutionContext` fields and construct dependencies once at the `execute_plan` entry point. Pass the context through to all downstream functions.

### Theme 3: Entity Kind Classification Drift

**Description:** Sets like `function_kinds`, `class_kinds`, `import_kinds` are defined locally in `executor_definitions.py`, `finding_builders.py`, and `enrichment.py` with minor variations. When Rust entity kinds were added (e.g., `struct`, `enum`, `trait` in `executor_definitions.py:317-319`), some modules were updated and others were not.

**Root cause:** No single source of truth for entity-to-kind mappings.

**Affected principles:** P4 (cohesion), P7 (DRY), P10 (illegal states).

**Suggested approach:** Define canonical `FUNCTION_KINDS`, `CLASS_KINDS`, `IMPORT_KINDS` frozensets in `ir.py` (alongside the existing `EntityType` literal). All classification functions reference these constants.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7, P4 | Extract canonical entity-kind sets to single source in `ir.py` | small | Eliminates classification drift across 3+ modules |
| 2 | P20 | Replace `Query.with_*` methods with `msgspec.structs.replace()` | small | Removes 85 lines of boilerplate that must track field additions |
| 3 | P1, P21 | Rename `_execute_entity_query`/`_execute_pattern_query` to drop underscore prefix | small | Eliminates confusing private-import pattern in dispatch |
| 4 | P9 | Define `MatchData` struct to replace raw `dict[str, object]` in match pipeline | small | Eliminates defensive `isinstance` checks in match-to-finding conversion |
| 5 | P12, P23 | Wire `QueryExecutionContext.cache_backend` field to actual cache instance | medium | Enables unit testing without monkeypatch; makes DI explicit |

## Recommended Action Sequence

1. **Extract canonical entity-kind sets** (P4, P7) -- No dependencies; immediately reduces drift risk. Define `FUNCTION_KINDS`, `CLASS_KINDS`, `IMPORT_KINDS` in `ir.py` and update `executor_definitions.py`, `finding_builders.py`, `enrichment.py` to reference them.

2. **Replace `Query.with_*` boilerplate** (P20) -- Independent of other changes. Replace four methods with `msgspec.structs.replace()` calls at the ~5 call sites.

3. **Clean up naming: remove underscore prefix** (P1, P21) -- Rename `_execute_entity_query` and `_execute_pattern_query` in `executor.py`. Update `executor_dispatch.py` imports.

4. **Define `MatchData` struct** (P9) -- Replace raw dict in `build_match_data` and `match_to_finding`. This is a local refactor within `executor_ast_grep.py`.

5. **Wire `QueryExecutionContext` dependency injection** (P6, P12, P23) -- Construct `CqCacheBackend` and `CqCachePolicyV1` at `execute_plan` entry point and thread through context. Update `_build_entity_fragment_context` and `build_pattern_fragment_context` to accept cache from context.

6. **Extract cache fragment orchestration helper** (P2, P3, P7, P19) -- Depends on step 5. Unify the probe-scan-persist pattern from `executor.py` and `executor_ast_grep.py` into a single parameterized function.

7. **Decompose `executor.py`** (P2, P3) -- Depends on steps 5 and 6. Split into `executor_entity.py`, `executor_pattern.py`, and shared helpers. Keep `executor_dispatch.py` as the public facade.

8. **Separate IO from enrichment logic** (P16, P23) -- Refactor `enrich_records` to accept source text as input, enabling pure-function testing of the enrichment pipeline.
