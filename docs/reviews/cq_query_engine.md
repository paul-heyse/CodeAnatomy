# Design Review: tools/cq/query/

**Date:** 2026-02-16
**Scope:** `tools/cq/query/`
**Focus:** All principles (1-24)
**Depth:** deep (all 25 files)
**Files reviewed:** 25
**Total LOC:** 9,979

## Executive Summary

The CQ query engine exhibits strong architectural layering -- the parse-plan-execute pipeline flows cleanly from `ir.py` through `parser.py` to `planner.py` to `executor.py`, with immutable frozen IR and plan objects providing solid boundaries. The primary weaknesses are: (1) the main `executor.py` file at 1,216 LOC conflates cache orchestration, result assembly, summary mutation, and both entity/pattern execution paths; (2) duplicated knowledge exists across `executor.py` and `executor_ast_grep.py` (record-to-cache conversion, sort keys); and (3) CQS violations pervade the result-building layer where functions both mutate `CqResult.summary`/`key_findings` and implicitly return control flow signals. The strongest alignment is in the IR layer (`ir.py`, `language.py`) which makes illegal states unrepresentable with frozen msgspec structs, validated Literal types, and `__post_init__` enforcement.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | ast-grep-py `SgRoot`/`SgNode` types leak into 6+ modules |
| 2 | Separation of concerns | 1 | medium | medium | `executor.py` mixes cache orchestration, result assembly, summary mutation, entity/pattern dispatch |
| 3 | SRP (one reason to change) | 1 | medium | medium | `executor.py` changes for cache policy, result shape, entity logic, pattern logic, and multi-lang merge |
| 4 | High cohesion, low coupling | 2 | small | low | Good module grouping; `executor.py` imports from 25+ modules |
| 5 | Dependency direction | 2 | small | low | Core IR depends on nothing external; executor correctly depends inward |
| 6 | Ports & Adapters | 2 | medium | low | Cache backend accessed via `get_cq_cache_backend` but no formal port for ast-grep scanner |
| 7 | DRY (knowledge) | 1 | medium | medium | `extract_call_target` duplicated in `finding_builders.py` and `symbol_resolver.py`; cache record converters duplicated |
| 8 | Design by contract | 2 | small | low | IR structs use `__post_init__` validation; parser raises `QueryParseError` |
| 9 | Parse, don't validate | 3 | - | - | Parser converts raw strings to structured IR once at boundary |
| 10 | Make illegal states unrepresentable | 2 | small | low | `Query.__post_init__` enforces XOR of entity/pattern; Literal types constrain enum values; but `Query.entity=None, pattern_spec=None` is constructible until post_init fires |
| 11 | CQS | 1 | medium | medium | `_apply_entity_handlers`, `_process_def_query`, `process_import_query` both mutate result and return None |
| 12 | DI + explicit composition | 2 | small | low | `QueryExecutionContext` bundles dependencies; `Toolchain` injected; but `SymtableEnricher` created inline |
| 13 | Composition over inheritance | 3 | - | - | No class hierarchies; all behavior composed via functions and structs |
| 14 | Law of Demeter | 2 | small | low | `state.ctx.plan.sg_rules`, `state.ctx.query.scope_filter` chains in executor |
| 15 | Tell, don't ask | 1 | medium | medium | `result.summary["matches"] = len(result.key_findings)` pattern exposes raw data for external logic |
| 16 | Functional core, imperative shell | 2 | medium | low | IR/parser/planner are pure; executor mixes IO with result assembly |
| 17 | Idempotency | 3 | - | - | Query execution is read-only over source files; cache writes are content-hash keyed |
| 18 | Determinism / reproducibility | 3 | - | - | Records sorted deterministically; `assemble_pattern_output` orders by file path then position |
| 19 | KISS | 2 | small | low | Cache fragment orchestration adds complexity but serves a real performance need |
| 20 | YAGNI | 2 | small | low | `executor_bytecode.py` and `executor_cache.py` are empty placeholders |
| 21 | Least astonishment | 2 | small | low | `_record_to_cache_record` vs `record_to_cache_record` naming inconsistency across modules |
| 22 | Declare public contracts | 2 | small | low | `__all__` present on all modules; `ExecutePlanRequestV1` versioned |
| 23 | Design for testability | 2 | medium | medium | `_build_entity_fragment_context` directly calls `get_cq_cache_backend`; hard to test without real cache |
| 24 | Observability | 1 | medium | medium | No structured logging in executor; `enrichment.py` uses stdlib `logging` but executor is silent |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The IR layer (`ir.py`) provides excellent information hiding -- the `PatternSpec.to_yaml_dict()` and `RelationalConstraint.to_ast_grep_dict()` methods encapsulate ast-grep wire format details. The `Query` struct exposes only domain-level concepts.

**Findings:**
- `executor_ast_grep.py:15-16` directly imports `Config`, `Rule`, `SgRoot` from `ast_grep_py`, and these types propagate through `AstGrepRuleContext.node: SgNode` (`executor_ast_grep.py:115`), `AstGrepMatchSpan.match: SgNode` (`executor_ast_grep.py:127`), and `batch_spans.py:8`. Six modules directly reference ast-grep-py internals.
- `executor.py:14` imports `SgRecord` from `tools.cq.astgrep.sgpy_scanner` -- this is a scan-layer type that the executor must manipulate field-by-field (`executor.py:503-528`).
- `planner.py:41-95` re-expresses ast-grep YAML rule structure in `AstGrepRule` fields like `inside_stop_by`, `has_field` -- this does hide the wire format behind a typed struct, which is good.

**Suggested improvement:**
Introduce a thin `AstGrepAdapter` protocol (or module-level functions in `executor_ast_grep.py`) that wraps `SgRoot`/`SgNode` interactions. Other modules would call `execute_rules_on_file(path, rules, lang)` instead of directly constructing `SgRoot(src, lang).root()`. This would confine `ast_grep_py` imports to one file.

**Effort:** medium
**Risk if unaddressed:** low -- the current coupling is stable because ast-grep-py's API is unlikely to change frequently, but it limits the ability to swap scanning backends.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
The parse-plan-execute pipeline is cleanly separated at the macro level. However, `executor.py` conflates multiple concerns.

**Findings:**
- `executor.py:314-448` implements cache fragment orchestration for entity queries (building fragment context, probing cache, scanning misses, persisting writes). This is ~135 lines of cache plumbing interleaved with scan logic.
- `executor.py:875-895` (`_execute_entity_query`) mixes: state preparation, result creation, handler dispatch, summary mutation, insight attachment, cache metric snapshots -- six distinct concerns in one function.
- `executor.py:944-986` (`_execute_pattern_query`) duplicates the same result-assembly pattern: create result, update summary, extend findings, filter by scope, apply limit, count matches, add explain, finalize summary, snapshot cache.
- `executor.py:989-1049` (`execute_pattern_query_with_files`) is nearly identical to `_execute_pattern_query` with minor differences in file source, violating DRY.

**Suggested improvement:**
Extract cache fragment orchestration into a dedicated module (the placeholder `executor_cache.py` already exists). Extract result assembly into a `result_assembly.py` module with a `finalize_entity_result(state, result)` and `finalize_pattern_result(state, result)` function. The entity and pattern execution paths share a common finalization shape that could be unified.

**Effort:** medium
**Risk if unaddressed:** medium -- new execution paths (e.g., a third query mode) would require duplicating the same assembly pattern, increasing maintenance burden.

---

#### P3. SRP -- Alignment: 1/3

**Current state:**
`executor.py` changes for at least five independent reasons: cache policy changes, result schema changes, entity query logic, pattern query logic, and multi-language orchestration.

**Findings:**
- `executor.py` has 34 private functions and 5 public functions across 1,216 lines. A single change reason analysis:
  - Cache logic (lines 314-528): changes when cache format/policy evolves
  - Entity execution (lines 603-941): changes when entity query semantics evolve
  - Pattern execution (lines 650-1049): changes when pattern matching evolves
  - Multi-lang (lines 801-859): changes when language scope semantics evolve
  - Summary assembly (lines 225-301): changes when output contract evolves
- `executor_ast_grep.py` at 1,202 lines also has mixed concerns: cache fragment management (lines 144-463), ast-grep-py execution (lines 520-601), match conversion (lines 700-957), and prefilter logic (lines 466-517).

**Suggested improvement:**
Split `executor.py` into:
1. `executor_entity.py` -- entity query state preparation and result assembly
2. `executor_pattern.py` -- pattern query state preparation and result assembly
3. `executor_cache.py` -- cache fragment orchestration (currently a placeholder)
4. `executor.py` -- thin dispatch facade (`execute_plan` and auto-scope routing)

**Effort:** medium
**Risk if unaddressed:** medium -- the file is already the largest in the module and growing. Each new feature (e.g., new expander kind, new cache namespace) adds more to this single file.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Module boundaries are mostly well-drawn. Related concepts live together (IR structs in `ir.py`, parsing in `parser.py`, scan indexing in `scan.py`).

**Findings:**
- `executor.py` imports from 25+ distinct modules, creating a high fan-in. The import block spans lines 6-148.
- `executor.py:77-115` re-imports functions from `executor_ast_grep.py`, `executor_definitions.py`, and `finding_builders.py` with underscore prefixes (`collect_match_spans as _collect_match_spans`), suggesting these are implementation details that should be called via a narrower interface.
- `finding_builders.py` and `section_builders.py` share significant conceptual overlap (both build `Finding` objects from `SgRecord` data) but are cleanly split by concern (finding-level vs section-level).

**Suggested improvement:**
Reduce `executor.py` import surface by introducing a small execution facade that re-exports the key orchestration functions. The underscore-aliased imports (lines 77-115) indicate the module is acting as an integration layer that knows too many implementation details.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency graph flows correctly inward: `executor.py` -> `planner.py` -> `ir.py`. The IR layer (`ir.py`) depends only on `msgspec` and `language.py`. The parser depends only on the IR.

**Findings:**
- `ir.py:10-17` depends only on `msgspec` and `tools.cq.query.language` -- excellent.
- `parser.py:20-38` depends only on `ir.py` and `language.py` -- excellent.
- `planner.py:14-27` depends only on `ir.py` and `language.py` -- excellent.
- `executor.py:14-148` depends on cache, bootstrap, multilang, pathing, schema, scoring, search, and index modules -- this is the outermost shell, so the direction is correct.
- `enrichment.py:16` depends on `tools.cq.introspection.symtable_extract` -- an extraction-layer dependency reaching into the CQ introspection module, which is appropriate.

**Suggested improvement:**
No structural changes needed. The dependency direction is sound. The only minor concern is that `entity_front_door.py` reaches into `tools.cq.search.semantic.models` (line 36-41), coupling entity query output to the search semantic subsystem.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The cache backend is accessed via `get_cq_cache_backend()` which returns a `CqCacheBackend` instance -- effectively a port. The `Toolchain` struct acts as a capability port (`tc.has_sgpy`). However, there is no formal port for the ast-grep scanner itself.

**Findings:**
- `executor.py:604` checks `ctx.tc.has_sgpy` -- capability gating via the Toolchain port.
- `executor_ast_grep.py:543` directly calls `SgRoot(src, ctx.lang)` -- the scanning backend is hardcoded rather than injected.
- `sg_parser.py:130` directly calls `scan_files(files, rules, root, lang=lang, prefilter=True)` -- scanner implementation hardcoded.
- Cache backend is well-abstracted: `fragment_ctx.cache.get`, `fragment_ctx.cache.set` at `executor.py:336-388`.

**Suggested improvement:**
If the project ever needs to support alternative AST parsers (e.g., tree-sitter-based matching), extract a `ScannerPort` protocol. For now, the hardcoding is acceptable under YAGNI.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
Several pieces of domain knowledge are duplicated across modules.

**Findings:**
- **`extract_call_target` is duplicated:** `finding_builders.py:232-258` and `symbol_resolver.py:357-380` both implement identical call target extraction logic using the same regex patterns. They are not imports of each other.
- **Cache record converters are duplicated:** `executor.py:503-528` defines `_record_to_cache_record` and `_cache_record_to_record` with identical field-by-field mapping as `executor_ast_grep.py:1020-1055` (`record_to_cache_record` and `cache_record_to_record`). The only difference is the underscore prefix.
- **Sort key functions are duplicated:** `executor.py:531-564` defines `_record_sort_key`, `_finding_sort_key`, `_raw_match_sort_key` while `executor_ast_grep.py:1058-1082` defines `finding_sort_key`, `record_sort_key`, `raw_match_sort_key` with slightly different signatures (3-tuple vs 4-tuple for findings).
- **Entity kind sets are duplicated:** `executor_definitions.py:307-329` defines `function_kinds`, `class_kinds`, `import_kinds` inline within `matches_entity()`. `enrichment.py:277-278` defines similar sets. `planner.py:379-401` defines a third copy as `entity_patterns` and `entity_kinds`.

**Suggested improvement:**
1. Create a canonical `extract_call_target` in `finding_builders.py` and have `symbol_resolver.py` import it.
2. Move cache record converters to a shared location (e.g., `executor_cache.py` or `tools.cq.core.cache.contracts`).
3. Consolidate sort key functions into `shared_utils.py`.
4. Extract entity kind sets into a constant in `ir.py` or a new `entity_kinds.py`.

**Effort:** medium
**Risk if unaddressed:** medium -- drift between duplicates can cause subtle bugs (e.g., adding a new class kind to one set but not the other).

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The IR layer has good contract enforcement. The executor layer has weaker contracts.

**Findings:**
- `ir.py:613-624`: `Query.__post_init__` enforces mutual exclusion of `entity` and `pattern_spec` -- strong invariant.
- `ir.py:369-378`: `RelationalConstraint.__post_init__` validates operator/field_name compatibility.
- `parser.py:49`: `QueryParseError` provides a domain-specific error type for parse failures.
- `planner.py:334`: uses `assert query.pattern_spec is not None` -- this is a debug-only check that vanishes with `-O`. Should be a proper guard.
- `executor.py:884`: `isinstance(state, CqResult)` is used as a flow control mechanism rather than an explicit error return type (union return).

**Suggested improvement:**
Replace `assert` statements in `planner.py:334` and `planner.py:373` with explicit `if` guards that raise `ValueError`. This ensures contract enforcement in optimized builds.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
The parser layer exemplifies this principle. Raw query strings are tokenized once (`_tokenize`), then parsed into strongly-typed `Query` IR objects. After parsing, all downstream code operates on well-formed `Query` and `ToolPlan` structs.

**Findings:**
- `parser.py:222-261`: `parse_query()` converts strings to `Query` in one step.
- `parser.py:414-439`: `_parse_entity()` validates and casts to `EntityType` Literal.
- `planner.py:268-287`: `compile_query()` converts `Query` to `ToolPlan` without re-validation.
- No scattered validation in the executor -- it trusts the IR.

**Suggested improvement:**
None needed. This is well-aligned.

**Effort:** -
**Risk if unaddressed:** -

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
The IR uses `Literal` types and frozen structs to constrain the state space. However, some invalid states are caught at runtime rather than structurally prevented.

**Findings:**
- `ir.py:20`: `EntityType = Literal["function", "class", "method", "module", "callsite", "import", "decorator"]` -- strong type-level constraint.
- `ir.py:553-624`: `Query` allows `entity=None, pattern_spec=None` at the type level -- the XOR is enforced only by `__post_init__` at runtime, not by construction.
- `planner.py:41-95`: `AstGrepRule` has 15 optional fields, many of which are semantically exclusive (e.g., `inside`/`precedes`/`follows` are typically mutually exclusive for a single constraint). The struct does not prevent setting all simultaneously.
- `ir.py:245-300`: `PatternSpec` correctly uses `requires_yaml_rule()` to determine behavior from its own state rather than exposing raw booleans.

**Suggested improvement:**
Consider modeling `Query` as a tagged union (e.g., `EntityQuery | PatternQuery`) instead of one struct with two optional fields. This would make the XOR structural. However, the `with_scope()` / `with_expand()` builder methods would need to work across both variants, so the current approach may be a reasonable trade-off.

**Effort:** small
**Risk if unaddressed:** low -- the `__post_init__` check is reliable.

---

#### P11. CQS -- Alignment: 1/3

**Current state:**
Many executor-layer functions violate CQS by both mutating `CqResult` objects and implicitly returning state or control flow information.

**Findings:**
- `executor.py:679-703` (`_apply_entity_handlers`): Mutates `result.key_findings` and `result.summary` in place, returns `None`. This is a command masquerading as a procedure.
- `executor_definitions.py:39-79` (`process_import_query`): Directly mutates `result.key_findings` and `result.summary["total_imports"]`, `result.summary["matches"]`.
- `executor_definitions.py:82-136` (`process_def_query`): Mutates result via `append_definition_findings`, then calls `append_def_query_sections` and `append_expander_sections`, all of which mutate the result in place.
- `executor.py:269-287` (`_finalize_single_scope_summary`): Mutates `result.summary` in place.
- `entity_front_door.py:70-127` (`attach_entity_front_door_insight`): Mutates `result.summary` with telemetry and insight data.
- `executor.py:603-623` (`_prepare_entity_state`): Returns either `EntityExecutionState | CqResult` -- using the return type as both a success value and an error signal violates CQS because the caller must inspect the type to determine if an error occurred.

**Suggested improvement:**
Adopt a builder pattern for result assembly: `EntityResultBuilder` that accumulates findings, summary entries, and sections, then produces a frozen `CqResult` via `.build()`. This would separate the accumulation (command) from the finalization (query). Alternatively, have mutation functions accept and return the result explicitly rather than mutating it in place.

**Effort:** medium
**Risk if unaddressed:** medium -- the current mutation-based approach makes it difficult to reason about what state `result` is in at any point during execution, and makes testing individual steps harder.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`QueryExecutionContext` and request dataclasses (`EntityQueryRequest`, `PatternQueryRequest`) bundle dependencies explicitly. The `Toolchain` provides capability injection.

**Findings:**
- `execution_context.py:16-26`: `QueryExecutionContext` cleanly bundles all execution dependencies.
- `executor.py:970`: `SymtableEnricher(state.ctx.root)` is created inline rather than injected -- this creates a hidden dependency that cannot be substituted in tests.
- `executor.py:424`: `get_cq_cache_backend(root=resolved_root)` is called directly -- the cache backend is resolved via a module-level function rather than injected.
- `section_builders.py:566-570`: `SymtableEnricher(root)` and `def_to_finding` are imported and called directly within `build_scope_section` -- hidden construction.

**Suggested improvement:**
Add `cache_backend: CqCacheBackend | None = None` and `symtable_enricher: SymtableEnricher | None = None` to `QueryExecutionContext` so these dependencies can be injected in tests. Use the `resolve` pattern only as a fallback when not provided.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
The entire module avoids inheritance. All behavior is composed via functions, frozen structs, and dataclasses. There is not a single class hierarchy in the 25 files.

**Findings:**
- No `class X(Y)` patterns (except `CqStruct` and `msgspec.Struct` as base structs).
- `_EntityQueryState` and `_PatternQueryState` in `parser.py` are independent dataclasses with a shared `.build()` method -- composition via parallel structure rather than inheritance.

**Suggested improvement:**
None needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most access patterns use direct collaborators. Some chained access exists in the executor.

**Findings:**
- `executor.py:685`: `state.ctx.query` -- two-level chain through execution state to get the query.
- `executor.py:737`: `query.pattern_spec.strictness if query.pattern_spec else None` -- chained attribute access with guard.
- `section_builders.py:169-177`: `finding.details.get("caller_count")` then `isinstance` check -- asking the finding for raw data, then making decisions externally.
- `entity_front_door.py:137-141`: `_detail_int(finding, "caller_count")` -- helper function used to extract data from finding details, which is a reasonable mitigation.

**Suggested improvement:**
The `state.ctx.query` and `state.ctx.plan` chains are sufficiently shallow and stable to be acceptable. The `finding.details.get(...)` pattern is more concerning because `details` is an untyped `dict[str, object]` -- consider adding typed accessor methods to `Finding` for common fields, or creating a `DefinitionFindingDetails` typed struct.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
The result assembly pattern heavily follows an "ask" pattern: functions query raw data from records and findings, then make decisions externally.

**Findings:**
- `executor.py:981`: `result.summary["matches"] = len(result.key_findings)` -- asking the result for its findings count, then writing it back into the summary. This should be the result's responsibility.
- `executor_definitions.py:258-289` (`filter_to_matching`): Asks each record for its `kind`, then checks membership in sets. This is acceptable for a filtering function.
- `executor.py:1052-1131` (`_process_decorator_query`): Reads source code, parses decorators, checks filter criteria, builds findings -- 80 lines of logic operating on data pulled from records and findings. The decorator query handler is essentially reimplementing decorator analysis by asking records for their data.
- `entity_front_door.py:131-195` (`_build_candidate_neighborhood`): Pulls `caller_count`, `callee_count`, `enclosing_scope` from `finding.details` dict -- heavily asking rather than telling.

**Suggested improvement:**
Have `ScanContext` or `EntityCandidates` expose methods like `callers_for(def_record)` and `callees_within(def_record)` instead of exposing raw `calls_by_def` dicts. The decorator query handler could be extracted into a `DecoratorQueryProcessor` with encapsulated filtering logic.

**Effort:** medium
**Risk if unaddressed:** medium -- the ask pattern leads to duplicated conditional logic when the same data is queried from multiple call sites.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The inner layers (IR, parser, planner) are pure functions -- no IO, no mutation, fully deterministic. The executor layer mixes IO (file reads, cache reads/writes, ast-grep scanning) with result construction.

**Findings:**
- `ir.py`, `parser.py`, `planner.py` are fully pure -- excellent functional core.
- `metavar.py` is pure -- filter functions and regex extraction with no side effects.
- `executor_ast_grep.py:538-539`: `file_path.read_text(encoding="utf-8")` -- file IO embedded within the matching loop.
- `executor.py:368-388`: Cache write operations embedded within the scan-and-assemble flow.
- `enrichment.py:339`: `file_path.read_text(encoding="utf-8")` -- file IO within the enrichment function.

**Suggested improvement:**
The IO concentration could be improved by separating file content acquisition from matching. Pass pre-read content to matching functions rather than having them read files. However, this may increase memory pressure for large scans, so the trade-off should be evaluated. The current approach is pragmatically acceptable.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Query execution is inherently idempotent. It reads source files and produces results without modifying source state. Cache writes are content-hash keyed, so re-running produces the same cache entries.

**Findings:**
- `executor_ast_grep.py:279-281`: Cache keys include `rules_digest` (SHA-256 of rule specs) and content hashes -- writes are deterministic.
- `executor.py:454`: Content hash via `file_content_hash(file_path).digest` ensures idempotent cache behavior.
- No mutation of source files anywhere in the module.

**Suggested improvement:**
None needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Results are deterministically ordered. All sorting uses stable, multi-key sort functions.

**Findings:**
- `executor_ast_grep.py:460-463`: `assemble_pattern_output` sorts findings, records, and raw matches by stable keys.
- `executor.py:499`: `ordered_records.sort(key=_record_sort_key)` ensures deterministic record ordering.
- `executor_ast_grep.py:307`: Paths sorted by POSIX representation: `sorted(paths, key=lambda item: item.as_posix())`.
- `sg_parser.py:315`: `sorted(result.files, key=lambda path: path.as_posix())` -- files returned in deterministic order.

**Suggested improvement:**
None needed.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The parse-plan-execute pipeline is elegantly simple. The cache fragment system adds complexity but serves a real performance need.

**Findings:**
- `parser.py:159-168` (`_apply_token_handlers`): A table-driven token handler pattern that is pleasantly simple.
- `executor.py:314-448` (`_scan_entity_records` + cache logic): 135 lines of cache fragment orchestration with `FragmentRequestV1`, `FragmentProbeRuntimeV1`, `FragmentPersistRuntimeV1` -- complex but necessary for performance.
- `executor_ast_grep.py:144-259` (`execute_ast_grep_rules`): Similarly complex cache orchestration for pattern queries.
- `planner.py:456-559`: The relational constraint application uses a mutable `_RelationalState` dataclass with per-operator application functions -- straightforward but verbose.

**Suggested improvement:**
The cache orchestration in both `executor.py` and `executor_ast_grep.py` follows the same fragment probe/miss/persist pattern. A shared `execute_with_cache_fragments(entries, probe, compute_miss, persist)` higher-order function could reduce boilerplate by ~100 lines while making the pattern explicit.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions are justified by actual usage. Two placeholder files suggest speculative structure.

**Findings:**
- `executor_bytecode.py:1-11`: Empty placeholder with comment "will be implemented here when needed." 11 lines of nothing.
- `executor_cache.py:1-11`: Empty placeholder with comment "will remain in executor.py for now." 11 lines of nothing.
- `executor_metavars.py:1-21`: Pure re-export of `metavar.py` functions. Provides no additional value beyond an import alias.
- `ir.py:499-535` (`JoinTarget`, `JoinConstraint`): Join infrastructure exists in the IR but appears minimally used in the executor. This may be forward-looking infrastructure.

**Suggested improvement:**
Delete `executor_bytecode.py` and `executor_cache.py` -- they serve no purpose and create false expectations about module organization. Delete `executor_metavars.py` -- it is a pure re-export with no added value. Consumers should import from `metavar.py` directly.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs generally behave as expected. Naming is mostly consistent.

**Findings:**
- `executor.py:503`: `_record_to_cache_record` (private, underscore prefix) vs `executor_ast_grep.py:1020`: `record_to_cache_record` (public, no prefix). Identical functions with different visibility -- confusing.
- `executor.py:531-564`: Sort key functions return 4-element tuples. `executor_ast_grep.py:1058-1082`: Sort key functions return 3-element tuples. Same conceptual operation, different signatures.
- `executor.py:603`: `_prepare_entity_state` returns `EntityExecutionState | CqResult` -- the CqResult branch represents an error. Using a result type for error signaling is unexpected; callers must `isinstance` check.
- `parse_query` in `parser.py:222` returns `Query` and raises `QueryParseError` -- this is the expected behavior.

**Suggested improvement:**
Standardize sort key functions in one location. For the `_prepare_entity_state` return type, consider using a dedicated `PrepareResult = EntityExecutionState | ErrorResult` type alias to make the error path explicit.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
All modules have `__all__` lists. Request/response types use `V1` suffixes for versioning.

**Findings:**
- `executor.py:188-196`: `ExecutePlanRequestV1` -- versioned request contract.
- `executor_ast_grep.py:76-89`: `PatternFragmentContext` -- not versioned.
- `__init__.py:27-42`: Clean `__all__` declaration of public API.
- `ir.py` has no `__all__` but exports are controlled via `__init__.py`.
- `executor_definitions.py:708-730`: Comprehensive `__all__` list.

**Suggested improvement:**
Add `__all__` to `ir.py` and version contract types that cross module boundaries (e.g., `PatternFragmentContext` -> `PatternFragmentContextV1` if it participates in caching or serialization).

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The IR/parser/planner layers are trivially testable (pure functions). The executor layer requires real cache backends and filesystem access.

**Findings:**
- `parser.py`: All functions are pure -- `parse_query("entity=function name=foo")` can be tested with no setup.
- `planner.py`: `compile_query(query)` is pure -- testable with no dependencies.
- `executor.py:424`: `get_cq_cache_backend(root=resolved_root)` is a module-level factory call -- cannot be easily stubbed without monkeypatch.
- `executor_ast_grep.py:543`: `SgRoot(src, ctx.lang)` -- direct construction of scanner, not injectable.
- `enrichment.py:389-397`: `SymtableEnricher._get_scope_graph` reads files from disk -- requires real filesystem for testing.
- `executor.py:864-872` (`_attach_entity_insight`): Calls `resolve_runtime_services(root)` which resolves global services -- hard to test in isolation.

**Suggested improvement:**
Make `QueryExecutionContext` accept optional `cache_backend` and `services` parameters. When provided, the executor uses them directly; when absent, it resolves them via the current module-level functions. This enables clean DI for tests while preserving the current ergonomics for production callers.

**Effort:** medium
**Risk if unaddressed:** medium -- integration-level tests are currently needed for what should be unit-testable logic.

---

#### P24. Observability -- Alignment: 1/3

**Current state:**
The executor layer has almost no structured logging or tracing. The enrichment module uses stdlib `logging` but the main execution path is silent.

**Findings:**
- `enrichment.py:26`: `logger = logging.getLogger(__name__)` -- used for syntax error reporting and debug messages.
- `executor.py`: No `logger` instance. No structured logging for query execution timing, cache hit/miss ratios, or scan metrics.
- `executor_ast_grep.py`: No `logger` instance. File read failures at line 540-541 silently `continue` without any logging.
- `sg_parser.py`: No logging for cache operations or file inventory.
- Cache telemetry is captured via `record_cache_get` / `record_cache_set` (imported from `tools.cq.core.cache.telemetry`) but there are no trace spans for the overall query execution lifecycle.
- `entity_front_door.py` records semantic telemetry via the `EntitySemanticTelemetry` dataclass but does not emit structured logs.

**Suggested improvement:**
Add a `logger` to `executor.py` and `executor_ast_grep.py`. Log at `DEBUG` level: query plan compilation, file scan count, cache hit/miss ratio, match count, execution duration. Log at `WARNING` level: file read failures (currently silently skipped), scan errors. Consider adding OpenTelemetry spans for `execute_plan`, `_execute_entity_query`, and `_execute_pattern_query` to enable distributed tracing when CQ is used as a library.

**Effort:** medium
**Risk if unaddressed:** medium -- silent failures (e.g., unreadable files) make debugging production issues difficult. Without timing data, performance regressions go unnoticed.

---

## Cross-Cutting Themes

### Theme 1: executor.py as God Module

`executor.py` at 1,216 LOC is the integration nexus that touches cache, scanning, result assembly, summary construction, multi-language dispatch, and insight attachment. This is the root cause behind P2 (separation of concerns), P3 (SRP), P4 (import fan-in), and P11 (CQS violations). The existing placeholder files (`executor_cache.py`, `executor_bytecode.py`) acknowledge this problem but have not been acted upon.

**Root cause:** The executor was likely grown organically as features were added, with each new capability layered onto the existing integration point.

**Affected principles:** P2, P3, P4, P11, P23, P24

**Suggested approach:** Decompose into 4 focused modules (entity execution, pattern execution, cache orchestration, dispatch facade) with each module having a single integration point. This unlocks independent testing, clearer logging boundaries, and easier extension.

### Theme 2: Knowledge Duplication Across executor.py and executor_ast_grep.py

Both modules independently implement: cache record conversion (field-by-field mapping between `SgRecord` and `SgRecordCacheV1`), sort key functions (for findings, records, and raw matches), and fragment cache orchestration patterns.

**Root cause:** `executor_ast_grep.py` was extracted from `executor.py` but both retained their own versions of shared utilities rather than consolidating.

**Affected principles:** P7, P21

**Suggested approach:** Extract shared utilities (converters, sort keys) into `shared_utils.py` or a new `cache_converters.py`. Extract the common cache fragment orchestration pattern into a reusable higher-order function.

### Theme 3: Mutation-Based Result Assembly

The result-building pattern throughout the executor layer relies on in-place mutation of `CqResult.summary`, `CqResult.key_findings`, and `CqResult.sections`. This makes it hard to reason about intermediate result states and prevents CQS compliance.

**Root cause:** `CqResult` is a mutable container (not frozen), and the assembly process adds data incrementally through multiple passes.

**Affected principles:** P11, P15, P23

**Suggested approach:** Introduce a builder or accumulator pattern that collects summary entries, findings, and sections separately, then produces the final `CqResult` in one step. This makes the assembly process more transparent and testable.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 | Consolidate duplicated `extract_call_target` from `symbol_resolver.py:357` and `finding_builders.py:232` | small | Eliminates drift risk between two identical implementations |
| 2 | P7 | Consolidate cache record converters from `executor.py:503-528` and `executor_ast_grep.py:1020-1055` | small | Removes 50+ lines of duplicated field mapping |
| 3 | P20 | Delete empty placeholders `executor_bytecode.py`, `executor_cache.py`, `executor_metavars.py` | small | Reduces confusion; stops false module-organization promises |
| 4 | P8 | Replace `assert` guards in `planner.py:334,373` with proper `if` + `ValueError` | small | Ensures contracts hold in optimized Python builds |
| 5 | P24 | Add `logger = logging.getLogger(__name__)` to `executor.py` and `executor_ast_grep.py` with file-read-failure warnings | small | Makes silent failures visible in debug output |

## Recommended Action Sequence

1. **Consolidate duplicated knowledge (P7).** Merge `extract_call_target` into a single authoritative location in `finding_builders.py`; update `symbol_resolver.py` to import it. Merge cache record converters into `shared_utils.py` or a new `cache_converters.py`. Merge sort key functions. This is prerequisite work for later refactoring.

2. **Clean up YAGNI placeholders (P20).** Delete `executor_bytecode.py`, `executor_cache.py`, and `executor_metavars.py`. This simplifies the module surface for subsequent decomposition.

3. **Add observability (P24).** Add loggers to `executor.py` and `executor_ast_grep.py`. Log query execution start/end with duration, cache hit/miss counts, and file scan counts. Log file read failures as warnings rather than silently continuing.

4. **Fix contract enforcement (P8).** Replace `assert` statements in `planner.py` with proper guards.

5. **Extract cache orchestration (P2, P3).** Move the fragment cache probe/miss/persist pattern from `executor.py:314-448` and `executor_ast_grep.py:144-259` into a dedicated `executor_cache.py` module with a shared `execute_with_cache_fragments()` function.

6. **Decompose executor.py (P2, P3, P11).** Split into `executor_entity.py`, `executor_pattern.py`, and a thin `executor.py` facade. Each sub-module owns its result assembly flow.

7. **Introduce result builder (P11, P15).** Create an `EntityResultBuilder` / `PatternResultBuilder` that accumulates findings and summary data, producing a frozen `CqResult` via `.build()`. This eliminates CQS violations in the result assembly path.
