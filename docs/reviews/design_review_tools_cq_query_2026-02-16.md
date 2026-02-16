# Design Review: tools/cq/query/ + tools/cq/astgrep/

**Date:** 2026-02-16
**Scope:** `tools/cq/query/` and `tools/cq/astgrep/`
**Focus:** Boundaries (1-6), Knowledge (7-11), Composition (12-15)
**Depth:** deep (all files)
**Files reviewed:** 29

## Executive Summary

The CQ query engine has a clean, well-layered pipeline from DSL parsing through IR compilation to execution, with strong immutability conventions via `msgspec.Struct, frozen=True`. The principal structural risks are: (1) the cache ceremony duplication between `executor.py` and `executor_ast_grep.py`, which is the single largest DRY violation -- identical fragment orchestration patterns spanning ~150 lines in each module; (2) entity-kind knowledge fragmented across four locations with no single dispatch authority; (3) `dict[str, object]` payloads leaking across module boundaries where typed contracts should enforce shape. The pipeline boundary between parse/plan and execute is architecturally sound, but the executor layer conflates execution, caching, and result assembly into a single module with 7+ responsibilities.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `_EntityFragmentContext` internals exposed via mutable state patterns; `cache_backend: object` on context |
| 2 | Separation of concerns | 1 | large | high | `executor.py` conflates caching, execution, summary building, auto-scope merging, and result finalization |
| 3 | SRP (one reason to change) | 1 | large | high | `executor.py` changes for 7+ independent reasons; `executor_ast_grep.py` mixes cache wiring with AST matching |
| 4 | High cohesion, low coupling | 2 | medium | medium | Both executors import 20+ cache modules; cross-module fan-in is high |
| 5 | Dependency direction | 2 | small | low | Core query IR is dependency-free; executors depend on infrastructure as expected |
| 6 | Ports & Adapters | 1 | medium | medium | No port abstraction for caching; both executors wire fragment runtime directly |
| 7 | DRY (knowledge, not lines) | 0 | medium | high | Cache ceremony duplicated across 2 modules; entity-kind knowledge in 4+ locations; metavar extraction in 2 locations |
| 8 | Design by contract | 2 | small | low | IR types enforce invariants; `Query.__post_init__` validates mutual exclusivity |
| 9 | Parse, don't validate | 2 | small | low | Parser produces typed IR; planner produces typed plan; good boundary hygiene |
| 10 | Make illegal states unrepresentable | 2 | small | medium | `QueryExecutionContext.cache_backend: object` and `symtable_enricher: object` allow any value |
| 11 | CQS | 2 | small | low | Most functions are pure transforms or side-effecting procedures; summary mutation is localized |
| 12 | DI + explicit composition | 1 | medium | medium | Cache backends created inline via `get_cq_cache_backend()`; no injection point for fragment runtime |
| 13 | Prefer composition over inheritance | 3 | - | - | No inheritance hierarchies; all composition via dataclass/struct aggregation |
| 14 | Law of Demeter | 2 | small | low | Minor violations: `result.summary.update(...)`, `finding.details["key"]` chains |
| 15 | Tell, don't ask | 1 | medium | medium | `matches_entity()` asks record for `.kind` then dispatches externally; pattern data assembled by caller interrogation |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The IR layer (`ir.py`) and planner (`planner.py`) successfully hide internal decisions behind frozen struct surfaces. However, execution context types expose implementation holes.

**Findings:**
- `tools/cq/query/execution_context.py:29-30`: `cache_backend: object | None` and `symtable_enricher: object | None` use bare `object` type, leaking the absence of a typed contract. Callers cannot know what capabilities these fields provide without runtime inspection.
- `tools/cq/query/executor.py:194-205`: `_EntityFragmentContext` is a well-scoped frozen dataclass, but its sibling `PatternFragmentContext` at `executor_ast_grep.py:86-99` duplicates the same shape with slight field differences, suggesting neither fully hides the fragment context contract.
- `tools/cq/query/executor.py:266-285`: `_summary_common_for_query()` returns `dict[str, object]`, exposing internal summary structure to all callers rather than using a typed builder.

**Suggested improvement:**
Type the `cache_backend` field as `CqCacheBackend | None` and `symtable_enricher` as `SymtableEnricher | None` on `QueryExecutionContext`. Extract a shared `FragmentContext` protocol or base struct that both entity and pattern fragment contexts implement.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
The parse-IR-plan pipeline has excellent separation. The execution layer does not. `executor.py` (1,223 LOC) intermingles seven distinct concerns.

**Findings:**
- `tools/cq/query/executor.py:340-538`: Cache orchestration (~200 lines) for entity fragment scanning, including context building, entry creation, miss handling, hit assembly, and eviction. This is infrastructure plumbing, not query execution logic.
- `tools/cq/query/executor.py:875-990`: Entity and pattern query execution functions that build results, apply scope filters, attach insights, finalize summaries, and assign finding IDs -- a linear script mixing execution with result assembly.
- `tools/cq/query/executor.py:816-840`: Auto-scope plan execution that orchestrates cross-language merging, summary assembly, and cache eviction in a single function.
- `tools/cq/query/executor.py:1058-1141`: `_process_decorator_query()` reads files from disk (IO), checks entity kinds (domain logic), and builds findings (result assembly) in one function.
- `tools/cq/query/executor_ast_grep.py:155-251`: `execute_ast_grep_rules()` mixes fragment cache wiring with AST scanning dispatch.

**Suggested improvement:**
Extract cache orchestration into a dedicated `query_cache.py` module that exposes two functions: `scan_entity_records(ctx, paths, scope_globs) -> list[SgRecord]` and `scan_pattern_fragments(rules, paths, root, query, globs, run_id) -> tuple[list[Finding], list[SgRecord], list[MatchData]]`. This would reduce `executor.py` by ~200 lines and `executor_ast_grep.py` by ~200 lines. The executors would call the cache module rather than wiring fragment runtimes inline.

**Effort:** large
**Risk if unaddressed:** high -- Any change to caching policy requires editing two modules in lockstep.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`executor.py` changes for at least seven independent reasons: cache policy changes, entity query logic changes, pattern query logic changes, auto-scope behavior changes, summary format changes, result assembly changes, and explain-mode changes.

**Findings:**
- `tools/cq/query/executor.py` has 1,223 lines and 30+ functions spanning cache wiring, entity execution, pattern execution, auto-scope merging, summary building, explain mode, decorator queries, callsite queries, file tabulation, and ripgrep integration.
- `tools/cq/query/executor_ast_grep.py` has 1,174 lines similarly mixing cache ceremony with AST matching logic.
- `tools/cq/query/executor.py:947-990` (`_execute_pattern_query`) and `executor.py:993-1055` (`execute_pattern_query_with_files`) are structurally near-identical, differing only in path resolution -- copy-paste SRP violation.

**Suggested improvement:**
Decompose `executor.py` into:
1. `executor_entity.py` -- entity query execution logic (prepare state, apply handlers, finalize)
2. `executor_pattern.py` -- pattern query execution logic (prepare state, run ast-grep, finalize)
3. `query_cache.py` -- fragment cache orchestration for both entity and pattern queries
4. `executor.py` -- thin facade with `execute_plan()` dispatching to the above

This would give each module a single reason to change.

**Effort:** large
**Risk if unaddressed:** high -- Contributors must understand the full 1,223-line module to make safe changes to any one concern.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Both executors have high fan-in from the cache infrastructure (20+ imports from `tools.cq.core.cache.*`). Internal cohesion within `ir.py`, `parser.py`, and `planner.py` is excellent.

**Findings:**
- `tools/cq/query/executor.py:16-46`: Imports 15 symbols from 11 distinct `tools.cq.core.cache.*` submodules. This import surface is duplicated nearly identically at `executor_ast_grep.py:19-44`.
- `tools/cq/query/executor.py:62-68`: Further coupling to `tools.cq.orchestration.*` and `tools.cq.search.*` for auto-scope merging and diagnostics -- concerns that are not intrinsic to query execution.
- `tools/cq/query/executor_dispatch.py:1-31`: Exists solely to break a circular import between `executor.py` and modules that need to call entity/pattern execution. This is a symptom of over-coupling.

**Suggested improvement:**
Consolidating cache imports behind a facade (`query_cache.py`) would reduce the import surface of both executors from ~40 cache-related symbols to ~2-3 function calls. The `executor_dispatch.py` shim could be eliminated if execution functions lived in separate modules that don't create circular dependencies.

**Effort:** medium
**Risk if unaddressed:** medium -- High coupling increases the blast radius of cache infrastructure changes.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency direction is mostly correct: `ir.py` depends on nothing outside `language.py`; `parser.py` depends on `ir.py`; `planner.py` depends on `ir.py`; executors depend on everything. The core-to-detail direction is respected.

**Findings:**
- `tools/cq/query/ir.py:66-109`: `PYTHON_AST_FIELDS` is a Python-specific constant embedded in the language-agnostic IR module. This is a minor dependency direction violation -- the IR should not contain language-specific AST knowledge.
- `tools/cq/query/planner.py:241-266`: `_ENTITY_RECORDS`, `_ENTITY_EXTRA_RECORDS`, `_EXPANDER_RECORDS`, `_FIELD_RECORDS` encode entity-to-record-type mapping knowledge in the planner rather than in the entity kind registry where it conceptually belongs.

**Suggested improvement:**
Move `PYTHON_AST_FIELDS` to a language-specific constants module (e.g., `tools/cq/query/language.py` or a dedicated `python_constants.py`). The entity-to-record-type mappings in the planner could reference `ENTITY_KINDS` from `entity_kinds.py` for kind-set membership, even if the record-type mapping stays in the planner.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
There is no port abstraction for the cache infrastructure. Both executors wire `FragmentProbeRuntimeV1` and `FragmentPersistRuntimeV1` inline with lambda closures, creating a tight coupling to the specific cache backend implementation.

**Findings:**
- `tools/cq/query/executor.py:361-384`: Inline construction of `FragmentProbeRuntimeV1` and `FragmentPersistRuntimeV1` with lambda wrappers around `fragment_ctx.cache.get`, `.set`, `.set_many`, and `.transact`. This exact pattern is duplicated at `executor_ast_grep.py:206-232`.
- `tools/cq/query/executor.py:431-485`: `_build_entity_fragment_context()` constructs cache backend, resolves policy, builds scope hash, and creates snapshot fingerprint -- all adapter-level concerns hardcoded into the executor.
- `tools/cq/query/executor_ast_grep.py:299-361`: `build_pattern_fragment_context()` performs the same adapter-level construction with slightly different parameters.

**Suggested improvement:**
Define a `QueryCachePort` protocol (or use the existing `run_fragment_scan` as the sole entry point) and build a factory function that constructs the probe/persist runtimes from a fragment context. This would reduce the inline wiring in both executors to a single function call: `scan_result = query_cache.run_entity_scan(ctx, paths, scope_globs)`.

**Effort:** medium
**Risk if unaddressed:** medium -- Adding a new cache backend or changing the fragment protocol requires coordinated edits in two modules.

---

### Category: Knowledge

#### P7. DRY (knowledge, not lines) -- Alignment: 0/3

**Current state:**
This is the most significant design issue in the reviewed scope. Three distinct knowledge categories are duplicated across module boundaries.

**Findings:**

**Cache ceremony duplication (critical):**
- `tools/cq/query/executor.py:340-538` and `tools/cq/query/executor_ast_grep.py:155-393`: Both modules implement the identical fragment caching lifecycle:
  1. Build fragment context (scope hash, snapshot fingerprint, cache policy, tag resolution)
  2. Create fragment entries (content hash, cache key construction per file)
  3. Wire `FragmentProbeRuntimeV1` + `FragmentPersistRuntimeV1` with identical lambda patterns
  4. Call `run_fragment_scan()` with identical dispatch structure
  5. Handle misses (scan, group by file, create writes)
  6. Assemble output from hits + miss payloads

  The two implementations differ only in:
  - Namespace name (`"query_entity_fragment"` vs `"pattern_fragment"`)
  - Payload type (`QueryEntityScanCacheV1` vs `PatternFragmentCacheV1`)
  - The scan function called on misses (`sg_scan` vs `run_ast_grep`)
  - Fragment entry extras (record_types vs rules_digest + query_filters_digest)

  This is ~200 lines of duplicated *knowledge* about how fragment caching works.

**Entity-kind knowledge fragmentation:**
- `tools/cq/core/entity_kinds.py:11-44`: Canonical `EntityKindRegistry` defining `function_kinds`, `class_kinds`, `import_kinds`, `decorator_kinds`.
- `tools/cq/query/ir.py:20`: `EntityType = Literal["function", "class", "method", "module", "callsite", "import", "decorator"]` -- the canonical list of queryable entity types, not derived from the registry.
- `tools/cq/query/planner.py:241-266`: `_ENTITY_RECORDS`, `_ENTITY_EXTRA_RECORDS` mappings from entity type to record types -- a second encoding of which record types each entity needs.
- `tools/cq/query/executor_definitions.py:293-330`: `matches_entity()` contains a third encoding of entity-to-kind mapping via if/elif chain, referencing `ENTITY_KINDS` for some branches but hardcoding `{"function", "async_function"}` for `method` at line 318.
- `tools/cq/query/executor.py:1074-1080`: `_process_decorator_query()` hardcodes kind sets `{"function", "async_function", "function_typeparams", "class", "class_bases"}` instead of using `ENTITY_KINDS.decorator_kinds`.

**Metavariable extraction duplication:**
- `tools/cq/astgrep/sgpy_scanner.py:426-463`: `_extract_metavars()` extracts metavariable captures from `SgNode` matches, using `_node_payload()` and `_is_variadic_separator()` helpers.
- `tools/cq/query/executor_ast_grep.py:834-881`: `extract_match_metavars()` performs the same extraction with slightly different output shape (text-only vs full payload).
- `tools/cq/query/executor_ast_grep.py:810-821`: `node_payload()` duplicates `sgpy_scanner._node_payload()`.
- `tools/cq/query/executor_ast_grep.py:824-831`: `is_variadic_separator()` duplicates `sgpy_scanner._is_variadic_separator()`.

**Suggested improvement:**
1. **Cache ceremony**: Extract a `tools/cq/query/query_cache.py` module that parameterizes the fragment lifecycle by namespace, payload type, and scan function. Both executors call a single `run_cached_scan(config)` function.
2. **Entity kinds**: Add record-type mappings to `EntityKindRegistry` (e.g., `entity_record_types: dict[str, frozenset[str]]`). Have `planner.py` and `executor_definitions.py` derive their mappings from this single registry.
3. **Metavar extraction**: Promote `executor_ast_grep.extract_match_metavars()` as the canonical implementation and have `sgpy_scanner` import it (or extract a shared `metavar_extract.py` module).

**Effort:** medium (cache ~4h, entity kinds ~1h, metavar ~1h)
**Risk if unaddressed:** high -- Knowledge drift between duplicated implementations causes subtle behavioral differences (e.g., the method entity hardcoding `{"function", "async_function"}` at `executor_definitions.py:318` is already inconsistent with `ENTITY_KINDS.function_kinds` which also includes `"function_typeparams"`).

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The IR layer enforces structural contracts well. The Query struct validates mutual exclusivity of entity/pattern_spec in `__post_init__`. Planner output types are frozen and well-constrained.

**Findings:**
- `tools/cq/query/ir.py`: `Query`, `PatternSpec`, `Scope`, `Expander`, `MetaVarCapture`, `RelationalConstraint` -- all frozen structs with constrained Literal types. Good contract hygiene.
- `tools/cq/query/match_contracts.py:1-37`: `MatchData`, `MatchRange`, `MatchRangePoint` are typed msgspec structs replacing previous `dict[str, object]` payloads. Good contract migration in progress.
- `tools/cq/query/executor.py:256-285`: `_summary_common_for_query()` returns `dict[str, object]` with no contract enforcement on shape. The keys `"query"`, `"mode"`, `"python_semantic_overview"`, etc. are stringly-typed with no validation.
- `tools/cq/query/executor_ast_grep.py:424-435`: `pattern_data_from_hits()` validates hit payload shape via runtime `isinstance` checks on tuple length and element types, rather than relying on typed decode.

**Suggested improvement:**
Replace the `dict[str, object]` return of `_summary_common_for_query()` with a typed struct (e.g., `QuerySummaryCommon`) that enforces the expected keys at construction time. This would catch summary shape errors at build time rather than downstream.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The parse-to-IR boundary is well-designed. The parser produces typed, validated IR objects. The planner produces typed plans. Downstream consumers work with structured representations, not raw strings.

**Findings:**
- `tools/cq/query/parser.py`: Converts raw query text into `Query` IR via mutable builder state (`_EntityQueryState`, `_PatternQueryState`), then freezes into immutable structs. Good parse-once pattern.
- `tools/cq/query/planner.py:269-`: `compile_query()` transforms `Query` -> `ToolPlan`, producing structured rule objects rather than raw YAML strings.
- `tools/cq/query/executor_ast_grep.py:691-715`: `execute_rule_matches()` converts typed `AstGrepRule` to YAML dict only at the point of calling `node.find_all(config=config)`. This is a necessary boundary conversion, well-localized.
- `tools/cq/query/executor.py:266-278`: `_summary_common_for_query()` constructs summary data as `dict[str, object]` rather than a parsed struct -- validation deferred to callers.

**Suggested improvement:**
Minor: the summary common data could be returned as a typed struct and converted to dict only at the serialization boundary.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
IR types use Literal unions and frozen structs effectively. Some execution-layer types allow overly permissive states.

**Findings:**
- `tools/cq/query/execution_context.py:29-30`: `cache_backend: object | None` and `symtable_enricher: object | None` can hold any Python object, making it impossible for the type system to prevent misuse.
- `tools/cq/query/executor.py:173-182`: `EntityExecutionState` is a mutable `@dataclass` (no `frozen=True`), allowing post-construction mutation of `records`, `scan`, and `candidates`. This could be frozen.
- `tools/cq/query/executor.py:185-191`: `PatternExecutionState` is similarly mutable without clear reason.
- `tools/cq/query/ir.py:20`: `EntityType` includes `"method"` and `"module"`, but `matches_entity()` at `executor_definitions.py:318-319` treats `"method"` as identical to `"function"` and `"module"` always returns `False`. These are dead states that suggest the Literal type is wider than the actual domain.

**Suggested improvement:**
Type `cache_backend` and `symtable_enricher` with their actual types. Make `EntityExecutionState` and `PatternExecutionState` frozen. Consider whether `"method"` and `"module"` should remain in `EntityType` if they have no distinct behavior.

**Effort:** small
**Risk if unaddressed:** medium -- `object`-typed fields bypass all static analysis.

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Query functions return results without side effects. Command functions mutate state without returning results.

**Findings:**
- `tools/cq/query/executor.py:292-312`: `_finalize_single_scope_summary()` mutates `result.summary` in place (command). Clean CQS.
- `tools/cq/query/executor.py:875-896`: `_execute_entity_query()` both creates a result and mutates it through multiple helper calls before returning. The mutation chain (`result.summary.update(...)`, `_apply_entity_handlers(state, result)`, `_finalize_single_scope_summary(ctx, result)`, `_attach_entity_insight(result)`) is a sequence of commands that also queries state. This is acceptable as an orchestration function but blurs CQS at the individual step level.
- `tools/cq/query/executor_ast_grep.py:556-565`: `run_ast_grep()` mutates `AstGrepExecutionState` in place while iterating files. The mutation is contained by the state object, following the "command with explicit state" pattern.

**Suggested improvement:**
No major changes needed. The pattern of building a result then mutating it through a pipeline of steps is pragmatic for this domain.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
Cache backends and fragment runtimes are created inline via factory functions rather than injected. This makes the executors untestable without the actual cache infrastructure and prevents substituting alternative caching strategies.

**Findings:**
- `tools/cq/query/executor.py:462`: `cache = get_cq_cache_backend(root=resolved_root)` -- inline creation of cache backend inside `_build_entity_fragment_context()`. Not injectable.
- `tools/cq/query/executor_ast_grep.py:345`: Same pattern: `cache=get_cq_cache_backend(root=resolved_root)` inside `build_pattern_fragment_context()`.
- `tools/cq/query/executor.py:361-384`: `FragmentProbeRuntimeV1` and `FragmentPersistRuntimeV1` constructed inline with lambda closures wrapping `fragment_ctx.cache.*` methods. These are runtime strategies that should be injected or constructed by a factory.
- `tools/cq/query/executor.py:790-800`: `QueryExecutionContext` is the natural injection point but uses `cache_backend: object | None` (untyped) and does not thread it to the fragment context builders.

**Suggested improvement:**
Thread `CqCacheBackend` through `QueryExecutionContext` (already has the field, just needs proper typing and wiring). Build a `FragmentRuntimeFactory` that constructs probe/persist runtimes from a cache backend, eliminating the inline lambda construction in both executors.

**Effort:** medium
**Risk if unaddressed:** medium -- Testing either executor requires a real cache backend; mocking requires patching module-level factory functions.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The query engine uses no inheritance hierarchies. All types are composed via dataclass/struct aggregation, function composition, and protocol-based dispatch. This is exemplary.

**Findings:**
- All IR types (`Query`, `PatternSpec`, `Scope`, `Expander`, etc.) are flat structs composed via fields.
- `EntityExecutionState`, `PatternExecutionState`, and `QueryExecutionContext` compose their dependencies via fields rather than inheriting from a base executor class.
- `AstGrepRule.to_yaml_dict()` delegates to helper functions (`_build_pattern_rule`, `_apply_relational_rule`, etc.) rather than using template method inheritance.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most access patterns are through direct collaborators. Minor violations exist in the result assembly layer.

**Findings:**
- `tools/cq/query/executor.py:296`: `common = result.summary.to_dict()` -- reaches through `result` to `summary` to call a method. This is a mild Demeter violation but pragmatic given CqResult's design.
- `tools/cq/query/executor.py:324`: `result.summary.update(_summary_common_for_context(ctx))` -- same pattern throughout: callers reach into `result.summary` to mutate it.
- `tools/cq/query/executor.py:1136-1137`: `finding.details["decorators"] = decorators` and `finding.details["decorator_count"] = count` -- callers reach into `finding.details` dict to add data rather than telling the finding to incorporate decorator information.
- `tools/cq/query/merge.py:119`: `result.summary.front_door_insight = to_public_front_door_insight_dict(insight)` -- direct attribute mutation on a collaborator's collaborator.

**Suggested improvement:**
For the finding details pattern, consider adding a `Finding.add_details(**kwargs)` method or building details at construction time. For summary mutation, this is inherent to CqResult's mutable-summary design and would require a larger architectural change to address.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
Several critical dispatch points ask objects for data and then make decisions externally, rather than telling the object to perform the operation.

**Findings:**
- `tools/cq/query/executor_definitions.py:293-330`: `matches_entity()` interrogates `record.kind` and dispatches through an if/elif chain. The record does not know how to match against an entity type -- the knowledge is external. This is the canonical "ask" anti-pattern: ask for kind, then decide.
- `tools/cq/query/executor.py:576-581`: Entity type string checked via `query.entity in {"function", "class", "method", "decorator"}` to decide which records to filter. The query does not provide a method to determine its applicable record category.
- `tools/cq/query/executor.py:689-702`: `_apply_entity_handlers()` checks `query.entity` via if/elif to dispatch to `_process_import_query`, `_process_decorator_query`, `_process_call_query`, or `_process_def_query`. This is stringly-typed dispatch that should be table-driven.
- `tools/cq/query/executor_ast_grep.py:424-435`: `pattern_data_from_hits()` interrogates hit payload shape via `isinstance(payload, tuple) and len(payload) == 3` -- asking the payload what it is rather than having a typed decode path.
- `tools/cq/query/executor.py:1074-1080`: `_process_decorator_query()` checks `def_record.kind not in {"function", "async_function", ...}` instead of asking a kind registry or the record itself.

**Suggested improvement:**
1. Replace `matches_entity()` with a dispatch table: `ENTITY_MATCHER: dict[str, Callable[[SgRecord], bool]]` keyed by entity type, where each callable encapsulates the matching logic using `ENTITY_KINDS`.
2. Replace the `_apply_entity_handlers()` if/elif chain with a handler registry: `ENTITY_HANDLERS: dict[str, Callable[[EntityExecutionState, CqResult, ...], None]]`.
3. Have `ENTITY_KINDS` provide a `matches(entity_type, record_kind) -> bool` method, centralizing the dispatch knowledge.

**Effort:** medium
**Risk if unaddressed:** medium -- Every new entity type requires coordinated changes across `ir.py`, `planner.py`, `executor_definitions.py`, and `executor.py`.

---

## Cross-Cutting Themes

### Theme 1: Cache Ceremony as Architectural Concern

The fragment caching lifecycle (context building, entry creation, probe/persist runtime wiring, miss handling, output assembly) is replicated in both `executor.py` and `executor_ast_grep.py`. This is not merely code duplication -- it is **knowledge duplication** about how the fragment cache protocol works. Any change to the fragment caching contract (new fields, different eviction policy, changed key structure) must be applied in both places.

**Root cause:** The fragment cache was not designed as a reusable abstraction parameterized by payload type and scan function. Instead, each consumer re-implements the full lifecycle.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P6 (ports & adapters), P7 (DRY), P12 (DI).

**Suggested approach:** Create a `QueryFragmentCacheService` that encapsulates the full lifecycle, parameterized by:
- Namespace name
- Payload type (for decode)
- Scan function (for miss handling)
- Entry extras (for cache key construction)

Both executors would call this service with their specific parameters rather than re-implementing the protocol.

### Theme 2: Entity-Kind Knowledge Fragmentation

Entity-type knowledge is encoded in four+ locations with no single authoritative dispatch. The `EntityKindRegistry` at `core/entity_kinds.py` was created to centralize kind sets, but downstream consumers partially use it and partially hardcode their own sets.

**Root cause:** The registry was introduced after the executor and planner were already built with inline kind knowledge. Migration was incomplete.

**Affected principles:** P7 (DRY), P10 (illegal states), P15 (tell don't ask).

**Suggested approach:** Extend `EntityKindRegistry` to be the sole authority for:
1. Kind-set membership (`function_kinds`, `class_kinds`, etc.) -- already done
2. Entity-to-record-type mappings (currently in `planner.py:241-266`)
3. Entity matching dispatch (currently in `executor_definitions.py:293-330`)
4. A `matches(entity_type: str, record_kind: str) -> bool` method replacing external if/elif chains

### Theme 3: dict[str, object] as Cross-Boundary Contract

Multiple functions return or accept `dict[str, object]` where a typed contract would be more appropriate. This prevents static analysis from catching key typos, missing fields, or shape mismatches.

**Root cause:** The CqResult.summary type is itself a dynamic dict-like object, so upstream code naturally produces dicts to feed into it.

**Affected principles:** P1 (information hiding), P8 (design by contract), P9 (parse don't validate), P10 (illegal states).

**Instances:**
- `executor.py:260` -- `_summary_common_for_query() -> dict[str, object]`
- `executor_ast_grep.py:810-821` -- `node_payload() -> dict[str, object]`
- `executor.py:1136` -- `finding.details["decorators"] = decorators`
- `planner.py:118` -- `AstGrepRule.to_yaml_dict() -> dict[str, object]` (necessary for ast-grep-py API boundary)

The `to_yaml_dict()` case is justified because ast-grep-py expects untyped dicts. The others should be typed.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate `node_payload()` and `is_variadic_separator()` -- `executor_ast_grep.py:810-831` duplicates `sgpy_scanner.py:412-423`. Import from one canonical location. | small | Eliminates 2 function duplications and prevents behavioral drift |
| 2 | P10 | Type `cache_backend` and `symtable_enricher` fields on `QueryExecutionContext` (`execution_context.py:29-30`) with their actual types instead of `object`. | small | Enables static analysis across the execution pipeline |
| 3 | P7 (DRY) | Unify decorator kind checking -- replace `executor.py:1074-1080` hardcoded kind set with `ENTITY_KINDS.decorator_kinds`. | small | Single source of truth for decorator-eligible kinds |
| 4 | P5 | Move `PYTHON_AST_FIELDS` from `ir.py:66-109` to `language.py` or a dedicated constants module. | small | Removes Python-specific knowledge from language-agnostic IR |
| 5 | P15 | Add `EntityKindRegistry.matches(entity_type, record_kind) -> bool` method, replace `matches_entity()` if/elif chain at `executor_definitions.py:293-330`. | small | Centralizes entity matching logic in the registry |

## Recommended Action Sequence

1. **Quick wins (P7, P10, P5)**: Apply the five quick wins above. These are independent, low-risk changes that immediately improve knowledge centralization and type safety. (~2 hours total)

2. **Entity-kind consolidation (P7, P15)**: Extend `EntityKindRegistry` with record-type mappings and a `matches()` method. Update `planner.py:241-266` to derive `_ENTITY_RECORDS` from the registry. Update `executor_definitions.py:293-330` to use the registry's `matches()`. Update `executor.py:1074-1080` to use `ENTITY_KINDS.decorator_kinds`. (~2 hours)

3. **Metavar extraction unification (P7)**: Choose `executor_ast_grep.extract_match_metavars()` as the canonical implementation. Have `sgpy_scanner.py` import it instead of maintaining `_extract_metavars()`. Ensure the output shapes are compatible or provide an adapter. (~1 hour)

4. **Cache ceremony extraction (P2, P3, P6, P7, P12)**: Create `tools/cq/query/query_cache.py` with a parameterized `run_cached_scan()` function. Migrate entity fragment scanning from `executor.py:340-538` and pattern fragment scanning from `executor_ast_grep.py:155-393`. Thread `CqCacheBackend` through `QueryExecutionContext`. (~4 hours)

5. **Executor decomposition (P2, P3)**: After step 4, the remaining executor code is manageable enough to split into `executor_entity.py` and `executor_pattern.py`, with `executor.py` as a thin facade. This depends on step 4 being complete first. (~3 hours)

6. **Summary typing (P8, P9)**: Replace `_summary_common_for_query() -> dict[str, object]` with a typed `QuerySummaryCommon` struct. This is lower priority because the summary contract is shared with the broader CQ infrastructure and may require coordinated changes outside this scope. (~2 hours)
