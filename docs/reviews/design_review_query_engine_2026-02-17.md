# Design Review: Query Engine + Index + AST-Grep + Introspection

**Date:** 2026-02-17
**Scope:** `tools/cq/query/` (30 files), `tools/cq/index/` (9 files), `tools/cq/astgrep/` (4 files), `tools/cq/introspection/` (4 files), `tools/cq/utils/` (4 files)
**Focus:** All principles (1-24)
**Depth:** moderate
**Files reviewed:** 20 of 51 (entry points, dispatch, IR, executors, public interfaces, all index/astgrep/introspection/utils)

## Executive Summary

The query engine layer demonstrates strong IR design with a well-structured `Query` -> `ToolPlan` -> `Executor` pipeline, frozen msgspec contracts, and clean parse-don't-validate boundaries. The main structural weaknesses are: (1) `executor_runtime.py` at ~1100 LOC is a God module concentrating scan, dispatch, caching, summary, and execution concerns; (2) two parallel dispatch hierarchies (`executor_dispatch.py` vs `executor.py`) re-export the same functions through unnecessary indirection layers; and (3) CQS is violated in several executor functions that both mutate result objects and return values. The index, astgrep, introspection, and utils sub-packages are well-bounded with clear single responsibilities.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `executor.py` re-exports private `_`-prefixed functions as public API |
| 2 | Separation of concerns | 1 | medium | medium | `executor_runtime.py` mixes scan, cache, dispatch, summary, and execution |
| 3 | SRP (one reason to change) | 1 | medium | medium | `executor_runtime.py` changes for cache, scan, summary, and execution reasons |
| 4 | High cohesion, low coupling | 2 | medium | low | Good IR/planner cohesion; executor modules have high inter-module coupling |
| 5 | Dependency direction | 2 | small | low | Core IR is dependency-free; executors correctly depend inward |
| 6 | Ports & Adapters | 2 | small | low | `SymbolIndex` protocol exists; ast-grep-py adapter well-separated |
| 7 | DRY (knowledge) | 1 | medium | medium | Entity pattern maps duplicated between planner and executor_definitions |
| 8 | Design by contract | 2 | small | low | Query `__post_init__` enforces mutual exclusion; some executor inputs unchecked |
| 9 | Parse, don't validate | 3 | - | - | Excellent: parser produces frozen Query IR; downstream operates on typed data |
| 10 | Make illegal states unrepresentable | 2 | medium | low | Query allows entity+composite without validation; `_EntityQueryState`/`_PatternQueryState` duplication |
| 11 | CQS | 1 | medium | medium | Multiple executor functions mutate result AND return values |
| 12 | DI + explicit composition | 2 | small | low | `QueryExecutionContext` bundles dependencies; some inline construction |
| 13 | Composition over inheritance | 3 | - | - | No class hierarchies; all behavior via composition |
| 14 | Law of Demeter | 2 | small | low | `state.ctx.query.scope_filter` chains in executor_runtime |
| 15 | Tell, don't ask | 2 | small | low | Finding details accessed via `.get()` throughout entity_front_door |
| 16 | Functional core, imperative shell | 2 | medium | low | IR/planner are pure; executors mix IO and logic |
| 17 | Idempotency | 3 | - | - | Frozen IR + deterministic plans; cache is content-addressed |
| 18 | Determinism / reproducibility | 3 | - | - | Sorted outputs, deterministic scan ordering, content-hashed cache |
| 19 | KISS | 2 | small | low | Pattern fragment caching adds significant complexity |
| 20 | YAGNI | 2 | small | low | `batch_spans.py` exists but appears minimally used |
| 21 | Least astonishment | 1 | small | medium | Two dispatch facades (`executor.py` and `executor_dispatch.py`) with overlapping names |
| 22 | Public contracts | 2 | small | low | `__all__` exported consistently; version suffixes on contracts |
| 23 | Design for testability | 2 | medium | medium | Pure IR/planner testable; executors need full context to test |
| 24 | Observability | 2 | small | low | Logger present; structured telemetry in entity_front_door; no span tracing in executors |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The IR module (`ir.py`) and planner (`planner.py`) have clean stable surfaces. However, `executor.py` publicly re-exports private `_`-prefixed functions from `executor_runtime.py`.

**Findings:**
- `tools/cq/query/executor.py:9-22` re-exports `_execute_entity_query`, `_execute_pattern_query`, `_collect_match_spans`, `_execute_ast_grep_rules`, `_filter_records_by_spans` -- all underscore-prefixed names that signal internal-only use, yet they appear in `__all__`.
- `tools/cq/query/executor_entity.py:16` imports `_execute_entity_query` from `executor` (which itself re-exports from `executor_runtime`), creating a three-layer indirection: `executor_entity` -> `executor` -> `executor_runtime`.
- `tools/cq/query/executor_pattern.py:16` follows the same pattern.

**Suggested improvement:**
Remove the `executor.py` re-export facade entirely. Have `executor_entity.py` and `executor_pattern.py` import directly from `executor_runtime.py`. Rename the underscore-prefixed public functions to non-prefixed names in `executor_runtime.py` since they are effectively public.

**Effort:** small
**Risk if unaddressed:** low -- the indirection creates confusion but doesn't cause bugs.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`executor_runtime.py` (~1126 lines) is the central concern aggregation point. It handles: scan orchestration, cache fragment management, entity/pattern execution dispatch, result summary construction, multi-language auto-scope merging, ripgrep prefiltering, and insight card attachment.

**Findings:**
- `tools/cq/query/executor_runtime.py:235-264` (`_summary_common_for_query`) builds summary dictionaries with hardcoded telemetry initialization -- this is presentation/reporting logic mixed with execution.
- `tools/cq/query/executor_runtime.py:318-352` (`_scan_entity_records`) interleaves cache fragment orchestration (build context, build entries, run scan, decode) with record scanning -- cache mechanics are not separated from scan logic.
- `tools/cq/query/executor_runtime.py:654-711` (`execute_plan`) contains auto-scope routing, plan execution, context construction, and cache eviction in a single function.
- `tools/cq/query/executor_runtime.py:964-1043` (`_process_decorator_query`) reads files from disk (IO), enriches decorators (logic), filters by decorator predicates (policy), and mutates result (state) -- four concerns in one function.

**Suggested improvement:**
Extract three focused modules from `executor_runtime.py`:
1. `query_scan.py` -- scan orchestration, record assembly, file tabulation.
2. `query_summary.py` -- summary construction, telemetry initialization, explain-mode formatting.
3. Keep `executor_runtime.py` as thin dispatch: prepare state, delegate to scan/execution, attach summary.

**Effort:** medium
**Risk if unaddressed:** medium -- the module is the primary change target for any query feature; its breadth increases merge conflicts and makes isolated testing difficult.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`executor_runtime.py` changes for at least five independent reasons: (1) cache strategy changes, (2) scan logic changes, (3) summary format changes, (4) new entity types, (5) auto-scope orchestration changes.

**Findings:**
- `tools/cq/query/executor_runtime.py:386-413` (`_build_entity_fragment_context`) is cache infrastructure code embedded in the executor.
- `tools/cq/query/executor_runtime.py:271-291` (`_finalize_single_scope_summary`) is presentation logic coupled to execution flow.
- `tools/cq/query/executor_ast_grep.py` at ~1068 lines is also large but has better internal cohesion -- all ast-grep execution logic. It could still benefit from separating cache fragment concerns (lines 142-325) from core matching (lines 473-822).

**Suggested improvement:**
Same as P2: extract scan, cache, and summary into separate modules. The `executor_ast_grep.py` cache fragment functions (`build_pattern_fragment_context`, `pattern_fragment_entries`, `decode_pattern_fragment_payload`, `pattern_data_from_hits`, `compute_pattern_miss_data`, `assemble_pattern_output`) should move to a dedicated `pattern_cache.py`.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The IR module (`ir.py`) and planner (`planner.py`) are highly cohesive with minimal external dependencies. The index sub-package (`def_index.py`, `call_resolver.py`, `files.py`) demonstrates excellent cohesion around symbol resolution. However, executor modules have high coupling to `core/` types.

**Findings:**
- `tools/cq/query/executor_runtime.py:16-136` imports from 28 distinct modules -- an unusually high fan-in that signals low cohesion.
- `tools/cq/query/executor_ast_grep.py:16-74` imports from 20 modules.
- In contrast, `tools/cq/query/ir.py:10-13` imports only from 2 modules (msgspec and core.types), demonstrating proper boundary design.
- `tools/cq/index/call_resolver.py` imports only from `def_index.py` and `core.python_ast_utils` -- excellent coupling discipline.

**Suggested improvement:**
Reducing executor_runtime.py's scope (per P2/P3) would naturally reduce its coupling. Additionally, the cache-related imports (lines 17-22, 53-60, 110-116) should move with the cache extraction.

**Effort:** medium
**Risk if unaddressed:** low -- high coupling makes changes more expensive but doesn't create correctness issues.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Dependencies generally flow inward: `cli_app` -> `executor_dispatch` -> `executor_runtime` -> `ir`/`planner`. The IR module has minimal dependencies. One inversion exists.

**Findings:**
- `tools/cq/astgrep/sgpy_scanner.py:19` imports from `tools.cq.query.language` -- the astgrep adapter (lower-level) depends on query-level language constants. This should flow the other way or use a shared types module.
- `tools/cq/astgrep/sgpy_scanner.py:20` imports from `tools.cq.query.metavar` -- metavar extraction is query-level logic leaking into the scanner.
- `tools/cq/astgrep/sgpy_scanner.py:449` has an inline import from `tools.cq.query.executor_ast_grep` -- a circular dependency that's managed via deferred import but signals architectural inversion.

**Suggested improvement:**
Move `extract_metavar_names` and `extract_variadic_metavar_names` from `query/metavar.py` to `astgrep/` since they operate on pattern strings, not query IR. Move language constants (`DEFAULT_QUERY_LANGUAGE`, `is_rust_language`) to `core/types.py` where `QueryLanguage` is already defined.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The `SymbolIndex` protocol in `index/protocol.py` is a well-designed port, enabling dependency injection for the `DefIndex` implementation. The ast-grep integration uses `ast_grep_py` through a clean adapter layer in `sgpy_scanner.py`.

**Findings:**
- `tools/cq/index/protocol.py:13-146` defines `SymbolIndex` as a proper Protocol with 8 methods -- this is the correct pattern.
- `tools/cq/index/files.py:9` directly imports `pygit2` at module level rather than through a port -- git access is hardwired.
- `tools/cq/index/repo.py:8` same direct pygit2 dependency.
- `tools/cq/query/executor_ast_grep.py:17` imports `SgRoot`, `Config`, `Rule` directly from `ast_grep_py` -- no port abstraction, but this is acceptable given ast-grep-py is a fundamental tool dependency, not a swappable adapter.

**Suggested improvement:**
The pygit2 coupling in `index/files.py` and `index/repo.py` is acceptable for now since git access is fundamental to CQ's file discovery. If testing without git becomes needed, extract a `RepoFileProvider` protocol. No action needed currently.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
Entity-to-pattern mapping knowledge is duplicated between the planner and executor.

**Findings:**
- `tools/cq/query/planner.py:376-383` defines `entity_patterns` mapping entity types to ast-grep patterns for Python (`"function": "$FUNC"`, `"class": "$CLASS"`, etc.).
- `tools/cq/query/planner.py:384-388` defines `entity_kinds` mapping entity types to tree-sitter node kinds.
- `tools/cq/core/entity_kinds.py` (imported at `executor_runtime.py:25` and `executor_definitions.py:14`) is the canonical `ENTITY_KINDS` registry that also maps entity types to record types and kind filters.
- The entity-pattern knowledge in `planner.py:376-398` (Python) and `planner.py:407-455` (Rust) is independent of `ENTITY_KINDS` -- there are two parallel sources of truth for "what does entity=function mean in ast-grep terms."
- `tools/cq/query/parser.py:426-434` (`_parse_entity`) has its own `valid_entities` tuple that must stay synchronized with `ir.py:16` `EntityType` Literal.
- `tools/cq/query/parser.py:442-459` duplicates strictness mode validation against `ir.py:41` `StrictnessMode`.

**Suggested improvement:**
For parser validation, derive valid values from `get_args(EntityType)` and `get_args(StrictnessMode)` at module level rather than maintaining parallel tuples. For entity-to-pattern mapping, consolidate into `ENTITY_KINDS` or a companion `ENTITY_PATTERNS` registry that both the planner and executor reference.

**Effort:** medium
**Risk if unaddressed:** medium -- adding a new entity type requires changes in 3-4 locations with no compile-time enforcement.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The IR has good contract enforcement. `Query.__post_init__` validates entity/pattern mutual exclusion. `RelationalConstraint.__post_init__` validates operator/field combinations. `CompositeRule.to_ast_grep_dict` validates `not` takes exactly one pattern. `PatternSpec.requires_yaml_rule` clearly documents when YAML execution is needed.

**Findings:**
- `tools/cq/query/ir.py:563-574` (`Query.__post_init__`) enforces the core invariant that entity and pattern_spec are mutually exclusive.
- `tools/cq/query/ir.py:319-328` (`RelationalConstraint.__post_init__`) validates that precedes/follows don't use field constraints.
- Missing: `executor_runtime.py:783-803` (`_execute_entity_query`) does not validate that `ctx.plan.is_pattern_query` is False before executing entity logic -- relies on callers to dispatch correctly.
- Missing: `executor_runtime.py:853-895` (`_execute_pattern_query`) does not assert `ctx.plan.is_pattern_query` is True.

**Suggested improvement:**
Add assertions at executor entry points: `assert not ctx.plan.is_pattern_query` in `_execute_entity_query` and `assert ctx.plan.is_pattern_query` in `_execute_pattern_query`. These are cheap safety nets that document the contract.

**Effort:** small
**Risk if unaddressed:** low -- dispatch is already correct, but the defensive check prevents future regressions.

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
This is the strongest principle in the codebase. The parser (`parser.py`) converts raw query strings into frozen `Query` IR objects. After parsing, all downstream code operates on typed, validated data structures. The `_EntityQueryState` and `_PatternQueryState` builder classes ensure only valid `Query` objects can be constructed.

**Findings:**
- `tools/cq/query/parser.py:220-259` (`parse_query`) performs complete tokenization and type-safe construction in one pass.
- `tools/cq/query/ir.py:503-648` (`Query`) is a frozen msgspec Struct -- immutable after construction.
- `tools/cq/query/planner.py:255-274` (`compile_query`) transforms the validated Query IR into a validated ToolPlan -- another frozen struct.

No action needed.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
The `Query` struct's `__post_init__` enforces entity/pattern mutual exclusion at construction time. Frozen structs prevent post-construction mutation. However, some combinations are still representable but nonsensical.

**Findings:**
- `tools/cq/query/ir.py:503` allows `Query(entity="function", scope_filter=None, decorator_filter=DecoratorFilter(...))` -- a decorator filter on a non-decorator entity query. The filter is silently ignored rather than rejected.
- `tools/cq/query/ir.py:503` allows `Query(pattern_spec=PatternSpec(...), scope_filter=ScopeFilter(...), joins=(...))` -- joins on pattern queries are silently ignored. A tagged union (separate `EntityQuery` and `PatternQuery` types) would make these illegal by construction.
- `tools/cq/query/planner.py:202-238` (`ToolPlan`) duplicates `is_pattern_query` as a boolean flag when the presence/absence of `sg_rules` and `sg_record_types` already implies the mode.

**Suggested improvement:**
Consider splitting `Query` into `EntityQuery` and `PatternQuery` as separate structs with a `Query = EntityQuery | PatternQuery` union. This eliminates the `__post_init__` validation and makes entity-only fields (like `decorator_filter`, `joins`) impossible to set on pattern queries. This is a larger refactor that should be weighed against the current state's simplicity.

**Effort:** medium
**Risk if unaddressed:** low -- the current validation catches the most important invariant.

---

#### P11. CQS (Command-Query Separation) -- Alignment: 1/3

**Current state:**
Several key functions violate CQS by both mutating state and returning values, making it harder to reason about side effects.

**Findings:**
- `tools/cq/query/executor_runtime.py:584-609` (`_apply_entity_handlers`) mutates `result.key_findings` in place (command) while also being called in a flow that depends on its side effects. The function returns `None` but modifies the result object through multiple code paths.
- `tools/cq/query/executor_runtime.py:271-291` (`_finalize_single_scope_summary`) mutates `result.summary` in place and returns `None`.
- `tools/cq/query/executor_runtime.py:964-1043` (`_process_decorator_query`) mutates `result.key_findings` and `result.summary` while also performing IO (reading files) and computation (enrichment).
- `tools/cq/query/executor_runtime.py:1045-1074` (`_process_call_query`) mutates `result.key_findings` and `result.summary`.
- Contrast: `tools/cq/query/executor_ast_grep.py:473-482` (`run_ast_grep`) cleanly separates query from command -- context is input, state is the explicit output accumulator.

**Suggested improvement:**
Adopt the pattern used in `executor_ast_grep.py`: define an explicit mutable state object that accumulates findings, then merge findings into the result at a single point. For example, `_apply_entity_handlers` should return a `list[Finding]` rather than mutating `result` directly. The summary construction should be a separate query that reads findings and returns summary data.

**Effort:** medium
**Risk if unaddressed:** medium -- CQS violations make it harder to understand what changes after calling a function, increasing the risk of order-dependent bugs.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`QueryExecutionContext` is a well-designed DI container that bundles query, plan, toolchain, root, and services. Dependencies are passed in rather than constructed inline in most places.

**Findings:**
- `tools/cq/query/execution_context.py:19-32` (`QueryExecutionContext`) is a frozen struct with 11 fields providing everything an executor needs.
- `tools/cq/query/executor_runtime.py:879` constructs `SymtableEnricher(state.ctx.root)` inline rather than receiving it via DI. Same at line 945.
- `tools/cq/query/executor_definitions.py:72` and line 123 also construct `SymtableEnricher(root)` inline.
- `tools/cq/query/executor_runtime.py:530-531` constructs `resolve_repo_context(root)` and `build_repo_file_index(repo_context)` inline -- heavy operations that could be pre-computed and injected.

**Suggested improvement:**
Add `symtable_enricher` to `QueryExecutionContext` (it already exists as an optional field at line 32) and ensure it's always populated during context construction, rather than lazily constructed in multiple executor paths.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The entire scope uses composition exclusively. No class hierarchies exist. Behavior is assembled by combining frozen structs, dataclasses, and function composition. `DefIndexVisitor` extends `ast.NodeVisitor` which is the standard library's visitor pattern -- appropriate inheritance.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
The executor modules frequently reach through context chains.

**Findings:**
- `tools/cq/query/executor_runtime.py:878` accesses `state.ctx.query.scope_filter` -- three levels of indirection.
- `tools/cq/query/executor_runtime.py:866-872` accesses `state.ctx.plan.sg_rules`, `state.file_result.files`, `state.ctx.root`, `state.ctx.query`, `state.scope_globs`, `state.ctx.run_id` -- six different properties through the state wrapper.
- `tools/cq/query/entity_front_door.py:258` accesses `Path(result.run.root) / finding.anchor.file` -- reaching through result -> run -> root and finding -> anchor -> file.

**Suggested improvement:**
`PatternExecutionState` and `EntityExecutionState` could expose convenience properties for commonly accessed nested values (e.g., `state.query` instead of `state.ctx.query`, `state.root` instead of `state.ctx.root`). This is a minor ergonomic improvement.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Finding details are frequently interrogated with `.get()` and type checks rather than having the Finding expose typed accessors.

**Findings:**
- `tools/cq/query/entity_front_door.py:138-155` extracts `caller_count`, `callee_count`, `enclosing_scope` from `finding.details` using `.get()` with manual type checks and fallbacks.
- `tools/cq/query/entity_front_door.py:416-418` (`_detail_int`) is a helper that interrogates `finding.details.get(key)` and coerces to int.
- This pattern is widespread because `Finding.details` is typed as `dict[str, object]` -- a generic bag that forces callers to perform type-unsafe access.

**Suggested improvement:**
For entity query results, define a `DefinitionDetail` struct with typed fields (`caller_count: int`, `callee_count: int`, `enclosing_scope: str`, `kind: str`, `name: str`). Populate this in `def_to_finding` and access it downstream without `.get()` chains. The `Finding.details` dict can still carry the serialization form, but typed access should go through the struct.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The IR (`ir.py`) and planner (`planner.py`) form a clean functional core -- frozen structs, pure transformations, no IO. The executor layer mixes IO (file reads, subprocess calls via ripgrep, cache operations) with transformation logic.

**Findings:**
- `tools/cq/query/ir.py` and `tools/cq/query/planner.py` are pure functional modules with no IO -- excellent.
- `tools/cq/query/executor_ast_grep.py:491-492` reads files from disk (`file_path.read_text`) inside the matching loop.
- `tools/cq/query/executor_runtime.py:988-993` reads files from disk inside the decorator query loop.
- `tools/cq/introspection/bytecode_index.py`, `cfg_builder.py`, `symtable_extract.py` are all pure transformations on code objects/source text -- excellent functional core modules.

**Suggested improvement:**
The file-reading IO in executor loops is inherent to the scanning process and difficult to fully separate without over-engineering. The current pattern (read file, process, accumulate) is pragmatic. The main improvement opportunity is separating the cache IO from the execution logic (per P2/P3 suggestions).

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Frozen IR objects ensure recompilation produces identical plans. Cache keys are content-addressed (SHA-256 of rule digests, file content hashes). Running the same query twice produces identical results.

No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Outputs are deterministically ordered throughout.

**Findings:**
- `tools/cq/query/executor_ast_grep.py:413-416` sorts findings, records, and raw_matches by file/line/column.
- `tools/cq/query/executor_runtime.py:441-442` sorts assembled entity records by a detailed sort key.
- `tools/cq/index/files.py:145` sorts filtered files by POSIX path.
- `tools/cq/query/executor_ast_grep.py:271-273` computes deterministic SHA-256 digests for rule and filter configurations.

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The IR and parser are appropriately simple. The fragment caching layer adds significant complexity that may not be proportional to its benefit for all query sizes.

**Findings:**
- `tools/cq/query/executor_ast_grep.py:142-325` -- the pattern fragment caching infrastructure (build context, build entries, scan misses, decode, data from hits, compute miss data, assemble output) is ~180 lines of caching orchestration for what could be a simpler "scan and return" function.
- `tools/cq/query/executor_runtime.py:318-413` -- similar entity fragment caching complexity (~95 lines).
- The caching provides real value for large repos with repeated queries, so this complexity is justified at scale. But the caching logic should be extractable to a separate module to keep the execution path readable.

**Suggested improvement:**
Extract fragment caching into `query/fragment_cache.py` with clear interfaces: `scan_with_cache(files, scanner_fn) -> records`. The executor modules should only see a simple "give me records for these files" interface.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions serve clear current purposes. A few elements appear underutilized.

**Findings:**
- `tools/cq/query/batch_spans.py` exists in the file listing but was not read; based on the name and the presence of `batch.py`, there may be duplication or underutilization.
- `tools/cq/query/symbol_resolver.py` exists but was not imported by any of the 20 files reviewed -- may be dead or very narrowly scoped.
- `tools/cq/utils/uuid_factory.py:154-165` (`uuid8_or_uuid7`) and `uuid_temporal_contracts.py:82-94` (`gated_uuid8`) provide UUIDv8 support that appears unused in the query engine scope -- the functionality exists for potential future use.

**Suggested improvement:**
Verify whether `batch_spans.py` and `symbol_resolver.py` have active callers. If not, consider removal to reduce surface area.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
The dual dispatch facades create confusion about the canonical import path for query execution.

**Findings:**
- `tools/cq/query/executor_dispatch.py` defines `execute_entity_query(ctx)` and `execute_pattern_query(ctx)` as the "dispatch facade."
- `tools/cq/query/executor.py` re-exports `_execute_entity_query`, `_execute_pattern_query`, `execute_entity_query_from_records`, `execute_pattern_query_with_files`, `execute_plan`, and `rg_files_with_matches` from `executor_runtime.py`.
- `tools/cq/query/executor_entity.py` defines `execute_entity_query(ctx)` (same name as `executor_dispatch.py`!) that delegates to `executor._execute_entity_query`.
- A developer looking for "execute_entity_query" finds it in three files: `executor_dispatch.py`, `executor_entity.py`, and `executor.py` (re-exported from runtime). This is confusing.
- `tools/cq/query/executor_runtime.py:714-721` (`_execute_single_context`) imports from `executor_dispatch` at call time -- creating a circular dependency chain: `executor_runtime` -> `executor_dispatch` -> `executor_entity` -> `executor` -> `executor_runtime`.

**Suggested improvement:**
Consolidate to a single dispatch entry point. Remove `executor.py` (the re-export facade) and `executor_dispatch.py`. Have `executor_entity.py` and `executor_pattern.py` contain the actual implementations (they currently just delegate). The runtime module should call these directly without the indirection chain.

**Effort:** small
**Risk if unaddressed:** medium -- the current structure is confusing for anyone navigating the codebase for the first time.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
Contracts are versioned with `V1` suffixes and use msgspec Structs. `__all__` is consistently declared.

**Findings:**
- `tools/cq/query/executor_runtime.py:186` (`ExecutePlanRequestV1`) -- properly versioned request contract.
- `tools/cq/query/match_contracts.py:8-37` (`MatchData`, `MatchRange`, `MatchRangePoint`) -- typed contracts with `__all__`.
- `tools/cq/query/execution_context.py:19` (`QueryExecutionContext`) -- frozen struct serving as the context contract.
- Missing: `ScanContext` and `EntityCandidates` in `scan.py` use `@dataclass` rather than `CqStruct`/msgspec, making them mutable and unversioned -- they cross module boundaries between scan, executor, and definitions modules.

**Suggested improvement:**
Make `ScanContext` and `EntityCandidates` frozen dataclasses since they are constructed once and shared read-only across executor modules.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The IR and planner are highly testable -- pure functions on frozen data. The executor layer requires significant setup (toolchain, services, root path, cache backend) to test.

**Findings:**
- `tools/cq/query/ir.py` and `tools/cq/query/parser.py` can be tested with zero dependencies -- just construct Query objects and verify properties.
- `tools/cq/query/planner.py:255-274` (`compile_query`) is a pure function -- easily testable.
- `tools/cq/query/executor_runtime.py:783-803` (`_execute_entity_query`) requires a `QueryExecutionContext` with 11 fields including `CqRuntimeServices` -- testing requires constructing the full context.
- `tools/cq/query/executor_ast_grep.py:473-482` (`run_ast_grep`) takes an `AstGrepExecutionContext` and `AstGrepExecutionState` -- this pattern is testable because the context is a simple dataclass with no service dependencies.
- `tools/cq/index/def_index.py:420-478` (`DefIndex.build`) performs filesystem scanning -- testable only with real or temp filesystem.
- `tools/cq/introspection/bytecode_index.py:57-115` (`extract_instruction_facts`) takes a `CodeType` and returns a list -- perfectly testable.

**Suggested improvement:**
The executor's dependency on `CqRuntimeServices` for insight card attachment (`_attach_entity_insight`) makes isolated entity query testing difficult. Consider making insight attachment optional or moving it to a post-processing step that callers invoke separately, so the core entity execution can be tested without services.

**Effort:** medium
**Risk if unaddressed:** medium -- difficulty testing executors in isolation leads to under-testing of edge cases.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Logger statements exist at key decision points. Entity front-door provides structured semantic telemetry. However, the core execution path lacks structured span-level tracing.

**Findings:**
- `tools/cq/query/executor_runtime.py:156` creates a module-level logger used at lines 174, 678, 718-719 for debug-level dispatch logging.
- `tools/cq/query/executor_ast_grep.py:77` creates a module-level logger used for scan debugging and file warnings.
- `tools/cq/query/entity_front_door.py:48-62` (`EntitySemanticTelemetry`) provides structured telemetry with per-language attempt/applied/failed/timed_out counters -- good observability.
- Missing: No OpenTelemetry span instrumentation in executor functions. The `src/` pipeline uses `SCOPE_SEMANTICS` for tracing, but the CQ query engine has no equivalent.
- Missing: Cache hit/miss ratios are captured in `snapshot_backend_metrics` but not exposed as structured metrics at the query execution level.

**Suggested improvement:**
Add structured timing to key execution phases (scan, cache lookup, ast-grep execution, filtering, summary construction) and expose them in the result summary. This can be lightweight -- just elapsed_ms fields in the summary dict -- without requiring full OpenTelemetry integration.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: executor_runtime.py as a God Module

**Description:** `executor_runtime.py` concentrates five distinct concerns (scan, cache, dispatch, summary, execution) into a single 1126-line module with 28+ imports. It is the single point of change for almost any query engine modification.

**Root cause:** Organic growth -- features were added to the executor as the query engine evolved, and the module was not decomposed as it grew.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P4 (coupling), P11 (CQS), P14 (Demeter), P23 (testability).

**Suggested approach:** Extract `query_scan.py`, `query_summary.py`, and `fragment_cache.py` from `executor_runtime.py`. The runtime module becomes a thin orchestration layer that delegates to these modules. This addresses 6 principles simultaneously.

### Theme 2: Dual Dispatch Facade Confusion

**Description:** Four modules (`executor.py`, `executor_dispatch.py`, `executor_entity.py`, `executor_pattern.py`) form a dispatch chain with circular imports and overlapping names. The same logical function (`execute_entity_query`) exists in three files with different indirection depths.

**Root cause:** Historical layering -- `executor.py` was the original facade, `executor_dispatch.py` was added to break circular imports, and `executor_entity.py`/`executor_pattern.py` were added as additional dispatch points.

**Affected principles:** P1 (information hiding), P21 (least astonishment), P19 (KISS).

**Suggested approach:** Remove `executor.py` and `executor_dispatch.py`. Make `executor_entity.py` and `executor_pattern.py` contain real implementations (moved from `executor_runtime.py`). The runtime module keeps shared infrastructure.

### Theme 3: Well-Designed IR Pipeline

**Description:** The `ir.py` -> `parser.py` -> `planner.py` pipeline demonstrates strong design discipline. Frozen msgspec structs, parse-don't-validate, proper contract enforcement, and minimal dependencies. This is the model that the executor layer should follow.

**Affected principles:** P9, P10, P17, P18 -- all scored 2-3.

**No action needed** -- this is a strength to preserve and extend.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P21 | Remove dual dispatch facades (`executor.py` + `executor_dispatch.py`) | small | Eliminates confusion for all developers |
| 2 | P8 | Add assertions at executor entry points for plan mode validation | small | Prevents dispatch errors and documents contracts |
| 3 | P1 | Rename `_`-prefixed re-exported functions to proper public names | small | Honest API surface |
| 4 | P7 | Derive parser validation tuples from IR Literal types via `get_args()` | small | Single source of truth for entity/mode types |
| 5 | P22 | Make `ScanContext` and `EntityCandidates` frozen dataclasses | small | Prevents accidental mutation across module boundaries |

## Recommended Action Sequence

1. **Remove dual dispatch facades** (P21, P1). Delete `executor.py` and `executor_dispatch.py`. Update imports in `executor_entity.py`, `executor_pattern.py`, and `executor_runtime.py` to form a single dispatch chain. This cleans up the most confusing aspect of the architecture.

2. **Add executor entry-point assertions** (P8). Add `assert not ctx.plan.is_pattern_query` / `assert ctx.plan.is_pattern_query` guards to the entity/pattern execution entry points.

3. **Derive parser validation from IR types** (P7). Replace hardcoded tuples in `parser.py` (`valid_entities`, `valid_modes`, etc.) with `get_args(EntityType)`, `get_args(StrictnessMode)`, etc.

4. **Freeze ScanContext and EntityCandidates** (P22). Change to `@dataclass(frozen=True)` and ensure construction is complete before sharing.

5. **Extract query_scan.py and query_summary.py from executor_runtime.py** (P2, P3, P4). Move scan orchestration (lines 308-513) to `query_scan.py`. Move summary construction (lines 220-305) to `query_summary.py`. This reduces executor_runtime.py by ~300 lines and isolates three distinct change vectors.

6. **Extract fragment_cache.py from executor_ast_grep.py** (P2, P19). Move lines 142-416 (pattern fragment caching) to a dedicated module. The executor keeps the core `run_ast_grep` / `process_ast_grep_file` / `process_ast_grep_rule` functions.

7. **Fix dependency inversion in astgrep/sgpy_scanner.py** (P5). Move `extract_metavar_names` to `astgrep/` and language constants to `core/types.py`.

8. **Address CQS violations in executor functions** (P11). Refactor `_apply_entity_handlers`, `_process_decorator_query`, `_process_call_query` to return findings lists rather than mutating result objects. Centralize result mutation in the top-level execution functions.
