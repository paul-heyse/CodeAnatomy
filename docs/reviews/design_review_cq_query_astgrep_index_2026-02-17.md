# Design Review: tools/cq/query, tools/cq/astgrep, tools/cq/index

**Date:** 2026-02-17
**Scope:** `tools/cq/query/` (31 files), `tools/cq/astgrep/` (5 files), `tools/cq/index/` (9 files)
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 45

## Executive Summary

The query/astgrep/index subsystem forms CQ's core analysis pipeline: parse query strings into frozen IR, compile to plans, execute via ast-grep, and assemble results. The IR layer (`ir.py`) and contract types are excellent -- frozen msgspec structs with `__post_init__` validation, immutable builder methods, and clean type narrowing. However, the execution layer suffers from a God module (`executor_runtime_impl.py`, 1001 LOC with 5+ responsibilities), duplicated entity-to-pattern knowledge between planner and executor, a three-layer facade chain that adds indirection without value, and pervasive use of `dict[str, object]` where typed structs would be safer. The index module is well-structured with clean functional utilities (`graph_utils.py`) and proper protocol usage (`protocol.py`), though `DefIndex`/`DefIndexVisitor` have some SRP tension. Top priorities are: (1) decompose `executor_runtime_impl.py`, (2) consolidate entity-pattern knowledge into `ENTITY_KINDS`, and (3) eliminate the three-layer facade chain.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Facade re-exports underscore-prefixed internals in `__all__` |
| 2 | Separation of concerns | 1 | large | medium | `executor_runtime_impl.py` mixes entity/pattern/auto-scope/decorator/call/ripgrep |
| 3 | SRP (one reason to change) | 1 | large | medium | `executor_runtime_impl.py` changes for 5+ unrelated reasons |
| 4 | High cohesion, low coupling | 2 | medium | medium | Three-layer facade chain; upward dependency from astgrep to query |
| 5 | Dependency direction | 1 | medium | high | `sgpy_scanner.py:452` imports from `query/executor_ast_grep.py` (lower -> higher) |
| 6 | Ports & Adapters | 2 | medium | low | ast-grep-py adapter exists but couples scanner to executor |
| 7 | DRY (knowledge) | 1 | medium | high | Entity-pattern maps duplicated in `planner.py:377` and `ENTITY_KINDS` |
| 8 | Design by contract | 2 | small | low | Good `__post_init__` on Query/RelationalConstraint; weak on execution functions |
| 9 | Parse, don't validate | 3 | - | - | Query parser produces typed IR; downstream code operates on well-formed types |
| 10 | Make illegal states unrepresentable | 2 | medium | medium | `dict[str, object]` summary payloads allow arbitrary keys |
| 11 | CQS | 1 | medium | medium | `apply_call_evidence` mutates dict param; `SymbolTable.resolve` appends to `unresolved` |
| 12 | DI + explicit composition | 2 | medium | low | `SymtableEnricher` created inline; `load_default_rulepacks` is module-level singleton |
| 13 | Composition over inheritance | 3 | - | - | Only `DefIndexVisitor(ast.NodeVisitor)` inherits, which is stdlib-required |
| 14 | Law of Demeter | 2 | small | low | `entity_front_door.py:163` chains `finding.details.get("enclosing_scope")` |
| 15 | Tell, don't ask | 2 | medium | medium | External code inspects `finding.details.get(key)` to reconstruct logic |
| 16 | Functional core, imperative shell | 2 | medium | low | Good pure IR/planner core; executor mixes computation with IO |
| 17 | Idempotency | 3 | - | - | Queries are read-only; repeated execution produces same results |
| 18 | Determinism / reproducibility | 2 | small | low | `uuid7_str()` for run_id introduces controlled nondeterminism |
| 19 | KISS | 1 | medium | medium | Three-layer facade chain is pure accidental complexity |
| 20 | YAGNI | 2 | small | low | `python_path` parameter in `enrich_records` is unused |
| 21 | Least astonishment | 1 | medium | medium | `executor_entity.py` -> `executor_runtime.py` -> `executor_runtime_impl.py` triple indirection |
| 22 | Declare and version public contracts | 2 | small | low | `__all__` present everywhere; version suffixes on contracts (V1) |
| 23 | Design for testability | 2 | medium | medium | `SymtableEnricher` hard-creates `ScopeGraph` via filesystem; no injection point |
| 24 | Observability | 2 | small | low | Logger present; structured fields in summary; lacks span/trace instrumentation |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Most modules properly use `__all__` to declare public surfaces. The IR types are well-encapsulated. However, the facade layer leaks internal names.

**Findings:**
- `tools/cq/query/executor_runtime.py:27-29` re-exports underscore-prefixed names (`_collect_match_spans`, `_execute_ast_grep_rules`, `_filter_records_by_spans`) and includes them in `__all__` at line 96-98. Underscore-prefixed names are a conventional signal of "internal" but they appear in the public contract.
- `tools/cq/query/executor_runtime.py:24-25` creates module-level type aliases (`EntityExecutionState = executor_runtime_impl.EntityExecutionState`) that expose implementation-detail types through the facade.
- `tools/cq/astgrep/sgpy_scanner.py:49` uses `Mapping[str, Any]` for `RuleSpec.config`, which exposes the full ast-grep config schema rather than a narrower interface.

**Suggested improvement:**
Remove underscore-prefixed names from `executor_runtime.py.__all__`. If callers genuinely need those functions, expose them under clean public names. Consider making `RuleSpec.config` accept a dedicated `RuleConfig` typed struct instead of `Mapping[str, Any]`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
The parser-IR-planner pipeline achieves excellent separation. The execution layer does not.

**Findings:**
- `tools/cq/query/executor_runtime_impl.py` (1001 LOC) handles: (a) plan dispatch and auto-scope orchestration (lines 447-568), (b) entity query execution (lines 587-687), (c) pattern query execution (lines 690-828), (d) decorator query processing (lines 831-912), (e) call query processing (lines 915-949), (f) ripgrep file matching (lines 952-991), (g) file tabulation (lines 271-304), (h) explain metadata assembly (lines 394-444). These are at least 5 distinct concerns in one module.
- `tools/cq/query/entity_front_door.py` (461 LOC) mixes semantic enrichment orchestration, telemetry counter management, and insight card assembly in a single module. The four `_record_semantic_*` helpers (lines 344-387) are essentially a telemetry accumulator that should be encapsulated in the `EntitySemanticTelemetry` dataclass itself.
- `tools/cq/query/enrichment.py` cleanly separates symtable analysis, bytecode analysis, and scope filtering -- good separation within this module.

**Suggested improvement:**
Decompose `executor_runtime_impl.py` into:
- `executor_plan_dispatch.py` -- `execute_plan`, `_execute_auto_scope_plan`, `_run_scoped_auto_query`
- `executor_entity_impl.py` -- `execute_entity_query`, `execute_entity_query_from_records`, `_prepare_entity_state`, `_apply_entity_handlers`
- `executor_pattern_impl.py` -- `execute_pattern_query`, `execute_pattern_query_with_files`, `_prepare_pattern_state`
- `executor_call_decorator.py` -- `_process_decorator_query`, `_process_call_query`

Move `rg_files_with_matches` to `tools/cq/search/rg/adapter.py` or a shared utilities module.

**Effort:** large
**Risk if unaddressed:** medium -- the God module accumulates responsibilities over time and makes changes risky.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
Several modules change for multiple reasons.

**Findings:**
- `tools/cq/query/executor_runtime_impl.py` changes when: entity query semantics change, pattern query semantics change, auto-scope dispatch logic changes, decorator/call query handling changes, file tabulation changes, or explain format changes. This is 6+ distinct reasons to change in a single module.
- `tools/cq/query/executor_definitions.py` (715 LOC) handles both definition query processing and import query processing, including 7 regex-based import extraction functions (lines 456-572). The import extraction functions form a cohesive sub-concern that could live separately.
- `tools/cq/index/def_index.py` (645 LOC) contains `DefIndexVisitor`, `DefIndex`, and three declaration dataclasses (`ParamInfo`, `FnDecl`, `ClassDecl`, `ModuleInfo`). The visitor and the index are separate concerns (extraction vs. querying).

**Suggested improvement:**
For `executor_definitions.py`, extract import name extraction into `import_extractors.py`. For `def_index.py`, the current coupling between visitor and index is acceptable given their tight collaboration; documenting the two sub-concerns is sufficient.

**Effort:** medium (import extractors), large (executor decomposition)
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The IR module has high cohesion (all query-related types). The facade chain creates unnecessary coupling.

**Findings:**
- Three-layer facade chain: `executor_entity.py` imports from `executor_runtime.py` which imports from `executor_runtime_impl.py`. Every function call traverses 3 module boundaries before reaching actual logic. `executor_entity.py:22` calls `runtime_execute_entity_query(ctx)` which at `executor_runtime.py:38` calls `executor_runtime_impl.execute_entity_query(ctx)`. This is pure coupling without cohesion benefit.
- `tools/cq/query/executor_runtime_impl.py` has 137 import lines (lines 1-137), importing from 25+ distinct modules. This high fan-in indicates low cohesion.
- `tools/cq/query/scan.py` (147 LOC) is highly cohesive: `ScanContext`, `EntityCandidates`, `build_scan_context`, `build_entity_candidates`, `assign_calls_to_defs` all relate to scan indexing.

**Suggested improvement:**
Collapse the facade chain. `executor_entity.py` and `executor_pattern.py` should either be eliminated (callers import directly from the implementation module) or become the actual implementation modules after the God module is decomposed.

**Effort:** medium
**Risk if unaddressed:** medium -- the indirection slows comprehension and adds maintenance burden.

---

#### P5. Dependency direction -- Alignment: 1/3

**Current state:**
The intended layering is: IR (lowest) -> planner -> executor -> facades (highest). And: astgrep (lower) -> query (higher).

**Findings:**
- `tools/cq/astgrep/sgpy_scanner.py:452` imports `from tools.cq.query.executor_ast_grep import extract_match_metavars`. This is an upward dependency: the ast-grep adapter layer depends on the query executor layer. The `_extract_metavars` function in `sgpy_scanner.py` (a low-level scanner) reaches into the query execution layer for metavariable extraction.
- `tools/cq/query/executor_runtime_impl.py:508-509` uses deferred imports of `executor_entity` and `executor_pattern` inside `_execute_single_context`, creating a circular reference pattern between the impl module and the facades. This is needed to break import cycles but signals a design smell.
- `tools/cq/query/executor_definitions.py:356` imports `from tools.cq.query.finding_builders import extract_call_name` -- this is fine (peer modules at same level).

**Suggested improvement:**
Extract metavariable extraction utilities from `executor_ast_grep.py` into `tools/cq/astgrep/metavar_extract.py` (alongside existing `metavar.py`), so `sgpy_scanner.py` can import from within its own package. This eliminates the upward dependency from astgrep to query.

**Effort:** medium
**Risk if unaddressed:** high -- the inverted dependency means changes to query/executor_ast_grep.py can break the ast-grep scanner, and the circular import pattern is fragile.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The ast-grep-py integration is properly encapsulated in `sgpy_scanner.py` and `rulepack_loader.py`. The ripgrep integration is abstracted via `find_files_with_pattern`.

**Findings:**
- `tools/cq/astgrep/sgpy_scanner.py:15` imports `from ast_grep_py import Config, Rule, SgNode, SgRoot` -- the third-party types are contained within this module and do not leak to callers. `SgRecord` is the boundary type. This is good adapter design.
- `tools/cq/query/enrichment.py:18` imports `from tools.cq.introspection.symtable_extract import ScopeGraph, extract_scope_graph`. The symtable/bytecode analysis functions are effectively adapter code that wraps stdlib `symtable` and `dis` modules. They are well-isolated.
- `tools/cq/index/repo.py:90` wraps `pygit2` behind `resolve_repo_context` and `open_repo`, providing a clean adapter. The `pygit2` dependency does not leak into other index modules.
- The upward dependency in `sgpy_scanner.py:452` (see P5) violates the adapter boundary by pulling in query-layer logic.

**Suggested improvement:**
Fix the upward dependency in P5 to complete the adapter boundary for astgrep. The current adapter design is otherwise solid.

**Effort:** medium (tied to P5 fix)
**Risk if unaddressed:** low -- the adapter boundary mostly works; the one violation is localized.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
Entity-to-pattern and entity-to-kind knowledge is expressed in multiple places.

**Findings:**
- `tools/cq/query/planner.py:377-399` defines `entity_patterns` and `entity_kinds` dicts mapping entity types to ast-grep patterns and tree-sitter kinds. The canonical entity-kind knowledge also lives in `tools/cq/core/entity_kinds.py` (imported as `ENTITY_KINDS` at planner.py:14). The planner's local maps duplicate knowledge about which entities map to which kinds.
- `tools/cq/query/planner.py:408-456` separately defines `_rust_entity_to_ast_grep_rules` with Rust-specific entity-to-kind mappings (struct_item, enum_item, trait_item, function_item, etc.). These Rust kind mappings have no authoritative source -- they exist only here.
- `tools/cq/query/executor_definitions.py:325-329` uses `ENTITY_KINDS.matches()` for filtering, which is the correct authoritative source. But `planner.py` bypasses `ENTITY_KINDS` and maintains its own parallel mapping.
- `tools/cq/query/symbol_resolver.py:358-438` defines `_is_builtin` with a hardcoded set of 50+ Python builtins. Python's `builtins` module provides the canonical set via `dir(builtins)`.
- Import name extraction logic appears twice: `executor_definitions.py:422-572` (extract_import_name with kind-based dispatch) and `symbol_resolver.py:234-320` (_parse_import, _parse_from_import_stmt). Both parse import statement text but in different ways for different purposes. The knowledge of "how to parse an import statement" is duplicated.

**Suggested improvement:**
(1) Move the entity-to-pattern mapping into `ENTITY_KINDS` or a companion `EntityPatterns` registry in `core/entity_kinds.py` so planner draws from a single source. (2) Replace `_is_builtin` with `import builtins; name in dir(builtins)` or reference a shared constant. (3) Unify import parsing by extracting a shared `import_parser.py` module that both `executor_definitions.py` and `symbol_resolver.py` consume.

**Effort:** medium
**Risk if unaddressed:** high -- when entity kinds evolve (e.g., adding "trait" entity for Rust), the planner's local maps must be updated separately, creating drift risk.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The IR layer has strong contracts. The execution layer relies on assertions and runtime checks.

**Findings:**
- `tools/cq/query/ir.py:567-578` -- `Query.__post_init__` enforces mutual exclusivity of `entity` and `pattern_spec`. This is a proper contract.
- `tools/cq/query/ir.py:323-331` -- `RelationalConstraint.__post_init__` validates that `precedes`/`follows` operators don't have `field_name`. Good.
- `tools/cq/query/executor_runtime_impl.py:595-597` and `698-700` use bare `assert` for precondition checking (`assert not ctx.plan.is_pattern_query`). Assertions are stripped with `-O` flag and should not be used for contract enforcement in production code.
- `tools/cq/query/enrichment.py:469` has a quiet behavior when `len(findings) != len(records)`: it returns the unfiltered findings list. This is a precondition violation that silently degrades rather than failing fast.

**Suggested improvement:**
Replace `assert` statements in executor functions with explicit `ValueError` raises. For `enrichment.py:469`, either raise an error or log a warning when the parallel-array invariant is broken.

**Effort:** small
**Risk if unaddressed:** low -- the assertions currently work in development but could silently pass in optimized builds.

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
The query pipeline exemplifies this principle well.

**Findings:**
- `tools/cq/query/parser.py` converts raw query strings into typed `Query` IR objects with all fields validated and typed. Downstream code never re-parses query text.
- `tools/cq/query/planner.py:256-275` -- `compile_query` takes a well-typed `Query` and produces a well-typed `ToolPlan`. No re-validation needed.
- `tools/cq/astgrep/rulepack_loader.py:56` uses `decode_yaml_strict(path.read_bytes(), type_=CliRuleFile)` to parse YAML into typed structs at the boundary. This is exactly "parse, don't validate."
- `tools/cq/query/ir.py:480-489` -- `JoinTarget.parse` converts string specs into typed `JoinTarget` structs with validation at parse time.

**Suggested improvement:**
None needed. This is well-aligned.

**Effort:** -
**Risk if unaddressed:** -

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
The IR types do well; the summary/enrichment payloads do not.

**Findings:**
- `tools/cq/query/ir.py:507-578` -- `Query` enforces that exactly one of `entity` or `pattern_spec` must be set via `__post_init__`. This prevents an illegal state.
- `tools/cq/query/ir.py:162-178` -- `MetaVarFilter.matches` properly encapsulates the match/negate logic, preventing callers from combining `pattern` and `negate` incorrectly.
- `tools/cq/query/executor_runtime_impl.py:384-391` -- `_entity_summary_updates` returns `dict[str, object]` with keys like "matches", "total_defs", etc. Nothing prevents callers from passing arbitrary keys. A `SummaryUpdate` struct would make illegal combinations unrepresentable.
- `tools/cq/query/enrichment.py:251-297` -- `_enrich_record` returns `dict[str, object]` where keys can be "symtable_info" and "bytecode_info". These should be a typed `RecordEnrichment` dataclass.
- `tools/cq/query/entity_front_door.py:47-63` -- `EntitySemanticTelemetry` uses `dict[str, object] | None` for `semantic_planes`. The content shape is unconstrained.

**Suggested improvement:**
Replace `dict[str, object]` summary and enrichment payloads with frozen msgspec structs. Start with the highest-traffic path: `_entity_summary_updates` -> `EntitySummaryUpdate(matches: int, total_defs: int, total_calls: int, total_imports: int)`.

**Effort:** medium
**Risk if unaddressed:** medium -- untyped dicts allow subtle key name typos and type mismatches that surface only at runtime.

---

#### P11. CQS -- Alignment: 1/3

**Current state:**
Multiple functions both compute results and mutate state.

**Findings:**
- `tools/cq/query/finding_builders.py:164-193` -- `apply_call_evidence(details, evidence, call_target)` mutates the `details` dict parameter (adding keys like "resolved_globals", "bytecode_calls", "globals_has_target", "bytecode_has_target") and returns `None`. This is a command masquerading as a helper. The caller at `executor_runtime_impl.py:942` passes in a local dict and expects mutation.
- `tools/cq/query/symbol_resolver.py:131-160` -- `SymbolTable.resolve()` is a query (returns `SgRecord | None`) but also mutates `self.unresolved` at line 159 (`self.unresolved.append((file, name))`). This means calling `resolve` twice for the same unresolved name appends duplicates.
- `tools/cq/query/entity_front_door.py:344-387` -- The four `_record_semantic_*` functions mutate the `telemetry` dataclass fields in place while being called from what is otherwise a functional pipeline. The telemetry object is passed by reference and accumulated into.
- `tools/cq/query/enrichment.py:375` -- `SymtableEnricher._cache` is mutated during `enrich_function_finding` calls via `_get_scope_graph`. The enricher is a stateful query object.

**Suggested improvement:**
(1) Change `apply_call_evidence` to return a new dict: `def build_call_evidence(evidence, call_target) -> dict[str, object]` and merge at the call site. (2) Separate `SymbolTable.resolve` from tracking: add a separate `track_unresolved(file, name)` method, or return a `ResolveResult(record, status)` where status includes "unresolved". (3) For `EntitySemanticTelemetry`, consider making it a frozen struct with `replace` semantics, or at minimum document the intentional mutation contract.

**Effort:** medium
**Risk if unaddressed:** medium -- CQS violations make it hard to reason about function effects, especially when enrichers are reused across calls.

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
Most dependencies are passed as parameters. A few are created inline.

**Findings:**
- `tools/cq/query/executor_runtime_impl.py:719` and `799` create `SymtableEnricher(state.ctx.root)` inline. This makes testing require filesystem access. The enricher could be injected via the context.
- `tools/cq/astgrep/rulepack_loader.py:125-126` -- `@lru_cache(maxsize=1)` on `load_default_rulepacks()` creates a module-level singleton. The `clear_rulepack_cache()` at line 148 is a test escape hatch, which is a sign that the singleton creates testing friction.
- `tools/cq/query/execution_context.py:33` -- `QueryExecutionContext` is a frozen struct with `services: CqRuntimeServices` field, which is good explicit DI. Callers can inject different service implementations.
- `tools/cq/query/executor_runtime_impl.py:577-583` -- `_attach_entity_insight` properly accepts `services` and delegates to `services.entity.attach_front_door`. This is correct DI usage.

**Suggested improvement:**
Add `symtable_enricher` as an optional field on `QueryExecutionContext` (or on `PatternQueryRequest`/`EntityQueryRequest`) to allow injection. For `load_default_rulepacks`, consider making it a method on a `RulePackRegistry` that can be instantiated and injected.

**Effort:** medium
**Risk if unaddressed:** low -- the inline creation is functional but creates test friction.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
Almost no inheritance is used. The one case is stdlib-required.

**Findings:**
- `tools/cq/index/def_index.py:284` -- `class DefIndexVisitor(ast.NodeVisitor)` inherits from stdlib's visitor pattern. This is the canonical way to use `ast.NodeVisitor` and cannot be avoided.
- All other types use composition: `QueryExecutionContext` contains `ToolPlan`, `Query`, `Toolchain`; `EntityExecutionState` contains `QueryExecutionContext`, `ScanContext`, `EntityCandidates`.
- The execution pipeline composes functions rather than using inheritance hierarchies.

**Suggested improvement:**
None needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. Some chains exist in the front-door module.

**Findings:**
- `tools/cq/query/entity_front_door.py:162-165` chains `finding.details.get("enclosing_scope")` and casts to `str`, then checks against `{"", "<module>"}`. This is reaching two levels deep through the finding to interpret raw detail data.
- `tools/cq/query/entity_front_door.py:448-455` accesses `finding.details.score.evidence_kind`, `score.confidence_score`, `score.confidence_bucket` -- three levels of traversal through the finding details.
- `tools/cq/query/executor_runtime_impl.py:850-851` accesses `def_record.kind` via `ENTITY_KINDS.decorator_kinds`, which is appropriate (single-level access to a collaborator's set).

**Suggested improvement:**
Add helper methods to `Finding` or the `details` type (e.g., `details.get_str("enclosing_scope")`, `details.confidence_bucket`) to reduce traversal depth. Alternatively, extract the insight assembly logic into functions that accept decomposed parameters rather than reaching into nested structures.

**Effort:** small
**Risk if unaddressed:** low -- the chains are localized to the front-door module.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Several patterns inspect finding details externally rather than having findings expose behavior.

**Findings:**
- `tools/cq/query/entity_front_door.py:148-158` iterates over candidates and extracts `_detail_int(finding, "caller_count")` and `_detail_int(finding, "callee_count")` to compute neighborhood totals. The finding holds raw data; the front-door module reimplements the interpretation logic.
- `tools/cq/query/executor_runtime_impl.py:874-877` inspects `decorator_info.get("decorators", [])` and then checks `isinstance(decorators_value, list)`. This is "ask for data, then decide" rather than having the enrichment result encapsulate the behavior.
- `tools/cq/query/enrichment.py:499-528` -- The scope matching functions `_matches_scope_type`, `_matches_captures`, `_matches_cells` all extract data from `scope_info: dict[str, object]` and apply logic externally. If `scope_info` were a typed struct, it could have `matches(filter)` methods.

**Suggested improvement:**
Make enrichment results typed structs with query methods: `scope_info.matches_filter(scope_filter)` instead of three separate functions extracting raw keys. Similarly, create a `NeighborhoodStats` type on `Finding` that provides `caller_count`, `callee_count` directly.

**Effort:** medium
**Risk if unaddressed:** medium -- the "ask" pattern causes enrichment interpretation logic to scatter.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The IR and planner layers are excellent functional cores. The execution layer mixes pure computation with IO.

**Findings:**
- `tools/cq/query/ir.py` is entirely pure: frozen structs, no IO, no side effects. This is a textbook functional core.
- `tools/cq/query/planner.py` is pure: `compile_query` transforms `Query` -> `ToolPlan` with no IO.
- `tools/cq/astgrep/metavar.py` is pure: regex-based extraction with no IO.
- `tools/cq/index/graph_utils.py` is entirely pure: all functions take dict adjacency and return graph results with no IO.
- `tools/cq/query/executor_runtime_impl.py:587-625` -- `execute_entity_query` mixes pure result assembly with IO: filesystem access via scope resolution, cache metrics snapshot, and front-door insight assembly that may trigger LSP requests.
- `tools/cq/query/enrichment.py:336-360` -- `enrich_records` reads files from disk inside a loop. The IO is appropriately at the edge of the enrichment boundary, but it is interleaved with pure enrichment logic.

**Suggested improvement:**
In `executor_runtime_impl.py`, separate the pure result assembly (filtering, scoring, summary construction) from IO operations (file tabulation, cache snapshots, semantic enrichment). The pure assembly could be extracted into a testable function that takes pre-loaded data.

**Effort:** medium
**Risk if unaddressed:** low -- the current structure works but makes testing require filesystem setup.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Queries are inherently idempotent -- they read source files and produce results without modifying state.

**Findings:**
- All query execution functions produce a `CqResult` without modifying the repository or any external state.
- The `SymtableEnricher._cache` accumulates entries but this is a read cache that produces the same results on repeated calls for the same input.
- `SymbolTable.resolve` appending to `unresolved` (P11) is an idempotency concern only for the unresolved tracking list, not for the return value.

**Suggested improvement:**
None needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
Results are mostly deterministic given the same source files. Controlled nondeterminism exists.

**Findings:**
- `tools/cq/query/executor_runtime_impl.py:470` uses `uuid7_str()` for `active_run_id` when no run_id is provided. This introduces a new unique ID per execution, which is correct (each run should have a unique ID) and appropriately controlled.
- `tools/cq/query/executor_runtime_impl.py:497` uses `ms()` for `started_ms` timestamp -- a nondeterministic value, but it is metadata, not affecting result content.
- The entity-query result order depends on the order of records from `sg_scan`, which processes files in directory-listing order. This is deterministic for a given filesystem state.

**Suggested improvement:**
Document that result ordering depends on filesystem scan order. If deterministic ordering is required across environments, add an explicit sort step on record keys.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 1/3

**Current state:**
The three-layer facade chain is the primary simplicity violation.

**Findings:**
- A call to `execute_entity_query` traverses: `executor_entity.py:22` -> `executor_runtime.py:38` -> `executor_runtime_impl.py:587`. Three files, three function definitions, three docstrings, zero logic added at the facade layers. This is accidental complexity.
- Similarly, `execute_pattern_query` traverses: `executor_pattern.py:22` -> `executor_runtime.py:56` -> `executor_runtime_impl.py:690`. Same three-layer chain.
- `tools/cq/query/executor_runtime.py:27-29` creates aliases for underscore-prefixed imports with new underscore-prefixed names, adding a layer of name mapping that serves no purpose.
- `tools/cq/query/executor_ast_grep.py` (78 LOC) is a facade for `executor_ast_grep_impl.py` (828 LOC) with a similar re-export pattern. This adds a fourth facade layer for ast-grep execution.

**Suggested improvement:**
After decomposing the God module (P2), eliminate the facade layers entirely. Callers should import directly from the implementation modules. If backward compatibility is needed, use `__init__.py` re-exports rather than dedicated facade files.

**Effort:** medium
**Risk if unaddressed:** medium -- the facade chain makes the codebase harder to navigate and creates redundant maintenance burden.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most code serves active use cases. A few speculative parameters exist.

**Findings:**
- `tools/cq/query/enrichment.py:303` -- `python_path: str | None = None` parameter is unused. Line 323 logs "python_path is currently unused." This is YAGNI -- the parameter was added for a future use case that hasn't materialized.
- `tools/cq/astgrep/sgpy_scanner.py:182` -- `rulepack_path: Path | None = None` parameter is unused (line 203: `_ = rulepack_path`). Same pattern.
- `tools/cq/query/ir.py:33-42` -- `FieldType` Literal has 8 values including "decorated_functions" which appears to be forward-looking based on the existing decorator query support.

**Suggested improvement:**
Remove unused parameters `python_path` and `rulepack_path`. If they are needed in the future, they can be added back with a clear use case.

**Effort:** small
**Risk if unaddressed:** low -- the unused parameters add minor confusion but no functional risk.

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
The triple-facade indirection and naming conventions create surprise.

**Findings:**
- A developer looking for the entity query implementation finds `executor_entity.py`, which delegates to `executor_runtime.py`, which delegates to `executor_runtime_impl.py`. The actual logic is in the module with the least discoverable name. This violates expectations that `executor_entity.py` would contain entity execution logic.
- `tools/cq/query/executor_runtime.py:96-98` exports `_collect_match_spans`, `_execute_ast_grep_rules`, `_filter_records_by_spans` with leading underscores in `__all__`. Exporting underscore-prefixed names in `__all__` contradicts the Python convention that underscore means "private."
- `tools/cq/query/finding_builders.py:164` -- `apply_call_evidence` returns `None` (void) but mutates its first argument. The name suggests it returns enriched evidence, but it actually mutates a dict parameter in place.

**Suggested improvement:**
(1) After God-module decomposition, name the resulting modules clearly: `entity_executor.py`, `pattern_executor.py`, `plan_dispatcher.py`. (2) Stop exporting underscore-prefixed names. (3) Rename `apply_call_evidence` to `mutate_call_evidence_in_place` or, better, change it to return a new dict.

**Effort:** medium
**Risk if unaddressed:** medium -- surprising naming causes developers to look in wrong places and misunderstand function contracts.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
Public contracts are generally well-declared with `__all__` and V1 suffixes.

**Findings:**
- All 45 files have `__all__` lists declaring their public surface. This is good.
- `tools/cq/query/executor_runtime_impl.py:163` -- `ExecutePlanRequestV1` uses V1 suffix for versioning. Good.
- `tools/cq/query/match_contracts.py` -- `MatchData`, `MatchRange`, `MatchRangePoint` are frozen msgspec structs. These are properly declared contracts without version suffixes, which is acceptable for internal-only types.
- `tools/cq/query/execution_requests.py` uses frozen dataclasses for request contracts. These are stable internal types.
- `tools/cq/query/__init__.py` re-exports key types from `ir` and `parser`, providing a clean public surface for the query subpackage.

**Suggested improvement:**
Add V1 suffixes to contracts that cross subpackage boundaries (e.g., `DefQueryRelationshipPolicyV1` already has this; ensure consistency).

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure modules (IR, planner, graph_utils) are highly testable. Execution modules require filesystem access.

**Findings:**
- `tools/cq/query/ir.py`, `tools/cq/query/planner.py`, `tools/cq/astgrep/metavar.py`, `tools/cq/index/graph_utils.py` are entirely testable without mocks -- pure functions with typed inputs and outputs.
- `tools/cq/query/enrichment.py:363-396` -- `SymtableEnricher` hardcodes `Path.read_text` for file access. To test `enrich_function_finding`, you need actual Python source files on disk. A `source_reader` callable parameter would enable injection.
- `tools/cq/query/executor_runtime_impl.py:719` creates `SymtableEnricher(state.ctx.root)` inline, making it impossible to inject a test double.
- `tools/cq/index/def_index.py:420-479` -- `DefIndex.build()` scans the filesystem directly. The visitor (`DefIndexVisitor`) is independently testable with synthetic AST, but the index builder is not.
- `tools/cq/astgrep/rulepack_loader.py:125` -- the `@lru_cache` singleton requires `clear_rulepack_cache()` in tests, which is a sign of testing friction from module-level state.

**Suggested improvement:**
Add `source_reader: Callable[[Path], str | None] = None` parameter to `SymtableEnricher.__init__` defaulting to `Path.read_text`. This enables test doubles. Similarly, `DefIndex.build` could accept an iterator of `(path, source)` pairs for testing.

**Effort:** medium
**Risk if unaddressed:** medium -- testing execution paths requires real filesystem fixtures, slowing test development.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Logging is present throughout. Summary metadata provides structured observability. Tracing is absent.

**Findings:**
- `tools/cq/query/executor_runtime_impl.py:139` -- `logger = logging.getLogger(__name__)` with structured debug messages at lines 471-477, 512, 514.
- `tools/cq/query/entity_front_door.py:47-63` -- `EntitySemanticTelemetry` tracks attempted/applied/failed/timed_out counts per language. This is good structured observability.
- `tools/cq/query/enrichment.py:28` uses logger for warning/debug messages during enrichment.
- No OpenTelemetry spans are used in any of the 45 files. The main `src/` modules use `SCOPE_SEMANTICS` and `otel` integration, but the CQ tool does not participate in the tracing infrastructure.
- `tools/cq/index/def_index.py:18` uses logging for skipped files during index building.

**Suggested improvement:**
Add structured telemetry to the query execution hot path: record timing for parse, compile, scan, enrich, and assemble phases. This could be a lightweight timing dict rather than full OpenTelemetry if the CQ tool doesn't require distributed tracing.

**Effort:** small
**Risk if unaddressed:** low -- the current logging is adequate for debugging; structured timing would help performance analysis.

---

## Cross-Cutting Themes

### 1. God Module and Facade Proliferation

**Root cause:** `executor_runtime_impl.py` accumulated responsibilities over time. To manage its size, thin facade modules were added (`executor_runtime.py`, `executor_entity.py`, `executor_pattern.py`) rather than decomposing the implementation. This created the worst of both worlds: the God module still exists, and navigating the code requires traversing 3-4 layers of indirection.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P4 (cohesion/coupling), P19 (KISS), P21 (least astonishment).

**Suggested approach:** Decompose the God module into focused implementation modules, then collapse the facade layers. The decomposition should follow the natural responsibility boundaries already visible in the code: plan dispatch, entity execution, pattern execution, decorator/call queries.

### 2. Untyped dict[str, object] Payloads

**Root cause:** Summary updates, enrichment results, and finding details use `dict[str, object]` as a flexible payload format. This was likely expedient during initial development but creates type safety gaps.

**Affected principles:** P10 (illegal states), P11 (CQS -- mutating dicts), P14 (Demeter -- reaching into nested dicts), P15 (tell don't ask -- extracting data from dicts).

**Suggested approach:** Introduce typed msgspec structs for the highest-traffic payloads: `EntitySummaryUpdate`, `RecordEnrichment`, `ScopeInfo`. Migrate incrementally, starting with summary updates which flow through every query execution.

### 3. Upward Dependency from astgrep to query

**Root cause:** Metavariable extraction logic that logically belongs in the ast-grep layer was implemented in the query executor layer. The scanner needed this logic, creating an inverted dependency.

**Affected principles:** P5 (dependency direction), P6 (ports & adapters).

**Suggested approach:** Extract `extract_match_metavars` and related utilities from `executor_ast_grep_impl.py` into `tools/cq/astgrep/metavar_extract.py`. The scanner imports from within its own package; the executor imports from astgrep.

### 4. Entity-Pattern Knowledge Duplication

**Root cause:** The planner needs entity-to-pattern mappings for ast-grep rule generation, but this knowledge was implemented locally rather than drawn from the canonical `ENTITY_KINDS` registry.

**Affected principles:** P7 (DRY).

**Suggested approach:** Extend `ENTITY_KINDS` (or a companion `EntityPatterns`) to provide `pattern_for_entity(entity_type, language)` and `kind_for_entity(entity_type, language)`. The planner then queries this registry instead of maintaining local maps.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P8 | Replace `assert` preconditions in executor with explicit `ValueError` | small | Prevents silent pass in optimized builds |
| 2 | P20 | Remove unused `python_path` and `rulepack_path` parameters | small | Reduces reader confusion |
| 3 | P1 | Remove underscore-prefixed names from `executor_runtime.py.__all__` | small | Fixes public surface leak |
| 4 | P11 | Change `apply_call_evidence` from void mutator to pure function returning dict | small | Eliminates CQS violation at highest-traffic call site |
| 5 | P7 | Replace `_is_builtin` hardcoded set with `name in dir(builtins)` | small | Eliminates 50-line hardcoded constant; auto-tracks Python version |

## Recommended Action Sequence

1. **Quick wins (P8, P20, P1, P11, P7)** -- Small changes with no structural impact. Can be done as a single PR to improve code quality immediately.

2. **Fix upward dependency (P5, P6)** -- Extract `extract_match_metavars` from `executor_ast_grep_impl.py` into `tools/cq/astgrep/metavar_extract.py`. Update imports in `sgpy_scanner.py` and `executor_ast_grep_impl.py`. This fixes the dependency direction and completes the adapter boundary.

3. **Consolidate entity-pattern knowledge (P7)** -- Extend `ENTITY_KINDS` or create `EntityPatterns` in `core/entity_kinds.py`. Update `planner.py` to use the registry. Remove local `entity_patterns` and `entity_kinds` dicts.

4. **Decompose God module (P2, P3)** -- Split `executor_runtime_impl.py` into `executor_plan_dispatch.py`, `executor_entity_impl.py`, `executor_pattern_impl.py`, `executor_call_decorator.py`. This is the largest change and should be done after steps 2-3 to avoid merge conflicts.

5. **Collapse facade chain (P4, P19, P21)** -- After decomposition, replace `executor_entity.py`, `executor_pattern.py`, and `executor_runtime.py` with direct imports from the new implementation modules. Update all callers.

6. **Type summary payloads (P10, P15)** -- Introduce `EntitySummaryUpdate`, `PatternSummaryUpdate`, `RecordEnrichment` as frozen msgspec structs. Migrate callers incrementally.

7. **Improve testability (P23)** -- Add `source_reader` injection to `SymtableEnricher`. Add enricher injection to `QueryExecutionContext`. Convert `load_default_rulepacks` from lru_cache singleton to injectable registry.
