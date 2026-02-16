# Design Review: tools/cq/query

**Date:** 2026-02-15
**Scope:** `tools/cq/query/` -- all 16 Python files
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 16 (9,034 LOC)

## Executive Summary

The query subsystem implements a clean compiler architecture (parse -> IR -> plan -> execute) with well-designed frozen IR types. However, `executor.py` at 3,457 LOC concentrates too many responsibilities -- caching, AST execution, finding construction, relationship analysis, name extraction, and result assembly -- violating SRP and creating information-hiding failures as `batch.py` and `batch_spans.py` import its private (`_`-prefixed) functions. Duplicated knowledge exists across three files (`executor.py`, `merge.py`, `entity_front_door.py`) for result counting and language-gap detection. The highest-impact improvements are decomposing `executor.py` into focused submodules and extracting a shared "ast-grep match execution" module to eliminate private cross-module imports.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | `batch.py` and `batch_spans.py` import 8 private functions from `executor.py` |
| 2 | Separation of concerns | 1 | large | medium | `executor.py` mixes caching, execution, finding construction, relationship analysis |
| 3 | SRP | 0 | large | high | `executor.py` has at least 6 distinct reasons to change across 3,457 LOC |
| 4 | High cohesion, low coupling | 1 | large | medium | `executor.py` has 111 private + 6 public functions; low internal cohesion |
| 5 | Dependency direction | 2 | small | low | Core IR is clean; executor depends on framework types appropriately |
| 6 | Ports & Adapters | 2 | medium | low | ast-grep access is direct but confined; no protocol abstraction for scan backend |
| 7 | DRY (knowledge) | 1 | small | medium | `_count_result_matches` and `_missing_languages_from_summary` duplicated; `_extract_def_name` in 3 files |
| 8 | Design by contract | 2 | small | low | `Query.__post_init__` validates mutual exclusivity; parser validates metavars |
| 9 | Parse, don't validate | 3 | n/a | n/a | Parser converts text to frozen IR at boundary; downstream code operates on typed structs |
| 10 | Make illegal states unrepresentable | 2 | small | low | Query IR uses frozen structs + Literal types; `with_*` methods bypass `msgspec.structs.replace` |
| 11 | CQS | 2 | small | low | Most functions are queries or commands; `_EntityFragmentContext` combines cache check + return |
| 12 | DI + explicit composition | 1 | medium | medium | `executor.py` creates `SymtableEnricher` inline; no injection points for scan/cache backends |
| 13 | Composition over inheritance | 3 | n/a | n/a | No inheritance hierarchies; all behavior via composition and frozen structs |
| 14 | Law of Demeter | 2 | small | low | `_find_enclosing_class` walks `index.by_file[...].intervals` triple-dot chain |
| 15 | Tell, don't ask | 2 | small | low | `_extract_def_name` pattern across 3 files pulls data out of records for external logic |
| 16 | Functional core, imperative shell | 1 | large | medium | `executor.py` interleaves file IO, caching, and pure transforms in single call chains |
| 17 | Idempotency | 3 | n/a | n/a | All execution is read-only; cache writes are idempotent by key |
| 18 | Determinism | 3 | n/a | n/a | Same query + same files = same results; no non-deterministic paths |
| 19 | KISS | 1 | medium | medium | `executor.py` complexity far exceeds what a single module should own |
| 20 | YAGNI | 2 | small | low | `symbol_resolver.py` not yet integrated; otherwise minimal speculative code |
| 21 | Least astonishment | 2 | small | low | Public API surface in `__init__.py` is clean; private import patterns surprise |
| 22 | Declare public contracts | 2 | small | low | `__init__.py` exports are explicit but `executor.py` lacks `__all__` |
| 23 | Design for testability | 1 | medium | medium | Testing executor requires full file scan setup; pure transforms not extractable |
| 24 | Observability | 1 | small | medium | Only `enrichment.py` has structured logging; executor has no logging at all |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
`executor.py` exposes internal implementation details through private function imports consumed by sibling modules. Two files within the same package import `_`-prefixed functions, creating tight coupling to executor internals that should be hidden behind a stable interface.

**Findings:**
- `tools/cq/query/batch.py:14-19` imports `_build_entity_candidates` and `_build_scan_context` from executor. These are prefixed with `_` indicating they are intended as private, yet they are required by a sibling module.
- `tools/cq/query/batch_spans.py:12-20` imports 6 private functions from executor: `_execute_rule_matches`, `_group_match_spans`, `_match_passes_filters`, `_partition_query_metavar_filters`, `_resolve_rule_metavar_names`, `_resolve_rule_variadic_metavars`. This constitutes a fragile dependency on executor internals.
- `tools/cq/query/executor.py` has no `__all__` declaration, so the public surface is undefined. The 6 public functions (`build_scan_context`, `execute_plan`, `execute_entity_query_from_records`, `execute_pattern_query_with_files`, `rg_files_with_matches`, `assign_calls_to_defs`) are mixed in with 111 private functions.

**Suggested improvement:**
Extract the 8 private functions imported by `batch.py` and `batch_spans.py` into dedicated submodules with a stable public surface. Specifically: (a) create `tools/cq/query/ast_grep_match.py` for `_execute_rule_matches`, `_group_match_spans`, `_match_passes_filters`, `_partition_query_metavar_filters`, `_resolve_rule_metavar_names`, `_resolve_rule_variadic_metavars`, and `AstGrepMatchSpan`; (b) create `tools/cq/query/scan.py` for `_build_entity_candidates`, `_build_scan_context`, `ScanContext`, and `EntityCandidates`. Add `__all__` to `executor.py` to declare the intentional public surface.

**Effort:** medium
**Risk if unaddressed:** high -- Any refactor of executor internals risks breaking batch.py and batch_spans.py silently.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`executor.py` mixes at least five distinct concerns in a single 3,457-line module: (1) cache fragment management (`_EntityFragmentContext`, `_PatternFragmentContext`, `_build_entity_fragment_context`, `_scan_entity_records`), (2) ast-grep rule execution (`_run_ast_grep`, `_process_ast_grep_file`, `_execute_rule_matches`), (3) finding construction (`_def_to_finding`, `_import_to_finding`, `_call_to_finding`, `_match_to_finding`), (4) relationship analysis (`_build_callers_section`, `_build_callees_section`, `_build_imports_section`, `_build_raises_section`, `_build_scope_section`, `_build_bytecode_surface_section`), and (5) multi-language auto-scope orchestration (`_execute_auto_scope_plan`, `_run_scoped_auto_query`).

**Findings:**
- `tools/cq/query/executor.py:174-199` defines `_EntityFragmentContext` and `_PatternFragmentContext` -- caching dataclasses that belong in a dedicated cache module.
- `tools/cq/query/executor.py:1458-1581` contains `_run_ast_grep` and `_process_ast_grep_file` -- ast-grep scanning mechanics that are reusable outside of entity execution context.
- `tools/cq/query/executor.py:2996-3336` contains 6 section-builder functions (`_build_callers_section`, `_build_callees_section`, `_build_imports_section`, `_build_raises_section`, `_build_scope_section`, `_build_bytecode_surface_section`) that produce report sections from records -- a distinct "report assembly" concern.
- `tools/cq/query/executor.py:905-964` contains `_execute_auto_scope_plan` -- multi-language orchestration logic that is architecturally distinct from single-language execution.

**Suggested improvement:**
Decompose `executor.py` into focused submodules: (a) `executor_cache.py` for fragment context and cache-aware scanning, (b) `ast_grep_match.py` for ast-grep rule execution and match processing, (c) `finding_builders.py` for converting records to Finding objects, (d) `section_builders.py` for relationship analysis section construction, (e) retain `executor.py` as the orchestration layer that composes these submodules.

**Effort:** large
**Risk if unaddressed:** medium -- The monolithic structure makes it difficult to understand, test, or modify any single concern without risk of unintended side effects.

---

#### P3. SRP (one reason to change) -- Alignment: 0/3

**Current state:**
`executor.py` has at least 6 distinct reasons to change: (1) cache key/fragment strategy changes, (2) ast-grep execution API changes, (3) finding/report format changes, (4) relationship analysis algorithm changes, (5) multi-language scope execution strategy changes, (6) metavar filter evaluation changes. This is the most severe SRP violation in the subsystem.

**Findings:**
- `tools/cq/query/executor.py` contains 117 functions (6 public, 111 private) across 3,457 LOC -- roughly 6x the size of the next-largest file (`parser.py` at 993 LOC).
- `tools/cq/query/executor.py:132-240` defines 10 dataclasses/structs serving different concerns: `ScanContext` (scan state), `EntityExecutionState` (entity execution), `PatternExecutionState` (pattern execution), `_EntityFragmentContext` (caching), `_PatternFragmentContext` (caching), `AstGrepExecutionContext` (ast-grep), `AstGrepExecutionState` (ast-grep), `AstGrepRuleContext` (ast-grep), `DefQueryRelationshipPolicyV1` (relationship analysis), `ExecutePlanRequestV1` (orchestration).
- `tools/cq/query/executor.py:2987-2995` defines `CallTargetContext` dataclass 2,800 lines after the other dataclasses, indicating organic growth rather than intentional design.

**Suggested improvement:**
Group the 10 dataclasses by concern and co-locate them with the functions that use them in separate submodules. The `CallTargetContext` dataclass should live alongside the caller/callee section builders. The fragment context classes should live with cache-aware scanning logic. Target: no single file exceeds 800 LOC.

**Effort:** large
**Risk if unaddressed:** high -- The monolithic file is the primary barrier to maintainability and testability of the query subsystem.

---

#### P4. High cohesion, low coupling -- Alignment: 1/3

**Current state:**
Within `executor.py`, the 117 functions span multiple conceptual groups with weak internal cohesion. Between modules, coupling is elevated because `batch.py` and `batch_spans.py` reach into executor's private interface.

**Findings:**
- `tools/cq/query/executor.py:2744-2779` defines `_extract_def_name` which is also defined separately in `tools/cq/query/symbol_resolver.py:204` and (with a slightly different name) in `tools/cq/query/enrichment.py:434` as `_extract_def_name_from_record`. These three implementations serve the same concept but live in three different modules.
- `tools/cq/query/batch_spans.py:12-20` imports 6 private functions plus 1 public type from executor, creating high coupling. If executor's internal match-processing pipeline changes, batch_spans breaks.
- `tools/cq/query/executor.py:1014` defines `execute_entity_query_from_records` which calls into `_build_entity_candidates` (also used by `batch.py`), showing that the shared functionality should be in a common module rather than being reached into via private imports.

**Suggested improvement:**
Extract `_extract_def_name` into a shared `record_utils.py` module and import it from there in all three files. Extract the ast-grep match execution pipeline into `ast_grep_match.py` with a public interface, eliminating the private import pattern in `batch_spans.py`.

**Effort:** large (due to number of call sites across executor.py)
**Risk if unaddressed:** medium -- Changes to shared concepts require remembering to update multiple implementations.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The overall dependency direction is sound: `ir.py` (core) depends on nothing in the package; `parser.py` depends on `ir.py`; `planner.py` depends on `ir.py`; `executor.py` depends on `ir.py`, `planner.py`, `enrichment.py`, etc. The core types flow outward correctly.

**Findings:**
- `tools/cq/query/ir.py` imports only `msgspec` and standard library types -- the cleanest dependency profile in the package.
- `tools/cq/query/executor.py:1-130` imports from `tools.cq.astgrep`, `tools.cq.core`, `tools.cq.search`, and internal query modules -- this is appropriate for the outermost orchestration layer.
- `tools/cq/query/enrichment.py:1-26` imports `logging`, `symtable`, `dis`, `tools.cq.astgrep` -- external dependencies are appropriate for an enrichment adapter.

**Suggested improvement:**
Minor: ensure `ir.py` never acquires dependencies on executor or planner types. Currently clean; document this invariant.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The subsystem does not define formal port protocols, but it has implicit port boundaries: ast-grep scanning is confined to `executor.py` and `sg_parser.py`; symtable/bytecode enrichment is confined to `enrichment.py`. Swapping scan backends would require changes to executor internals but not to IR or planner.

**Findings:**
- `tools/cq/query/executor.py` directly calls `SgRoot(src, lang).root()` at lines 1525 and 1935 -- the ast-grep library is used directly without a port abstraction.
- `tools/cq/query/enrichment.py:308-340` directly uses `symtable.symtable()` and `dis.get_instructions()` -- reasonable for an adapter module, but there is no protocol that would allow substituting a different enrichment source.
- `tools/cq/query/sg_parser.py:100-140` directly calls `scan_files()` from `tools.cq.astgrep.sgpy_scanner` -- the scan backend is hardwired.

**Suggested improvement:**
This is acceptable for the current scale. If a second scan backend is ever needed (e.g., tree-sitter native queries), extract a `ScanPort` protocol. No immediate action required.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Three instances of duplicated knowledge exist in the query subsystem, all representing semantic duplication (same business truth encoded in multiple places).

**Findings:**
- `tools/cq/query/executor.py:966` and `tools/cq/query/merge.py:29` both define `_count_result_matches(result: CqResult | None) -> int` -- identical logic for counting findings across sections.
- `tools/cq/query/entity_front_door.py:446` and `tools/cq/query/merge.py:41` both define `_missing_languages_from_summary(summary: dict[str, object]) -> list[str]` -- identical logic for extracting missing language annotations.
- `tools/cq/query/executor.py:2744`, `tools/cq/query/symbol_resolver.py:204`, and `tools/cq/query/enrichment.py:434` all implement name-from-record extraction logic (the first two are named `_extract_def_name`, the third `_extract_def_name_from_record`).

**Suggested improvement:**
(a) Move `_count_result_matches` to `tools/cq/query/merge.py` (or a new `result_utils.py`) and import it in `executor.py`. (b) Move `_missing_languages_from_summary` to `merge.py` and import it in `entity_front_door.py`. (c) Create a single `_extract_def_name` in a shared `record_utils.py` and use it across all three files.

**Effort:** small
**Risk if unaddressed:** medium -- When the counting or name-extraction logic needs to change, forgetting to update one copy creates subtle inconsistencies.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The IR layer has good contract enforcement. The parser layer validates input structure. The executor layer has weaker contracts.

**Findings:**
- `tools/cq/query/ir.py:577-618` (`Query.__post_init__`) validates mutual exclusivity of `entity` and `pattern_spec`, enforces valid `entity` values against `EntityType`, validates `scope` filter compatibility, and checks `join` field requirements. This is strong contract enforcement.
- `tools/cq/query/parser.py:270-310` (token handler dispatch) validates metavariable patterns and raises `QueryParseError` with descriptive messages. Good boundary validation.
- `tools/cq/query/metavar.py:87-114` (`validate_pattern_metavars`) checks for lowercase, digit-start, and hyphen violations. Clear precondition enforcement.
- `tools/cq/query/executor.py:851-903` (`execute_plan`) accepts `ExecutePlanRequestV1` but does not validate that the plan is compatible with the query before execution. An incompatible plan (e.g., entity plan for a pattern query) would fail deep in execution rather than at the boundary.

**Suggested improvement:**
Add a validation step in `execute_plan` that checks plan-query compatibility before dispatching to `_execute_entity_query` or `_execute_pattern_query`. This would convert late failures into early, attributable errors.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
The query subsystem exemplifies this principle well. The parser (`parser.py`) converts raw query text into frozen `Query` IR objects. Downstream code operates on well-typed structs and never re-parses or re-validates the query string. The planner compiles `Query` into `ToolPlan`; the executor consumes `ToolPlan`. At each stage, the output type is richer and more constrained than the input.

**Findings:**
- `tools/cq/query/parser.py:29-47` (`parse_query`) is the single boundary where text is converted to structured `Query`. No downstream code re-parses query text.
- `tools/cq/query/ir.py:553-766` defines `Query` as `frozen=True`, making it immutable after construction. The `with_*` methods return new instances.

**Effort:** n/a
**Risk if unaddressed:** n/a

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
The IR uses frozen structs and Literal types effectively but has minor gaps.

**Findings:**
- `tools/cq/query/ir.py:553-570` -- `Query` struct uses `entity: EntityType | None` and `pattern_spec: PatternSpec | None` with `__post_init__` validation enforcing mutual exclusivity. This is good but not optimal: a discriminated union (separate `EntityQuery` and `PatternQuery` types) would make the illegal state (both set, or neither set with wrong fields) unrepresentable by construction.
- `tools/cq/query/ir.py:32-46` -- `EntityType`, `ExpanderKind`, `FieldType` etc. use `Literal` unions effectively to constrain values at the type level.
- `tools/cq/query/ir.py:627-720` -- `with_scope()`, `with_expand()`, `with_fields()`, `with_relational()` methods manually reconstruct the entire `Query` struct rather than using `msgspec.structs.replace`. This is verbose (each method lists all 20+ fields) but is functionally correct and explicit.
- `tools/cq/query/ir.py:245-330` -- `PatternSpec` has `pattern: str` which is validated in `__post_init__` at line 306 (`if not self.pattern.strip()`). A newtype wrapper would prevent empty patterns by construction.

**Suggested improvement:**
Consider splitting `Query` into `EntityQuery` and `PatternQuery` variants that share a common base or are wrapped in a discriminated union. This eliminates the runtime validation for mutual exclusivity. Also consider using `msgspec.structs.replace` in `with_*` methods to reduce boilerplate and the risk of missing new fields.

**Effort:** small (for `msgspec.structs.replace`), medium (for discriminated union)
**Risk if unaddressed:** low -- Current validation catches issues; this is a quality-of-life improvement.

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Pure query functions (`_extract_def_name`, `_extract_call_target`, etc.) return values without side effects. Builder functions (`_build_callers_section`, etc.) construct and return section objects. The main gap is in cache-aware scanning.

**Findings:**
- `tools/cq/query/executor.py:268-365` (`_build_entity_fragment_context`) both checks the cache and returns a context object. If the cache has a hit, it returns a populated context with `cached_result` set; otherwise it returns a context with `pending_records`. This combines query (checking cache) with state setup.
- `tools/cq/query/executor.py:367-460` (`_scan_entity_records`) reads files, populates cache, and returns candidates. It interleaves IO (file scanning) with cache writes (mutation) and result construction (query).

**Suggested improvement:**
Separate cache lookup from scan execution: (a) a pure function that checks the cache and returns a cache decision (hit/miss), (b) a scan function that only scans and returns results, (c) a cache-write function that stores results. This makes the cache interaction auditable and testable.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
Dependencies are created inline rather than injected. The executor creates its own enrichers, and the scan pipeline is hardwired.

**Findings:**
- `tools/cq/query/executor.py:3248` creates `SymtableEnricher(root)` inline within `_build_scope_section`. The enricher is not injectable, making it impossible to test scope section building without a real filesystem.
- `tools/cq/query/executor.py:3294` imports `BytecodeInfo` and `enrich_records` inline within `_build_bytecode_surface_section`. Same testability concern.
- `tools/cq/query/executor.py:851-903` (`execute_plan`) takes an `ExecutePlanRequestV1` + `Toolchain` but the Toolchain is passed through opaquely. The executor internally decides which sub-executors to call based on plan type, which is appropriate, but enrichment dependencies are hidden.
- `tools/cq/query/sg_parser.py:56-80` creates `FileInventoryCacheV1` with hardwired cache key construction. No injection point for alternative cache strategies.

**Suggested improvement:**
For section builders that need enrichment (`_build_scope_section`, `_build_bytecode_surface_section`), accept the enricher/enrichment function as a parameter. This enables testing section builders with mock enrichment data. For the cache layer, consider a `CachePort` protocol if the caching strategy needs to vary.

**Effort:** medium
**Risk if unaddressed:** medium -- Testability of executor internals is limited by inline dependency creation.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
The query subsystem uses no inheritance hierarchies. All behavior is composed via function calls and frozen struct construction. The only class inheritance is `QueryParseError(ValueError)` which is appropriate for exception specialization.

**Findings:**
- `tools/cq/query/parser.py:49` -- `QueryParseError(ValueError)` is the only inheritance in the subsystem.
- All IR types (`Query`, `PatternSpec`, `ToolPlan`, `AstGrepRule`, etc.) are frozen `msgspec.Struct` instances composed via fields.
- Executor state (`EntityExecutionState`, `PatternExecutionState`, etc.) uses `dataclass` composition, not inheritance.

**Effort:** n/a
**Risk if unaddressed:** n/a

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. A few functions reach through internal structures.

**Findings:**
- `tools/cq/query/executor.py:3431-3444` (`_find_enclosing_class`) accesses `index.by_file.get(record.file)` then iterates `file_index.intervals`, accessing `.intervals` which is an internal collection of the `FileIntervalIndex`. This is a two-level reach-through.
- `tools/cq/query/executor.py:3009-3016` (`_build_callers_section`) chains through: `call_contexts` -> tuple unpacking -> passing to `_build_caller_findings` which accesses `evidence_map.get(_record_key(containing))`. The intermediate tuple structure makes the data flow opaque.
- `tools/cq/query/enrichment.py:355-380` (`enrich_function_finding`) accesses `finding.anchor.file` and `finding.anchor.line` -- reaching through the Finding's Anchor. This is a minor violation but common in reporting code.

**Suggested improvement:**
For `_find_enclosing_class`, add a method on `FileIntervalIndex` (e.g., `find_enclosing_class(record)`) that encapsulates the traversal. For the tuple-based call contexts, define a small dataclass `CallContext(call: SgRecord, target: str, containing: SgRecord | None)` to make the data structure self-documenting.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The `SgRecord` type is a data carrier that many functions interrogate externally. This is typical for record-oriented pipelines and not inherently problematic, but the pattern of extracting names and kinds externally is widespread.

**Findings:**
- `tools/cq/query/executor.py:2744-2779` (`_extract_def_name`) extracts the definition name from an `SgRecord` by inspecting `record.text` and `record.kind` externally. This function is called from at least 15 different locations in executor.py.
- `tools/cq/query/executor.py:3371-3392` (`_extract_call_target`) uses regex on `call.text` to extract the target name. The record itself does not know its "call target" -- this is computed externally each time.
- `tools/cq/query/enrichment.py:434` defines its own variant (`_extract_def_name_from_record`) showing that the record type does not self-describe its name.

**Suggested improvement:**
If `SgRecord` is under the project's control, add a `.def_name` computed property (or method) that encapsulates the name-extraction logic. If `SgRecord` is external (from `ast_grep_py`), create a thin wrapper or standalone `record_utils.py` with `def_name(record)`, `call_target(record)`, `call_receiver(record)` functions -- eliminating scattered regex parsing.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 1/3

**Current state:**
The IR and planner layers are largely functional (pure transformations on immutable data). However, `executor.py` deeply interleaves IO operations (file reads, cache lookups) with pure transformations (finding construction, filtering), making it difficult to test the pure logic in isolation.

**Findings:**
- `tools/cq/query/executor.py:1517-1580` (`_process_ast_grep_file`) reads a file from disk (`Path.read_text`), parses it with ast-grep, and then processes matches -- file IO and pure logic in one function.
- `tools/cq/query/executor.py:367-460` (`_scan_entity_records`) calls `scan_files()` (IO), builds entity candidates (pure), and writes cache fragments (IO) in a single flow.
- `tools/cq/query/executor.py:3248-3280` (`_build_scope_section`) creates a `SymtableEnricher` (which does file IO on enrich) and builds findings (pure) in one function.
- In contrast, `tools/cq/query/enrichment.py:186-260` (`SymtableEnricher.enrich_function_finding`) separates the scope graph construction (IO) from the finding decoration (pure) reasonably well, though both happen within the same class.
- `tools/cq/query/ir.py` and `tools/cq/query/planner.py` are fully functional -- no IO, no mutation. These are the "functional core" done right.

**Suggested improvement:**
Refactor executor functions to separate IO from transforms: (a) `_process_ast_grep_file` should accept `src: str` (already-read content) rather than `Path`, with the caller responsible for IO; (b) `_scan_entity_records` should be split into scan (IO) and candidate-building (pure); (c) Section builders should accept enrichment data rather than creating enrichers internally.

**Effort:** large
**Risk if unaddressed:** medium -- Pure logic cannot be unit-tested without file fixtures.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All query execution is read-only with respect to source files. Cache writes are keyed by content hash, so re-execution with the same inputs produces the same cache state. No mutation of external state occurs.

**Findings:**
- Cache keys in `tools/cq/query/sg_parser.py:56-80` (`FileInventoryCacheV1`) are content-based, ensuring idempotent cache population.
- `tools/cq/query/executor.py` never modifies source files or external state beyond cache writes.

**Effort:** n/a
**Risk if unaddressed:** n/a

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Given the same query string, files, and file contents, the subsystem produces identical results. There are no sources of non-determinism (no random, no timestamps in outputs, no unordered iteration that affects output).

**Findings:**
- `tools/cq/query/ir.py` uses frozen structs ensuring immutability.
- `tools/cq/query/executor.py` processes files in the order provided by `sg_parser.py:list_scan_files()`, which returns a deterministic file list.
- `tools/cq/query/metavar.py:178` (`extract_metavar_names`) returns `tuple(sorted(...))`, ensuring deterministic ordering.

**Effort:** n/a
**Risk if unaddressed:** n/a

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 1/3

**Current state:**
The subsystem's architecture (parse -> IR -> plan -> execute) is elegantly simple. However, `executor.py` at 3,457 LOC far exceeds what a single module should own. The cognitive load of navigating 117 functions in one file is high. The 10 dataclasses defined across the file (some separated by 2,800 lines) add to the complexity.

**Findings:**
- `tools/cq/query/executor.py` is 3,457 LOC with 117 functions. For comparison: `ir.py` is 766 LOC, `parser.py` is 993 LOC, `planner.py` is 661 LOC. The executor is 3.5x the size of the next-largest file.
- `tools/cq/query/executor.py:132` defines `ScanContext` dataclass; `tools/cq/query/executor.py:2987` defines `CallTargetContext` dataclass -- separated by 2,855 lines. A reader must scroll through the entire file to find all type definitions.
- The `_execute_entity_query` function at `tools/cq/query/executor.py:462-650` is approximately 188 lines long, orchestrating cache checks, scanning, filtering, section building, and result assembly in a single function.

**Suggested improvement:**
Decompose `executor.py` as described in P2/P3 findings. The target state: no single file exceeds 800 LOC; each file has a single-sentence purpose description.

**Effort:** medium
**Risk if unaddressed:** medium -- New contributors face a steep learning curve; maintenance burden is concentrated.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The subsystem is mostly lean. One module (`symbol_resolver.py`) is built but not yet integrated into the main execution path.

**Findings:**
- `tools/cq/query/symbol_resolver.py` (484 LOC) provides `SymbolTable`, `resolve_call_target`, and related functions. Per the architecture doc, this is "not yet integrated" into core query execution. It has test coverage but is unused in production code paths.
- `tools/cq/query/ir.py:499-551` defines `JoinTarget` and `JoinConstraint` structs for join operations. The parser supports `join=` syntax. This is a designed extension point that appears to have active usage.
- No other speculative abstractions were identified. The codebase does not over-abstract.

**Suggested improvement:**
Evaluate whether `symbol_resolver.py` should be integrated or removed. If integration is planned near-term, document the timeline. If not, consider moving it to an experimental location to avoid implying it is part of the active subsystem.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
The public API (`__init__.py`) is clean and predictable. Internal naming conventions are mostly consistent. A few surprises exist.

**Findings:**
- `tools/cq/query/__init__.py:1-43` exports `Query`, `parse_query`, and key IR types -- clean, predictable surface.
- `tools/cq/query/executor.py:654` defines `build_scan_context` as a public function (no `_` prefix), but it is primarily used by `batch.py` which imports `_build_scan_context` (the private variant) instead. The relationship between the public `build_scan_context` and private `_build_scan_context` is potentially confusing.
- `tools/cq/query/batch.py:17` imports `_build_entity_candidates` -- a caller would not expect to need private imports from a sibling module.
- `tools/cq/query/enrichment.py:434` names its function `_extract_def_name_from_record` while `executor.py:2744` and `symbol_resolver.py:204` use `_extract_def_name`. The naming inconsistency may surprise.

**Suggested improvement:**
Consolidate the public `build_scan_context` and private `_build_scan_context` -- either make one call the other or remove the unnecessary variant. Standardize name-extraction function naming.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
`__init__.py` declares exports. IR types use versioned naming for some structs. `executor.py` lacks `__all__`.

**Findings:**
- `tools/cq/query/__init__.py:36-43` has an explicit `__all__` list with 10 exports -- the public surface is declared.
- `tools/cq/query/executor.py:220-236` uses `V1` suffix for `DefQueryRelationshipPolicyV1` and `ExecutePlanRequestV1` -- versioned contract types.
- `tools/cq/query/executor.py` has no `__all__`, so its public surface is ambiguous. The 6 public functions are mixed with 111 private functions.
- `tools/cq/query/sg_parser.py:56` uses `V1` suffix for `FileInventoryCacheV1` -- consistent versioning.

**Suggested improvement:**
Add `__all__` to `executor.py`, `enrichment.py`, `planner.py`, and other modules that have public-facing functions. This makes the intended public surface explicit and prevents accidental use of internals.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
The IR and planner are highly testable (pure functions on immutable data). The executor is difficult to test in isolation because it creates its own dependencies and interleaves IO.

**Findings:**
- `tools/cq/query/ir.py` and `tools/cq/query/planner.py` can be tested with pure unit tests -- no IO, no mocks needed. This is excellent.
- `tools/cq/query/executor.py:1517-1520` (`_process_ast_grep_file`) reads files from disk, requiring test fixtures or temp files for unit testing.
- `tools/cq/query/executor.py:3248` creates `SymtableEnricher(root)` inline, requiring a real filesystem to test `_build_scope_section`.
- `tools/cq/query/executor.py:3294-3297` imports and calls `enrich_records` inline, requiring real files to test `_build_bytecode_surface_section`.
- `tools/cq/query/executor.py:367-460` (`_scan_entity_records`) calls `scan_files()` which requires real files on disk.
- The section builders (`_build_callers_section`, `_build_callees_section`, etc.) that work on `SgRecord` lists are testable with synthetic records -- this is a bright spot within executor.py.

**Suggested improvement:**
(a) Have `_process_ast_grep_file` accept source text as a parameter rather than reading from disk. (b) Accept enricher instances as parameters to section builders. (c) Extract pure finding-construction functions into a separate module that can be tested without any file IO.

**Effort:** medium
**Risk if unaddressed:** medium -- Test coverage of executor internals is limited to integration-level tests that require file fixtures.

---

#### P24. Observability -- Alignment: 1/3

**Current state:**
Structured logging is almost entirely absent from the query subsystem. Only `enrichment.py` has a logger. The executor -- the most complex module -- has zero logging, making operational debugging difficult.

**Findings:**
- `tools/cq/query/enrichment.py:10,26` -- `import logging` and `logger = logging.getLogger(__name__)` -- the only structured logging in the entire subsystem.
- `tools/cq/query/executor.py` (3,457 LOC) has no `import logging`, no `logger`, no structured log statements. Cache hits/misses, scan durations, match counts, and error paths are all invisible.
- `tools/cq/query/sg_parser.py` has no logging. File scanning completion and cache hit rates are not observable.
- `tools/cq/query/parser.py` raises `QueryParseError` on invalid input but does not log parse attempts or failures for telemetry.

**Suggested improvement:**
Add structured logging to: (a) `executor.py` -- log cache hit/miss rates, scan file counts, match counts, query execution duration, and section build outcomes; (b) `sg_parser.py` -- log file inventory cache hits and scan completion; (c) `parser.py` -- log parse failures for diagnostic telemetry. Use `logging.getLogger(__name__)` consistently. Align log field names with existing CQ observability conventions.

**Effort:** small
**Risk if unaddressed:** medium -- When query execution produces unexpected results, there is no telemetry to diagnose root causes.

---

## Cross-Cutting Themes

### Theme 1: executor.py is a God Module

**Description:** `executor.py` at 3,457 LOC with 117 functions and 10 dataclasses is the root cause of violations across P1 (information hiding), P2 (separation of concerns), P3 (SRP), P4 (cohesion), P12 (DI), P16 (functional core), P19 (KISS), and P23 (testability). Every other finding in this review either originates from or is exacerbated by this single file's monolithic structure.

**Affected principles:** P1, P2, P3, P4, P12, P16, P19, P23

**Root cause:** Organic growth -- the executor accumulated responsibilities over time without periodic decomposition. The file has no internal organizational structure beyond function ordering.

**Suggested approach:** Decompose into 4-5 focused submodules (see P2 suggestion), each under 800 LOC. Start with the lowest-risk extraction: ast-grep match execution (used by batch_spans.py) and finding builders (pure functions with no IO). Then extract cache logic, then section builders. Retain `executor.py` as a thin orchestration layer.

### Theme 2: Private Cross-Module Imports

**Description:** `batch.py` imports 2 private functions and `batch_spans.py` imports 6 private functions from `executor.py`. This couples sibling modules to executor's internal implementation and violates information hiding. The pattern indicates that executor contains reusable functionality that should have a public interface.

**Affected principles:** P1, P4, P21, P22

**Root cause:** The batch modules were added after executor.py, and the reusable functionality was already private. Rather than promoting the functions to public or extracting them to a shared module, the developers imported them directly.

**Suggested approach:** Extract the 8 imported functions into dedicated modules with public interfaces (`ast_grep_match.py` and `scan.py`). Both executor.py and batch*.py then import from these new modules.

### Theme 3: Duplicated Knowledge Across 3 Pairs

**Description:** Three pairs of duplicated functions exist: `_count_result_matches` (executor/merge), `_missing_languages_from_summary` (entity_front_door/merge), and `_extract_def_name` (executor/symbol_resolver/enrichment). Each pair encodes the same business truth in multiple locations.

**Affected principles:** P7, P4, P15

**Root cause:** When new modules needed shared logic, the functions were copied rather than extracted to a common location. The executor's monolithic structure made it unappealing to import from.

**Suggested approach:** Create `tools/cq/query/record_utils.py` for record-level utilities and `tools/cq/query/result_utils.py` for result-level utilities. Move canonical implementations there; update all call sites.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Deduplicate `_count_result_matches`, `_missing_languages_from_summary`, and `_extract_def_name` into shared modules | small | Eliminates 3 semantic duplications that risk drift |
| 2 | P22 (Contracts) | Add `__all__` to `executor.py`, `enrichment.py`, `planner.py`, `sg_parser.py` | small | Makes public surface explicit; prevents accidental private imports |
| 3 | P24 (Observability) | Add `logging.getLogger(__name__)` and structured log statements to `executor.py` and `sg_parser.py` | small | Enables operational debugging of query execution |
| 4 | P1 (Info hiding) | Extract ast-grep match execution functions into `ast_grep_match.py` with public interface | medium | Eliminates 6 private cross-module imports from `batch_spans.py` |
| 5 | P14 (Demeter) | Define `CallContext` dataclass replacing tuple in `_collect_call_contexts` return type | small | Self-documenting data flow in caller analysis |

## Recommended Action Sequence

1. **Deduplicate shared functions (P7).** Create `record_utils.py` and `result_utils.py`. Move `_extract_def_name`, `_count_result_matches`, `_missing_languages_from_summary` to their canonical homes. Update all import sites. This is zero-risk and immediately eliminates drift potential.

2. **Add `__all__` declarations (P22).** Add `__all__` to `executor.py`, `enrichment.py`, `planner.py`, `sg_parser.py`, `symbol_resolver.py`. This makes the intended public surface explicit and prepares for step 3.

3. **Extract ast-grep match execution module (P1, P4).** Create `ast_grep_match.py` containing `execute_rule_matches`, `group_match_spans`, `match_passes_filters`, `partition_query_metavar_filters`, `resolve_rule_metavar_names`, `resolve_rule_variadic_metavars`, and `AstGrepMatchSpan`. Update imports in `executor.py`, `batch_spans.py`. This eliminates the worst private cross-module import cluster.

4. **Extract scan context module (P1).** Create `scan.py` containing `ScanContext`, `EntityCandidates`, `build_entity_candidates`, `build_scan_context`. Update imports in `executor.py`, `batch.py`. This eliminates the remaining private cross-module imports.

5. **Add structured logging (P24).** Add `logger = logging.getLogger(__name__)` to `executor.py`, `sg_parser.py`, `parser.py`. Log cache hits/misses, scan file counts, query execution timing, and parse failures.

6. **Extract finding builders (P2, P16).** Create `finding_builders.py` containing `_def_to_finding`, `_import_to_finding`, `_call_to_finding`, `_match_to_finding` and their helper functions. These are pure functions with no IO dependencies.

7. **Extract section builders (P2, P3, P12).** Create `section_builders.py` containing the 6 `_build_*_section` functions. Refactor them to accept enrichment data as parameters rather than creating enrichers inline.

8. **Extract cache logic (P2, P11).** Create `executor_cache.py` containing `_EntityFragmentContext`, `_PatternFragmentContext`, `_build_entity_fragment_context`, `_scan_entity_records`. Separate cache lookup from cache write.

9. **Validate plan-query compatibility (P8).** Add a guard in `execute_plan` that checks plan type matches query type before dispatching.

10. **Evaluate `symbol_resolver.py` integration (P20).** Either integrate into the execution pipeline or document its experimental status.
