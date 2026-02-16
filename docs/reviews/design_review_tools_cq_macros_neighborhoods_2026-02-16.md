# Design Review: tools/cq/macros + tools/cq/neighborhood + tools/cq/index + tools/cq/introspection

**Date:** 2026-02-16
**Scope:** `tools/cq/macros/`, `tools/cq/neighborhood/`, `tools/cq/index/`, `tools/cq/introspection/`
**Focus:** Knowledge (7-11), Composition (12-15), Correctness (16-18)
**Depth:** deep
**Files reviewed:** 48

## Executive Summary

These four modules form the analysis backbone of the CQ tool -- macros provide high-level code analysis commands, neighborhood assembles semantic context, index provides repo-wide symbol resolution, and introspection offers bytecode/symtable/CFG primitives. Overall alignment is strong: contracts are explicit via msgspec.Struct and frozen dataclasses, the macro pipeline pattern (Request -> Scan -> Analyze -> Build Result) is consistent, and determinism is well maintained through sha256-based stable identifiers and deterministic section ordering. The primary gaps are: (1) CQS violations in `attach_target_metadata` and `_analyze_function` where functions both mutate state and return information, (2) mutable containers inside frozen `_AnalyzeContext`/`TaintState` that undermine the "make illegal states unrepresentable" principle, (3) `DefIndex.build()` as a static factory that hinders dependency injection and testability, and (4) duplicated comprehension taint-handler logic across four near-identical functions.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 7 | DRY (knowledge) | 2 | small | low | Comprehension taint handlers duplicate iteration logic; Rust def regex appears twice |
| 8 | Design by contract | 3 | - | - | Contracts well-expressed via typed Structs and explicit request envelopes |
| 9 | Parse, don't validate | 2 | medium | low | `result.summary` mutated via setattr/dict update bypasses structured parsing |
| 10 | Illegal states | 1 | medium | medium | Mutable containers inside frozen structs; `CallSite` mutable struct allows post-construction mutation |
| 11 | CQS | 1 | medium | medium | `attach_target_metadata` and `_analyze_function` mix mutation and return; `_populate_*` helpers mutate passed result |
| 12 | DI + explicit composition | 2 | medium | low | `DefIndex.build()` static factory; `INTERACTIVE` profile imported as concrete dependency |
| 13 | Composition over inheritance | 3 | - | - | Shallow inheritance; composition-first design throughout |
| 14 | Law of Demeter | 2 | small | low | `result.summary.target_file`, `bundle.subject.file_path` chains |
| 15 | Tell, don't ask | 2 | small | low | External code mutates `result.key_findings`, `result.sections` directly |
| 16 | Functional core, imperative shell | 2 | medium | low | Pure analysis logic mostly separated; taint analysis mixes IO reads with transforms |
| 17 | Idempotency | 3 | - | - | Index builds and analysis pipelines are idempotent by construction |
| 18 | Determinism | 3 | - | - | sha256 stable IDs; deterministic section ordering via SECTION_ORDER |

## Detailed Findings

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 2/3

**Current state:**
Constants are well-centralized in `tools/cq/macros/constants.py` (2 constants, imported consistently). The `INTERACTIVE` search profile is imported from `tools/cq/search/pipeline/profiles` in multiple macros (`impact.py`, `calls_target.py`, `shared.py`) -- this is acceptable as it references a single authoritative source.

**Findings:**
- Four comprehension taint handlers (`_tainted_listcomp`, `_tainted_setcomp`, `_tainted_generator`, `_tainted_dictcomp`) at `tools/cq/macros/impact.py:355-394` share identical iteration-over-generators logic. The `listcomp`, `setcomp`, and `generator` variants are byte-for-byte identical except for the parameter type annotation. This is semantic duplication of the invariant "a comprehension is tainted if any of its generator iterables or its element expression is tainted."
- The Rust function definition regex `_RUST_DEF_RE` at `tools/cq/macros/calls_target.py:30-33` duplicates the inline pattern at `calls_target.py:130-132` (inside `_resolve_rust_target_definition`). Both encode the same grammar rule for Rust `fn` declarations.
- `_collect_call_sites_for_file` at `tools/cq/macros/calls/analysis.py:475-510` and `_build_call_site_from_record` at `analysis.py:665-715` duplicate the pattern of parsing a file, building a call index, computing context windows, and extracting snippets. The `CallSiteBuildContext` dataclass exists to share context but is only used in the record-based path.

**Suggested improvement:**
Extract a single `_tainted_comprehension` handler parameterized by the element accessor (`.elt` vs `.key`/`.value`) and register it for all four comprehension types. For the Rust regex, extract a module-level compiled pattern used by both callsites. For call site construction, refactor both paths to share `CallSiteBuildContext` construction.

**Effort:** small
**Risk if unaddressed:** low -- The comprehension handlers are unlikely to diverge independently, but if taint semantics for comprehensions change, all four copies must be updated in lockstep.

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
Contracts are explicit and well-structured throughout these modules:
- `MacroRequestBase`, `ScopedMacroRequestBase`, `CallsRequest`, `ImpactRequest` at `tools/cq/macros/contracts.py:18-44` define typed request envelopes with frozen msgspec.Struct semantics.
- `ScoringDetailsV1` at `contracts.py:66-74` captures scoring invariants with explicit bucket levels.
- `NeighborhoodExecutionRequest` at `tools/cq/neighborhood/executor.py:24-36` uses `CqStruct` for the execution envelope.
- `RenderSnbRequest` at `tools/cq/neighborhood/snb_renderer.py:21-30` encapsulates all render inputs.
- `SymbolIndex` Protocol at `tools/cq/index/protocol.py:13-145` defines the DI contract for symbol resolution.
- `InstructionFact`, `ExceptionEntry` at `tools/cq/introspection/bytecode_index.py:17-55,220-242` are frozen dataclasses with explicit fields.
- `BundleBuildRequest` and `TreeSitterNeighborhoodCollectRequest` define explicit input contracts for neighborhood assembly.

Postconditions are enforced by typed return values (`CqResult`, `DefIndex`, `BytecodeIndex`, `CFG`). Preconditions are implicit but reasonable (file existence checks, syntax error handling).

**Suggested improvement:** None required. Contract coverage is strong.

**Effort:** -
**Risk if unaddressed:** -

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Good boundary parsing exists in several places:
- `parse_target_spec()` called at `tools/cq/neighborhood/executor.py:51` converts raw target strings to structured `TargetSpec` once.
- `_parse_signature()` in `tools/cq/macros/sig_impact.py` converts string signatures to `SigParam` lists via AST.
- `_parse_call_expr()` at `tools/cq/macros/calls/analysis.py:253-260` parses call text into `ast.Call` once.

**Findings:**
- `result.summary` is populated via untyped `setattr` at `tools/cq/neighborhood/snb_renderer.py:78` (`setattr(result.summary, key, value)`) and via dict-like `.update()` at `tools/cq/macros/result_builder.py:58` (`self.result.summary.update(kwargs)`). These bypass compile-time type safety; the summary acts as an open dict rather than a parsed, validated structure. Fields like `target_file`, `total_slices`, `semantic_health` are set ad hoc on an opaque summary object.
- `_build_impact_summary` at `tools/cq/macros/impact.py:766-780` returns a raw `dict[str, object]` rather than a typed struct, which is then assigned via `summary_from_mapping()`. The mapping is validated by convention, not by type.
- `context_window` fields (`start_line`, `end_line`) are passed as `dict[str, int]` at `tools/cq/macros/calls/analysis.py:496-497,679` rather than a typed struct, requiring downstream code to access string keys.

**Suggested improvement:**
Define a `ContextWindow` frozen struct with `start_line: int, end_line: int` fields, replacing the `dict[str, int]` usage. Consider a `MacroSummary` typed struct (or a union of per-macro summary types) to replace the open-dict summary pattern. This would shift field name errors from runtime to type-check time.

**Effort:** medium
**Risk if unaddressed:** low -- The current dict-based summaries work but make field-name typos invisible until runtime.

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
Good use of `Literal` types for constrained domains:
- `TaintSiteKind = Literal["source", "call", "return", "assign"]` at `tools/cq/macros/impact.py:18`
- `CallBinding = Literal["ok", "ambiguous", "would_break", "unresolved"]` at `tools/cq/macros/calls/analysis.py:26`
- `CfgEdgeType = Literal["fallthrough", "jump", "exception"]` at `tools/cq/introspection/cfg_builder.py:15`
- `BucketLevel = Literal["low", "med", "high"]` at `tools/cq/macros/contracts.py:15`

**Findings:**
- `TaintState` at `tools/cq/macros/impact.py:73-88` is a `msgspec.Struct` (not frozen) containing mutable `set[str]` and `list[TaintedSite]`. It is then embedded inside `_AnalyzeContext` at `impact.py:467-471` which IS `frozen=True`. This creates the illusion of immutability while the inner `TaintState` is freely mutated via `context.state.visited.add(key)` at `impact.py:487` and `context.state.tainted_sites.extend(...)` at `impact.py:507`. A frozen outer container with mutable inner state is a representation that can mislead readers about mutation safety.
- `CallSite` at `tools/cq/macros/calls/analysis.py:49-121` is a non-frozen `msgspec.Struct` with 22 fields, many of which (`symtable_info`, `bytecode_info`, `context_window`, `context_snippet`) are populated post-construction via `msgspec.structs.replace()` at `analysis.py:503-509`. The non-frozen struct allows any field to be mutated at any point, when in practice fields are set once during construction.
- `ScopeGraph` at `tools/cq/introspection/symtable_extract.py:109-128` is a mutable dataclass whose `scopes`, `scope_by_name`, and `root_scope` are populated incrementally during `_walk_table`. Once construction completes, the graph is treated as immutable. The construction-time mutability could be isolated to a builder.
- `ParamInfo` at `tools/cq/index/def_index.py:22-36` is a non-frozen dataclass with a mutable `kind` field that is set after construction at `def_index.py:205-206` (`param.kind = "POSITIONAL_ONLY"`). This could be a frozen dataclass with `kind` set in the constructor.

**Suggested improvement:**
(1) Make `CallSite` frozen and construct all fields at creation time (pass `context_window` and `context_snippet` to the constructor rather than using post-construction replace). (2) Isolate `TaintState` mutation into an explicit mutable accumulator that is NOT embedded in a frozen struct -- use a separate parameter or return-value pattern. (3) Make `ParamInfo` frozen by passing `kind` to `_extract_param_info()` as a parameter. (4) Consider a `ScopeGraphBuilder` that produces a frozen `ScopeGraph` upon completion.

**Effort:** medium
**Risk if unaddressed:** medium -- Mutable containers inside frozen structs create subtle bugs when code assumes frozen means deeply immutable.

---

#### P11. Command-Query Separation (CQS) -- Alignment: 1/3

**Current state:**
The macro pipeline generally follows a command pattern: scan -> analyze -> build result. However, several key functions violate CQS by both mutating state and returning information.

**Findings:**
- `attach_target_metadata` at `tools/cq/macros/calls_target.py:458-519` is the most significant CQS violation. It (a) mutates `result.summary.target_file` and `result.summary.target_line` at lines 511-512, (b) appends sections to `result.sections` via `add_target_callees_section` at line 513, (c) writes to the cache backend via `persist_target_metadata_cache` at line 500, and (d) returns a 3-tuple `(target_location, target_callees, resolved_language)`. Callers rely on both the mutation and the return value.
- `_analyze_function` at `tools/cq/macros/impact.py:474-530` mutates `context.state.visited` (line 487) and `context.state.tainted_sites` (line 507) as side effects while being called recursively. The function returns `None` but communicates results entirely through state mutation. This makes the recursion hard to reason about.
- `_populate_summary`, `_populate_findings`, `_populate_artifacts` at `tools/cq/neighborhood/snb_renderer.py:55-147` all mutate the passed `result` object. While they return `None` (pure commands), they are called in a sequence where the caller must understand the cumulative mutation order. The render pipeline would be clearer as a functional accumulation.
- `_append_depth_findings`, `_append_kind_sections`, `_append_callers_section`, `_append_evidence` at `tools/cq/macros/impact.py:578-677` all take a `CqResult` and mutate it by appending findings/sections. These are commands (no return value), but the pattern of passing a result and mutating it in 4+ separate functions distributes the result construction logic widely.

**Suggested improvement:**
(1) Split `attach_target_metadata` into a pure query `resolve_target_metadata(request) -> TargetMetadata` that returns the resolved data, and a separate command `apply_target_metadata(result, metadata)` that performs the mutation. (2) Refactor `_analyze_function` to return a `TaintState` (or a list of `TaintedSite`) rather than mutating via side-effect. The recursive accumulation can use return-value merging. (3) Consider a builder pattern for the neighborhood renderer where `_populate_*` functions return data that the builder incorporates.

**Effort:** medium
**Risk if unaddressed:** medium -- The `attach_target_metadata` function is a critical path in the calls macro; its mixed responsibilities make it hard to test the query logic (resolve + cache lookup) independently from the mutation logic (result enrichment).

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
The `SymbolIndex` Protocol at `tools/cq/index/protocol.py:13` provides a clean abstraction for symbol lookup, enabling DI for testing. `MacroResultBuilder` at `tools/cq/macros/result_builder.py:24` accepts `Toolchain | None` as a parameter, enabling injection. The neighborhood pipeline composes cleanly: `executor.py` orchestrates `resolve_target -> build_neighborhood_bundle -> render_snb_result`.

**Findings:**
- `DefIndex.build()` at `tools/cq/index/def_index.py:424-483` is a static factory that performs filesystem IO (reads all Python files in the repo) and returns a fully-populated index. Callers like `cmd_impact` at `tools/cq/macros/impact.py:846` call `DefIndex.build(request.root)` directly, creating a hidden dependency on the filesystem. The `SymbolIndex` Protocol exists but `DefIndex.build()` does not accept or return through it -- callers create `DefIndex` concretely.
- The `INTERACTIVE` profile at `tools/cq/search/pipeline/profiles` is imported as a concrete module-level constant in `tools/cq/macros/impact.py:38`, `tools/cq/macros/calls_target.py:27`, and `tools/cq/macros/shared.py:19`. While this is a stable value, it creates a coupling to the search pipeline's profile system. If the profile needs to be configurable per-invocation, the current pattern does not allow injection.
- `_enrich_call_site` at `tools/cq/macros/calls/analysis.py:290-344` performs a lazy import of `analyze_bytecode` and `analyze_symtable` from `tools.cq.query.enrichment` at line 311. This creates a hidden runtime dependency on the query enrichment module.
- `BytecodeIndex.from_code()` at `tools/cq/introspection/bytecode_index.py:126-141` and `build_cfg()` at `tools/cq/introspection/cfg_builder.py:394-419` are well-designed factory methods that take explicit `CodeType` inputs with no hidden dependencies.

**Suggested improvement:**
Add a `symbol_index` parameter to macro entry points (e.g., `cmd_impact(request, *, index: SymbolIndex | None = None)`) defaulting to `DefIndex.build(request.root)` when not provided. This enables test injection without changing the default call path. For `INTERACTIVE`, consider adding a `limits` parameter to macro request structs with `INTERACTIVE` as the default.

**Effort:** medium
**Risk if unaddressed:** low -- The current pattern works but requires filesystem access for all testing of macro logic.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
Inheritance is minimal and shallow throughout:
- `MacroRequestBase` -> `ScopedMacroRequestBase` at `tools/cq/macros/contracts.py:18-30` is one level of inheritance for adding include/exclude fields.
- `MacroRequestBase` -> `CallsRequest`, `ImpactRequest` at `contracts.py:33-37` and `impact.py:91-96` add domain-specific fields.
- No deep hierarchies exist in any of the four modules.
- Behavior composition is the dominant pattern: `MacroResultBuilder` composes `RunContext`, `CqResult`, `RustFallbackPolicyV1`. The neighborhood pipeline composes `resolve_target + build_neighborhood_bundle + render_snb_result`. `CFG` composes `BasicBlock` and `CFGEdge`.

**Suggested improvement:** None required. The shallow inheritance is justified for shared request fields, and composition is used correctly everywhere else.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most function signatures pass direct collaborators. However, there are several chain-access patterns.

**Findings:**
- `tools/cq/neighborhood/snb_renderer.py:61-73`: Seven lines of `result.summary.X = ...` access, reaching two levels deep. Examples: `result.summary.target = request.target`, `result.summary.total_slices = len(bundle.slices)`, `result.summary.bundle_id = bundle.bundle_id`.
- `tools/cq/neighborhood/snb_renderer.py:68-73`: `bundle.subject.file_path`, `bundle.subject.name`, `bundle.graph.node_count`, `bundle.graph.edge_count` -- three-level access chains through the bundle's nested objects.
- `tools/cq/macros/calls_target.py:511-512`: `result.summary.target_file = target_location[0]` reaches into the result's summary to set individual fields.
- `tools/cq/macros/impact.py:487`: `context.state.visited.add(key)` is a three-level chain (`context` -> `state` -> `visited` -> `add`).
- `tools/cq/macros/calls/entry.py` (from earlier reading): `result.run.run_id` type of chains for accessing metadata.

**Suggested improvement:**
For the `_populate_summary` function, consider having `RenderSnbRequest` provide a method like `to_summary_fields() -> dict[str, object]` that the caller applies in one step, rather than the renderer reaching into multiple levels of the request and bundle. For `_AnalyzeContext`, expose a `record_visit(key)` method on TaintState rather than allowing callers to access the internal visited set.

**Effort:** small
**Risk if unaddressed:** low -- These are read-only chains in most cases and unlikely to cause cascading breakage, but they do couple the renderer to the internal structure of `SemanticNeighborhoodBundleV1`.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The `MacroResultBuilder` at `tools/cq/macros/result_builder.py:24-114` is a good example of "tell" -- callers tell the builder to add findings, set summary fields, and apply policies through its API. However, several patterns across the codebase expose raw data structures for external mutation.

**Findings:**
- `_append_depth_findings(result, summary)` at `tools/cq/macros/impact.py:578-603` takes a `CqResult` and appends findings directly to `result.key_findings`. The result object does not encapsulate the "add a depth finding" rule -- external code decides what to append. Similarly for `_append_kind_sections` (line 613), `_append_callers_section` (line 635), `_append_evidence` (line 657).
- `_populate_findings` at `tools/cq/neighborhood/snb_renderer.py:81-136` directly appends to `result.key_findings`, `result.sections`, and `result.evidence`. The `CqResult` is treated as a passive data bag.
- `collect_call_sites` at `tools/cq/macros/calls/analysis.py:407-417` is a thin wrapper over `_collect_call_sites` that adds no behavior -- the public API merely delegates without encapsulation.
- `add_target_callees_section(result, ...)` at `tools/cq/macros/calls_target.py:419-444` asks for the result and externally decides how to build and append the section. The result does not "know" about callee sections.

**Suggested improvement:**
The `MacroResultBuilder` pattern is the right direction. Extend its usage so that `_build_impact_result` uses builder methods (`builder.add_findings(...)`, `builder.add_section(...)`) instead of directly mutating `result.key_findings` and `result.sections`. The impact macro already partially uses `MacroResultBuilder` (line 792) but then bypasses it to mutate the result directly (lines 801-828).

**Effort:** small
**Risk if unaddressed:** low -- Scattered mutation of result internals increases the risk of inconsistent result states, but the current code is deterministic in practice.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The analysis modules have a clear separation in many places:
- `_analyze_call()` at `tools/cq/macros/calls/analysis.py:176-218` is a pure function: `ast.Call -> CallAnalysis`.
- `_detect_hazards()` at `analysis.py:263-287` is pure: `(ast.Call, CallAnalysis) -> list[str]`.
- `extract_instruction_facts()` at `tools/cq/introspection/bytecode_index.py:57-110` is pure: `CodeType -> list[InstructionFact]`.
- `build_cfg()` at `tools/cq/introspection/cfg_builder.py:394-419` is pure: `CodeType -> CFG`.
- `_generate_bundle_id()` in `tools/cq/neighborhood/bundle_builder.py` is pure: inputs -> deterministic sha256 hash.

**Findings:**
- `_analyze_function` at `tools/cq/macros/impact.py:474-530` mixes filesystem IO (reading source files at lines 490-497) with analysis logic (taint visitor at lines 505-507) and recursive propagation (lines 510-530). The "imperative shell" (file reading) is interleaved with the "functional core" (taint analysis) at each recursion level.
- `_collect_call_sites_for_file` at `tools/cq/macros/calls/analysis.py:475-510` reads the file (IO), parses it (transform), finds calls (pure analysis), computes context windows (pure), and extracts snippets (pure) all in one function. The IO is at the top but not separated from the analysis pipeline.
- `resolve_target_definition` at `tools/cq/macros/calls_target.py:36-57` coordinates filesystem search, AST parsing, and result selection. The search (IO) and parsing (transform) are interleaved.
- `build_neighborhood_bundle` in `tools/cq/neighborhood/bundle_builder.py` performs tree-sitter parsing (IO/transform boundary) and neighborhood collection (pure transform) within the same pipeline.

**Suggested improvement:**
For `_analyze_function`, separate file reading into a "source provider" function that can be injected or pre-loaded, then pass the source text to a pure `analyze_taint(source, fn, tainted_params, index) -> list[TaintedSite]` function. This would allow the taint analysis logic to be tested without filesystem access. For call site collection, pre-load source texts at the pipeline entry point and pass them through.

**Effort:** medium
**Risk if unaddressed:** low -- The current pattern works correctly but requires real files for testing the analysis logic.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Operations across all four modules are idempotent by construction:
- `DefIndex.build(root)` at `tools/cq/index/def_index.py:424-483` produces the same index given the same filesystem state. Re-running produces identical results.
- `extract_instruction_facts(code)` at `tools/cq/introspection/bytecode_index.py:57-110` is deterministic for a given code object.
- `build_cfg(code)` at `tools/cq/introspection/cfg_builder.py:394-419` produces identical CFGs for identical code objects.
- `extract_scope_graph(source, filename)` at `tools/cq/introspection/symtable_extract.py:131-164` is deterministic.
- `_stable_callsite_id` at `tools/cq/macros/calls/analysis.py:347-356` generates deterministic IDs via sha256.
- Cache write/read in `calls_target_cache.py` uses snapshot-based invalidation (`target_scope_snapshot_digest` at line 61-83), ensuring stale cache entries are refreshed correctly.
- `execute_neighborhood` at `tools/cq/neighborhood/executor.py:39-100` produces deterministic results via sha256 bundle IDs and deterministic section ordering.

**Suggested improvement:** None required. Idempotency is well maintained.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is explicitly maintained across the pipeline:
- `_stable_callsite_id` at `tools/cq/macros/calls/analysis.py:347-356` uses `sha256(f"{file}:{line}:{col}:{callee}:{context}")` for reproducible call site identifiers.
- `_generate_bundle_id` in `tools/cq/neighborhood/bundle_builder.py` uses sha256 over bundle content for deterministic IDs.
- `SECTION_ORDER` at `tools/cq/neighborhood/section_layout.py` (17-slot tuple) ensures deterministic section ordering regardless of assembly order.
- `assign_result_finding_ids(result)` at `tools/cq/neighborhood/executor.py:98` and `tools/cq/macros/result_builder.py:113` ensures stable finding identifiers.
- File iteration in `_iter_source_files` at `tools/cq/index/def_index.py:268-285` uses sorted glob patterns.
- `tabulate_files` at `tools/cq/index/files.py:144-146` returns files sorted by path: `sorted(filtered, key=lambda path: path.as_posix())`.
- `uuid7_str()` at `tools/cq/neighborhood/executor.py:46` uses monotonic UUID7 for run IDs, providing ordered but unique identifiers.
- The `_EXPR_TAINT_HANDLERS` dispatch table at `tools/cq/macros/impact.py:399-417` is static and deterministic.

**Suggested improvement:** None required. Determinism guarantees are strong.

**Effort:** -
**Risk if unaddressed:** -

---

## Cross-Cutting Themes

### Theme 1: CqResult as a Mutable Bag Pattern

**Description:** Throughout macros and neighborhood rendering, `CqResult` is treated as an open mutable container. Functions receive it as a parameter and directly append to `.key_findings`, `.sections`, `.evidence`, and set attributes on `.summary`. This pattern appears in `impact.py` (4 `_append_*` functions), `snb_renderer.py` (3 `_populate_*` functions), `calls_target.py` (`attach_target_metadata`), and `calls/insight.py` (5 `_add_*_section` functions).

**Root cause:** `CqResult` is designed as a general-purpose result container with mutable lists. There is no builder-only construction phase that locks the result after assembly.

**Affected principles:** P10 (illegal states), P11 (CQS), P14 (Demeter), P15 (tell don't ask).

**Suggested approach:** The `MacroResultBuilder` already exists and provides a cleaner API. Extend its adoption: have all macro pipelines construct results exclusively through the builder, then call `.build()` to produce a sealed result. The builder's `add_findings`, `add_section`, `set_summary` methods already encapsulate the mutation; the gap is that many callsites bypass the builder after initial construction.

### Theme 2: Frozen Container With Mutable Interior

**Description:** Several frozen structs contain mutable interior data: `_AnalyzeContext` (frozen) wrapping `TaintState` (mutable sets/lists), `CallSiteBuildContext` (frozen) wrapping mutable `dict[tuple, ast.Call]`. This pattern appears in the taint analysis pipeline and the call site construction pipeline.

**Root cause:** Recursive/accumulative algorithms need mutable state, but the surrounding context is frozen to prevent accidental field reassignment.

**Affected principles:** P10 (illegal states), P16 (functional core).

**Suggested approach:** Use explicit accumulator patterns. Instead of embedding mutable state in frozen containers, pass the accumulator as a separate parameter or use return-value accumulation. For taint analysis, `_analyze_function` could return `list[TaintedSite]` and the caller merges results. For `_AnalyzeContext`, drop the `state` field and pass it explicitly.

### Theme 3: IO Interleaved With Analysis

**Description:** Several analysis functions read files from disk in the middle of their logic: `_analyze_function` reads source at each recursion level, `_collect_call_sites_for_file` reads and parses per-file, `resolve_target_definition` searches and parses files. This interleaving makes unit testing require real filesystems.

**Root cause:** The analysis APIs were designed for convenience -- pass a root path and the function does everything. Separating IO from analysis would require pre-loading source texts.

**Affected principles:** P12 (DI), P16 (functional core), P23 (testability).

**Suggested approach:** Introduce a `SourceProvider` protocol (or simple callable `Path -> str | None`) that can be injected. Default implementation reads from disk; test implementation returns pre-loaded strings. This is a common pattern in the CQ codebase (the search pipeline already uses similar abstractions).

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P10 | Make `CallSite` frozen; pass all fields at construction time | small | Prevents post-construction mutation surprises |
| 2 | P7 | Extract shared comprehension taint handler in `impact.py` | small | Eliminates 4 near-identical functions |
| 3 | P14 | Add `TaintState.record_visit(key)` method to hide internal set | small | Reduces chain access, encapsulates mutation |
| 4 | P11 | Split `attach_target_metadata` into query + command | medium | Enables isolated testing of resolution logic |
| 5 | P10 | Make `ParamInfo` frozen with `kind` passed to constructor | small | Eliminates post-construction field mutation |

## Recommended Action Sequence

1. **Make `CallSite` frozen** (P10): Change to `frozen=True` and ensure all fields are passed at construction. Update `_collect_call_sites_for_file` and `_build_call_site_from_record` to provide all fields upfront rather than using `msgspec.structs.replace()`. This is a low-risk change since `replace` already returns a new instance.

2. **Extract shared comprehension taint handler** (P7): Create `_tainted_comprehension(visitor, expr, *, element_accessor)` in `impact.py` and register for `ListComp`, `SetComp`, `GeneratorExp`. Create a separate `_tainted_dictcomp` variant that checks both key and value. Removes 3 of 4 duplicate functions.

3. **Encapsulate TaintState mutation** (P10, P14): Add `record_visit(key)`, `add_sites(sites)`, and `mark_tainted(var)` methods to `TaintState`. Update `_analyze_function` and `TaintVisitor` to use these methods instead of direct set/list access.

4. **Split `attach_target_metadata`** (P11): Extract `resolve_target_metadata(request) -> TargetMetadataResult` as a pure query function that performs resolution and caching. Keep `apply_target_metadata(result, metadata)` as the mutation command. Update `calls/entry.py` to call both in sequence.

5. **Add `index` parameter to macro entry points** (P12): Add `index: SymbolIndex | None = None` parameter to `cmd_impact` and similar macros, defaulting to `DefIndex.build(request.root)`. This enables test injection without changing the default path.

6. **Define typed `ContextWindow` struct** (P9): Replace `dict[str, int]` context window pattern with a frozen `ContextWindow(start_line: int, end_line: int)` struct. Update `_compute_context_window` and all consumers.

7. **Consolidate Rust def regex** (P7): Extract the duplicated Rust function definition regex in `calls_target.py` into a single module-level constant used by both `_RUST_DEF_RE` and the inline pattern in `_resolve_rust_target_definition`.
