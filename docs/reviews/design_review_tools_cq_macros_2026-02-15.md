# Design Review: tools/cq/macros/ + tools/cq/index/ + tools/cq/introspection/

**Date:** 2026-02-15
**Scope:** `tools/cq/macros/` (primary), `tools/cq/index/`, `tools/cq/introspection/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 28

## Executive Summary

The analysis commands subsystem is well-structured at the inter-module level: contracts.py and shared.py provide genuine shared infrastructure, the Rust fallback policy is cleanly factored, and the introspection layer is exemplary in its separation of concerns. However, `calls.py` at 2,274 lines is a clear SRP violation that conflates scanning, analysis, neighborhood construction, semantic overlay, and front-door insight assembly into a single file. The index layer (`DefIndex`, `CallResolver`, `ArgBinder`) lacks dependency inversion -- it is always constructed via `DefIndex.build()` directly, preventing test injection and reuse. String-typed enumerations for confidence levels and site kinds propagate through the system without type safety, and the `macro_scoring_details` function returns `dict[str, object]` instead of a typed struct, creating a latent contract drift risk.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | DefIndex exposes mutable internal state; CallSite uses `dict[str, object]` for symtable/bytecode info |
| 2 | Separation of concerns | 1 | large | high | `calls.py` merges scanning, analysis, neighborhood, semantic overlay, and insight assembly |
| 3 | SRP | 1 | large | high | `calls.py` changes for 5+ independent reasons; `def_index.py` mixes traversal, storage, and lookup |
| 4 | High cohesion, low coupling | 2 | medium | medium | Macro modules are cohesive; but `calls.py` depends on 15+ imports spanning 6 packages |
| 5 | Dependency direction | 2 | small | low | Core logic mostly depends inward; macros depend on index which depends on core utils |
| 6 | Ports & Adapters | 1 | medium | medium | No port abstraction for DefIndex or rg adapter; macros directly call concrete implementations |
| 7 | DRY (knowledge) | 2 | medium | medium | Two-stage collection pattern varies per macro; `_SELF_CLS` duplicated; `load_or_build` duplicates `build` |
| 8 | Design by contract | 2 | small | low | msgspec structs enforce shape; but preconditions are implicit (e.g., `max_depth > 0`) |
| 9 | Parse, don't validate | 2 | small | low | Signature parsing in sig_impact uses AST boundary parsing; but confidence strings not parsed to enum |
| 10 | Make illegal states unrepresentable | 1 | medium | medium | String-typed enums for confidence, kind, binding; `CFGEdge.edge_type` is bare `str` |
| 11 | CQS | 2 | small | low | Most functions are query-style; `_walk_table` and visitor patterns mutate + traverse simultaneously |
| 12 | DI + explicit composition | 1 | medium | medium | DefIndex always built inline; no injection point; macros hard-wire rg adapter |
| 13 | Composition over inheritance | 3 | - | - | No deep hierarchies; MacroRequestBase is shallow (1 level); behavior composed via functions |
| 14 | Law of Demeter | 2 | small | low | `payload.get("coverage").get("status")` chains in `calls.py:1819-1823`; generally acceptable |
| 15 | Tell, don't ask | 2 | medium | medium | Finding construction scattered across callers rather than encapsulated in domain objects |
| 16 | Functional core, imperative shell | 2 | medium | medium | Taint handlers are pure; but scan/neighborhood/semantic overlay mixes IO and logic in `calls.py` |
| 17 | Idempotency | 3 | - | - | All macros are read-only analyses; re-running produces same results |
| 18 | Determinism / reproducibility | 2 | small | low | Mostly deterministic; ripgrep candidate ordering may vary; hash-based call_id is stable |
| 19 | KISS | 2 | medium | medium | Context snippet extraction uses complex indentation heuristics (7 helper functions) |
| 20 | YAGNI | 2 | small | low | `load_or_build` exists but duplicates `build`; `BytecodeIndex.filter_by_stack_effect` may be unused |
| 21 | Least astonishment | 2 | small | low | `load_or_build` name implies persistence but does not persist; `cmd_calls` takes 4 positional args unlike other macros |
| 22 | Declare and version public contracts | 2 | small | low | msgspec structs versioned (V1 suffix); but scoring returns untyped dict |
| 23 | Design for testability | 1 | medium | high | DefIndex non-injectable; taint propagation requires full AST+index setup; no pure-logic extraction |
| 24 | Observability | 2 | small | low | RunContext/RunMeta provide structured telemetry; semantic telemetry tracked; but no structured logging in index layer |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Most modules hide their internals effectively. The `macros/contracts.py` base classes expose only what callers need. However, several leaks exist in the index and calls layers.

**Findings:**
- `tools/cq/index/def_index.py:418`: `DefIndex.modules` is a public mutable `dict[str, ModuleInfo]` with no access control. Any consumer can mutate the index after construction.
- `tools/cq/macros/calls.py:160-163`: `CallSite.symtable_info` and `CallSite.bytecode_info` are typed as `dict[str, object] | None`, exposing unstructured internal analysis details to all consumers.
- `tools/cq/index/def_index.py:292-296`: `DefIndexVisitor` exposes its internal lists (`functions`, `classes`, `module_aliases`, `symbol_aliases`) as public mutable attributes rather than building a frozen result.

**Suggested improvement:**
Make `DefIndex.modules` a read-only property backed by a private `_modules` field, or make `DefIndex` a frozen dataclass populated at construction. Replace `dict[str, object]` in `CallSite` with typed structs (`SymtableInfoV1`, `BytecodeInfoV1`).

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`calls.py` is the primary violation. It contains scanning logic (ripgrep pre-filtering, AST parsing), analysis logic (call shape summarization, hazard detection), neighborhood construction, semantic overlay application, front-door insight assembly, and result formatting -- all in a single 2,274-line file.

**Findings:**
- `tools/cq/macros/calls.py:1-2274`: The file handles at least 5 distinct concerns: (1) call site scanning via rg+AST (`_scan_call_sites`, lines ~400-500), (2) call analysis and summarization (`_summarize_sites`, `_analyze_calls_sites`), (3) neighborhood construction (`_build_calls_neighborhood`, lines ~1600-1700), (4) semantic overlay application (`_apply_calls_semantic`, lines ~1700-1811), (5) front-door insight assembly (`_build_calls_front_door_insight`, `_build_calls_front_door_state`, lines ~1936-2077).
- `tools/cq/macros/calls.py:550-750`: Context snippet extraction is a self-contained text-processing concern (7 helper functions: `_line_indent`, `_is_blank`, `_first_nonblank_index`, `_skip_docstring_block`, `_collect_function_header_indices`, `_collect_anchor_block_indices`, `_select_context_indices`) embedded in the calls module.
- Contrast with the well-separated smaller macros: `side_effects.py` (442 lines), `exceptions.py` (567 lines), `bytecode.py` (423 lines) each handle a single concern cleanly.

**Suggested improvement:**
Extract `calls.py` into sub-modules: `calls/scanning.py` (rg pre-filter + AST collection), `calls/analysis.py` (shape summarization, hazard detection), `calls/neighborhood.py` (neighborhood construction), `calls/semantic.py` (semantic overlay), `calls/insight.py` (front-door insight assembly), `calls/context_snippet.py` (context window extraction). The top-level `calls/__init__.py` would re-export `cmd_calls` and `collect_call_sites`.

**Effort:** large
**Risk if unaddressed:** high -- any change to neighborhood logic forces touching a 2,274-line file; review and merge conflict surface is excessive.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`calls.py` changes for at least 5 independent reasons: scanning strategy, analysis heuristics, neighborhood rendering, semantic overlay protocol, and insight card schema. `def_index.py` (669 lines) mixes AST traversal, data storage, and lookup logic.

**Findings:**
- `tools/cq/macros/calls.py:2226-2273`: The `cmd_calls` entry point orchestrates scan -> result build -> rust fallback -> ID assignment -> cache eviction. Each of these is a separate axis of change.
- `tools/cq/index/def_index.py:286-402`: `DefIndexVisitor` is an AST visitor that both traverses and constructs domain objects (`FnDecl`, `ClassDecl`, `ModuleInfo`). The traversal logic and the domain-object construction logic change for different reasons.
- `tools/cq/index/def_index.py:405-513`: `DefIndex` class has `build()` (construction), `load_or_build()` (duplicate construction), `all_functions()`, `all_classes()`, `find_function_by_name()`, `find_function_by_qualified_name()`, `find_class_by_name()`, `resolve_import_alias()` -- combining index construction with query interface.

**Suggested improvement:**
For `DefIndex`: separate construction (`DefIndexBuilder`) from the query interface (`DefIndex`). The builder scans files and produces a frozen index; the index provides lookup methods. For `calls.py`: the decomposition suggested in P2 would resolve the SRP violation.

**Effort:** large
**Risk if unaddressed:** high -- modifications to any concern in `calls.py` risk regressions in unrelated concerns.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The smaller macro modules (`side_effects.py`, `exceptions.py`, `bytecode.py`, `scopes.py`, `imports.py`) are highly cohesive. The shared infrastructure (`contracts.py`, `shared.py`, `rust_fallback_policy.py`) effectively reduces coupling between macros. However, `calls.py` has excessive fan-out.

**Findings:**
- `tools/cq/macros/calls.py:1-63`: Imports from 15+ distinct modules spanning `tools/cq/core/`, `tools/cq/macros/`, `tools/cq/query/`, `tools/cq/search/pipeline/`, `tools/cq/search/rg/`, plus `msgspec`, `ast`, `hashlib`, `re`, `Counter`. This indicates the module has absorbed responsibilities from too many domains.
- `tools/cq/macros/shared.py:9-20`: Imports from scoring, index.files, index.repo, contracts, search.pipeline.profiles, search.rg.adapter -- pulling in search infrastructure for what should be a utilities module.

**Suggested improvement:**
Decomposing `calls.py` (per P2) would naturally reduce fan-out per sub-module to 5-8 imports each. For `shared.py`, consider whether `resolve_target_files` (which depends on `find_symbol_definition_files` from the search adapter) belongs in a more specialized module.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The overall dependency flow is sound: macros depend on index, index depends on core utils, introspection depends only on stdlib. The inward-dependency principle is mostly respected.

**Findings:**
- `tools/cq/macros/calls.py:46-53`: The calls macro directly imports from `tools/cq/search/rg/adapter` and `tools/cq/query/sg_parser`, creating a dependency from the macro layer into the search/query infrastructure. Ideally, the macro would depend on an abstraction rather than the concrete search adapter.
- The introspection layer (`tools/cq/introspection/`) is exemplary -- it depends only on stdlib (`dis`, `symtable`, `dataclasses`) and sibling modules within introspection.

**Suggested improvement:**
The dependency on `search.rg.adapter` could be inverted by defining a `CandidateFinder` protocol in the macros or index layer, with the rg adapter implementing it. This is a moderate change that may not be justified by current pain.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
There are no port abstractions for the key external dependencies: ripgrep search, AST parsing, file system access, or the DefIndex. Macros directly call concrete implementations.

**Findings:**
- `tools/cq/macros/impact.py:31`: `from tools.cq.index.def_index import DefIndex, FnDecl` -- direct concrete dependency with no abstraction boundary. The impact macro calls `DefIndex.build(request.root)` at line ~712 (via `_resolve_functions`), hard-wiring the construction strategy.
- `tools/cq/macros/calls.py:48-53`: Direct imports of `find_call_candidates`, `find_def_lines`, `find_files_with_pattern` from the rg adapter. No port interface exists for candidate finding.
- `tools/cq/macros/sig_impact.py:25`: `from tools.cq.macros.calls import collect_call_sites, group_candidates, rg_find_candidates` -- sig_impact couples directly to calls internals rather than through a port.

**Suggested improvement:**
Define a `SymbolIndex` protocol with `find_function_by_name`, `find_function_by_qualified_name`, `all_functions` methods. `DefIndex` would implement this protocol. Macros would depend on the protocol, enabling test doubles. Similarly, define a `CandidateFinder` protocol for the ripgrep abstraction.

**Effort:** medium
**Risk if unaddressed:** medium -- currently prevents unit testing macro logic without building a real DefIndex from source files.

---

### Category: Knowledge

#### P7. DRY (knowledge, not lines) -- Alignment: 2/3

**Current state:**
The shared scoring infrastructure (`macro_scoring_details`, `macro_score_payload`) centralizes scoring knowledge. The Rust fallback policy (`RustFallbackPolicyV1`) eliminates per-macro duplication. However, several knowledge duplications remain.

**Findings:**
- `tools/cq/index/def_index.py:16` and `tools/cq/index/call_resolver.py:11`: `_SELF_CLS: set[str] = {"self", "cls"}` is defined identically in both files. This is a semantic truth (what names indicate method receivers) encoded in two places.
- `tools/cq/index/def_index.py:420-476` and `tools/cq/index/def_index.py:478-513`: `DefIndex.build()` and `DefIndex.load_or_build()` contain duplicated default pattern lists (`exclude_patterns = ["**/.*", "**/__pycache__/**", ...]`). `load_or_build` is literally a wrapper that calls `build` with identical defaults.
- `tools/cq/macros/imports.py:31-77`: `_STDLIB_PREFIXES` is a hand-maintained set of stdlib module names. This knowledge is likely available from `sys.stdlib_module_names` (Python 3.10+) or could be derived from a single authoritative source.
- The two-stage collection pattern (rg pre-filter then AST parse) is repeated across macros with minor variations, but the variations are meaningful (different AST visitors, different output types), so this is not a DRY violation per se.

**Suggested improvement:**
Extract `_SELF_CLS` to a shared constant in `tools/cq/index/__init__.py` or a constants module. Remove `load_or_build` entirely (it adds no value over `build`). Replace `_STDLIB_PREFIXES` with `sys.stdlib_module_names` where available.

**Effort:** medium
**Risk if unaddressed:** medium -- the `_SELF_CLS` duplication is a latent divergence risk; the `_STDLIB_PREFIXES` set will silently become incomplete as Python evolves.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
msgspec structs provide structural contracts at the boundary. The macro request base classes (`MacroRequestBase`, `ScopedMacroRequestBase`) define clear input shapes. However, preconditions and invariants are often implicit.

**Findings:**
- `tools/cq/macros/impact.py:93`: `ImpactRequest.max_depth` defaults to 5 but has no documented or enforced minimum. Passing `max_depth=0` would silently skip all analysis without error.
- `tools/cq/index/def_index.py:423`: `DefIndex.build(root, max_files=10000)` -- the `max_files` parameter has no lower-bound check. `max_files=0` would produce an empty index silently.
- `tools/cq/macros/calls.py:91-163`: `CallSite` has 23 fields but no invariant relating them (e.g., `resolution_confidence` should constrain `target_names` -- if "unresolved", `target_names` should be empty).

**Suggested improvement:**
Add `__post_init__` validation to `ImpactRequest` (or a `validate` classmethod) ensuring `max_depth >= 1`. Add defensive checks at `DefIndex.build` entry for `max_files >= 1`. Document the relationship between `resolution_confidence` and `target_names` in `CallSite`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Signature parsing in `sig_impact.py` is a good example of this principle: it parses a string into `SigParam` structs at the boundary. The macro request structs parse CLI input into typed structures. However, confidence levels remain as strings throughout.

**Findings:**
- `tools/cq/macros/sig_impact.py:68-94`: `_parse_signature` converts a raw string into a list of `SigParam` structs via AST parsing -- a textbook application of "parse, don't validate."
- `tools/cq/index/call_resolver.py:59`: `ResolvedCall.confidence` is typed as `str` with comment "exact", "likely", "ambiguous", "unresolved". This is validation-style thinking (check the string later) rather than parsing into a structured type at the boundary.
- `tools/cq/macros/calls.py:154`: `CallSite.resolution_confidence: str` -- same issue, propagated from `ResolvedCall`.

**Suggested improvement:**
Define a `ResolutionConfidence` enum (`EXACT`, `LIKELY`, `AMBIGUOUS`, `UNRESOLVED`) in the index layer. Have `resolve_call_targets` return the enum directly. Similarly, define `SiteKind` enum for `TaintedSite.kind`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
Several data types permit states that should be impossible, relying on convention rather than type enforcement.

**Findings:**
- `tools/cq/index/call_resolver.py:59`: `confidence: str` allows any string, not just the 4 valid values.
- `tools/cq/macros/impact.py:64`: `TaintedSite.kind: str` allows any string, not just "source", "call", "return", "assign".
- `tools/cq/macros/calls.py:125`: `CallSite.binding: str` allows any string, not just "ok", "ambiguous", "would_break", "unresolved".
- `tools/cq/introspection/cfg_builder.py:72`: `CFGEdge.edge_type: str` with comment "fallthrough", "jump", "exception" -- three valid values encoded as bare string.
- `tools/cq/macros/side_effects.py:92` (approx): `SideEffect.kind` is typed as `Literal["top_level_call", "global_write", "ambient_read"]` -- this is the correct pattern, but it is the exception rather than the rule.

**Suggested improvement:**
Replace bare `str` types with `Literal` unions or enums for all categorical fields: `ResolutionConfidence`, `TaintSiteKind`, `CallBinding`, `CFGEdgeType`. This shifts invalid-state bugs from runtime to type-check time.

**Effort:** medium
**Risk if unaddressed:** medium -- string typos in these fields silently produce incorrect behavior.

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most functions follow CQS. The taint handlers are pure queries. The main violation pattern is visitor methods that both traverse and mutate state.

**Findings:**
- `tools/cq/introspection/symtable_extract.py:167-224`: `_walk_table` both traverses the symbol table (query) and mutates `graph.scopes` and `graph.scope_by_name` (command) in the same call.
- `tools/cq/macros/impact.py:116-206`: `TaintVisitor` visit methods both traverse AST nodes and mutate `self.tainted`, `self.sites`, and `self.calls` -- inherent to the visitor pattern but still a CQS violation.
- `tools/cq/macros/calls.py:1993-1995`: `_attach_calls_neighborhood_section` mutates `result.sections` (command) and returns `None` -- correctly follows CQS for a command.

**Suggested improvement:**
For `_walk_table`, consider having it return a list of `ScopeFact` rather than mutating the graph in-place, then constructing the graph from the collected facts. This is a minor improvement. The visitor-pattern mutations are inherent to the pattern and not worth changing.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
Dependencies are constructed inline rather than injected. The most impactful case is `DefIndex`, which is always built by calling `DefIndex.build(root)` directly from macro code.

**Findings:**
- `tools/cq/macros/impact.py:673-677` (via `_resolve_functions` called from `cmd_impact`): The impact macro constructs `DefIndex.build(request.root)` inline. There is no way to inject a pre-built or mock index.
- `tools/cq/macros/calls.py:46-53`: Ripgrep adapter functions (`find_call_candidates`, `find_def_lines`, `find_files_with_pattern`) are imported directly. There is no way to substitute a test double for the search backend.
- `tools/cq/macros/shared.py:119-126`: `resolve_target_files` imports and calls `find_symbol_definition_files` from the rg adapter directly, hard-wiring the search implementation.
- Contrast: `RustFallbackPolicyV1` is a good example of explicit composition -- the policy is constructed and passed to `apply_rust_fallback_policy`, not hidden inside the macro.

**Suggested improvement:**
Introduce a `MacroRuntime` or `AnalysisContext` that holds the `DefIndex`, candidate finder, and search limits. Macros would receive this context (or have it injected via the request) rather than constructing dependencies inline. This enables test injection without monkeypatching.

**Effort:** medium
**Risk if unaddressed:** medium -- testing macro logic requires real filesystem access and real ripgrep execution.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The subsystem strongly favors composition. Inheritance is used minimally and appropriately.

**Findings:**
- `tools/cq/macros/contracts.py:1-65`: `MacroRequestBase` and `ScopedMacroRequestBase` form a shallow 1-level hierarchy via msgspec.Struct inheritance. This is appropriate for shared fields.
- AST visitors inherit from `ast.NodeVisitor`, which is the standard library pattern and unavoidable.
- Behavior is composed via functions (taint handlers, scoring utilities, Rust fallback policy) rather than class hierarchies.

**Suggested improvement:** None needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. A few chains exist in the calls module when navigating nested dict payloads from semantic overlays.

**Findings:**
- `tools/cq/macros/calls.py:1819-1823`: `payload.get("coverage").get("status")` -- navigating two levels into an untyped dict. The guard `isinstance(coverage, dict)` mitigates crashes but the chain remains.
- `tools/cq/macros/calls.py:1826-1831`: Similar nested navigation: `payload.get(key)` -> `value.values()` -> `nested` -> `isinstance(item, dict)`. Three levels of structure navigation.
- `tools/cq/macros/calls.py:1850-1864`: `_extract_rust_calls_reason` navigates `payload.get("semantic_planes").get("reason")` and `payload.get("degrade_events")` -> iteration -> `event.get("category")`.

**Suggested improvement:**
Define typed structs for semantic overlay payloads (`SemanticOverlayPayloadV1`) and parse them at the boundary. The payload-navigation functions would then access typed fields rather than chaining `dict.get()` calls.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Finding construction is scattered across macro functions rather than encapsulated in domain objects. Macros extract data from domain objects and construct `Finding` instances externally.

**Findings:**
- `tools/cq/macros/impact.py:580-599`: `_append_depth_findings` extracts `request.function_name`, `request.param_name`, `summary.site_count`, `summary.files_affected`, `summary.depth_counts` and manually constructs `Finding` objects. The `ImpactDepthSummary` could have a `to_findings()` method.
- `tools/cq/macros/exceptions.py` (throughout): Raise and catch sites are extracted, then finding construction happens in separate formatting functions. The `RaiseSite` and `CatchSite` structs could encapsulate their own `to_finding()` method.
- `tools/cq/macros/impact.py:602-627`: `_append_kind_sections` iterates `by_kind`, extracts `site.depth`, `site.param` into a dict, then constructs `Finding`. This is a classic "ask for data, then process externally" pattern.

**Suggested improvement:**
Add `to_finding()` methods to `TaintedSite`, `RaiseSite`, `CatchSite`, and similar domain objects. This encapsulates the formatting knowledge with the data it operates on, reducing scattered conditional logic.

**Effort:** medium
**Risk if unaddressed:** medium -- finding construction logic must be kept in sync across multiple macro files.

---

### Category: Correctness

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The taint handler dispatch table (`_EXPR_TAINT_HANDLERS`) is a well-designed functional core -- 17 pure functions dispatched by type. The introspection layer is purely functional. However, `calls.py` mixes IO-heavy operations (ripgrep subprocess calls, filesystem reads, semantic overlay HTTP) with analysis logic in the same flow.

**Findings:**
- `tools/cq/macros/impact.py:396-414`: `_EXPR_TAINT_HANDLERS` is an exemplary functional dispatch table -- 17 pure functions mapping AST node types to taint-checking logic. No IO, no mutation.
- `tools/cq/introspection/cfg_builder.py:392-417`: `build_cfg` is a pure transformation: `CodeType -> CFG`. No side effects.
- `tools/cq/macros/calls.py:2188-2223`: `_build_calls_result` interleaves pure analysis (`_analyze_calls_sites`) with IO-heavy operations (`_build_calls_front_door_state` which calls `attach_target_metadata` involving disk cache, and `_apply_calls_semantic_with_telemetry` which may invoke LSP).

**Suggested improvement:**
Structure `_build_calls_result` as a pipeline where IO steps (scan, target metadata, semantic overlay) are separated from pure analysis steps (summarization, insight card construction). The IO results would be passed as data to the pure steps.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All macros are read-only analyses. Re-running with the same inputs produces the same results. The calls target cache uses content-based invalidation.

**Findings:**
- All `cmd_*` functions return `CqResult` without modifying any external state (besides cache writes, which are idempotent).
- `tools/cq/macros/calls_target.py`: The target metadata cache uses `snapshot_digest` for content-based invalidation with a 900s TTL, ensuring stale entries are rebuilt rather than accumulated.

**Suggested improvement:** None needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
Most outputs are deterministic given the same source files. The hash-based `call_id` in `CallSite` provides stable identifiers. However, ripgrep output ordering and file system traversal order could introduce non-determinism.

**Findings:**
- `tools/cq/macros/calls.py:128`: `call_id: str` is computed as a hash of `(file, line, col, callee)`, providing deterministic identity.
- `tools/cq/index/def_index.py:460-465`: `_iter_source_files` uses `root_path.glob(pattern)`, whose ordering is filesystem-dependent. This means `DefIndex.modules` iteration order may vary across runs on different filesystems.
- `tools/cq/macros/impact.py`: Taint propagation visits call sites in AST order (deterministic for a given file), but `find_callers` via ripgrep may return results in varying order.

**Suggested improvement:**
Sort `DefIndex.modules` keys after construction to ensure deterministic iteration. Sort ripgrep results by (file, line) before processing.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Simplicity

#### P19. KISS -- Alignment: 2/3

**Current state:**
The smaller macros are simple and straightforward. The introspection layer is elegantly simple. However, the context snippet extraction in `calls.py` introduces significant complexity.

**Findings:**
- `tools/cq/macros/calls.py:550-750` (approx): Context snippet extraction uses 7 helper functions (`_line_indent`, `_is_blank`, `_first_nonblank_index`, `_skip_docstring_block`, `_collect_function_header_indices`, `_collect_anchor_block_indices`, `_select_context_indices`, `_render_selected_context_lines`) for what is essentially "show the code around the match." The indentation-based heuristics for header detection and anchor block selection are fragile and complex.
- `tools/cq/introspection/cfg_builder.py`: In contrast, the CFG builder is a model of simplicity -- clear block identification, straightforward edge construction, well-named functions.

**Suggested improvement:**
Consider using AST `end_lineno` to determine function boundaries instead of indentation heuristics. The context snippet module could be simplified to "show N lines centered on the match, with the function signature prepended."

**Effort:** medium
**Risk if unaddressed:** medium -- indentation heuristics are fragile for unusual formatting.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The subsystem is generally lean. A few speculative features exist.

**Findings:**
- `tools/cq/index/def_index.py:478-513`: `load_or_build` is a method that exists alongside `build` but does exactly the same thing (calls `build` with the same defaults). The name suggests caching that was removed, but the method persists with no added value.
- `tools/cq/introspection/bytecode_index.py:190-217`: `BytecodeIndex.filter_by_stack_effect` provides min/max stack effect filtering. This may be unused -- it is a speculative API for future bytecode analysis use cases.

**Suggested improvement:**
Remove `load_or_build` -- callers should use `build` directly. Audit `filter_by_stack_effect` usage; if unused, remove it.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs mostly behave as expected. Two naming/signature surprises exist.

**Findings:**
- `tools/cq/index/def_index.py:478`: `load_or_build` -- the name implies it will attempt to load from a persistent cache and only build if the cache is cold. In reality, it always builds fresh. This is misleading.
- `tools/cq/macros/calls.py:2226-2231`: `cmd_calls(tc, root, argv, function_name)` takes 4 positional arguments, while other macros take a single typed request object (e.g., `cmd_impact(request: ImpactRequest)`, `cmd_sig_impact(request: SigImpactRequest)`). This inconsistency is surprising.
- `tools/cq/macros/shared.py:132-164`: `macro_scoring_details` returns `dict[str, object]` -- callers must use string keys and casts to access fields. This is surprising for a strongly-typed codebase.

**Suggested improvement:**
Rename `load_or_build` to `build` (or remove it per P20). Refactor `cmd_calls` to accept a `CallsRequest` struct consistent with other macros. Replace `macro_scoring_details` return type with a typed struct.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
msgspec structs with V1 suffixes (`RustFallbackPolicyV1`, `MacroRequestBase`, `MacroScorePayloadV1`, `AttachTargetMetadataRequestV1`) declare versioned contracts. However, the scoring details and semantic overlay payloads lack formal contracts.

**Findings:**
- `tools/cq/macros/shared.py:140`: `macro_scoring_details` returns `dict[str, object]` with keys "impact_score", "impact_bucket", etc. These keys are an implicit contract that could drift.
- `tools/cq/macros/calls.py:80-89`: `CallAnalysis` is a msgspec struct (good contract).
- `tools/cq/macros/calls.py:91-163`: `CallSite` is a msgspec struct with 23 fields (good contract, but large surface area).
- `tools/cq/macros/__init__.py:1-24`: Clean re-export surface declaring the public API.

**Suggested improvement:**
Replace `macro_scoring_details` with a `ScoringDetailsV1` msgspec struct. This makes the scoring contract explicit and versionable.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
The macro layer is difficult to unit test because key dependencies (`DefIndex`, ripgrep adapter, file system, semantic overlay providers) are hard-wired with no injection points. Testing requires real source files and real subprocess execution.

**Findings:**
- `tools/cq/macros/impact.py:673-677`: `_resolve_functions` calls `DefIndex.build(request.root)` -- testing the impact macro requires a real repo directory with real Python files.
- `tools/cq/macros/calls.py:48-53`: `find_call_candidates`, `find_def_lines`, `find_files_with_pattern` are imported directly from the rg adapter. Testing call site collection requires ripgrep to be installed and real files on disk.
- `tools/cq/macros/impact.py:116-206`: `TaintVisitor` is testable in isolation (given AST nodes), but `_analyze_function` at line 471 requires a `DefIndex`, a `TaintState`, and source files on disk.
- `tools/cq/introspection/bytecode_index.py:57-110`: `extract_instruction_facts` is perfectly testable -- it takes a `CodeType` and returns a list. This is the model to follow.

**Suggested improvement:**
Introduce injection points as described in P12. At minimum, allow `cmd_impact` to accept an optional `DefIndex` parameter. Make `_analyze_function` accept a source-text provider function rather than reading files directly. For `calls.py`, the decomposition in P2 would produce smaller units that are independently testable.

**Effort:** medium
**Risk if unaddressed:** high -- macro logic is only testable via integration tests, making the feedback loop slow and fragile.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The `RunContext`/`RunMeta` system provides structured run telemetry. Semantic overlay telemetry is tracked. However, the index layer has no structured logging, and errors during DefIndex construction are silently swallowed.

**Findings:**
- `tools/cq/macros/calls.py:2109-2126`: Semantic telemetry is structured as `python_semantic_telemetry` and `rust_semantic_telemetry` dicts in the result summary -- good observability practice.
- `tools/cq/index/def_index.py:467-471`: Parse errors during index construction are silently `continue`d: `except (SyntaxError, OSError, UnicodeDecodeError): continue`. There is no logging of skipped files, making it impossible to diagnose why a function was not found in the index.
- `tools/cq/introspection/symtable_extract.py:14`: `logger = logging.getLogger(__name__)` -- the introspection layer has structured logging. This is the correct pattern.
- `tools/cq/index/def_index.py`: No `logger` defined anywhere in the file.

**Suggested improvement:**
Add a module-level logger to `def_index.py` and log files skipped during index construction at `DEBUG` level. Log the total modules indexed and any files that failed to parse.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: calls.py is a monolith that blocks multiple principles

The 2,274-line `calls.py` is the root cause of violations in P2 (separation of concerns), P3 (SRP), P4 (coupling), P16 (functional core), and P19 (KISS). It absorbs responsibilities from at least 5 distinct domains. Decomposing it into a `calls/` package with focused sub-modules would simultaneously improve all five principles.

**Affected principles:** P2, P3, P4, P16, P19
**Suggested approach:** Extract to `calls/scanning.py`, `calls/analysis.py`, `calls/neighborhood.py`, `calls/semantic.py`, `calls/insight.py`, `calls/context_snippet.py`. The top-level package exports `cmd_calls` and `collect_call_sites`.

### Theme 2: DefIndex lacks both injection and abstraction

`DefIndex` is always constructed inline via `DefIndex.build(root)`. There is no protocol abstraction, no injection point, and no way to substitute a test double. This blocks P6 (Ports & Adapters), P12 (DI), and P23 (testability) simultaneously.

**Affected principles:** P6, P12, P23
**Suggested approach:** Define a `SymbolIndex` protocol. Have `DefIndex` implement it. Pass the index as a parameter to macro entry points (or via a runtime context object).

### Theme 3: String-typed categorical fields lack type safety

Confidence levels, site kinds, binding classifications, and edge types are all bare `str` throughout the system. This blocks P9 (parse, don't validate) and P10 (make illegal states unrepresentable) across 5+ data types.

**Affected principles:** P9, P10
**Suggested approach:** Define enums or `Literal` unions for all categorical fields and apply them at the point of construction.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P10, P9 | Replace bare `str` fields with `Literal` unions or enums for confidence, kind, binding, edge_type | small | Eliminates entire class of silent-wrong-string bugs |
| 2 | P7, P20, P21 | Remove `load_or_build`; extract shared `_SELF_CLS`; replace `_STDLIB_PREFIXES` with `sys.stdlib_module_names` | small | Eliminates duplication and misleading API |
| 3 | P24 | Add logger to `def_index.py`; log skipped files at DEBUG level | small | Makes "function not found" diagnoses trivial |
| 4 | P22, P21 | Replace `macro_scoring_details` return type with typed `ScoringDetailsV1` struct | small | Eliminates untyped dict contract drift |
| 5 | P8 | Add precondition checks for `max_depth >= 1` in `ImpactRequest` and `max_files >= 1` in `DefIndex.build` | small | Fails fast on invalid inputs |

## Recommended Action Sequence

1. **String-typed enums (P9, P10):** Define `ResolutionConfidence`, `TaintSiteKind`, `CallBinding`, `CFGEdgeType` enums/Literals. Apply them to `ResolvedCall`, `TaintedSite`, `CallSite`, `CFGEdge`. This is a mechanical change with high type-safety payoff. No architectural changes required.

2. **Remove dead code and fix DRY (P7, P20, P21):** Remove `DefIndex.load_or_build`. Extract `_SELF_CLS` to a shared location. Replace `_STDLIB_PREFIXES` with `sys.stdlib_module_names`. Rename `cmd_calls` to use a request struct matching other macros. Each is small and independent.

3. **Add observability to DefIndex (P24):** Add a logger and log skipped files. Minimal code change, immediate diagnostic value.

4. **Type the scoring contract (P22):** Replace `macro_scoring_details` with a `ScoringDetailsV1` msgspec struct. Small change that firms up a cross-cutting contract.

5. **Introduce SymbolIndex protocol (P6, P12, P23):** Define the protocol in the index layer. Have `DefIndex` implement it. Update `ImpactRequest` (or its context) to accept the protocol. This unlocks unit testing of taint logic without real filesystems.

6. **Decompose calls.py (P2, P3, P4, P16, P19):** This is the largest change. Convert `calls.py` into a `calls/` package with focused sub-modules. Preserve the public API (`cmd_calls`, `collect_call_sites`, `group_candidates`, `rg_find_candidates`) via `calls/__init__.py` re-exports. This should be done last because it benefits from the preceding changes (typed enums, SymbolIndex protocol).
