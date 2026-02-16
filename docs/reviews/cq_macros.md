# Design Review: tools/cq/macros/

**Date:** 2026-02-16
**Scope:** `tools/cq/macros/` (22 Python files, 7,417 LOC)
**Focus:** All principles (1-24)
**Depth:** deep (all files read)
**Files reviewed:** 22

## Executive Summary

The macros module is well-organized with a clean layered architecture: shared infrastructure (`contracts.py`, `shared.py`, `rust_fallback_policy.py`) feeds into eight independent macro commands, each following a consistent scan-analyze-emit pattern. The strongest aspects are separation of concerns within the `calls/` sub-module (clean decomposition into scanning, analysis, neighborhood, semantic, insight, and context_snippet), correct dependency direction (macros depend on core/query/search, never vice versa at module-load time), and explicit versioned contracts via `CqStruct`. The main weaknesses are: (1) heavy structural duplication in the cmd_* result-building boilerplate across macros, (2) a `calls_target.py` file that mixes caching plumbing with domain logic, and (3) the `_STDLIB_PREFIXES` set in `imports.py` being a hardcoded knowledge source that should be derived or centralized.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `calls/__init__.py` exports private `_`-prefixed symbols |
| 2 | Separation of concerns | 2 | medium | medium | `calls_target.py` mixes caching mechanics with domain resolution |
| 3 | SRP | 2 | medium | low | `calls_target.py` has 3 reasons to change: target resolution, cache protocol, result enrichment |
| 4 | High cohesion, low coupling | 2 | small | low | `calls/` sub-module well-decomposed; some tight coupling via `entry.py` orchestration imports |
| 5 | Dependency direction | 3 | - | - | Correct: macros depend on core/query/search; search uses lazy imports for two small helpers |
| 6 | Ports & Adapters | 2 | medium | low | Cache backend accessed directly in `calls_target.py` rather than via injected port |
| 7 | DRY (knowledge) | 1 | medium | medium | Result-building boilerplate duplicated 8 times; `_STDLIB_PREFIXES` hardcoded |
| 8 | Design by contract | 2 | small | low | Request structs are well-typed; but `cmd_calls` signature is loose (4 positional args, no struct) |
| 9 | Parse, don't validate | 2 | small | low | Request parsing at boundary is clean for most macros; `cmd_calls` builds its own context internally |
| 10 | Make illegal states unrepresentable | 2 | small | low | `ScoringDetailsV1` uses bare strings for `impact_bucket`/`confidence_bucket` instead of enums |
| 11 | CQS | 2 | small | low | `attach_target_metadata` both mutates result and returns data |
| 12 | DI + explicit composition | 2 | medium | low | Macros directly instantiate `DefIndex`, cache backends; no injection seams |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; pure composition throughout |
| 14 | Law of Demeter | 2 | small | low | `scheduler.policy.calls_file_workers` reaches through two objects |
| 15 | Tell, don't ask | 2 | small | low | `_calls_payload_has_signal` probes deep into dict structure rather than asking payload to classify itself |
| 16 | Functional core, imperative shell | 2 | medium | low | Analysis functions are mostly pure; but result mutation mixed into analysis in some macros |
| 17 | Idempotency | 3 | - | - | All macros are read-only analyses; cache writes are idempotent by key |
| 18 | Determinism | 3 | - | - | Same inputs produce same outputs; stable callsite IDs via sha256 |
| 19 | KISS | 2 | small | low | `calls_target.py` cache logic is overengineered for its purpose |
| 20 | YAGNI | 2 | small | low | `CallSite` struct has 22 fields; some rarely populated for simpler macros |
| 21 | Least astonishment | 2 | small | medium | `cmd_calls` takes 4 positional args while all other macros take a single request struct |
| 22 | Public contracts | 2 | small | low | `__all__` lists are present everywhere; but `calls/__init__.py` exports `_`-prefixed internals |
| 23 | Design for testability | 2 | medium | low | Pure analysis functions testable in isolation; cache and filesystem accesses not injectable |
| 24 | Observability | 2 | small | low | Scoring/telemetry attached to results; no structured logging in macro execution path |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The module's public surface is declared via `__all__` in every file, which is good. However, the `calls/__init__.py` deliberately exports private symbols with `_` prefix.

**Findings:**
- `tools/cq/macros/calls/__init__.py:12-20`: Exports `_extract_context_snippet`, `_find_function_signature`, `_rg_find_candidates`, and `_calls_payload_reason` as part of the package's public API. These are underscore-prefixed "private" symbols being re-exported, which confuses the contract boundary. The comment on line 11 says "Private exports for internal use (tests)" but they appear in `__all__`.
- `tools/cq/macros/calls_target.py:492-501`: `_TargetMetadataCacheContext` and `_TargetPayloadState` are private dataclasses, properly hidden. Good information hiding here.

**Suggested improvement:**
Create public wrappers (without underscore prefix) for the symbols that tests need, or move test-only helpers into a `_testing` submodule. Remove underscore-prefixed symbols from `__all__`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
Most macros cleanly separate scanning, analysis, and result construction. The `calls/` sub-module is a good example of concern separation (6 files, each with a clear role). However, `calls_target.py` mixes three distinct concerns.

**Findings:**
- `tools/cq/macros/calls_target.py:537-576`: `_build_target_metadata_cache_context` intermixes cache policy resolution, scope hashing, and cache key construction -- all caching infrastructure -- with the domain logic of target resolution in the same file.
- `tools/cq/macros/calls_target.py:579-651`: `_resolve_target_payload_state` contains cache read/invalidation/snapshot-comparison logic interleaved with domain resolution. The cache plumbing obscures the core concern (resolve a target definition and its callees).
- `tools/cq/macros/calls_target.py:687-737`: `attach_target_metadata` does three things: resolves targets, manages cache, and mutates the result object. These are three separate concerns in one function.

**Suggested improvement:**
Extract the cache orchestration into a dedicated helper (e.g., `_cached_target_resolution`) that wraps the pure resolution logic, following the pattern of "cache-aside" where the caller doesn't need to know about caching internals.

**Effort:** medium
**Risk if unaddressed:** medium -- cache bugs are hard to diagnose when interleaved with domain logic.

---

#### P3. SRP -- Alignment: 2/3

**Current state:**
Most macro files have a single reason to change (the analysis logic for their specific domain). The calls sub-module achieves good SRP through its 6-file decomposition.

**Findings:**
- `tools/cq/macros/calls_target.py` (748 lines): Changes for three reasons: (1) target resolution logic changes, (2) cache protocol/policy changes, (3) result section construction changes. This is the largest file in the module and could be split.
- `tools/cq/macros/calls/entry.py` (514 lines): Changes for two reasons: (1) orchestration flow changes, (2) summary/telemetry schema changes. The `_attach_calls_semantic_summary` function at line 391 builds specific telemetry dicts that are a separate concern from orchestration.

**Suggested improvement:**
Split `calls_target.py` into: (1) `calls_target_resolution.py` (pure resolution: `resolve_target_definition`, `scan_target_callees`, `infer_target_language`), (2) `calls_target_cache.py` (cache orchestration), (3) keep `add_target_callees_section` in `calls_target.py` as the thin coordinator.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The `calls/` sub-module demonstrates good internal cohesion. Each of the 6 files has a clear focused responsibility. Cross-module coupling is reasonable -- macros depend on `core`, `query`, `search`, and `index`, which is the expected dependency direction.

**Findings:**
- `tools/cq/macros/calls/entry.py:26-60`: Imports from 11 different modules, including 5 sibling files and 6 external modules. This is the expected orchestration fan-in for an entry point, but the import list is dense.
- `tools/cq/macros/calls/analysis.py:21-24`: Imports from both `context_snippet` and `neighborhood` siblings, creating bidirectional awareness within the `calls/` package. However, these are narrow, specific imports.

**Suggested improvement:**
No immediate action needed. The coupling is well-managed through narrow interfaces. If `entry.py` grows further, consider a dedicated `orchestration.py` to hold the wiring separate from the public `cmd_calls` entry point.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
Dependency direction is correct throughout. Macros depend on `core/`, `query/`, `search/`, and `index/` -- the lower-level infrastructure. The reverse dependency (search importing from macros) is limited to two lazy imports in `search/pipeline/context_window.py:87,105` for context window computation and snippet extraction. These are deferred imports inside functions, so they don't create load-time circular dependencies.

**Findings:**
- The dependency graph is: `cli_app/commands` -> `macros` -> `{core, query, search, index}`. This is the correct direction.
- `tools/cq/search/pipeline/context_window.py:87`: Uses a lazy import `from tools.cq.macros.calls import compute_calls_context_window`. While this is a reverse dependency, it is a leaf function and the lazy import prevents circular loading.

**Suggested improvement:**
No action needed. The current approach is pragmatic. If the lazy imports multiply, consider extracting the shared context_window logic into a shared utility that both macros and search can import.

**Effort:** -
**Risk if unaddressed:** -

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The macros do not define explicit ports for their external needs (cache, filesystem, search). They directly call into concrete implementations.

**Findings:**
- `tools/cq/macros/calls_target.py:13-28`: Imports 14 symbols from the cache subsystem directly. The cache backend is obtained via `get_cq_cache_backend()` global factory rather than injection.
- `tools/cq/macros/impact.py:842`: `DefIndex.build(request.root)` directly constructs the index. No way to substitute a test index without monkeypatching.
- `tools/cq/macros/shared.py:15-19`: Direct imports of file index builders and ripgrep adapter. These are effectively infrastructure adapters used without explicit port boundaries.

**Suggested improvement:**
For the highest-value improvement, make the cache backend injectable on `AttachTargetMetadataRequestV1` (optional field, default to `get_cq_cache_backend()`). This would make `calls_target.py` testable without touching the real cache.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
There is significant structural duplication across the eight macro implementations. Each macro follows the same pattern: start timer -> scan -> build RunContext -> mk_result -> set summary -> compute scoring -> append key_findings -> append sections -> append evidence -> apply rust fallback. This pattern is repeated 8 times without a shared template.

**Findings:**
- Result-building boilerplate: The sequence `RunContext.from_parts(...).to_runmeta(name)` + `mk_result(run)` + `result.summary = {...}` appears in `impact.py:790-797`, `sig_impact.py:328-335`, `imports.py:529-536`, `scopes.py:266-273`, `exceptions.py:486-492`, `side_effects.py:359-366`, `bytecode.py:356-362`, and `calls/entry.py:250-258`. Each copy is slightly different in field names but structurally identical.
- `tools/cq/macros/imports.py:31-77`: `_STDLIB_PREFIXES` is a hardcoded 37-element set of stdlib module names. This knowledge is not derived from `sys.stdlib_module_names` (available in Python 3.10+) and will drift as Python evolves.
- `tools/cq/macros/side_effects.py:33-70`: `SAFE_TOP_LEVEL` and `AMBIENT_PATTERNS` are hardcoded knowledge sets with no single authoritative source. If a new safe decorator is added to Python, this set must be manually updated.
- Scoring pattern: `macro_scoring_details(sites=..., files=..., evidence_kind=...)` followed by `build_detail_payload(scoring=scoring_details)` appears in every macro with the same shape.

**Suggested improvement:**
1. Replace `_STDLIB_PREFIXES` with `sys.stdlib_module_names` (Python 3.10+), falling back to the current set for compatibility.
2. Extract a `MacroResultBuilder` helper that encapsulates the common `RunContext -> mk_result -> summary -> scoring -> rust_fallback` flow. Each macro would provide a domain-specific scan function and a summary builder, while the builder handles the repetitive plumbing.

**Effort:** medium (for the result builder); small (for stdlib derivation)
**Risk if unaddressed:** medium -- the boilerplate drift risk is real; when the result format changes, all 8 macros must be updated in sync.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Request structs use typed, frozen `CqStruct` with explicit fields and defaults. This is a good implicit contract. However, one macro breaks the pattern.

**Findings:**
- `tools/cq/macros/calls/entry.py:462-467`: `cmd_calls(tc, root, argv, function_name)` takes 4 loose positional parameters instead of a request struct. Every other macro takes a single request object (`cmd_impact(request: ImpactRequest)`, `cmd_scopes(request: ScopeRequest)`, etc.). This inconsistency means `cmd_calls` has no explicit contract envelope.
- `tools/cq/macros/contracts.py:48-55`: `MacroScorePayloadV1` uses `float` for impact/confidence scores with no documented range. The `impact_bucket` and `confidence_bucket` fields are bare `str` rather than a constrained type.

**Suggested improvement:**
Create a `CallsRequest(MacroRequestBase)` struct and refactor `cmd_calls` to accept it, aligning with the other 7 macros. This would also simplify the `request_factory.py` code that constructs these requests.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The macro request structs serve as parsed, validated inputs. Once constructed, the rest of the code operates on well-formed values. The `shared.py` file scanning functions produce `list[Path]` results that downstream code can rely on.

**Findings:**
- `tools/cq/macros/calls/entry.py:487-492`: `cmd_calls` receives raw `Path` and `str` arguments and internally constructs `CallsContext`. The parsing happens inside the function rather than at the boundary.
- `tools/cq/macros/sig_impact.py:69-149`: `_parse_signature` is a good example of parse-don't-validate -- it converts a raw string into a `list[SigParam]` once, and all downstream code operates on the structured representation.

**Suggested improvement:**
Adopt a request struct for `cmd_calls` (see P8 suggestion), which would move parsing to the boundary.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Data models generally prevent impossible states through frozen structs and typed fields. However, some string-typed fields could be more constrained.

**Findings:**
- `tools/cq/macros/contracts.py:53-54`: `impact_bucket` and `confidence_bucket` are `str` fields that in practice only take values `"low"`, `"medium"`, `"high"`. A `Literal["low", "medium", "high"]` type would make invalid buckets unrepresentable.
- `tools/cq/macros/calls/analysis.py:26`: `CallBinding = Literal["ok", "ambiguous", "would_break", "unresolved"]` is a good example of making illegal states unrepresentable.
- `tools/cq/macros/impact.py:18`: `TaintSiteKind = Literal["source", "call", "return", "assign"]` is another good example.

**Suggested improvement:**
Add `BucketLevel = Literal["low", "medium", "high"]` to `contracts.py` and use it for `impact_bucket` and `confidence_bucket`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions are either queries (returning data) or commands (mutating state). However, a few functions violate CQS.

**Findings:**
- `tools/cq/macros/calls_target.py:687-737`: `attach_target_metadata` both mutates `result` (adds summary entries and sections) AND returns a 3-tuple of `(target_location, target_callees, resolved_language)`. This forces callers to handle both the side effect and the return value.
- `tools/cq/macros/calls/entry.py:262-294`: `_analyze_calls_sites` mutates `result` (appends key_findings and sections) and returns `(analysis, score)`. The mutation and return are both meaningful.
- `tools/cq/macros/multilang_fallback.py:42-76`: `apply_rust_macro_fallback` mutates `result.evidence`, `result.key_findings`, and `result.summary`, then returns the same `result`. The return value is redundant since it's the same object.

**Suggested improvement:**
For `attach_target_metadata`, split into: (1) a pure query `resolve_target_metadata(...)` that returns the 3-tuple plus the section data, (2) a command `apply_target_metadata(result, metadata)` that mutates the result. For `apply_rust_macro_fallback`, either return `None` (pure command) or return a new result (pure query via `msgspec.structs.replace`).

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
Dependencies are generally obtained via module-level imports or global factories rather than injection. This is pragmatic for a CLI tool but limits testability.

**Findings:**
- `tools/cq/macros/impact.py:842`: `DefIndex.build(request.root)` is a direct construction with no injection seam.
- `tools/cq/macros/calls_target.py:545`: `get_cq_cache_backend(root=resolved_root)` is a global factory call embedded in domain logic.
- `tools/cq/macros/calls/entry.py:107`: `infer_target_language(root_path, function_name)` is a global function call that could be a pluggable strategy.

**Suggested improvement:**
For the highest-value change, add an optional `cache_backend` parameter to `AttachTargetMetadataRequestV1` that defaults to `None` (meaning "use global default"). This would enable test injection without changing production callers.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in the macros module. All behavior is composed through function calls, data structs, and module-level organization. The `MacroRequestBase` / `ScopedMacroRequestBase` hierarchy is minimal (one level) and uses struct composition semantics via `CqStruct`.

**Findings:**
- `tools/cq/macros/contracts.py:16-28`: `MacroRequestBase` -> `ScopedMacroRequestBase` is the only inheritance, and it's a single-level data extension adding `include`/`exclude` fields. This is the correct use of inheritance for data extension.

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code communicates with direct collaborators. A few chains reach deeper than ideal.

**Findings:**
- `tools/cq/macros/calls_target.py:123`: `scheduler.policy.calls_file_workers` reaches through the scheduler into its policy object. The caller should ask the scheduler for a worker count directly.
- `tools/cq/macros/calls/analysis.py:445`: `scheduler.policy.calls_file_workers` -- same pattern repeated.
- `tools/cq/macros/calls/semantic.py:92-94`: `semantic_payload.get("semantic_planes")` followed by `isinstance` checks -- deep dict probing through an untyped payload. This is partly inherent to working with dynamic payloads.

**Suggested improvement:**
Add a `max_file_workers` property to the scheduler that delegates to `self.policy.calls_file_workers`, reducing the chain to one step.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The macros generally follow tell-don't-ask for their request/result flow. However, some functions interrogate data structures to make decisions.

**Findings:**
- `tools/cq/macros/calls/semantic.py:116-138`: `_calls_payload_has_signal` deeply probes a `dict[str, object]` payload, checking for nested dicts, lists of dicts, string values, etc. This is "ask" behavior -- the code reaches deep into an opaque structure to decide if it has signal.
- `tools/cq/macros/calls/insight.py:137-139`: `any(site.symtable_info and site.symtable_info.get("is_closure") for site in all_sites)` asks each site about its internals rather than having the site classify itself.

**Suggested improvement:**
For `_calls_payload_has_signal`, the semantic payload producer should include a `has_signal: bool` field in its output, eliminating the need for heuristic probing. For call sites, add a `has_closure_capture` property to `CallSite`.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The analysis functions (taint tracking, signature parsing, scope extraction, exception scanning) are largely pure or side-effect-free. IO (file reading, cache access) is concentrated in scanning functions. However, result mutation is spread across both "core" and "shell" layers.

**Findings:**
- `tools/cq/macros/impact.py:474-530`: `_analyze_function` is a pure recursive analysis function that only mutates its `TaintState` parameter (which is owned by the caller). Good functional core design.
- `tools/cq/macros/calls/entry.py:205-241`: `_append_calls_findings` mutates `result` directly. This is the "imperative shell" reaching into what should be data assembly. The analysis (`_summarize_sites`) is pure, but the result construction is not.
- `tools/cq/macros/exceptions.py:319-351`, `imports.py:339-374`, `side_effects.py:278-318`: The `_append_*_section` functions all mutate `result` by appending to `result.sections`. This is consistent across macros and is a reasonable shell pattern, but it makes testing individual section construction harder.

**Suggested improvement:**
Consider having `_append_*` functions return `Section` objects instead of mutating `result`, and have the top-level `cmd_*` function assemble the final result from returned sections. This would make section construction pure and testable.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All macros are read-only analyses that produce `CqResult` objects. Cache writes in `calls_target.py` use content-addressed keys, making re-execution with the same inputs produce the same cache entries.

**Findings:**
- `tools/cq/macros/calls_target.py:654-684`: Cache writes use `cache_key` derived from function name, language, and scope hash. Re-running produces the same key and value, satisfying idempotency.

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
All analyses are deterministic given the same filesystem state. Call site IDs use sha256-based stable hashing. No random or time-dependent logic affects analysis results.

**Findings:**
- `tools/cq/macros/calls/analysis.py:347-356`: `_stable_callsite_id` uses `sha256(f"{file}:{line}:{col}:{callee}:{context}")`. This produces stable, reproducible IDs.
- Timing via `ms()` is only used for metadata, not for analysis decisions.

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most macros are straightforward scan-analyze-emit pipelines. The `calls/` sub-module is more complex due to its front-door insight assembly, semantic enrichment, and neighborhood integration.

**Findings:**
- `tools/cq/macros/calls_target.py:579-651`: `_resolve_target_payload_state` has 4 return paths, each constructing a `_TargetPayloadState` with slightly different `should_write_cache` values. The cache invalidation logic (snapshot comparison, null snapshot handling) adds complexity that could be simplified.
- `tools/cq/macros/calls/entry.py:417-459`: `_build_calls_result` orchestrates 8 sequential operations with intermediate state threading. While each operation is simple, the orchestration is dense.
- `tools/cq/macros/impact.py:397-417`: The `_EXPR_TAINT_HANDLERS` dispatch table is a clean alternative to a long if-elif chain. Good simplicity pattern.

**Suggested improvement:**
Simplify `_resolve_target_payload_state` by separating the cache-hit validation into a dedicated `_validate_cached_payload` function that returns either the validated payload or `None`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The module is generally lean. However, the `CallSite` struct carries fields that are not always populated.

**Findings:**
- `tools/cq/macros/calls/analysis.py:49-121`: `CallSite` has 22 fields. When used by `sig_impact.py`, only `file`, `line`, `num_args`, `kwargs`, `has_star_args`, `has_star_kwargs`, and `arg_preview` are needed. The remaining 15 fields are unnecessary baggage for that use case.
- `tools/cq/macros/calls_target.py:480-488`: `AttachTargetMetadataRequestV1` has a `run_id` field specifically for cache tagging. This couples the request envelope to the caching strategy.

**Suggested improvement:**
No immediate action -- the overhead is small and splitting `CallSite` would introduce a second type. However, if performance becomes a concern, consider a `CallSiteCore` subset for the `sig_impact` use case.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
The macro APIs are generally consistent and predictable. The main surprise is the `cmd_calls` signature.

**Findings:**
- `tools/cq/macros/calls/entry.py:462-467`: `cmd_calls(tc, root, argv, function_name)` is the only macro taking 4 positional arguments. All other macros take a single request struct. A user familiar with `cmd_impact(request)` would expect `cmd_calls(request)` as well.
- `tools/cq/macros/multilang_fallback.py:42-76`: `apply_rust_macro_fallback` returns `CqResult` but also mutates it in-place. The return value suggests a new object, but it's the same object. This is confusing -- callers might assume they need to use the return value while the mutation already happened.

**Suggested improvement:**
1. Align `cmd_calls` to take a request struct (see P8).
2. In `apply_rust_macro_fallback`, either return `None` (making mutation explicit) or create and return a new result object.

**Effort:** small
**Risk if unaddressed:** medium -- the `cmd_calls` inconsistency will trip up contributors.

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
Every file has an `__all__` list. Contract types use `CqStruct` with `frozen=True` and versioned names (`V1` suffix). This is good practice.

**Findings:**
- `tools/cq/macros/calls/__init__.py:12-20`: Exports `_extract_context_snippet`, `_find_function_signature`, `_rg_find_candidates`, and `_calls_payload_reason` with leading underscores, signaling "private" while making them part of the package API. This sends mixed signals about contract stability.
- `tools/cq/macros/contracts.py:31-37`: `MacroExecutionRequestV1` has a docstring saying "Compatibility envelope" -- this suggests it exists for backward compatibility but there's no deprecation notice or migration guide.

**Suggested improvement:**
Remove underscore prefixes from symbols exported in `__all__`, or don't export them in `__all__` and have tests import them directly from their source modules.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The pure analysis functions (taint tracking, signature parsing, scope extraction) are directly testable. However, the integration points (cache, filesystem, search adapters) are not injectable.

**Findings:**
- `tools/cq/macros/impact.py:474-530`: `_analyze_function` is testable with a constructed `_AnalyzeContext` and `FnDecl` -- good.
- `tools/cq/macros/calls_target.py:545`: `get_cq_cache_backend(root=resolved_root)` is a global factory. Testing cache behavior requires monkeypatching.
- `tools/cq/macros/impact.py:842`: `DefIndex.build(request.root)` performs filesystem scanning. Testing `cmd_impact` end-to-end requires real files.
- `tools/cq/macros/calls/analysis.py:290-344`: `_enrich_call_site` calls `analyze_symtable` and `analyze_bytecode` -- these are testable via import because they accept source strings, not file paths. Good testability.

**Suggested improvement:**
Make `DefIndex` injectable on `ImpactRequest` (optional field with `None` default meaning "build from filesystem"). This would allow unit tests to provide a pre-built index without filesystem access.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Each macro attaches comprehensive scoring and telemetry to its `CqResult`. The `calls` macro includes semantic telemetry, scan method tracking, and degradation notes. However, there is no structured logging during macro execution.

**Findings:**
- `tools/cq/macros/calls/entry.py:391-414`: `_attach_calls_semantic_summary` builds detailed telemetry with `python_semantic_telemetry` and `rust_semantic_telemetry` counters. Good structured observability in the output.
- `tools/cq/macros/calls/neighborhood.py:175-176`: `degradation_notes.append(f"tree_sitter_neighborhood_unavailable:{type(exc).__name__}")` captures failure modes as structured data. Good pattern.
- No structured logging (via `logging` module or similar) exists in any macro. All observability is embedded in the result payload. If a macro hangs or crashes partway through, there's no trace of progress.

**Suggested improvement:**
Add a lightweight structured logger to the macro execution path that records: macro start, scan completion, analysis completion, and result construction. This would help diagnose performance issues and partial failures.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Structural boilerplate duplication across macros

**Root cause:** Each macro independently implements the same result-construction pattern (timer start -> scan -> RunContext -> mk_result -> summary -> scoring -> sections -> evidence -> rust_fallback) without a shared template.

**Affected principles:** P7 (DRY), P21 (least astonishment -- slight variations between macros), P23 (testability -- testing the common pattern requires testing each macro).

**Suggested approach:** Introduce a `MacroExecutionTemplate` that accepts pluggable scan/analyze/section-build callbacks. Each macro provides its domain-specific functions; the template handles the common flow. This is a composition pattern (P13 aligned) that would reduce the 8-way duplication.

### Theme 2: `calls_target.py` as a mixed-concern module

**Root cause:** The target resolution feature evolved to include caching, and the cache logic was added in-place rather than being extracted.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P6 (ports & adapters), P19 (KISS).

**Suggested approach:** Split into `calls_target_resolution.py` (pure domain logic) and `calls_target_cache.py` (cache orchestration wrapping the pure resolution). The entry point (`attach_target_metadata`) would coordinate them.

### Theme 3: `cmd_calls` API inconsistency

**Root cause:** `cmd_calls` predates the request-struct pattern adopted by the other macros, and was not refactored when the pattern was established.

**Affected principles:** P8 (design by contract), P9 (parse don't validate), P21 (least astonishment).

**Suggested approach:** Introduce a `CallsRequest(MacroRequestBase)` struct and refactor `cmd_calls` to accept it. Update `cli_app/commands/analysis.py`, `core/services.py`, `core/bundles.py`, and `run/step_executors.py` callers.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 | Replace `_STDLIB_PREFIXES` with `sys.stdlib_module_names` in `imports.py:31` | small | Eliminates knowledge drift for stdlib detection |
| 2 | P21, P8 | Create `CallsRequest` struct and align `cmd_calls` signature with other macros | small | Eliminates the most visible API inconsistency |
| 3 | P22, P1 | Remove `_`-prefixed symbols from `calls/__init__.py` `__all__`; provide public wrappers or direct test imports | small | Cleans up the public contract boundary |
| 4 | P10 | Add `BucketLevel = Literal["low", "medium", "high"]` to `contracts.py` | small | Prevents invalid bucket values at type level |
| 5 | P14 | Add `max_file_workers` property to worker scheduler to avoid `scheduler.policy.calls_file_workers` chain | small | Reduces Demeter violations in two files |

## Recommended Action Sequence

1. **P7: Replace `_STDLIB_PREFIXES`** with `sys.stdlib_module_names` (Quick win, no dependencies, immediate correctness improvement).

2. **P21/P8: Introduce `CallsRequest` struct** for `cmd_calls`. This touches `calls/entry.py`, `cli_app/commands/analysis.py`, `core/services.py`, `core/bundles.py`, and `run/step_executors.py` but is a straightforward mechanical refactor.

3. **P22/P1: Clean up `calls/__init__.py` exports**. Remove `_`-prefixed symbols from `__all__`. Have tests import directly from source modules.

4. **P10: Add `BucketLevel` type** to `contracts.py` and use it in `ScoringDetailsV1` and `MacroScorePayloadV1`.

5. **P2/P3: Split `calls_target.py`** into resolution and cache modules. This is the highest-effort but highest-structural-impact improvement.

6. **P7: Extract `MacroResultBuilder`** to reduce boilerplate across the 8 macros. This should be done after item 2, since the `CallsRequest` alignment simplifies the template design.

7. **P11: Split `attach_target_metadata`** into a pure query and a mutation command. Do this as part of item 5.

8. **P24: Add structured logging** to macro execution paths. Low effort, improves operational visibility.
