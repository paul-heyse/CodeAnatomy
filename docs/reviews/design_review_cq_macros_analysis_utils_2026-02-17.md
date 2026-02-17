# Design Review: tools/cq/macros, tools/cq/analysis, tools/cq/introspection, tools/cq/utils

**Date:** 2026-02-17
**Scope:** `tools/cq/macros/`, `tools/cq/analysis/`, `tools/cq/introspection/`, `tools/cq/utils/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 37 files, ~9,667 LOC

## Executive Summary

This subsystem demonstrates strong layering between analysis (pure logic) and macros (orchestration), good use of frozen msgspec contracts for request/response types, and a well-designed `MacroResultBuilder` pattern. The primary structural issues are: (1) eight unfrozen mutable msgspec structs in macros that function as data-transfer objects and should be frozen, (2) pervasive `dict[str, object]` payloads in macro summaries and enrichment where typed contracts exist or should be created, (3) `calls/analysis.py` at 771 LOC with mixed concerns spanning Python AST analysis, Rust record processing, enrichment, and parallel I/O dispatch, and (4) inconsistent result-assembly patterns where some macros bypass `MacroResultBuilder` and directly manipulate `CqResult` tuples. Dependency direction is clean: `analysis/` and `introspection/` never import from `macros/`, and `utils/` has no upward dependencies.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Internal `_analyze_sites` tuple return leaks implementation shape |
| 2 | Separation of concerns | 1 | medium | medium | `calls/analysis.py` mixes AST analysis, Rust records, enrichment I/O |
| 3 | SRP | 1 | medium | medium | `calls/analysis.py` (771 LOC) has 5+ change reasons |
| 4 | High cohesion, low coupling | 2 | small | low | Macro modules are cohesive; `calls/` subpackage has tight internal coupling |
| 5 | Dependency direction | 3 | - | - | Clean: analysis/introspection never import from macros |
| 6 | Ports & Adapters | 2 | medium | low | File I/O scattered through macros; no filesystem port |
| 7 | DRY (knowledge) | 1 | medium | medium | Scoring-dict conversion repeated in 6+ macros; file scanning pattern duplicated |
| 8 | Design by contract | 2 | small | low | Request structs well-typed; return types sometimes bare tuples |
| 9 | Parse, don't validate | 2 | small | low | `parse_signature` is good; some raw dict payloads bypass typed parsing |
| 10 | Illegal states | 2 | small | low | 8 unfrozen structs allow mutation after construction |
| 11 | CQS | 2 | small | low | `_analyze_function` mutates state via context; `TaintState` mixes query/command |
| 12 | DI + explicit composition | 2 | small | low | `MacroResultBuilder` injected; `DefIndex.build` hides filesystem coupling |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; composition throughout |
| 14 | Law of Demeter | 2 | small | low | `result.summary.semantic_planes` chain in `calls/semantic.py:94-97` |
| 15 | Tell, don't ask | 2 | small | low | `CallSite` has 22 fields exposing raw data for external logic |
| 16 | Functional core, imperative shell | 2 | medium | medium | `analysis/taint.py` is pure; `impact.py:149` reads files inline |
| 17 | Idempotency | 3 | - | - | All macros produce deterministic results from same inputs |
| 18 | Determinism | 3 | - | - | `_stable_callsite_id` uses SHA256; UUID7 for run IDs |
| 19 | KISS | 2 | small | low | `calls/insight.py` deferred imports make flow hard to follow |
| 20 | YAGNI | 2 | small | low | `uuid8_or_uuid7`, `legacy_compatible_event_id` appear unused |
| 21 | Least astonishment | 2 | small | low | `_append_*` functions sometimes return `Section|None`, sometimes `CqResult` |
| 22 | Public contracts | 2 | small | low | `__all__` present everywhere; some private `_` functions exported via `__init__` |
| 23 | Testability | 2 | medium | medium | `analysis/` is testable; macros couple to filesystem via `DefIndex.build` |
| 24 | Observability | 2 | small | low | `calls/entry.py` has logging; other macros silent |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Macro request structs (`ImpactRequest`, `SigImpactRequest`, etc.) provide clean public interfaces. However, internal analysis returns leak implementation details through bare tuples.

**Findings:**
- `tools/cq/macros/calls/analysis.py:719-765`: `_analyze_sites` returns a 5-element tuple `(Counter, Counter, int, Counter, Counter)` that callers must destructure positionally. This is exposed to `entry.py` at line 205 and `insight.py` constructors.
- `tools/cq/macros/calls/analysis.py:291-345`: `_enrich_call_site` returns `dict[str, dict[str, object] | None]` with magic string keys `"symtable"` and `"bytecode"`, when a typed struct would be safer.

**Suggested improvement:**
Replace the 5-tuple return from `_analyze_sites` with a named struct (the `CallAnalysisSummary` dataclass already exists in `neighborhood.py:41-48` and is used to receive this data). Replace the `dict[str, dict[str, object] | None]` from `_enrich_call_site` with a frozen dataclass like `CallSiteEnrichment(symtable: SymtableInfo | None, bytecode: BytecodeInfo | None)`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`analysis/taint.py` is purely computational with no I/O -- well separated. However, several macro files embed domain logic that should live in lower layers.

**Findings:**
- `tools/cq/macros/impact.py:128-194`: `_analyze_function` contains the core taint propagation algorithm (recursion, callsite resolution, argument binding) interleaved with filesystem reads at line 149 (`filepath.read_text`). The pure analysis logic (`analyze_function_node` from `analysis/taint.py`) is cleanly separated, but the orchestration loop that reads files and resolves call targets is embedded in the macro layer.
- `tools/cq/macros/calls/analysis.py:291-345`: `_enrich_call_site` performs both symtable extraction and bytecode compilation (lines 320-343), mixing analysis with I/O (compiling source at line 332). These are introspection operations that belong in `introspection/`.
- `tools/cq/macros/side_effects.py:33-70`: `AMBIENT_PATTERNS` and `SAFE_TOP_LEVEL` are domain knowledge constants embedded in the macro module. They represent policy decisions about what constitutes a side effect. These should live in `analysis/` as they are reusable classification knowledge.
- `tools/cq/macros/exceptions.py:106-256`: `ExceptionVisitor` is a pure AST visitor performing domain analysis but is defined in the macro module rather than `analysis/`.

**Suggested improvement:**
Extract `ExceptionVisitor` and `SideEffectVisitor` into `analysis/exceptions.py` and `analysis/side_effects.py` respectively, keeping only the orchestration (file scanning, result building) in the macro modules. Move `AMBIENT_PATTERNS` and `SAFE_TOP_LEVEL` into the analysis module alongside the visitor.

**Effort:** medium
**Risk if unaddressed:** medium -- makes it harder to test analysis logic independently and creates a precedent for embedding analysis in orchestration code.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
Most macro files have a single command entry point, which is good. However, `calls/analysis.py` is a god module.

**Findings:**
- `tools/cq/macros/calls/analysis.py` (771 LOC): Changes for at least 5 reasons: (1) Python AST call matching logic (lines 222-243, 360-405), (2) Rust call site processing (lines 569-664), (3) call enrichment with symtable/bytecode (lines 291-345), (4) parallel I/O dispatch for file collection (lines 421-462), (5) argument preview formatting (lines 156-219). This should be at least 3 files.
- `tools/cq/macros/calls/entry.py` (536 LOC): Orchestration + front-door insight assembly + semantic telemetry attachment. The front-door and semantic concerns could be extracted.
- `tools/cq/macros/calls_target.py` (576 LOC): Mixes target resolution (Python and Rust), callee scanning, parallel dispatch, and result attachment in one module.

**Suggested improvement:**
Split `calls/analysis.py` into: (1) `calls/matching.py` (target matching, call analysis), (2) `calls/rust_calls.py` (Rust record processing), (3) move enrichment to `introspection/` or a dedicated `calls/enrichment.py`. Split `calls_target.py` into `calls_target_resolve.py` (resolution) and `calls_target_attach.py` (result mutation).

**Effort:** medium
**Risk if unaddressed:** medium -- changes to Rust call processing risk breaking Python analysis and vice versa.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Module cohesion is generally good. Each macro file handles one analysis domain. The `calls/` subpackage demonstrates good decomposition (entry, scanning, analysis, insight, neighborhood, semantic, context_snippet).

**Findings:**
- `tools/cq/macros/calls/entry.py:29-33`: Imports private symbols (`_analyze_sites`, `_collect_call_sites_from_records`) from `analysis.py`, breaking the private/public boundary. These are used at lines 152 and 205.
- `tools/cq/macros/calls/entry.py:53-54`: Imports private symbols (`_rg_find_candidates`, `_group_candidates`) from `scanning.py` alongside their public wrappers that exist at lines 20-31 and 67-73.

**Suggested improvement:**
Use the public wrappers (`rg_find_candidates`, `group_candidates`, `collect_call_sites`) instead of their private `_` counterparts in `entry.py`. Add public wrappers for `_analyze_sites` and `_collect_call_sites_from_records` if they need cross-module access.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
Dependency direction is clean and well-maintained.

**Findings:**
- `analysis/` has zero imports from `macros/`, `introspection/`, or `utils/`. It depends only on `tools.cq.core`.
- `introspection/` has zero imports from `macros/`, `analysis/`, or `utils/`. It depends only on stdlib.
- `utils/` has zero imports from `macros/`, `analysis/`, or `introspection/`. It depends only on `tools.cq.core.structs` and stdlib.
- `macros/` correctly imports downward into `analysis/`, `introspection/` (via `query/enrichment`), and `utils/`.

**Effort:** --
**Risk if unaddressed:** --

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
File I/O is scattered through macro implementations rather than concentrated at boundaries.

**Findings:**
- `tools/cq/macros/impact.py:149`: `filepath.read_text(encoding="utf-8")` embedded in analysis loop.
- `tools/cq/macros/bytecode.py:175`: `pyfile.read_text(encoding="utf-8")` in `_collect_surfaces`.
- `tools/cq/macros/scopes.py:168`: `pyfile.read_text(encoding="utf-8")` in `_collect_scopes`.
- `tools/cq/macros/exceptions.py:285`: `pyfile.read_text(encoding="utf-8")` in `_scan_exceptions`.
- `tools/cq/macros/side_effects.py:259`: `pyfile.read_text(encoding="utf-8")` in `_scan_side_effects`.
- `tools/cq/macros/imports.py:251`: `pyfile.read_text(encoding="utf-8")` in `_collect_imports`.

All six follow the identical pattern: iterate files, read text, parse, visit, collect results. This is a repeated adapter pattern that could be extracted into a shared `scan_python_files` higher-order function.

**Suggested improvement:**
Extract a shared `scan_python_files(root, include, exclude, visitor_factory) -> list[T]` helper in `macros/shared.py` that handles file iteration, reading, parsing, and error handling. Each macro provides only the visitor/analyzer function.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
Several pieces of knowledge are duplicated across macro modules.

**Findings:**
- **Scoring-dict conversion pattern**: `msgspec.structs.asdict(scoring_details)` is called at 6 locations across `impact.py` (lines 247, 265, 282, 306, 326) to convert a `ScoringDetailsV1` into a dict for `build_detail_payload`. This conversion should happen once, or `build_detail_payload` should accept `ScoringDetailsV1` directly.
- **File scanning boilerplate**: The pattern `iter_files -> read_text -> ast.parse -> Visitor -> extend results` appears identically in `exceptions.py:277-293`, `side_effects.py:238-268`, `imports.py:234-266`, `scopes.py:159-173`, and `bytecode.py:166-184`. Five copies of the same orchestration with different visitors.
- **Rust fallback application**: Every macro command ends with `apply_rust_fallback_policy(builder.build(), ...)` -- 9 call sites in 9 macros. The `scopes.py:333` variant uses `builder.apply_rust_fallback(...).build()` instead, an inconsistency that embeds the same knowledge two ways.
- **Summary construction**: `summary_from_mapping({...})` with ad-hoc `dict[str, object]` is repeated in every macro. The key names (`"files_scanned"`, `"scope_file_count"`, `"scope_filter_applied"`) are informal contracts duplicated across files.

**Suggested improvement:**
(1) Add a `scoring` parameter directly to `build_detail_payload` that accepts `ScoringDetailsV1` (eliminating `asdict` calls). (2) Extract the file-scan-visit pattern into `shared.py`. (3) Standardize on `builder.apply_rust_fallback(...).build()` pattern (already in `scopes.py`) rather than the two-step pattern.

**Effort:** medium
**Risk if unaddressed:** medium -- divergence of the scoring-dict pattern across macros makes it easy to introduce inconsistencies.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Request types are well-defined frozen structs with clear fields. Return types are less rigorous.

**Findings:**
- `tools/cq/macros/impact.py:439`: `_build_impact_summary` returns `dict[str, object]` -- an untyped payload for what is a structured summary. Same pattern in `calls/entry.py:187`, `imports.py:467`, `exceptions.py:492`.
- `tools/cq/macros/calls/analysis.py:719-765`: `_analyze_sites` return type is `tuple[Counter[str], Counter[str], int, Counter[str], Counter[str]]` -- no names, positionally fragile.

**Suggested improvement:**
Define a `MacroSummaryPayload` or per-macro summary struct (frozen) for each macro's summary return, replacing the `dict[str, object]` intermediate.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`analysis/signature.py` demonstrates excellent parse-don't-validate: it parses a signature string into a `list[SigParam]` once at the boundary (`sig_impact.py:153`), and all downstream logic operates on the parsed `SigParam` list.

**Findings:**
- `tools/cq/macros/expand.py:46`: `handle: dict[str, object]` is passed through without parsing into a typed representation. The `_identity_expand` function at line 22-26 just passes the dict through, missing an opportunity to validate the payload shape.
- `tools/cq/macros/calls/semantic.py:119-141`: `_calls_payload_has_signal` operates on `dict[str, object]` using `.get()` chains with `isinstance` checks (lines 123-141), repeatedly validating rather than parsing once into a typed `SemanticPayload`.

**Suggested improvement:**
Define a `SemanticPayloadV1` msgspec struct to replace the `dict[str, object]` payload in `calls/semantic.py`, parsing it once via `msgspec.convert` at the boundary.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Request types are frozen and well-constrained. Eight data-transfer structs are unfrozen, allowing post-construction mutation.

**Findings:**
- `tools/cq/macros/impact.py:46`: `TaintState(msgspec.Struct)` -- mutable by design (accumulator pattern). This is intentional.
- `tools/cq/macros/bytecode.py:44`: `BytecodeSurface(msgspec.Struct)` -- unfrozen but never mutated after construction in `_analyze_code_object`. Should be frozen.
- `tools/cq/macros/exceptions.py:37,67`: `RaiseSite` and `CatchSite` -- unfrozen but fields are set at construction and never mutated. Should be frozen.
- `tools/cq/macros/side_effects.py:73`: `SideEffect` -- unfrozen, never mutated after construction. Should be frozen.
- `tools/cq/macros/imports.py:42,75`: `ImportInfo` and `ModuleDeps` -- `ModuleDeps.depends_on` is mutated at `imports.py:261-263` via `.add()`. `ImportInfo` is never mutated.
- `tools/cq/macros/scopes.py:30`: `ScopeInfo` -- unfrozen, never mutated after construction.

**Suggested improvement:**
Freeze `BytecodeSurface`, `RaiseSite`, `CatchSite`, `SideEffect`, `ImportInfo`, and `ScopeInfo` (6 structs). For `ModuleDeps`, build `depends_on` as a set during construction rather than mutating it post-construction -- pass it as a parameter to the constructor instead of mutating via `.add()`. Keep `TaintState` mutable as it is an intentional accumulator.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. The `MacroResultBuilder` cleanly separates state mutation (`.add_finding`, `.add_section`) from the final query (`.build()`).

**Findings:**
- `tools/cq/macros/impact.py:128-194`: `_analyze_function` both mutates `context.state` (via `record_visit`, `add_sites`) and performs recursive analysis. The mutation is essential to prevent cycles, but the function does two things: reads state to check visits and writes state to record results.
- `tools/cq/macros/calls/semantic.py:94-97`: `existing_planes.clear(); existing_planes.update(dict(semantic_planes))` mutates the result summary's `semantic_planes` dict in-place. This is a CQS violation -- a function that is enriching a result is mutating a nested dict on the input.
- `tools/cq/macros/result_builder.py:183-197`: `apply_rust_fallback` both queries the current result (`.result`) and replaces all internal state. This is by design for the builder pattern but blurs the query/command line.

**Suggested improvement:**
In `calls/semantic.py:94-97`, replace the in-place mutation with creating a new summary dict via `msgspec.structs.replace`, consistent with how the rest of the codebase handles immutable result enrichment.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`MacroResultBuilder` is constructed explicitly and injected with configuration. Good use of DI in the builder pattern.

**Findings:**
- `tools/cq/macros/impact.py:515`: `DefIndex.build(request.root)` hides filesystem scanning behind a class method. This makes it harder to test `cmd_impact` without a real filesystem.
- `tools/cq/macros/calls/entry.py:121`: `get_worker_scheduler()` is a module-level singleton accessor. The worker scheduler is a shared global that cannot be injected for testing.

**Suggested improvement:**
Accept an optional `index: DefIndex | None` parameter in `cmd_impact` (and similar macros) to allow pre-built indexes for testing. The production path would default to `DefIndex.build(root)`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in the reviewed scope. All behavior is composed via functions and structs.

**Findings:**
- `MacroRequestBase` and `ScopedMacroRequestBase` at `contracts.py:18-30` use msgspec struct composition (nested frozen inheritance) rather than class hierarchies. `ScopedMacroRequestBase` extends `MacroRequestBase` with scope filters -- this is shallow, one-level composition.
- AST visitors (`ExceptionVisitor`, `SideEffectVisitor`, `ImportVisitor`, `TaintVisitor`, `CallFinder`) inherit from `ast.NodeVisitor` which is the standard library's visitor pattern. This is idiomatic, not problematic.

**Effort:** --
**Risk if unaddressed:** --

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. A few violations exist.

**Findings:**
- `tools/cq/macros/calls/semantic.py:94-97`: `result.summary.semantic_planes` -- reaching through `result` into `summary` into `semantic_planes`, then mutating it with `.clear()` and `.update()`. Three levels of traversal plus mutation.
- `tools/cq/macros/calls/insight.py:395`: `getattr(summary, telemetry_key)` -- dynamic attribute access on a summary object using a computed key, bypassing the type system.
- `tools/cq/macros/calls/entry.py:403`: `Path(result.run.root)` -- reaching through the result's run metadata to get the root path.

**Suggested improvement:**
For `semantic.py:94-97`, pass the semantic planes dict directly as a parameter rather than reaching through the result. For `insight.py:395`, add explicit accessors on `SummaryEnvelopeV1` for Python and Rust telemetry.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`CallSite` is a data carrier with 22 fields that callers interrogate extensively.

**Findings:**
- `tools/cq/macros/calls/analysis.py:50-122`: `CallSite` has 22 public fields. Callers in `insight.py:266-288` read 16 of them to build a details dict. The struct exposes raw data without encapsulating any behavior.
- `tools/cq/macros/calls/analysis.py:719-765`: `_analyze_sites` asks each `CallSite` for `.num_args`, `.num_kwargs`, `.has_star_args`, `.has_star_kwargs`, `.kwargs`, `.context`, `.hazards` to build aggregate counters. The site itself could provide a `shape_key` property.

**Suggested improvement:**
Add a `to_details_dict()` method on `CallSite` and a `shape_key` property to encapsulate the interrogation logic. This moves the knowledge of how to summarize a call site into the struct that owns the data.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
`analysis/taint.py` is an excellent functional core -- pure AST analysis with no I/O. `analysis/signature.py` and `analysis/calls.py` are similarly pure. The imperative shell (macros) handles I/O and orchestration. However, some I/O leaks into analysis-adjacent code.

**Findings:**
- `tools/cq/macros/impact.py:144-149`: File reading (`filepath.read_text`) is embedded in the taint propagation loop, mixing I/O with the recursive analysis algorithm.
- `tools/cq/macros/calls/analysis.py:311-343`: `_enrich_call_site` performs `compile(source, ...)` and `analyze_symtable` -- compilation is a CPU-bound transform but `analyze_symtable` and `analyze_bytecode` are imported from `query/enrichment.py` (deferred import at line 312), blurring the boundary.

**Suggested improvement:**
Separate the file-reading phase from the analysis phase in `impact.py`. Pre-read all needed source files into a `dict[str, str]` and pass sources to the analysis loop, making the loop itself pure.

**Effort:** medium
**Risk if unaddressed:** medium -- the current design prevents testing the taint propagation algorithm without filesystem access.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All macro commands are idempotent: same inputs produce same outputs. No state is persisted between runs (the cache in `calls_target_cache.py` is invalidated by snapshot digests).

**Findings:**
- `tools/cq/macros/calls_target_cache.py:176-242`: `resolve_target_payload_state` properly invalidates cache on snapshot mismatch (line 222-229), maintaining idempotency.

**Effort:** --
**Risk if unaddressed:** --

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is well-maintained.

**Findings:**
- `tools/cq/macros/calls/analysis.py:348-357`: `_stable_callsite_id` uses SHA256 over `file:line:col:callee:context`, producing deterministic identifiers.
- `tools/cq/utils/uuid_factory.py:33-46`: UUID7 generation is thread-safe via `_UUID_LOCK`, producing time-ordered but intentionally non-deterministic identifiers for run IDs (which is correct).

**Effort:** --
**Risk if unaddressed:** --

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforward. Some complexity is justified by the problem domain. A few areas introduce unnecessary complexity.

**Findings:**
- `tools/cq/macros/calls/insight.py:301-372`: `_build_calls_front_door_insight` has 7 deferred imports from 4 different modules inside the function body. This makes the function hard to understand at a glance and hides its true dependency set.
- `tools/cq/macros/calls/entry.py:440-484`: `_build_calls_result` chains 10+ function calls to build the result, with complex state threading through `CallsFrontDoorState`, `CallsInsightSummary`, `insight`, `semantic_telemetry`, and `semantic_reasons`. The flow would benefit from an explicit pipeline or state machine.
- `tools/cq/introspection/cfg_builder.py:138-172`: `to_mermaid` renders both jump and fallthrough edges with `" --> "` (lines 165-169), making all edges look identical except exception edges. The `style` variable is computed but never differentiated for jump vs fallthrough.

**Suggested improvement:**
For `insight.py:301-372`, move the deferred imports to module level (in `TYPE_CHECKING` block if needed for cycles, or directly if no cycle). For `cfg_builder.py`, differentiate jump vs fallthrough edge rendering.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most code serves clear use cases. A few utilities appear speculative.

**Findings:**
- `tools/cq/utils/uuid_factory.py:154-165`: `uuid8_or_uuid7()` -- UUIDv8 is not referenced anywhere in the reviewed scope. It appears to be speculative future-proofing.
- `tools/cq/utils/uuid_factory.py:134-151`: `legacy_compatible_event_id` with `node` and `clock_seq` parameters -- these parameters are never used at call sites (always `None`).
- `tools/cq/utils/uuid_temporal_contracts.py:82-94`: `gated_uuid8` -- another v8 variant, duplicating `uuid8_or_uuid7` with a gate flag.
- `tools/cq/introspection/bytecode_index.py:195-222`: `filter_by_stack_effect` -- not referenced by any file in the reviewed scope.

**Suggested improvement:**
No action needed now -- these are small and do not increase maintenance burden significantly. If a future review consolidates UUID utilities, `uuid8_or_uuid7` and `gated_uuid8` could be merged into a single gated function.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs are generally predictable. Some naming inconsistencies create minor surprise.

**Findings:**
- `tools/cq/macros/imports.py:298-378`: `_append_cycle_section`, `_append_external_section`, `_append_relative_section` all take and return `CqResult`, modifying it functionally. But `_append_kind_sections` in `side_effects.py:278` returns `list[Section]` instead. The naming prefix `_append_` suggests mutation but the behavior varies.
- `tools/cq/macros/scopes.py:272`: `builder.set_summary(...)` vs `builder.with_summary(summary_from_mapping(...))` used in other macros. Two different APIs for the same operation.
- `tools/cq/macros/result_builder.py:81-108`: `set_summary(**kwargs)` vs `with_summary(summary)` vs `set_summary_field(key, value)` -- three ways to set summary. While each serves a purpose, the API surface is wider than necessary.

**Suggested improvement:**
Standardize on `with_summary(summary_from_mapping({...}))` pattern across all macros. Deprecate `set_summary(**kwargs)` to reduce API surface.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
Every module has `__all__` exports. Request/response types are versioned (`V1` suffix).

**Findings:**
- `tools/cq/macros/calls/entry.py:30-33`: Imports private symbols `_analyze_sites`, `_collect_call_sites_from_records` from `analysis.py`. These are implementation details that leak across module boundaries.
- `tools/cq/macros/calls/entry.py:36-47`: Imports `_add_context_section`, `_add_hazard_section`, `_add_kw_section`, `_add_shape_section`, `_add_sites_section`, `_build_call_scoring`, `_build_calls_confidence`, `_build_calls_front_door_insight`, `_finalize_calls_semantic_state`, `_find_function_signature` -- 10 private functions from `insight.py`. These should be public if they are part of the module's contract.

**Suggested improvement:**
Either make these functions public (remove `_` prefix) and add to `__all__`, or restructure so that `entry.py` uses higher-level public APIs from `insight.py` and `analysis.py`.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
`analysis/` is highly testable -- pure functions with clear inputs/outputs. `introspection/` is testable via code objects. `utils/` is testable. Macros are harder to test due to filesystem coupling.

**Findings:**
- `tools/cq/macros/impact.py:515`: `cmd_impact` immediately calls `DefIndex.build(request.root)`, coupling to filesystem. Testing requires a real directory structure.
- `tools/cq/macros/calls/entry.py:106-181`: `_scan_call_sites` directly calls `find_files_with_pattern`, `sg_scan`, `infer_target_language` -- all filesystem-bound. No injection point for test doubles.
- `tools/cq/analysis/taint.py:52-163`: `TaintVisitor` is testable by constructing AST nodes directly -- excellent testability.
- `tools/cq/analysis/calls.py:37-98`: `classify_call_against_signature` operates on the `CallSiteLike` protocol -- excellent testability via protocol.

**Suggested improvement:**
For `cmd_impact`, accept an optional `index` parameter. For `_scan_call_sites`, extract the scanning strategy into a protocol or callback that can be replaced in tests.

**Effort:** medium
**Risk if unaddressed:** medium -- without injection points, macro-level tests require real filesystem fixtures.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`calls/entry.py` has structured logging via `logger.debug` and `logger.warning`. Other macros are silent.

**Findings:**
- `tools/cq/macros/calls/entry.py:78,507,510,526-530`: Logger with structured debug/warning messages for scan start, fallback, and completion. Good.
- `tools/cq/macros/impact.py`: No logging. Taint analysis failures silently return early (lines 146, 152).
- `tools/cq/macros/bytecode.py:177`: SyntaxError/OSError during compilation silently caught with `continue`.
- `tools/cq/macros/exceptions.py:287`: Same silent error handling.
- `tools/cq/introspection/symtable_extract.py:14,151-155`: Has `logger` with `.debug` and `.warning` for symtable failures. Good.

**Suggested improvement:**
Add `logger = logging.getLogger(__name__)` and structured debug logging to `impact.py`, `bytecode.py`, `scopes.py`, `exceptions.py`, `side_effects.py`, and `imports.py`. Log file count, error count, and elapsed time at minimum.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Unfrozen data-transfer structs

**Root cause:** Early development used `msgspec.Struct` without `frozen=True` as default. As the codebase matured, request types adopted `frozen=True` but data-transfer types were not retroactively frozen.

**Affected principles:** P10 (illegal states), P11 (CQS)

**Files:** `bytecode.py:44`, `exceptions.py:37,67`, `side_effects.py:73`, `imports.py:42,75`, `scopes.py:30`

**Approach:** Add `frozen=True` to 6 structs. For `ModuleDeps`, refactor the construction at `imports.py:257-264` to build `depends_on` as a frozenset passed to the constructor.

### Theme 2: dict[str, object] payload anti-pattern

**Root cause:** Summary payloads and enrichment results use untyped dicts because the `summary_from_mapping` and `build_detail_payload` APIs accept `dict[str, object]`. This propagates throughout all macros.

**Affected principles:** P8 (contracts), P9 (parse don't validate), P14 (Demeter), P15 (tell don't ask)

**Files:** `impact.py:439`, `calls/entry.py:187`, `imports.py:467-468`, `calls/analysis.py:291-345`, `calls/semantic.py:121,155,174`, `expand.py:19-46`

**Approach:** Define typed summary structs per macro. Extend `build_detail_payload` to accept `ScoringDetailsV1` directly. Define a `SemanticPayloadV1` for semantic enrichment results.

### Theme 3: Domain analysis embedded in macro layer

**Root cause:** Macros were initially small scripts that grew. AST visitors that perform domain analysis were added to the macro module that uses them rather than to the shared `analysis/` package.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P16 (functional core), P23 (testability)

**Files:** `exceptions.py:106-256` (`ExceptionVisitor`), `side_effects.py:128-229` (`SideEffectVisitor`), `imports.py:152-196` (`ImportVisitor`)

**Approach:** Extract visitors to `analysis/` alongside `taint.py`, `signature.py`, and `calls.py`. The macro modules retain only orchestration (file scanning, result building).

### Theme 4: File-scan-visit boilerplate duplication

**Root cause:** Each macro independently implements the pattern: resolve files -> iterate -> read text -> parse AST -> create visitor -> visit -> collect results -> handle errors. This pattern appears 6 times with minor variations.

**Affected principles:** P7 (DRY)

**Files:** `bytecode.py:166-184`, `scopes.py:159-173`, `exceptions.py:259-294`, `side_effects.py:238-268`, `imports.py:234-266`, `impact.py:144-149`

**Approach:** Create a generic `scan_python_files` helper that accepts a visitor factory and returns collected results.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P10 | Freeze 6 unfrozen data-transfer structs | small | Prevents accidental mutation, improves hashability |
| 2 | P7 | Accept `ScoringDetailsV1` directly in `build_detail_payload` | small | Eliminates 6+ `asdict()` conversion sites |
| 3 | P4/P22 | Replace private `_` imports in `calls/entry.py` with public wrappers | small | Fixes 12 cross-module private symbol imports |
| 4 | P24 | Add structured logging to 5 silent macros | small | Makes failures visible in all macros |
| 5 | P21 | Standardize on `builder.apply_rust_fallback(...).build()` pattern | small | Eliminates inconsistency between `scopes.py` and all other macros |

## Recommended Action Sequence

1. **Freeze data-transfer structs (P10):** Change 6 struct declarations to `frozen=True`. Refactor `ModuleDeps` construction in `imports.py:257-264` to pass `depends_on` as a constructor argument. Minimal blast radius, high confidence.

2. **Extend `build_detail_payload` to accept `ScoringDetailsV1` (P7):** Add an overload or parameter to `core/scoring.py:build_detail_payload` that accepts `ScoringDetailsV1` directly, eliminating repeated `msgspec.structs.asdict` calls across 6 macros.

3. **Fix private symbol imports in `calls/entry.py` (P4, P22):** Either expose `_analyze_sites`, `_collect_call_sites_from_records` etc. as public APIs with `__all__` entries, or restructure the call graph to use existing public wrappers.

4. **Add logging to silent macros (P24):** Add `logger = logging.getLogger(__name__)` and debug-level logging for scan start, file count, error count, and elapsed time to `impact.py`, `bytecode.py`, `scopes.py`, `exceptions.py`, `side_effects.py`, `imports.py`.

5. **Extract file-scan-visit helper (P7, P6):** Create `scan_python_files(root, include, exclude, visitor_factory)` in `macros/shared.py` to deduplicate the file iteration pattern across 6 macros.

6. **Extract domain visitors to `analysis/` (P2, P3, P16):** Move `ExceptionVisitor`, `SideEffectVisitor`, and `ImportVisitor` to `analysis/exceptions.py`, `analysis/side_effects.py`, and `analysis/imports.py`. Keep orchestration in macros.

7. **Split `calls/analysis.py` (P3):** Extract Rust call site processing (lines 569-664) to `calls/rust_calls.py`. Move `_enrich_call_site` (lines 291-345) to `calls/enrichment.py` or `introspection/`. Reduces the 771 LOC module to ~450 LOC with clear single responsibilities.

8. **Fix CQS violation in `calls/semantic.py` (P11, P14):** Replace in-place `semantic_planes` mutation at lines 94-97 with `msgspec.structs.replace` to produce a new summary.

9. **Type summary payloads (P8, P9):** Define per-macro summary structs (e.g., `ImpactSummaryV1`, `CallsSummaryV1`) to replace the `dict[str, object]` intermediates. This is larger but provides compile-time safety for summary field names.
