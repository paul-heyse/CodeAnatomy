# Design Review: tools/cq/macros + tools/cq/neighborhood

**Date:** 2026-02-16
**Scope:** `tools/cq/macros/` (24 files, ~12,600 LOC) + `tools/cq/neighborhood/` (9 files, ~2,032 LOC)
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 33

## Executive Summary

The macros and neighborhood modules demonstrate strong compositional design with well-defined request/result contracts, a consistent Rust fallback policy, and clean separation between scanning, analysis, and result construction phases. The primary systemic weaknesses are: (1) duplicated constants and knowledge across the `calls/` subpackage (four files independently define `_FRONT_DOOR_PREVIEW_PER_SLICE = 5`), (2) inconsistent use of struct systems (mixed `dataclass`, `msgspec.Struct`, and `CqStruct` for semantically equivalent roles), and (3) result-mutating functions that violate CQS by both populating `CqResult` state and returning computed values. The neighborhood module is notably cleaner, with well-separated collection, layout, and rendering concerns.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Private helpers exported in `__init__.py` via underscore-prefixed names in `calls/entry.py` imports |
| 2 | Separation of concerns | 2 | medium | medium | `calls/entry.py` mixes orchestration, result construction, and telemetry in one flow |
| 3 | SRP | 2 | medium | medium | `calls_target.py` handles resolution, scanning, caching orchestration, and result mutation |
| 4 | High cohesion, low coupling | 2 | small | low | `calls/` subpackage is highly cohesive; cross-macro coupling is appropriately low |
| 5 | Dependency direction | 2 | small | low | Macros depend on core schemas and search infrastructure; direction is correct |
| 6 | Ports & Adapters | 2 | medium | low | Rust fallback policy is a good adapter pattern; direct file IO scattered in analysis |
| 7 | DRY (knowledge) | 1 | small | medium | `_FRONT_DOOR_PREVIEW_PER_SLICE = 5` defined in 4 files; scoring boilerplate repeated |
| 8 | Design by contract | 2 | small | low | Request structs enforce preconditions; postconditions are implicit |
| 9 | Parse, don't validate | 2 | small | low | Target resolution parses once at boundary; some re-parsing in `_parse_call_expr` |
| 10 | Make illegal states unrepresentable | 2 | medium | medium | `CallSite` is mutable `msgspec.Struct` with many optional fields that can be inconsistent |
| 11 | CQS | 1 | medium | medium | `_populate_summary`, `_populate_findings` mutate result AND are called for their side effects |
| 12 | DI + explicit composition | 2 | small | low | `MacroResultBuilder` centralizes construction; some hidden creation via deferred imports |
| 13 | Composition over inheritance | 3 | - | - | Single-level inheritance only (`ScopedMacroRequestBase` extends `MacroRequestBase`) |
| 14 | Law of Demeter | 2 | small | low | `result.run.run_id`, `result.summary["key"]` chains are acceptable for data containers |
| 15 | Tell, don't ask | 1 | medium | medium | External functions interrogate `CqResult` internals to populate sections |
| 16 | Functional core, imperative shell | 2 | medium | low | Analysis functions are largely pure; result assembly is imperative mutation |
| 17 | Idempotency | 3 | - | - | Macro commands are read-only analysis; cache writes are idempotent by design |
| 18 | Determinism | 2 | small | low | Bundle IDs are deterministic; `_merge_slices` sorts output; `Counter.most_common` is stable |
| 19 | KISS | 2 | small | low | Generally straightforward; `calls/entry.py` orchestration has accidental complexity |
| 20 | YAGNI | 2 | small | low | `MacroExecutionRequestV1` and `MacroTargetResolutionV1` appear unused within scope |
| 21 | Least astonishment | 2 | small | low | Public/private API boundary is unclear: `_analyze_sites` exported but underscore-prefixed |
| 22 | Declare public contracts | 2 | small | low | `__all__` is consistently declared; version suffixes (V1) on contracts |
| 23 | Design for testability | 2 | medium | medium | Pure analysis functions are testable; result mutation functions require full `CqResult` setup |
| 24 | Observability | 2 | small | low | Logger in `calls/entry.py`; most macros lack structured logging |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Modules generally hide internal decisions behind well-named functions. The `__all__` export lists are consistently maintained.

**Findings:**
- `tools/cq/macros/calls/entry.py:26-49` imports underscore-prefixed private functions (`_analyze_sites`, `_collect_call_sites_from_records`, `_add_context_section`, `_build_calls_confidence`, etc.) from sibling modules. These are implementation details exposed across module boundaries within the `calls/` package.
- `tools/cq/macros/calls/__init__.py:9-13` re-exports `cmd_calls`, `CallSite`, `collect_call_sites`, etc. but the internal structure (which sub-module owns what) leaks through import paths.

**Suggested improvement:**
Convert cross-module private function usage within `calls/` to explicit public interfaces. For example, `analysis.py` should export `analyze_sites()` (without underscore) if `entry.py` needs it. This makes the internal contract explicit rather than relying on convention violations.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
Most macros follow a clean pattern: collect data, analyze, build result. The `calls` macro is the exception.

**Findings:**
- `tools/cq/macros/calls/entry.py:420-462` (`_build_calls_result`) chains together scanning, analysis, front-door state construction, semantic enrichment, telemetry attachment, and insight finalization in a single linear flow. This function is the "orchestrator" but it mixes policy decisions (when to apply semantic enrichment) with plumbing (attaching telemetry counters to summary dicts).
- `tools/cq/macros/calls/entry.py:394-417` (`_attach_calls_semantic_summary`) manually constructs Python and Rust telemetry dicts with duplicated conditional logic (`if target_language == "python" else 0`).

**Suggested improvement:**
Extract a `CallsTelemetryBuilder` or equivalent that encapsulates telemetry dict construction, separating "what telemetry to collect" from "how to attach it to a result." This would also consolidate the duplicated Python/Rust conditional patterns.

**Effort:** medium
**Risk if unaddressed:** medium -- telemetry logic is fragile and easy to get wrong when adding new languages.

---

#### P3. SRP -- Alignment: 2/3

**Current state:**
Most macros have a single responsibility. `calls_target.py` is the exception.

**Findings:**
- `tools/cq/macros/calls_target.py` (529 LOC) handles four distinct responsibilities: (a) target definition resolution (lines 36-188), (b) callee scanning (lines 254-416), (c) result section construction (lines 419-444), and (d) cache-aware metadata attachment orchestration (lines 447-519). Changes to cache strategy, target resolution heuristics, or callee scanning would all modify this file.
- `tools/cq/macros/calls_target_cache.py` correctly separates cache orchestration, but `calls_target.py` still orchestrates the cache context lifecycle alongside resolution logic.

**Suggested improvement:**
Extract target definition resolution (`resolve_target_definition`, `_resolve_python_target_definition`, `_resolve_rust_target_definition`) into a dedicated `calls_target_resolution.py`. Keep `calls_target.py` as the composition point that wires resolution + scanning + caching.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Macros correctly depend inward on core schemas (`CqResult`, `Finding`, `Section`) and search infrastructure. No reverse dependencies detected.

**Findings:**
- `tools/cq/macros/calls/entry.py:61` uses a deferred import `from tools.cq.core.front_door_schema import FrontDoorInsightV1` under `TYPE_CHECKING`. This is the correct pattern, but `calls/insight.py:307-317` performs heavy runtime imports from `tools.cq.core.front_door_builders` inside `_build_calls_front_door_insight`. These deferred imports add latency on every call.

**Suggested improvement:**
Move `front_door_builders` imports to module level under `TYPE_CHECKING` for type annotations and keep runtime imports only for actual construction calls. Alternatively, consolidate the deferred imports into a single lazy-import helper.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The `RustFallbackPolicyV1` pattern is an excellent adapter-like design -- macros declare what they need and `apply_rust_fallback_policy` handles the implementation.

**Findings:**
- Every macro independently calls `apply_rust_fallback_policy` at the end of its `cmd_*` function with a `RustFallbackPolicyV1` constant. This is clean but the Rust search itself (`_rust_fallback.py:39-43`) uses `suppress(OSError, TimeoutError, RuntimeError, ValueError)` to silently swallow all search errors.

**Suggested improvement:**
The broad `suppress` in `_rust_fallback.py:39` is acceptable for graceful degradation but should emit a degrade diagnostic (similar to the neighborhood collector's `DegradeEventV1` pattern) so callers know the fallback search failed silently.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
The most significant DRY violation is the constant `_FRONT_DOOR_PREVIEW_PER_SLICE = 5` defined independently in four files.

**Findings:**
- `tools/cq/macros/calls/entry.py:70`, `tools/cq/macros/calls/insight.py:34`, `tools/cq/macros/calls/neighborhood.py:22`, `tools/cq/macros/calls/semantic.py:19` all define `_FRONT_DOOR_PREVIEW_PER_SLICE = 5`. If this value needs to change, four files must be updated in sync.
- The scoring boilerplate pattern (`macro_scoring_details(...) -> ScoringDetailsV1 -> msgspec.structs.asdict(scoring_details) -> build_detail_payload(scoring=scoring_dict)`) is repeated in every macro with minor variations. Six macros follow identical `scoring_dict = msgspec.structs.asdict(scoring_details)` + `build_detail_payload(scoring=scoring_dict)` chains.
- `tools/cq/macros/calls/entry.py:69` and `tools/cq/macros/calls_target.py:29` both define `_CALLS_TARGET_CALLEE_PREVIEW = 10`.

**Suggested improvement:**
(1) Define `_FRONT_DOOR_PREVIEW_PER_SLICE` once in `tools/cq/macros/contracts.py` or a new `tools/cq/macros/constants.py` and import it. (2) Consider adding a `build_detail_payload_from_scoring(details: ScoringDetailsV1, data=None)` helper that eliminates the repeated `asdict` + `build_detail_payload` two-step.

**Effort:** small
**Risk if unaddressed:** medium -- constant drift causes subtle behavioral inconsistencies.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Request types are well-structured with frozen `msgspec.Struct` or `CqStruct`. Domain models are less constrained.

**Findings:**
- `tools/cq/macros/calls/analysis.py:49-121`: `CallSite` is a **mutable** `msgspec.Struct` (no `frozen=True`). It has 22 fields, many with default values that can represent inconsistent states. For example, `resolution_confidence="unresolved"` paired with `binding="ok"` is semantically invalid but structurally allowed. The `context_window` field is `dict[str, int] | None` rather than a typed struct.
- `tools/cq/macros/impact.py:72-87`: `TaintState` uses mutable `set` and `list` fields, which is appropriate for its accumulator role but means it cannot be shared safely across threads.

**Suggested improvement:**
Make `CallSite` frozen (it is already replaced via `msgspec.structs.replace` at `analysis.py:504`). Replace `context_window: dict[str, int] | None` with a typed `ContextWindow(start_line: int, end_line: int)` struct to prevent key-name typos.

**Effort:** medium
**Risk if unaddressed:** medium -- mutable struct fields invite accidental mutation bugs.

---

#### P11. CQS -- Alignment: 1/3

**Current state:**
Many functions both mutate the `CqResult` object and return computed values or are called purely for side effects.

**Findings:**
- `tools/cq/neighborhood/snb_renderer.py:55-78` (`_populate_summary`): Takes `result` by reference, mutates `result.summary` dict in place, returns `None`. This is a command, which is fine in isolation.
- `tools/cq/macros/calls/entry.py:265-297` (`_analyze_calls_sites`): Mutates `result.key_findings` AND returns `(CallAnalysisSummary, ScoreDetails | None)`. The caller depends on both the mutation and the return value, making the function hard to reason about.
- `tools/cq/macros/calls/entry.py:300-350` (`_build_calls_front_door_state`): Mutates `result` via `_attach_calls_neighborhood_section(result, ...)` inside its body AND returns a `CallsFrontDoorState`. Side effects are hidden in the middle of what looks like a builder function.
- `tools/cq/macros/calls_target.py:458-519` (`attach_target_metadata`): Mutates `result.summary` and appends sections, AND returns a 3-tuple. Callers must track both the mutations and the return values.

**Suggested improvement:**
Separate computation from result mutation. For example, `_analyze_calls_sites` should return `(CallAnalysisSummary, ScoreDetails, list[Finding])` and let the caller append findings. This makes the data flow explicit and the function testable without constructing a full `CqResult`.

**Effort:** medium
**Risk if unaddressed:** medium -- hidden mutations make refactoring and testing unreliable.

---

### Category: Composition (12-15)

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
Inheritance is minimal and shallow. `ScopedMacroRequestBase` extends `MacroRequestBase` to add include/exclude filters. All behavior is built through composition of request structs, analysis functions, and result builders.

No action needed.

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
The `CqResult` object is treated as a mutable data bag that external functions interrogate and populate.

**Findings:**
- Throughout all macros, the pattern is: create empty `CqResult`, pass it to multiple `_append_*` functions that each interrogate existing state and add to `result.sections`, `result.key_findings`, `result.evidence`, and `result.summary`. This is "ask then tell" -- functions ask what is already in the result to decide what to add.
- `tools/cq/macros/calls/entry.py:261`: `result.summary = _build_calls_summary(...)` directly replaces the summary dict.
- `tools/cq/macros/multilang_fallback.py:52-53`: `existing_summary = dict(result.summary) if isinstance(result.summary, dict) else {}` -- the code must defensively check the type of `result.summary` because it is `dict[str, object]` with no schema.

**Suggested improvement:**
The `CqResult` schema is a shared contract and changing it is out of scope. However, macro-specific builders could construct complete section lists and finding lists internally, then assign them to the result at the end. This concentrates the "ask" into a single final assembly step.

**Effort:** medium
**Risk if unaddressed:** medium

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Analysis functions (`_analyze_call`, `_detect_hazards`, `_classify_call`, `_collect_scopes`) are effectively pure. The imperative shell is the `cmd_*` entry points that read files and mutate results.

**Findings:**
- `tools/cq/macros/calls/analysis.py:290-344` (`_enrich_call_site`): Reads files, compiles source code, and calls `analyze_symtable`/`analyze_bytecode` -- IO mixed into what looks like an analysis function.
- `tools/cq/neighborhood/bundle_builder.py:188-233` (`_store_artifacts_with_preview`): Performs filesystem writes (creating directories, writing JSON files) inside what is otherwise a pure assembly pipeline.

**Suggested improvement:**
For `_store_artifacts_with_preview`, return the artifact content as data and let the caller decide whether to persist. The pure computation (content generation, digest computation) can be separated from the IO (file write).

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

Macro commands are read-only analyses that produce deterministic results for the same inputs. Cache writes use content-addressed keys with snapshot fingerprinting (`calls_target_cache.py:61-83`), making re-runs safe.

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Individual macros are straightforward. The `calls` macro has accumulated significant complexity.

**Findings:**
- `tools/cq/macros/calls/entry.py` is 515 LOC with 14 imports from sibling modules, 7 from core, and 5 from other CQ packages. The `_build_calls_result` function (lines 420-462) chains 8 distinct phases. This is the most complex orchestration in the scope.
- `tools/cq/macros/impact.py:396-416`: The `_EXPR_TAINT_HANDLERS` dispatch table uses 16 `cast("TaintHandler", ...)` calls. This is a clean dispatch pattern but the cast noise obscures readability.

**Suggested improvement:**
For `_EXPR_TAINT_HANDLERS`, define the handler type as `Callable[[TaintVisitor, Any], bool]` (since handlers accept specific `ast.*` subtypes) and remove the casts. Alternatively, use `type: ignore[dict-item]` on the dict literal.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most code serves a clear purpose.

**Findings:**
- `tools/cq/macros/contracts.py:39-53`: `MacroExecutionRequestV1` and `MacroTargetResolutionV1` appear to be unused within the macros scope. No file in the reviewed scope constructs or consumes these types.
- `tools/cq/neighborhood/contracts.py:65-92`: `plan_feasible_slices` is exported from `__init__.py` and `bundle_builder.py` but never called in the neighborhood assembly pipeline. It appears to be speculative infrastructure for future capability gating.

**Suggested improvement:**
Verify whether `MacroExecutionRequestV1`, `MacroTargetResolutionV1`, and `plan_feasible_slices` are used elsewhere in the codebase. If not, remove them to reduce surface area.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs are generally predictable.

**Findings:**
- `tools/cq/macros/calls/scanning.py` exports both `rg_find_candidates` (public) and `_rg_find_candidates` (private, with additional `lang_scope` parameter). The public wrapper simply delegates. `calls/entry.py:49` imports the private `_rg_find_candidates` directly, bypassing the public API. This is confusing -- which is the "real" API?
- `tools/cq/macros/calls/analysis.py:407-417`: `collect_call_sites` is a public function that simply delegates to `_collect_call_sites`. The wrapper adds no value.

**Suggested improvement:**
Remove the public/private duality. Make `rg_find_candidates` the single function with an optional `lang_scope` parameter. Remove the trivial `collect_call_sites` wrapper and export `_collect_call_sites` as `collect_call_sites` directly.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure analysis functions (hazard detection, signature parsing, taint visitors) are highly testable. Result assembly functions are harder to test.

**Findings:**
- `tools/cq/macros/calls/entry.py:265-297`: `_analyze_calls_sites` requires constructing a full `CqResult` object to test because it mutates the result as a side effect. The interesting return value (analysis summary) could be tested independently if separated.
- `tools/cq/macros/impact.py:473-529`: `_analyze_function` takes an `_AnalyzeContext` with a `DefIndex` that requires building from the filesystem. Testing requires either real files or monkeypatching the index.
- `tools/cq/neighborhood/tree_sitter_collector.py:596-663`: `collect_tree_sitter_neighborhood` performs filesystem reads. The parsing step could accept `bytes` directly instead of resolving files internally, enabling testing without real files.

**Suggested improvement:**
Add an overload or internal variant of `collect_tree_sitter_neighborhood` that accepts pre-loaded `source_bytes` and a `language` string, allowing unit tests to bypass filesystem access.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`calls/entry.py` has a module-level logger with debug/warning messages. Other macros have no logging.

**Findings:**
- `tools/cq/macros/calls/entry.py:71,485,488,504`: Logger used for debug start/completion and warning on fallback. Good pattern.
- `tools/cq/macros/impact.py`, `sig_impact.py`, `imports.py`, `scopes.py`, `bytecode.py`, `side_effects.py`, `exceptions.py`: None of these macros have any logging. Failures in file parsing are silently swallowed via bare `continue` in except blocks.
- `tools/cq/neighborhood/tree_sitter_collector.py:545-546`: Parse errors are captured into `DegradeEventV1` diagnostics, which is excellent structured observability. This pattern is not used in macros.

**Suggested improvement:**
Add a module-level logger to each macro with `debug` messages for start/completion and `warning` messages for fallback paths. The neighborhood module's `DegradeEventV1` pattern should be adopted by macros for structured error reporting.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Mixed struct systems create cognitive overhead

Three struct systems coexist: `dataclass(frozen=True)`, `msgspec.Struct`, and `CqStruct` (which extends `msgspec.Struct`). The choice between them appears inconsistent:
- Cross-module contracts use `CqStruct` (correct per CLAUDE.md)
- Domain models use `msgspec.Struct` (reasonable for hot paths)
- Internal context objects use `@dataclass(frozen=True)` (13 occurrences)

The `@dataclass` usage introduces a third system with no clear benefit over `msgspec.Struct(frozen=True)`. This forces developers to remember which system each type uses when calling `msgspec.structs.replace` (works only on msgspec types) vs `dataclasses.replace`.

**Root cause:** Organic growth without a clear decision policy for when to use which system.
**Affected principles:** P10 (illegal states), P19 (KISS), P21 (least astonishment).
**Suggested approach:** Migrate `@dataclass(frozen=True)` internal types to `msgspec.Struct(frozen=True)` for consistency. Reserve `CqStruct` for cross-module contracts and `msgspec.Struct` for internal models.

### Theme 2: Result assembly via side-effectful mutation

Every macro follows the same pattern: create empty `CqResult`, pass it to 5-10 helper functions that mutate it in place, return the result. This makes the data flow hard to follow because each helper both reads and writes the result object. The neighborhood module partially addresses this with `BundleViewV1` as an intermediate pure data structure, but the final `render_snb_result` still mutates `CqResult` in place.

**Root cause:** `CqResult` is a mutable data bag by design (from `core/schema.py`).
**Affected principles:** P11 (CQS), P15 (tell don't ask), P23 (testability).
**Suggested approach:** Adopt a two-phase pattern: (1) Pure computation returns immutable intermediate structures. (2) A single assembly step converts intermediates to `CqResult`. The neighborhood module's `BundleViewV1` -> `render_snb_result` is the right direction.

### Theme 3: Calls macro accumulated complexity

The `calls/` subpackage (7 files, ~2,488 LOC) is the largest and most complex macro. It has accumulated semantic enrichment, front-door insight building, neighborhood integration, and cache-aware target resolution. These are all valuable features, but the orchestration in `entry.py` has become a 515-LOC pipeline with deep coupling to 7+ external modules.

**Root cause:** Incremental feature additions without periodic extraction.
**Affected principles:** P2 (separation of concerns), P3 (SRP), P19 (KISS).
**Suggested approach:** Extract front-door insight construction into a separate module (`calls/front_door.py`). The current `entry.py` would delegate to: (1) scanning, (2) analysis, (3) front-door assembly, (4) semantic enrichment, (5) result finalization. Each phase would be a single function call.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate `_FRONT_DOOR_PREVIEW_PER_SLICE` and `_CALLS_TARGET_CALLEE_PREVIEW` into single definitions | small | Eliminates 4-file constant drift risk |
| 2 | P21 (Least astonishment) | Remove public/private wrapper dualities in `scanning.py` and `analysis.py` | small | Clearer API surface |
| 3 | P24 (Observability) | Add module-level loggers to macros that lack them (6 files) | small | Enables debugging of silent failures |
| 4 | P20 (YAGNI) | Audit and remove unused contract types (`MacroExecutionRequestV1`, `MacroTargetResolutionV1`) | small | Reduces maintenance surface |
| 5 | P1 (Info hiding) | Rename underscore-prefixed functions imported across `calls/` modules to be properly public | small | Explicit internal contract within subpackage |

## Recommended Action Sequence

1. **Consolidate duplicated constants (P7).** Move `_FRONT_DOOR_PREVIEW_PER_SLICE` and `_CALLS_TARGET_CALLEE_PREVIEW` into `tools/cq/macros/contracts.py`. Update 6 import sites. Zero risk, immediate benefit.

2. **Clean up public/private API dualities (P21).** Remove trivial wrappers in `scanning.py:20-31` and `analysis.py:407-417`. Make the parameterized versions the sole exported functions. Update `calls/__init__.py` accordingly.

3. **Add structured logging to all macros (P24).** Add `logger = logging.getLogger(__name__)` and `debug`/`warning` calls following the `calls/entry.py` pattern to `impact.py`, `sig_impact.py`, `imports.py`, `scopes.py`, `bytecode.py`, `side_effects.py`, and `exceptions.py`.

4. **Separate computation from mutation in `_analyze_calls_sites` (P11, P23).** Return findings as a list rather than appending to `result.key_findings` inside the function. Let `_build_calls_result` handle all result mutation.

5. **Extract target resolution from `calls_target.py` (P3).** Create `calls_target_resolution.py` with `resolve_target_definition`, `_resolve_python_target_definition`, `_resolve_rust_target_definition`, and related helpers. Keep `calls_target.py` as the composition layer.

6. **Migrate `@dataclass` internal types to `msgspec.Struct` (Theme 1).** Convert 13 `@dataclass(frozen=True)` types to `msgspec.Struct(frozen=True)` for consistency. This enables uniform use of `msgspec.structs.replace` and `msgspec.structs.asdict` across the codebase.

7. **Make `CallSite` frozen and type its `context_window` field (P10).** Add `frozen=True` to `CallSite`, define a `ContextWindowV1(start_line: int, end_line: int)` struct, and replace `dict[str, int] | None`.

8. **Extract front-door assembly from `calls/entry.py` (Theme 3).** Create `calls/front_door.py` containing `_build_calls_front_door_state`, `_build_calls_front_door_insight`, and `_apply_calls_semantic_with_telemetry`. Reduces `entry.py` from 515 LOC to approximately 250 LOC.
