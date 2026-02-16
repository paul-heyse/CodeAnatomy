# Design Review: tools/cq CLI, Execution, and Supporting Modules

**Date:** 2026-02-16
**Scope:** `tools/cq/{cli_app,run,orchestration,index,introspection,astgrep,ldmd,utils,perf}`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 71

## Executive Summary

These modules form the outer shell of the CQ tool: CLI parsing, multi-step execution, orchestration, repository indexing, and supporting utilities. Overall alignment is strong. The CLI layer demonstrates disciplined separation between cyclopts parameter types (dataclasses) and internal option structs (msgspec), and the run engine uses tagged unions effectively for step dispatch. The most significant design debt is duplicated neighborhood orchestration logic between the CLI command and the run step executor, and the pervasive use of `result.summary[key] = value` as an untyped side-channel that undermines several principles simultaneously. The index and introspection modules are clean, focused, and well-isolated.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `result.summary` dict used as public mutation surface |
| 2 | Separation of concerns | 2 | medium | medium | Neighborhood CLI command embeds full orchestration logic |
| 3 | SRP | 2 | medium | medium | `step_executors.py` owns both dispatch and neighborhood orchestration |
| 4 | High cohesion, low coupling | 3 | - | - | Modules well-partitioned; narrow cross-module imports |
| 5 | Dependency direction | 2 | small | low | `ldmd/format.py` imports from `cli_app/types.py` |
| 6 | Ports & Adapters | 3 | - | - | `RenderEnrichmentPort` adapter pattern well-implemented |
| 7 | DRY (knowledge) | 1 | medium | high | Neighborhood orchestration duplicated across two files |
| 8 | Design by contract | 2 | medium | medium | `result.summary` lacks typed contract for mutation keys |
| 9 | Parse, don't validate | 3 | - | - | Step decode, query parse, target spec parse all at boundary |
| 10 | Make illegal states unrepresentable | 2 | small | low | `RunPlan.steps` permits empty tuple; `SearchStep.mode` uses raw strings |
| 11 | CQS | 2 | small | low | `_save_general_artifacts` and `_save_search_artifacts` both mutate and return |
| 12 | DI + explicit composition | 2 | small | low | `RequestFactory` uses lazy imports instead of injected dependencies |
| 13 | Prefer composition over inheritance | 3 | - | - | `FilterParams` inheritance is shallow and appropriate |
| 14 | Law of Demeter | 2 | small | low | `result.run.run_id`, `ctx.toolchain.to_dict()` chains acceptable but common |
| 15 | Tell, don't ask | 2 | medium | medium | `result.summary.get("error")` pattern spreads decision logic to callers |
| 16 | Functional core, imperative shell | 2 | medium | medium | Run engine mixes pure step compilation with IO execution |
| 17 | Idempotency | 3 | - | - | UUID7-based run IDs and deterministic merge ensure safe re-execution |
| 18 | Determinism | 3 | - | - | `normalize_step_ids()` and deterministic merge ordering |
| 19 | KISS | 3 | - | - | Modules are appropriately simple for their responsibilities |
| 20 | YAGNI | 2 | small | low | `contracts.py` is an empty placeholder; `_STEP_TAGS` unused externally |
| 21 | Least astonishment | 2 | small | low | `handle_result` silently pops search object views as side effect |
| 22 | Declare public contracts | 2 | small | low | `__all__` used consistently; some internal helpers lack underscore prefix |
| 23 | Design for testability | 2 | medium | medium | `_execute_neighborhood_step` hard to test without full stack |
| 24 | Observability | 2 | small | low | 5 of 9 modules use logging; CLI telemetry good but not all modules covered |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The CLI layer properly hides cyclopts details behind the params/options conversion boundary. Internal dispatch tables (`_STEP_BUILDERS` in `chain.py:188`, `_NON_SEARCH_DISPATCH` in `step_executors.py:494`) are module-private. However, `CqResult.summary` is a `dict[str, object]` used as a public mutation surface throughout the execution layer.

**Findings:**
- `run/q_execution.py:119-126` and `run/q_execution.py:185-192`: Direct dict mutation of `result.summary["lang"]` and `result.summary["query_text"]` after execution, repeated identically in both entity and pattern paths.
- `run/runner.py:83`: `merged.summary["plan_version"] = plan.version` -- run metadata injected via untyped dict.
- `run/step_executors.py:435`: `result.summary["target_resolution_kind"] = resolved.resolution_kind` -- neighborhood metadata via dict.
- `cli_app/result.py:61`: `result.summary["front_door_insight"] = to_public_front_door_insight_dict(updated)` -- insight data via dict.

**Suggested improvement:**
Define a `RunSummaryFields` TypedDict (or msgspec Struct) that declares the known summary keys (`lang`, `query_text`, `plan_version`, `target_resolution_kind`, `front_door_insight`, `error`, `cache_backend`). Use a typed helper function like `set_summary_field(result, key, value)` that validates against the contract, or migrate `summary` to a typed struct.

**Effort:** medium
**Risk if unaddressed:** medium -- Any consumer of `result.summary` must guess which keys exist; typos in key names silently create new entries.

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The CLI layer properly separates parameter parsing (params.py) from domain logic (commands/*.py) from output rendering (result.py). The run engine separates plan loading (loader.py) from execution (runner.py) from step dispatch (step_executors.py). However, two modules embed orchestration logic that belongs in a shared service.

**Findings:**
- `cli_app/commands/neighborhood.py:56-113`: The CLI command directly imports and orchestrates `parse_target_spec`, `resolve_target`, `BundleBuildRequest`, `build_neighborhood_bundle`, `render_snb_result`, `mk_runmeta`, `assign_result_finding_ids`, and `maybe_evict_run_cache_tag`. This is a full orchestration pipeline embedded in a CLI handler.
- `run/step_executors.py:353-438`: `_execute_neighborhood_step` contains nearly identical orchestration logic with the same imports, same field-by-field `BundleBuildRequest` construction, and same post-processing steps.
- `cli_app/result.py:247-265`: `_handle_artifact_persistence` mixes concerns of search artifact lifecycle management (popping search object views) with general artifact saving.

**Suggested improvement:**
Extract a shared `execute_neighborhood()` function in a new `tools/cq/neighborhood/executor.py` (or extend the existing neighborhood module) that both the CLI command and the run step executor delegate to. The function should accept a simple request struct and return a `CqResult`, encapsulating all internal orchestration.

**Effort:** medium
**Risk if unaddressed:** medium -- Bug fixes or feature additions to neighborhood must be applied in two places, and they can drift silently.

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
Most modules have a single clear responsibility. `spec.py` owns step types, `chain.py` owns chain compilation, `loader.py` owns plan loading. The main SRP tension is in `step_executors.py`.

**Findings:**
- `run/step_executors.py` (513 lines): This module owns three distinct responsibilities: (a) the non-Q step dispatch table and routing logic (lines 53-83, 494-504), (b) individual step executor implementations for 8+ step types (lines 279-351), and (c) the full neighborhood orchestration pipeline (lines 353-438). Changes to neighborhood logic, changes to dispatch routing, and adding new step types all require modifying this single file.
- `cli_app/result.py` (382 lines): Combines result rendering (`render_result`), result filtering (`apply_result_filters`), artifact persistence (`_handle_artifact_persistence`, `_save_search_artifacts`, `_save_general_artifacts`), search artifact bundle construction (`_build_search_artifact_bundle`), and output emission (`_emit_output`). These are at least three distinct concerns.

**Suggested improvement:**
Split `step_executors.py` into: (a) `step_dispatch.py` for the dispatch table and routing, (b) keep individual executors in `step_executors.py`, and (c) delegate neighborhood to the shared executor from P2. For `result.py`, extract artifact persistence into `artifact_persistence.py` within `cli_app/`.

**Effort:** medium
**Risk if unaddressed:** low -- The current state is manageable but will degrade as step types grow.

---

#### P4. High cohesion, low coupling -- Alignment: 3/3

**Current state:**
Module boundaries are well-chosen. The `index/` package is self-contained with only `protocol.py` defining its external contract. `introspection/` has zero imports from other CQ packages. `astgrep/` imports only from the scanner contract. Cross-module imports within scope are narrow and follow a clear layering: `cli_app` -> `run` -> `orchestration` -> `core`.

No action needed.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Generally correct. Core modules (`introspection/`, `utils/`, `index/`) do not depend on outer layers. However, one inversion exists.

**Findings:**
- `ldmd/format.py:9`: `from tools.cq.cli_app.types import LdmdSliceMode` -- The LDMD format parser (a library-level utility) imports an enum from the CLI application layer. This means the LDMD format module cannot be used independently of the CLI layer.
- `utils/interval_index.py:7`: `from tools.cq.astgrep.sgpy_scanner import SgRecord, group_records_by_file` -- The generic interval index utility depends on a specific scanner type. The generic `IntervalIndex[T]` is properly parameterized, but `FileIntervalIndex` and `from_records` hard-code `SgRecord`.

**Suggested improvement:**
Move `LdmdSliceMode` to a shared types module (e.g., `tools/cq/core/types.py` or `tools/cq/ldmd/types.py`) so the LDMD format parser does not depend on the CLI layer. For `interval_index.py`, keep `IntervalIndex[T]` generic and move the `SgRecord`-specific `FileIntervalIndex` to `tools/cq/astgrep/` or `tools/cq/query/`.

**Effort:** small
**Risk if unaddressed:** low -- The LDMD format module is tightly coupled to CLI but unlikely to be reused independently soon.

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
The adapter pattern is well-applied. `orchestration/render_enrichment.py` implements `RenderEnrichmentPort` from `core/ports.py`, cleanly adapting smart-search classification for the render layer. `index/protocol.py` defines `SymbolIndex` as a Protocol for dependency injection. `run/helpers.py:15` defines `RunContextLike` Protocol and `run/q_execution.py:27` defines `ParsedQStepLike` Protocol, both enabling clean structural typing.

No action needed.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
The most significant DRY violation in scope is the duplicated neighborhood orchestration knowledge.

**Findings:**
- `cli_app/commands/neighborhood.py:56-113` and `run/step_executors.py:353-438`: Both locations encode identical knowledge about: (a) how to parse target specs, (b) how to resolve targets with language casting, (c) the full field list for `BundleBuildRequest` construction, (d) post-render steps (`assign_result_finding_ids`, `maybe_evict_run_cache_tag`, setting `target_resolution_kind` on summary). If the `BundleBuildRequest` contract adds a field, both sites must be updated.
- `run/q_execution.py:119-126` and `run/q_execution.py:185-192`: The four-line block `result.summary["lang"] = step.plan.lang; result.summary["query_text"] = step.step.query; results.append(...)` appears identically in both the success and error paths of both `execute_entity_q_steps` and `execute_pattern_q_steps` (4 total repetitions).
- `run/step_executors.py:222-238` and `run/step_executors.py:258-276`: `_execute_search_step` and `execute_search_fallback` both construct `RequestContextV1` and call `RequestFactory.search` with overlapping construction patterns. The `_build_search_includes` call and `resolve_runtime_services(ctx.root).search.execute(request)` pattern is duplicated.

**Suggested improvement:**
(1) Extract neighborhood orchestration into a shared function as described in P2. (2) Extract a `_annotate_q_result(result, step)` helper for the repeated summary annotation. (3) Consider a `_build_search_request(plan, ctx, ...)` factory helper to consolidate search request construction.

**Effort:** medium
**Risk if unaddressed:** high -- The neighborhood duplication is the most likely source of future drift bugs.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Typed contracts are used well for step types (`RunStep` tagged union), CLI options (msgspec structs), and index types (`FnDecl`, `ClassDecl`). The primary gap is the untyped `result.summary` dict.

**Findings:**
- `result.summary` is typed as `dict[str, object]` but functions throughout the codebase write specific keys with specific expected types. At least 15 distinct keys are written across the scoped modules (`lang`, `query_text`, `plan_version`, `error`, `cache_backend`, `target_resolution_kind`, `front_door_insight`, `python_semantic_telemetry`, `rust_semantic_telemetry`, `semantic_planes`, `mode`, `query`, `macro_summaries`, `step_summaries`, `skipped`). None of these are declared as part of a contract.
- `run/runner.py:218`: `_results_have_error` checks `"error" in result.summary` -- the error signaling mechanism is a string key existence check with no typed contract.

**Suggested improvement:**
Define a `SummaryContract` TypedDict with known keys and their expected types. Use helper functions for reading and writing summary fields that provide runtime validation in debug mode.

**Effort:** medium
**Risk if unaddressed:** medium -- Silent key typos and type mismatches are possible.

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
Boundary parsing is consistently applied. `run/step_decode.py` parses JSON into typed `RunStep` objects using msgspec at the boundary. `run/chain.py` compiles raw CLI tokens into typed `ChainCommand` then `RunStep` objects. `run/loader.py` loads TOML into validated `RunPlan` at entry. `cli_app/params.py` uses cyclopts converters (`_run_step_converter`, `_run_steps_converter`) to parse JSON strings into typed objects at the CLI boundary. `ldmd/format.py` parses raw LDMD text into a validated `LdmdIndex` with stack-based section tracking.

No action needed.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
The tagged union pattern for `RunStep` types is well-chosen and prevents most invalid step configurations. However, some opportunities remain.

**Findings:**
- `run/spec.py:15-30`: `RunPlan.steps` is typed as `tuple[RunStep, ...]` which permits an empty tuple. The `loader.py:57-59` validates non-emptiness at load time, but the type itself does not prevent construction of an empty plan elsewhere.
- `run/spec.py`: `SearchStep.mode` is `str | None` where the valid values are `"regex"`, `"literal"`, or `None`. A `Literal["regex", "literal"] | None` type would prevent invalid mode strings.
- `cli_app/context.py:56-80`: `CliContext.output_format` is `OutputFormat | None` where `None` means "use default". The default resolution happens in `result.py:174`. A non-optional field with a default would be clearer.

**Suggested improvement:**
Use `Literal["regex", "literal"] | None` for `SearchStep.mode`. Consider a `NewType` or validation in `RunPlan.__post_init__` for non-empty steps (msgspec supports `__post_init__`).

**Effort:** small
**Risk if unaddressed:** low -- Current validation catches these at runtime.

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most functions follow CQS cleanly. The main violations are in the artifact persistence path.

**Findings:**
- `cli_app/result.py:268-291` (`_save_search_artifacts`): This function both saves artifacts to disk (command) and mutates `result.artifacts` (command) and mutates `result.summary["front_door_insight"]` via `_attach_insight_artifact_refs` (command). The caller has no indication that the result object is being mutated in-place.
- `cli_app/result.py:294-308` (`_save_general_artifacts`): Same pattern -- saves to disk, appends to `result.artifacts`, and mutates `result.summary`.
- `cli_app/result.py:143-197` (`handle_result`): Combines filtering (transform), artifact persistence (command), rendering (query), and output emission (command) in a single function, though this is acceptable as a top-level orchestrator.

**Suggested improvement:**
Have artifact saving functions return the artifact references and let the caller decide whether to attach them to the result. This separates the IO side effect from the result mutation.

**Effort:** small
**Risk if unaddressed:** low -- The mutation is localized and unlikely to cause bugs in practice.

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
Dependency injection is used effectively in some areas (Protocol types, `SymbolIndex`, `RunContextLike`). However, `RequestFactory` in `orchestration/request_factory.py` uses lazy imports inside methods rather than injected dependencies.

**Findings:**
- `orchestration/request_factory.py:64-158`: All `RequestFactory` static methods use lazy `from tools.cq.macros.contracts import ...` imports inside the method body. This avoids circular imports but hides the dependency graph. The factory cannot be easily tested with alternative implementations.
- `run/step_executors.py:237`: `services = resolve_runtime_services(ctx.root)` -- runtime service resolution happens inside the executor rather than being injected. This pattern repeats at lines 237, 275, 283, 284.

**Suggested improvement:**
Pass `runtime_services` as a parameter through `RunExecutionContext` rather than resolving them inside each executor. For `RequestFactory`, the lazy imports are an acceptable pragmatic choice given the circular dependency constraints, but documenting which contracts each method requires would improve clarity.

**Effort:** small
**Risk if unaddressed:** low -- The current approach works; the main cost is testing difficulty.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
Inheritance is used sparingly and appropriately. `FilterParams` is a base dataclass extended by `QueryParams`, `SearchParams`, `ReportParams`, etc. in `cli_app/params.py`. This is a single level of inheritance for parameter grouping, which is the right tool for the job. `RunStepBase` uses msgspec's tagged union via `tag=True, tag_field="type"` rather than class hierarchy dispatch.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most access chains are short (one or two dots). The primary concern is the `result.run.run_id` and `result.summary.get(key)` patterns which appear throughout.

**Findings:**
- `cli_app/result.py:255`: `result.run.run_id` -- reaching through `result` to `run` to `run_id`.
- `cli_app/result.py:256`: `result.run.macro == "search"` -- behavioral dispatch based on reaching into nested fields.
- `run/step_executors.py:420`: `ctx.toolchain.to_dict()` -- acceptable since `toolchain` is a direct collaborator property.
- `orchestration/bundles.py:232`: `merged.summary["macro_summaries"] = merged.summary.get("step_summaries", {})` -- reading and writing to the same nested dict.

**Suggested improvement:**
Add convenience methods on `CqResult` like `result.is_search_macro()` and `result.get_run_id()` to encapsulate these access patterns. This is a minor improvement; the current chains are shallow.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The dispatch-table patterns in `step_executors.py` and `chain.py` follow tell-don't-ask well. However, several locations inspect `result.summary` to make decisions externally.

**Findings:**
- `run/runner.py:218`: `_results_have_error` inspects `"error" in result.summary` to decide whether to stop execution. The result object should be able to tell the caller whether it represents an error.
- `cli_app/result.py:256`: `if result.run.macro == "search"` -- dispatching artifact behavior based on interrogating the result object rather than having the result know how to persist itself.
- `run/step_executors.py:150`: `if stop_on_error and result.summary.get("error")` -- same pattern of asking the result about its error state.

**Suggested improvement:**
Add a `CqResult.has_error` property (or `is_error` boolean field) to encapsulate error detection logic. For artifact persistence dispatch, consider a strategy pattern keyed by macro type.

**Effort:** medium
**Risk if unaddressed:** medium -- As the number of summary-key-based decisions grows, the fragility increases.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The separation is partially achieved. Pure computation (query parsing, plan compilation, chain compilation, step normalization) is well-separated from IO. However, the run engine interleaves pure and effectful operations.

**Findings:**
- `run/runner.py:59-126` (`execute_run_plan`): Mixes pure operations (step partitioning, normalization, result merging) with IO operations (step execution, cache eviction, backend metrics snapshot). The pure planning phase is not separated from the execution phase.
- `run/step_executors.py:353-438` (`_execute_neighborhood_step`): Mixes target resolution (pure-ish) with bundle building (IO), rendering (transform), cache eviction (IO), and result annotation (mutation) in a single function.
- The chain compilation path (`chain.py`) is entirely pure, which is good.

**Suggested improvement:**
In `execute_run_plan`, separate into two phases: (1) `compile_run_execution(plan, ctx)` returns a pure execution plan with partitioned steps and scope resolutions, (2) `execute_compiled_plan(compiled, ctx)` performs IO. This enables testing the compilation logic without executing steps.

**Effort:** medium
**Risk if unaddressed:** medium -- Testing the run engine requires full integration setup.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
UUID7-based run IDs (`utils/uuid_factory.py`) provide unique identity for each invocation. `normalize_step_ids()` in `spec.py` ensures deterministic step naming. The `maybe_evict_run_cache_tag` call at the end of run execution is safe on re-invocation. Artifact saving uses unique paths derived from run IDs.

No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Step ID assignment is deterministic via `normalize_step_ids()`. Multi-language result merging uses sorted language keys in `multilang_orchestrator.py`. The interval tree construction in `interval_index.py` uses stable sorting. The run summary aggregation collects metrics deterministically.

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 3/3

**Current state:**
Modules are appropriately simple. The `introspection/` package uses standard library facilities (dis, symtable) with thin wrappers. `astgrep/rules.py` is a straightforward dispatch. `ldmd/format.py` uses a stack-based parser that is clear and minimal. `utils/uuid_factory.py` wraps uuid6 with minimal added complexity. The chain compilation in `chain.py` reuses cyclopts parsing infrastructure rather than building a custom parser.

No action needed.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Almost no speculative generality. Two minor exceptions exist.

**Findings:**
- `cli_app/contracts.py`: Empty placeholder file containing only `__all__: list[str] = []`. This is a speculative placeholder for "future CLI-specific contracts" per its docstring. It should be deleted until needed.
- `run/spec.py`: `_STEP_TAGS` dict (mapping step type to tag string) is defined but only used by `step_type()` which is a simple accessor. The dict duplicates information already encoded in the msgspec tag metadata.

**Suggested improvement:**
Delete `contracts.py`. For `_STEP_TAGS`, consider using `msgspec.structs.struct_info()` to derive tags dynamically rather than maintaining a parallel dict.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs are generally predictable. The main surprise is a hidden side effect in the result handling pipeline.

**Findings:**
- `cli_app/result.py:164`: `handle_result` calls `pop_search_object_view_for_run(run_id)` which destructively consumes a search object view from a global registry as a side effect. The function name `handle_result` does not suggest it will pop items from a global cache.
- `cli_app/result.py:254-258`: When `no_save` is True for a search macro, the function still pops the search object view (to prevent memory leaks), which is correct behavior but surprising -- "no save" still has a side effect.
- `run/chain.py:82`: `cast("Any", cli_app)` -- casting to `Any` to work around cyclopts type narrowing is unexpected but documented by the context.

**Suggested improvement:**
Rename the pop call to something more explicit like `_cleanup_search_state(result, no_save)` that encapsulates the lifecycle concern. Add a comment explaining why cleanup is needed even in the no-save path.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
`__all__` exports are declared consistently in every module (verified across all 71 files). The `run/__init__.py` re-exports key step types. `introspection/__init__.py` re-exports all public symbols. The `orchestration/__init__.py` uses lazy `__getattr__` for deferred module loading.

**Findings:**
- Several internal helper functions lack leading underscores despite being implementation details: `error_result` in `run/helpers.py:30` (re-imported with underscore alias in `step_executors.py:23` showing the intent), `merge_in_dir` in `run/helpers.py:56`.
- `index/def_index.py`: `SELF_CLS_NAMES` (line ~25) is a module-level constant imported by `arg_binder.py` and `call_resolver.py`. It is part of the public contract but not declared in `__all__` of the index package.

**Suggested improvement:**
Prefix internal helpers with underscores at the definition site. Add `SELF_CLS_NAMES` to the index package `__all__` if it is intentionally public.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure modules (`introspection/`, `utils/interval_index.py`, `ldmd/format.py`, `run/spec.py`, `run/chain.py`, `run/scope.py`) are fully testable without IO. The Protocol types (`SymbolIndex`, `RunContextLike`, `ParsedQStepLike`) enable test doubles. However, several key functions are hard to test in isolation.

**Findings:**
- `run/step_executors.py:353-438` (`_execute_neighborhood_step`): Requires `parse_target_spec`, `resolve_target`, `build_neighborhood_bundle`, `render_snb_result`, and cache eviction -- all tightly coupled internal imports that cannot be swapped without monkeypatching.
- `run/step_executors.py:202-238` (`_execute_search_step`): Calls `resolve_runtime_services(ctx.root)` to get a search service, then immediately executes. The service resolution is not injectable.
- `cli_app/result.py:143-197` (`handle_result`): Depends on `console` (imported from `app.py`), `FilterConfig`, `OutputFormat`, and `pop_search_object_view_for_run` -- all resolved via lazy imports inside the function body.

**Suggested improvement:**
For step executors, accept a `services` parameter (or include it in `RunExecutionContext`) rather than resolving inside the function. For `handle_result`, the lazy imports are a pragmatic circular-dependency avoidance, but the function could accept a `ConsoleAdapter` protocol for the output sink.

**Effort:** medium
**Risk if unaddressed:** medium -- Integration tests compensate but are slower and more fragile.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Logging is present in 5 of 9 scoped packages: `run/step_executors.py`, `run/runner.py`, `ldmd/format.py`, `introspection/symtable_extract.py`, and `index/def_index.py`. The CLI telemetry system (`cli_app/telemetry.py`) provides structured event capture with UUID7 identity, timing, and error classification.

**Findings:**
- `cli_app/commands/neighborhood.py`: No logging despite being a complex orchestration path. Failures in target resolution or bundle building would produce no diagnostic output.
- `orchestration/multilang_orchestrator.py`: No logger despite orchestrating parallel language-scoped execution. Timeouts or fallbacks would be silent.
- `orchestration/bundles.py`: No logger despite running multi-step report bundles. The `result.summary["skipped"] = reason` at line 422 writes a skip reason to the summary dict but does not log it.
- `index/files.py`: No logging for file tabulation, which can silently produce empty file lists.

**Suggested improvement:**
Add `logger = logging.getLogger(__name__)` and debug-level logging to `neighborhood.py`, `multilang_orchestrator.py`, `bundles.py`, and `files.py`. Log at minimum: entry/exit with key parameters, error conditions, and performance-relevant metrics (file count, step count, execution time).

**Effort:** small
**Risk if unaddressed:** low -- The telemetry system captures high-level events, but per-module diagnostics are missing for troubleshooting.

---

## Cross-Cutting Themes

### Theme 1: Untyped Summary Dict as Communication Channel

**Root cause:** `CqResult.summary` is `dict[str, object]`, used as the universal metadata carrier across the entire execution pipeline. At least 15 distinct keys are written by scoped modules, with no compile-time or runtime contract.

**Affected principles:** P1 (information hiding), P8 (design by contract), P10 (make illegal states), P15 (tell don't ask).

**Suggested approach:** Introduce a `SummaryFields` TypedDict or frozen struct that declares the known keys. Migration can be incremental: define the type, add a helper to write fields, then migrate call sites one at a time. This is the single highest-value improvement across all principles.

### Theme 2: Neighborhood Duplication

**Root cause:** The neighborhood command was implemented as a CLI command first, then the run-step executor was added independently with copy-paste adaptation.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P7 (DRY), P23 (testability).

**Suggested approach:** Extract a `tools/cq/neighborhood/executor.py:execute_neighborhood()` function that accepts a typed request and returns a `CqResult`. Both `cli_app/commands/neighborhood.py` and `run/step_executors.py:_execute_neighborhood_step` delegate to it.

### Theme 3: Runtime Service Resolution Inside Executors

**Root cause:** `resolve_runtime_services(ctx.root)` is called inside individual step executors rather than being provided via the execution context.

**Affected principles:** P12 (DI), P23 (testability), P16 (functional core/imperative shell).

**Suggested approach:** Add a `services` field to `RunExecutionContext` that is resolved once at run plan entry and threaded through. This enables test doubles and separates service resolution from step execution.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7/P2 | Extract shared neighborhood executor | medium | Eliminates highest-risk duplication |
| 2 | P5 | Move `LdmdSliceMode` out of `cli_app/types.py` | small | Fixes dependency inversion |
| 3 | P20 | Delete empty `cli_app/contracts.py` | small | Removes speculative placeholder |
| 4 | P24 | Add loggers to 4 modules missing observability | small | Improves troubleshooting coverage |
| 5 | P22 | Prefix internal helpers with underscores | small | Clarifies public API surface |

## Recommended Action Sequence

1. **Extract neighborhood executor** (P7, P2, P3, P23): Create `tools/cq/neighborhood/executor.py` with a shared `execute_neighborhood()` function. Update both `cli_app/commands/neighborhood.py` and `run/step_executors.py` to delegate to it. This resolves the highest-risk duplication and the primary SRP violation.

2. **Fix dependency inversion in LDMD** (P5): Move `LdmdSliceMode` from `cli_app/types.py` to `ldmd/types.py` (or `core/types.py`). Update the import in `ldmd/format.py`.

3. **Add observability to uncovered modules** (P24): Add `logger = logging.getLogger(__name__)` and structured debug logging to `neighborhood.py`, `multilang_orchestrator.py`, `bundles.py`, and `files.py`.

4. **Define summary field contract** (P1, P8, P15): Introduce a `SummaryFields` TypedDict declaring known keys. Add read/write helpers. Migrate call sites incrementally.

5. **Clean up public surface** (P20, P22): Delete `contracts.py`. Prefix `error_result` and `merge_in_dir` in `helpers.py` with underscores. Add `SELF_CLS_NAMES` to index `__all__`.

6. **Thread runtime services through context** (P12, P23): Add a `services` field to `RunExecutionContext`, resolve once in `execute_run_plan()`, and pass through to step executors.

7. **Tighten step type constraints** (P10): Use `Literal["regex", "literal"] | None` for `SearchStep.mode`. Consider validating non-empty steps in `RunPlan`.
