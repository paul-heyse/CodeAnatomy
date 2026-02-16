# Design Review: tools/cq CLI, Run Engine, Orchestration, and Supporting Modules

**Date:** 2026-02-16
**Scope:** `tools/cq/cli_app/` + `tools/cq/run/` + `tools/cq/orchestration/` + `tools/cq/ldmd/` + `tools/cq/utils/` + `tools/cq/perf/`
**Focus:** Boundaries (1-6), Simplicity (19-22), Quality (23-24)
**Depth:** deep
**Files reviewed:** 50

## Executive Summary

The CQ CLI and execution layer demonstrate strong architectural discipline overall. The CLI (`cli_app/`) cleanly separates cyclopts parameter parsing from domain logic via the Params/Options/Context layering, and the run engine (`run/`) correctly uses a tagged-union step specification with a dispatch table for extensibility. The strongest design wins are the `RequestFactory` as a canonical request construction boundary (centralizing 9 macro request types), the `RunStepBase` tagged union with msgspec for parse-don't-validate step decoding, and the clean functional core in `ldmd/format.py`. The primary areas for improvement are: (1) duplicated `_result_match_count` implementations between `q_step_collapsing.py` and `multilang_orchestrator.py`, (2) direct mutation of `CqResult.summary` fields in the run engine violating CQS, and (3) the `result.py` module combining filtering, rendering, artifact persistence, and output emission in a single 382-line file that changes for four distinct reasons.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `result.py` exposes artifact persistence internals; `CliContext.build` assembles services inline |
| 2 | Separation of concerns | 1 | medium | medium | `result.py` handles filtering + rendering + persistence + output in one module |
| 3 | SRP (one reason to change) | 2 | medium | medium | `step_executors.py` combines dispatch registry + parallel scheduling + scope filtering |
| 4 | High cohesion, low coupling | 2 | small | low | Good use of `RequestFactory`; minor coupling via `RunOptions` import in `run/loader.py` |
| 5 | Dependency direction | 2 | small | low | `run/loader.py` imports `cli_app.options.RunOptions`; should depend on spec-layer types |
| 6 | Ports & Adapters | 3 | - | - | `RenderEnrichmentPort` protocol properly implemented; `RequestFactory` is a clean adapter |
| 19 | KISS | 2 | small | low | `telemetry.py` triple-nested try/except; `_NON_SEARCH_DISPATCH` dict is simple and good |
| 20 | YAGNI | 2 | small | low | `uuid_factory.py` exposes 14 public symbols including v6/v8 variants of limited demonstrated use |
| 21 | Least astonishment | 2 | small | low | `CliResult.result` union of `CqResult | CliTextResult | int` requires isinstance checks everywhere |
| 22 | Declare and version public contracts | 3 | - | - | Strong: versioned V1 structs, `__all__` exports, tagged unions |
| 23 | Design for testability | 2 | medium | medium | `CliContext.build()` calls `Toolchain.detect()` and `resolve_runtime_services` -- hard to inject |
| 24 | Observability | 2 | small | low | Run engine has structured logging; telemetry events are well-structured but not emitted to OpenTelemetry |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Modules generally hide their internals well. The `RequestFactory` class in `request_factory.py` centralizes request construction behind a stable API, and the cyclopts `Parameter(parse=False)` pattern correctly hides context injection from the CLI framework. However, `result.py` exposes artifact persistence details (search artifact bundle construction, diagnostic artifact saving) that are implementation details of the output pipeline.

**Findings:**
- `tools/cq/cli_app/result.py:260-335` -- `_save_search_artifacts` and `_build_search_artifact_bundle` contain deep knowledge of `SearchObjectResolvedViewV1` internals (accessing `.summaries`, `.occurrences`, `.snippets`), coupling the CLI output layer to search object model details.
- `tools/cq/cli_app/context.py:107-126` -- `CliContext.build()` directly calls `Toolchain.detect()` and `resolve_runtime_services(root)`, hiding these behind a classmethod but making them untestable without monkeypatching.
- `tools/cq/cli_app/result.py:37-62` -- `_attach_insight_artifact_refs` directly mutates `result.summary.front_door_insight`, reaching through the result to modify an internal payload.

**Suggested improvement:**
Extract `_build_search_artifact_bundle` and `_save_search_artifacts` into `tools/cq/core/artifacts.py` (which already owns `save_artifact_json` and `save_search_artifact_bundle_cache`). The artifact bundle construction belongs with the artifact persistence layer, not the CLI result handler.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
The CLI command modules (`commands/search.py`, `commands/query.py`, etc.) are well-separated: each command function does minimal work (parse options, build request, delegate to service, wrap in CliResult). However, `result.py` is a significant concern-mixing module.

**Findings:**
- `tools/cq/cli_app/result.py` (382 LOC) handles four distinct concerns in one module: (a) result filtering via `apply_result_filters` (lines 65-97), (b) rendering via `render_result` (lines 100-141), (c) artifact persistence via `_handle_artifact_persistence`, `_save_search_artifacts`, `_save_general_artifacts` (lines 248-309), and (d) output emission via `_emit_output` and `_handle_non_cq_result` (lines 201-235). Each concern changes for a different reason: adding a new format changes rendering; changing the cache schema changes persistence; adding a new filter type changes filtering.
- `tools/cq/run/step_executors.py` (457 LOC) mixes dispatch registration (`_NON_SEARCH_DISPATCH` dict at line 439), parallel execution scheduling (lines 151-195), scope filtering (lines 386-427), and individual step executor implementations (lines 273-383). The step executors themselves are clean functions, but the module has too many reasons to change.

**Suggested improvement:**
Split `result.py` into three focused modules: `result_filter.py` (filtering), `result_render.py` (format dispatch and rendering), and `result_persist.py` (artifact saving). Keep `handle_result` as the orchestrator in a thin `result.py` that delegates to these three. For `step_executors.py`, extract `_apply_run_scope_filter` to `run/scope.py` (which already exists and handles scope merging).

**Effort:** medium
**Risk if unaddressed:** medium -- as new output formats and artifact types are added, this module will grow and become a merge-conflict hotspot.

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
Most modules have clear single responsibilities. The command modules are exemplary: each command function is a thin adapter between cyclopts parameters and the request/service layer.

**Findings:**
- `tools/cq/run/runner.py` (288 LOC) changes for two reasons: (1) the run orchestration algorithm (step ordering, stop-on-error, caching) and (2) the Q-step preparation/expansion logic (`_prepare_q_step`, `_expand_q_step_by_scope`, `_partition_q_steps`). The preparation logic could live with Q-step execution.
- `tools/cq/orchestration/bundles.py` (429 LOC) has a clear single purpose (bundle preset execution) but the four `_run_*` functions (lines 256-414) contain inline request construction that duplicates the `RequestFactory` patterns. The `BundleContext` correctly encapsulates the needed state.

**Suggested improvement:**
Move `_prepare_q_step`, `_expand_q_step_by_scope`, and `_partition_q_steps` from `runner.py` to `q_execution.py`, which already handles Q-step execution. This keeps Q-step concerns together.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The `RequestFactory` in `orchestration/request_factory.py` is a high-cohesion success: it centralizes 9 request construction patterns behind static methods, reducing coupling between command surfaces and macro internals. The `RunStep` tagged union in `run/spec.py` keeps all step types cohesive.

**Findings:**
- `tools/cq/run/loader.py:11` -- imports `RunOptions` from `tools.cq.cli_app.options`. This creates a coupling from the run engine (domain layer) to the CLI layer (presentation layer). `RunOptions` contains CLI-specific concerns (inherits `CommonFilters` with impact/confidence/severity filter fields) that are irrelevant to plan loading.
- `tools/cq/run/q_step_collapsing.py:10-11` imports `merge_language_cq_results` and `runmeta_for_scope_merge` from `orchestration/multilang_orchestrator.py`. This cross-cutting dependency is reasonable since Q-step collapsing genuinely requires multi-language merge, but creates a bidirectional concern between run and orchestration layers.

**Suggested improvement:**
Define a `RunLoadOptions` protocol or struct in `run/spec.py` containing only the fields needed for plan loading (`plan: Path | None`, `step: list[RunStep]`, `steps: list[RunStep]`). Have `load_run_plan` accept this narrower type, and construct it from `RunOptions` at the CLI boundary.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction (inward dependencies) -- Alignment: 2/3

**Current state:**
Dependencies generally flow correctly: CLI commands depend on `orchestration/request_factory.py` which depends on `core` and `macros`. The `ldmd/format.py` module correctly depends on `core/types.py` for `LdmdSliceMode` (not on `cli_app/types.py` -- the suspected violation is a false positive). The `orchestration/` layer correctly serves as an intermediary between CLI and core.

**Findings:**
- `tools/cq/run/loader.py:11` -- `from tools.cq.cli_app.options import RunOptions` is an inward-to-outward dependency violation. The run engine (`run/`) is a domain execution layer; importing from `cli_app/` (the outermost shell) inverts the dependency direction. The loader should depend on its own spec types.
- `tools/cq/cli_app/result.py:165` -- `from tools.cq.search.pipeline.smart_search import pop_search_object_view_for_run` -- the CLI result handler reaches into the search pipeline's module-level state to pop cached search views. This is a deep coupling from the output layer into search engine internals.
- `tools/cq/cli_app/result.py:207` -- `from tools.cq.cli_app.app import console` -- the result module imports a module-level console singleton from app.py, creating a circular-prone dependency between the app module and its result handler.

**Suggested improvement:**
For the `loader.py` issue: extract a `RunLoadInput` struct into `run/spec.py` with only `plan`, `step`, `steps` fields. For the `pop_search_object_view_for_run` issue: have the search service return the object view as part of the result or attach it to the `CqResult.artifacts`, eliminating the need for module-level state mutation.

**Effort:** small (loader) + medium (search view)
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
Well aligned. `RenderEnrichmentPort` in `core/ports.py` is implemented by `SmartSearchRenderEnrichmentAdapter` in `orchestration/render_enrichment.py` -- a textbook port/adapter pattern. The `RequestFactory` serves as an adapter layer between CLI concerns and macro request contracts. `NeighborhoodExecutionRequest` in `neighborhood/executor.py` acts as a clean port between the CLI and the neighborhood execution engine.

**Findings:**
No significant gaps. The design correctly uses protocols (`ParsedQStepLike` in `q_execution.py:27-46`) to decouple the Q-step execution from the concrete `ParsedQStep` dataclass defined in `runner.py`.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The CLI commands are refreshingly simple -- each is typically under 80 lines with a clear flow: require context, parse options, build request, execute, wrap result. The run engine's `_NON_SEARCH_DISPATCH` dict at `step_executors.py:439` is a simple and effective dispatch pattern.

**Findings:**
- `tools/cq/cli_app/telemetry.py:60-155` -- `invoke_with_telemetry` has a triple-nested try/except structure (parse error -> execution error -> runtime error) that is harder to follow than necessary. The CycloptsError handling is duplicated between the inner and outer catch blocks (lines 97-111 and lines 126-140 produce nearly identical `CqInvokeEvent` payloads).
- `tools/cq/run/runner.py:129-172` -- `_execute_q_steps` builds an intermediate `batch_specs` tuple-of-tuples to iterate over two groups (entity steps, pattern steps). The double-dispatch via `_run_grouped_q_batches` adds indirection; a simpler approach would sequentially process entity steps then pattern steps explicitly.
- `tools/cq/cli_app/types.py:190-270` -- `comma_separated_list` and `comma_separated_enum` are nearly identical functions (7 lines differ by one call). The generic `comma_separated_list(str)` and `comma_separated_list(SomeEnum)` would suffice if `str` and enum constructors had the same interface.

**Suggested improvement:**
Refactor `invoke_with_telemetry` to use an early-return pattern with a shared `_build_error_event` helper, eliminating the duplicated event construction. Merge `comma_separated_enum` into `comma_separated_list` -- they are functionally identical.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most code is exercised by the CLI, run engine, or test suite. The `RequestFactory` was a justified investment -- it eliminated scattered inline request construction across 3 call sites per macro.

**Findings:**
- `tools/cq/utils/uuid_factory.py` exposes 14 public symbols in `__all__`, including `uuid8_or_uuid7`, `legacy_compatible_event_id`, and `normalize_legacy_identity`. The UUIDv6 and UUIDv8 variants are speculative extensions: `uuid8_or_uuid7` is called from `uuid_temporal_contracts.py:gated_uuid8` but that function has no callers outside tests. The codebase consistently uses `uuid7_str()` and `run_id()`.
- `tools/cq/cli_app/result_action.py` -- The `apply_result_action` function supports a sequence of result-action policies (lines 43-55), but the actual usage is always a fixed 2-tuple: `(cq_result_action, "return_int_as_exit_code_else_zero")`. The flexible policy chain is unused beyond this single configuration.

**Suggested improvement:**
Consider making `uuid8_or_uuid7`, `legacy_compatible_event_id`, and `normalize_legacy_identity` private (prefixed with `_`) until there is a concrete second use case. The flexible result-action policy chain is harmless but worth noting as a seam that hasn't needed extension.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
The CLI commands behave as expected. The `--format` flag correctly dispatches to 8 renderers. The `--in` flag on search correctly scopes to directories. The `q` command's fallback to search for non-query strings is a well-documented and intuitive behavior.

**Findings:**
- `tools/cq/cli_app/context.py:159` -- `CliResultPayload = CqResult | CliTextResult | int` is a union of three unrelated types. Callers must use `isinstance` checks (`is_cq_result` property at line 182, `_handle_non_cq_result` at `result.py:220`). An `int` exit code as a valid result payload is surprising; it conflates the result with the exit code.
- `tools/cq/run/q_step_collapsing.py:140-158` -- `_normalize_single_group_result` mutates its input (`result.summary.query = ...`, `result.summary.lang = None`). The function name suggests normalization (which typically implies producing a new value), but it mutates in place and returns the same object. This is surprising for a frozen-looking CqResult.
- `tools/cq/cli_app/commands/ldmd.py:145-149` -- The `root` alias resolution (`if section_id == "root"`) is implemented inline in the `get` command but the same `_resolve_section_id` function already exists in `ldmd/format.py:161-167`. The LDMD command duplicates this logic.

**Suggested improvement:**
Remove `int` from the `CliResultPayload` union. Exit codes should be conveyed through `CliResult.exit_code`, not through the `result` field. For the Q-step collapsing mutation, either make it explicitly named `_mutate_single_group_result` or use `msgspec.structs.replace` to produce a new summary.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 3/3

**Current state:**
Excellent contract discipline. All cross-boundary structs use V1 suffixes (`RequestContextV1`, `SearchRequestOptionsV1`, `RunIdentityContractV1`, `TemporalUuidInfoV1`, `FrontDoorInsightV1`). The `run/spec.py` module explicitly versions the `RunPlan` (`version: int = 1`) and uses tagged unions for step types. `__all__` exports are declared in all modules.

**Findings:**
No significant gaps. The contract versioning is consistent and well-maintained.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The command functions are testable by constructing a `CliContext` and passing it directly (the `ctx` parameter accepts injection). The `ParsedQStepLike` protocol in `q_execution.py` enables testing with lightweight fakes. The pure functions in `ldmd/format.py` (parsing bytes, building indexes) are directly testable.

**Findings:**
- `tools/cq/cli_app/context.py:107-126` -- `CliContext.build()` calls `Toolchain.detect()` (probes the filesystem for ast-grep binary) and `resolve_runtime_services(root)` (constructs real service instances). Tests must either monkeypatch these or construct `CliContext` directly, bypassing the `build()` classmethod.
- `tools/cq/cli_app/result.py:207` -- `_emit_output` imports `console` from `tools.cq.cli_app.app`, a module-level singleton. Testing output emission requires capturing the console's file stream.
- `tools/cq/perf/smoke_report.py:59-108` -- `build_perf_smoke_report` directly calls `Toolchain.detect()`, `resolve_runtime_services`, and real search/query/calls macros with no injection seams. This is appropriate for a benchmark harness but means it cannot be unit-tested.
- `tools/cq/run/step_executors.py:39-45` -- `RUN_STEP_NON_FATAL_EXCEPTIONS` is a module-level tuple imported by `q_execution.py:24`. This shared exception list works but makes it impossible to test different error-handling policies without monkeypatching.

**Suggested improvement:**
Add a `CliContext.from_parts()` factory method that accepts pre-built `Toolchain` and `CqRuntimeServices` instances (similar to `RunContext.from_parts()`). This would make `CliContext` directly constructible in tests without filesystem probing. The `console` singleton could be made injectable via a `ConsolePort` protocol or passed explicitly to `_emit_output`.

**Effort:** medium
**Risk if unaddressed:** medium -- as the CLI grows, test setup becomes increasingly complex without proper injection.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The run engine uses structured `logging.debug` calls with consistent field ordering (`"Executing run plan steps=%d stop_on_error=%s root=%s"`). The `CqInvokeEvent` telemetry struct in `telemetry.py` captures parse/exec timing, command name, error classification, and UUID-based event identity -- a well-structured observability contract.

**Findings:**
- `tools/cq/run/runner.py:76-77,125` -- Debug logging uses structured format strings (`"Executing run plan steps=%d stop_on_error=%s root=%s"`) which is good, but there is no warning/error level logging when Q-step preparation fails silently (error results are appended to `immediate_results` without logging at line 189).
- `tools/cq/cli_app/telemetry.py:24-36` -- `CqInvokeEvent` captures timing and error info but is never emitted to any telemetry backend (no OpenTelemetry span, no file logging, no metrics). It's returned from `invoke_with_telemetry` but the caller (`launcher` at `app.py:225`) discards the event.
- `tools/cq/ldmd/format.py:115,133,230,245-247` -- LDMD parsing warnings use `logger.warning` with descriptive messages ("LDMD parse failure: %s", "LDMD TLDR parse failed"), which is appropriate for a parser module.

**Suggested improvement:**
Add warning-level logging in `_partition_q_steps` when a step produces an immediate error result. The `CqInvokeEvent` is well-structured but currently dead -- either wire it to a metrics sink (even a simple file) or document it as a structured contract for future observability integration.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Duplicated knowledge between run engine and orchestration

The `_result_match_count` function exists in two places: `tools/cq/run/q_step_collapsing.py:79-88` and `tools/cq/orchestration/multilang_orchestrator.py:152-159`. Both extract match counts from `CqResult.summary` using the same field-probing logic (`summary.matches` -> `summary.total_matches` -> `len(key_findings)`). The implementations differ only in whether they accept `CqResult | None` vs `CqResult`. This is a classic DRY violation on knowledge: the "how to extract match count from a summary" truth is duplicated.

**Root cause:** The run engine's Q-step collapsing module needed the same summary introspection that the orchestration layer already had, but imported the merge function rather than the utility.

**Affected principles:** P7 (DRY), P4 (cohesion)

**Suggested approach:** Move `_result_match_count` to `tools/cq/core/schema.py` or `tools/cq/core/summary_contract.py` as a public utility (e.g., `extract_match_count(summary: CqSummary) -> int`), and have both modules call it.

### Theme 2: Mutable summary fields treated as an output builder

Across the run engine (`q_step_collapsing.py`, `q_execution.py`, `run_summary.py`, `runner.py`), `CqResult.summary` is mutated directly via attribute assignment. There are 14 such mutations in `run/` alone (e.g., `result.summary.lang = step.plan.lang`, `merged.summary.plan_version = plan.version`). This pattern treats `CqSummary` as a mutable builder despite the rest of the architecture using frozen msgspec structs.

**Root cause:** `CqSummary` appears to be intentionally mutable (it's not frozen) to allow incremental summary population. This is a pragmatic choice but it means any code holding a reference to a `CqResult` can observe unexpected mutations.

**Affected principles:** P11 (CQS -- functions both mutate and return), P21 (least astonishment)

**Suggested approach:** Consider a builder pattern (`CqSummaryBuilder`) that produces a frozen summary at the end of the pipeline, or document the mutation convention explicitly and ensure mutations only happen during the "build" phase before the result is returned to callers.

### Theme 3: `result.py` as a growing concern-aggregation point

`tools/cq/cli_app/result.py` (382 LOC) is the most concern-dense module in scope. It handles result filtering, format rendering, artifact persistence (with two distinct artifact strategies: search vs general), output emission, and insight card attachment. Every new feature (format, filter type, artifact kind) touches this file.

**Root cause:** The initial design correctly centralized "handle result" as a single entry point, but as the system grew (LDMD format, search artifact bundles, insight cards, diagnostics artifacts), the module accumulated responsibilities without splitting.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P1 (information hiding)

**Suggested approach:** Decompose into `result_filter.py`, `result_render.py`, `result_persist.py` as described in P2. Keep `handle_result` as the top-level coordinator.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7/DRY | Deduplicate `_result_match_count` into `core/summary_contract.py` | small | Eliminates knowledge duplication between run and orchestration |
| 2 | P5 | Replace `RunOptions` import in `run/loader.py` with a narrower spec-layer type | small | Fixes inward-to-outward dependency violation |
| 3 | P19/KISS | Merge `comma_separated_enum` into `comma_separated_list` in `cli_app/types.py` | small | Removes 35 lines of duplicated converter logic |
| 4 | P21 | Use `_resolve_section_id` from `ldmd/format.py` instead of inline root-alias logic in `ldmd` command | small | Eliminates duplicated root-section resolution |
| 5 | P24 | Add warning-level log in `_partition_q_steps` when step produces error result | small | Makes Q-step failures visible in logs |

## Recommended Action Sequence

1. **Deduplicate `_result_match_count`** (P7) -- Extract to `core/summary_contract.py` as a public helper. Both `q_step_collapsing.py` and `multilang_orchestrator.py` import and use the shared version. No behavioral change.

2. **Fix `run/loader.py` dependency direction** (P5) -- Define a `RunLoadInput` protocol or struct in `run/spec.py` with fields `plan`, `step`, `steps`. Update `load_run_plan` to accept it. Update the CLI command to construct `RunLoadInput` from `RunOptions`.

3. **Merge `comma_separated_enum` into `comma_separated_list`** (P19) -- The enum converter is functionally identical to the generic list converter. Remove the separate function and use `comma_separated_list(SomeEnum)` in `params.py`.

4. **Remove inline root-alias logic in LDMD `get` command** (P21) -- Use the existing `_resolve_section_id` function from `ldmd/format.py` instead of the 4-line inline version at `commands/ldmd.py:144-149`.

5. **Add `CliContext.from_parts` factory** (P23) -- Create an alternative constructor that accepts pre-built `Toolchain` and `CqRuntimeServices`, enabling lightweight test construction without filesystem probing.

6. **Add warning logging to `_partition_q_steps`** (P24) -- Log at warning level when a step produces an immediate error result, including the step ID and error message.

7. **Split `result.py` into focused modules** (P2/P3) -- Decompose into `result_filter.py`, `result_render.py`, `result_persist.py`. This is the highest-effort item but addresses the most structural debt. Depends on items 1-6 being complete to avoid merge conflicts.

8. **Extract Q-step preparation to `q_execution.py`** (P3) -- Move `_prepare_q_step`, `_expand_q_step_by_scope`, and `_partition_q_steps` from `runner.py` to `q_execution.py` to co-locate all Q-step concerns.
