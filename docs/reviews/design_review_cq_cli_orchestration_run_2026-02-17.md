# Design Review: CQ CLI, Orchestration, Run, Neighborhood, LDMD, Perf

**Date:** 2026-02-17
**Scope:** `tools/cq/cli_app/`, `tools/cq/orchestration/`, `tools/cq/run/`, `tools/cq/neighborhood/`, `tools/cq/ldmd/`, `tools/cq/perf/`, `tools/cq/__init__.py`, `tools/cq/__main__.py`, `tools/cq/cli.py`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 68

## Executive Summary

The CQ CLI, orchestration, and run subsystems demonstrate strong contract-driven design with consistent use of frozen msgspec structs, a clean request/response pattern via `RequestFactory`, and well-structured run-step tagged unions. The primary weaknesses are: (1) module-level mutable singletons (`console`, `error_console`, `app.config`) that undermine testability and information hiding, (2) duplicated knowledge across the Params/Options/Schema triple hierarchy and `schema_projection.py` boilerplate, (3) god modules in `tree_sitter_collector.py` (661 LOC) and `multilang_orchestrator.py` (515 LOC) that combine multiple change reasons, and (4) shell-layer commands containing domain logic (include-glob building, search fallback). The highest-impact improvement is extracting the Params/Schema projection into a generic mechanism, followed by decomposing the tree-sitter collector and introducing a Console protocol to replace module-level singletons.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | Module-level `console`/`error_console` singletons and `app.config` mutation expose internal state |
| 2 | Separation of concerns | 1 | medium | medium | Shell commands embed domain logic (include-glob building, search fallback routing) |
| 3 | SRP | 1 | large | medium | `tree_sitter_collector.py` (661 LOC), `multilang_orchestrator.py` (515 LOC), `bundles.py` (434 LOC) each have 3+ change reasons |
| 4 | High cohesion, low coupling | 2 | medium | medium | `bundles.py` directly imports 7 macro functions; `result_render.py` imports `console` from `app.py` |
| 5 | Dependency direction | 2 | small | low | Core orchestration is clean; CLI shell imports core correctly; minor inversion in `result_render.py:60` |
| 6 | Ports & Adapters | 2 | medium | low | `render_enrichment.py` shows good port/adapter pattern; missing for Console and persistence |
| 7 | DRY | 1 | medium | medium | Triple Params/Options/Schema hierarchy; `schema_projection.py` has 11 identical function pairs; scope filter duplication |
| 8 | Design by contract | 2 | small | low | Request structs have clear contracts; `RunPlan` preconditions implicit rather than validated |
| 9 | Parse, don't validate | 2 | small | low | `RunStep` tagged union parses once at boundary; `NeighborhoodParams` validates inline |
| 10 | Make illegal states unrepresentable | 2 | small | low | Tagged union for `RunStep` prevents invalid combinations; `CliResult.result` union could be tighter |
| 11 | CQS | 1 | medium | medium | `handle_result()` renders output AND returns exit code; `_configure_app()` mutates global state |
| 12 | DI + explicit composition | 1 | medium | high | `CliContext.build()` hardcodes side effects; `result_render.py:60` imports singleton; no Console injection |
| 13 | Composition over inheritance | 3 | - | - | `RunStepBase` uses struct inheritance appropriately; no deep hierarchies |
| 14 | Law of Demeter | 2 | small | low | `chain.py` uses `getattr` chains; `telemetry.py:90` accesses `command.__qualname__` |
| 15 | Tell, don't ask | 2 | small | low | `bundles.py` queries `target.bundle_kind` then switches; `run_summary.py` inspects `isinstance` chains |
| 16 | Functional core, imperative shell | 1 | medium | medium | `search.py:58-64` builds include globs in shell; `query.py:55-89` contains search fallback logic |
| 17 | Idempotency | 3 | - | - | Run steps are stateless transforms; re-execution is safe |
| 18 | Determinism | 2 | small | low | `multilang_orchestrator.py` has deterministic merge ordering; parallel execution has fallback-to-serial |
| 19 | KISS | 2 | small | low | `schema_projection.py` adds unnecessary ceremony; `params.py` metaprogramming is complex but justified |
| 20 | YAGNI | 2 | small | low | `perf/smoke_report.py` uses `argparse` instead of cyclopts (separate concern); generally lean |
| 21 | Least astonishment | 2 | small | low | `NeighborhoodParams` defined differently from all other params (not schema-generated) |
| 22 | Declare and version public contracts | 2 | small | low | `__all__` exports declared consistently; `RequestContextV1` versioned; `CliContext` surface implicit |
| 23 | Testability | 1 | medium | high | Module singletons, `CliContext.build()` side effects, `handle_result()` imports singletons at call time |
| 24 | Observability | 2 | small | low | `telemetry.py` provides structured invoke events; `runner.py` has debug logging; no structured metrics |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
Module-level mutable singletons are the primary violation. `app.py:153-154` creates `console` and `error_console` as module globals. `app.py:190-194` (`_configure_app`) mutates `app.config` at runtime. These internals are accessed by `result_render.py:60` (`from tools.cq.cli_app.app import console`) and `result_action.py:58` (`from tools.cq.cli_app.app import console`).

**Findings:**
- `tools/cq/cli_app/app.py:153-154`: `console = _make_console()` and `error_console = _make_console(stderr=True)` are module-level mutable singletons accessible to any importer.
- `tools/cq/cli_app/app.py:190-194`: `_configure_app()` mutates `app.config` on a module-level `App` instance, making config state globally visible and order-dependent.
- `tools/cq/cli_app/result_render.py:60`: `from tools.cq.cli_app.app import console` -- rendering module reaches into app module for the console singleton.
- `tools/cq/cli_app/result_action.py:58`: `from tools.cq.cli_app.app import console` -- same pattern in result action handling.
- `tools/cq/cli_app/protocol_output.py:1-6`: `_JSON_ENCODER` module-level singleton, though this is effectively immutable and low-risk.

**Suggested improvement:**
Introduce a `ConsolePort` protocol (e.g., `Protocol` with `print()` and `file` property) and thread it through `CliContext`. Replace direct imports of `console` from `app.py` with dependency injection via the context. For `app.config` mutation, consider making the config chain immutable and passing it to the launcher rather than mutating a module global.

**Effort:** medium
**Risk if unaddressed:** high -- Module-level mutables create hidden coupling that breaks parallel test execution and makes behavior order-dependent.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
Several shell-layer command handlers contain domain logic that should live in the core. The `search.py` command handler builds include globs from `--in` directory flags, and `query.py` implements the search-fallback routing policy when query parsing fails.

**Findings:**
- `tools/cq/cli_app/commands/search.py:57-64`: The `--in` directory-to-glob translation logic (`looks_like_file`, `f"{in_value}/**"` construction) is domain logic embedded in the CLI shell layer.
- `tools/cq/cli_app/commands/query.py:55-89`: The search-fallback policy on `QueryParseError` (deciding when to fall back to search vs. return an error) is a domain routing decision living in the command handler.
- `tools/cq/cli_app/result_action.py:19-54`: `handle_result()` mixes filtering, ID assignment, persistence, rendering, and output emission in a single function -- 5 distinct concerns in 35 lines.
- `tools/cq/cli_app/telemetry.py:60-155`: `invoke_with_telemetry()` mixes timing measurement, argument parsing, command dispatch, result-action application, and event construction in deeply nested try/except blocks.

**Suggested improvement:**
Extract the `--in` to glob conversion into a function in `tools/cq/run/scope.py` or a shared utility. Move the search-fallback routing policy into a dedicated function in `tools/cq/query/` that the shell simply delegates to. Decompose `handle_result()` into a pipeline of explicit steps (filter, assign IDs, persist, render, emit) that can be tested independently.

**Effort:** medium
**Risk if unaddressed:** medium -- Domain logic in the shell layer makes it harder to reuse the logic from run-step execution and harder to test without CLI infrastructure.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
Three modules each have multiple independent change reasons, creating large, hard-to-navigate files.

**Findings:**
- `tools/cq/neighborhood/tree_sitter_collector.py` (661 LOC): Combines tree parsing (`_parser`, `_parse_tree_for_request`), anchor resolution (`_resolve_anchor`, `_lift_anchor`, `_find_definition_by_name`), relationship collection (`_collect_parent_nodes`, `_collect_sibling_nodes`, `_collect_callers_callees`, `_collect_conjunction_peers`), slice building (`_build_slice`, `_build_slices`), and structural export invocation. At least 4 distinct change reasons.
- `tools/cq/orchestration/multilang_orchestrator.py` (515 LOC): Combines language priority/ordering (`language_priority`, `execute_by_language_scope`), finding merge/sort (`merge_partitioned_items`, `_finding_score`, `_finding_location`, `_clone_finding_with_language`), semantic telemetry aggregation (`_zero_semantic_telemetry`, `_coerce_semantic_telemetry`, `_sum_semantic_telemetry`, `_aggregate_semantic_telemetry`), front-door insight selection (`_select_front_door_insight`, `_collect_insights_by_language`, `_select_ordered_insight`), and the main merge orchestration (`merge_language_cq_results`). At least 4 distinct change reasons.
- `tools/cq/orchestration/bundles.py` (434 LOC): Combines target scope resolution (`resolve_target_scope`, `TargetScope`), result filtering (`filter_result_by_scope`), result merging (`merge_bundle_results`), and 4 preset runner functions (`_run_refactor_impact`, `_run_safety_reliability`, `_run_change_propagation`, `_run_dependency_health`). At least 3 distinct change reasons.

**Suggested improvement:**
For `tree_sitter_collector.py`: Extract anchor resolution into `anchor_resolution.py`, relationship collection into `relationship_collector.py`, and slice building into `slice_builder.py`. The top-level `collect_tree_sitter_neighborhood` becomes a thin orchestrator. For `multilang_orchestrator.py`: Extract telemetry aggregation into `multilang_telemetry.py` and insight selection into `multilang_insight.py`. For `bundles.py`: Extract preset definitions into `bundle_presets.py` and scope filtering into the existing `scope.py`.

**Effort:** large
**Risk if unaddressed:** medium -- Large modules accumulate more merge conflicts and are harder for developers to navigate and reason about.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Most modules have reasonable cohesion. The `RequestFactory` pattern successfully centralizes request construction. However, `bundles.py` directly imports 7 macro command functions, creating high fan-out coupling.

**Findings:**
- `tools/cq/orchestration/bundles.py:18-24`: Direct imports of `cmd_bytecode_surface`, `cmd_exceptions`, `cmd_impact`, `cmd_imports`, `cmd_scopes`, `cmd_side_effects`, `cmd_sig_impact` -- 7 macro functions imported directly.
- `tools/cq/cli_app/commands/chain.py:7`: Imports `app` from `app.py` creating a circular dependency path between commands and the app module.
- `tools/cq/run/step_executors.py:436-445`: `_NON_SEARCH_DISPATCH` dispatch table imports 8 step types and directly references executor functions, which is good dispatch isolation.

**Suggested improvement:**
For `bundles.py`, route macro invocations through the `CqRuntimeServices` service layer (similar to how `search` and `calls` already use `ctx.services.search.execute()` and `ctx.services.calls.execute()`). This would decouple bundle execution from macro implementation details.

**Effort:** medium
**Risk if unaddressed:** medium -- Direct macro imports mean bundle code must change whenever a macro's interface changes.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The overall dependency direction is sound: CLI commands depend on orchestration, orchestration depends on core schemas/contracts, and core has no upward dependencies. `RequestFactory` uses lazy imports to avoid circular references. One minor inversion exists in the rendering path.

**Findings:**
- `tools/cq/cli_app/result_render.py:60`: `from tools.cq.cli_app.app import console` -- the rendering utility (lower-level concern) imports from the app module (higher-level orchestration), creating an upward dependency.
- `tools/cq/orchestration/request_factory.py:62-67`: `RequestFactory` correctly uses lazy imports for all macro request types, keeping the factory module independent of macro implementations at import time.
- `tools/cq/run/step_executors.py:276-338`: Each executor function uses lazy imports for macro commands (e.g., `from tools.cq.macros.impact import cmd_impact`), maintaining correct dependency direction.

**Suggested improvement:**
Make `emit_output` accept a console-like object as a parameter rather than importing the singleton, resolving the single upward dependency.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`render_enrichment.py` demonstrates a clean port/adapter pattern with `SmartSearchRenderEnrichmentAdapter` implementing `RenderEnrichmentPort`. The `CqRuntimeServices` pattern acts as a service locator for search and calls services. However, Console output and artifact persistence lack port abstractions.

**Findings:**
- `tools/cq/orchestration/render_enrichment.py`: Clean implementation of `RenderEnrichmentPort` protocol -- a good exemplar.
- `tools/cq/cli_app/result_render.py:58-70`: `emit_output()` directly accesses `console.file` and `console.print()` without any abstraction. A `ConsolePort` protocol would enable testing without Rich dependency.
- `tools/cq/cli_app/result_persist.py`: Artifact persistence is tightly coupled to file system operations without a storage port.

**Suggested improvement:**
Define a `ConsolePort` protocol in `tools/cq/cli_app/protocols.py` with `print(text)` and `write_raw(text)` methods. Thread it through `CliContext` and use it in `emit_output()` and `handle_result()`.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
The Params/Options/Schema triple hierarchy is the single largest DRY violation. Each command's parameter contract is expressed three times: as a msgspec schema in `command_schema.py`, as a runtime-generated cyclopts dataclass in `params.py`, and as a type alias in `options.py`. On top of this, `schema_projection.py` contains 11 pairs of nearly identical projection functions.

**Findings:**
- `tools/cq/cli_app/schema_projection.py:63-237`: 11 pairs of `project_*_params()` and `*_options_from_projected_params()` functions that all follow the identical pattern of calling `_project()` and `_options()`. This is 175 lines of boilerplate that could be a generic function or a registry.
- `tools/cq/cli_app/options.py:28-38`: 11 type aliases (`CommonFilters = CommonFilterCommandSchema`, etc.) that add a naming layer with no behavioral difference.
- `tools/cq/cli_app/params.py:319-396`: 11 runtime-generated `*Params` classes that mirror the schemas in `command_schema.py`. The generation is necessary for cyclopts, but the TYPE_CHECKING block (lines 307-318) creates a confusing dual identity.
- `tools/cq/run/step_executors.py:383-424` vs `tools/cq/orchestration/bundles.py:166-206`: Both contain nearly identical scope-filtering logic for findings by file path. `_apply_run_scope_filter` in `step_executors.py` and `filter_result_by_scope` in `bundles.py` duplicate the same in-scope check pattern.
- `tools/cq/cli_app/commands/neighborhood.py:23-63`: `NeighborhoodParams` is defined as a plain dataclass with inline cyclopts annotations, diverging from the schema-driven generation pattern used by all other commands. This is a separate knowledge source for the same concept.

**Suggested improvement:**
For `schema_projection.py`: Replace the 11 function pairs with a single generic `project_params(params, schema_type)` and `options_from_params(params, schema_type, options_type)` function. Callers can call these directly with type arguments. For scope filtering: Extract the common finding-scope-filter logic into `tools/cq/core/scope_filter.py` and have both `step_executors.py` and `bundles.py` delegate to it. For `NeighborhoodParams`: Either add `NeighborhoodCommandSchema` to `command_schema.py` and generate params like other commands, or document the intentional divergence.

**Effort:** medium
**Risk if unaddressed:** medium -- Every new command requires changes in 4+ files following identical patterns, increasing the risk of inconsistency.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Request structs use frozen msgspec structs with typed fields, providing implicit structural contracts. `RunPlan` and `RunStep` have well-typed fields. However, some preconditions are implicit rather than validated.

**Findings:**
- `tools/cq/run/spec.py:15-22`: `RunPlan` accepts `steps` as a tuple with no validation that step IDs are unique or that step types are compatible. `normalize_step_ids()` is called separately rather than being part of construction.
- `tools/cq/neighborhood/executor.py:41-55`: `NeighborhoodExecutionRequest` has `lang` typed as `str | None` rather than `QueryLanguage | None`, weakening the contract.
- `tools/cq/orchestration/request_factory.py:47-59`: `SearchRequestOptionsV1` has well-typed defaults providing clear postcondition guarantees.

**Suggested improvement:**
Add a `__post_init__` validation to `RunPlan` or use a factory function that normalizes step IDs at construction time. Tighten `NeighborhoodExecutionRequest.lang` to `QueryLanguage | None`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The `RunStep` tagged union (`spec.py`) is an excellent example of parse-don't-validate: JSON input is parsed into a discriminated union once at the boundary via `msgspec.json.decode(raw, type=RunStep)`. However, some command handlers parse inputs repeatedly.

**Findings:**
- `tools/cq/run/spec.py:24-31`: `RunStepBase` with `tag=True, tag_field="type"` provides clean parse-once semantics for run steps.
- `tools/cq/run/step_decode.py`: `parse_run_step_json()` and `parse_run_steps_json()` use `decode_json_strict` for single-point parsing.
- `tools/cq/cli_app/commands/expand.py`: Performs JSON parsing and validation inline in the command handler rather than at a typed boundary.

**Suggested improvement:**
Move the expand command's JSON parsing into a dedicated decode function following the `step_decode.py` pattern.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
The `RunStep` union type prevents invalid step type combinations. `CliResult` uses a union payload type. However, some states remain representable that should not be.

**Findings:**
- `tools/cq/cli_app/context.py:188-233`: `CliResult.result` is typed as `CqResult | CliTextResult | int` but the handling code in `result_action.py:32-33` checks `is_cq_result` as a runtime property rather than using exhaustive pattern matching. A non-CQ result that is neither `CliTextResult` nor `int` would silently return the exit code.
- `tools/cq/run/spec.py:43-52`: `SearchStep.mode` is `QueryMode | None` where `None` means "auto-detect" -- this is reasonable but could be more explicit with a sentinel value.
- `tools/cq/cli_app/types.py`: Enum types (`OutputFormat`, `ImpactBucket`, etc.) correctly constrain value spaces.

**Suggested improvement:**
Consider making `CliResult.result` a proper tagged union or using exhaustive match. The current `is_cq_result` property approach works but is less safe than structural dispatch.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS (Command/Query Separation) -- Alignment: 1/3

**Current state:**
Several functions violate CQS by both performing side effects and returning meaningful values.

**Findings:**
- `tools/cq/cli_app/result_action.py:19-54`: `handle_result()` performs filtering, ID assignment, persistence, rendering, AND output emission (all side effects), then returns an exit code (a query). This is the most significant CQS violation -- callers cannot observe the result without triggering output.
- `tools/cq/cli_app/app.py:190-194`: `_configure_app()` mutates the module-level `app.config` attribute as a side effect of the launcher flow. While it returns `None`, the mutation is hidden from the caller.
- `tools/cq/run/runner.py:49-119`: `execute_run_plan()` mutates `merged` via repeated reassignment (which is fine for local variables with frozen structs), but also calls `maybe_evict_run_cache_tag()` (lines 116-117) as a side effect embedded in what looks like a query-oriented function.

**Suggested improvement:**
Split `handle_result()` into `prepare_output(cli_result) -> PreparedOutput` (pure query) and `emit_prepared_output(prepared) -> None` (command). The caller then calls both and returns the exit code. For `execute_run_plan()`, extract cache eviction into a post-execution hook or make it explicit in the caller.

**Effort:** medium
**Risk if unaddressed:** medium -- Functions that mix queries and commands are hard to test and reason about.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
`CliContext.build()` hardcodes side-effectful operations (filesystem access, toolchain detection, service resolution) with no ability to inject alternatives. The `handle_result()` function imports singletons at call time via lazy imports.

**Findings:**
- `tools/cq/cli_app/context.py:117-163`: `CliContext.build()` calls `resolve_repo_context()` (filesystem), `Toolchain.detect()` (subprocess calls), and `resolve_runtime_services()` (service initialization) directly. The `from_parts()` classmethod (line 89-114) exists as an injection point but `build()` is the primary entry point from the CLI.
- `tools/cq/cli_app/result_render.py:60`: `emit_output()` imports `console` from `app.py` via lazy import inside the function body, hiding the dependency.
- `tools/cq/cli_app/result_action.py:25-30`: `handle_result()` uses 6 lazy imports from various modules, making its dependency surface invisible to callers.
- `tools/cq/orchestration/request_factory.py:62-396`: `RequestFactory` uses lazy imports extensively but does so as a conscious architectural choice to break circular dependencies. This is acceptable.

**Suggested improvement:**
Add a `CliContext.build()` overload or parameter that accepts pre-resolved components for testing. Alternatively, make `build()` accept a `ContextFactory` protocol that provides `resolve_root()`, `detect_toolchain()`, and `resolve_services()` methods. The default implementation calls the real functions; test code injects stubs.

**Effort:** medium
**Risk if unaddressed:** high -- Testing CLI commands currently requires filesystem access and toolchain detection, making unit tests slow and fragile.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The codebase uses inheritance sparingly and appropriately. `RunStepBase` provides a common `id` field via struct inheritance, and all step types compose via the `RunStep` union rather than deep class hierarchies. `CommonFilterCommandSchema` serves as a base for command schemas with a single level of inheritance.

**Findings:**
- `tools/cq/run/spec.py:24-31`: `RunStepBase` as a single-level base for step types is appropriate and avoids deep hierarchies.
- `tools/cq/cli_app/command_schema.py`: `CommonFilterCommandSchema` as a base for command schemas uses single-level inheritance effectively.
- No deep inheritance chains found in the reviewed scope.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most modules talk to direct collaborators. A few violations exist in chain compilation and telemetry.

**Findings:**
- `tools/cq/run/chain.py`: Uses `getattr` extensively to extract fields from parsed cyclopts params objects, reaching through the params structure.
- `tools/cq/cli_app/telemetry.py:90`: `command_name = getattr(command, "__qualname__", command_name)` -- accesses a dunder attribute on a callable, though this is an accepted Python idiom.
- `tools/cq/cli_app/validators.py`: `validate_launcher_invariants()` uses `getattr` to inspect kwargs, reaching into the parameter structure.

**Suggested improvement:**
For `chain.py`, consider adding accessor methods to the params types or using a dedicated extraction function rather than scattered `getattr` calls.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The code generally tells objects to do things, but some modules query objects extensively before acting.

**Findings:**
- `tools/cq/orchestration/bundles.py:71-84`: `_bundle_target_kind()` and `_bundle_target_value()` extract values from `TargetSpecV1` and then the bundle steps switch on the extracted kind. A dispatch-on-target-kind method on the target would be cleaner.
- `tools/cq/run/run_summary.py:44-54`: Inspects `merged.summary` fields with multiple `isinstance` and `is None` checks before deciding what to populate. The summary object could provide a method like `needs_population(field)`.
- `tools/cq/neighborhood/tree_sitter_collector.py:120`: `getattr(child, "is_named", False)` -- asks the node for a property rather than calling a typed method.

**Suggested improvement:**
These are minor; the `isinstance` pattern in `run_summary.py` is idiomatic for summary population. No high-priority action needed.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 1/3

**Current state:**
Several shell-layer commands contain domain logic that should be pure functions in the core. The shell is "thick" in places where it should only be a thin adapter between CLI parsing and core orchestration.

**Findings:**
- `tools/cq/cli_app/commands/search.py:57-64`: The `--in` directory handling logic determines whether a path looks like a file (`candidate.is_file()`, `requested.suffix`), builds include globs, and appends patterns. This is domain logic (how to translate a user-facing `--in` flag to scan scope) embedded in the shell.
- `tools/cq/cli_app/commands/query.py:55-89`: The search-fallback routing policy (fall back to search when query parsing fails and the input has no query tokens) is a domain decision. This 34-line block should be a function in the query module.
- `tools/cq/cli_app/context.py:144-155`: `CliContext.build()` performs I/O (repo root resolution, toolchain detection, service initialization) inside what appears to be a construction method. This mixes imperative initialization with the context value object.
- `tools/cq/cli_app/result_action.py:19-54`: `handle_result()` interleaves pure transformations (filtering, ID assignment) with I/O operations (persistence, rendering, output emission) in a single function without separation.

**Suggested improvement:**
Extract `--in` to glob conversion into `tools/cq/run/scope.py:apply_in_dir_scope(in_dir, root) -> list[str]`. Move query-fallback routing into `tools/cq/query/router.py:route_query_or_search(query_string, ctx) -> CqResult`. Separate `handle_result()` into pure preparation and impure emission phases.

**Effort:** medium
**Risk if unaddressed:** medium -- Thick shells mean domain logic cannot be reused from non-CLI entry points (run steps, API, tests) without duplicating or importing CLI code.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All run steps are effectively stateless transforms. Re-executing a run plan with the same inputs produces the same results. Cache eviction in `runner.py:116-117` is idempotent by design (evicting an already-evicted tag is a no-op).

**Findings:**
- No idempotency violations found. The frozen struct patterns and stateless execution model naturally support idempotent operations.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
The `multilang_orchestrator.py` merge ordering uses deterministic priority-based sorting with language, score, and location as tie-breakers. Parallel execution has a fallback-to-serial mechanism that preserves determinism when parallelism fails.

**Findings:**
- `tools/cq/orchestration/multilang_orchestrator.py:111-117`: `merge_partitioned_items()` uses a deterministic sort key `(priority, -score, *location)`.
- `tools/cq/orchestration/multilang_orchestrator.py:79-86`: `execute_by_language_scope()` falls back to serial execution on timeout, ensuring deterministic results.
- `tools/cq/run/step_executors.py:190-195`: Parallel step execution falls back to serial on timeout, preserving determinism.
- `tools/cq/run/runner.py:65`: `run_id` is generated from `uuid7_str()` which includes a timestamp component, meaning repeated runs get different IDs. This is correct for tracing but means exact byte-level reproducibility requires ID normalization.

**Suggested improvement:**
Minor: consider adding an option to supply a deterministic `run_id` for reproducibility testing.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforwardly implemented. The `params.py` metaprogramming (`make_dataclass` from msgspec schemas) is complex but justified by the need to bridge msgspec and cyclopts. `schema_projection.py` adds unnecessary ceremony.

**Findings:**
- `tools/cq/cli_app/params.py:126-158`: `_build_params_class()` using `make_dataclass` is complex metaprogramming but serves a clear purpose: generating cyclopts-compatible dataclasses from msgspec schemas without manual duplication.
- `tools/cq/cli_app/schema_projection.py:63-237`: 11 function pairs that could be replaced by 2 generic functions. This adds 175 lines of complexity for no behavioral gain.
- `tools/cq/cli_app/telemetry.py:60-155`: `invoke_with_telemetry()` has 3 levels of nested try/except with duplicated event construction. Could be simplified with a shared event builder.

**Suggested improvement:**
Replace `schema_projection.py` function pairs with generic versions. Simplify `invoke_with_telemetry()` by extracting event construction into a helper.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The codebase is generally lean without speculative generality. The seams exist where they should (e.g., `RequestFactory`, `_NON_SEARCH_DISPATCH` table). One minor concern.

**Findings:**
- `tools/cq/perf/smoke_report.py`: Uses `argparse` directly rather than cyclopts, creating an inconsistency. However, this is a standalone developer tool, so the divergence is acceptable under YAGNI (no need to force cyclopts integration for a perf tool).
- `tools/cq/cli_app/result_action.py:83-95`: `_apply_single_action()` supports 4 string literal actions (`return_int_as_exit_code_else_zero`, `return_value`, `return_zero`, `return_none`). Only `return_int_as_exit_code_else_zero` appears to be used in practice (line 128). The others may be speculative.

**Suggested improvement:**
Audit whether `return_value`, `return_zero`, and `return_none` action literals are actually used. If not, remove them.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs generally behave as expected. The main surprise is the inconsistency in how `NeighborhoodParams` is defined compared to all other command params.

**Findings:**
- `tools/cq/cli_app/commands/neighborhood.py:23-63`: `NeighborhoodParams` is a hand-written dataclass with inline cyclopts annotations, while all other command params are generated from `command_schema.py` via `_build_params_class()`. A developer familiar with the pattern would expect neighborhood to follow the same approach.
- `tools/cq/cli_app/options.py:28-38`: The `*Options` aliases (e.g., `CommonFilters = CommonFilterCommandSchema`) are pure aliases with no behavioral difference, which may confuse developers who expect `Options` types to be distinct from `CommandSchema` types.

**Suggested improvement:**
Either add `NeighborhoodCommandSchema` to `command_schema.py` and generate `NeighborhoodParams` like other commands, or add a prominent comment explaining why neighborhood params diverge from the pattern.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
All modules declare `__all__` exports consistently. `RequestContextV1` and `SearchRequestOptionsV1` are versioned with `V1` suffix. The `CliContext` public surface is implicit.

**Findings:**
- Consistent `__all__` declarations across all 68 files reviewed.
- `tools/cq/orchestration/request_factory.py:29`: `RequestContextV1` uses versioned naming.
- `tools/cq/neighborhood/executor.py:41`: `NeighborhoodExecutionRequest` lacks version suffix, though it is effectively V1.
- `tools/cq/run/spec.py:24`: `RunStepBase` lacks version suffix but is versioned via the `RunPlan.version` field.

**Suggested improvement:**
Minor: consider adding version suffixes to `NeighborhoodExecutionRequest` and `BundleContext` for consistency with other public contracts.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
Testing CLI commands requires significant infrastructure setup due to module-level singletons, side-effectful construction, and lazy imports of global state.

**Findings:**
- `tools/cq/cli_app/app.py:153-154`: Module-level `console` and `error_console` are created at import time. Tests that import any module touching `app.py` get the real Console instances with no way to substitute.
- `tools/cq/cli_app/context.py:117-163`: `CliContext.build()` calls `resolve_repo_context()`, `Toolchain.detect()`, and `resolve_runtime_services()` -- three side-effectful operations that require monkeypatching to test. `from_parts()` (line 89-114) is the testable alternative but `build()` is the primary production path.
- `tools/cq/cli_app/result_action.py:25-30`: Six lazy imports inside `handle_result()` make the dependency surface invisible and require careful monkeypatching to isolate for testing.
- `tools/cq/cli_app/result_render.py:60`: `emit_output()` imports `console` inside the function body, requiring module-level patching to test without actual terminal output.
- `tools/cq/orchestration/bundles.py:18-24`: Direct macro imports mean testing a bundle preset requires all macro dependencies to be available.

**Suggested improvement:**
(1) Thread `Console` through `CliContext` instead of importing the singleton. (2) Make `CliContext.build()` accept an optional `ContextFactory` protocol for testing. (3) Move `handle_result()` lazy imports to module-level imports guarded by TYPE_CHECKING, and pass dependencies explicitly. (4) For bundle testing, use the `CqRuntimeServices` service layer to indirect macro calls.

**Effort:** medium
**Risk if unaddressed:** high -- Low testability leads to insufficient test coverage of CLI-layer logic, increasing the risk of regressions when modifying the result pipeline.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The `telemetry.py` module provides structured `CqInvokeEvent` payloads with parse/exec timing, error classification, and event identity. `runner.py` uses Python logging at debug level. No structured metrics or tracing spans are emitted.

**Findings:**
- `tools/cq/cli_app/telemetry.py:23-36`: `CqInvokeEvent` is a well-structured telemetry payload with timing, error classification, and identity fields.
- `tools/cq/run/runner.py:66-71,118`: Debug-level logging for plan execution start and completion with step count and run ID.
- `tools/cq/run/step_executors.py:67,111`: Debug and warning logging for step execution.
- `tools/cq/ldmd/format.py:26,110,128,147,248`: Warning-level logging for parse failures with contextual messages.
- No OpenTelemetry spans or structured metrics found in the CLI/orchestration/run layers (contrast with `src/obs/` which has OpenTelemetry integration).

**Suggested improvement:**
Consider adding OpenTelemetry spans to `execute_run_plan()` and `execute_neighborhood()` for tracing multi-step workflows. The existing `CqInvokeEvent` could be emitted as a span event.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Module-Level Mutable Singletons Undermine Multiple Principles

**Root cause:** `app.py` creates `console`, `error_console`, and `app` at module level. These are imported by `result_render.py`, `result_action.py`, and `chain.py`, creating hidden coupling.

**Affected principles:** P1 (information hiding), P11 (CQS -- `_configure_app` mutates `app.config`), P12 (DI -- no injection point), P23 (testability -- cannot substitute in tests).

**Suggested approach:** Define a `ConsolePort` protocol. Thread it through `CliContext`. Replace direct `console` imports with context-based access. For `app.config`, make the config chain immutable and pass it to the launcher as a parameter.

### Theme 2: Triple Params/Schema/Options Hierarchy Creates Maintenance Burden

**Root cause:** The need to bridge cyclopts (dataclass-based) and msgspec (struct-based) created three representations of the same parameter contracts, plus a projection layer.

**Affected principles:** P7 (DRY -- knowledge expressed 3x), P19 (KISS -- unnecessary ceremony in `schema_projection.py`), P21 (least astonishment -- `NeighborhoodParams` diverges from pattern).

**Suggested approach:** (1) Replace 11 `schema_projection.py` function pairs with 2 generic functions. (2) Consider whether `options.py` aliases add value or just confusion. (3) Bring `NeighborhoodParams` into the schema-driven pattern or document the exception.

### Theme 3: Domain Logic in Shell Layer

**Root cause:** The CLI command handlers grew to include domain logic (scope resolution, search fallback, include-glob building) that should live in the core modules.

**Affected principles:** P2 (separation of concerns), P16 (functional core, imperative shell), P23 (testability -- testing domain logic requires CLI infrastructure).

**Suggested approach:** Extract domain logic into pure functions in the appropriate core modules (`run/scope.py`, `query/router.py`). Command handlers should be thin adapters: parse params, delegate to core, wrap result.

### Theme 4: God Modules with Multiple Change Reasons

**Root cause:** `tree_sitter_collector.py`, `multilang_orchestrator.py`, and `bundles.py` accumulated responsibilities over time without decomposition.

**Affected principles:** P3 (SRP), P4 (cohesion), P23 (testability -- large modules are harder to test in isolation).

**Suggested approach:** Decompose each module into focused sub-modules as described in P3 findings. Prioritize `tree_sitter_collector.py` (661 LOC, 4+ change reasons) first.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 | Replace 11 `schema_projection.py` function pairs with 2 generic functions | small | Eliminates 175 lines of boilerplate; prevents future drift |
| 2 | P7 | Extract shared scope-filter logic from `step_executors.py:383-424` and `bundles.py:166-206` into `core/scope_filter.py` | small | Removes duplicated filtering knowledge |
| 3 | P16 | Extract `--in` dir-to-glob logic from `search.py:57-64` into `run/scope.py` | small | Makes domain logic testable and reusable from run steps |
| 4 | P19 | Simplify `telemetry.py:60-155` by extracting event builder helper | small | Reduces nested try/except complexity |
| 5 | P21 | Add `NeighborhoodCommandSchema` to follow schema-driven params pattern | small | Eliminates the one inconsistent command definition |

## Recommended Action Sequence

1. **Consolidate `schema_projection.py`** (P7, P19): Replace 11 function pairs with generic `project_params(params, schema_type)` and `options_from_params(params, schema_type, options_type)`. This is safe, self-contained, and eliminates the most visible boilerplate. Update callers in `commands/analysis.py`, `commands/search.py`, `commands/query.py`, `commands/report.py`, and `commands/run.py`.

2. **Extract shared scope-filter logic** (P7): Create `tools/cq/core/scope_filter.py` with a `filter_result_by_scope()` function. Update `tools/cq/run/step_executors.py:383-424` and `tools/cq/orchestration/bundles.py:166-206` to delegate to it.

3. **Extract domain logic from shell commands** (P2, P16): Move `--in` dir-to-glob conversion into `tools/cq/run/scope.py`. Move query-search fallback routing into `tools/cq/query/router.py`. This unblocks testing domain logic without CLI infrastructure.

4. **Introduce ConsolePort and thread through CliContext** (P1, P12, P23): Define `ConsolePort` protocol. Add it to `CliContext`. Replace `from tools.cq.cli_app.app import console` in `result_render.py` and `result_action.py` with context-based access. This is the highest-impact testability improvement.

5. **Decompose `tree_sitter_collector.py`** (P3): Extract anchor resolution, relationship collection, and slice building into separate modules. The top-level function becomes a thin orchestrator. This requires more effort but significantly improves navigability.

6. **Decompose `multilang_orchestrator.py`** (P3): Extract semantic telemetry aggregation into `multilang_telemetry.py` and front-door insight selection into `multilang_insight.py`. Depends on step 5 being complete to establish the decomposition pattern.

7. **Split `handle_result()` into preparation and emission** (P11): Create `prepare_output(cli_result) -> PreparedOutput` (pure) and `emit_prepared_output(prepared) -> None` (side-effectful). This makes the result pipeline testable without actual terminal output. Depends on step 4 (ConsolePort) being in place.

8. **Normalize `NeighborhoodParams` to schema-driven pattern** (P7, P21): Add `NeighborhoodCommandSchema` to `command_schema.py` and generate `NeighborhoodParams` via `_build_params_class()`. This is a consistency improvement that depends on step 1 being complete.
