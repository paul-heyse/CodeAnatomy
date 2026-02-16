# Design Review: tools/cq CLI, Execution, and Peripheral Subsystems

**Date:** 2026-02-15
**Scope:** `tools/cq/cli_app/`, `tools/cq/run/`, `tools/cq/neighborhood/`, `tools/cq/ldmd/`, `tools/cq/astgrep/`, `tools/cq/utils/`, `tools/cq/perf/`
**Focus:** All principles (1-24)
**Depth:** moderate
**Files reviewed:** 20 of 52 total (entry points, orchestrators, public contracts, data models)

## Executive Summary

The CLI and execution subsystems demonstrate strong architectural separation between user-facing CLI dispatch and backend analysis logic, with well-designed typed boundaries (cyclopts dataclasses to msgspec structs via `convert_strict`). The multi-step execution engine (`run/runner.py`) is the most complex module in scope and carries the highest design debt, particularly around its 1,297-LOC orchestrator that mixes batching strategy, language expansion, result collapsing, and step dispatch. The LDMD and neighborhood subsystems are clean, focused modules with good contract stability. The primary improvement opportunities are: (1) extracting the runner's batch/dispatch/collapse responsibilities into cohesive sub-modules, (2) replacing `dict[str, object]` summary payloads with typed structs, and (3) standardizing the two redundant context-injection mechanisms (`require_ctx` decorator vs `require_context` function).

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | LDMD `_is_collapsed` imports private names from neighborhood module |
| 2 | Separation of concerns | 1 | large | high | `runner.py` (1,297 LOC) mixes 5+ concerns |
| 3 | SRP | 1 | large | high | `runner.py` changes for batch strategy, step dispatch, scope filtering, result collapsing, and telemetry aggregation |
| 4 | High cohesion, low coupling | 2 | medium | medium | CLI types well cohesive; runner couples to 15+ modules |
| 5 | Dependency direction | 2 | small | low | CLI properly depends on core; minor inward leak in LDMD |
| 6 | Ports & Adapters | 2 | medium | low | Step dispatch via dict is adapter-like; no formal port protocol |
| 7 | DRY | 2 | small | low | Dual context-injection patterns; minor summary key duplication |
| 8 | Design by contract | 2 | small | medium | RunPlan validated; CqResult.summary untyped dict lacks contracts |
| 9 | Parse, don't validate | 3 | - | - | Typed boundary protocol (`convert_strict`) at all CLI boundaries |
| 10 | Illegal states | 2 | medium | medium | `CliResult.result: Any` allows impossible combinations |
| 11 | CQS | 2 | small | low | `handle_result` both renders and mutates result artifacts |
| 12 | DI + explicit composition | 2 | medium | medium | `resolve_runtime_services` called at multiple scattered sites |
| 13 | Composition over inheritance | 3 | - | - | Params use dataclass inheritance minimally; step types use tagged unions |
| 14 | Law of Demeter | 2 | small | low | Chain step builders use `getattr(opts, ...)` chains |
| 15 | Tell, don't ask | 2 | small | low | `result.summary.get(...)` scattered throughout runner |
| 16 | Functional core, imperative shell | 2 | medium | medium | Bundle builder is pure; runner mixes IO and transforms |
| 17 | Idempotency | 3 | - | - | Run plans are stateless specifications; re-execution is safe |
| 18 | Determinism | 3 | - | - | Bundle IDs are SHA256-deterministic; step IDs are index-based |
| 19 | KISS | 2 | medium | medium | Chain command adds complexity for marginal benefit over `run --steps` |
| 20 | YAGNI | 2 | small | low | `plan_feasible_slices` exists but unused by bundle builder |
| 21 | Least astonishment | 2 | small | medium | `--no-semantic-enrichment` flag name contradicts `NeighborhoodStep.no_semantic_enrichment` |
| 22 | Public contracts | 2 | medium | medium | LDMD format lacks version marker; CqResult.summary unversioned |
| 23 | Testability | 2 | medium | medium | Runner hard to unit test due to 15+ import dependencies |
| 24 | Observability | 2 | small | low | Telemetry events structured; no trace correlation across steps |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Most modules expose clean public APIs via `__all__`. The LDMD subsystem has an information-hiding violation where `format.py` imports private names from `neighborhood/section_layout.py`.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py:62-63` imports `_DYNAMIC_COLLAPSE_SECTIONS` and `_UNCOLLAPSED_SECTIONS` (private names with underscore prefix) from `neighborhood.section_layout`. This creates a hidden dependency from the LDMD parser into neighborhood internals.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:79-85` exposes `RUN_STEP_NON_FATAL_EXCEPTIONS` as a module-level constant but it is an implementation detail of the error boundary strategy.

**Suggested improvement:**
Export a public `is_section_collapsed(section_id: str) -> bool` function from `neighborhood/section_layout.py` and have `format.py` call that instead of importing private constants. This decouples the LDMD parser from the neighborhood module's internal data layout.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
The CLI layer (`cli_app/`) cleanly separates parameter parsing, context construction, and result rendering into distinct modules. However, `runner.py` is a 1,297-LOC file that combines at least five distinct concerns.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:99-157` (`execute_run_plan`) orchestrates: step partitioning, Q-step batch execution, non-Q parallel/serial dispatch, summary metadata synthesis, cache eviction, and finding ID assignment -- all in one function.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:160-258` contains telemetry aggregation logic (`_aggregate_run_semantic_telemetry`, `_select_run_semantic_planes`, `_semantic_plane_signal_score`) that is a separate concern from step execution.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:261-338` contains run-level scope/metadata derivation logic (`_derive_run_summary_metadata`, `_derive_run_scope_metadata`, `_derive_scope_from_orders`) that is a separate concern from both telemetry and execution.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:955-999` contains 8+ executor functions (`_execute_calls`, `_execute_impact`, etc.) that each follow an identical pattern: build `RequestContextV1`, call `RequestFactory`, invoke macro. These are adapters that should be co-located.

**Suggested improvement:**
Extract `runner.py` into four focused modules: (1) `runner.py` -- thin orchestrator calling the others, (2) `q_step_batch.py` -- Q-step batching, language expansion, and result collapsing, (3) `step_executors.py` -- the `_NON_SEARCH_DISPATCH` table and all `_execute_*` functions, (4) `run_summary.py` -- metadata derivation and telemetry aggregation.

**Effort:** large
**Risk if unaddressed:** high -- the file will continue to grow as new step types and metadata concerns are added, making it increasingly difficult to modify safely.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
Most modules in CLI and peripheral subsystems have clear single responsibilities. The primary violation is `runner.py`.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py` changes for at least five independent reasons: (1) new step type added, (2) batch optimization strategy changes, (3) summary metadata format changes, (4) scope filtering logic changes, (5) parallelism strategy changes. Each of these is a separate axis of change.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py` has two distinct responsibilities: (1) result rendering dispatch (lines 99-140) and (2) artifact persistence (lines 247-382). These change for different reasons (new output formats vs new artifact types).

**Suggested improvement:**
For `result.py`, extract `_handle_artifact_persistence`, `_save_search_artifacts`, and `_save_general_artifacts` into a dedicated `artifact_persistence.py` module. For `runner.py`, see P2 suggestion.

**Effort:** large (runner), medium (result)
**Risk if unaddressed:** high -- mixed responsibilities mean any change risks breaking unrelated behavior.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The CLI type system (`types.py`, `options.py`, `params.py`) is highly cohesive. The run subsystem has high fan-out coupling.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:1-77` imports from 15+ distinct modules spanning `cli_app`, `core`, `query`, `search`, and `run` packages. This makes the module difficult to understand or test in isolation.
- `/Users/paulheyse/CodeAnatomy/tools/cq/neighborhood/bundle_builder.py` has tight cohesion: all functions relate to SNB assembly. Coupling is low -- it imports only from `core/schema`, `core/snb_schema`, `neighborhood/contracts`, and `neighborhood/tree_sitter_collector`.

**Suggested improvement:**
Reduce runner's import surface by extracting step executors (which import macro modules) into a separate module with lazy imports. The runner itself should only depend on step specs, batch infrastructure, and result merging.

**Effort:** medium
**Risk if unaddressed:** medium -- high coupling makes refactoring risky and increases merge conflicts.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency direction is generally correct: CLI depends on core, core does not depend on CLI. There is one minor inversion.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py:62-63` reaches into `neighborhood/section_layout` for collapse policy. LDMD is a generic format module; it should not depend on neighborhood-specific layout decisions.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py:206` imports `console` from `cli_app/app.py` at function call time. This is a dependency from a "lower" module (result rendering) back up to the "higher" module (app configuration). The console should be injected or resolved via a service locator.

**Suggested improvement:**
For the LDMD/neighborhood coupling, inject the collapse policy as a callback parameter to `build_index()` or use a configurable default. For the console dependency, pass `console` as a parameter to `_emit_output` and `_handle_non_cq_result` instead of importing it from `app.py`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The step dispatch pattern in `runner.py` approximates an adapter architecture with `_NON_SEARCH_DISPATCH` acting as an adapter registry. However, there is no formal port protocol.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:825` uses a `dict[type, Callable[..., CqResult]]` dispatch table which is structurally similar to a port/adapter pattern but lacks a formal protocol definition. The callable signatures are inconsistent -- some take `(step, ctx)`, while `SearchStep` and `NeighborhoodStep` take additional parameters (`plan`, `run_id`).
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py:124-140` uses a renderer dispatch dict with `dict[str, Callable[[CqResult], str]]`, which is a clean adapter pattern for output formats. The LDMD format breaks the pattern with a special-cased lazy import.

**Suggested improvement:**
Define a `StepExecutor` protocol: `Protocol` with `def execute(step: RunStep, ctx: CliContext, *, run_id: str, plan: RunPlan) -> CqResult`. Register all executors conforming to this protocol. This would also make new step types self-contained.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY -- Alignment: 2/3

**Current state:**
The codebase avoids gratuitous duplication. The primary DRY concern is dual context-injection patterns.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/infrastructure.py:81-96` defines `require_ctx` (decorator) and `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/infrastructure.py:99-108` defines `require_context` (function). Both enforce the same invariant (context must be a CliContext), but commands use both: the decorator on the function definition AND `require_context(ctx)` as the first line of the body. This is redundant validation of the same truth.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/neighborhood.py:62` calls `ctx = require_context(ctx)` after `@require_ctx` already validated it. This double-check pattern is repeated across all command handlers.

**Suggested improvement:**
Choose one mechanism. Since `@require_ctx` validates at decorator level and `require_context` narrows the type from `CliContext | None` to `CliContext`, keep `require_context` as the idiomatic narrowing function and remove the `@require_ctx` decorator, or have `@require_ctx` inject a guaranteed non-None context so the manual call is unnecessary.

**Effort:** small
**Risk if unaddressed:** low

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Boundary contracts are strong: `RunPlan` and `RunStep` use msgspec tagged unions with schema validation, and `convert_strict` enforces typed boundaries. The weakness is `CqResult.summary`.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:115-116` writes `merged.summary["plan_version"] = plan.version` into an untyped `dict[str, object]`. The summary dict has no schema, no required keys, and no validation. Code throughout the runner reads from it with `.get()` calls and `isinstance` checks.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:166-189` in `_populate_run_summary_metadata` uses `setdefault` to populate summary keys, relying on string-key conventions with no compile-time or runtime contract.
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/writer.py:192-193` accesses `result.summary.get("front_door_insight")` and must runtime-check whether it is a `FrontDoorInsightV1` or a `dict`.

**Suggested improvement:**
Define a `RunSummaryV1(msgspec.Struct)` with typed fields for the known summary keys (`query`, `mode`, `lang_scope`, `language_order`, `plan_version`, `semantic_planes`, etc.). Use it as the type for `CqResult.summary` in run contexts, or at minimum define it as a validation target that `_populate_run_summary_metadata` constructs and serializes.

**Effort:** medium
**Risk if unaddressed:** medium -- untyped summaries are the primary source of runtime `isinstance` checks and will accumulate more string-key conventions over time.

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
The typed boundary protocol (`convert_strict`, `decode_json_strict`, `decode_toml_strict`) is consistently applied at all CLI boundaries.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/step_decode.py:9-44` uses `decode_json_strict(raw, type_=RunStep)` to parse JSON into typed step unions in a single pass.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/options.py:119-127` uses `convert_strict(params, type_=T, from_attributes=True)` to transform CLI dataclasses into typed option structs at the boundary.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/loader.py:52-53` uses `decode_toml_strict(payload, type_=RunPlan)` for plan file loading.

This is exemplary application of parse-don't-validate.

**Effort:** -
**Risk if unaddressed:** -

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
The `RunStep` tagged union and `OutputFormat` enum make many illegal states unrepresentable. However, `CliResult` has a weak type for its core payload.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/context.py:165` defines `result: Any` on `CliResult`. This allows any value, requiring runtime `isinstance` checks in `is_cq_result` (line 173) and `_handle_non_cq_result` (result.py:219-234). The actual invariant is that `result` is one of `CqResult | CliTextResult | int`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:897-899` validates `step.regex and step.literal` at runtime for `SearchStep`, but the step spec allows both to be `True` simultaneously. This could be prevented at the type level with a discriminated union or validator on the struct.

**Suggested improvement:**
Replace `CliResult.result: Any` with a discriminated union: `result: CqResult | CliTextResult | int`. For `SearchStep`, add a msgspec `__post_init__` validator or use mutually exclusive fields.

**Effort:** medium
**Risk if unaddressed:** medium -- `Any` types erode type safety and require defensive runtime checks.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. The primary violation is in the result handling pipeline.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py:143-197` `handle_result` both queries the result (rendering) and commands state changes (artifact persistence, finding ID assignment at line 184). It also mutates the result in-place via `assign_result_finding_ids(result)`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py:295-296` `_save_general_artifacts` mutates `result.artifacts.append(artifact)` as a side effect of what reads like a save operation.

**Suggested improvement:**
Split `handle_result` into `prepare_result` (pure: filter + assign IDs, returns new result) and `emit_result` (imperative: persist artifacts + render output). This makes the pure transform testable independently.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
The system uses `resolve_runtime_services` for service composition, but it is called at multiple scattered sites rather than injected once.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:929` calls `resolve_runtime_services(ctx.root)` inside `_execute_search_step`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:959` calls it again inside `_execute_calls`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:951` calls it a third time inside `_execute_search_fallback`.
- Each of these resolves the same service root. The resolution should happen once at the beginning of `execute_run_plan` and be threaded through as a parameter.

**Suggested improvement:**
Resolve services once in `execute_run_plan` and pass the resolved `RuntimeServices` instance to all executor functions. This makes the dependency explicit and eliminates redundant resolution.

**Effort:** medium
**Risk if unaddressed:** medium -- scattered service resolution makes it difficult to swap implementations for testing and creates hidden coupling to the global service locator.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The codebase strongly favors composition. Step types use tagged union discrimination (composition) rather than inheritance-based dispatch. Parameter groups use minimal dataclass inheritance (`QueryParams(FilterParams)`) which is appropriate for extending a shared field set.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/spec.py:22-33` uses `RunStepBase` as a base with tagged union discrimination, not polymorphic dispatch. Each step type is a flat data struct.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/params.py:80-151` `FilterParams` is extended by `QueryParams`, `SearchParams`, etc. This is shallow inheritance (1 level) used for field composition, which is idiomatic for dataclasses.

No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code accesses direct collaborators. Chain step builders use `getattr` chains that reach through untyped intermediaries.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/chain.py:129-133` accesses `getattr(opts, "regex", False)`, `getattr(opts, "literal", False)`, `getattr(opts, "include_strings", False)`, `getattr(opts, "in_dir", None)` -- reaching through an untyped `opts: object | None`. This is a Demeter violation because the builder must know the internal structure of an opaque object.
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/writer.py:53-56` uses `getattr(subject, "name", "<unknown>")`, `getattr(subject, "file_path", "")`, etc. on `bundle: object`. The writer accepts `object` type but reaches into its internal attributes.

**Suggested improvement:**
For the chain builders, type `opts` as the specific params dataclass type (e.g., `SearchParams`) instead of `object | None`. For the LDMD writer, type the `bundle` parameter as `SemanticNeighborhoodBundleV1` instead of `object`, eliminating all `getattr` calls.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The runner extensively queries `result.summary.get(...)` to extract metadata, rather than having the result object encapsulate its own metadata access.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:860` checks `result.summary.get("error")` to detect step failures. The result should expose an `is_error` property.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:166-167` checks `merged.summary.get("query")` and `merged.summary.get("mode")` to determine whether to derive metadata. The summary dict is queried rather than being told what to do.

**Suggested improvement:**
Add `is_error: bool` property to `CqResult` and use it instead of `summary.get("error")`. For summary metadata, see P8 suggestion for typed summary struct.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The neighborhood bundle builder (`bundle_builder.py`) is a good example of functional core: `_merge_slices`, `_generate_bundle_id`, and `_build_graph_summary` are pure transforms. The runner mixes IO and transforms.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/neighborhood/bundle_builder.py:92-142` `build_neighborhood_bundle` is a clean pipeline: collect -> merge -> summarize -> store artifacts. The only side effect is artifact storage, which is isolated at the end.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:99-157` `execute_run_plan` interleaves pure transforms (metadata derivation) with IO (cache eviction, service resolution). The function body cannot be tested without mocking multiple external services.
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py:76-151` `build_index` is a pure function: takes bytes, returns index. This is excellent.

**Suggested improvement:**
Extract the pure summary-derivation logic from `execute_run_plan` into a separate function that takes `list[tuple[str, CqResult]]` and returns a metadata dict. Keep IO (cache eviction, service resolution) in the shell.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Run plans are stateless specifications. Re-executing the same plan with the same inputs produces equivalent results. Cache eviction uses run-scoped tags that are idempotent.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:155-156` `maybe_evict_run_cache_tag` is idempotent by design -- evicting an already-evicted tag is a no-op.
- `/Users/paulheyse/CodeAnatomy/tools/cq/neighborhood/bundle_builder.py:199-202` generates deterministic bundle IDs via SHA256 of target coordinates, ensuring repeated builds produce the same ID.

No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Bundle IDs, step IDs, and section ordering are all deterministic.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/spec.py:198-213` `normalize_step_ids` assigns deterministic IDs based on step type and index position.
- `/Users/paulheyse/CodeAnatomy/tools/cq/neighborhood/bundle_builder.py:199-202` uses `hashlib.sha256` for deterministic bundle ID generation.
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py:76-151` `build_index` produces deterministic indices from the same input bytes.

No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most subsystems are appropriately simple. The chain command and the runner's language-expansion/collapsing machinery add significant complexity.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/chain.py:69-105` `compile_chain_segments` re-parses each segment through the full CLI parser (`app.parse_known_args`), which adds complexity and fragility. The chain command essentially duplicates `run --steps` functionality with a different syntax.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:802-816` result collapsing for multi-language Q-steps (`_collapse_parent_q_results`) adds complexity to support `lang=auto` expansion. This is necessary but the implementation could be simpler if extracted to a dedicated module.
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py` is an example of good KISS: 377 LOC with clean separation of index building, slice extraction, and search.

**Suggested improvement:**
Document the chain command as a convenience layer over `run --steps` and consider whether its marginal benefit justifies the maintenance cost of the full-CLI-reparse approach. If kept, the token parsing should be simplified.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most code serves a clear purpose. One function appears to be speculative infrastructure.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/neighborhood/bundle_builder.py:48-89` `plan_feasible_slices` exists as exported public API but is not called by `build_neighborhood_bundle` or any other code in the scope. It appears to be speculative infrastructure for a future capability-negotiation system.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/contracts.py` is a 5-LOC placeholder file. While small, it signals a planned but unbuilt feature.

**Suggested improvement:**
Mark `plan_feasible_slices` as internal (`_plan_feasible_slices`) or remove it until a caller exists. The `contracts.py` placeholder is harmless but should not grow without a concrete need.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
CLI naming is generally good. There is one naming inconsistency between the CLI flag and the step spec.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/neighborhood.py:41-49` uses `--semantic-enrichment` / `--no-semantic-enrichment` (positive form).
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/spec.py:113` defines `no_semantic_enrichment: bool = False` (negative form). A user seeing `{"type":"neighborhood","no_semantic_enrichment":true}` in a run plan would expect the CLI equivalent to be `--no-semantic-enrichment`, but the mapping is inverted: the step field is `no_semantic_enrichment` while the CLI flag is `--semantic-enrichment`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/types.py:226-248` has docstrings with formatting artifacts: `"Parameters\n        ----------\n\n        Args:"` mixing NumPy and Google style within the same docstring.

**Suggested improvement:**
Rename `NeighborhoodStep.no_semantic_enrichment` to `enable_semantic_enrichment: bool = True` to match the CLI flag's positive form. Fix the docstring formatting in `types.py`.

**Effort:** small
**Risk if unaddressed:** medium -- naming inconsistencies between CLI and run-plan specs confuse users writing TOML plans.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
The SNB schema is versioned (`schema_version="cq.snb.v1"`). The LDMD format lacks a version marker.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py:13-21` defines marker regex patterns but no version attribute in the marker grammar. The BEGIN marker has `id`, `title`, `level`, `parent`, `tags` but no `version`. If the marker format evolves, old parsers will fail silently.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/snb_schema.py` (referenced) defines `schema_version: str = "cq.snb.v1"` -- good versioning practice.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/spec.py:14` `RunPlan` has `version: int = 1` -- good.
- `CqResult.summary` has no versioning or schema declaration.

**Suggested improvement:**
Add an optional `version` attribute to the LDMD BEGIN marker: `<!--LDMD:BEGIN id="..." version="1" ...-->`. The parser should check version compatibility and emit a clear error when encountering unsupported versions.

**Effort:** medium
**Risk if unaddressed:** medium -- format evolution without versioning risks silent breakage for LDMD consumers.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The LDMD parser and neighborhood bundle builder are highly testable (pure functions). The runner and result handler have testing challenges.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:1-77` imports from 15+ modules at the top level, making it impossible to instantiate the module without all dependencies. Testing any individual function requires the entire dependency tree.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py:164` imports `pop_search_object_view_for_run` from the search pipeline at function call time, creating a hidden dependency that must be mocked for testing.
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py:76-151` `build_index` is a pure function taking bytes and returning an index -- excellent testability.
- `/Users/paulheyse/CodeAnatomy/tools/cq/neighborhood/bundle_builder.py:157-196` `_merge_slices` is a pure function -- excellent testability.

**Suggested improvement:**
Extract the pure logic in `runner.py` (metadata derivation, scope merging, result collapsing) into separate modules that can be tested without importing macro modules. Use lazy imports for macro executor functions.

**Effort:** medium
**Risk if unaddressed:** medium -- difficulty testing the runner leads to reliance on integration tests, which are slower and less precise.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The telemetry module provides structured invocation events. Step-level observability within runs is limited.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/telemetry.py:22-34` `CqInvokeEvent` captures parse/exec timing, error classification, and event identity. This is well-structured.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:160-189` aggregates telemetry from steps but does not emit per-step timing events. There is no correlation ID linking a step's execution time to the parent run event.
- `/Users/paulheyse/CodeAnatomy/tools/cq/neighborhood/bundle_builder.py:100-101` captures `started = ms()` but only stores `created_at_ms` as elapsed time. No trace ID links the bundle build to the run invocation.

**Suggested improvement:**
Add `step_id` and `run_id` to per-step execution telemetry. Emit a `StepInvokeEvent` for each step with timing and error information, linked to the parent `CqInvokeEvent` via `run_id`.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: runner.py as a Concentration Risk

**Description:** `runner.py` at 1,297 LOC is the single largest module in scope and concentrates five distinct concerns: step dispatch, batch optimization, language expansion/collapsing, metadata synthesis, and telemetry aggregation. Nearly every design principle scores lower because of this file -- it is the root cause of SRP (P3), separation of concerns (P2), testability (P23), and coupling (P4) gaps.

**Root cause:** The runner grew organically as new step types and cross-language features were added. Each feature was added inline rather than extracted to a dedicated module.

**Affected principles:** P2, P3, P4, P12, P16, P23

**Suggested approach:** Decompose into 4 focused modules (see P2 suggestion) behind a thin orchestrator. This single change would improve alignment on 6 principles simultaneously.

### Theme 2: Untyped Summary Dict

**Description:** `CqResult.summary: dict[str, object]` is the system's primary metadata carrier, but it has no schema, no required keys, and no versioning. Code throughout the runner, LDMD writer, and result handler accesses it via string keys with `isinstance` guards.

**Root cause:** The summary was originally a flexible escape hatch for command-specific metadata. As the system matured, it accumulated de facto required fields without formalizing them.

**Affected principles:** P8, P10, P15, P22

**Suggested approach:** Define typed summary structs for each macro type (RunSummaryV1, SearchSummaryV1, etc.) and use them as the construction target. The dict representation can remain for serialization, but construction and access should go through typed APIs.

### Theme 3: Dual Context Injection

**Description:** Commands use both `@require_ctx` decorator and `require_context(ctx)` function call to validate the same invariant. This creates confusion about which mechanism is authoritative and leads to redundant validation.

**Root cause:** Both patterns were introduced for different reasons -- the decorator for framework-level validation and the function for type narrowing -- but they overlap completely.

**Affected principles:** P7, P19

**Suggested approach:** Consolidate to `require_context(ctx)` as the single mechanism, since it provides both validation and type narrowing. Remove `@require_ctx` decorator.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7/P19 | Remove dual context-injection (`@require_ctx` + `require_context`) -- pick one | small | Eliminates confusion and redundant validation across all 12 command handlers |
| 2 | P21 | Rename `NeighborhoodStep.no_semantic_enrichment` to `enable_semantic_enrichment` | small | Eliminates naming inconsistency between CLI and run-plan specs |
| 3 | P1 | Export public `is_section_collapsed()` from `section_layout.py` instead of importing privates | small | Decouples LDMD parser from neighborhood internals |
| 4 | P11 | Split `handle_result` into pure `prepare_result` and imperative `emit_result` | small | Makes result preparation independently testable |
| 5 | P20 | Mark `plan_feasible_slices` as internal or remove until called | small | Reduces public API surface of unused code |

## Recommended Action Sequence

1. **Consolidate context injection (P7/P19):** Remove `@require_ctx` decorator, keep `require_context()`. Update all 12 command handlers. This is the simplest change with the widest impact. (small effort)

2. **Fix naming inconsistency (P21):** Rename `NeighborhoodStep.no_semantic_enrichment` to `enable_semantic_enrichment: bool = True`. Update runner's neighborhood executor to invert the flag. (small effort)

3. **Export collapse policy function (P1/P5):** Add `is_section_collapsed(section_id: str) -> bool` to `section_layout.py`'s public API. Update `format.py` to call it instead of importing private constants. (small effort)

4. **Split handle_result (P11):** Extract pure result preparation into `prepare_result()`. Keep artifact persistence and console emission in `emit_result()`. (small effort)

5. **Type CliResult.result (P10):** Replace `result: Any` with `result: CqResult | CliTextResult | int`. Update `is_cq_result` and `_handle_non_cq_result` to use match statements. (medium effort, depends on core schema changes)

6. **Extract step executors from runner (P2/P3/P23):** Move `_execute_calls`, `_execute_impact`, etc. and `_NON_SEARCH_DISPATCH` into `tools/cq/run/step_executors.py`. This reduces runner.py by ~200 LOC and isolates macro imports. (medium effort)

7. **Extract run summary derivation (P2/P16):** Move `_populate_run_summary_metadata`, `_aggregate_run_semantic_telemetry`, `_select_run_semantic_planes`, and `_derive_*` functions into `tools/cq/run/run_summary.py`. This reduces runner.py by ~200 LOC and makes metadata logic independently testable. (medium effort)

8. **Resolve services once (P12):** In `execute_run_plan`, call `resolve_runtime_services(ctx.root)` once and thread the result to all executor functions. (medium effort, depends on step 6)

9. **Add LDMD version marker (P22):** Extend LDMD BEGIN marker grammar with optional `version` attribute. Update parser to check compatibility. (medium effort)

10. **Type CqResult.summary (P8/P15/P22):** Define `RunSummaryV1`, `SearchSummaryV1`, etc. as typed structs. Migrate `_populate_run_summary_metadata` to construct typed structs. (large effort, systemic change)
