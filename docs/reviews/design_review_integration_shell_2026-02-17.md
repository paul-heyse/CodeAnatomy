# Design Review: Integration Shell

**Date:** 2026-02-17
**Scope:** `tools/cq/cli_app/`, `tools/cq/macros/`, `tools/cq/run/`, `tools/cq/orchestration/`, `tools/cq/neighborhood/`, `tools/cq/ldmd/`, `tools/cq/perf/`
**Focus:** All principles (1-24)
**Depth:** moderate
**Files reviewed:** 20 of 88

## Executive Summary

The Integration Shell layer demonstrates strong boundary discipline at its edges -- CLI commands are thin, request factories centralize construction, and macro entry points follow a consistent `Request -> CqResult` contract. The main structural weakness is that **macros contain substantial domain logic** (taint analysis, AST walking, signature parsing) that should arguably live in lower layers, making the "imperative shell" thicker than expected. Composition patterns are generally sound, with `RequestFactory` providing a clean single-authority for request construction and a dispatch-table strategy for run-step execution. The most impactful improvements involve (1) extracting pure analysis logic from macros into testable core modules, (2) replacing CqResult mutation patterns with builder-based immutable construction, and (3) eliminating the module-global mutable `app.config` reassignment in the CLI bootstrap path.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Module-global `console`/`error_console` in `app.py`; `_build_launch_context` mutates `app.config` |
| 2 | Separation of concerns | 1 | large | high | Macros embed domain logic (taint analysis, AST walking) mixed with result construction |
| 3 | SRP (one reason to change) | 2 | medium | medium | `calls/entry.py` orchestrates scan + analysis + neighborhood + semantic + insight + result |
| 4 | High cohesion, low coupling | 2 | medium | medium | `calls/entry.py` imports from 15+ modules; params/options duplication across layers |
| 5 | Dependency direction | 2 | small | low | Shell modules correctly depend inward; minor reverse dependency in `ldmd/format.py` |
| 6 | Ports & Adapters | 2 | medium | low | `RequestFactory` acts as adapter; filesystem IO in macros lacks port abstraction |
| 7 | DRY (knowledge) | 1 | medium | medium | Params-Options dual hierarchy duplicates field definitions for every command |
| 8 | Design by contract | 2 | small | low | Request structs enforce preconditions via frozen msgspec; postconditions implicit |
| 9 | Parse, don't validate | 2 | small | low | `RunStep` tagged union + `parse_run_step_json` parse at boundary; `in_dir` re-parsed in commands |
| 10 | Illegal states | 2 | small | low | `RunStep` tagged union prevents invalid step types; `CliContext.build` allows both `options` and `**kwargs` |
| 11 | CQS | 1 | medium | medium | `_build_launch_context` mutates `app.config` while returning a value; macros mutate `result` lists |
| 12 | DI + explicit composition | 2 | small | low | `CliContext` injects services/toolchain; `require_context` enforces at command entry |
| 13 | Composition over inheritance | 3 | - | - | Minimal inheritance; `RunStepBase` tagged union is appropriate; `FilterParams` inheritance shallow |
| 14 | Law of Demeter | 2 | small | low | `ctx.services.search.execute(request)` is a two-dot chain; `result.summary.front_door_insight` common |
| 15 | Tell, don't ask | 2 | medium | medium | Macros reach into `result.key_findings`, `result.sections` directly rather than asking result to add |
| 16 | Functional core / imperative shell | 1 | large | high | Macros mix pure analysis (taint, signature parsing) with IO (file reads) and result mutation |
| 17 | Idempotency | 2 | small | low | Macros are effectively idempotent given same inputs; `maybe_evict_run_cache_tag` is safe |
| 18 | Determinism / reproducibility | 2 | small | low | `normalize_step_ids` provides deterministic IDs; `uuid7_str` run_ids are non-deterministic but traced |
| 19 | KISS | 2 | small | low | CLI commands are lean; `telemetry.py` has deep nested try/except but manageable |
| 20 | YAGNI | 2 | small | low | `EXPANDERS` dict in `expand.py` has 7 identity entries -- potential over-registration |
| 21 | Least astonishment | 2 | small | low | `--in` flag vs `in_dir` naming mismatch; `step` vs `steps` both accept lists |
| 22 | Public contracts | 2 | small | low | `VERSION = "0.4.0"` declared; `RunPlan.version` versioned; `__all__` exports mostly present |
| 23 | Design for testability | 1 | medium | high | Macro domain logic tied to filesystem reads; `CliContext.build` calls `Toolchain.detect()` |
| 24 | Observability | 2 | small | low | Structured logging in `runner.py` and `step_executors.py`; telemetry events in CLI invocation |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Module-global singletons `console` and `error_console` are created at import time in `tools/cq/cli_app/app.py:153-154` and used by `result_action.py:58`. The `_build_launch_context` function at `app.py:176` mutates the module-global `app.config` on line 181, exposing configuration internals across the module boundary.

**Findings:**
- `tools/cq/cli_app/app.py:153-154` -- Module-global `console` and `error_console` singletons created at import time, preventing configuration changes and making test isolation difficult.
- `tools/cq/cli_app/app.py:181` -- `_build_launch_context` mutates `app.config` as a side effect while also returning a `LaunchContext` value, leaking configuration mutation through a function that looks like a pure constructor.
- `tools/cq/cli_app/result_action.py:58` -- `_handle_non_cq_result` imports `console` from `app.py`, creating a reverse import from result handling back into the app module.

**Suggested improvement:**
Move console creation into `CliContext` or pass consoles as parameters through the result-action pipeline. Eliminate the `app.config` mutation by building the config chain once in `launcher()` and passing it through without side effects.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
The macros layer (`tools/cq/macros/`) is intended as an integration shell that composes lower-level analysis, but several macros contain substantial domain logic interleaved with IO operations and result construction.

**Findings:**
- `tools/cq/macros/impact.py:135-427` -- The `TaintVisitor` class (290 lines) is a full AST-walking taint analysis engine with 13 expression-type handlers in `_EXPR_TAINT_HANDLERS`. This is pure domain logic embedded in the shell layer, mixed with filesystem reads at line 505 (`filepath.read_text`).
- `tools/cq/macros/sig_impact.py:69-149` -- `_parse_signature` is a standalone signature parser using `ast.parse`, pure logic that belongs in a lower-level analysis module rather than the shell.
- `tools/cq/macros/calls/entry.py:103-178` -- `_scan_call_sites` mixes file enumeration (IO) with AST-based call-site collection and ripgrep fallback logic in a single 75-line function.
- `tools/cq/macros/impact.py:484-540` -- `_analyze_function` reads files from disk (line 505), parses AST, runs the taint visitor, and recursively propagates -- mixing IO, parsing, analysis, and recursion in one function.

**Suggested improvement:**
Extract `TaintVisitor` and `_EXPR_TAINT_HANDLERS` into a `tools/cq/analysis/taint.py` module that accepts pre-parsed AST nodes and returns taint results without performing IO. Extract `_parse_signature` into `tools/cq/analysis/signature.py`. The macro entry points should then compose: read files -> parse -> analyze (pure) -> build result.

**Effort:** large
**Risk if unaddressed:** high -- Domain logic in the shell layer resists unit testing, and changes to taint analysis require modifying the integration layer.

---

#### P3. SRP -- Alignment: 2/3

**Current state:**
Most CLI commands follow a clean single-responsibility pattern: parse options -> build request -> execute -> wrap result. However, macro entry points accumulate orchestration duties.

**Findings:**
- `tools/cq/macros/calls/entry.py:427-469` -- `_build_calls_result` coordinates scan initialization, analysis, front-door state construction, confidence computation, insight summary, semantic enrichment with telemetry, finalization, and front-door rendering -- 8 distinct concerns in one function.
- `tools/cq/cli_app/commands/query.py:53-89` -- `q` command handles both structured query execution and search fallback, embedding the fallback routing decision that could be a separate dispatcher.

**Suggested improvement:**
Split `_build_calls_result` into a pipeline of named stages: `init -> analyze -> enrich -> render`. Each stage should be a separate function that takes the previous stage's output. The query fallback logic in `q` could use a dedicated `resolve_query_or_search` function.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The `calls/entry.py` module imports from 15+ distinct modules spanning scanning, analysis, insight, neighborhood, semantic, target resolution, constants, contracts, result building, and rust fallback. This high fan-in suggests the module is doing too much.

**Findings:**
- `tools/cq/macros/calls/entry.py:1-68` -- 22 import statements pulling from `core.cache`, `core.schema`, `core.scoring`, `core.summary_contract`, `macros.calls.analysis`, `macros.calls.insight`, `macros.calls.neighborhood`, `macros.calls.scanning`, `macros.calls.semantic`, `macros.calls_target`, `macros.constants`, `macros.contracts`, `macros.result_builder`, `macros.rust_fallback_policy`, `query.sg_parser`, `search.pipeline.profiles`, and `search.rg.adapter`.
- `tools/cq/cli_app/params.py` and `tools/cq/cli_app/options.py` -- Each command type has a parallel Params dataclass (CLI-specific with `Annotated[..., Parameter()]`) and an Options struct (msgspec-based). The field names and types are duplicated, creating tight coupling between these two modules.

**Suggested improvement:**
For the params/options duplication: consider generating Options from Params (or vice versa) using a single source of truth. For `calls/entry.py`: the sub-modules (`calls.analysis`, `calls.insight`, `calls.neighborhood`, `calls.semantic`) already exist; promote the orchestration in `_build_calls_result` to use a `CallsPipeline` or stage-based pattern that reduces direct import coupling.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The shell layer correctly depends inward on `core/`, `search/`, and `query/` modules. The `orchestration/request_factory.py` uses lazy imports (TYPE_CHECKING) to avoid pulling in macro implementations at import time. One minor inversion exists.

**Findings:**
- `tools/cq/ldmd/format.py:66-77` -- `_is_collapsed` imports from `tools.cq.neighborhood.section_layout` at runtime, creating a dependency from the LDMD parser (which should be infrastructure-level) up into the neighborhood domain module. This is wrapped in a try/except ImportError, indicating awareness of the coupling.

**Suggested improvement:**
Invert the dependency: have `section_layout.py` register its collapse rules with the LDMD module or pass collapse configuration as a parameter to `build_index`/`get_slice`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`RequestFactory` at `tools/cq/orchestration/request_factory.py:62` serves as a clean adapter layer between CLI/run surfaces and macro implementations. The `CqRuntimeServices` object on `CliContext` provides a service locator for search and calls execution.

**Findings:**
- `tools/cq/macros/impact.py:500-505` and `tools/cq/macros/calls/analysis.py:481-484` -- Direct filesystem reads (`filepath.read_text`) are scattered through macros with no port abstraction. Each macro independently handles `OSError`/`UnicodeDecodeError`, duplicating the IO boundary across 8+ files.
- `tools/cq/macros/shared.py:111-113` -- `resolve_target_files` does `Path(target).exists()` and `target_path.is_file()` directly, embedding filesystem assumptions.

**Suggested improvement:**
Introduce a `SourceReader` protocol (or similar) that macros receive via their request context. This protocol would handle `read_text`, `exists`, and error handling consistently. The current `CqRuntimeServices` pattern could be extended to include a source-reader service.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY -- Alignment: 1/3

**Current state:**
The most significant knowledge duplication is the Params/Options dual hierarchy.

**Findings:**
- `tools/cq/cli_app/params.py` and `tools/cq/cli_app/options.py` -- Every command has parallel definitions: `SearchParams` (dataclass with cyclopts annotations) and `SearchOptions` (msgspec struct). Field names, types, and defaults are duplicated. For example, `SearchParams.enrich: bool = True` at `params.py:195-203` mirrors `SearchOptions.enrich: bool = True` at `options.py:58`. This pattern repeats for `RunParams`/`RunOptions`, `ImpactParams`/`ImpactOptions`, `QueryParams`/`QueryOptions`, etc. (10 pairs).
- `tools/cq/macros/contracts.py:18-24` (`MacroRequestBase`) and `tools/cq/orchestration/request_factory.py:29-44` (`RequestContextV1`) -- Both carry `root: Path`, `argv: list[str]`, `tc: Toolchain`. The `RequestFactory` methods then destructure `ctx` to reconstruct request objects, re-passing the same three fields.

**Suggested improvement:**
For the Params/Options split: Consider making Options the single source of truth and deriving Params annotations programmatically, or using a single definition with dual-purpose decorators. For the request context duplication: `MacroRequestBase` could accept a `RequestContextV1` directly rather than flattening its fields.

**Effort:** medium
**Risk if unaddressed:** medium -- As new commands are added, the parallel Params/Options definitions will drift.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Request structs use frozen `msgspec.Struct` with typed fields, enforcing structural preconditions. The `RunStepBase` tagged union at `spec.py:26-33` prevents invalid step type construction.

**Findings:**
- `tools/cq/cli_app/params.py:44` and `params.py:297` -- Cyclopts validators (`_LIMIT_VALIDATOR`, `_DEPTH_VALIDATOR`) enforce numeric range constraints at the CLI boundary.
- `tools/cq/macros/impact.py:489-497` -- `_analyze_function` checks `current_depth >= context.max_depth` and `context.state.has_visited(key)` as preconditions, but these are implicit guards rather than declared contracts.
- Postconditions on `CqResult` are not enforced -- macros return results where `key_findings` may be empty, `summary` may be unset, etc.

**Suggested improvement:**
Consider adding a `validate()` method or post-construction check on `CqResult` to ensure that at least `summary` and `run` are populated when a macro completes.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The `RunStep` tagged union at `spec.py:123-135` is a textbook "parse, don't validate" pattern: JSON input is decoded into a typed union member via `parse_run_step_json`, and downstream code can pattern-match on the concrete type.

**Findings:**
- `tools/cq/cli_app/commands/search.py:60-64` -- The `--in` directory value undergoes ad-hoc re-parsing: checking `is_absolute()`, `is_file()`, suffix presence, and slash trailing. This late validation should be done once at boundary.
- `tools/cq/run/step_executors.py:207-213` -- Search step mode conversion from string `"regex"`/`"literal"` to `QueryMode` enum happens deep in the executor, rather than at the step parsing boundary.

**Suggested improvement:**
Extend `SearchStep` to store a `QueryMode | None` directly rather than a `SearchMode` literal string, performing the parse at the boundary in `step_decode.py`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Illegal states -- Alignment: 2/3

**Current state:**
The `RunStep` tagged union effectively prevents constructing invalid step types. `CliContext` requires `root`, `toolchain`, and `services` at construction.

**Findings:**
- `tools/cq/cli_app/context.py:57-85` -- `CliContext` has `output_format: OutputFormat | None = None`, which allows a state where the format is unknown. Callers must then handle `None` (e.g., `result_action.py:39` defaults to `OutputFormat.md`). If `md` is the fallback, it should be the default.
- `tools/cq/run/spec.py:112-121` -- `NeighborhoodStep.lang: str = "python"` uses a bare string rather than the `NeighborhoodLanguageToken` enum, allowing invalid language values.

**Suggested improvement:**
Default `CliContext.output_format` to `OutputFormat.md` instead of `None`. Change `NeighborhoodStep.lang` to use a constrained type or `Literal["python", "rust"]`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 1/3

**Current state:**
The most significant CQS violations occur in macro result construction, where functions both return values and mutate state.

**Findings:**
- `tools/cq/cli_app/app.py:176-192` -- `_build_launch_context` mutates the module-global `app.config` (line 181) while also constructing and returning a `LaunchContext` value. This is a command masquerading as a query.
- `tools/cq/macros/calls/entry.py:212-248` -- `_append_calls_findings` mutates `result.key_findings` and `result.sections` in place, receiving the `result` object and modifying it. This pattern repeats across all macros: 61 occurrences of `result.(key_findings|evidence|sections).(append|extend|insert)` across 12 files in `tools/cq/macros/`.
- `tools/cq/neighborhood/executor.py:119-121` -- `_coerce_neighborhood_summary` mutates `result.summary` in place, then `execute_neighborhood` further mutates `neighborhood_summary.target_resolution_kind` on the next lines.

**Suggested improvement:**
For `_build_launch_context`: separate the `app.config` mutation into its own command function called before the context construction. For macro result construction: the existing `MacroResultBuilder` at `result_builder.py` already provides a builder pattern -- extend it to support all finding/section/evidence additions, so that the final `build()` call produces an immutable result. This avoids scatter-shot mutation across helper functions.

**Effort:** medium
**Risk if unaddressed:** medium -- Mutation-based result construction makes it difficult to reason about what state a `CqResult` is in at any point during macro execution.

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
`CliContext.build()` at `context.py:114-161` constructs dependencies (`Toolchain.detect()`, `resolve_runtime_services`) internally. The `from_parts()` classmethod provides a DI-friendly alternative. CLI commands receive context via cyclopts injection.

**Findings:**
- `tools/cq/cli_app/context.py:149` -- `Toolchain.detect()` is called inside `CliContext.build()`, making the detection side effect invisible to callers. Test code must use `from_parts()` to avoid this.
- `tools/cq/macros/impact.py:860` -- `DefIndex.build(request.root)` is called inside `cmd_impact`, creating the index internally rather than receiving it as a dependency.
- `tools/cq/perf/smoke_report.py:65-66` -- `Toolchain.detect()` and `resolve_runtime_services()` are called inline in the benchmark function, coupling the benchmark to real detection.

**Suggested improvement:**
The current split between `build()` (convenient, side-effecting) and `from_parts()` (DI-friendly) is acceptable. Document that `from_parts()` is the preferred entry point for test contexts. For `cmd_impact`, consider making `DefIndex` injectable via the request or a service locator.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
Inheritance usage is minimal and appropriate. `RunStepBase` provides tagged-union machinery via `tag=True, tag_field="type"`. `FilterParams` provides shared filter fields to command-specific params via a single level of inheritance. `MacroRequestBase` provides the `(tc, root, argv)` triple.

No deep hierarchies exist. Behavior is composed via request factories, dispatch tables, and builder patterns.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Two-dot chains are common but generally acceptable as accessing cohesive composite structures.

**Findings:**
- `tools/cq/cli_app/commands/search.py:83` -- `ctx.services.search.execute(request)` chains through the services locator. This is a standard service locator pattern but technically a Demeter violation.
- `tools/cq/macros/calls/entry.py:468` -- `result.summary.front_door_insight = to_public_front_door_insight_dict(insight)` reaches two levels deep into the result to set a summary field.
- `tools/cq/orchestration/multilang_orchestrator.py:381` -- `priority.get(cast("QueryLanguage", finding.details.data.get("language", "python")), 99)` chains through `finding.details.data.get(...)` -- three levels of navigation.

**Suggested improvement:**
For the `finding.details.data.get("language")` pattern: add a `language` property on `Finding` or `DetailPayload` that extracts this common metadata.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Macro result construction follows an "ask and mutate" pattern where helper functions receive a `CqResult`, inspect its state, and append to its collections.

**Findings:**
- `tools/cq/macros/calls/entry.py:222-248` -- `_append_calls_findings` takes `result` and conditionally appends to `result.key_findings` based on `analysis.forwarding_count > 0`. The caller is "telling" the result to accumulate, but the decision logic is in the helper, not the result object.
- `tools/cq/macros/sig_impact.py:365-401` -- Multiple conditional blocks check `buckets["would_break"]`, `buckets["ambiguous"]`, `buckets["ok"]` and append findings individually. The result object has no encapsulation of "add classified findings."

**Suggested improvement:**
Extend `MacroResultBuilder` with semantic methods like `add_classified_findings(buckets)`, `add_taint_findings(sites)`, etc., so the building logic is encapsulated rather than spread across helper functions that directly mutate the result's lists.

**Effort:** medium
**Risk if unaddressed:** medium

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 1/3

**Current state:**
This is the most significant structural finding. The "Integration Shell" layer was expected to be thin wiring, but the macros contain substantial pure domain logic mixed with IO.

**Findings:**
- `tools/cq/macros/impact.py:135-427` -- `TaintVisitor` and its 13 `_EXPR_TAINT_HANDLERS` comprise ~290 lines of pure, deterministic AST analysis logic. This is a functional core that belongs in a lower analysis layer, but it is embedded in the shell alongside filesystem reads (`filepath.read_text()` at line 505).
- `tools/cq/macros/sig_impact.py:69-149` -- `_parse_signature` and `_classify_call` are pure functions that parse signatures and classify call compatibility. They perform no IO and should be testable independently.
- `tools/cq/macros/calls/analysis.py:1-550` (estimated) -- Call site analysis, argument shape extraction, hazard detection, and forwarding pattern identification are all pure analysis operations mixed with source file reading.
- `tools/cq/ldmd/format.py:80-158` -- `build_index` is an excellent example of a proper functional core: it takes bytes and returns an index with no side effects. This pattern should be emulated by the macros.

**Suggested improvement:**
Refactor macros to follow the pattern already established by `ldmd/format.py` and `neighborhood/contracts.py`:
1. Extract pure analysis functions (taint visitor, signature parser, call classifier) into `tools/cq/analysis/` as a new "functional core" package.
2. Macro entry points (`cmd_impact`, `cmd_sig_impact`, etc.) become thin orchestrators: read inputs -> call pure analysis -> build result.
3. This makes the analysis logic independently testable without filesystem setup.

**Effort:** large
**Risk if unaddressed:** high -- The current structure means testing taint analysis requires creating real files, and changes to analysis logic require modifying the integration layer.

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Macros are effectively idempotent: given the same repository state and inputs, they produce the same results. The `maybe_evict_run_cache_tag` calls in `calls/entry.py:509-510` and `runner.py:108-109` are cleanup operations that are safe to re-run.

**Findings:**
- `tools/cq/neighborhood/bundle_builder.py:220-221` -- `artifact_path.parent.mkdir(parents=True, exist_ok=True)` and `artifact_path.write_text(content)` are idempotent by virtue of deterministic filenames based on content hashes.
- `tools/cq/run/runner.py:60` -- `run_id = run_ctx.run_id or uuid7_str()` generates a new UUID for each run, which means re-running the same plan produces different run IDs. This is acceptable for tracing but worth noting.

**Suggested improvement:**
No significant changes needed. The idempotency property is well-maintained.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
Step IDs are deterministically assigned via `normalize_step_ids` at `spec.py:214-229`. Bundle IDs use `sha256` content hashing at `bundle_builder.py:156-159`. Language ordering uses `language_priority` at `multilang_orchestrator.py:36-44`.

**Findings:**
- `tools/cq/run/spec.py:214-229` -- `normalize_step_ids` assigns deterministic IDs like `q_0`, `search_1` based on step type and index, ensuring stable ordering.
- `tools/cq/orchestration/multilang_orchestrator.py:102-108` -- Multi-language merge uses explicit `priority` ordering and stable sort keys `(language_rank, -score, file, line, col)`, ensuring deterministic output.
- `tools/cq/perf/smoke_report.py:103` -- `generated_at_epoch_s=time.time()` introduces non-determinism in benchmark reports, but this is expected for time-series data.

**Suggested improvement:**
No significant changes needed.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
CLI commands are appropriately simple -- most are 10-20 lines that parse options, build a request, execute, and wrap the result. The `telemetry.py` module has complex nested try/except blocks.

**Findings:**
- `tools/cq/cli_app/telemetry.py:60-155` -- `invoke_with_telemetry` has three nested try/except blocks handling `CycloptsError` and `_INVOCATION_RUNTIME_ERRORS` at different stages. While the error classification is valuable, the nesting depth makes the control flow hard to follow.
- `tools/cq/cli_app/result_action.py:83-95` -- `_apply_single_action` uses string-based dispatch (`"return_int_as_exit_code_else_zero"`, `"return_value"`, `"return_zero"`, `"return_none"`) that is less discoverable than an enum-based approach.

**Suggested improvement:**
For `invoke_with_telemetry`: extract the inner execution into a separate function to reduce nesting. For `_apply_single_action`: replace string literals with an enum or typed constants.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions are justified by current use cases.

**Findings:**
- `tools/cq/macros/expand.py:28-36` -- `EXPANDERS` dict maps 7 detail-kind keys to the same `_identity_expand` function. If all expanders are identity functions, the entire dispatch mechanism may be premature.
- `tools/cq/cli_app/result_action.py:12-16` -- `ResultAction` supports a sequence of actions (`ResultActionLiteral | ResultActionCallable | Sequence[...]`), but the only actual usage at line 126-129 is a fixed 2-element tuple.

**Suggested improvement:**
For the expanders: either add actual expansion logic or simplify to a direct pass-through until distinct expanders are needed. For result actions: the sequence support is fine as a design seam.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
CLI naming follows standard conventions (`--format`, `--verbose`, `--include`).

**Findings:**
- `tools/cq/cli_app/params.py:211` and `tools/cq/cli_app/commands/search.py:60` -- The CLI flag `--in` maps to `in_dir` internally, but users might expect it to be a directory filter. In `search.py:63`, the code inspects whether the value "looks like a file" via suffix heuristics, which could surprise users providing directory names with dots.
- `tools/cq/run/spec.py:39-54` -- `QStep` and `SearchStep` both have a `query` field, but `CallsStep` uses `function` instead. The naming is consistent per command but the step/parameter naming conventions differ subtly.
- `tools/cq/cli_app/params.py:301-322` -- Both `--step` and `--steps` accept lists of run steps, differing only in JSON format (single object vs array). This is potentially confusing.

**Suggested improvement:**
Document the `--in` heuristic in help text. Consider unifying `--step` and `--steps` into a single flag that accepts both formats.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
`VERSION = "0.4.0"` at `app.py:28`, `RunPlan.version: int = 1` at `spec.py:20`, and `schema_version="cq.snb.v1"` at `bundle_builder.py:98` explicitly declare version contracts. Most modules have `__all__` exports.

**Findings:**
- `tools/cq/macros/impact.py` -- No `__all__` export; `ImpactRequest`, `TaintedSite`, `TaintState` are all public classes without explicit export declarations.
- `tools/cq/cli_app/options.py` -- No `__all__` export for the 10 Options classes.

**Suggested improvement:**
Add `__all__` to `impact.py` and `options.py` to declare the public surface.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
The CLI commands themselves are testable because they receive `CliContext` and return `CliResult`. However, the macro layer is difficult to unit test because pure logic is interleaved with filesystem IO.

**Findings:**
- `tools/cq/cli_app/context.py:149-153` -- `CliContext.build()` calls `Toolchain.detect()` and `resolve_runtime_services()`, both of which perform real environment detection. Only `from_parts()` is test-friendly.
- `tools/cq/macros/impact.py:484-540` -- `_analyze_function` reads files from disk, parses them, and runs analysis. Testing requires creating actual files in a temp directory.
- `tools/cq/macros/calls/analysis.py:481-545` -- Similar: `_analyze_single_file` and `_analyze_match_in_scope` read real files to extract call sites.
- `tools/cq/macros/impact.py:860` -- `cmd_impact` creates `DefIndex.build(request.root)` internally, making it impossible to inject a pre-built index for testing.

**Suggested improvement:**
1. Make `DefIndex` injectable via the request or a service locator, following the `CqRuntimeServices` pattern.
2. Extract pure analysis functions that accept parsed AST/source strings rather than file paths.
3. The `_analyze_function` pattern should be: caller reads the file, passes source text to a pure analysis function.

**Effort:** medium
**Risk if unaddressed:** high -- Without testable pure functions, macro logic can only be tested via integration tests that require filesystem setup.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Structured logging is present in the run layer (`runner.py`, `step_executors.py`). The `CqInvokeEvent` telemetry struct at `telemetry.py:23-35` captures timing, error classification, and event identity. Run IDs are threaded through execution.

**Findings:**
- `tools/cq/macros/calls/entry.py:75` and `tools/cq/run/runner.py:41` -- Both use `logger.debug(...)` with structured key=value format strings.
- `tools/cq/cli_app/telemetry.py:23-35` -- `CqInvokeEvent` captures `parse_ms`, `exec_ms`, `exit_code`, `error_class`, and `error_stage`, providing good invocation-level observability.
- Macro-level timing is not captured in a structured way -- the `ms()` start timestamps exist but are used only for `RunMeta.started_ms`, not for macro-stage profiling.

**Suggested improvement:**
Consider adding stage-level timing within macros (scan time, analysis time, enrichment time) to the summary or telemetry payload, similar to how `created_at_ms` is captured in `BundleMetaV1`.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Macros as Thick Shell

The primary structural issue is that `tools/cq/macros/` is not a thin imperative shell but contains substantial pure domain logic (taint analysis, signature parsing, call-site classification). This violates P2 (Separation of Concerns) and P16 (Functional Core / Imperative Shell), and it compounds P23 (Testability) because the domain logic cannot be tested without IO setup.

**Root cause:** The macros grew organically as feature-specific modules, and the analysis logic was never extracted into a separate pure-function layer.

**Affected principles:** P2, P3, P16, P23

**Suggested approach:** Introduce a `tools/cq/analysis/` package containing pure analysis functions (taint propagation, signature parsing, call classification, scope analysis). Macros become thin orchestrators that read inputs, invoke analysis, and build results.

### Theme 2: CqResult Mutation Sprawl

With 61 occurrences of `result.(key_findings|evidence|sections).(append|extend|insert)` across 12 macro files, the mutable result construction pattern is pervasive. This makes it difficult to reason about what state a result is in at any point during macro execution, and it violates P11 (CQS) because helper functions both mutate state and return values.

**Root cause:** `CqResult` uses mutable `list` fields, and the `MacroResultBuilder` was introduced as an improvement but is only partially adopted.

**Affected principles:** P11, P15

**Suggested approach:** Extend `MacroResultBuilder` to support all finding/section/evidence additions. Macro helpers should return data to the builder rather than directly mutating the result. The `build()` method then produces an immutable, fully-formed result.

### Theme 3: Params/Options Duplication

Every CLI command requires parallel definitions in `params.py` (cyclopts-annotated dataclass) and `options.py` (msgspec struct), with `options_from_params()` converting between them. This creates 10+ pairs of nearly-identical field definitions that must stay synchronized.

**Root cause:** cyclopts requires `dataclass` + `Annotated[..., Parameter()]` for CLI binding, while the internal API uses `msgspec.Struct` for performance and serialization. The bridge function `options_from_params` at `options.py:121-129` exists to span this gap.

**Affected principles:** P7 (DRY)

**Suggested approach:** Investigate whether cyclopts can accept msgspec structs directly (reducing the need for dual definitions), or generate one from the other at module-load time using a decorator or metaclass.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P11 (CQS) | Remove `app.config` mutation from `_build_launch_context` | small | Eliminates module-global side effect in a function that looks pure |
| 2 | P10 (Illegal states) | Default `CliContext.output_format` to `OutputFormat.md` | small | Removes scattered None-handling in result_action and render |
| 3 | P9 (Parse, don't validate) | Change `SearchStep.mode` from string to `QueryMode \| None` | small | Eliminates late string-to-enum conversion in step executors |
| 4 | P22 (Public contracts) | Add `__all__` to `impact.py` and `options.py` | small | Declares public API surface for 12+ classes |
| 5 | P5 (Dependency direction) | Invert LDMD -> neighborhood dependency for collapse config | small | Removes upward dependency from infrastructure to domain |

## Recommended Action Sequence

1. **Quick wins 1-5** (above) -- Low-risk, immediate improvements to CQS, contract clarity, and dependency direction.

2. **Extend MacroResultBuilder adoption** (P11, P15) -- Add semantic methods to `MacroResultBuilder` for adding classified findings, taint sites, and bucket sections. Migrate `calls/entry.py` and `impact.py` to use the builder exclusively, eliminating direct result mutation.

3. **Extract pure analysis from impact macro** (P2, P16, P23) -- Move `TaintVisitor`, `_EXPR_TAINT_HANDLERS`, and `_parse_signature` into `tools/cq/analysis/taint.py` and `tools/cq/analysis/signature.py`. Add unit tests for the pure functions. Update `cmd_impact` and `cmd_sig_impact` to use the extracted modules.

4. **Extract pure analysis from calls macro** (P2, P16, P23) -- Similarly move call-site classification, arg-shape analysis, and hazard detection from `calls/analysis.py` into standalone pure-function modules.

5. **Reduce Params/Options duplication** (P7) -- Investigate code generation or a shared schema definition that produces both cyclopts-compatible params and msgspec-compatible options from a single source.

6. **Make DefIndex injectable** (P12, P23) -- Add `DefIndex` to `CqRuntimeServices` or accept it as an optional parameter on `ImpactRequest`, enabling test injection.

7. **Introduce SourceReader protocol** (P6) -- Abstract filesystem reads behind a protocol that macros receive via their context, enabling test doubles and consistent error handling.
