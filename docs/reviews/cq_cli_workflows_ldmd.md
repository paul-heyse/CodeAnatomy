# Design Review: tools/cq/cli_app/, tools/cq/run/, tools/cq/ldmd/

**Date:** 2026-02-16
**Scope:** `tools/cq/cli_app/` (26 files), `tools/cq/run/` (9 files), `tools/cq/ldmd/` (3 files)
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 38 (7,930 LOC total)

## Executive Summary

The CLI layer is well-structured as a thin adapter over core logic, with strong separation between parameter parsing (`params.py`), option structs (`options.py`), and command execution. The run engine demonstrates good compositional design with typed step specifications and a tagged union for run steps. However, three systemic issues reduce alignment: (1) significant code duplication across the `cli_app/commands/` and `run/` modules -- particularly `_has_query_tokens`, `_semantic_env_from_bundle`, `_error_result`, and `_merge_in_dir` are copy-pasted verbatim; (2) the `run/runner.py` module at 517 lines carries too many responsibilities (parsing, batching, execution, error handling, scope merging); and (3) the LDMD writer relies heavily on untyped `getattr` access patterns that bypass the type system. Overall alignment is good (20 of 24 principles score 2 or 3), with focused improvements available at modest effort.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `_STEP_TAGS` dict duplicates msgspec tag metadata |
| 2 | Separation of concerns | 2 | medium | medium | `runner.py` mixes orchestration with scope merging and error formatting |
| 3 | SRP | 1 | medium | medium | `runner.py` has 5+ reasons to change; `result_action.py` mixes exit code policy with result rendering |
| 4 | High cohesion, low coupling | 2 | small | low | `run/` imports `cli_app.context.CliContext` directly; tight coupling |
| 5 | Dependency direction | 2 | medium | medium | `run/chain.py` imports `cli_app.app` singleton; `run/` depends on CLI layer |
| 6 | Ports & Adapters | 2 | medium | low | Commands are thin adapters; but no explicit port protocol between CLI and core |
| 7 | DRY | 1 | small | high | 4 functions duplicated verbatim across `runner.py`/`step_executors.py`/`commands/` |
| 8 | Design by contract | 2 | small | low | `RunPlan`, `RunStep` use msgspec with tag validation; `CliContext.build` validates |
| 9 | Parse, don't validate | 3 | - | - | Boundary parsing via `decode_json_strict`/`decode_toml_strict` in `step_decode.py` and `loader.py` |
| 10 | Make illegal states unrepresentable | 2 | small | low | `RunStep` tagged union prevents invalid step types; but `mode` as raw string in LDMD |
| 11 | CQS | 2 | small | low | `handle_result` both mutates result (appends artifacts) and returns exit code |
| 12 | DI + explicit composition | 2 | small | low | Commands receive context via injection; lazy imports used appropriately |
| 13 | Composition over inheritance | 3 | - | - | `RunStepBase` is shallow (1 level), all steps compose via tagged union |
| 14 | Law of Demeter | 2 | small | low | `ctx.toolchain.to_dict()` and `ctx.toolchain.has_sgpy` are borderline |
| 15 | Tell, don't ask | 1 | medium | medium | LDMD writer uses 23 `getattr` calls to probe opaque objects |
| 16 | Functional core, imperative shell | 2 | small | low | LDMD `format.py` is purely functional; but `runner.py` mixes IO with transforms |
| 17 | Idempotency | 3 | - | - | Run plans are stateless; re-execution produces same results |
| 18 | Determinism | 3 | - | - | `normalize_step_ids` ensures deterministic IDs; console output is width-fixed |
| 19 | KISS | 2 | small | low | `telemetry.py` CqInvokeEvent construction is repeated 4 times |
| 20 | YAGNI | 3 | - | - | No speculative abstractions; `contracts.py` is empty placeholder (harmless) |
| 21 | Least astonishment | 2 | small | low | `neighbors` command returns JSON even without `--format json` |
| 22 | Public contracts | 2 | small | low | `VERSION = "0.4.0"` in `app.py`; `__all__` exports defined consistently |
| 23 | Testability | 2 | medium | medium | `runner.py` functions hard to test in isolation due to coupled `CliContext` |
| 24 | Observability | 1 | medium | medium | No logging/tracing in `run/` or `ldmd/`; only `telemetry.py` captures CLI timing |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Modules generally hide internals well. The `_STEP_TAGS` dict in `spec.py` and the `_NON_SEARCH_DISPATCH` table in `step_executors.py` are private. However, the `_STEP_TAGS` dict at `/Users/paulheyse/CodeAnatomy/tools/cq/run/spec.py:167-179` manually duplicates tag values that are already declared via `msgspec.Struct(tag="...")` on each step class, creating a maintenance burden where tag values must be kept in sync in two places.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/spec.py:167-179`: `_STEP_TAGS` dict manually maps each step type to its tag string, duplicating the `tag=` keyword in each class definition (e.g., line 35: `QStep(RunStepBase, tag="q")`). If a tag is renamed in one place but not the other, `step_type()` returns the wrong value.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/app.py:153-154`: Module-level `console` and `error_console` singletons are created at import time. While `_make_console` is private, the singletons themselves are importable and imported by `result_action.py:206` and `result_action.py:220`.

**Suggested improvement:**
Derive `_STEP_TAGS` from the msgspec struct metadata rather than maintaining a parallel dict. The `__struct_config__` attribute on msgspec structs exposes the tag value, allowing a single authoritative source.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The CLI layer achieves good separation: `params.py` handles CLI parameter declarations, `options.py` defines option structs, and command files are thin orchestrators. However, `runner.py` mixes multiple concerns.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:65-123`: `execute_run_plan` handles step partitioning, Q-step execution, non-Q-step dispatch, result merging, summary population, cache eviction, and finding ID assignment -- all in one function. Scope merging logic (`_apply_run_scope`, `_merge_in_dir`, `_merge_excludes`) at lines 421-450 is plan-level concern mixed with execution.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/neighborhood.py:56-115`: The neighborhood command function is 60 lines, performing target resolution, bundle building, run metadata creation, result rendering, and cache eviction. This is denser than other commands which delegate to macros.

**Suggested improvement:**
Extract scope merging (`_apply_run_scope`, `_merge_in_dir`, `_merge_excludes`) from `runner.py` into a dedicated `run/scope.py` module. Extract the neighborhood execution sequence from both `commands/neighborhood.py` and `step_executors.py` into a shared `neighborhood/executor.py` that both call.

**Effort:** medium
**Risk if unaddressed:** medium -- as more step types are added, `runner.py` will grow further.

---

#### P3. SRP -- Alignment: 1/3

**Current state:**
`runner.py` (517 lines) changes for at least five distinct reasons: (1) new step types, (2) batch execution strategy changes, (3) scope merging logic, (4) error result formatting, (5) query token detection. Similarly, `result_action.py` conflates exit code normalization, result-action dispatch, and CQ-specific result rendering.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py` (517 lines): Contains 21 functions spanning step parsing, batch scanning, entity query execution, pattern query execution, scope merging, error formatting, and file tabulation. The module has the most diverse import surface in the scope (25+ imports).
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result_action.py:1-82`: Mixes three concerns: cyclopts result-action dispatch (`apply_result_action`), CQ-specific result rendering (`cq_result_action` which calls `handle_result`), and exit code normalization.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py:1-382`: At 382 lines, handles filtering, rendering, artifact persistence, search artifact bundling, and output emission. Five helper functions at the bottom (`_search_query`, `_search_artifact_summary`, `_search_artifact_diagnostics`) are search-specific knowledge embedded in a generic result module.

**Suggested improvement:**
Split `runner.py` into: (1) `runner.py` -- orchestration only (step partitioning, dispatch, merging), (2) `q_execution.py` -- entity and pattern Q-step execution, (3) `scope.py` -- scope merging utilities. Move search artifact construction helpers from `result.py` to a dedicated `search_artifact_builder.py`.

**Effort:** medium
**Risk if unaddressed:** medium -- new step types or output formats will make these files harder to maintain.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Related concepts are generally grouped well: option structs live together in `options.py`, parameter declarations in `params.py`, types in `types.py`. However, the `run/` package has a structural coupling to `cli_app/`.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:12`: `from tools.cq.cli_app.context import CliContext` -- the run engine directly depends on the CLI context type.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/step_executors.py:11`: Same import of `CliContext`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/q_step_collapsing.py:7`: Same import of `CliContext`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/chain.py:10`: `from tools.cq.cli_app.app import app` -- the chain compiler imports the module-level app singleton, creating a hard dependency on the CLI application instance.

**Suggested improvement:**
Define a lightweight `RunExecutionContext` protocol in `run/` that captures only the fields the run engine needs (`root`, `argv`, `toolchain`, `artifact_dir`). Have `CliContext` satisfy this protocol. This decouples `run/` from `cli_app/` and makes the run engine independently testable.

**Effort:** small
**Risk if unaddressed:** low -- but the coupling currently prevents testing `run/` without constructing a full `CliContext`.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Commands correctly depend inward on core logic (macros, request factory). However, the run engine (`run/`) depends outward on the CLI layer (`cli_app/`), violating the expected direction where the CLI should be a thin adapter over the run engine, not the other way around.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/chain.py:10`: `from tools.cq.cli_app.app import app` -- the chain compiler depends on the CLI application singleton to use `app.app_stack` and `app.parse_known_args`. This is the most severe inversion: a core execution module depends on the UI layer.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/loader.py:11`: `from tools.cq.cli_app.options import RunOptions` -- the plan loader depends on CLI option structs rather than a run-domain type.

**Suggested improvement:**
For `chain.py`, accept the cyclopts `App` as a parameter rather than importing the singleton. For `loader.py`, define a `LoadRunPlanRequest` struct in `run/spec.py` that carries the needed fields (`plan`, `step`, `steps`) and convert from `RunOptions` at the CLI boundary.

**Effort:** medium
**Risk if unaddressed:** medium -- the dependency inversion makes the run engine unusable outside the CLI context and prevents programmatic use (e.g., from tests or an API layer).

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The CLI commands act as adapters: they parse CLI parameters, construct request objects, delegate to core macros/services, and wrap results in `CliResult`. The `protocol_output.py` module provides clean adapter helpers (`text_result`, `json_result`, `emit_payload`). However, there is no explicit port interface that the CLI adapts.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/analysis.py:38-296`: Each command follows a consistent adapter pattern: `require_context` -> `options_from_params` -> build `RequestContextV1` -> delegate to macro -> wrap in `CliResult`. This is the correct hexagonal structure.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/neighborhood.py:56-115`: The neighborhood command does NOT follow this pattern. It directly imports and orchestrates bundle building, target resolution, and result rendering -- acting as both adapter AND orchestrator.

**Suggested improvement:**
Extract the neighborhood orchestration logic into a macro or service function (similar to `cmd_impact`, `cmd_scopes`, etc.), so the CLI command becomes a thin adapter like the others.

**Effort:** medium
**Risk if unaddressed:** low -- but inconsistency makes the codebase harder to navigate.

---

### Category: Knowledge (7-11)

#### P7. DRY -- Alignment: 1/3

**Current state:**
Four functions are duplicated verbatim across the scope. This is the most significant design gap in the reviewed code.

**Findings:**
- `_has_query_tokens`: Duplicated at `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/query.py:27-34` and `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:494-496`. Both contain the same regex pattern for detecting query tokens. Notably, the `runner.py` version has double-escaped backslashes (`\\\\w` instead of `\\w`), suggesting the copies have already drifted.
- `_semantic_env_from_bundle`: Duplicated at `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/neighborhood.py:118-136` and `/Users/paulheyse/CodeAnatomy/tools/cq/run/step_executors.py:429-447`. Same key-mapping logic, identical structure.
- `_error_result`: Duplicated at `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:499-511` and `/Users/paulheyse/CodeAnatomy/tools/cq/run/step_executors.py:507-519`. Identical function body constructing error results.
- `_merge_in_dir`: Duplicated at `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:433-436` and `/Users/paulheyse/CodeAnatomy/tools/cq/run/step_executors.py:501-504`. Identical 4-line function.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/ldmd.py:52-54,137-139,201-203,239-241`: The `doc_path = Path(path) / if not doc_path.exists(): return text_result(...)` pattern is repeated in all 4 LDMD subcommands.

**Suggested improvement:**
(1) Move `_has_query_tokens` to `tools/cq/query/parser.py` where query parsing lives. (2) Move `_semantic_env_from_bundle` to `tools/cq/neighborhood/bundle_builder.py`. (3) Move `_error_result` and `_merge_in_dir` to a shared `run/helpers.py`. (4) Extract LDMD document loading into a helper `_load_ldmd_document(path: str, ctx: CliContext) -> tuple[bytes, LdmdIndex] | CliResult`.

**Effort:** small
**Risk if unaddressed:** high -- the `_has_query_tokens` copies have already drifted (different escaping), meaning one may silently produce different results than the other. This is exactly the kind of semantic duplication that DRY exists to prevent.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Boundary contracts are well-enforced through msgspec tagged unions (`RunStep`), typed boundary decoders (`decode_json_strict`, `decode_toml_strict`), and cyclopts validators (number ranges, mutual exclusion). The `validate_launcher_invariants` function at `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/validators.py:78-97` validates cross-option invariants.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/step_decode.py:9-44`: Uses `decode_json_strict` for boundary parsing of run steps -- exemplary contract enforcement.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/params.py:45-47`: Validators for `limit`, `depth`, and `max_files` enforce numeric bounds.
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py:195-247`: The `get_slice` function validates `mode` against a string set rather than using the `LdmdSliceMode` enum from `types.py`, weakening the contract.

**Suggested improvement:**
Accept `LdmdSliceMode` instead of `str` for the `mode` parameter in `get_slice`, or at minimum validate against the enum values rather than a bare string set.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
The codebase excels at this principle. CLI inputs are parsed into typed structs at the boundary via `options_from_params` (which uses `convert_strict` from `typed_boundary`). Run step JSON is parsed into typed `RunStep` union members via `decode_json_strict`. TOML plan files are parsed via `decode_toml_strict`. After parsing, downstream code operates on well-typed values.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/options.py:119-127`: `options_from_params` converts dataclass params to msgspec options structs in one shot.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/loader.py:20-42`: `load_run_plan` parses and validates in one pass, returning a fully typed `RunPlan`.

No action needed.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
The `RunStep` tagged union (11 concrete types) prevents invalid step combinations. `RunPlan` uses `frozen=True` for immutability. However, some representations allow impossible states.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/spec.py:41-50`: `SearchStep` has both `regex: bool` and `literal: bool` fields, which are mutually exclusive. The combination `regex=True, literal=True` is structurally representable but semantically illegal. The check occurs at runtime in `/Users/paulheyse/CodeAnatomy/tools/cq/run/step_executors.py:193-195`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/types.py:165-180`: `LdmdSliceMode` enum exists but `get_slice` in `format.py` accepts `str`, not the enum.

**Suggested improvement:**
Replace `regex: bool` + `literal: bool` on `SearchStep` with a `mode: SearchMode | None = None` field using a `SearchMode` enum (`regex`, `literal`). This makes the mutual exclusion structural.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS well. Queries return data, commands change state. However, `handle_result` is a notable violator.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py:143-197`: `handle_result` both mutates the result (appending artifacts at lines 296, 299, 302; mutating `result.summary` at line 61) AND returns an exit code. It is simultaneously a command and a query.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/q_step_collapsing.py:143,146,153-156`: `_normalize_single_group_result` mutates `result.summary` via `setdefault` and `pop` calls while also returning the result.

**Suggested improvement:**
Separate artifact persistence (side effect) from result rendering (query) in `handle_result`. Return the rendered output and exit code as a value; let the caller handle persistence.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
Commands receive `CliContext` via cyclopts' `Parameter(parse=False)` injection. Services are resolved lazily via `resolve_runtime_services`. The app is composed explicitly in `app.py` with command registration.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/app.py:229-291`: Commands are registered declaratively with string-based lazy loading (`app.command("tools.cq.cli_app.commands.analysis:impact")`). This is clean explicit composition.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/chain.py:10,87-94`: `compile_chain_segments` depends on the `app` singleton for parsing chain segments. This hidden dependency prevents testing the chain compiler with a different app configuration.

**Suggested improvement:**
Pass the `App` instance as a parameter to `compile_chain_segments` rather than importing the global singleton.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
Inheritance is minimal and shallow. `RunStepBase` is a single-level base for tagged union members. `CommonFilters` is extended by command-specific option structs, but this is a single level of data inheritance, not behavioral. `FilterParams` (dataclass) uses inheritance for shared CLI parameters, which is appropriate for cyclopts' flatten pattern.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. However, some chain-access patterns exist.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/neighborhood.py:96`: `ctx.toolchain.to_dict()` -- reaching through context to toolchain to serialize it. This pattern is repeated in `/Users/paulheyse/CodeAnatomy/tools/cq/run/step_executors.py:408`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py:50`: `result.summary.get("front_door_insight")` -- reaching into result summary dict to find insight. This is borderline since `summary` is a public `dict[str, object]`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:290`: `ctx.toolchain.has_sgpy` -- reaching through context to toolchain capabilities.

**Suggested improvement:**
Add a convenience method to `CliContext` (or the proposed `RunExecutionContext`) that provides `toolchain_dict()` and `has_tool(name)` methods, so callers don't need to know the toolchain's internal structure.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
The LDMD writer extensively violates this principle by using `getattr` to probe opaque objects rather than requiring typed interfaces.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/writer.py:52-156`: Functions `_emit_snb_target`, `_emit_snb_summary`, `_emit_snb_slices`, `_emit_snb_slice`, `_emit_snb_diagnostics`, `_emit_snb_provenance` collectively use 23 `getattr(obj, "attr", default)` calls to extract data from `bundle: object`. This is a classic "ask" pattern -- the writer asks the bundle for its internal structure rather than the bundle knowing how to present itself.
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/writer.py:81-116`: `_emit_snb_slice` probes slice objects with `getattr(s, "kind", "unknown")`, `getattr(s, "total", 0)`, `getattr(s, "preview", [])`, etc. If the bundle schema changes, this code silently produces wrong output instead of failing.

**Suggested improvement:**
Type the `bundle` parameter as `SemanticNeighborhoodBundleV1` instead of `object`. The module already has the import available under TYPE_CHECKING. If the goal is to decouple from the schema, define a protocol `LdmdRenderable` that the bundle satisfies, replacing untyped `getattr` with protocol methods.

**Effort:** medium
**Risk if unaddressed:** medium -- silent data loss when schema changes.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
LDMD `format.py` is a pure functional core: all functions take inputs and return outputs with no side effects. The CLI commands form the imperative shell. However, `runner.py` mixes functional transforms with imperative orchestration.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py:76-377`: All functions are pure: `build_index`, `get_slice`, `search_sections`, `get_neighbors` take data and return data. Exemplary.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:65-123`: `execute_run_plan` mixes pure transforms (step partitioning, result merging) with imperative effects (cache eviction at lines 121-122, finding ID assignment at line 120).

**Suggested improvement:**
Move cache eviction and finding ID assignment to the CLI command layer (`commands/run.py`), leaving `execute_run_plan` as a pure orchestration function that returns results without side effects.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Run plans are stateless specifications. Re-executing a plan with the same inputs produces the same results. `normalize_step_ids` ensures deterministic IDs. Cache eviction uses `maybe_evict_run_cache_tag` which is safe on repeated calls.

No action needed.

---

#### P18. Determinism -- Alignment: 3/3

**Current state:**
Console output uses fixed width (100 chars) via `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/app.py:144-150`. Step IDs are deterministic via `normalize_step_ids`. JSON serialization uses `order="deterministic"` at `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/protocol_output.py:10`. UUID7 provides time-ordered but deterministic-within-run identifiers.

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The overall design is straightforward. However, `telemetry.py` has unnecessary repetition that adds complexity.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/telemetry.py:60-155`: `invoke_with_telemetry` constructs `CqInvokeEvent` four times (lines 99-111, 115-125, 128-140, 143-155) with nearly identical field sets. The only varying fields are `ok`, `exit_code`, `error_class`, and `error_stage`. This makes the function harder to read and maintain.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/types.py:208-288`: `comma_separated_list` and `comma_separated_enum` are nearly identical functions that could share an implementation.

**Suggested improvement:**
Extract a `_make_event(ok, command_name, parse_ms, exec_ms, exit_code, error_class, error_stage, event_identity)` helper in `telemetry.py` to reduce the four construction sites to one. For the converters, `comma_separated_enum` could be implemented as `comma_separated_list(enum_type)` since they share the same structure.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 3/3

**Current state:**
No speculative generality detected. The empty `contracts.py` at `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/contracts.py` is a placeholder but causes no harm. The converter infrastructure in `types.py` serves a clear current need (cyclopts integration).

No action needed.

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most commands behave predictably. However, the LDMD `neighbors` command has a surprising output format inconsistency.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/ldmd.py:249-262`: The `neighbors` subcommand always returns JSON output (line 262: `json.dumps(nav, indent=2)` with `media_type="application/json"`), even when `--format json` is not specified. The `if wants_json(ctx)` branch (line 249) adds a slightly different structure, but the fallback also produces JSON. This differs from other commands that produce human-readable text by default.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/ldmd.py:211-212`: The `search` subcommand also always returns JSON regardless of format. This is a pattern: all 4 LDMD subcommands produce JSON by default.

**Suggested improvement:**
Either document that LDMD subcommands always produce JSON (since they are protocol commands), or add human-readable text formatting for non-JSON mode. The former is more appropriate given the protocol nature of LDMD.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
`__all__` exports are defined in most modules. `VERSION = "0.4.0"` provides version identity. The `CqStruct` base class signals stable contract types. However, some public surfaces are implicit.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/app.py:28`: `VERSION = "0.4.0"` is the only version declaration, but it's not propagated to the package `__init__.py`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/__init__.py:1-35`: Clean `__all__` export of all public step types and utilities.
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/__init__.py:5-7`: Only exports `admin`, not other command modules. This is correct (commands are registered via string references), but could surprise developers looking at the package.

**Suggested improvement:**
Add `__version__` to `cli_app/__init__.py` re-exported from `app.py`, making the version discoverable at the package level.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Testability -- Alignment: 2/3

**Current state:**
Individual command functions are testable because they accept a `CliContext` parameter. LDMD `format.py` functions are pure and easy to test. However, the run engine (`runner.py`) is difficult to test in isolation.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py:65`: `execute_run_plan` takes `CliContext` which requires `root`, `toolchain`, `argv` -- all heavyweight to construct. Testing a simple 2-step plan requires a real file system path and toolchain detection.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/chain.py:87-94`: `compile_chain_segments` depends on the global `app` singleton (imported at module level), making it impossible to test with a mock app.
- No test files exist under `tests/unit/cq/cli_app/` for the CLI command modules specifically.
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/ldmd/test_writer.py` and `/Users/paulheyse/CodeAnatomy/tests/unit/cq/ldmd/test_root_alias.py` exist for LDMD.

**Suggested improvement:**
Define a `RunExecutionContext` protocol (or dataclass) with only the fields the run engine needs, and have `CliContext` satisfy it. This allows tests to provide a minimal context. For `chain.py`, accept the `App` as a parameter.

**Effort:** medium
**Risk if unaddressed:** medium -- insufficient test coverage of the run engine increases regression risk.

---

#### P24. Observability -- Alignment: 1/3

**Current state:**
`telemetry.py` captures CLI invocation timing (parse/exec milliseconds) and error classification, which is good. However, the run engine and LDMD modules have no logging, tracing, or metrics.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/telemetry.py:23-35`: `CqInvokeEvent` captures structured telemetry for CLI invocations -- good.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py`: No logging anywhere in the 517-line module. When a multi-step plan fails on step 3 of 5, there is no trace of what happened during steps 1-2. The only observability is the error result appended to `key_findings`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/step_executors.py`: No logging. Parallel execution via worker scheduler (line 170-183) has no visibility into worker lifecycle.
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py`: No logging. Parse failures raise `LdmdParseError` but don't log context (file path, byte offset of failure).
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/run_summary.py`: No logging of aggregation decisions.

**Suggested improvement:**
Add structured logging at key points: (1) in `execute_run_plan` -- log plan start with step count, log each step completion with timing, log plan completion with summary; (2) in `execute_non_q_step_safe` -- log step errors with step type and exception; (3) in LDMD `build_index` -- log section count and parse duration. Use the project's observability conventions.

**Effort:** medium
**Risk if unaddressed:** medium -- debugging multi-step plan failures currently requires adding print statements or debugger breakpoints.

---

## Cross-Cutting Themes

### Theme 1: Code Duplication Between CLI Commands and Run Engine

**Root cause:** The neighborhood command and several Q-step utilities were developed independently in `cli_app/commands/` and `run/step_executors.py` without extracting shared logic.

**Affected principles:** P7 (DRY), P3 (SRP), P2 (Separation of concerns)

**Suggested approach:** Create shared utility functions in domain-appropriate locations (e.g., `_semantic_env_from_bundle` in `neighborhood/`, `_error_result` and `_merge_in_dir` in `run/helpers.py`, `_has_query_tokens` in `query/parser.py`). This is a low-effort, high-impact change.

### Theme 2: Run Engine Coupling to CLI Layer

**Root cause:** The run engine was likely built as part of the CLI and never decoupled. `CliContext` carries CLI-specific fields (`output_format`, `save_artifact`) that the run engine doesn't need.

**Affected principles:** P4 (Coupling), P5 (Dependency direction), P12 (DI), P23 (Testability)

**Suggested approach:** Introduce a `RunExecutionContext` protocol in `run/` that captures only what the engine needs. This is the single structural change that would improve the most principles simultaneously.

### Theme 3: Untyped Object Access in LDMD Writer

**Root cause:** The LDMD writer was designed to be schema-agnostic (accepting `object` parameters), but this agnosticism came at the cost of type safety and silent failure modes.

**Affected principles:** P15 (Tell don't ask), P8 (Design by contract), P10 (Illegal states)

**Suggested approach:** Either type the `bundle` parameter as `SemanticNeighborhoodBundleV1` (which is already imported under TYPE_CHECKING) or define a `LdmdRenderable` protocol that the writer depends on.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 DRY | Extract `_has_query_tokens`, `_semantic_env_from_bundle`, `_error_result`, `_merge_in_dir` to shared locations | small | Eliminates 4 duplicated functions, fixes already-drifted regex escaping |
| 2 | P19 KISS | Extract `_make_event` helper in `telemetry.py` to consolidate 4 `CqInvokeEvent` construction sites | small | Reduces telemetry code complexity by ~40 lines |
| 3 | P10 Illegal states | Replace `regex: bool` + `literal: bool` on `SearchStep` with `mode: SearchMode \| None` | small | Makes mutual exclusion structural instead of runtime |
| 4 | P15 Tell/ask | Type LDMD writer `bundle` parameter as `SemanticNeighborhoodBundleV1` instead of `object` | small | Replaces 23 `getattr` calls with typed attribute access; catches schema drift at type-check time |
| 5 | P21 Least astonishment | Document LDMD subcommands as always-JSON protocol commands | small | Eliminates confusion about default output format |

## Recommended Action Sequence

1. **Extract duplicated functions (P7).** Move `_has_query_tokens` to `query/parser.py`, `_semantic_env_from_bundle` to `neighborhood/bundle_builder.py`, and `_error_result`/`_merge_in_dir` to `run/helpers.py`. Fix the drifted regex escaping in `runner.py:495`. This is the highest-value change with no structural risk.

2. **Type the LDMD writer's bundle parameter (P15, P8).** Change `render_ldmd_document(bundle: object)` to accept `SemanticNeighborhoodBundleV1`. Replace `getattr` calls with typed attribute access. This catches schema drift at type-check time.

3. **Introduce `RunExecutionContext` protocol (P4, P5, P23).** Define a minimal protocol in `run/spec.py` or `run/context.py` with `root`, `argv`, `toolchain`, `artifact_dir`. Have `CliContext` satisfy it. Update `runner.py`, `step_executors.py`, and `q_step_collapsing.py` to depend on the protocol.

4. **Replace `SearchStep.regex`/`literal` with `SearchStep.mode` (P10).** Define `SearchMode` enum in `run/spec.py`. Update `step_executors.py:193-205` to use the enum instead of boolean checks.

5. **Consolidate telemetry event construction (P19).** Extract `_make_event` helper to reduce the 4 construction sites in `telemetry.py` to 1.

6. **Split `runner.py` responsibilities (P3, P2).** Extract Q-step execution into `run/q_execution.py` and scope merging into `run/scope.py`. Keep `runner.py` as the orchestration entry point.

7. **Add structured logging to run engine (P24).** Add log statements at plan start, step completion, and plan completion in `runner.py` and `step_executors.py`.

8. **Decouple `chain.py` from app singleton (P5, P12, P23).** Accept `App` as a parameter in `compile_chain_segments`. Pass it from `commands/chain.py`.
