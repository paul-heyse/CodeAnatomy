# Design Review: src/obs/ and src/cli/

**Date:** 2026-02-16
**Scope:** `src/obs/` (29 files), `src/cli/` (26 files)
**Focus:** All principles (1-24), with emphasis on dependency direction (P5), ports & adapters (P6), and information hiding (P1)
**Depth:** moderate (all 55 files surveyed at header level; 12 files deep-read)
**Files reviewed:** 55

## Executive Summary

The `obs/` and `cli/` modules demonstrate good internal cohesion and clear separation of OTel internals behind a facade (`obs/otel/__init__.py`). However, the facade is widely bypassed -- 49 files across `semantics/`, `extract/`, `datafusion_engine/`, `storage/`, and `graph/` import directly from `obs.otel.tracing`, `obs.otel.metrics`, `obs.otel.scopes`, and `obs.otel.run_context`, coupling core business logic to OTel implementation details. A reverse dependency from `serde_schema_registry.py` to `cli.config_models` violates the inward dependency rule. The CLI itself is well-structured as an adapter layer using Cyclopts, though the `app.py` file carries excessive OTel configuration plumbing (~250 lines of `ObservabilityOptions` + override builders). Top priorities: (1) enforce the `obs.otel` facade to hide OTel internals, (2) remove the reverse dependency from `serde_schema_registry` to `cli`, (3) extract the OTel config mapping from `app.py`.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | OTel facade bypassed by 49 files; internals of `obs.otel.*` directly imported |
| 2 | Separation of concerns | 2 | medium | medium | `obs/diagnostics.py` mixes artifact domain structs with OTel emission; `cli/app.py` mixes config assembly with OTel plumbing |
| 3 | SRP | 2 | small | low | `obs/metrics.py` owns dataset stats schemas + OTel metric recording + fragment utilities |
| 4 | High cohesion, low coupling | 1 | medium | high | 49 external files coupled to OTel sub-modules; `cli/telemetry.py` imports Cyclopts private APIs |
| 5 | Dependency direction | 1 | small | high | `serde_schema_registry.py` (core) imports from `cli.config_models` (adapter); core modules import `obs.otel.*` directly |
| 6 | Ports & Adapters | 1 | large | high | No tracing/metrics port; core modules call OTel functions directly instead of through an abstraction |
| 7 | DRY (knowledge) | 2 | small | low | `SCOPE_EXTRACT` string imported individually by 5 extractors; scope constants well-centralized in `constants.py` |
| 8 | Design by contract | 2 | small | low | `DiagnosticsCollector` has clear interface; OTel config has `OtelConfig` dataclass with explicit fields |
| 9 | Parse, don't validate | 2 | small | low | `OtelConfig` resolves env vars once at bootstrap; `RootConfigSpec` provides typed boundary parse |
| 10 | Illegal states | 2 | small | low | `ExitCode` IntEnum prevents arbitrary exit codes; `OtelConfig` has typed sampler/filter fields; heartbeat uses mutable module-level dicts |
| 11 | CQS | 2 | small | low | `DiagnosticsCollector.record_*` methods both store state and emit OTel events (dual side-effect), but this is acceptable for observability |
| 12 | DI + explicit composition | 1 | medium | medium | `_registry()` uses module-level singleton cache; `obs.otel.tracing` uses module-level `ContextVar` with dict fallback |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; composition used throughout |
| 14 | Law of Demeter | 2 | small | low | `cli/telemetry.py:64-72` accesses `app.app_stack.resolve(...)` and `cyclopts._result_action` / `cyclopts._run` private APIs |
| 15 | Tell, don't ask | 2 | small | low | `engine_metrics_bridge.py` does extensive isinstance checking on `run_result` dict structure |
| 16 | Functional core | 2 | small | low | `diagnostics_report.py` is largely pure transforms; `runtime_capabilities_summary.py` is pure |
| 17 | Idempotency | 3 | - | - | `DataFusionRun.next_commit_version()` returns new immutable run; OTel bootstrap is safe to re-call |
| 18 | Determinism | 3 | - | - | Metric names/scopes canonicalized via enums; `run_id` threaded through context |
| 19 | KISS | 2 | small | low | `obs/otel/sampling.py` manually parses sampler args with ~70 lines when the OTel SDK handles this natively |
| 20 | YAGNI | 2 | small | low | `ConfiguredMeterProvider` subclass in `bootstrap.py:73` exposes `exemplar_filter` that no caller reads |
| 21 | Least astonishment | 2 | small | low | `cli/telemetry.py` uses private Cyclopts imports (`_run_maybe_async_command`, `_result_action`) |
| 22 | Public contracts | 2 | small | low | `obs/otel/__init__.py` declares `__all__` facade, but callers bypass it; CLI has `ExitCode` and `CliResult` contracts |
| 23 | Testability | 1 | medium | medium | `_REGISTRY_CACHE`, `_EXPORTERS`, `_HEARTBEAT_CONTEXT` are module-level mutable globals; OTel bootstrap has no DI seam for tests |
| 24 | Observability | 3 | - | - | Comprehensive: canonical metric names, scope enums, attribute normalization, heartbeat, diagnostics bundle, run context correlation |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
`obs/otel/__init__.py` declares a clean facade with lazy imports and `__all__` (lines 43-70). However, this facade is systematically bypassed.

**Findings:**
- 49 files outside `src/obs/` import directly from `obs.otel.tracing`, `obs.otel.metrics`, `obs.otel.scopes`, `obs.otel.run_context`, `obs.otel.logs`, `obs.otel.heartbeat`, and `obs.otel.cache`. Examples:
  - `/Users/paulheyse/CodeAnatomy/src/semantics/compiler.py:42-43` imports `SCOPE_SEMANTICS` and `stage_span` from `obs.otel.scopes` and `obs.otel.tracing` directly.
  - `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/session/facade.py:35-37` imports `record_datafusion_duration`, `record_error`, `record_write_duration` from `obs.otel.metrics` directly.
  - `/Users/paulheyse/CodeAnatomy/src/storage/deltalake/delta.py:33-35` imports `get_query_id`, `get_run_id` from `obs.otel.run_context` directly.
  - `/Users/paulheyse/CodeAnatomy/src/graph/product_build.py:26-27` imports `set_run_id`, `reset_run_id`, `record_exception`, `root_span`, `set_span_attributes` from OTel sub-modules directly.
- The facade `obs/otel/__init__.py` itself only exports a subset of what callers actually need (e.g., `stage_span` is heavily used but not in the facade `__all__`).

**Suggested improvement:**
Expand the `obs.otel.__init__` facade to re-export all symbols that external modules need (`stage_span`, `SCOPE_*`, `record_*`, `get_run_id`, `set_run_id`, `cache_span`, etc.). Then enforce (via lint rule or import boundary test) that files outside `src/obs/` only import from `obs.otel` or `obs`, never from sub-modules like `obs.otel.tracing`. This consolidates the surface area and allows internal refactoring of OTel modules without cross-codebase churn.

**Effort:** medium
**Risk if unaddressed:** high -- Any internal reorganization of `obs.otel.*` modules forces changes across 49+ files.

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The `obs/` module generally separates telemetry plumbing from domain logic. However, two areas mix concerns.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/obs/diagnostics.py` (332 lines) defines domain-specific artifact structs (`PreparedStatementSpec`, `SemanticQualityArtifact`) alongside OTel emission calls. These structs describe pipeline artifacts, not observability primitives, and would belong better in `serde_artifact_specs` or a domain-specific module.
- `/Users/paulheyse/CodeAnatomy/src/cli/app.py:114-274` -- `ObservabilityOptions` (161 lines of Annotated Parameter declarations) + `_build_otel_cli_overrides` (84 lines) + `_build_otel_options` (55 lines) = ~300 lines of OTel configuration plumbing embedded in the main CLI app file. This makes `app.py` change for two reasons: CLI structure changes and OTel configuration changes.

**Suggested improvement:**
Extract `ObservabilityOptions`, `_build_otel_cli_overrides`, and `_build_otel_options` from `cli/app.py` into a dedicated `cli/otel_options.py` module. Move `PreparedStatementSpec` and `SemanticQualityArtifact` from `obs/diagnostics.py` to `serde_artifact_specs` or keep them in `obs/` but in a separate `obs/artifact_types.py`.

**Effort:** medium
**Risk if unaddressed:** medium -- `app.py` currently has too many reasons to change.

---

#### P3. SRP -- Alignment: 2/3

**Current state:**
Most modules have a single clear responsibility. One notable exception.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/obs/metrics.py` serves three roles: (1) defines PyArrow schemas for dataset/column/scan stats (`DATASET_STATS_SCHEMA` etc. at lines 44-78), (2) provides PyArrow-level dataset stats computation functions (`column_stats_table`, `dataset_stats_table`), and (3) provides fragment-level scan utilities (`fragment_file_hints`, `list_fragments`, `row_group_count`). These are all data pipeline concerns, not observability concerns -- they happen to live in `obs` because they produce "observation" data, but they are Arrow table builders.

**Suggested improvement:**
Consider whether the PyArrow stats-table builders in `obs/metrics.py` belong in `datafusion_engine/stats/` or a dedicated `obs/stats_tables.py`, separating them from the OTel metric recording concerns in `obs/otel/metrics.py`. The naming collision between `obs/metrics.py` (Arrow tables) and `obs/otel/metrics.py` (OTel instruments) is confusing.

**Effort:** small
**Risk if unaddressed:** low -- naming confusion but no structural risk.

---

#### P4. High cohesion, low coupling -- Alignment: 1/3

**Current state:**
Within `obs/otel/`, cohesion is high -- each module has a clear role (config, bootstrap, tracing, metrics, etc.). Coupling to the rest of the codebase is the problem.

**Findings:**
- 49 files across 6 top-level packages (`semantics`, `extract`, `datafusion_engine`, `storage`, `graph`, `cli`) import directly from `obs.otel.*` sub-modules rather than through the facade, creating a wide coupling surface.
- `/Users/paulheyse/CodeAnatomy/src/cli/telemetry.py:12-13` imports from `cyclopts._result_action` and `cyclopts._run` -- private/internal APIs of the Cyclopts library. These are not part of Cyclopts' public contract and could break on minor version bumps.
- `/Users/paulheyse/CodeAnatomy/src/obs/diagnostics.py:14-19` imports 4 functions from `datafusion_engine.lineage.diagnostics`, coupling obs to datafusion_engine internals.

**Suggested improvement:**
For the Cyclopts private imports: if Cyclopts does not expose a public way to run commands programmatically with custom result handling, consider opening an issue upstream or wrapping the private calls in an adapter with a comment documenting the pin. For the obs/otel coupling: see P1 recommendation.

**Effort:** medium
**Risk if unaddressed:** high -- Cyclopts upgrade could break CLI; obs/otel coupling prevents internal refactoring.

---

#### P5. Dependency direction -- Alignment: 1/3

**Current state:**
The intended direction is: core (`semantics`, `relspec`, `cpg`) -> details (`obs`, `cli`, `storage`). Two violations exist.

**Findings:**
- **Critical:** `/Users/paulheyse/CodeAnatomy/src/serde_schema_registry.py:16` imports from `cli.config_models`. `serde_schema_registry` is a core serialization module used across the codebase; `cli.config_models` is an adapter-layer module. This creates a dependency from core toward the adapter layer, violating inward-only dependency direction.
- **Pervasive:** Core business modules (`semantics/compiler.py`, `semantics/pipeline.py`, `graph/product_build.py`, `graph/build_pipeline.py`, `extraction/orchestrator.py`, `datafusion_engine/session/facade.py`, etc.) import directly from `obs.otel.*`. While observability is a cross-cutting concern, the current pattern hardcodes the OTel implementation into core modules rather than depending on an abstraction.

**Suggested improvement:**
1. Move `RootConfigSpec` and related config model types from `cli/config_models.py` to a shared location (e.g., `runtime_models/` or `core/config_models.py`) so that `serde_schema_registry` does not depend on `cli/`. This is a small, high-impact change.
2. For the observability coupling: introduce a thin `obs.ports` module that defines protocol types or function signatures for `record_stage_duration`, `stage_span`, `get_run_id`, etc. Core modules would import from `obs.ports`; the OTel implementation in `obs.otel` would satisfy the protocols. This is a larger structural change.

**Effort:** small (config models move) / large (ports extraction)
**Risk if unaddressed:** high -- The `serde_schema_registry -> cli` dependency is an architectural violation that could cause circular import issues and prevents clean layering.

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
The CLI is correctly structured as an adapter (Cyclopts framework wrapping domain operations). The obs module, however, does not define ports -- it IS the implementation, and core modules call it directly.

**Findings:**
- No tracing port or metrics port exists. Core modules like `/Users/paulheyse/CodeAnatomy/src/semantics/compiler.py:42-43` import `stage_span` and `SCOPE_SEMANTICS` directly from OTel modules. If the team wanted to replace OTel with a different telemetry backend, every one of the 49+ calling modules would need modification.
- The `DiagnosticsCollector` in `/Users/paulheyse/CodeAnatomy/src/obs/diagnostics.py:49-99` is the closest thing to a port -- it provides an in-memory collection interface that the pipeline uses. But it hardcodes OTel emission inside its `record_*` methods (lines 61-62, 70-71, 78-79), making it impossible to use without OTel side effects.
- The `OtelDiagnosticsSink` in `/Users/paulheyse/CodeAnatomy/src/obs/otel/logs.py:26-100` provides a pure OTel sink, but it and `DiagnosticsCollector` are not unified behind a common protocol.

**Suggested improvement:**
Define a `DiagnosticsSink` protocol in `obs/ports.py` (or expand the one in `datafusion_engine/lineage/diagnostics.py`) that `DiagnosticsCollector` and `OtelDiagnosticsSink` both implement. Similarly, define a `TracingPort` protocol with `stage_span`, `get_tracer`, and `record_exception` that core modules depend on instead of importing OTel directly. The OTel adapter in `obs/otel/` would implement these ports. This enables testing core modules without OTel and allows future backend substitution.

**Effort:** large
**Risk if unaddressed:** high -- OTel vendor lock-in across 49+ core modules; testing core logic requires OTel SDK initialization.

---

### Category: Knowledge (7-11)

#### P7. DRY -- Alignment: 2/3

**Current state:**
Constants are well-centralized. Scope names, metric names, and attribute names all live in `obs/otel/constants.py` as StrEnums.

**Findings:**
- Scope constants are centralized in `/Users/paulheyse/CodeAnatomy/src/obs/otel/constants.py:47-62` and re-exported from `/Users/paulheyse/CodeAnatomy/src/obs/otel/scopes.py:7-21`. However, `scopes.py` also defines `_LAYER_SCOPE_MAP` (lines 23-32), which encodes a mapping that could drift from the constants.
- The `_RUN_ID_KEY = "codeanatomy.run_id"` constant is duplicated in `/Users/paulheyse/CodeAnatomy/src/obs/otel/processors.py:15` and `/Users/paulheyse/CodeAnatomy/src/obs/otel/processors.py:68`. It should reference `AttributeName.RUN_ID` from constants.

**Suggested improvement:**
Replace the hardcoded `"codeanatomy.run_id"` in `processors.py` with `AttributeName.RUN_ID.value`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Configuration models use typed dataclasses with explicit field types. `OtelConfig` in `/Users/paulheyse/CodeAnatomy/src/obs/otel/config.py:57-80+` has ~30 typed fields providing a clear contract.

**Findings:**
- `DiagnosticsCollector` in `/Users/paulheyse/CodeAnatomy/src/obs/diagnostics.py:49` uses `Mapping[str, object]` for both events and artifacts. The contract for what constitutes a valid event/artifact payload is implicit -- any dict with string keys is accepted.
- `CliResult` in `/Users/paulheyse/CodeAnatomy/src/cli/result.py:16` has clear contracts with typed factory methods `success()` and `error()`.

**Suggested improvement:**
No urgent action needed. The `Mapping[str, object]` pattern is appropriate for a flexible diagnostics sink. Consider adding a brief docstring to `DiagnosticsCollector` documenting expected payload shapes.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
OTel configuration is parsed once at bootstrap via `resolve_otel_config()` into a typed `OtelConfig` dataclass. CLI config is parsed from TOML/env into `RootConfigSpec`.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/cli/app.py:280-315` defines `_config_str`, `_config_bool`, `_config_int`, `_config_float` helpers that repeatedly extract and validate JSON config values. These are used in `_build_otel_options` (lines 468-525) which manually extracts ~15 config fields. This is "validate, don't parse" -- the config is never parsed into a single typed OTel config object at the CLI layer; instead, individual fields are extracted with fallback logic.

**Suggested improvement:**
Have `_build_otel_options` parse the `otel` config section into an `OtelConfigSpec` (which already exists in `/Users/paulheyse/CodeAnatomy/src/cli/config_models.py`) using msgspec decode, then map to `OtelBootstrapOptions`. This eliminates 15 individual field extractions.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Illegal states -- Alignment: 2/3

**Current state:**
`ExitCode` IntEnum prevents arbitrary exit codes. `ConfigSource` StrEnum constrains sources. However, mutable module-level state allows inconsistent runtime states.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/obs/otel/heartbeat.py:23-32` uses a mutable `dict` (`_HEARTBEAT_CONTEXT`) with string keys mapping to `int | None`. Any code can set arbitrary keys. A frozen dataclass would make the valid state explicit.
- `/Users/paulheyse/CodeAnatomy/src/obs/otel/diagnostics_bundle.py:37` uses `_EXPORTERS: dict[str, DiagnosticsExporters | None] = {"value": None}` -- a mutable singleton pattern that allows partial construction (some exporters registered, others None).

**Suggested improvement:**
Replace `_HEARTBEAT_CONTEXT` with a frozen dataclass that is replaced atomically. Replace `_EXPORTERS` with an `Optional[DiagnosticsExporters]` module-level variable (the dict-with-"value"-key pattern adds no value over a simple module-level Optional).

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. The `DiagnosticsCollector.record_*` methods both mutate internal state and emit OTel events, which is a dual command, but acceptable for observability sinks.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/obs/diagnostics.py:55-62`: `record_events` appends to internal buffer AND emits OTel events AND records artifact counts -- three side effects in one call. This is expected for a diagnostics sink pattern.
- No query functions have unexpected side effects.

**Suggested improvement:**
No action needed. The multi-effect pattern is standard for observability sinks.

**Effort:** N/A
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 1/3

**Current state:**
`obs.otel.metrics` uses a module-level singleton (`_REGISTRY_CACHE`) that is lazily initialized. OTel providers are assembled in `bootstrap.py:configure_otel()`. Dependencies are not injectable.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/obs/otel/metrics.py:130` -- `_REGISTRY_CACHE: dict[str, MetricsRegistry | None] = {"value": None}` is a global singleton. `_registry()` lazily creates and caches a `MetricsRegistry`. Test isolation requires calling `reset_metrics_registry()`, a function that exists solely for testing.
- `/Users/paulheyse/CodeAnatomy/src/obs/otel/tracing.py:21-25` -- `_ROOT_SPAN_CONTEXT` uses a ContextVar with a dict-based fallback (`_ROOT_SPAN_CONTEXT_FALLBACK`). The fallback is module-level mutable state.
- `/Users/paulheyse/CodeAnatomy/src/obs/otel/heartbeat.py:20-33` -- Five module-level mutable dicts for heartbeat state, with a module-level lock.
- `DiagnosticsCollector` in `obs/diagnostics.py` is properly injected into callers (good).

**Suggested improvement:**
For the metrics registry: pass `MetricsRegistry` as a parameter to functions that record metrics, or use a ContextVar-based registry that can be swapped in tests. For heartbeat state: encapsulate in a `HeartbeatState` dataclass passed through the context, rather than global mutable dicts.

**Effort:** medium
**Risk if unaddressed:** medium -- Global mutable state makes tests fragile and prevents parallel test execution.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No significant inheritance hierarchies exist. `ConfiguredMeterProvider(MeterProvider)` in `bootstrap.py:73` is the only subclass, and it adds a single attribute.

**Findings:**
None.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. Two violations noted.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/cli/telemetry.py:12-13` imports `cyclopts._result_action.handle_result_action` and `cyclopts._run._run_maybe_async_command` -- reaching into Cyclopts internals through private module paths.
- `/Users/paulheyse/CodeAnatomy/src/cli/telemetry.py:89-93` accesses `app.app_stack(...)`, then `app.parse_args(...)`, then passes `bound.arguments` to modify resolved parameters -- a chain of framework internal navigation.

**Suggested improvement:**
Investigate whether Cyclopts exposes a public `run` API that handles both parsing and execution. If not, wrap the private calls in a dedicated `cli/_cyclopts_compat.py` module with version-pinning comments, isolating the breakage risk to one file.

**Effort:** small
**Risk if unaddressed:** low (limited blast radius since only one file uses these)

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Most modules follow tell-don't-ask. One exception.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/obs/engine_metrics_bridge.py:14-80` -- `record_engine_metrics()` receives a `dict[str, object]` and performs extensive `isinstance` checking to extract metrics: `isinstance(trace_summary, dict)`, `isinstance(elapsed_nanos, (int, float))`, `isinstance(operator_metrics, (list, tuple))`, `isinstance(op_metric, dict)`, etc. This is "ask-then-act" on an untyped dict. The function should receive a typed `RunResult` struct instead.

**Suggested improvement:**
Define a typed `EngineRunResult` struct (or use one from the Rust engine bindings) and parse the raw dict into it at the boundary. Then `record_engine_metrics` can tell the typed object to emit its metrics.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
`diagnostics_report.py` and `runtime_capabilities_summary.py` are predominantly pure transforms. OTel bootstrap (`bootstrap.py`) is properly imperative-shell.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/obs/diagnostics_report.py` is a good example -- pure functions that transform span/log data into a structured `DiagnosticsReport`.
- The OTel metrics recording functions in `obs/otel/metrics.py` are inherently imperative (they record to global state), which is appropriate for observability at the boundary.

**Suggested improvement:**
No urgent action needed.

**Effort:** N/A
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
`DataFusionRun.next_commit_version()` at `/Users/paulheyse/CodeAnatomy/src/obs/datafusion_runs.py:43-78` returns a new immutable `DataFusionRun` with incremented commit sequence, supporting idempotent writes. OTel counter/histogram metrics are naturally idempotent (re-recording the same value is additive, not corrupting).

**Findings:**
None.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P18. Determinism -- Alignment: 3/3

**Current state:**
Metric names are canonicalized via `MetricName` StrEnum. Scope names via `ScopeName`. Run IDs are threaded through context. Resource attributes use deterministic resolution from env vars.

**Findings:**
None.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforward. A few areas add unnecessary complexity.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/obs/otel/sampling.py:37-72` -- `_parse_sampling_args()` manually parses positional and keyword arguments for the `should_sample` method with 36 lines of validation. The OTel SDK's `Sampler.should_sample()` has a fixed signature; this manual parsing adds complexity without clear benefit.
- The `dict[str, X | None] = {"value": None}` singleton pattern is used in at least 4 places (`_REGISTRY_CACHE`, `_EXPORTERS`, `_STAGE_FALLBACK`, `_ROOT_SPAN_CONTEXT_FALLBACK`). A simple module-level `Optional[X]` variable would be simpler and more conventional.

**Suggested improvement:**
Replace the `dict[str, X | None] = {"value": None}` pattern with module-level `Optional[X]` variables. Simplify `_parse_sampling_args` if the OTel SDK signature is stable.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Generally lean. One speculative extension noted.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/obs/otel/bootstrap.py:73-76` -- `ConfiguredMeterProvider(MeterProvider)` subclass that exposes `exemplar_filter` as a public attribute. No caller reads this attribute. This appears to be speculative: adding an extension point for a feature that is not yet used.

**Suggested improvement:**
Remove `ConfiguredMeterProvider` and use `MeterProvider` directly, passing the exemplar filter through the standard constructor.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
API naming is generally clear. Two surprises.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/obs/metrics.py` vs `/Users/paulheyse/CodeAnatomy/src/obs/otel/metrics.py` -- Two modules named `metrics.py` at different levels. `obs/metrics.py` contains PyArrow dataset stats table builders; `obs/otel/metrics.py` contains OTel metric instruments. A developer looking for "obs metrics" could easily find the wrong one.
- `/Users/paulheyse/CodeAnatomy/src/cli/telemetry.py:12-13` uses Cyclopts private imports which would surprise maintainers expecting stable framework APIs.

**Suggested improvement:**
Rename `obs/metrics.py` to `obs/dataset_stats.py` or `obs/stats_tables.py` to disambiguate from the OTel metrics module.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
`obs/otel/__init__.py` declares a 24-symbol `__all__` facade. `cli/__init__.py` exports `CliResult`, `ExitCode`, `main`. Exit codes have a taxonomy with ranges.

**Findings:**
- The `obs/otel/__init__.py` facade at `/Users/paulheyse/CodeAnatomy/src/obs/otel/__init__.py:43-70` is well-declared but not enforced -- callers bypass it (see P1).
- `stage_span`, the most-imported function from `obs.otel.tracing`, is NOT in the facade's `__all__`. It should be if it is part of the public contract.

**Suggested improvement:**
Add `stage_span`, `SCOPE_*` constants, `get_run_id`, `set_run_id`, `cache_span`, and other heavily-used symbols to the `obs.otel.__init__` facade.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Testability -- Alignment: 1/3

**Current state:**
Module-level mutable globals are the primary testability obstacle.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/obs/otel/metrics.py:130` -- `_REGISTRY_CACHE` global singleton requires `reset_metrics_registry()` between tests.
- `/Users/paulheyse/CodeAnatomy/src/obs/otel/diagnostics_bundle.py:37` -- `_EXPORTERS` global dict requires manual setup/teardown.
- `/Users/paulheyse/CodeAnatomy/src/obs/otel/heartbeat.py:20-33` -- Five module-level mutable dicts + locks for heartbeat state.
- `/Users/paulheyse/CodeAnatomy/src/obs/otel/tracing.py:25` -- `_ROOT_SPAN_CONTEXT_FALLBACK` global dict.
- Testing `DiagnosticsCollector` requires OTel to be initialized (since `record_*` methods call `emit_diagnostics_event` and `record_artifact_count` unconditionally).

**Suggested improvement:**
Make `DiagnosticsCollector.record_*` methods optionally emit to OTel (guarded by a flag or by dependency injection of the OTel sink). For heartbeat, replace module-level dicts with a `HeartbeatState` class that can be created fresh per test. For metrics, consider a ContextVar-based registry.

**Effort:** medium
**Risk if unaddressed:** medium -- Test fragility and inability to run obs tests in parallel.

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
This IS the observability module and it is comprehensive.

**Findings:**
- Canonical metric names via `MetricName` StrEnum at `/Users/paulheyse/CodeAnatomy/src/obs/otel/constants.py:8-24`.
- Canonical scope names via `ScopeName` StrEnum at `/Users/paulheyse/CodeAnatomy/src/obs/otel/constants.py:47-62`.
- Canonical attribute names via `AttributeName` StrEnum at `/Users/paulheyse/CodeAnatomy/src/obs/otel/constants.py:27-44`.
- Attribute normalization with redaction at `/Users/paulheyse/CodeAnatomy/src/obs/otel/attributes.py:40-46`.
- Run context correlation via `ContextVar` at `/Users/paulheyse/CodeAnatomy/src/obs/otel/run_context.py:7-8`.
- Heartbeat with stage tracking at `/Users/paulheyse/CodeAnatomy/src/obs/otel/heartbeat.py:46-76`.
- Diagnostics bundle with in-memory export capture at `/Users/paulheyse/CodeAnatomy/src/obs/otel/diagnostics_bundle.py:28-35`.
- CLI invocation telemetry with structured events at `/Users/paulheyse/CodeAnatomy/src/cli/telemetry.py:30-40`.

**Suggested improvement:**
No action needed. The observability infrastructure is well-designed and comprehensive.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

## Cross-Cutting Themes

### Theme 1: OTel Implementation Leak

**Root cause:** The `obs.otel` facade exists but is not enforced. Core modules across the codebase directly import from `obs.otel.tracing`, `obs.otel.metrics`, `obs.otel.scopes`, and `obs.otel.run_context`.

**Affected principles:** P1 (information hiding), P4 (coupling), P5 (dependency direction), P6 (ports & adapters), P22 (public contracts), P23 (testability).

**Approach:** Three-phase fix: (1) Expand the `obs.otel.__init__` facade to re-export all needed symbols. (2) Migrate all 49+ external callers to import from `obs.otel` instead of sub-modules. (3) Add an import boundary test (similar to existing `tests/unit/cq/architecture/test_import_boundaries.py`) that enforces the rule.

### Theme 2: Module-Level Mutable State

**Root cause:** OTel requires global state (meter providers, tracer providers are global by design). The codebase follows this pattern with module-level dicts for caching and fallback state.

**Affected principles:** P10 (illegal states), P12 (DI), P19 (KISS), P23 (testability).

**Approach:** Replace `dict[str, X | None] = {"value": None}` singletons with simpler `Optional[X]` module-level variables. For heartbeat and metrics registry, consider ContextVar-based storage that can be scoped per test.

### Theme 3: Reverse Dependency from Core to Adapter

**Root cause:** `serde_schema_registry.py` (core serialization) imports config model types from `cli.config_models` (CLI adapter). This likely happened because config models were first defined in the CLI and then needed downstream.

**Affected principles:** P5 (dependency direction).

**Approach:** Move the config model types (`RootConfigSpec`, `CacheConfigSpec`, `DataFusionCacheConfigSpec`, etc.) from `cli/config_models.py` to a shared location that both `cli/` and `serde_schema_registry` can depend on (e.g., `runtime_models/config_specs.py` or `core/config_models.py`).

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P5 | Move config model types out of `cli/config_models.py` to break `serde_schema_registry -> cli` reverse dependency | small | Fixes critical layering violation |
| 2 | P1/P22 | Expand `obs.otel.__init__` facade to re-export `stage_span`, `SCOPE_*`, `get_run_id`, etc. | small | Prepares for enforcement of facade boundary |
| 3 | P7 | Replace hardcoded `"codeanatomy.run_id"` in `obs/otel/processors.py` with `AttributeName.RUN_ID.value` | small | Eliminates knowledge duplication |
| 4 | P19 | Replace `dict[str, X] = {"value": None}` pattern with `Optional[X]` variables (4 locations) | small | Reduces unnecessary complexity |
| 5 | P21 | Rename `obs/metrics.py` to `obs/dataset_stats.py` to disambiguate from `obs/otel/metrics.py` | small | Eliminates naming confusion |

## Recommended Action Sequence

1. **Move config models** (P5, quick win #1): Extract `RootConfigSpec` and related types from `cli/config_models.py` to `runtime_models/config_specs.py`. Update imports in `serde_schema_registry.py` and `cli/config_loader.py`. This eliminates the most critical layering violation with minimal risk.

2. **Expand obs.otel facade** (P1/P22, quick win #2): Add `stage_span`, all `SCOPE_*` constants, `get_run_id`, `set_run_id`, `reset_run_id`, `cache_span`, `emit_diagnostics_event`, `OtelDiagnosticsSink`, and other externally-used symbols to `obs/otel/__init__.py`.

3. **Migrate external callers to facade** (P1/P4): Update the 49+ files that import from `obs.otel.tracing`, `obs.otel.metrics`, `obs.otel.scopes`, `obs.otel.run_context` to import from `obs.otel` instead. Automated with a search-and-replace pass.

4. **Add import boundary test** (P1): Create a test in `tests/unit/architecture/` that asserts no file outside `src/obs/` imports from `obs.otel.*` sub-modules (only from `obs.otel` or `obs`).

5. **Extract ObservabilityOptions** (P2/P3): Move `ObservabilityOptions`, `_build_otel_cli_overrides`, and `_build_otel_options` from `cli/app.py` to `cli/otel_options.py`.

6. **Simplify module-level state** (P19, quick win #4): Replace `dict[str, X | None] = {"value": None}` with `Optional[X]` in `obs/otel/metrics.py`, `obs/otel/diagnostics_bundle.py`, `obs/otel/tracing.py`, `obs/otel/heartbeat.py`.

7. **Fix knowledge duplication** (P7, quick win #3): Replace `_RUN_ID_KEY = "codeanatomy.run_id"` in `processors.py` with `AttributeName.RUN_ID.value`.

8. **Wrap Cyclopts private imports** (P14/P21): Isolate `cyclopts._result_action` and `cyclopts._run` imports into `cli/_cyclopts_compat.py` with version-pinning comments.

9. **Long-term: Define observability ports** (P6): Introduce `obs/ports.py` with `TracingPort` and `MetricsPort` protocols. Migrate core modules to depend on these protocols. Wire OTel implementation via DI or ContextVar-based adapters. This is the largest change and should be done after steps 1-8 stabilize.
