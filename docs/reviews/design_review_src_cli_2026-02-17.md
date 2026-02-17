# Design Review: src/cli

**Date:** 2026-02-17
**Scope:** `src/cli/` (CLI layer) + `src/graph/` (build entry point) + `src/runtime_models/` (validation models) + `src/planning_engine/` (engine config)
**Focus:** All principles (1-24), with emphasis on boundaries (1-6), simplicity (19-22), quality (23-24)
**Depth:** moderate
**Files reviewed:** 18

## Executive Summary

The CLI layer is well-structured as an outer-ring module that appropriately depends on inner and middle layers. The `cli/app.py` uses cyclopts for command registration with a clean `meta_launcher` pattern for config/OTel injection. The `runtime_models/` package provides Pydantic-based validation with a clear separation between spec types (msgspec) and runtime validation types (Pydantic). The `graph/product_build.py` serves as the primary build entry point with appropriate outer-ring dependencies. Key improvement areas include the `cli/config_loader.py` complexity, the dual-framework pattern (msgspec for specs, Pydantic for validation), and the `cli/config_models.py` re-export layer.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 3 | - | - | CLI internals well-hidden; commands loaded lazily |
| 2 | Separation of concerns | 2 | small | low | Config loading mixes IO + parsing + validation |
| 3 | SRP | 2 | medium | medium | `app.py` handles both framework setup and command registration |
| 4 | High cohesion, low coupling | 2 | small | low | `runtime_models/` is cohesive; `cli/` has appropriate coupling |
| 5 | Dependency direction | 3 | - | - | Outer ring correctly depends on inner/middle rings |
| 6 | Ports & Adapters | 2 | medium | low | No explicit port for config source; `ConfigWithSources` is informal |
| 7 | DRY | 2 | medium | medium | Config spec and runtime model define same structure twice |
| 8 | Design by contract | 2 | small | low | `RootConfigRuntime` enforces contracts; `extra="forbid"` is explicit |
| 9 | Parse, don't validate | 2 | small | low | Config loaded as TOML then parsed to typed specs at boundary |
| 10 | Make illegal states unrepresentable | 2 | small | low | Frozen models; Literal types for enums; optional fields nullable |
| 11 | CQS | 3 | - | - | Config loading returns config; build returns result; clean separation |
| 12 | DI + explicit composition | 3 | - | - | `RunContext` injected via `meta_launcher`; clean DI pattern |
| 13 | Composition over inheritance | 2 | small | low | `RuntimeBase` is single-level inheritance; acceptable for Pydantic |
| 14 | Law of Demeter | 3 | - | - | No deep chain access |
| 15 | Tell, don't ask | 3 | - | - | Config objects encapsulate their validation logic |
| 16 | Functional core, imperative shell | 2 | small | low | Config resolution is functional; app.py correctly imperative |
| 17 | Idempotency | 3 | - | - | Config loading is idempotent; build is idempotent with same inputs |
| 18 | Determinism | 3 | - | - | Same config produces same build request |
| 19 | KISS | 2 | small | low | Config resolution has deprecated-key translation complexity |
| 20 | YAGNI | 2 | small | low | `cli/result_action.py`, `cli/completion.py` may be underused |
| 21 | Least astonishment | 3 | - | - | CLI commands follow standard patterns |
| 22 | Public contracts | 2 | small | low | `__all__` defined; config spec is the public contract |
| 23 | Testability | 2 | small | low | `RunContext` is injectable; config loader is testable with temp files |
| 24 | Observability | 3 | - | - | OTel bootstrapped in meta_launcher; spans for all commands |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 3/3

**Current state:**
CLI commands are loaded lazily via string references in `app.py`. Internal modules (`config_loader`, `config_source`, `converters`, `kv_parser`, `path_utils`, `validators`) are implementation details not exposed through `__init__.py`.

**Findings:**
- `src/cli/app.py:1-648` uses cyclopts with lazy command loading via string references (e.g., `"cli.commands.build:build_command"`)
- `src/cli/__init__.py` and `src/cli/__main__.py` expose only the entry point
- Command implementations in `cli/commands/` are correctly isolated

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
As an outer-ring module, `cli/` correctly depends on inner/middle rings. No violations detected.

**Findings:**
- `src/cli/app.py:24` imports from `obs.otel` (inner ring) -- correct
- `src/cli/config_loader.py:13` imports from `cli.config_models` which re-exports from `core.config_specs` (inner ring) -- correct
- `src/cli/config_loader.py:14-15` imports from `runtime_models.adapters` and `runtime_models.root` (inner ring) -- correct
- `src/cli/context.py:14-16` uses TYPE_CHECKING imports from `obs.otel` and `cli.runtime_services` -- correct
- `src/graph/product_build.py:18` imports from `obs.otel` -- correct for outer ring

**Suggested improvement:**
No action needed. Dependency directions are correct.

**Effort:** -
**Risk if unaddressed:** -

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The CLI uses `RunContext` as an injected context object but does not define formal port interfaces for config sources or runtime services.

**Findings:**
- `src/cli/context.py:20-47` `RunContext` is a frozen dataclass with optional `runtime_services`, `config_sources`, and `otel_options`. This serves as an informal port for command injection
- `src/cli/runtime_services.py` likely defines `CliRuntimeServices` as the service bundle -- this acts as an implicit adapter bundle
- No protocol-based port definition means commands depend on concrete `RunContext` rather than an abstract interface

**Suggested improvement:**
Define a `CommandContext` protocol in `cli/ports.py` that `RunContext` implements. This would allow alternative context implementations for testing without needing the full `RunContext` setup.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge

#### P7. DRY -- Alignment: 2/3

**Current state:**
Config structure is defined twice: once as msgspec Structs in `core/config_specs.py` and again as Pydantic models in `runtime_models/root.py`. The two must be kept in sync manually.

**Findings:**
- `src/core/config_specs.py:1-206` defines `RootConfigSpec` with nested `PlanConfigSpec`, `CacheConfigSpec`, etc. as msgspec Structs
- `src/runtime_models/root.py:1-175` defines `RootConfigRuntime` with matching `PlanConfigRuntime`, `CacheConfigRuntime`, etc. as Pydantic BaseModels
- Both define the same field names and types, but in different frameworks. Changes to one must be manually reflected in the other
- `src/cli/config_models.py:1-15` is a thin re-export facade from `core.config_specs` -- known issue #2 is **resolved** (it no longer imports from outer ring)

**Suggested improvement:**
Consider generating the Pydantic runtime models from the msgspec specs, or using a shared schema definition that both frameworks can consume. Alternatively, document the sync requirement prominently and add a test that verifies field parity between spec and runtime model types.

**Effort:** medium
**Risk if unaddressed:** medium -- Schema drift between spec and runtime models could cause config values to be silently dropped or rejected.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Config is loaded as TOML (raw dict), then validated through Pydantic runtime models, then converted to typed specs. The boundary parsing is correct but involves multiple conversion steps.

**Findings:**
- `src/cli/config_loader.py` loads TOML to dict, validates via `ROOT_CONFIG_ADAPTER.validate_python()`, then converts to `RootConfigSpec`
- `src/runtime_models/adapters.py:1-22` defines `TypeAdapter` instances that perform the parse-at-boundary step
- The flow is: TOML file -> raw dict -> Pydantic validation -> msgspec spec. This is a 3-step boundary parse that could be simplified

**Suggested improvement:**
Consider validating directly into msgspec Structs using `msgspec.convert()` rather than the Pydantic intermediate. This would eliminate one framework dependency and reduce the conversion chain.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Composition

#### P12. DI + explicit composition -- Alignment: 3/3

**Current state:**
The `meta_launcher` pattern in `app.py` creates `RunContext` and injects it into all commands. Config, OTel options, and runtime services are composed and injected explicitly.

**Findings:**
- `src/cli/app.py` `meta_launcher()` creates `RunContext` with all dependencies explicitly composed
- Commands receive `RunContext` as a parameter, not via global state
- `src/graph/product_build.py:30-60` `GraphProductBuildRequest` is a frozen dataclass used as an explicit parameter to `build_graph_product()`

**Suggested improvement:**
No action needed. This is a clean DI pattern.

**Effort:** -
**Risk if unaddressed:** -

---

#### P13. Composition over inheritance -- Alignment: 2/3

**Current state:**
`runtime_models/base.py` defines `RuntimeBase(BaseModel)` as a single-level inheritance base. All runtime models inherit from it.

**Findings:**
- `src/runtime_models/base.py:8-16` `RuntimeBase` extends `BaseModel` with shared `ConfigDict` settings
- All 14 runtime model classes in `runtime_models/root.py` inherit from `RuntimeBase`
- This is single-level inheritance for shared configuration, which is the standard Pydantic pattern. No deep hierarchies exist

**Suggested improvement:**
No action needed. Single-level inheritance for framework integration is acceptable.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity

#### P19. KISS -- Alignment: 2/3

**Current state:**
Config loading includes deprecated key translation and parent-directory TOML search, adding complexity beyond the core load-parse-validate flow.

**Findings:**
- `src/cli/config_loader.py` searches parent directories for `codeanatomy.toml` or `pyproject.toml` with `[tool.codeanatomy]` section
- Deprecated config key translation (e.g., old key names to new ones) adds conditional logic
- The dual-framework approach (msgspec + Pydantic) for config types adds conceptual overhead

**Suggested improvement:**
Document the deprecated key translation with sunset dates. Consider consolidating to a single framework for config types once Pydantic runtime models can be replaced.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Several CLI modules appear to have limited usage.

**Findings:**
- `src/cli/completion.py` -- shell completion support (justified for CLI UX)
- `src/cli/result_action.py` -- result action handling (may be underused)
- `src/cli/kv_parser.py` -- key-value parsing for CLI arguments (utility, justified)
- `src/planning_engine/config.py:43-46` `EngineExecutionOptions.runtime_config` is typed as `object | None` -- overly generic, may indicate a placeholder for future functionality

**Suggested improvement:**
Audit `cli/result_action.py` usage. If it serves only one command, inline it. Tighten `EngineExecutionOptions.runtime_config` to a concrete type or protocol once the target type is known.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
`RunContext` is injectable. Config loading can be tested with temporary TOML files. Runtime models are independently testable via `TypeAdapter`.

**Findings:**
- `src/cli/context.py:20-47` `RunContext` is frozen and injectable -- good for testing
- `src/runtime_models/adapters.py:12-15` `TypeAdapter` instances are module-level constants that can be used independently in tests
- `src/graph/product_build.py` `build_graph_product()` takes a `GraphProductBuildRequest` -- testable with constructed requests
- Signal handler setup in `build_graph_product()` may interfere with test runners if not properly isolated

**Suggested improvement:**
Make signal handler registration optional or extract it to a composable wrapper, so tests can call `build_graph_product()` without installing signal handlers.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Dual-Framework Config Validation

**Root cause:** The codebase uses msgspec Structs for serialization contracts and Pydantic BaseModels for runtime validation. Config types exist in both frameworks, requiring manual synchronization.

**Affected principles:** P7 (DRY), P9 (parse don't validate), P19 (KISS)

**Approach:** Long-term, consolidate to msgspec-only config validation using `msgspec.convert()` with custom `dec_hook` for the validation logic currently in Pydantic models. Short-term, add a test that asserts field parity between spec and runtime model types.

### Theme 2: Clean Outer-Ring Boundaries

**Root cause:** The CLI and graph modules correctly sit in the outer ring with appropriate inward dependencies.

**Affected principles:** P5 (dependency direction) -- positively

**Approach:** Maintain this pattern. The `meta_launcher` -> `RunContext` -> command injection pattern is a model for other outer-ring modules.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 DRY | Add test asserting field parity between config spec and runtime model | small | Catches schema drift before runtime |
| 2 | P19 KISS | Document deprecated config keys with sunset timeline | small | Reduces confusion for maintainers |
| 3 | P20 YAGNI | Audit `cli/result_action.py` usage | small | Remove or inline if underused |
| 4 | P23 Testability | Make signal handler registration optional in `build_graph_product` | small | Cleaner test isolation |
| 5 | P6 Ports | Define `CommandContext` protocol for command injection | medium | Enables lightweight test contexts |

## Recommended Action Sequence

1. **Add config parity test** (P7) -- Write a test that compares field names between `RootConfigSpec` and `RootConfigRuntime` and their nested types, ensuring no drift.

2. **Document deprecated config keys** (P19) -- Add sunset dates and migration instructions to each deprecated key translation in `config_loader.py`.

3. **Extract signal handler setup** (P23) -- Make signal handler registration in `build_graph_product()` configurable via a parameter to improve test isolation.

4. **Tighten EngineExecutionOptions types** (P20, P10) -- Replace `object | None` with concrete types for `runtime_config` and `extraction_config` in `planning_engine/config.py`.

5. **Long-term: Consolidate config frameworks** (P7, P9, P19) -- Migrate runtime validation from Pydantic to msgspec, eliminating the dual-framework pattern and the `runtime_models/` package.
