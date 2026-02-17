# Design Review: src/obs

**Date:** 2026-02-17
**Scope:** `src/obs/` (observability layer) + `src/serde_artifacts.py`, `src/serde_artifact_specs.py`
**Focus:** All principles (1-24), with emphasis on boundaries (1-6), simplicity (19-22), quality (23-24)
**Depth:** moderate
**Files reviewed:** 22

## Executive Summary

The `src/obs/` module provides a comprehensive observability layer built on OpenTelemetry, with structured metrics, tracing, diagnostics, and log emission. The OTel sub-package (`obs/otel/`) is well-decomposed internally with clean constant definitions, scope management, and a unified lazy-loading facade. However, the module suffers from a critical dependency direction violation: multiple files in `obs/` import directly from `datafusion_engine` (engine ring), inverting the intended inner-to-outer dependency flow. The `serde_artifacts.py` and `serde_schema_registry.py` hub modules further compound this by acting as cross-cutting gravity wells that pull in types from every architectural ring.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Lazy facades work well; some callers bypass facade |
| 2 | Separation of concerns | 1 | large | high | `obs/diagnostics.py` and `obs/metrics.py` mix OTel plumbing with DataFusion domain types |
| 3 | SRP | 2 | medium | medium | `obs/otel/config.py` handles resolution, validation, and defaults (~840 LOC) |
| 4 | High cohesion, low coupling | 1 | large | high | `obs/` depends on `datafusion_engine` (engine ring), violating ring boundaries |
| 5 | Dependency direction | 0 | large | high | Inner-ring module imports from engine ring in 6 files |
| 6 | Ports & Adapters | 1 | medium | medium | No port abstractions for DataFusion/Arrow types consumed by obs |
| 7 | DRY | 2 | small | low | Scope constants have single authority; minor duplication in metric recording |
| 8 | Design by contract | 2 | small | low | OtelConfig has clear defaults; validation in resolve_otel_config |
| 9 | Parse, don't validate | 2 | small | low | `resolve_otel_config()` does boundary parsing; env vars parsed once |
| 10 | Make illegal states unrepresentable | 2 | small | low | Frozen dataclasses; StrEnum for constants |
| 11 | CQS | 3 | - | - | Record functions are commands; query functions are pure |
| 12 | DI + explicit composition | 2 | medium | medium | `configure_otel` uses DI; `_STATE` singleton pattern for providers |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; pure composition |
| 14 | Law of Demeter | 2 | small | low | Generally good; `metrics_snapshot` accesses provider internals |
| 15 | Tell, don't ask | 3 | - | - | `record_*` functions encapsulate metric logic |
| 16 | Functional core, imperative shell | 2 | medium | medium | Config resolution is mostly pure; bootstrap has global side effects |
| 17 | Idempotency | 2 | small | low | `configure_otel` is mostly idempotent; `reset_providers_for_tests` not |
| 18 | Determinism | 3 | - | - | Config resolution is deterministic given inputs |
| 19 | KISS | 2 | small | low | Lazy facade pattern is simple but duplicated across modules |
| 20 | YAGNI | 2 | small | low | `GaugeStore` is well-justified; no obvious speculative code |
| 21 | Least astonishment | 2 | small | low | `from obs.otel import stage_span` is intuitive but bypasses facade |
| 22 | Public contracts | 2 | small | low | `__all__` defined everywhere; no version markers |
| 23 | Testability | 1 | medium | medium | `reset_providers_for_tests` patches OTel SDK internals; bootstrap has global state |
| 24 | Observability | 3 | - | - | The module IS the observability layer; self-instrumented via heartbeat |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The `obs/otel/__init__.py` provides a comprehensive lazy-loading facade re-exporting ~50 symbols. The `obs/__init__.py` provides a narrower facade for metrics/stats functions. Internal module structure (`obs/otel/attributes.py`, `obs/otel/scope_metadata.py`) is well-hidden.

**Findings:**
- The facade at `src/obs/otel/__init__.py:1-215` correctly hides internal module structure via `_EXPORT_MAP`
- 55 files across the codebase import from `obs.otel` directly (the facade), which is correct behavior
- However, 0 files import from `obs.otel.*` sub-modules outside `src/obs/` -- the facade bypass issue from known issue #1 is **not confirmed**; external consumers use `from obs.otel import ...` (the facade), not `from obs.otel.metrics import ...`
- Internal `obs/` files do import from `obs.otel.*` sub-modules (e.g., `obs/metrics.py:33`, `obs/diagnostics.py:22-23`), which is acceptable for sibling access within the package

**Suggested improvement:**
No action needed. The facade pattern is working as intended for external consumers.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`obs/diagnostics.py` mixes OTel event emission with DataFusion domain types. `obs/metrics.py` mixes Arrow table construction with observability metrics.

**Findings:**
- `src/obs/diagnostics.py:14-21` imports `DiagnosticsSink`, `ViewArtifact`, `ValidationViolation` from `datafusion_engine`. The `DiagnosticsCollector` class at line 46 both records OTel events AND understands DataFusion schema contracts
- `src/obs/metrics.py:16-32` imports 9 symbols from `datafusion_engine.arrow.*` and `datafusion_engine.encoding.*`. Functions like `dataset_stats_table()` and `column_stats_table()` construct Arrow tables directly, coupling observation concerns with data representation
- `src/obs/scan_telemetry.py:11-12` imports from `datafusion_engine.arrow` for schema serialization
- `src/obs/datafusion_runs.py:18` imports `DiagnosticsSink` from `datafusion_engine.lineage.diagnostics`

**Suggested improvement:**
Extract DataFusion-dependent observation logic into a separate `obs/engine/` sub-package or into `datafusion_engine/obs/` adapters. The `obs/` core should define protocols (e.g., `StatsTableBuilder`, `DiagnosticsPort`) that DataFusion-specific adapters implement. This would keep `obs/` free of engine-ring dependencies while preserving functionality.

**Effort:** large
**Risk if unaddressed:** high -- This violation makes it impossible to use `obs/` without pulling in the entire DataFusion engine, creating circular dependency risk and bloating import time for lightweight consumers.

---

#### P5. Dependency direction -- Alignment: 0/3

**Current state:**
`obs/` is classified as an inner-ring module. The dependency direction map requires that inner-ring modules depend only on other inner-ring modules (`utils`, `core`, `core_types`, `arrow_utils`, `validation`). Six files in `obs/` violate this by importing from `datafusion_engine` (engine ring) and `storage` (middle ring).

**Findings:**
- `src/obs/diagnostics.py:14,20,21` -- imports from `datafusion_engine.lineage.diagnostics`, `datafusion_engine.schema.contracts`, `datafusion_engine.views.artifacts`
- `src/obs/metrics.py:16-32` -- imports from `datafusion_engine.arrow.abi`, `datafusion_engine.arrow.build`, `datafusion_engine.arrow.encoding`, `datafusion_engine.arrow.interop`, `datafusion_engine.encoding.policy`, `datafusion_engine.identity`
- `src/obs/scan_telemetry.py:11-12` -- imports from `datafusion_engine.arrow.abi`, `datafusion_engine.arrow.interop`
- `src/obs/datafusion_runs.py:16,18` -- imports from `storage.deltalake.delta_read`, `datafusion_engine.lineage.diagnostics`
- `src/obs/datafusion_engine_runtime_metrics.py:10-11` -- imports from `obs.otel.scopes`, `obs.otel.tracing` (acceptable, intra-package)
- `src/obs/engine_metrics_bridge.py:26` -- TYPE_CHECKING import from `obs.otel` (acceptable)

**Suggested improvement:**
Move `obs/diagnostics.py`, `obs/metrics.py`, `obs/scan_telemetry.py`, `obs/datafusion_runs.py` to a new `obs/engine_bridge/` sub-package with explicit documentation that these are engine-ring adapters, not inner-ring modules. Alternatively, relocate them to `datafusion_engine/obs/` so the dependency direction is correct (engine depends on inner, not vice versa).

**Effort:** large
**Risk if unaddressed:** high -- Circular dependency risk; prevents clean modularization; forces engine-ring imports for any observability consumer.

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
The OTel layer does not define port abstractions for the DataFusion types it consumes. Instead, it directly imports concrete engine types.

**Findings:**
- `src/obs/diagnostics.py:44-45` uses `DiagnosticsSink` and `ViewNode` from `datafusion_engine` as TYPE_CHECKING imports but still depends on concrete types at runtime (line 14-21)
- No protocol or ABC defines the observation-side contract that DataFusion adapters could implement
- `src/obs/otel/config.py:21` imports from `runtime_models.adapters` -- this is a runtime validation concern leaking into config resolution

**Suggested improvement:**
Define `DiagnosticsPort`, `StatsPort`, and `TelemetryPort` protocols in `obs/ports.py`. Have `datafusion_engine` provide adapter implementations. This inverts the dependency: engine depends on obs ports, not obs depending on engine internals.

**Effort:** medium
**Risk if unaddressed:** medium

---

### Category: Knowledge

#### P7. DRY -- Alignment: 2/3

**Current state:**
Scope constants are well-centralized in `obs/otel/scopes.py` and `obs/otel/constants.py`. Metric names are canonical StrEnums.

**Findings:**
- `src/obs/otel/constants.py:1-65` defines `MetricName`, `AttributeName`, `ScopeName`, `ResourceAttribute` as single-authority enums -- excellent DRY
- `src/obs/otel/scopes.py:1-78` re-exports scope constants as module-level names (e.g., `SCOPE_ROOT = ScopeName.ROOT`) and provides `scope_for_layer()` helper
- Minor: `src/obs/otel/metrics.py` and `src/obs/metrics.py` both construct Arrow schemas for stats tables; the schema knowledge is split between the two

**Suggested improvement:**
No urgent action. The minor schema duplication between `obs/otel/metrics.py` (OTel metrics) and `obs/metrics.py` (Arrow stats tables) could be consolidated but serves different purposes.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Configuration types use frozen dataclasses and StrEnums to constrain valid states.

**Findings:**
- `src/obs/otel/config.py:19-35` `OtelConfig` is frozen with typed fields; `service_name`, `enable_tracing`, etc. have sensible defaults
- `src/obs/otel/bootstrap.py:68-92` `OtelBootstrapOptions` is frozen with strongly-typed fields
- However, `OtelConfig.service_name` defaults to `"codeanatomy"` rather than requiring explicit specification, which could mask misconfiguration in multi-service deployments

**Suggested improvement:**
No action needed for current use case. The defaults are appropriate for a single-project tool.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
`configure_otel()` accepts options via `OtelBootstrapOptions` (good DI). However, the module uses a module-level `_STATE` dict for singleton provider management.

**Findings:**
- `src/obs/otel/bootstrap.py` uses `_STATE: dict[str, object] = {}` at module level as a singleton pattern for storing `TracerProvider`, `MeterProvider`, `LoggerProvider`
- `src/obs/otel/bootstrap.py:267-320` `OtelProviders.activate_global()` sets OTel SDK global state, making the configuration effectively a singleton
- `src/obs/otel/metrics.py:160-180` `_REGISTRY_CACHE` stores the lazily-initialized `MetricsRegistry`
- These singletons are necessary for OTel SDK integration but make composition and testing harder

**Suggested improvement:**
Document the singleton pattern as an intentional architectural choice for OTel SDK compatibility. The `reset_providers_for_tests()` escape hatch is appropriate given the constraint.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Config resolution (`resolve_otel_config`) is mostly pure/functional. Bootstrap (`configure_otel`) is correctly at the imperative edge. Record functions are thin wrappers over OTel SDK.

**Findings:**
- `src/obs/otel/config.py` `resolve_otel_config()` is largely deterministic given inputs (env vars read at call time)
- `src/obs/otel/bootstrap.py` `configure_otel()` is correctly imperative -- it creates providers, sets global state
- `src/obs/otel/tracing.py:30-50` `stage_span()` context manager mixes span creation (imperative) with metric recording (side effect) -- acceptable for instrumentation code

**Suggested improvement:**
No action needed. The functional/imperative split is appropriate for an observability layer.

**Effort:** -
**Risk if unaddressed:** low

---

### Category: Simplicity

#### P19. KISS -- Alignment: 2/3

**Current state:**
The lazy-loading `__getattr__` facade pattern is effective but adds complexity. The OTel config resolution chain has ~840 LOC with multiple fallback paths.

**Findings:**
- `src/obs/otel/config.py` has ~840 LOC for config resolution including env var fallbacks, spec conversion, override merging -- this is the most complex file in the module
- The lazy-loading `__getattr__` + `_EXPORT_MAP` + `__all__` + `__dir__` pattern is duplicated across `obs/__init__.py`, `obs/otel/__init__.py`, and other modules
- `src/obs/otel/bootstrap.py` `reset_providers_for_tests()` at line ~720 directly manipulates OTel SDK private attributes (`_TRACER_PROVIDER_SET_ONCE`, `_METER_PROVIDER_SET_ONCE`, etc.) -- fragile coupling to SDK internals

**Suggested improvement:**
Extract the lazy-loading facade into a shared utility (e.g., `utils/lazy_module.py`) that generates `__getattr__`/`__dir__`/`__all__` from an `_EXPORT_MAP`. This would reduce boilerplate across 5+ modules using the same pattern.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
OTel integration testing requires `reset_providers_for_tests()` which patches OTel SDK private attributes. The module-level `_STATE` and `_REGISTRY_CACHE` singletons make isolation difficult.

**Findings:**
- `src/obs/otel/bootstrap.py:720+` `reset_providers_for_tests()` accesses `otel_api._TRACER_PROVIDER_SET_ONCE`, `otel_metrics._METER_PROVIDER_SET_ONCE`, `otel_logs._LOGGER_PROVIDER_SET_ONCE` -- these are SDK internal details that can break on version upgrades
- `src/obs/otel/metrics.py:160-180` `_REGISTRY_CACHE` singleton means tests must manually reset state
- `src/obs/otel/tracing.py` `_ROOT_SPAN_CONTEXT: ContextVar` is global state affecting all tests in a process

**Suggested improvement:**
Wrap the OTel provider lifecycle in a `ProviderLifecycle` class that encapsulates setup/teardown without reaching into SDK private attributes. Use the official `otel_test` utilities or `InMemorySpanExporter`/`InMemoryMetricReader` patterns for test isolation.

**Effort:** medium
**Risk if unaddressed:** medium -- OTel SDK version upgrades could break tests silently.

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
This IS the observability layer. It provides structured metrics, tracing spans, diagnostic event emission, heartbeat monitoring, and log aggregation. Self-instrumented via heartbeat and scope metadata.

**Findings:**
- Complete OTel integration: traces, metrics, logs, resource detection
- Scope hierarchy (`ScopeName` enum) provides clean instrumentation boundaries
- `metrics_snapshot()` provides runtime health inspection
- Heartbeat system (`obs/otel/heartbeat.py`) provides liveness monitoring

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

### Serde Hub Modules (`serde_artifacts.py`, `serde_artifact_specs.py`, `serde_schema_registry.py`)

#### P5. Dependency direction -- Alignment: 0/3 (for serde_schema_registry)

**Current state:**
`serde_schema_registry.py` acts as a gravity well, importing types from every architectural ring.

**Findings:**
- `src/serde_schema_registry.py:17-65` imports from `core.config_specs` (inner), `datafusion_engine.*` (~15 imports across 8 sub-modules, engine ring), `obs.otel` (inner), `relspec` (middle), `schema_spec` (inner), `semantics` (middle), `storage` (middle/outer)
- `src/serde_artifact_specs.py:16-17` imports from `relspec.execution_package` and `relspec.policy_calibrator` (middle ring)
- `src/serde_artifacts.py:10-14` imports primarily from `core_types`, `serde_msgspec`, `serde_msgspec_ext`, `utils.hashing` (all inner ring) -- this is clean
- The registry pattern requires knowing all types, creating an inherent hub, but the current location in `src/` root means it has no ring classification

**Suggested improvement:**
Accept `serde_schema_registry.py` as a cross-cutting concern that intentionally spans rings. Document it as a registration-time-only module that should only be imported at module initialization, not in hot paths. Consider splitting the type imports into lazy registration so the module does not force-load all engine types at import time.

**Effort:** medium
**Risk if unaddressed:** medium -- Import-time side effects slow cold starts; changes to any registered type require touching this file.

---

## Cross-Cutting Themes

### Theme 1: Inner-Ring Modules Importing from Engine Ring

**Root cause:** The `obs/` package was designed to provide observability for the entire pipeline, including DataFusion-specific metrics and diagnostics. Over time, engine-specific observation code accumulated in `obs/` rather than being placed in the engine layer.

**Affected principles:** P2 (separation of concerns), P4 (coupling), P5 (dependency direction), P6 (ports & adapters)

**Approach:** Create an explicit `obs/engine_bridge/` sub-package or relocate engine-specific observation code to `datafusion_engine/obs/`. Define port protocols in `obs/ports.py` for the general observation contract.

### Theme 2: Lazy-Loading Facade Boilerplate

**Root cause:** Multiple modules (`obs/__init__`, `obs/otel/__init__`, `schema_spec/__init__`, `runtime_models/__init__`) use the same `__getattr__` + `_EXPORT_MAP` pattern without a shared utility.

**Affected principles:** P7 (DRY), P19 (KISS)

**Approach:** Extract a `utils/lazy_module.py` helper that generates the facade functions from a mapping.

### Theme 3: Global Singleton State in OTel Bootstrap

**Root cause:** OTel SDK requires global provider registration. The module mirrors this with module-level `_STATE` dicts and cache objects.

**Affected principles:** P12 (DI), P23 (testability)

**Approach:** Accept as architectural constraint; improve test isolation by wrapping lifecycle in a proper class with documented reset semantics.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P19 KISS | Extract lazy-loading facade utility from duplicated `__getattr__` pattern | small | Reduces boilerplate in 5+ modules |
| 2 | P23 Testability | Replace SDK private attribute patching with official test utilities | medium | Prevents breakage on OTel SDK upgrades |
| 3 | P7 DRY | Consolidate scope re-export pattern (constants + module-level aliases) | small | Cleaner scope management |
| 4 | P3 SRP | Split `obs/otel/config.py` into `config_types.py` (dataclasses) and `config_resolution.py` (logic) | small | Easier to test config resolution in isolation |
| 5 | P5 Dependency | Move `obs/datafusion_runs.py` to `datafusion_engine/obs/` as first step | small | Reduces one dependency direction violation |

## Recommended Action Sequence

1. **Extract lazy-loading facade utility** (P19, P7) -- Create `utils/lazy_module.py` providing `make_lazy_loader(export_map)` returning `__getattr__` and `__dir__` functions. Refactor `obs/__init__`, `obs/otel/__init__`, `schema_spec/__init__`, `runtime_models/__init__` to use it.

2. **Relocate engine-bridge observation code** (P5, P2, P4, P6) -- Move `obs/diagnostics.py`, `obs/metrics.py`, `obs/scan_telemetry.py`, `obs/datafusion_runs.py` to `datafusion_engine/obs/` or `obs/engine_bridge/`. Define port protocols in `obs/ports.py`.

3. **Improve OTel test isolation** (P23) -- Replace `reset_providers_for_tests()` SDK private attribute access with official `InMemorySpanExporter` and `InMemoryMetricReader` patterns. Wrap provider lifecycle in a testable class.

4. **Split OTel config module** (P3) -- Separate `obs/otel/config.py` into type definitions and resolution logic for better testability and SRP.

5. **Document serde registry as cross-cutting** (P5) -- Add module docstring to `serde_schema_registry.py` explaining its intentional cross-ring nature and import-time registration contract.
