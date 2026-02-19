# Design Review: Python Infrastructure Modules

**Date:** 2026-02-18
**Scope:** `src/obs/`, `src/utils/`, `src/cli/`, `src/storage/`, `src/schema_spec/`, `src/runtime_models/`, `src/core/`, `src/arrow_utils/`, `src/cache/`, `src/validation/`, `src/test_support/`, `src/cpg/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 136 Python files (cpg/ is empty — only pycache present at review time)

---

## Executive Summary

These infrastructure modules are, on balance, well-architected. The observability stack (`src/obs/`) is a particularly strong example of Ports and Adapters applied consistently: `DiagnosticsPort` isolates consumers from OTel mechanics, `MetricName`/`AttributeName`/`ScopeName` enums form a single authoritative telemetry vocabulary, and `configure_otel` correctly centralises all provider wiring in one function. `src/utils/` is tight, cohesive, and dependency-clean. The cache, validation, and core modules are compact and focused.

The primary concerns sit in three areas. First, `src/storage/deltalake/` has a cross-module private-symbol dependency: `delta_write.py` imports `_normalize_commit_metadata`, `_DeltaMergeExecutionState`, and `_DeltaMergeExecutionResult` directly from `delta_read.py`, coupling two files at their implementation seams. Second, `src/schema_spec/` and `src/storage/deltalake/` both import pervasively from `datafusion_engine`, which belongs at the application layer, creating an upward dependency from infrastructure to the engine. Third, `src/cli/commands/build.py` contains a 250-line command handler with four private helper dataclasses, a mixed config-building and orchestration concern, and a complexity surface that accumulates all build-specific options in one place.

---

## Alignment Scorecard

| #  | Principle | Alignment | Effort | Risk | Key Finding |
|----|-----------|-----------|--------|------|-------------|
| 1  | Information hiding | 1 | small | medium | `delta_write.py` imports three private symbols from `delta_read.py` |
| 2  | Separation of concerns | 2 | medium | low | `build_command` mixes config assembly, SCIP config, incremental config, and orchestration |
| 3  | SRP | 2 | medium | low | `delta_read.py` owns both read operations and the merge execution state machine |
| 4  | High cohesion, low coupling | 2 | small | low | `schema_spec` tightly coupled to `datafusion_engine`; reasonable given the domain |
| 5  | Dependency direction | 1 | large | medium | `schema_spec/` and `storage/` depend upward into `datafusion_engine` (application-tier) |
| 6  | Ports & Adapters | 3 | — | — | `DiagnosticsPort`, `ControlPlanePort`, `MetricsSchemaPort` are well-defined ports |
| 7  | DRY | 2 | small | low | `OtelConfig` and `OtelConfigSpec` duplicate 25+ field names; intentional but comment-free |
| 8  | Design by contract | 2 | small | low | `DiagnosticsCollector.record_event` has no guard for empty `name`; `env_bool` returns `None` on invalid value without logging by default |
| 9  | Parse, don't validate | 3 | — | — | `env_utils` parses at boundary; `RuntimeBase` (pydantic) parses on construction |
| 10 | Illegal states unrepresentable | 3 | — | — | `ScanTelemetry` frozen struct, `DiskCacheSettings` constrained by `NonNegativeInt`/`PositiveInt` |
| 11 | CQS | 2 | small | low | `DiagnosticsCollector.record_event` records to memory AND emits OTel; `cache_for_kind` returns a value AND populates `_CACHE_POOL` |
| 12 | Dependency inversion + explicit composition | 2 | medium | low | `build_command` creates `SemanticIncrementalConfig`, `ScipIndexConfig`, `RuntimeConfig` internally — no injection point |
| 13 | Prefer composition over inheritance | 3 | — | — | `RuntimeBase`/`MutableRegistry` use inheritance minimally and appropriately |
| 14 | Law of Demeter | 2 | small | low | `build_command` traverses `options.incremental_state_dir`, `options.scip_python_bin` etc. directly, then re-packs into private structs |
| 15 | Tell, don't ask | 3 | — | — | `DiagnosticsPort` and telemetry helpers tell the sink what to record |
| 16 | Functional core, imperative shell | 2 | medium | low | `fragment_telemetry` in `scan_telemetry.py` both computes telemetry AND calls `set_scan_telemetry` (OTel side-effect) in the same function body |
| 17 | Idempotency | 3 | — | — | `configure_otel` guards re-entry via `_STATE`; cache pool uses fingerprints to avoid duplicates |
| 18 | Determinism / reproducibility | 3 | — | — | All hash helpers produce stable outputs; `CacheKeyBuilder` sorts components |
| 19 | KISS | 2 | medium | low | `OtelBootstrapOptions` has 22 fields; `build_command` has 40+ parameters across two dataclasses |
| 20 | YAGNI | 2 | small | low | `MappingRegistryAdapter` is a third registry implementation that appears unused in this scope |
| 21 | Least astonishment | 2 | small | low | `env_bool` returns `None` on invalid value without logging by default; callers may silently miss misconfiguration |
| 22 | Declare and version public contracts | 2 | small | low | `__all__` lists are comprehensive; no explicit stability annotations on ports or versioned schema types |
| 23 | Design for testability | 3 | — | — | `reset_providers_for_tests` and DiskCache pool are cleanly resettable; `DiagnosticsCollector` is memory-backed and injectable |
| 24 | Observability | 3 | — | — | Canonical `MetricName`/`AttributeName`/`ScopeName` enums, `stage_span` context manager, structured `CliInvokeEvent`, structured `ScanTelemetry` |

---

## Detailed Findings

### Category: Boundaries (P1-P6)

#### P1. Information Hiding — Alignment: 1/3

**Current state:**
`src/storage/deltalake/delta_write.py` imports three symbols prefixed with `_` from `delta_read.py`, bypassing that module's public surface. Line 29 imports `_normalize_commit_metadata`; lines 39-40 (TYPE_CHECKING) import `_DeltaMergeExecutionResult` and `_DeltaMergeExecutionState`; lines 227 and 297 repeat these imports inside function bodies at runtime.

**Findings:**
- `src/storage/deltalake/delta_write.py:29` — `from storage.deltalake.delta_read import _normalize_commit_metadata` (private helper).
- `src/storage/deltalake/delta_write.py:39-40` — TYPE_CHECKING imports of `_DeltaMergeExecutionState` and `_DeltaMergeExecutionResult`.
- `src/storage/deltalake/delta_write.py:227` and `297` — runtime imports of the same private state dataclasses inside `_build_delta_merge_state` and `_execute_delta_merge_state`.
- The merge state machine (`_DeltaMergeExecutionState`, `_DeltaMergeExecutionResult`) spans two files, with `delta_read.py` owning the type definitions and `delta_write.py` owning the construction and execution logic. This creates a shared-internals coupling that makes each file impossible to understand without the other.

**Suggested improvement:**
Move `_normalize_commit_metadata` and the merge execution dataclasses to a dedicated `delta_types_internal.py` (or promote them to the public surface of `delta_read.py` under non-prefixed names with documented semantics). Alternatively, co-locate the full merge state machine in `delta_write.py`, making `delta_read.py` responsible only for read operations. The goal is that neither file imports private symbols from the other.

**Effort:** small
**Risk if unaddressed:** medium — any internal restructuring of the merge flow requires coordinating both files, and the `_` prefix signals intent-to-hide that is already being violated.

---

#### P2. Separation of Concerns — Alignment: 2/3

**Current state:**
`src/cli/commands/build.py:455-715` contains `build_command` and its helpers. The function assembles five independent concerns in sequence: CLI config overrides, SCIP configuration, incremental configuration, orchestration request construction, and post-build logging. Each concern could in principle change independently.

**Findings:**
- `src/cli/commands/build.py:499-514` — `_CliConfigOverrides`, `_ScipOverrides`, `_IncrementalOverrides` are all private builder dataclasses defined in the same file as the command handler, mixing command-layer types with configuration-translation logic.
- `src/cli/commands/build.py:527-540` — `ScipIndexConfig.from_cli_overrides(...)` is called with ~12 positional keyword arguments, encoding SCIP config translation directly in the command function rather than delegating to a dedicated translator.
- `src/cli/commands/build.py:558-569` — `_build_incremental_config` is a private function that also creates `SemanticIncrementalConfig` from CLI options, mixing CLI concerns with domain model construction.

**Suggested improvement:**
Extract a `BuildCommandTranslator` (or equivalent module-level functions in `cli/commands/build_config.py`) that converts `BuildOptions`/`BuildRequestOptions` → `OrchestrateBuildRequestV1`. The command function itself becomes: load config, translate, call orchestrate, log. Each translator step changes for its own reason.

**Effort:** medium
**Risk if unaddressed:** low — the current structure is functional and the file changes infrequently, but every new build option requires editing the same large function.

---

#### P3. SRP — Alignment: 2/3

**Current state:**
`src/storage/deltalake/delta_read.py` owns at least three distinct responsibilities: (a) raw Delta read/write helpers (`coerce_delta_input`, `DeltaWriteResult`), (b) merge execution state machine (`_DeltaMergeExecutionState`, `_DeltaMergeExecutionResult`), and (c) commit metadata normalization (`_normalize_commit_metadata`). The file changes when the Arrow read API changes, when merge orchestration changes, and when commit metadata policy changes.

**Findings:**
- `src/storage/deltalake/delta_read.py:75-313` — types for reads, writes, snapshots, and CDF are all co-located.
- `src/storage/deltalake/delta_read.py:315-330` — private state machine dataclasses live here but are used only in `delta_write.py`.
- `src/storage/deltalake/delta_read.py:41-72` — commit metadata normalization logic, conceptually a policy/data concern, mixed into the read module.

**Suggested improvement:**
Consider splitting into: `delta_types.py` (public value types: `SnapshotKey`, `DeltaWriteResult`, `DeltaMergeArrowRequest` etc.), `delta_commit.py` (commit metadata normalization and `CommitProperties` construction), and `delta_merge_state.py` (the merge execution state machine). This makes `delta_write.py` import from purpose-named modules rather than peeking into `delta_read.py` internals.

**Effort:** medium
**Risk if unaddressed:** low — the current grouping is inconvenient rather than dangerous, but it makes adding merge-related features error-prone.

---

#### P4. High Cohesion, Low Coupling — Alignment: 2/3

**Current state:**
`src/schema_spec/` and `src/storage/deltalake/` have high internal cohesion but are coupled to `datafusion_engine` via 50+ direct imports (see P5). Within this scope, `src/utils/` stands out positively: `hashing.py`, `env_utils.py`, `storage_options.py`, `registry_protocol.py` each have narrow, cohesive interfaces with no cross-dependencies inside utils.

**Findings:**
- `src/obs/diagnostics.py:9-16` imports `datafusion_ext` (Rust extension) and multiple `serde_*` modules at module level. The side-effect import `_ = _datafusion_ext` on line 22 ensures the Rust extension is loaded as a side effect of importing the diagnostics module — coupling observability initialization to Rust extension availability.
- `src/cache/diskcache_factory.py:203` — `_CACHE_POOL: dict[str, Cache | FanoutCache] = {}` is a module-level mutable pool; `close_cache_pool()` is the only shutdown mechanism. This is a reasonable tradeoff for a persistent cache, but the pool is not thread-safe for concurrent writers (only the DiskCache instances themselves are safe).

**Suggested improvement:**
Remove the `import datafusion_ext as _datafusion_ext; _ = _datafusion_ext` pattern from `obs/diagnostics.py`. The OTel sink should not depend on Rust extension availability; the side-effect load belongs at the application bootstrap layer. If diagnostics requires the extension to function, model this as an explicit precondition checked at `DiagnosticsCollector` construction time.

**Effort:** small
**Risk if unaddressed:** low for the cache pool; medium for the Rust-extension side-effect, as it will silently fail or cause import errors when the extension is unavailable at test time.

---

#### P5. Dependency Direction — Alignment: 1/3

**Current state:**
Both `src/schema_spec/` and `src/storage/deltalake/` import from `datafusion_engine`, which sits at the application/engine layer above them. Infrastructure modules should depend inward on shared contracts, not outward on the engine that consumes them.

**Findings:**
- `src/schema_spec/dataset_spec_runtime.py:14-27` — 14 imports from `datafusion_engine.*` at module top level, covering Arrow types, Delta protocol, expression specs, kernel specs, schema alignment, finalization, and validation.
- `src/schema_spec/specs.py:20-31` — 12 imports from `datafusion_engine.*`.
- `src/storage/deltalake/delta_read.py:14-24` — 6 top-level imports from `datafusion_engine.*` (Arrow coercion, encoding, interop, application of encoding, error types, schema alignment).
- `src/storage/deltalake/delta_write.py:10` — `from datafusion_engine.session.helpers import deregister_table, register_temp_table`. This means that using the storage write module requires a live DataFusion `SessionContext`, coupling storage mutation to the session layer.
- `src/schema_spec/scan_policy.py:10` — `from datafusion_engine.extensions.schema_runtime import load_schema_runtime` brings in runtime extension machinery into a spec-level module.

This pattern is largely load-bearing: `schema_spec` types use Arrow schemas and DataFusion expression specs as their data model. Full inversion would require extracting an abstract Arrow/DataFusion interop layer. However, the `SessionContext` dependency in `delta_write.py` is directly addressable.

**Suggested improvement:**
For `delta_write.py`, inject `register_temp_table`/`deregister_table` as callables rather than importing them directly from the session layer. For `schema_spec`, document that this module accepts `datafusion_engine` as a stable shared platform dependency (analogous to depending on `pyarrow`), and confirm the policy explicitly in `schema_spec/__init__.py` or `AGENTS.md`. This reduces the perception gap without requiring a full architectural inversion.

**Effort:** large for full inversion; small for the `delta_write.py` SessionContext injection
**Risk if unaddressed:** medium — the current coupling makes `schema_spec` and `storage` impossible to test without a DataFusion environment, and means that DataFusion API changes ripple into both infrastructure modules.

---

#### P6. Ports & Adapters — Alignment: 3/3

**Current state:**
The observability stack correctly implements this principle. `src/obs/ports.py` defines `DiagnosticsPort` and `MetricsSchemaPort` as Protocols. `src/obs/diagnostics.py` provides the in-memory `DiagnosticsCollector` implementation. `src/storage/deltalake/ports.py` defines `ControlPlanePort` as a Protocol isolating storage from the Rust control plane. The OTel bootstrap returns `OtelProviders` which can be replaced by test fakes.

No action needed.

---

### Category: Knowledge (P7-P11)

#### P7. DRY — Alignment: 2/3

**Current state:**
`OtelConfig` (25+ fields, `obs/otel/config_types.py`) and `OtelConfigSpec` (same 25+ fields as optional strings/primitives, same file) form a parallel structure. This is a deliberate parse-then-use pattern — `OtelConfigSpec` is the serializable wire form and `OtelConfig` is the resolved runtime form. The duplication is intentional, but there is no comment explaining the relationship or why `OtelConfigSpec` cannot be the sole type. `OtelConfigOverrides` adds a third parallel structure with a subset of the same fields (12 of the same names).

**Findings:**
- `src/obs/otel/config_types.py:24-61` — `OtelConfig` with 25+ fields.
- `src/obs/otel/config_types.py:112-150` — `OtelConfigSpec` with the same 25+ fields as optional types.
- `src/obs/otel/config_types.py:153-168` — `OtelConfigOverrides` with 12 of the same field names again.
- When a new OTel config option is added (e.g., a new batch processor knob), it must be added to all three types, plus `OtelBootstrapOptions` in `bootstrap.py`, plus `ObservabilityOptions` in `cli/app.py`, plus the mapping function in `_build_otel_options`. That is five addition sites for a single config parameter.

**Suggested improvement:**
Add a module-level docstring to `config_types.py` explaining the three-tier pattern (spec → overrides → resolved config) and where each is appropriate. Consider defining a canonical field list as a shared constant or base class to make the synchronisation requirement explicit at code level. A table-driven mapping from config field names to their positions in the resolution stack would reduce the five-site update problem.

**Effort:** small
**Risk if unaddressed:** low — the duplication is bounded to one file and one concern, but it is a maintenance trap when adding new OTel knobs.

---

#### P8. Design by Contract — Alignment: 2/3

**Current state:**
Most boundary types are well-constrained: `DiskCacheSettings` uses `NonNegativeInt`/`PositiveInt` aliases, frozen structs prevent mutation, and the pydantic `RuntimeBase` validates on construction with `extra="forbid"`. However, a few precondition gaps exist at the util and diagnostics layer.

**Findings:**
- `src/utils/env_utils.py:207-246` — `env_bool` with `on_invalid="default"` and `log_invalid=False` silently returns the default when an invalid string is present. A caller setting `CODEANATOMY_ENABLE_TRACES=oops` will silently get the default `None`, which is indistinguishable from "not set." The function signature documents `on_invalid` but the default compound (`on_invalid="default"`, `log_invalid=False`) makes silent misconfiguration the default behavior.
- `src/obs/diagnostics.py:32-56` — `DiagnosticsCollector.record_event` accepts an empty `name: str` with no guard, which would produce metrics and OTel events under an empty string key.
- `src/cache/diskcache_factory.py:331-362` — `cache_for_kind` is documented to return a `Cache | FanoutCache` but silently creates a new instance if not found in the pool. There is no precondition check that `profile.root` is writable before attempting to open the cache.

**Suggested improvement:**
Change the default for `env_bool`'s `log_invalid` to `True` (or introduce `env_bool_strict` as the default recommeded function, demoting `env_bool` to opt-in-silent mode). Add an `if not name` guard to `DiagnosticsCollector.record_event` raising `ValueError("event name must be non-empty")`.

**Effort:** small
**Risk if unaddressed:** low for the diagnostics name guard; medium for the env_bool silent failure, as misconfigured observability will produce no error and no trace output.

---

#### P9. Parse, Don't Validate — Alignment: 3/3

**Current state:**
Parsing at the boundary is consistently applied. `src/utils/env_utils.py` converts raw string env vars to typed Python values (bool, int, float, enum, list) at the call site, never returning raw strings for consumption downstream. `src/runtime_models/base.py` uses pydantic's `validate_default=True, frozen=True` to parse untyped config dicts into validated frozen models at construction time. `src/obs/otel/config_resolution.py` resolves `OtelConfigSpec` → `OtelConfig` once at bootstrap.

No action needed.

---

#### P10. Make Illegal States Unrepresentable — Alignment: 3/3

**Current state:**
The data model design is strong throughout. `ScanTelemetry` is a frozen msgspec struct with `cache_hash=True`. `DiskCacheSettings` uses `NonNegativeInt` and `PositiveInt` type aliases that fail at construction. `ValidationViolation` uses `ViolationType` enum rather than raw strings. `ExitCode` is an enum in `cli/exit_codes.py`. `MetricName`, `AttributeName`, `ScopeName` all use `StrEnum`.

No action needed.

---

#### P11. CQS — Alignment: 2/3

**Current state:**
Most functions are either pure queries or clear commands. The pattern breaks in two places: `DiagnosticsCollector.record_event` both appends to `self.events` (command) and calls `emit_diagnostics_event` and `record_artifact_count` (OTel side-effects) inside the same method. Similarly, `fragment_telemetry` in `scan_telemetry.py` both computes and returns a `ScanTelemetry` value (query) and calls `set_scan_telemetry` to update OTel gauges (command, lines 188-192). `cache_for_kind` returns a `Cache` instance (query) and populates `_CACHE_POOL` (command).

**Findings:**
- `src/obs/diagnostics.py:32-56` — `record_event`, `record_events`, `record_artifact` each perform memory storage + OTel emission in the same call.
- `src/obs/scan_telemetry.py:154-207` — `fragment_telemetry` computes and returns `ScanTelemetry` but also calls `set_scan_telemetry` (OTel gauge mutation) at line 188-192.
- `src/cache/diskcache_factory.py:331-362` — `cache_for_kind` returns a value and populates a mutable global pool.

**Suggested improvement:**
For `fragment_telemetry`, remove the `set_scan_telemetry` call from the computation function and require callers to explicitly call `set_scan_telemetry` with the returned `ScanTelemetry`. This makes the telemetry emission optional and the computation testable without OTel. For `DiagnosticsCollector`, accept that command+emit is the intended semantics (the class is a combined collector+emitter), but document this explicitly in the class docstring as "records locally and emits to OTel simultaneously."

**Effort:** small
**Risk if unaddressed:** low — the combined semantics work correctly, but `fragment_telemetry` cannot be called in tests without triggering OTel metric writes.

---

### Category: Composition (P12-P15)

#### P12. Dependency Inversion + Explicit Composition — Alignment: 2/3

**Current state:**
`src/cli/commands/build.py:486-491` — `build_command` uses a lazy import (`from graph.build_pipeline import orchestrate_build`) to defer the dependency, which is acceptable. However, it also directly constructs `SemanticIncrementalConfig`, `ScipIndexConfig`, `RuntimeConfig`, and `OrchestrateBuildRequestV1` from CLI options, acting as a factory for domain objects. None of these domain objects are injected via the `run_context` or any composition root.

**Findings:**
- `src/cli/commands/build.py:527-540` — `ScipIndexConfig.from_cli_overrides(...)` is a static factory on a domain type, called directly from the CLI handler. If `ScipIndexConfig` changes its construction API, the CLI handler must change.
- `src/cli/commands/build.py:550-556` — `RuntimeConfig(compliance_capture=..., enable_rule_tracing=..., ...)` and `TracingConfig(...)` are constructed inline in the command body.
- `src/cli/commands/build.py:558-569` — `_build_incremental_config(...)` is a private factory in the CLI command module, producing `SemanticIncrementalConfig`. This is a domain-layer type assembled by a CLI-layer function.

**Suggested improvement:**
Extract a `BuildRequestAssembler` (or a set of pure translation functions in a separate `cli/commands/build_assembler.py`) that maps CLI option structs → domain request types. The `build_command` function then becomes: receive options → call assembler → call orchestrate → return code. This allows the assembler to be tested without invoking the CLI framework.

**Effort:** medium
**Risk if unaddressed:** low — the current approach is functionally correct, but it makes the CLI handler hard to unit-test and ties it tightly to domain construction APIs.

---

#### P13. Prefer Composition over Inheritance — Alignment: 3/3

**Current state:**
Inheritance use is minimal and appropriate: `RuntimeBase` (pydantic BaseModel) for the runtime config hierarchy, `ConfiguredMeterProvider` extending `MeterProvider` for a narrow attribute exposure, and `MutableRegistry`/`ImmutableRegistry` implementing the `Registry` protocol without extending each other. No deep hierarchies exist.

No action needed.

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
`build_command` in `cli/commands/build.py` accesses deeply into the CLI option structs (e.g., `options.scip_output_dir`, `options.incremental_state_dir`, `request.include_extract_errors`) and then re-packs them into private dataclasses (`_ScipOverrides`, `_IncrementalOverrides`, `_CliConfigOverrides`) before passing them on. This is a positive sign of intent (the private dataclasses isolate what is needed), but the command function itself touches 30+ fields of the option objects before that indirection.

**Findings:**
- `src/cli/commands/build.py:499-569` — six top-level assignments extract fields from `options` and `request`, then re-assemble them into three private structs, then those structs' fields are accessed again in helper functions. The final helper `_apply_cli_config_overrides` accesses `overrides.runtime_profile_name`, `overrides.determinism_override`, `overrides.incremental`, etc. — the same data now three hops deep.

**Suggested improvement:**
If the private dataclasses are the correct isolation mechanism, the extraction from `options` should happen inside each private dataclass's constructor or a `from_build_options` classmethod, not scattered through the command function body. This collapses the traversal from three hops to one.

**Effort:** small
**Risk if unaddressed:** low — the current structure is readable, just verbose.

---

#### P15. Tell, Don't Ask — Alignment: 3/3

**Current state:**
The ports pattern ensures that callers tell sinks what to record: `sink.record_event(name, properties)`, `sink.record_artifact(spec, payload)`. No raw data inspection of internal sink state by callers is present in scope. `DiskCacheProfile.settings_for(kind)` encapsulates the override-vs-base selection logic internally.

No action needed.

---

### Category: Correctness (P16-P18)

#### P16. Functional Core, Imperative Shell — Alignment: 2/3

**Current state:**
The hashing utilities (`utils/hashing.py`) and fingerprinting (`core/fingerprinting.py`) are purely functional. `scan_telemetry.py` partially violates this: `fragment_telemetry` computes a `ScanTelemetry` struct (functional) but also mutates global OTel gauge state (`set_scan_telemetry`, lines 188-192). The OTel bootstrap `configure_otel` is appropriately imperative and clearly at the shell layer. `DiagnosticsCollector` is mixed: it accumulates in-memory (functional-ish) but also emits to OTel (imperative).

**Findings:**
- `src/obs/scan_telemetry.py:186-192` — inside the otherwise-pure `fragment_telemetry` computation, `set_scan_telemetry` is called when `dataset_name` is present, producing an OTel side-effect in the middle of a data-gathering function.
- `src/obs/otel/bootstrap.py:709-716` — `configure_otel` mutates `os.environ` directly (`os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] = "http,db"`), which is a process-global side-effect from within the bootstrap function. This is acceptable for bootstrap, but `setdefault` is safer than assignment to avoid overriding user-set values (line 716 uses plain assignment).

**Suggested improvement:**
For `fragment_telemetry`, see P11 suggestion. For `configure_otel`, change line 716 to `os.environ.setdefault("OTEL_SEMCONV_STABILITY_OPT_IN", "http,db")` to avoid clobbering user-configured semconv settings.

**Effort:** small
**Risk if unaddressed:** low for the semconv override; low-medium for `fragment_telemetry` (it affects test isolation and correct OTel emission ordering).

---

#### P17. Idempotency — Alignment: 3/3

**Current state:**
`configure_otel` is idempotent via `_STATE["providers"]` guard (line 668-669). `cache_for_kind` is idempotent via fingerprint pool lookup. `close_cache_pool` and `reset_providers_for_tests` are provided as explicit tear-down points for test isolation. Delta write operations use idempotent commit properties via `IdempotentWriteOptions`.

No action needed.

---

#### P18. Determinism / Reproducibility — Alignment: 3/3

**Current state:**
All hash utilities produce stable outputs for the same input: `hash_msgpack_canonical` uses `MSGPACK_ENCODER` (deterministic ordered encoding), `hash_json_canonical` uses `JSON_ENCODER_SORTED`, `hash_sha256_hex` is purely deterministic. `CompositeFingerprint.from_components` sorts components by name. `DiskCacheProfile.fingerprint` includes root, kind, and settings fingerprint deterministically. `CacheKeyBuilder.build` uses `hash_msgpack_canonical` over a dict (which is insertion-ordered in Python 3.7+ but may vary across Python versions — this is acceptable given the version pin to 3.13).

No action needed.

---

### Category: Simplicity (P19-P22)

#### P19. KISS — Alignment: 2/3

**Current state:**
The `OtelBootstrapOptions` dataclass has 22 fields. The `ObservabilityOptions` dataclass in `cli/app.py` replicates 17 of those fields as CLI parameters with full `Parameter(name=..., help=..., group=...)` annotations, totalling approximately 300 lines for OTel parameter declarations alone. This is not gratuitous — each parameter corresponds to a real OTel knob — but the sheer surface area makes the bootstrap and CLI module dense.

**Findings:**
- `src/cli/app.py:116-274` — `ObservabilityOptions` with 17 annotated OTel parameter fields.
- `src/cli/app.py:494-551` — `_build_otel_options` manually maps each `ObservabilityOptions` field to an `OtelBootstrapOptions` field, a 50-line function that is mechanically isomorphic.
- `src/obs/otel/bootstrap.py:218-246` — `OtelBootstrapOptions` with 22 fields, many of which correspond 1:1 to `OtelConfig` fields.
- `src/cli/commands/build.py:117-411` — `BuildOptions` has ~30 annotated fields covering SCIP, incremental, scope, advanced, observability concerns.

**Suggested improvement:**
Consider grouping the BSP/BLRP batch processor settings in `OtelBootstrapOptions` into a nested `BatchProcessorOptions` struct (one per signal: traces, metrics, logs). This reduces the flat 22-field struct to ~6 top-level fields plus three nested 4-field structs, and the `_build_otel_options` mapping becomes structurally cleaner.

**Effort:** medium
**Risk if unaddressed:** low — the current structure is correct; complexity cost is primarily readability and maintenance of the five-site OTel param synchronisation problem (see P7).

---

#### P20. YAGNI — Alignment: 2/3

**Current state:**
`src/utils/registry_protocol.py` exports three registry implementations: `MutableRegistry`, `ImmutableRegistry`, and `MappingRegistryAdapter`. The `MappingRegistryAdapter` adds a third flavor that exposes `entries` as a public dict field, supports `read_only` and `allow_overwrite` modes, and offers `from_mapping`. Within this reviewed scope, no consumer of `MappingRegistryAdapter` was found (it is not used in obs, storage, schema_spec, cli, cache, validation, core, or arrow_utils). This may serve callers in other modules not in scope, but the existence of three parallel registry implementations without visible differentiated use is speculative.

**Findings:**
- `src/utils/registry_protocol.py:236-335` — `MappingRegistryAdapter` is a full 100-line implementation with public `entries` dict, `read_only` mode, `allow_overwrite` mode, `snapshot`/`restore`, and `from_mapping`. It duplicates most of `MutableRegistry`'s methods with additional options.

**Suggested improvement:**
Validate whether `MappingRegistryAdapter` has callers outside this scope. If not, remove it or consolidate its functionality into `MutableRegistry` (e.g., by adding a `read_only` flag there). If it does have callers, document its intended use case in the module docstring to distinguish it from `MutableRegistry`.

**Effort:** small
**Risk if unaddressed:** low — unused code is harmless, but it adds cognitive load when choosing a registry implementation.

---

#### P21. Least Astonishment — Alignment: 2/3

**Current state:**
The `env_bool` function default behavior (`on_invalid="default"`, `log_invalid=False`) will silently return the default when an invalid string is present. A caller who sets `MY_BOOL_FLAG=yes` will get `True` (correct), but a caller who sets `MY_BOOL_FLAG=enabled` will silently get the configured default with no log output. The function offers `log_invalid=True` as an opt-in, but the default behavior is the surprising one: environment variable misconfiguration produces no indication.

`env_bool_strict` (introduced separately) correctly defaults `log_invalid=True`, but `env_bool` — which is the more general and more likely to be called — does not.

**Findings:**
- `src/utils/env_utils.py:207-246` — `env_bool` has `log_invalid: bool = False` as default.
- `src/utils/env_utils.py:254-283` — `env_bool_strict` has `log_invalid: bool = True` as default.
- The asymmetry in defaults between the two functions means callers of the more general `env_bool` are more likely to have silent failures.

**Suggested improvement:**
Change the default for `env_bool`'s `log_invalid` parameter to `True`. If callers need silent behavior for intentional reasons, they can pass `log_invalid=False` explicitly.

**Effort:** small
**Risk if unaddressed:** low under normal operation; medium when operators misconfigure environment variables and expect error messages.

---

#### P22. Declare and Version Public Contracts — Alignment: 2/3

**Current state:**
All modules define `__all__` clearly. The port protocols in `obs/ports.py` and `storage/deltalake/ports.py` are small and stable. However, there is no explicit stability annotation system: no `@stable`, `@unstable`, or module-level comment distinguishing "stable public surface" from "internal implementation." The `OtelConfig` and `DiskCacheSettings` fingerprinting methods imply stability contracts but there is no declared versioning scheme for when fingerprints will change.

**Findings:**
- `src/obs/ports.py` — `DiagnosticsPort` and `MetricsSchemaPort` are stable protocol surfaces, but nothing marks them as such.
- `src/utils/registry_protocol.py` — three registry implementations; no guidance on which is the preferred public API.
- `src/core/fingerprinting.py` — `CompositeFingerprint` carries a `version: int` field, which is the right approach, but the version number carries no documentation of what changes would require a version bump.

**Suggested improvement:**
Add a brief module-level docstring to `obs/ports.py` and `utils/registry_protocol.py` noting which types are stable public contracts and which are internal. In `core/fingerprinting.py`, document the versioning policy: "increment `version` when the component set or hashing algorithm changes." This costs nothing at runtime and prevents ambiguity for maintainers.

**Effort:** small
**Risk if unaddressed:** low.

---

### Category: Quality (P23-P24)

#### P23. Design for Testability — Alignment: 3/3

**Current state:**
The infrastructure is highly testable. `DiagnosticsCollector` is an in-memory implementation of `DiagnosticsPort` — injectable in place of the OTel-emitting version for unit tests. `reset_providers_for_tests()` in `bootstrap.py` and `reset_metrics_registry()` in `metrics.py` provide explicit test-isolation tear-down hooks. `ImmutableRegistry.from_dict` and `MutableRegistry` are pure value objects. The `DiskCacheProfile`-based `cache_for_kind` is testable with a temp-dir profile.

No action needed.

---

#### P24. Observability — Alignment: 3/3

**Current state:**
This is the strongest principle in scope. `obs/otel/constants.py` provides canonical `MetricName`, `AttributeName`, `ScopeName`, and `ResourceAttribute` enums — a single authoritative vocabulary. `stage_span` context manager attaches stage, status, and duration to every traced operation automatically. `CliInvokeEvent` captures structured timing and error classification for every CLI invocation. `ScanTelemetry` is a rich structured telemetry payload for dataset scans. Metrics use explicit histogram bucket configurations. All scopes are named via `ScopeName` enum.

The only minor gap is that `fragment_telemetry` emits to OTel gauges inside a computation function (see P11/P16), which makes the emission order implicit. This is a low-severity concern.

No action needed on the core observability design.

---

## Cross-Cutting Themes

### Theme 1: Private-Symbol Coupling Across Storage Files

`delta_write.py` depends on `_normalize_commit_metadata`, `_DeltaMergeExecutionState`, and `_DeltaMergeExecutionResult` from `delta_read.py`. This is not accidental: the merge execution state was likely in `delta_write.py` originally and was moved to `delta_read.py` during a refactor without fully resolving the type ownership. The result is that the public/private boundary between these two files is illusory.

**Root cause:** The merge operation spans both read-setup (constructing the state from a `DeltaMergeArrowRequest`) and write-execution (running the merge via the Rust control plane). When this was split across two files, the shared state types ended up in the file where the split happened to be convenient, not where they conceptually belong.

**Affected principles:** P1 (Information hiding), P3 (SRP), P12 (Dependency inversion).

**Approach:** Move the merge execution types to a dedicated `delta_merge.py` or `delta_types.py` module. Neither `delta_read.py` nor `delta_write.py` should import private symbols from the other.

---

### Theme 2: Infrastructure Coupling to DataFusion Engine

`schema_spec/` and `storage/` both import pervasively from `datafusion_engine`, which is an application-layer module. This means that any change to the DataFusion Arrow interop API, schema alignment, or encoding policy API propagates into what should be stable infrastructure modules.

**Root cause:** The schema spec and storage modules grew organically alongside `datafusion_engine`, treating it as a platform library rather than a client. The distinction between "stable shared platform" (like `pyarrow`) and "application engine" (like `datafusion_engine`) was never codified.

**Affected principles:** P5 (Dependency direction), P12 (Dependency inversion).

**Approach:** Document `datafusion_engine.arrow.*` as a declared stable sub-platform (analogous to pyarrow) that `schema_spec` and `storage` are permitted to depend on. Separately, treat `datafusion_engine.session.*` and `datafusion_engine.delta.control_plane_core` as application-layer APIs that `storage` depends on through narrow call sites. Consider if `storage.deltalake.delta_write` could accept a `merge_fn: Callable[..., object]` rather than importing from `datafusion_engine.delta.control_plane_core` at runtime, removing the hard dependency on the control plane.

---

### Theme 3: OTel Configuration Parameter Proliferation

The OTel configuration knobs appear five times across the codebase: `OtelConfigSpec` (msgspec), `OtelConfig` (resolved dataclass), `OtelConfigOverrides` (override set), `OtelBootstrapOptions` (bootstrap-layer options), and `ObservabilityOptions` (CLI parameters). Adding a new OTel parameter requires updating all five.

**Root cause:** Each layer was added for a legitimate reason (serialization, resolution, test overrides, bootstrap composition, CLI exposure), but the layers were never consolidated at the parameter-name level.

**Affected principles:** P7 (DRY), P19 (KISS).

**Approach:** Define a canonical parameter name set as a Python `TypedDict` or dataclass in `config_types.py`, and derive each layer's struct from it via composition. A lighter-weight approach: add a code comment block listing all OTel parameter names and the five locations where each must be added, reducing the "hunt for all locations" problem.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P1 (Information hiding) | Promote or relocate `_normalize_commit_metadata`, `_DeltaMergeExecutionState`, `_DeltaMergeExecutionResult` out of `delta_read.py` private namespace so `delta_write.py` can import them cleanly | small | Removes private-import anti-pattern in storage layer |
| 2 | P21 (Least astonishment) | Change `env_bool` default `log_invalid` to `True` | small | Prevents silent env var misconfiguration |
| 3 | P16 (Functional core) | Extract OTel emission from `fragment_telemetry` — separate computation from metric write | small | Makes telemetry computation unit-testable without OTel setup |
| 4 | P8 (Design by contract) | Add `if not name: raise ValueError(...)` guard to `DiagnosticsCollector.record_event` | small | Prevents empty-string metric/event emission |
| 5 | P16 (Functional core) | Change `obs/otel/bootstrap.py:716` to `os.environ.setdefault("OTEL_SEMCONV_STABILITY_OPT_IN", "http,db")` instead of assignment | small | Respects user-set semconv configuration |

---

## Recommended Action Sequence

1. **Relocate merge execution types (P1, P3):** Create `src/storage/deltalake/delta_merge_types.py` containing `_DeltaMergeExecutionState` and `_DeltaMergeExecutionResult` (without the `_` prefix on the new home). Move `_normalize_commit_metadata` and `_normalize_commit_metadata_key` to `delta_commit.py`. Update `delta_read.py` and `delta_write.py` to import from the new modules.

2. **Fix env_bool default logging (P21, P8):** Change `env_bool` `log_invalid` default to `True`. Audit existing callers for `log_invalid=False` and confirm intent.

3. **Decouple fragment_telemetry from OTel emission (P11, P16):** Remove the `set_scan_telemetry` call from `fragment_telemetry`. Update callers to call `set_scan_telemetry` explicitly after receiving the `ScanTelemetry` result. This requires finding all callers (use `/cq calls fragment_telemetry` before making the change).

4. **Add record_event name guard (P8):** Add `if not name: raise ValueError("event name must be non-empty")` to `DiagnosticsCollector.record_event` and `record_events`.

5. **Fix semconv env assignment (P16):** `obs/otel/bootstrap.py:716` — change `os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] = "http,db"` to `os.environ.setdefault(...)`.

6. **Document or remove MappingRegistryAdapter (P20):** Verify callers. If none exist in-scope, remove it or add a clear use-case docstring with an example.

7. **Add stability annotations to port protocols (P22):** Add a one-line stability comment to `obs/ports.py` and `storage/deltalake/ports.py` noting that these are stable infrastructure contracts.

8. **Document schema_spec DataFusion dependency policy (P5):** Add to `src/schema_spec/AGENTS.md` (or the existing `AGENTS.md`) a declaration that `datafusion_engine.arrow.*` is a permitted platform dependency for schema_spec, while `datafusion_engine.session.*` and `datafusion_engine.delta.control_plane_core` are application-layer APIs used only in storage (not in schema_spec). This codifies intent and prevents future drift.

9. **Address OTel parameter proliferation (P7, P19):** Add a developer note to `obs/otel/config_types.py` listing the five locations where OTel parameters must be kept in sync. Long-term, consider a table-driven approach using a shared canonical field list.
