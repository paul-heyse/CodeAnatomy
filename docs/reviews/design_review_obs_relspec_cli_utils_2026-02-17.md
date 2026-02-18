# Design Review: obs / relspec / cli / graph / core / runtime_models / planning_engine / utils / arrow_utils / serde_*

**Date:** 2026-02-17
**Scope:** `src/obs/`, `src/relspec/`, `src/cli/`, `src/graph/`, `src/core/`, `src/runtime_models/`, `src/planning_engine/`, `src/utils/`, `src/arrow_utils/`, `src/serde_artifacts.py`, `src/serde_artifact_specs.py`, `src/serde_schema_registry.py`, `src/serde_msgspec.py`
**Focus:** Composition (P12-15), Simplicity (P19-22), Quality (P23-24), with agent-specific sections on DF52 impact, Rust migration, and planning-object consolidation
**Depth:** moderate
**Files reviewed:** 20 representative files across ~100 in scope

---

## Executive Summary

The scope is broadly well-structured. Key design achievements include clean port-protocol separation in `relspec/ports.py`, excellent canonical constants in `obs/otel/constants.py`, strong msgspec contract discipline in `serde_msgspec.py` and `relspec/compiled_policy.py`, and a clearly partitioned entry-point surface at `graph/product_build.py`. The two most significant findings are (1) a **coercion-utility proliferation** spanning three separate modules (`utils/coercion.py`, `utils/value_coercion.py`, and inline private helpers in `cli/commands/build.py`) that violates DRY and creates behavioral drift risk, and (2) a **pydantic / msgspec split** in `runtime_models/` that imposes two validation layers with overlapping semantics on config paths. Both are medium-effort, medium-risk improvements. KISS and testability scores are pulled down by the 905-LOC `build.py` command file whose SCIP config assembly logic is not independently testable.

---

## Alignment Scorecard

| #  | Principle                        | Alignment | Effort | Risk   | Key Finding |
|----|----------------------------------|-----------|--------|--------|-------------|
| 12 | Dependency inversion + explicit composition | 2 | small | low | `obs/otel/bootstrap.py` builds exporters internally; no exporter port protocol |
| 13 | Prefer composition over inheritance | 3 | — | — | No problematic inheritance; msgspec Struct bases used well |
| 14 | Law of Demeter | 2 | small | low | `policy_compiler.py` walks `semantic_ir.views[*].inferred_properties.inferred_join_strategy` three levels deep |
| 15 | Tell, don't ask | 2 | small | low | `graph/product_build.py` performs multi-field attribute probing on `build_result` via `getattr` guards |
| 19 | KISS | 1 | medium | medium | Three coercion modules + `cli/commands/build.py` has 240-LOC SCIP payload parsing private to one command |
| 20 | YAGNI | 3 | — | — | No speculative generality observed; protocol ports are minimal |
| 21 | Least astonishment | 2 | small | low | `obs/otel/bootstrap.py`: `configure_otel()` is idempotent by global dict, not explicitly documented; `_STATE` uses dict-as-mutable-cell |
| 22 | Declare and version public contracts | 2 | small | low | `serde_artifact_specs.py` (1493 LOC) has mixed typed/untyped specs — untyped specs are a documentation debt |
| 23 | Design for testability | 1 | medium | medium | `cli/commands/build.py` `_build_scip_config` (240 LOC) is not independently callable without a full config dict; `obs/otel/bootstrap.py` global `_STATE` requires `reset_providers_for_tests()` side-channel |
| 24 | Observability | 3 | — | — | `obs/otel/` layer is comprehensive; canonical scope names, metric names, and structured diagnostics events are consistently used |

---

## Detailed Findings

### Category: Simplicity (P19-22)

#### P19. KISS — Alignment: 1/3

**Current state:**
Coercion logic for JSON-to-typed-value conversion is duplicated across three modules with subtly different semantics:

- `src/utils/coercion.py` — "canonical type coercion helpers", strict variant (raises `TypeError`)
- `src/utils/value_coercion.py` — "tolerant and strict variants", returns `None` on failure, raises `CoercionError` on strict path
- `src/cli/commands/build.py:862-902` — four private functions (`_bool_from_payload`, `_optional_int`, `_str_from_payload`, `_optional_str`) that reimplement tolerant coercion inline within a single command

All three encode the same conceptual operation (coerce a `JsonValue | None` to a Python primitive) but with different error contracts, different `bool` handling, and different empty-string semantics. The `coerce_int` in `coercion.py` (line 22) **accepts** `bool` (returns `int(value)`), while `value_coercion.py:coerce_int` (line 21) **rejects** `bool` (returns `None`). This is a behavioral drift risk when callers are refactored.

Additionally, `cli/commands/build.py` contains 240 lines of SCIP config assembly (`_scip_payload_from_config`, `_build_scip_config`, `_ScipPayload`) that has no test seam: it requires a populated `config_contents: Mapping[str, JsonValue]` dict assembled by the upstream Cyclopts pipeline. The function signature contains 18 structural fields that are copy-constructed from defaults.

**Findings:**
- `src/utils/coercion.py:22` — `coerce_int` accepts `bool`, returns `int(value)`. `src/utils/value_coercion.py:21` — `coerce_int` rejects `bool`, returns `None`. These diverge silently.
- `src/cli/commands/build.py:862-902` — Four private coercion helpers that mirror `value_coercion.py` but are unexported and unshared.
- `src/cli/commands/build.py:798-860` — `_scip_payload_from_config` (60 LOC) plus `_build_scip_config` (90 LOC plus helper calls) is entirely private to the CLI command module with no reusable seam for the extraction layer.

**Suggested improvement:**
Consolidate the three coercion surfaces. Designate `src/utils/value_coercion.py` as the canonical module (it has the richer, better-named API), delete `src/utils/coercion.py` or reduce it to a re-export shim, and replace the four private helpers in `cli/commands/build.py:862-902` with calls to `value_coercion.coerce_int`, `coerce_bool`, etc. The SCIP config assembly (`_scip_payload_from_config`) should be moved to `extract/extractors/scip/config.py` as a `ScipIndexConfig.from_config_dict(...)` class method, giving it a natural test seam independent of the CLI layer.

**Effort:** medium
**Risk if unaddressed:** medium — behavioral drift between the two `coerce_int` implementations will cause hard-to-diagnose bugs if callers migrate between modules.

---

#### P21. Least astonishment — Alignment: 2/3

**Current state:**
`src/obs/otel/bootstrap.py:263` stores the global OtelProviders in `_STATE: dict[str, OtelProviders | None] = {"providers": None}` — a mutable dict used as a nullable cell. The pattern works but is surprising: readers expect `_STATE: OtelProviders | None = None` or a `dataclass`. The dict-as-mutable-cell is a workaround for Python's inability to rebind module-level names in closure contexts; here there is no closure, so the workaround is unnecessary and adds cognitive load.

`configure_otel()` (line 648) is idempotent: if providers are already configured, it returns the cached result. This idempotency is critical but is not stated in the public docstring (line 652-664). Callers who call `configure_otel()` from multiple threads or call sites cannot tell from the signature whether they will get the same or a new provider.

**Findings:**
- `src/obs/otel/bootstrap.py:263` — `_STATE: dict[str, OtelProviders | None]` should be a module-level `_PROVIDERS: OtelProviders | None = None` or a thread-safe container.
- `src/obs/otel/bootstrap.py:648-664` — Missing docstring statement about idempotency guarantees.

**Suggested improvement:**
Replace the dict-cell pattern with a simple `_PROVIDERS: OtelProviders | None = None` and access it through a module-level mutator function, matching Python idiom for module-global singletons. Add an explicit "Idempotent: safe to call multiple times; returns cached providers after first call" note to `configure_otel()`'s docstring. Similarly replace `_EXPORTERS: dict[str, DiagnosticsExporters | None]` in `obs/otel/diagnostics_bundle.py` with `_EXPORTERS: DiagnosticsExporters | None = None`.

**Effort:** small
**Risk if unaddressed:** low — no functional defect, but onboarding confusion and test-isolation complexity (`reset_providers_for_tests` side-channel is required).

---

#### P22. Declare and version public contracts — Alignment: 2/3

**Current state:**
`src/serde_artifact_specs.py` (1493 LOC) registers ~50 `ArtifactSpec` constants. The module docstring (line 6) explicitly acknowledges that some specs use `payload_type=None` (unvalidated) while others have typed `msgspec.Struct` payloads. This is correct documentation, but the un-typed specs represent a silent contract gap: any code calling `record_artifact` with these specs has no schema enforcement at the boundary.

`src/serde_schema_registry.py` (802 LOC) imports from 15+ modules at the top level to populate its registry at import time. This creates a high fan-in that makes the module an implicit coupling hub: importing `serde_schema_registry` for any purpose pulls in `datafusion_engine`, `semantics`, `schema_spec`, `obs`, and `relspec` — all of which are heavy subsystems. There is no versioning scheme for the registry format itself.

**Findings:**
- `src/serde_artifact_specs.py:6-12` — Acknowledged untyped specs have no contract enforcement; the policy for when a spec becomes typed (and who is responsible) is undocumented.
- `src/serde_schema_registry.py:17-80` — Top-level import of 15+ modules creates a high-cost import coupling. The registry itself has no version field or evolution contract.

**Suggested improvement:**
Add a `TYPED_SPEC_POLICY.md` comment block at the top of `serde_artifact_specs.py` stating the migration target (all specs must have `payload_type` within N releases) and designate responsibility. For `serde_schema_registry.py`, convert the top-level imports to lazy registration (use `importlib.import_module` inside the registry's `register_all()` call or use `__init_subclass__` hooks) to reduce import-time coupling. Consider adding a `schema_registry_version: int = 1` constant to mark evolution.

**Effort:** medium
**Risk if unaddressed:** low (existing) to medium (if untyped specs grow) — untyped specs silently accept garbage payloads.

---

### Category: Composition (P12-15)

#### P12. Dependency inversion + explicit composition — Alignment: 2/3

**Current state:**
`relspec/ports.py` is excellent: it defines `LineagePort`, `DatasetSpecProvider`, and `RuntimeProfilePort` as narrow Protocol types that allow the `relspec` layer to depend on abstractions. The concrete adapters `_DataFusionLineagePort` and `_DefaultDatasetSpecProvider` in `relspec/inferred_deps.py` implement these ports and are injected optionally via `InferredDepsInputs`.

The weaker area is `obs/otel/bootstrap.py`. The three builder functions `_build_span_exporter`, `_build_metric_exporter`, and `_build_log_exporter` are called directly inside `_build_tracer_provider`, `_build_meter_provider`, and `_build_logger_provider`. There is no exporter-factory port that a caller could override with a custom exporter (beyond the test-mode `InMemorySpanExporter` branch). The `build_meter_provider` function (line 545) is the sole public builder exposed, but there is no equivalent `build_tracer_provider` or `build_logger_provider` — composition is asymmetric.

**Findings:**
- `src/obs/otel/bootstrap.py:281-347` — `_build_span_exporter`, `_build_metric_exporter`, and `_build_log_exporter` are coupled to OTel protocol string constants internally with no injection point for alternate exporters.
- `src/obs/otel/bootstrap.py:545-567` — `build_meter_provider` is public but the analogous `build_tracer_provider` and `build_logger_provider` are private, preventing external composition.

**Suggested improvement:**
Expose `build_tracer_provider` and `build_logger_provider` as public functions matching the `build_meter_provider` pattern. This allows callers (e.g., tests, embedded contexts) to compose their own provider set without calling `configure_otel()` with its global side effects. For the exporter builders, introduce an `ExporterFactory` Protocol with a factory function signature, allowing callers to inject custom exporters in test or embedded contexts.

**Effort:** small
**Risk if unaddressed:** low — existing `test_mode` flag handles the most important case, but custom deployment scenarios (e.g., OpenTelemetry Collector bypass) require monkey-patching.

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
`relspec/policy_compiler.py` accesses semantic IR through three levels of attribute traversal:

```python
# policy_compiler.py:451-455
for view in semantic_ir.views:
    inferred = view.inferred_properties
    if inferred is None or inferred.inferred_join_strategy is None:
        continue
    strategies[view.name] = inferred.inferred_join_strategy
```

The compiler reaches through `semantic_ir` → `views` → `view.inferred_properties` → `inferred_properties.inferred_join_strategy`. If `SemanticIR` restructures its `InferredProperties` shape, `policy_compiler.py` breaks. The same pattern appears for inference confidence (lines 459-483) and cache hints (lines 295-319).

**Findings:**
- `src/relspec/policy_compiler.py:451-455` — Three-level traversal through `semantic_ir.views[*].inferred_properties.inferred_join_strategy`.
- `src/relspec/policy_compiler.py:459-483` — Identical pattern for `inferred_properties.inference_confidence`.
- `src/relspec/policy_compiler.py:295-319` — Identical pattern for `inferred_properties.inferred_cache_policy`.

**Suggested improvement:**
Add a `join_strategy_hints() -> dict[str, str]` method (and equivalents for cache hints and confidence) to `SemanticIR` that returns the flat map directly. The policy compiler calls these query methods instead of traversing the IR structure. This moves the traversal knowledge into `SemanticIR` where it belongs (tell, don't ask).

**Effort:** small
**Risk if unaddressed:** low — the traversal is stable today, but any refactor of `InferredProperties` will silently require updating `policy_compiler.py`.

---

#### P15. Tell, don't ask — Alignment: 2/3

**Current state:**
`graph/product_build.py:_parse_build_result` (lines 624-688) probes `build_result` via `getattr` with default values:

```python
all_outputs = {
    **getattr(build_result, "cpg_outputs", {}),
    **getattr(build_result, "auxiliary_outputs", {}),
}
```

This is defensive coding against an `object`-typed parameter (line 627 declares `build_result: object`). The pattern is a consequence of calling `orchestrate_build` across a module boundary where the return type annotation is not directly accessible in the `TYPE_CHECKING` block. Callers who use `getattr` to probe structure are telling the object how it should answer, rather than asking the object to present its data.

Additionally, `_int_field` (lines 457-470) performs five isinstance checks to coerce a value from a JSON dict — this is repeated "validation" that should be encapsulated in a typed parse.

**Findings:**
- `src/graph/product_build.py:647-651` — `getattr(build_result, "cpg_outputs", {})` probes for attribute existence rather than using a typed return.
- `src/graph/product_build.py:457-470` — `_int_field` is a 13-line coercion routine equivalent to `value_coercion.coerce_int`.

**Suggested improvement:**
Give `BuildResult` (defined in `graph/build_pipeline.py:44`) a public `all_outputs` property that merges `cpg_outputs` and `auxiliary_outputs`. Import it in `product_build.py` via `TYPE_CHECKING`. Replace `getattr` probing with a typed call. Replace `_int_field` with `value_coercion.coerce_int`.

**Effort:** small
**Risk if unaddressed:** low — the current defensive coding works; the risk is fragility if `BuildResult` grows new output categories.

---

### Category: Quality (P23-24)

#### P23. Design for testability — Alignment: 1/3

**Current state:**
Two structural testability problems are present.

**Problem 1: CLI command functions as integration test fixtures.**
`src/cli/commands/build.py:482-629` — `build_command` is the CLI handler. It performs: config loading, SCIP config assembly, incremental config assembly, extraction_config dict construction, and direct `orchestrate_build()` invocation. The function body is 148 lines. The SCIP config assembly alone (`_build_scip_config` + `_scip_payload_from_config`) is 260 lines of private logic with 18 interacting fields. There is no seam to test SCIP config assembly without constructing a full CLI invocation context.

**Problem 2: OTel bootstrap requires global teardown for test isolation.**
`src/obs/otel/bootstrap.py:738-744` — `reset_providers_for_tests()` is the only way to reset the global `_STATE` between tests. This means test suites must know about this internal function, coupling test code to implementation internals. The function's existence is an indicator that the global mutable state is the wrong design for a testable module.

**Findings:**
- `src/cli/commands/build.py:735-860` — `_build_scip_config` and `_scip_payload_from_config` are private to the command, have no test seam, and contain 126 lines of logic for resolving 18 config fields.
- `src/obs/otel/bootstrap.py:738-744` — `reset_providers_for_tests()` exposes a test-only side channel against a global singleton, making test ordering non-trivial.

**Suggested improvement:**
For the CLI: Extract `_build_scip_config` into a `ScipIndexConfig.from_cli_overrides(config: dict, repo_root: Path, overrides: ScipOverrides)` class method on `ScipIndexConfig` in `src/extract/extractors/scip/config.py`. This gives the SCIP config logic a natural unit test without CLI scaffolding.

For OTel bootstrap: Replace the global dict singleton with an `OtelRegistrar` dataclass that holds `providers` and `exporters`. Pass it explicitly through the bootstrap functions. At the module level, expose a module-level `_REGISTRAR` that is optionally injectable for testing. The `configure_otel()` function operates on the module-level instance by default but accepts an optional `registrar=` argument for test isolation.

**Effort:** medium
**Risk if unaddressed:** medium — SCIP config logic is undertested; OTel global state creates test ordering dependencies.

---

#### P24. Observability — Alignment: 3/3

The observability layer is comprehensive and structurally sound. `obs/otel/constants.py` provides canonical `MetricName`, `AttributeName`, `ScopeName`, and `ResourceAttribute` enums. Spans use structured attributes (not string concatenation). The `emit_diagnostics_event` pattern provides a consistent structured event log. The `DiagnosticsPort` protocol in `obs/ports.py` provides a clean abstraction for recording events and artifacts. The `run_context.py` ContextVar approach for propagating `run_id` is correct.

No findings requiring action.

---

## Cross-Cutting Themes

### Theme 1: Dual Validation Layer (pydantic + msgspec)

`src/runtime_models/` uses **pydantic** (`RootConfigRuntime`, `OtelConfigRuntime`, etc.) for runtime config validation arriving from the CLI/TOML path. The same config concepts are also represented as **msgspec Structs** in `src/core/config_specs.py` (used in `serde_schema_registry.py` for schema export). This creates two parallel object models for the same domain data.

Root cause: `runtime_models/` was built to leverage pydantic's rich validation (field-level validators, ge/le constraints) at the CLI intake point, while `core/config_specs.py` uses msgspec for serialization contracts. Both are appropriate for their use case, but together they require maintainers to update two parallel type trees when config fields change.

Affected principles: P7 (DRY), P10 (make illegal states unrepresentable), P19 (KISS).

**Suggested approach:** Designate `core/config_specs.py` (msgspec) as the canonical serialized form. Use pydantic only in `runtime_models/` for the narrow purpose of CLI validation, and add a `to_spec()` method to each `RuntimeBase` subclass that converts the validated pydantic model to the corresponding `core/config_specs` msgspec Struct. This makes the handoff explicit and removes semantic ambiguity about which model is authoritative.

---

### Theme 2: Coercion Knowledge Scattered Across Three Modules

The same "coerce JSON scalar to typed Python value" behavior appears in `utils/coercion.py`, `utils/value_coercion.py`, and inline in `cli/commands/build.py`. The three implementations diverge in `bool` handling (see P19 finding). This is a DRY violation at the knowledge level: the policy "what does it mean to coerce a JSON value to int?" is duplicated.

Root cause: `coercion.py` was likely written first for strict conversion; `value_coercion.py` introduced tolerant variants; the CLI command then added its own because neither existing module had the exact signature needed (`JsonValue | None` with a `default:` param).

Quick fix: Delete `utils/coercion.py` (or make it a thin re-export shim), add a `default:` parameter to `value_coercion.coerce_bool`, and replace all four private CLI helpers.

---

### Theme 3: CLI Command Size Undermines Boundary Clarity

`cli/commands/build.py` at 905 LOC is the largest file in scope. It contains: CLI parameter dataclasses, SCIP config assembly, incremental config assembly, `extraction_config` dict construction, orchestration invocation, and private coercion utilities. The command function itself is 148 lines. The "single reason to change" for a CLI command should be "the CLI interface changes." Changes to SCIP configuration defaults, incremental config fields, or extraction_config schema are currently also reasons to change this file.

Affected principles: P3 (SRP), P19 (KISS), P23 (testability).

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P19 / P7 | Replace 4 private coercion helpers in `cli/commands/build.py:862-902` with `value_coercion` imports | small | Removes behavioral drift risk, reduces file size by ~50 LOC |
| 2 | P21 | Replace `_STATE: dict[str, ...]` dict-cell in `obs/otel/bootstrap.py:263` and `_EXPORTERS` in `diagnostics_bundle.py` with direct nullable module variables | small | Improves readability, removes confusing idiom |
| 3 | P15 | Replace `_int_field` in `graph/product_build.py:457-470` with `value_coercion.coerce_int` | small | Removes 14 LOC, removes duplication |
| 4 | P14 | Add `join_strategy_hints()`, `cache_policy_hints()`, `confidence_hints()` query methods to `SemanticIR`; update three traversal loops in `policy_compiler.py` | small | Isolates `relspec` from `SemanticIR` internal structure changes |
| 5 | P12 | Expose `build_tracer_provider` and `build_logger_provider` as public functions in `obs/otel/bootstrap.py` | small | Enables external composition; reduces dependence on `configure_otel()` side effects |

---

## Recommended Action Sequence

1. **Coercion consolidation** (P19, P7): Merge `utils/coercion.py` into `utils/value_coercion.py`. Replace 4 private CLI helpers. Replace `_int_field` in `product_build.py`. One PR, ~1 hour.

2. **OTel bootstrap readability** (P21): Replace dict-cell state pattern in `bootstrap.py` and `diagnostics_bundle.py` with direct module-level nullables. One PR, ~30 minutes.

3. **SemanticIR query methods** (P14, P15): Add 3 query methods to `SemanticIR`; simplify 3 loops in `policy_compiler.py`. One PR, ~1 hour.

4. **CLI SCIP config extraction** (P23, P3): Move `_scip_payload_from_config` and `_build_scip_config` to `extract/extractors/scip/config.py` as a class or module-level function. Add unit tests. One PR, ~3 hours.

5. **OTel bootstrap testability** (P23): Extract `_REGISTRAR` injection point for test isolation, removing dependence on `reset_providers_for_tests()` side channel. One PR, ~2 hours.

6. **Pydantic/msgspec handoff protocol** (P7, Theme 1): Add `to_spec()` converters to each `RuntimeBase` subclass, making the pydantic→msgspec boundary explicit. One PR, ~3 hours.

7. **serde_artifact_specs untyped spec policy** (P22): Add typed `payload_type` to at least 5 highest-traffic untyped specs per release cycle. Track in a comment block at the top of the module. Ongoing, low per-cycle effort.

---

## Rust Migration Candidates

| Module | Current Approach | Rust Migration Fit | Rationale |
|---|---|---|---|
| `relspec/policy_compiler.py` (cache-policy derivation) | Python: iterates `rustworkx` graph topology via `task_graph.graph.out_degree(node_idx)` | High | The graph traversal (`_derive_cache_policies`) iterates a rustworkx graph; this is naturally expressible as a Rust graph operation. Moving policy compilation to Rust would eliminate the Python-Rust graph-crossing overhead on the hot path. |
| `relspec/inferred_deps.py` (lineage extraction coordination) | Python: orchestrates DataFusion plan lineage extraction via Python bindings | Medium | The coordination logic is thin; the actual lineage extraction is already in Rust-backed DataFusion. The Python layer could be a thin serialized-protocol dispatcher rather than a class. |
| `obs/otel/` (logging/tracing) | Python OpenTelemetry SDK | Low (by architecture choice) | The Rust `codeanatomy_engine` already uses `tracing`; bridging the two requires explicit OTLP endpoint configuration. Migrating the Python OTel layer to Rust `tracing` would require a complete overhaul of the bootstrap contract and is not justified unless the Python and Rust pipeline stages merge. |
| `utils/hashing.py` | Python: `hashlib` + `msgspec` | Low | Hashing is correct and fast enough; Rust migration adds no meaningful benefit for the call volumes here. |

---

## DF52 Migration Impact

| Component | Current API Usage | DF52 Change Impact |
|---|---|---|
| `relspec/inferred_deps.py` (plan lineage) | Calls `datafusion_engine.lineage.reporting.extract_lineage(plan, ...)` on `plan_bundle.optimized_logical_plan` | **Low direct impact.** The Python-side lineage extraction reads plan attributes. DF52 changes to `FileSource`, `FileScanConfig`, and `PhysicalOptimizerRule` are primarily Rust-side. If the Rust engine is upgraded to DF52, `optimized_logical_plan` serialization may change; the plan proto bytes extension types in `serde_msgspec_ext.py` (`LogicalPlanProtoBytes`, `OptimizedPlanProtoBytes`) may need version validation. |
| `relspec/compiled_policy.py` + `policy_compiler.py` | Uses `CachePolicyValue: Literal["none", "delta_staging", "delta_output"]` — no direct DataFusion API calls | **No impact.** Pure policy logic. |
| `serde_schema_registry.py` | Imports `datafusion_engine.plan.cache.PlanCacheEntry`, `PlanCacheKey`, `PlanProtoCacheEntry` | **Potential impact.** DF52 adds `statistics_cache()` and `list_files_cache()` as new cache lifecycle entry points. If `PlanCacheKey` or `PlanCacheEntry` structs are extended to carry statistics cache identifiers, the schema registry's `PlanCacheEntry` import may need corresponding field additions to remain round-trip safe. |
| `obs/otel/` | No DataFusion API usage | **No impact.** |
| `cli/commands/build.py` | No DataFusion API usage; passes `extraction_config` as opaque dict | **No impact at CLI layer.** DF52 scan pushdown changes (`FileSource.with_projection` removal, `try_pushdown_projection`) would affect `datafusion_engine/` layer, not CLI. |

---

## Planning-Object Consolidation

**Bespoke code replacing DF built-ins:**

`src/graph/product_build.py:_parse_finalize` (lines 473-484) and `_parse_table` (lines 487-491) manually deserialize JSON dicts returned by `orchestrate_build()` into Python dataclasses. These JSON dicts (`FinalizeDeltaReport`, `TableDeltaReport`) are produced by the Rust engine. The deserialization path uses string key access, `cast`, and the local `_int_field` coercion function rather than a typed msgspec decoder.

This is a bespoke deserialization layer replacing what `msgspec.json.decode(bytes, type=FinalizeDeltaReport)` could do if `BuildResult` returned msgspec-encodable structs. `BuildResult` in `graph/build_pipeline.py:44` uses `dict[str, dict[str, object]]` for `cpg_outputs` and `auxiliary_outputs`, which prevents typed decode.

**Recommended consolidation:**
Change `BuildResult.cpg_outputs` from `dict[str, dict[str, object]]` to `dict[str, FinalizeReport | TableReport]` using a Union type or `msgspec.Raw`, and decode at the Rust boundary using `msgspec.json.decode`. This eliminates `_parse_finalize`, `_parse_table`, `_int_field`, `_optional`, and `_require_cpg_output` from `product_build.py` — approximately 80 LOC of bespoke deserialization.

**LOC reduction estimate:** ~80 LOC in `product_build.py` + ~50 LOC in `cli/commands/build.py` (coercion helpers) = ~130 LOC total, plus elimination of the `_int_field` / coercion duplication from `value_coercion` consolidation (~40 LOC in `coercion.py`).
